use std::collections::BTreeSet;
use std::sync::Arc;

use agent_runtime::core::catalog::ResolvedModelProfile;
use agent_runtime::core::check_set::ActionClass;
use agent_runtime::core::grant::SecurityCheckMode;
use agent_runtime::core::provider::{Capabilities, ModelId, Provider};
use agent_runtime::core::tool::Tool;
use agent_runtime::provider::anthropic::{AnthropicConfig, AnthropicProvider};
use agent_runtime::provider::gemini::{GeminiInteractionsConfig, GeminiInteractionsProvider};
use agent_runtime::provider::openai::{OpenAiConfig, OpenAiProvider};
use agent_runtime::provider::responses::{ResponsesConfig, ResponsesProvider};
use agent_runtime::runtime::{Runtime, RuntimeBuilder};
use agent_runtime_mcp::{McpClient, McpConnection, McpTool};

use crate::CliError;
use crate::command::{OutputFormat, ProviderKind, ResolvedRunConfig};
use crate::mcp::{CliMcpPolicy, shutdown_connections};
use crate::transport::CliHttpTransport;

pub(crate) struct PreparedRun {
    pub(crate) runtime: Runtime,
    pub(crate) prompt: String,
    pub(crate) output: OutputFormat,
    pub(crate) connections: Vec<Arc<McpConnection>>,
    pub(crate) diagnostics: Vec<String>,
}

pub(crate) async fn prepare_run(config: ResolvedRunConfig) -> Result<PreparedRun, CliError> {
    let ResolvedRunConfig {
        provider,
        model,
        limits,
        base_url,
        api_key,
        prompt,
        output,
        mcp_servers,
    } = config;

    let (provider_impl, capabilities): (Arc<dyn Provider>, Capabilities) = match provider {
        ProviderKind::Openai => {
            let mut config = OpenAiConfig::openai(&model).with_api_key(api_key);
            if let Some(base_url) = base_url {
                config.base_url = base_url;
            }
            config.capabilities.max_output_tokens = Some(limits.max_output_tokens);
            let capabilities = config.capabilities.clone();
            let transport = transport_for(&config.base_url)?;
            (
                Arc::new(OpenAiProvider::new(transport, config)),
                capabilities,
            )
        }
        ProviderKind::OpenaiCompatible => {
            let base_url = base_url.expect("validated OpenAI-compatible base URL");
            let mut config = OpenAiConfig::new(base_url, &model).with_api_key(api_key);
            config.capabilities.max_output_tokens = Some(limits.max_output_tokens);
            let capabilities = config.capabilities.clone();
            let transport = transport_for(&config.base_url)?;
            (
                Arc::new(OpenAiProvider::new(transport, config)),
                capabilities,
            )
        }
        ProviderKind::Anthropic => {
            let mut config = AnthropicConfig::anthropic(&model).with_api_key(api_key);
            if let Some(base_url) = base_url {
                config.base_url = base_url;
            }
            config.capabilities.max_output_tokens = Some(limits.max_output_tokens);
            let capabilities = config.capabilities.clone();
            let transport = transport_for(&config.base_url)?;
            (
                Arc::new(AnthropicProvider::new(transport, config)),
                capabilities,
            )
        }
        ProviderKind::Xai => {
            let mut config = ResponsesConfig::xai(&model).with_api_key(api_key);
            if let Some(base_url) = base_url {
                config.base_url = base_url;
            }
            config.capabilities.max_output_tokens = Some(limits.max_output_tokens);
            let capabilities = config.capabilities.clone();
            let transport = transport_for(&config.base_url)?;
            let provider = ResponsesProvider::new(transport, config)
                .map_err(|error| CliError::ProviderSetup(error.message))?;
            (Arc::new(provider), capabilities)
        }
        ProviderKind::Gemini => {
            let mut config = GeminiInteractionsConfig::google(&model).with_api_key(api_key);
            if let Some(base_url) = base_url {
                config.base_url = base_url;
            }
            config.capabilities.max_output_tokens = Some(limits.max_output_tokens);
            let capabilities = config.capabilities.clone();
            let transport = transport_for(&config.base_url)?;
            let provider = GeminiInteractionsProvider::new(transport, config)
                .map_err(|error| CliError::ProviderSetup(error.message))?;
            (Arc::new(provider), capabilities)
        }
    };

    let model_id = ModelId::new(&model);
    let profile = ResolvedModelProfile::explicit(provider.as_str(), model_id.clone(), limits)
        .with_capabilities(capabilities);
    let mut builder = RuntimeBuilder::new(model_id)
        .provider_name(provider.as_str())
        .provider(provider_impl)
        .model_profile(profile);

    let mut connections = Vec::new();
    let mut diagnostics = Vec::new();
    let mut tools: Vec<Arc<dyn Tool>> = Vec::new();
    let mut tool_names = BTreeSet::new();
    let client = McpClient::new();

    for server in mcp_servers {
        let server_name = server.config.name.clone();
        let connected = client.connect_and_bind(&server.config).await;
        let (connection, bindings, rejected) = match connected {
            Ok(connected) => connected,
            Err(error) if server.required => {
                let cleanup = shutdown_connections(&connections).await;
                return Err(CliError::McpSetup(format_mcp_failure(
                    &server_name,
                    &error.to_string(),
                    &cleanup,
                )));
            }
            Err(error) => {
                diagnostics.push(format!(
                    "optional MCP server `{server_name}` is unavailable: {error}"
                ));
                continue;
            }
        };
        let connection = Arc::new(connection);
        let bound_names = bindings
            .iter()
            .map(|binding| binding.remote_name.clone())
            .collect::<BTreeSet<_>>();
        let missing = server
            .approved_tools
            .difference(&bound_names)
            .cloned()
            .collect::<Vec<_>>();

        if !rejected.is_empty() || !missing.is_empty() {
            let mut reasons = rejected.iter().map(ToString::to_string).collect::<Vec<_>>();
            if !missing.is_empty() {
                reasons.push(format!(
                    "approved tool(s) not advertised by the server: {}",
                    missing.join(", ")
                ));
            }
            let cleanup = shutdown_connections(std::slice::from_ref(&connection)).await;
            if server.required {
                let mut rollback = cleanup;
                rollback.extend(shutdown_connections(&connections).await);
                return Err(CliError::McpSetup(format_mcp_failure(
                    &server_name,
                    &reasons.join("; "),
                    &rollback,
                )));
            }
            diagnostics.push(format!(
                "optional {}",
                format_mcp_failure(&server_name, &reasons.join("; "), &cleanup)
            ));
            continue;
        }

        for binding in bindings {
            tool_names.insert(binding.model_facing_name.clone());
            tools.push(Arc::new(McpTool::new(
                connection.clone(),
                binding,
                server.config.request_timeout,
                server.config.max_output_bytes,
            )));
        }
        connections.push(connection);
    }

    if !tools.is_empty() {
        let policy = Arc::new(CliMcpPolicy::new(tool_names));
        builder = builder
            .tools(tools)
            .security_check(
                policy.clone(),
                SecurityCheckMode::Authoritative,
                policy.coverage(),
                ActionClass::new("cli-mcp"),
            )
            .approval(policy);
    }

    let runtime = match builder.build() {
        Ok(runtime) => runtime,
        Err(error) => {
            let _cleanup = shutdown_connections(&connections).await;
            return Err(error.into());
        }
    };

    Ok(PreparedRun {
        runtime,
        prompt,
        output,
        connections,
        diagnostics,
    })
}

fn format_mcp_failure(server: &str, reason: &str, cleanup: &[String]) -> String {
    let mut message = format!("MCP server `{server}` failed: {reason}");
    if !cleanup.is_empty() {
        message.push_str("; cleanup: ");
        message.push_str(&cleanup.join("; "));
    }
    message
}

fn transport_for(base_url: &str) -> Result<CliHttpTransport, CliError> {
    CliHttpTransport::new(base_url).map_err(|error| CliError::Configuration(error.message))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use agent_runtime::core::catalog::ModelLimits;
    use agent_runtime::core::store::Secret;
    use agent_runtime_mcp::McpServerConfig;

    use super::*;
    use crate::config::ResolvedMcpServer;

    fn config(provider: ProviderKind) -> ResolvedRunConfig {
        ResolvedRunConfig {
            provider,
            model: "model-1".to_owned(),
            limits: ModelLimits::new(16_384, 12_288, 4_096),
            base_url: (provider == ProviderKind::OpenaiCompatible)
                .then(|| "https://provider.example/v1".to_owned()),
            api_key: Secret::new("very-secret"),
            prompt: "hello".to_owned(),
            output: OutputFormat::Text,
            mcp_servers: Vec::new(),
        }
    }

    #[tokio::test]
    async fn every_supported_provider_composes_without_network_io() {
        for provider in [
            ProviderKind::Openai,
            ProviderKind::OpenaiCompatible,
            ProviderKind::Anthropic,
            ProviderKind::Xai,
            ProviderKind::Gemini,
        ] {
            assert!(
                prepare_run(config(provider)).await.is_ok(),
                "provider={provider:?}"
            );
        }
    }

    #[tokio::test]
    async fn unsafe_base_url_is_a_configuration_error() {
        let mut config = config(ProviderKind::OpenaiCompatible);
        config.base_url = Some("http://127.0.0.1:11434/v1".to_owned());
        let error = match prepare_run(config).await {
            Ok(_) => panic!("unsafe endpoint must fail"),
            Err(error) => error,
        };
        assert!(matches!(error, CliError::Configuration(_)));
        assert_eq!(error.exit_code(), 2);
    }

    #[cfg(unix)]
    fn failing_server(required: bool) -> ResolvedMcpServer {
        ResolvedMcpServer {
            config: McpServerConfig::stdio("broken", "/usr/bin/false")
                .with_tool_filter(agent_runtime_mcp::ToolFilter::Allow(vec![
                    "search".to_owned(),
                ]))
                .with_startup_timeout(Duration::from_secs(1)),
            required,
            approved_tools: BTreeSet::from(["search".to_owned()]),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn required_mcp_startup_failure_fails_before_the_provider_turn() {
        let mut config = config(ProviderKind::Openai);
        config.mcp_servers.push(failing_server(true));
        let error = match prepare_run(config).await {
            Ok(_) => panic!("required MCP failure must fail preparation"),
            Err(error) => error,
        };
        assert!(matches!(error, CliError::McpSetup(_)));
        assert_eq!(error.exit_code(), 1);
        assert!(error.to_string().contains("broken"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn optional_mcp_startup_failure_is_an_isolated_diagnostic() {
        let mut config = config(ProviderKind::Anthropic);
        config.mcp_servers.push(failing_server(false));
        let prepared = prepare_run(config)
            .await
            .expect("optional MCP failure is isolated");
        assert!(prepared.connections.is_empty());
        assert_eq!(prepared.diagnostics.len(), 1);
        assert!(prepared.diagnostics[0].contains("optional MCP server `broken`"));
    }
}
