use std::fmt;
use std::io::Read;
use std::path::PathBuf;

use agent_runtime::core::catalog::ModelLimits;
use agent_runtime::core::store::Secret;
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Deserialize;

use crate::CliError;
use crate::config::{self, LoadedConfig, ResolvedMcpServer};

/// Reference command-line host for `agent-runtime`.
#[derive(Debug, Parser)]
#[command(
    name = "agent-runtime",
    version,
    about = "Run one prompt through the embeddable agent runtime"
)]
pub struct Cli {
    /// Command to execute.
    #[command(subcommand)]
    pub command: Command,
}

/// Available commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Execute exactly one user turn and exit.
    Run(RunArgs),
    /// Inspect Model Context Protocol host configuration.
    Mcp(McpArgs),
}

/// MCP host commands.
#[derive(Debug, Args)]
pub struct McpArgs {
    /// MCP command to execute.
    #[command(subcommand)]
    pub command: McpCommand,
}

/// Available MCP host commands.
#[derive(Debug, Subcommand)]
pub enum McpCommand {
    /// Statically inspect configured servers without starting them.
    Inspect(McpInspectArgs),
}

/// Arguments for `agent-runtime mcp inspect`.
#[derive(Debug, Args)]
pub struct McpInspectArgs {
    /// Explicit versioned CLI configuration file.
    #[arg(long, value_name = "PATH")]
    pub config: PathBuf,

    /// Inspect only this configured server.
    #[arg(value_name = "SERVER")]
    pub server: Option<String>,
}

/// Arguments for `agent-runtime run`.
#[derive(Args)]
pub struct RunArgs {
    /// Explicit versioned CLI configuration file. No ambient files are searched.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Provider adapter to use.
    #[arg(long, value_enum)]
    pub provider: Option<ProviderKind>,

    /// Provider model identifier.
    #[arg(long)]
    pub model: Option<String>,

    /// Total input-plus-output context window in tokens.
    #[arg(long)]
    pub context_tokens: Option<u32>,

    /// Maximum accepted input tokens.
    #[arg(long)]
    pub max_input_tokens: Option<u32>,

    /// Maximum generated output tokens.
    #[arg(long)]
    pub max_output_tokens: Option<u32>,

    /// Override the provider base URL. Required for openai-compatible.
    #[arg(long)]
    pub base_url: Option<String>,

    /// Read the API key from this environment variable name.
    #[arg(long)]
    pub api_key_env: Option<String>,

    /// Output assistant text or canonical event envelopes.
    #[arg(long, value_enum)]
    pub output: Option<OutputFormat>,

    /// Explicitly authorize spawning this configured stdio MCP server for this run.
    #[arg(long, value_name = "SERVER", action = clap::ArgAction::Append)]
    pub allow_mcp_server: Vec<String>,

    /// Explicitly approve one configured MCP tool as SERVER/TOOL for this run.
    #[arg(long, value_name = "SERVER/TOOL", action = clap::ArgAction::Append)]
    pub allow_mcp_tool: Vec<String>,

    /// Prompt text. When omitted, a non-terminal stdin is read to EOF.
    #[arg(value_name = "PROMPT")]
    pub prompt: Option<String>,
}

impl fmt::Debug for RunArgs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunArgs")
            .field("config", &self.config)
            .field("provider", &self.provider)
            .field("model", &self.model)
            .field("context_tokens", &self.context_tokens)
            .field("max_input_tokens", &self.max_input_tokens)
            .field("max_output_tokens", &self.max_output_tokens)
            .field("base_url_configured", &self.base_url.is_some())
            .field("api_key_env", &self.api_key_env)
            .field("output", &self.output)
            .field("allow_mcp_server", &self.allow_mcp_server)
            .field("allow_mcp_tool", &self.allow_mcp_tool)
            .field(
                "prompt_chars",
                &self.prompt.as_ref().map(|prompt| prompt.chars().count()),
            )
            .finish()
    }
}

/// Supported first-party provider adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderKind {
    /// OpenAI Chat Completions-compatible API at the canonical OpenAI URL.
    Openai,
    /// OpenAI-compatible API at an explicit base URL.
    OpenaiCompatible,
    /// Anthropic Messages API.
    Anthropic,
    /// xAI Responses API.
    Xai,
    /// Google Gemini Interactions API.
    Gemini,
}

impl ProviderKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::OpenaiCompatible => "openai-compatible",
            Self::Anthropic => "anthropic",
            Self::Xai => "xai",
            Self::Gemini => "gemini",
        }
    }

    fn default_api_key_env(self) -> &'static str {
        match self {
            Self::Openai | Self::OpenaiCompatible => "OPENAI_API_KEY",
            Self::Anthropic => "ANTHROPIC_API_KEY",
            Self::Xai => "XAI_API_KEY",
            Self::Gemini => "GEMINI_API_KEY",
        }
    }
}

/// Stable stdout representation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Stream only visible assistant text.
    #[default]
    Text,
    /// Stream one canonical event envelope per line.
    Jsonl,
}

/// Injectable environment lookup.
pub trait Environment {
    /// Reads one Unicode environment value without exposing it in an error.
    fn read(&self, name: &str) -> Result<Option<String>, CliError>;
}

/// Process environment implementation.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessEnvironment;

impl Environment for ProcessEnvironment {
    fn read(&self, name: &str) -> Result<Option<String>, CliError> {
        match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(CliError::Configuration(format!(
                "environment variable `{name}` is not valid Unicode"
            ))),
        }
    }
}

pub(crate) struct ResolvedRunConfig {
    pub(crate) provider: ProviderKind,
    pub(crate) model: String,
    pub(crate) limits: ModelLimits,
    pub(crate) base_url: Option<String>,
    pub(crate) api_key: Secret,
    pub(crate) prompt: String,
    pub(crate) output: OutputFormat,
    pub(crate) mcp_servers: Vec<ResolvedMcpServer>,
}

impl fmt::Debug for ResolvedRunConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedRunConfig")
            .field("provider", &self.provider)
            .field("model", &self.model)
            .field("limits", &self.limits)
            .field("base_url_configured", &self.base_url.is_some())
            .field("api_key", &self.api_key)
            .field("prompt_chars", &self.prompt.chars().count())
            .field("output", &self.output)
            .field(
                "mcp_servers",
                &self
                    .mcp_servers
                    .iter()
                    .map(|server| server.config.name.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl ResolvedRunConfig {
    pub(crate) fn resolve(
        args: RunArgs,
        stdin: &mut impl Read,
        stdin_is_terminal: bool,
        environment: &impl Environment,
    ) -> Result<Self, CliError> {
        let loaded = args
            .config
            .as_deref()
            .map(|path| config::load(path, environment))
            .transpose()?;
        let defaults = loaded
            .as_ref()
            .map(|config| config.run.clone())
            .unwrap_or_default();

        let provider = args.provider.or(defaults.provider).ok_or_else(|| {
            CliError::Configuration(
                "--provider is required when it is not supplied by --config".to_owned(),
            )
        })?;
        let model = required_text(args.model.or(defaults.model), "--model")?;
        let context_tokens = required_number(
            args.context_tokens.or(defaults.context_tokens),
            "--context-tokens",
        )?;
        let max_input_tokens = required_number(
            args.max_input_tokens.or(defaults.max_input_tokens),
            "--max-input-tokens",
        )?;
        let max_output_tokens = required_number(
            args.max_output_tokens.or(defaults.max_output_tokens),
            "--max-output-tokens",
        )?;
        validate_limits(context_tokens, max_input_tokens, max_output_tokens)?;

        let base_url = args
            .base_url
            .or(defaults.base_url)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        if provider == ProviderKind::OpenaiCompatible && base_url.is_none() {
            return Err(CliError::Configuration(
                "--base-url is required for --provider openai-compatible".to_owned(),
            ));
        }

        let mcp_servers = match &loaded {
            Some(config) => config.resolve_selected(
                &args.allow_mcp_server,
                &args.allow_mcp_tool,
                environment,
            )?,
            None if args.allow_mcp_server.is_empty() && args.allow_mcp_tool.is_empty() => {
                Vec::new()
            }
            None => {
                return Err(CliError::Configuration(
                    "--allow-mcp-server and --allow-mcp-tool require --config".to_owned(),
                ));
            }
        };

        let prompt = resolve_prompt(args.prompt, stdin, stdin_is_terminal)?;
        let api_key_env = args
            .api_key_env
            .or(defaults.api_key_env)
            .unwrap_or_else(|| provider.default_api_key_env().to_owned());
        config::validate_env_name(&api_key_env, "--api-key-env")?;
        let api_key = environment
            .read(&api_key_env)?
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                CliError::Configuration(format!(
                    "provider credential is missing; set environment variable `{api_key_env}`"
                ))
            })?;

        Ok(Self {
            provider,
            model,
            limits: ModelLimits::new(context_tokens, max_input_tokens, max_output_tokens),
            base_url,
            api_key: Secret::new(api_key),
            prompt,
            output: args.output.or(defaults.output).unwrap_or_default(),
            mcp_servers,
        })
    }
}

pub(crate) fn load_for_inspection(
    args: &McpInspectArgs,
    environment: &impl Environment,
) -> Result<LoadedConfig, CliError> {
    config::load(&args.config, environment)
}

fn required_text(value: Option<String>, flag: &str) -> Result<String, CliError> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            CliError::Configuration(format!(
                "{flag} is required when it is not supplied by --config"
            ))
        })
}

fn required_number(value: Option<u32>, flag: &str) -> Result<u32, CliError> {
    value.ok_or_else(|| {
        CliError::Configuration(format!(
            "{flag} is required when it is not supplied by --config"
        ))
    })
}

fn validate_limits(
    context_tokens: u32,
    max_input_tokens: u32,
    max_output_tokens: u32,
) -> Result<(), CliError> {
    if context_tokens == 0 {
        return Err(CliError::Configuration(
            "--context-tokens must be greater than zero".to_owned(),
        ));
    }
    if max_input_tokens == 0 || max_input_tokens > context_tokens {
        return Err(CliError::Configuration(
            "--max-input-tokens must be greater than zero and no larger than --context-tokens"
                .to_owned(),
        ));
    }
    if max_output_tokens == 0 || max_output_tokens > context_tokens {
        return Err(CliError::Configuration(
            "--max-output-tokens must be greater than zero and no larger than --context-tokens"
                .to_owned(),
        ));
    }
    Ok(())
}

fn resolve_prompt(
    positional: Option<String>,
    stdin: &mut impl Read,
    stdin_is_terminal: bool,
) -> Result<String, CliError> {
    let prompt = match positional {
        Some(prompt) => prompt,
        None if stdin_is_terminal => {
            return Err(CliError::Configuration(
                "no prompt supplied; pass PROMPT or pipe prompt text on stdin".to_owned(),
            ));
        }
        None => {
            let mut prompt = String::new();
            stdin
                .read_to_string(&mut prompt)
                .map_err(|error| CliError::io("read prompt from stdin", error))?;
            prompt
        }
    };
    if prompt.trim().is_empty() {
        return Err(CliError::Configuration(
            "prompt must not be empty".to_owned(),
        ));
    }
    Ok(prompt)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use clap::Parser;

    use super::*;

    #[derive(Default)]
    struct MapEnvironment(BTreeMap<String, String>);

    impl Environment for MapEnvironment {
        fn read(&self, name: &str) -> Result<Option<String>, CliError> {
            Ok(self.0.get(name).cloned())
        }
    }

    fn args(provider: ProviderKind) -> RunArgs {
        RunArgs {
            config: None,
            provider: Some(provider),
            model: Some("model-1".to_owned()),
            context_tokens: Some(16_384),
            max_input_tokens: Some(12_288),
            max_output_tokens: Some(4_096),
            base_url: (provider == ProviderKind::OpenaiCompatible)
                .then(|| "https://provider.example/v1".to_owned()),
            api_key_env: Some("TEST_API_KEY".to_owned()),
            output: Some(OutputFormat::Text),
            allow_mcp_server: Vec::new(),
            allow_mcp_tool: Vec::new(),
            prompt: Some("hello".to_owned()),
        }
    }

    fn environment() -> MapEnvironment {
        MapEnvironment(BTreeMap::from([(
            "TEST_API_KEY".to_owned(),
            "very-secret".to_owned(),
        )]))
    }

    #[test]
    fn clap_accepts_every_provider_and_output_value() {
        for provider in ["openai", "openai-compatible", "anthropic", "xai", "gemini"] {
            let mut argv = vec![
                "agent-runtime",
                "run",
                "--provider",
                provider,
                "--model",
                "model-1",
                "--context-tokens",
                "16384",
                "--max-input-tokens",
                "12288",
                "--max-output-tokens",
                "4096",
                "--output",
                "jsonl",
            ];
            if provider == "openai-compatible" {
                argv.extend(["--base-url", "https://provider.example/v1"]);
            }
            argv.push("hello");
            assert!(Cli::try_parse_from(argv).is_ok(), "provider={provider}");
        }
    }

    #[test]
    fn positional_prompt_wins_without_reading_stdin() {
        struct FailingRead;
        impl Read for FailingRead {
            fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("stdin should not be read"))
            }
        }

        let resolved = ResolvedRunConfig::resolve(
            args(ProviderKind::Openai),
            &mut FailingRead,
            true,
            &environment(),
        )
        .expect("positional prompt resolves");
        assert_eq!(resolved.prompt, "hello");
    }

    #[test]
    fn piped_prompt_is_read_to_eof() {
        let mut run = args(ProviderKind::Anthropic);
        run.prompt = None;
        let resolved =
            ResolvedRunConfig::resolve(run, &mut &b"hello from stdin\n"[..], false, &environment())
                .expect("stdin prompt resolves");
        assert_eq!(resolved.prompt, "hello from stdin\n");
    }

    #[test]
    fn terminal_without_prompt_fails_before_environment_lookup() {
        let mut run = args(ProviderKind::Openai);
        run.prompt = None;
        let error =
            ResolvedRunConfig::resolve(run, &mut &b""[..], true, &MapEnvironment::default())
                .expect_err("terminal input must fail");
        assert!(error.to_string().contains("no prompt supplied"));
    }

    #[test]
    fn empty_prompt_and_invalid_limits_fail() {
        let mut empty = args(ProviderKind::Openai);
        empty.prompt = Some(" \n".to_owned());
        assert!(
            ResolvedRunConfig::resolve(empty, &mut &b""[..], true, &environment())
                .expect_err("empty prompt must fail")
                .to_string()
                .contains("must not be empty")
        );

        let mut invalid = args(ProviderKind::Openai);
        invalid.max_input_tokens = Some(invalid.context_tokens.unwrap() + 1);
        assert!(
            ResolvedRunConfig::resolve(invalid, &mut &b""[..], true, &environment())
                .expect_err("invalid limits must fail")
                .to_string()
                .contains("--max-input-tokens")
        );
    }

    #[test]
    fn openai_compatible_requires_a_base_url() {
        let mut run = args(ProviderKind::OpenaiCompatible);
        run.base_url = None;
        let error = ResolvedRunConfig::resolve(run, &mut &b""[..], true, &environment())
            .expect_err("base URL must be required");
        assert!(error.to_string().contains("--base-url is required"));
    }

    #[test]
    fn missing_credential_names_only_the_variable() {
        let error = ResolvedRunConfig::resolve(
            args(ProviderKind::Gemini),
            &mut &b""[..],
            true,
            &MapEnvironment::default(),
        )
        .expect_err("credential must be required");
        assert!(error.to_string().contains("`TEST_API_KEY`"));
    }

    #[test]
    fn resolved_debug_redacts_credential_and_prompt() {
        let resolved = ResolvedRunConfig::resolve(
            args(ProviderKind::Xai),
            &mut &b""[..],
            true,
            &environment(),
        )
        .expect("config resolves");
        let debug = format!("{resolved:?}");
        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("very-secret"));
        assert!(!debug.contains("hello"));
    }
}
