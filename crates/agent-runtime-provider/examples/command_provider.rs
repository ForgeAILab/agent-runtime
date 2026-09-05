//! Minimal consumer-owned JSONL command adapter.
//!
//! This example is compile-only: a real host supplies an authorized absolute
//! executable and working directory, plus a codec for one named CLI/version.

#[cfg(feature = "command-provider")]
mod enabled {
    use std::path::PathBuf;
    use std::sync::Arc;

    use agent_runtime_provider::command::{
        CommandAdapter, CommandAttempt, CommandOutputDecoder, CommandProcessConfig, CommandProvider,
    };
    use agent_runtime_provider::core::provider::{
        AuthKind, Capabilities, ModelDescriptor, ModelId, ProviderCallContext, ProviderError,
        ProviderErrorKind, ProviderRequest, ProviderStreamEvent, ReasoningSupport,
    };

    #[derive(Debug)]
    struct JsonlDecoder;

    impl CommandOutputDecoder for JsonlDecoder {
        fn decode_frame(
            &mut self,
            frame: &[u8],
        ) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
            serde_json::from_slice(frame)
                .map(|event| vec![event])
                .map_err(|_| {
                    ProviderError::new(
                        ProviderErrorKind::MalformedStream,
                        "named CLI emitted malformed JSONL",
                    )
                })
        }
    }

    #[derive(Debug)]
    struct NamedCliAdapter;

    impl CommandAdapter for NamedCliAdapter {
        fn describe(&self) -> Vec<ModelDescriptor> {
            vec![ModelDescriptor {
                id: ModelId::new("consumer-model"),
                display_name: "Consumer model CLI".to_owned(),
                vendor: "consumer".to_owned(),
                capabilities: Capabilities {
                    streaming: true,
                    tools: false,
                    reasoning: ReasoningSupport::Unsupported,
                    structured_output: false,
                    usage: false,
                    cache: false,
                    prompt_cache: Default::default(),
                    cache_contract: None,
                    auth: AuthKind::Custom("local_cli".to_owned()),
                    continuation: false,
                    max_output_tokens: None,
                },
            }]
        }

        fn prepare(
            &self,
            request: &ProviderRequest,
            _context: &ProviderCallContext,
        ) -> Result<CommandAttempt, ProviderError> {
            let stdin = serde_json::to_vec(request).map_err(|_| {
                ProviderError::new(
                    ProviderErrorKind::BadRequest,
                    "provider request could not be encoded",
                )
            })?;
            Ok(CommandAttempt::new(
                vec!["--machine-jsonl".to_owned()],
                stdin,
                Box::new(JsonlDecoder),
            ))
        }
    }

    #[allow(dead_code)]
    fn build(
        executable: PathBuf,
        cwd: PathBuf,
    ) -> Result<CommandProvider, Box<dyn std::error::Error>> {
        let process = CommandProcessConfig::new(executable, cwd)?.with_env("NO_COLOR", "1")?;
        Ok(CommandProvider::new(process, Arc::new(NamedCliAdapter))?)
    }
}

fn main() {}
