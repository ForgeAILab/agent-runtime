# Implementation Tasks

- [x] Add fluent builders and well-known provider presets to `OpenAiConfig` in `crates/agent-runtime-provider/src/openai.rs` <!-- id: 1 -->
- [x] Add fluent builders and `anthropic` preset to `AnthropicConfig` in `crates/agent-runtime-provider/src/anthropic.rs` <!-- id: 2 -->
- [x] Add fluent builders and `xai` preset to `ResponsesConfig` in `crates/agent-runtime-provider/src/responses.rs` <!-- id: 3 -->
- [x] Add fluent builders and `google` preset to `GeminiInteractionsConfig` in `crates/agent-runtime-provider/src/gemini.rs` <!-- id: 4 -->
- [x] Add comprehensive unit tests in `openai.rs`, `anthropic.rs`, `responses.rs`, and `gemini.rs` verifying presets and builder methods <!-- id: 5 -->
- [x] Verify test suite and clippy cleanliness across workspace (`cargo test --package agent-runtime-provider`, `cargo clippy --all-targets --all-features`) <!-- id: 6 -->
