//! Provider adapters and retry helpers for the shared agent runtime.
//!
//! `agent-runtime-provider` owns the reusable **provider mechanism**: the
//! injectable HTTP transport, SSE frame normalization, OpenAI-compatible chunk
//! mapping onto the neutral [`agent_runtime_core::provider`] vocabulary, a
//! deterministic scriptable fake, and the retryability/backoff classifier the
//! agent loop uses to record every attempt.
//!
//! It depends only on [`agent_runtime_core`] and injects all network I/O through
//! [`transport::HttpTransport`], so every adapter is fully offline-testable. It
//! contains **no** cost table or consumer domain type — those stay product
//! policy in the consuming host.
//!
//! - [`fake::FakeProvider`] — a deterministic, scriptable provider.
//! - [`openai::OpenAiProvider`] — a configurable OpenAI-compatible adapter over
//!   an injectable [`transport::HttpTransport`].
//! - [`anthropic::AnthropicProvider`] — a configurable Anthropic Messages API
//!   adapter over the same transport, with multimodal (image) user content.
//! - [`retry`] — retryability classification and backoff used by the agent loop
//!   to record every provider attempt.
//! - [`catalog`] — optional remote model-catalog sources. Resolution reads a
//!   host-owned cache and never the network; refresh is control-plane work.
#![forbid(unsafe_code)]

pub mod anthropic;
pub mod catalog;
pub mod fake;
pub mod openai;
pub mod retry;
pub mod sse;
pub mod transport;

pub use agent_runtime_core as core;
pub use agent_runtime_core::provider_credential::{
    CredentialInvalidation, ProviderAuthRejection, ProviderCredentialError,
    ProviderCredentialLease, ProviderCredentialRecovery, ProviderCredentialRevision,
    ProviderCredentialSource, ProviderCredentialTarget, StaticProviderCredentialSource,
};
