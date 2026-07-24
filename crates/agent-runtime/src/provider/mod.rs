//! Provider adapters and retry helpers.
//!
//! - [`fake::FakeProvider`] — a deterministic, scriptable provider.
//! - [`openai::OpenAiProvider`] — a configurable OpenAI-compatible adapter over
//!   an injectable [`transport::HttpTransport`].
//! - [`retry`] — retryability classification and backoff used by the agent loop
//!   to record every provider attempt.

pub mod fake;
pub mod openai;
pub mod retry;
pub mod sse;
pub mod transport;
