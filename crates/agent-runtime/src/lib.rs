//! The embeddable agent runtime.
//!
//! `agent-runtime` composes the host-neutral contracts from
//! [`agent_runtime_core`] into a working runtime: provider adapters, the direct
//! provider/tool loop, the tool registry and executor, and the embeddable
//! session facade. It owns reusable mechanism only; product policy (prompts,
//! configuration, presentation, persistence) stays in the consuming host.
//!
//! # Quick start
//!
//! ```
//! use std::sync::Arc;
//! use agent_runtime::core::prelude::*;
//! use agent_runtime::provider::fake::FakeProvider;
//! use agent_runtime::runtime::{RuntimeBuilder, StartSession};
//!
//! # async fn run() -> Result<(), RuntimeError> {
//! let runtime = RuntimeBuilder::new(ModelId::new("fake"))
//!     .provider(Arc::new(FakeProvider::text_reply("hello")))
//!     .build()?;
//!
//! let session = runtime.start_session(StartSession::new()).await?;
//! session.run(UserInput::text("hi")).await;
//! assert!(session.history().iter().any(|m| m.joined_text().contains("hello")));
//! # Ok(())
//! # }
//! ```
#![forbid(unsafe_code)]

pub mod agent;
pub mod ids;
pub mod provider;
pub mod runtime;
pub mod tool;

pub use agent_runtime_core as core;

/// The most commonly used runtime items.
pub mod prelude {
    pub use crate::agent::config::{DowngradePolicy, LoopConfig};
    pub use crate::provider::fake::FakeProvider;
    pub use crate::provider::openai::{OpenAiConfig, OpenAiProvider};
    pub use crate::provider::retry::RetryPolicy;
    pub use crate::provider::transport::{ByteStream, HttpRequest, HttpTransport};
    pub use crate::runtime::{
        Runtime, RuntimeBuilder, RuntimeEventStream, SessionHandle, StartSession,
    };
    pub use crate::tool::scheduler::ConflictPolicy;
    pub use crate::tool::{SealedToolRegistry, ToolExecutor, ToolRegistry};
    pub use agent_runtime_core::prelude::*;
}
