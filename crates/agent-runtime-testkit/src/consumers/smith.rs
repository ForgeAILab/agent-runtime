//! Smith fixture: a thin terminal host.
//!
//! Represents Smith reduced to one terminal-host package: an interactive
//! approval that a terminal user grants, a single workspace, and its own
//! instruction text. Neutral types only.

use std::sync::Arc;

use agent_runtime::prelude::*;
use agent_runtime_core::error::RuntimeError;

use crate::RecordingObserver;
use crate::tools::EchoTool;
use crate::workspace::MemoryWorkspace;

/// The instruction text a terminal host supplies (product policy stays here).
pub const INSTRUCTIONS: &str = "You are a terminal coding assistant. Be concise.";

/// Builds a runtime configured as the Smith terminal host would.
pub fn build(
    provider: Arc<dyn Provider>,
    observer: Arc<RecordingObserver>,
) -> Result<Runtime, RuntimeError> {
    RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(crate::scenarios::fake_model_profile())
        .provider(provider)
        .system_prompt(INSTRUCTIONS)
        .approval(Arc::new(AllowAll)) // the terminal user approves interactively
        .workspace(Arc::new(MemoryWorkspace::new("/repo")))
        .tool(Arc::new(EchoTool))
        .observer(observer)
        .clock(Arc::new(SystemClock))
        .build()
}
