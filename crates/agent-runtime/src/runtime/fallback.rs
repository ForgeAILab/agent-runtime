//! Frozen model bindings that the canonical turn loop may activate in order.

use std::sync::Arc;

use crate::agent::config::LoopConfig;
use crate::agent::planning::RunPlanner;
use agent_runtime_core::provider::Provider;

/// A preflighted provider/model binding captured from a runtime composition.
///
/// Only model-bound services are imported when it is selected. The serving
/// session retains its original tools, executor, authority, hooks, and history.
#[derive(Debug, Clone)]
pub struct ModelRoute {
    pub(crate) provider: Arc<dyn Provider>,
    pub(crate) planner: Arc<RunPlanner>,
    pub(crate) config: Arc<LoopConfig>,
    pub(crate) first_response_timeout_ms: Option<u64>,
}

impl ModelRoute {
    /// The serving provider's configuration identity.
    pub fn provider(&self) -> &str {
        &self.planner.profile().provider
    }

    /// The exact selected model id.
    pub fn model(&self) -> &agent_runtime_core::provider::ModelId {
        &self.config.model
    }
}
