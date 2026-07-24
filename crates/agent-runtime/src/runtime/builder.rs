//! The runtime builder.
//!
//! Collects host-injected services and neutral loop configuration, then
//! produces an immutable, shareable [`Runtime`]. Missing optional services get
//! fail-closed defaults: no approval policy → [`DenyAll`]; no workspace →
//! [`DenyAllWorkspace`]; no observers → none.

use std::sync::Arc;

use agent_runtime_core::approval::{ApprovalPolicy, DenyAll};
use agent_runtime_core::clock::{Clock, SystemClock};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::observer::EventObserver;
use agent_runtime_core::provider::{ModelId, Provider, ReasoningConfig};
use agent_runtime_core::store::{SecretStore, SessionStore};
use agent_runtime_core::tool::Tool;
use agent_runtime_core::workspace::{DenyAllWorkspace, Workspace};

use crate::agent::config::{DowngradePolicy, LoopConfig};
use crate::agent::driver::Driver;
use crate::provider::retry::RetryPolicy;
use crate::runtime::engine::{Runtime, RuntimeShared};
use crate::tool::ToolExecutor;
use crate::tool::registry::ToolRegistry;
use crate::tool::scheduler::ConflictPolicy;

/// Builds a [`Runtime`] from host services and configuration.
#[derive(Debug)]
pub struct RuntimeBuilder {
    provider: Option<Arc<dyn Provider>>,
    tools: Vec<Arc<dyn Tool>>,
    approval: Option<Arc<dyn ApprovalPolicy>>,
    workspace: Option<Arc<dyn Workspace>>,
    session_store: Option<Arc<dyn SessionStore>>,
    secret_store: Option<Arc<dyn SecretStore>>,
    observers: Vec<Arc<dyn EventObserver>>,
    clock: Arc<dyn Clock>,
    config: LoopConfig,
    event_buffer: usize,
    shutdown_timeout_ms: u64,
}

impl RuntimeBuilder {
    /// A builder targeting `model`.
    pub fn new(model: ModelId) -> Self {
        Self {
            provider: None,
            tools: Vec::new(),
            approval: None,
            workspace: None,
            session_store: None,
            secret_store: None,
            observers: Vec::new(),
            clock: Arc::new(SystemClock),
            config: LoopConfig::new(model),
            event_buffer: 1024,
            shutdown_timeout_ms: 5_000,
        }
    }

    /// Sets the provider (required).
    pub fn provider(mut self, provider: Arc<dyn Provider>) -> Self {
        self.provider = Some(provider);
        self
    }

    /// Registers a tool (conflicts are reported at [`RuntimeBuilder::build`]).
    pub fn tool(mut self, tool: Arc<dyn Tool>) -> Self {
        self.tools.push(tool);
        self
    }

    /// Registers many tools.
    pub fn tools(mut self, tools: impl IntoIterator<Item = Arc<dyn Tool>>) -> Self {
        self.tools.extend(tools);
        self
    }

    /// Sets the approval policy.
    pub fn approval(mut self, approval: Arc<dyn ApprovalPolicy>) -> Self {
        self.approval = Some(approval);
        self
    }

    /// Sets the workspace boundary.
    pub fn workspace(mut self, workspace: Arc<dyn Workspace>) -> Self {
        self.workspace = Some(workspace);
        self
    }

    /// Sets the session store.
    pub fn session_store(mut self, store: Arc<dyn SessionStore>) -> Self {
        self.session_store = Some(store);
        self
    }

    /// Sets the secret store.
    pub fn secret_store(mut self, store: Arc<dyn SecretStore>) -> Self {
        self.secret_store = Some(store);
        self
    }

    /// Adds an event observer.
    pub fn observer(mut self, observer: Arc<dyn EventObserver>) -> Self {
        self.observers.push(observer);
        self
    }

    /// Overrides the clock (e.g. a deterministic test clock).
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Sets host-supplied system instructions (neutral: no product prompt).
    pub fn system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.config.system_prompt = Some(prompt.into());
        self
    }

    /// Sets the maximum tool-execution steps in a turn.
    pub fn max_tool_steps(mut self, steps: u32) -> Self {
        self.config.max_tool_steps = steps;
        self
    }

    /// Sets the provider retry policy.
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.config.retry = retry;
        self
    }

    /// Sets an optional wall-clock turn time limit.
    pub fn turn_time_limit_ms(mut self, ms: u64) -> Self {
        self.config.turn_time_limit_ms = Some(ms);
        self
    }

    /// Sets the model-facing tool output limit (characters).
    pub fn output_limit(mut self, limit: usize) -> Self {
        self.config.output_limit = limit;
        self
    }

    /// Sets the reasoning configuration.
    pub fn reasoning(mut self, reasoning: ReasoningConfig) -> Self {
        self.config.reasoning = Some(reasoning);
        self
    }

    /// Sets the tool-write conflict policy.
    pub fn conflict_policy(mut self, policy: ConflictPolicy) -> Self {
        self.config.conflict_policy = policy;
        self
    }

    /// Sets the capability downgrade policy.
    pub fn downgrade_policy(mut self, policy: DowngradePolicy) -> Self {
        self.config.downgrade = policy;
        self
    }

    /// Replaces the entire loop configuration.
    pub fn loop_config(mut self, config: LoopConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the event broadcast buffer capacity.
    pub fn event_buffer(mut self, buffer: usize) -> Self {
        self.event_buffer = buffer;
        self
    }

    /// Sets the bounded-shutdown timeout.
    pub fn shutdown_timeout_ms(mut self, ms: u64) -> Self {
        self.shutdown_timeout_ms = ms;
        self
    }

    /// Builds the runtime, sealing the tool registry and applying fail-closed
    /// defaults for any omitted services.
    pub fn build(self) -> Result<Runtime, RuntimeError> {
        let provider = self
            .provider
            .ok_or_else(|| RuntimeError::config("a provider is required"))?;

        let mut registry = ToolRegistry::new();
        registry.register_all(self.tools)?;
        let registry = registry.seal();

        let approval = self.approval.unwrap_or_else(|| Arc::new(DenyAll));
        let workspace = self.workspace.unwrap_or_else(|| Arc::new(DenyAllWorkspace));

        let config = Arc::new(self.config);
        let executor = ToolExecutor::new(
            registry.clone(),
            approval,
            workspace,
            self.clock.clone(),
            config.output_limit,
            config.conflict_policy,
        );
        let driver = Driver::new(
            provider,
            registry,
            executor,
            self.clock.clone(),
            config.clone(),
        );

        let shared = RuntimeShared {
            driver,
            clock: self.clock,
            session_store: self.session_store,
            secret_store: self.secret_store,
            observers: Arc::from(self.observers.into_boxed_slice()),
            event_buffer: self.event_buffer,
            shutdown_timeout_ms: self.shutdown_timeout_ms,
        };
        Ok(Runtime::from_shared(Arc::new(shared)))
    }
}
