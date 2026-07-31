//! Tool execution: authorization, approval, workspace enforcement,
//! scheduling, and bounds.
//!
//! The executor is the single choke point where side effects happen. Before
//! any tool whose effects [`ToolEffects::requires_authorization`] runs, it
//! must obtain a composed authorization decision from the injected
//! [`SecurityCheckSet`] (security-enforcement's "Central default-deny
//! authorization"); a `RequireApproval` decision must then also obtain an
//! `Allow` from the injected [`ApprovalPolicy`], and every declared write
//! scope must lie inside the [`Workspace`]. A missing approval policy denies
//! by construction (the runtime injects [`agent_runtime_core::approval::DenyAll`]
//! when the host supplies none); a missing authoritative check for a
//! requested permission denies by construction too, since
//! [`SecurityCheckSet`] itself is default-deny. Unknown tools, denials,
//! workspace violations, deadlines, and tool errors all become canonical
//! error [`ToolResultBlock`]s so the model always receives a result for every
//! call it made.

use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::join_all;

use agent_runtime_core::approval::{ApprovalDecision, ApprovalPolicy, ApprovalRequest};
use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::check_set::SecurityCheckSet;
use agent_runtime_core::clock::{Clock, Deadline, SystemClock};
use agent_runtime_core::content::{ToolCall, ToolResultBlock};
use agent_runtime_core::grant::AuthorizationDecision;
use agent_runtime_core::ids::{RequestId, SessionId, TenantId};
use agent_runtime_core::security::{
    AuthorizationRequest, SecurityAction, SecurityContext, SecurityEvidence, SecuritySubject,
};
use agent_runtime_core::tool::{InvocationContext, Tool, ToolEffects};
use agent_runtime_core::workspace::Workspace;
use agent_runtime_registry::{Fingerprint, TrustClass};

use super::registry::SealedToolRegistry;
use super::scheduler::{ConflictPolicy, plan_batches};

/// The composed check set an executor authorizes every non-pure invocation
/// against, plus the identity used to build each invocation's
/// [`SecurityContext`].
///
/// `subject` and `tenant` are shared by every session this executor serves;
/// per-session scoping comes from the [`SessionId`] passed to
/// [`ToolExecutor::execute`] itself. Finer-grained per-session identity is
/// registry-routing work (`tasks.md` 2.3), not something this executor
/// invents on its own.
#[derive(Debug, Clone)]
pub struct SecurityConfig {
    /// The sealed, runtime-owned composer.
    pub check_set: Arc<SecurityCheckSet>,
    /// The security subject every request from this executor is attributed
    /// to.
    pub subject: SecuritySubject,
    /// The tenant every request from this executor is scoped to.
    pub tenant: TenantId,
}

/// Executes tool calls for one turn.
#[derive(Debug, Clone)]
pub struct ToolExecutor {
    registry: SealedToolRegistry,
    approval: Arc<dyn ApprovalPolicy>,
    workspace: Arc<dyn Workspace>,
    clock: Arc<dyn Clock>,
    output_limit: usize,
    conflict_policy: ConflictPolicy,
    security: SecurityConfig,
}

impl ToolExecutor {
    /// Builds an executor from its injected services.
    pub fn new(
        registry: SealedToolRegistry,
        approval: Arc<dyn ApprovalPolicy>,
        workspace: Arc<dyn Workspace>,
        clock: Arc<dyn Clock>,
        output_limit: usize,
        conflict_policy: ConflictPolicy,
        security: SecurityConfig,
    ) -> Self {
        Self {
            registry,
            approval,
            workspace,
            clock,
            output_limit,
            conflict_policy,
            security,
        }
    }

    pub(crate) fn security(&self) -> &SecurityConfig {
        &self.security
    }

    pub(crate) fn approval_policy(&self) -> &Arc<dyn ApprovalPolicy> {
        &self.approval
    }

    /// Executes `calls`, returning one [`ToolResultBlock`] per call in request
    /// order. Overlapping writes are serialized; independent calls in a batch
    /// run concurrently.
    pub async fn execute(
        &self,
        calls: &[ToolCall],
        request: &RequestId,
        session: &SessionId,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Vec<ToolResultBlock> {
        let resolved: Vec<Option<Arc<dyn Tool>>> =
            calls.iter().map(|c| self.registry.get(&c.name)).collect();
        let effects: Vec<ToolEffects> = resolved
            .iter()
            .map(|t| t.as_ref().map(|t| t.effects()).unwrap_or_default())
            .collect();

        let batches = plan_batches(&effects, self.conflict_policy);

        let mut results: Vec<Option<ToolResultBlock>> = vec![None; calls.len()];
        for batch in batches {
            let futures = batch.iter().map(|&i| {
                let call = &calls[i];
                let tool = resolved[i].clone();
                async move {
                    let block = self
                        .run_one(call, tool, request, session, cancel, deadline)
                        .await;
                    (i, block)
                }
            });
            for (i, block) in join_all(futures).await {
                results[i] = Some(block);
            }
        }

        results
            .into_iter()
            .enumerate()
            .map(|(i, block)| {
                block.unwrap_or_else(|| {
                    error_block(&calls[i], "tool was not executed", self.output_limit)
                })
            })
            .collect()
    }

    async fn run_one(
        &self,
        call: &ToolCall,
        tool: Option<Arc<dyn Tool>>,
        request: &RequestId,
        session: &SessionId,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> ToolResultBlock {
        let Some(tool) = tool else {
            return error_block(
                call,
                format!("tool `{}` is not available", call.name),
                self.output_limit,
            );
        };

        if cancel.is_cancelled() {
            return error_block(call, "cancelled before tool ran", self.output_limit);
        }
        if deadline.is_expired(self.clock.as_ref()) {
            return error_block(call, "deadline elapsed before tool ran", self.output_limit);
        }

        let effects = tool.effects();

        // Authorization, then fail-closed approval, then workspace
        // enforcement — all before any side effect.
        if effects.requires_authorization() {
            let (requested, resource) =
                effects.authorization_request(&call.name, self.workspace.root());
            let context = SecurityContext::new(
                self.security.subject.clone(),
                session.clone(),
                self.security.tenant.clone(),
                self.security.check_set.revision().clone(),
            );
            // No content-guard system is wired into the executor yet (a
            // later task), so evidence is a conservative placeholder: the
            // least-trusted non-extension class and a fingerprint of the
            // tool name rather than a real content-guard digest.
            let evidence = SecurityEvidence::new(
                TrustClass::ExternalContent,
                Fingerprint::of(call.name.as_str()),
            );
            let auth_request = AuthorizationRequest::new(
                context,
                SecurityAction::new(format!("tool.{}", call.name)),
                resource,
                requested,
                deadline,
                evidence,
            );
            let outcome = self
                .security
                .check_set
                .authorize(&auth_request, cancel)
                .await;
            match outcome.decision {
                AuthorizationDecision::Deny { code } => {
                    return error_block(
                        call,
                        format!("authorization denied: {code}"),
                        self.output_limit,
                    );
                }
                AuthorizationDecision::RequireApproval { eligible } => {
                    let approval_request = ApprovalRequest {
                        call_id: call.id.clone(),
                        tool: call.name.clone(),
                        arguments: call.arguments.clone(),
                        effects: effects.clone(),
                    };
                    let decision = self.approval.decide(&approval_request).await;
                    if !decision.is_allowed() {
                        let reason = match decision {
                            ApprovalDecision::Deny { reason } => reason,
                            ApprovalDecision::Allow => unreachable!(),
                        };
                        let _ = self.security.check_set.resolve_approval(eligible, false);
                        return error_block(
                            call,
                            format!("approval denied: {reason}"),
                            self.output_limit,
                        );
                    }
                    match self.security.check_set.resolve_approval(eligible, true) {
                        AuthorizationDecision::Allow { grant: _ } => {}
                        _ => unreachable!("resolve_approval(_, true) always returns Allow"),
                    }
                }
                AuthorizationDecision::Allow { grant: _ } => {}
            }
            for scope in effects.write_scopes() {
                if !self.workspace.contains(scope.as_str()) {
                    return error_block(
                        call,
                        format!(
                            "workspace violation: `{}` is outside `{}`",
                            scope.as_str(),
                            self.workspace.root()
                        ),
                        self.output_limit,
                    );
                }
            }
        }

        let ctx = InvocationContext {
            call_id: call.id.clone(),
            request: request.clone(),
            workspace: self.workspace.clone(),
            clock: self.clock.clone(),
            cancel: cancel.child(),
            deadline,
            output_limit: self.output_limit,
        };

        let outcome = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => {
                return error_block(call, "cancelled while tool was running", self.output_limit);
            }
            _ = wait_for_deadline(deadline) => {
                return error_block(
                    call,
                    "deadline elapsed while tool was running",
                    self.output_limit,
                );
            }
            result = tool.invoke(call.arguments.clone(), &ctx) => result,
        };

        match outcome {
            Ok(outcome) => {
                outcome.into_result_block(call.id.clone(), call.name.clone(), self.output_limit)
            }
            Err(err) => {
                let mut block = error_block(call, err.message, self.output_limit);
                block.is_error = true;
                block
            }
        }
    }
}

async fn wait_for_deadline(deadline: Deadline) {
    match deadline.remaining_millis(&SystemClock) {
        Some(0) => {}
        Some(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        None => pending::<()>().await,
    }
}

fn error_block(
    call: &ToolCall,
    message: impl Into<String>,
    output_limit: usize,
) -> ToolResultBlock {
    agent_runtime_core::tool::ToolOutcome::error(message).into_result_block(
        call.id.clone(),
        call.name.clone(),
        output_limit,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::registry::ToolRegistry;
    use agent_runtime_core::approval::{AllowAll, DenyAll};
    use agent_runtime_core::check_set::{ActionClass, EnforcementLimits, SecurityCheckSetBuilder};
    use agent_runtime_core::clock::SystemClock;
    use agent_runtime_core::compat::LegacyApprovalAuthority;
    use agent_runtime_core::error::RuntimeError;
    use agent_runtime_core::grant::{
        DecisionCode, SecurityCheck, SecurityCheckId, SecurityCheckMode, SecurityCheckOutcome,
        SecurityCheckRevision,
    };
    use agent_runtime_core::ids::ToolCallId;
    use agent_runtime_core::security::{PermissionSet, SecurityResource};
    use agent_runtime_core::tool::ToolOutcome;
    use agent_runtime_core::workspace::Workspace;
    use agent_runtime_registry::Permission;
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug)]
    struct EchoTool;
    #[async_trait]
    impl Tool for EchoTool {
        fn name(&self) -> &str {
            "echo"
        }
        fn description(&self) -> &str {
            "echoes"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::read_only()
        }
        async fn invoke(
            &self,
            arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::json(arguments))
        }
    }

    #[derive(Debug)]
    struct NetworkTool;
    #[async_trait]
    impl Tool for NetworkTool {
        fn name(&self) -> &str {
            "network"
        }
        fn description(&self) -> &str {
            "performs network I/O"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![]).with_network()
        }
        async fn invoke(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("fetched"))
        }
    }

    /// Ignores `should_stop()` entirely; only the executor's cancel/deadline
    /// preemption can end it.
    #[derive(Debug)]
    struct HangingTool;
    #[async_trait]
    impl Tool for HangingTool {
        fn name(&self) -> &str {
            "hang"
        }
        fn description(&self) -> &str {
            "never returns"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::read_only()
        }
        async fn invoke(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            unreachable!("preempted before the sleep elapses")
        }
    }

    #[derive(Debug)]
    struct WriteTool;
    #[async_trait]
    impl Tool for WriteTool {
        fn name(&self) -> &str {
            "write"
        }
        fn description(&self) -> &str {
            "writes"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::read_only().with_write("/ws/file")
        }
        async fn invoke(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("wrote"))
        }
    }

    #[derive(Debug)]
    struct LargeErrorTool;

    #[async_trait]
    impl Tool for LargeErrorTool {
        fn name(&self) -> &str {
            "large_error"
        }
        fn description(&self) -> &str {
            "fails with a large diagnostic"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::read_only()
        }
        async fn invoke(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Err(RuntimeError::tool("x".repeat(10_000)))
        }
    }

    #[derive(Debug)]
    struct WsRoot;
    impl Workspace for WsRoot {
        fn root(&self) -> &str {
            "/ws"
        }
        fn contains(&self, path: &str) -> bool {
            path.starts_with("/ws/")
        }
    }

    /// Denies whichever `permission` it is registered to cover; `NotApplicable`
    /// otherwise.
    #[derive(Debug)]
    struct DenyingCheck {
        id: SecurityCheckId,
        revision: SecurityCheckRevision,
        permission: Permission,
    }

    impl DenyingCheck {
        fn new(permission: Permission) -> Arc<Self> {
            Arc::new(Self {
                id: SecurityCheckId::new("denying"),
                revision: SecurityCheckRevision::new("v1"),
                permission,
            })
        }
    }

    #[async_trait]
    impl SecurityCheck for DenyingCheck {
        fn id(&self) -> &SecurityCheckId {
            &self.id
        }
        fn revision(&self) -> &SecurityCheckRevision {
            &self.revision
        }
        async fn evaluate(
            &self,
            request: &AuthorizationRequest,
            _cancel: &Cancellation,
        ) -> SecurityCheckOutcome {
            if request.requested.contains(&self.permission) {
                SecurityCheckOutcome::Deny {
                    code: DecisionCode::other("blocked"),
                }
            } else {
                SecurityCheckOutcome::NotApplicable
            }
        }
    }

    /// Denies an `fs.write` request only when its resource carries
    /// `forbidden_segment`; `NotApplicable` otherwise. Registered as
    /// `RequiredConstraint` so its `Deny` is enforcing without itself
    /// satisfying coverage.
    #[derive(Debug)]
    struct ScopedDenyingCheck {
        id: SecurityCheckId,
        revision: SecurityCheckRevision,
        forbidden_segment: String,
    }

    #[async_trait]
    impl SecurityCheck for ScopedDenyingCheck {
        fn id(&self) -> &SecurityCheckId {
            &self.id
        }
        fn revision(&self) -> &SecurityCheckRevision {
            &self.revision
        }
        async fn evaluate(
            &self,
            request: &AuthorizationRequest,
            _cancel: &Cancellation,
        ) -> SecurityCheckOutcome {
            let denies = match &request.resource {
                SecurityResource::Filesystem { segments, .. } => segments
                    .iter()
                    .any(|segment| segment == &self.forbidden_segment),
                _ => false,
            };
            if denies {
                SecurityCheckOutcome::Deny {
                    code: DecisionCode::other("scoped-deny"),
                }
            } else {
                SecurityCheckOutcome::NotApplicable
            }
        }
    }

    #[derive(Debug)]
    struct TrackingApproval {
        called: Arc<AtomicBool>,
    }
    #[async_trait]
    impl ApprovalPolicy for TrackingApproval {
        async fn decide(&self, _request: &ApprovalRequest) -> ApprovalDecision {
            self.called.store(true, Ordering::SeqCst);
            ApprovalDecision::Allow
        }
    }

    #[derive(Debug)]
    struct TrackingWriteTool {
        invoked: Arc<AtomicBool>,
    }
    #[async_trait]
    impl Tool for TrackingWriteTool {
        fn name(&self) -> &str {
            "tracked_write"
        }
        fn description(&self) -> &str {
            "writes, tracking whether it ran"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::read_only().with_write("/ws/tracked")
        }
        async fn invoke(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            self.invoked.store(true, Ordering::SeqCst);
            Ok(ToolOutcome::text("wrote"))
        }
    }

    #[derive(Debug)]
    struct TrackingNetworkTool {
        invoked: Arc<AtomicBool>,
    }
    #[async_trait]
    impl Tool for TrackingNetworkTool {
        fn name(&self) -> &str {
            "tracked_network"
        }
        fn description(&self) -> &str {
            "performs network I/O, tracking whether it ran"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![]).with_network()
        }
        async fn invoke(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            self.invoked.store(true, Ordering::SeqCst);
            Ok(ToolOutcome::text("fetched"))
        }
    }

    #[derive(Debug)]
    struct WriteOkTool;
    #[async_trait]
    impl Tool for WriteOkTool {
        fn name(&self) -> &str {
            "write_ok"
        }
        fn description(&self) -> &str {
            "writes to an allowed path"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::read_only().with_write("/ws/ok")
        }
        async fn invoke(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("wrote"))
        }
    }

    #[derive(Debug)]
    struct WriteForbiddenTool;
    #[async_trait]
    impl Tool for WriteForbiddenTool {
        fn name(&self) -> &str {
            "write_forbidden"
        }
        fn description(&self) -> &str {
            "writes to a forbidden path"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::read_only().with_write("/ws/forbidden")
        }
        async fn invoke(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("wrote"))
        }
    }

    fn registry() -> SealedToolRegistry {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(EchoTool)).unwrap();
        reg.register(Arc::new(WriteTool)).unwrap();
        reg.register(Arc::new(LargeErrorTool)).unwrap();
        reg.register(Arc::new(NetworkTool)).unwrap();
        reg.register(Arc::new(HangingTool)).unwrap();
        reg.seal()
    }

    fn call(name: &str, id: &str, args: Value) -> ToolCall {
        ToolCall {
            id: ToolCallId::new(id),
            name: name.into(),
            arguments: args,
        }
    }

    /// No checks registered at all — used to prove a read-only tool's
    /// invocation never reaches authorization in the first place, not merely
    /// that some particular check happens to allow it.
    fn empty_security_config() -> SecurityConfig {
        let builder =
            SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
        SecurityConfig {
            check_set: Arc::new(builder.seal().unwrap()),
            subject: SecuritySubject::new("test-subject"),
            tenant: TenantId::new("test-tenant"),
        }
    }

    /// Registers only [`LegacyApprovalAuthority`] — reproduces the migration
    /// posture: mutating/spawning/network invocations require approval, reads
    /// need no coverage at all.
    fn security_config() -> SecurityConfig {
        let mut builder =
            SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
        let compat = Arc::new(LegacyApprovalAuthority::new());
        builder.register(
            compat.clone(),
            SecurityCheckMode::Authoritative,
            compat.coverage().clone(),
            ActionClass::new("test"),
        );
        SecurityConfig {
            check_set: Arc::new(builder.seal().unwrap()),
            subject: SecuritySubject::new("test-subject"),
            tenant: TenantId::new("test-tenant"),
        }
    }

    /// Registers one [`DenyingCheck`] covering `permission`, authoritatively.
    fn denying_security_config(permission: Permission) -> SecurityConfig {
        let mut builder =
            SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
        builder.register(
            DenyingCheck::new(permission.clone()),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(permission),
            ActionClass::new("test"),
        );
        SecurityConfig {
            check_set: Arc::new(builder.seal().unwrap()),
            subject: SecuritySubject::new("test-subject"),
            tenant: TenantId::new("test-tenant"),
        }
    }

    #[tokio::test]
    async fn read_only_tool_runs_without_approval() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(DenyAll), // even with deny-all, read-only needs no approval
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            empty_security_config(), // no check covers anything — reads still need none
        );
        let calls = vec![call("echo", "c1", json!({"x":1}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert_eq!(out.len(), 1);
        assert!(!out[0].is_error);
    }

    #[tokio::test]
    async fn mutating_tool_is_denied_fail_closed() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(DenyAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("write", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("approval denied")
        );
    }

    #[tokio::test]
    async fn mutating_tool_runs_when_allowed_and_in_workspace() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("write", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(!out[0].is_error);
    }

    #[tokio::test]
    async fn unknown_tool_becomes_error_result() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("missing", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("not available")
        );
    }

    #[tokio::test]
    async fn tool_runtime_errors_are_output_bounded() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            20,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("large_error", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        let text = out[0].content[0].as_text().unwrap();
        assert!(out[0].is_error);
        assert_eq!(text.chars().count(), 20);
    }

    #[tokio::test]
    async fn network_only_tool_requires_approval() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(DenyAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("network", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("approval denied")
        );
    }

    #[tokio::test]
    async fn hanging_tool_that_ignores_should_stop_is_terminated_at_deadline() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("hang", "c1", json!({}))];
        let out = tokio::time::timeout(
            Duration::from_millis(2_000),
            ex.execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::after(&SystemClock, 30),
            ),
        )
        .await
        .expect("deadline must preempt a tool that ignores should_stop()");
        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("deadline elapsed")
        );
    }

    #[tokio::test]
    async fn hanging_tool_that_ignores_cancellation_is_terminated_on_cancel() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("hang", "c1", json!({}))];
        let cancel = Cancellation::new();
        let request = RequestId::new("r");
        let session = SessionId::new("s1");
        let run = ex.execute(&calls, &request, &session, &cancel, Deadline::never());
        let trigger = async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cancel.cancel(agent_runtime_core::cancel::CancelReason::UserRequested);
        };
        let (out, ()) = tokio::time::timeout(Duration::from_millis(2_000), async {
            tokio::join!(run, trigger)
        })
        .await
        .expect("cancellation must preempt a tool that ignores should_stop()");
        assert!(out[0].is_error);
        assert!(out[0].content[0].as_text().unwrap().contains("cancelled"));
    }

    #[tokio::test]
    async fn authorization_denial_short_circuits_before_approval_and_the_tool_body() {
        let approval_called = Arc::new(AtomicBool::new(false));
        let invoked = Arc::new(AtomicBool::new(false));
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(TrackingWriteTool {
            invoked: invoked.clone(),
        }))
        .unwrap();

        let ex = ToolExecutor::new(
            reg.seal(),
            Arc::new(TrackingApproval {
                called: approval_called.clone(),
            }),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            denying_security_config(Permission::FsWrite),
        );
        let calls = vec![call("tracked_write", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;

        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("authorization denied")
        );
        assert!(
            !approval_called.load(Ordering::SeqCst),
            "approval must not be consulted after an authorization denial"
        );
        assert!(
            !invoked.load(Ordering::SeqCst),
            "the tool body must not run after an authorization denial"
        );
    }

    #[tokio::test]
    async fn authorization_runs_before_the_tool_body_for_a_network_only_tool() {
        let invoked = Arc::new(AtomicBool::new(false));
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(TrackingNetworkTool {
            invoked: invoked.clone(),
        }))
        .unwrap();

        let ex = ToolExecutor::new(
            reg.seal(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            denying_security_config(Permission::NetHttp),
        );
        let calls = vec![call("tracked_network", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;

        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("authorization denied")
        );
        assert!(
            !invoked.load(Ordering::SeqCst),
            "the tool body must not run when authorization denies a network-only tool"
        );
    }

    #[tokio::test]
    async fn approval_cannot_widen_authorization_beyond_the_composed_grant() {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(WriteOkTool)).unwrap();
        reg.register(Arc::new(WriteForbiddenTool)).unwrap();

        let mut builder =
            SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
        let compat = Arc::new(LegacyApprovalAuthority::new());
        builder.register(
            compat.clone(),
            SecurityCheckMode::Authoritative,
            compat.coverage().clone(),
            ActionClass::new("test"),
        );
        builder.register(
            Arc::new(ScopedDenyingCheck {
                id: SecurityCheckId::new("scoped-deny"),
                revision: SecurityCheckRevision::new("v1"),
                forbidden_segment: "forbidden".to_owned(),
            }),
            SecurityCheckMode::RequiredConstraint,
            PermissionSet::single(Permission::FsWrite),
            ActionClass::new("test"),
        );
        let security = SecurityConfig {
            check_set: Arc::new(builder.seal().unwrap()),
            subject: SecuritySubject::new("test-subject"),
            tenant: TenantId::new("test-tenant"),
        };

        let ex = ToolExecutor::new(
            reg.seal(),
            // Would allow anything it is asked about — proving the denial
            // below is not something a permissive approval could rescue.
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security,
        );
        let calls = vec![
            call("write_ok", "c1", json!({})),
            call("write_forbidden", "c2", json!({})),
        ];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;

        assert!(
            !out[0].is_error,
            "the in-scope write must still succeed via approval"
        );
        assert!(out[1].is_error);
        assert!(
            out[1].content[0]
                .as_text()
                .unwrap()
                .contains("authorization denied"),
            "an unlimited approval policy must not widen authorization past the composed deny"
        );
    }
}
