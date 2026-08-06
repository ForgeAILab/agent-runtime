use super::*;
use crate::tool::registry::ToolRegistry;
use agent_runtime_core::approval::{AllowAll, DenyAll, UnavailableApproval};
use agent_runtime_core::check_set::{ActionClass, EnforcementLimits, SecurityCheckSetBuilder};
use agent_runtime_core::clock::SystemClock;
use agent_runtime_core::compat::LegacyApprovalAuthority;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::grant::{
    DecisionCode, GrantConstraints, SecurityCheck, SecurityCheckId, SecurityCheckMode,
    SecurityCheckOutcome, SecurityCheckRevision,
};
use agent_runtime_core::ids::ToolCallId;
use agent_runtime_core::security::{PermissionSet, SecurityResource};
use agent_runtime_core::tool::{LegacyTool, ToolCallDisplay, ToolOutcome};
use agent_runtime_core::workspace::Workspace;
use agent_runtime_registry::Permission;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct EchoTool;
#[async_trait]
impl LegacyTool for EchoTool {
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
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
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
impl LegacyTool for NetworkTool {
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
    async fn invoke_legacy(
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
impl LegacyTool for HangingTool {
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
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
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
impl LegacyTool for WriteTool {
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
        ToolEffects::new(vec![]).with_write("/ws/file")
    }
    async fn invoke_legacy(
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
impl LegacyTool for LargeErrorTool {
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
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
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
impl LegacyTool for TrackingWriteTool {
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
        ToolEffects::new(vec![]).with_write("/ws/tracked")
    }
    async fn invoke_legacy(
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
impl LegacyTool for TrackingNetworkTool {
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
    async fn invoke_legacy(
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
impl LegacyTool for WriteOkTool {
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
        ToolEffects::new(vec![]).with_write("/ws/ok")
    }
    async fn invoke_legacy(
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
impl LegacyTool for WriteForbiddenTool {
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
        ToolEffects::new(vec![]).with_write("/ws/forbidden")
    }
    async fn invoke_legacy(
        &self,
        _arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text("wrote"))
    }
}

#[derive(Debug)]
struct ExactEditTool {
    invoked_paths: Arc<Mutex<Vec<String>>>,
}

impl ExactEditTool {
    fn new(invoked_paths: Arc<Mutex<Vec<String>>>) -> Self {
        Self { invoked_paths }
    }
}

#[async_trait]
impl Tool for ExactEditTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            "exact_edit",
            "edits one exact workspace file",
            json!({
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
                "additionalProperties": false
            }),
            ToolEffects::new(vec![]).with_write("/ws"),
        )
    }

    async fn prepare(
        &self,
        mut arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        let relative = arguments
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| RuntimeError::tool("path is required"))?;
        if relative.starts_with('/')
            || relative
                .split('/')
                .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        {
            return Err(RuntimeError::tool("path must be a canonical relative path"));
        }
        let segments = relative.split('/').map(str::to_owned).collect::<Vec<_>>();
        let canonical = format!(
            "{}/{}",
            ctx.workspace.root().trim_end_matches('/'),
            segments.join("/")
        );
        arguments["path"] = Value::String(canonical.clone());
        Ok(PreparedToolCall::new(
            ctx.call_id.clone(),
            "exact_edit",
            arguments,
            PermissionSet::single(Permission::FsWrite),
            SecurityResource::filesystem(ctx.workspace.root(), segments),
            ToolEffects::new(vec![]).with_write(canonical.clone()),
            ToolCallDisplay::new("Edit workspace file").with_detail(canonical),
        ))
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let path = prepared
            .arguments()
            .get("path")
            .and_then(Value::as_str)
            .expect("prepared edit path")
            .to_owned();
        self.invoked_paths
            .lock()
            .expect("invoked paths poisoned")
            .push(path.clone());
        Ok(ToolOutcome::text(path))
    }
}

#[derive(Debug)]
struct RecordingApprovalCheck {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
    resources: Arc<Mutex<Vec<SecurityResource>>>,
}

#[async_trait]
impl SecurityCheck for RecordingApprovalCheck {
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
        self.resources
            .lock()
            .expect("resources poisoned")
            .push(request.resource.clone());
        SecurityCheckOutcome::RequireApproval {
            constraints: GrantConstraints::unconstrained(),
        }
    }
}

#[derive(Debug)]
struct EditThenAllow {
    calls: AtomicUsize,
    seen: Arc<Mutex<Vec<PreparedToolCall>>>,
    edited_arguments: Value,
}

#[async_trait]
impl ApprovalPolicy for EditThenAllow {
    async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
        self.seen
            .lock()
            .expect("approval observations poisoned")
            .push(request.prepared().clone());
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            ApprovalDecision::Edit {
                arguments: self.edited_arguments.clone(),
            }
        } else {
            ApprovalDecision::Allow
        }
    }
}

#[derive(Debug)]
struct HangingApproval;

#[async_trait]
impl ApprovalPolicy for HangingApproval {
    async fn decide(&self, _request: &ApprovalRequest) -> ApprovalDecision {
        std::future::pending().await
    }
}

#[derive(Debug, Clone, Copy)]
enum InvalidPreparation {
    TamperedFingerprint,
    ExceedsPermissionBound,
    MissingWriteEffect,
    MismatchedWriteResource,
}

#[derive(Debug)]
struct InvalidPreparedTool {
    mode: InvalidPreparation,
    invoked: Arc<AtomicBool>,
}

#[async_trait]
impl Tool for InvalidPreparedTool {
    fn spec(&self) -> ToolSpec {
        let effects = match self.mode {
            InvalidPreparation::TamperedFingerprint => ToolEffects::new(vec![]),
            InvalidPreparation::ExceedsPermissionBound => ToolEffects::read_only(),
            InvalidPreparation::MissingWriteEffect
            | InvalidPreparation::MismatchedWriteResource => {
                ToolEffects::new(vec![]).with_write("/ws")
            }
        };
        ToolSpec::new(
            "invalid_prepared",
            "returns an invalid prepared action",
            json!({"type":"object"}),
            effects,
        )
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        let prepared = match self.mode {
            InvalidPreparation::TamperedFingerprint => PreparedToolCall::new(
                ctx.call_id.clone(),
                "invalid_prepared",
                arguments,
                PermissionSet::new(),
                SecurityResource::other("tool", "invalid_prepared"),
                ToolEffects::new(vec![]),
                ToolCallDisplay::new("Invalid"),
            ),
            InvalidPreparation::ExceedsPermissionBound => PreparedToolCall::new(
                ctx.call_id.clone(),
                "invalid_prepared",
                arguments,
                PermissionSet::single(Permission::NetHttp),
                SecurityResource::network("https://example.test", "GET", Vec::new()),
                ToolEffects::new(vec![]).with_network(),
                ToolCallDisplay::new("Invalid"),
            ),
            InvalidPreparation::MissingWriteEffect => PreparedToolCall::new(
                ctx.call_id.clone(),
                "invalid_prepared",
                arguments,
                PermissionSet::single(Permission::FsWrite),
                SecurityResource::filesystem("/ws", vec!["target".into()]),
                ToolEffects::new(vec![]),
                ToolCallDisplay::new("Invalid"),
            ),
            InvalidPreparation::MismatchedWriteResource => PreparedToolCall::new(
                ctx.call_id.clone(),
                "invalid_prepared",
                arguments,
                PermissionSet::single(Permission::FsWrite),
                SecurityResource::filesystem("/ws", vec!["authorized".into()]),
                ToolEffects::new(vec![]).with_write("/ws/executed"),
                ToolCallDisplay::new("Invalid"),
            ),
        };
        if matches!(self.mode, InvalidPreparation::TamperedFingerprint) {
            let mut serialized = serde_json::to_value(prepared).unwrap();
            serialized["canonical_arguments"] = json!({"tampered": true});
            Ok(serde_json::from_value(serialized).unwrap())
        } else {
            Ok(prepared)
        }
    }

    async fn invoke(
        &self,
        _prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.invoked.store(true, Ordering::SeqCst);
        Ok(ToolOutcome::text("must not run"))
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
    let builder = SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
    SecurityConfig {
        check_set: Arc::new(builder.seal().unwrap()),
        subject: SecuritySubject::new("test-subject"),
        tenant: TenantId::new("test-tenant"),
    }
}

/// Registers only [`LegacyApprovalAuthority`] — reproduces the migration
/// posture: workspace reads pass authoritative policy without HITL, while
/// mutating/spawning/network invocations require approval.
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

fn recording_approval_security(resources: Arc<Mutex<Vec<SecurityResource>>>) -> SecurityConfig {
    let mut builder =
        SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
    builder.register(
        Arc::new(RecordingApprovalCheck {
            id: SecurityCheckId::new("recording-approval"),
            revision: SecurityCheckRevision::new("v1"),
            resources,
        }),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsWrite),
        ActionClass::new("test"),
    );
    SecurityConfig {
        check_set: Arc::new(builder.seal().unwrap()),
        subject: SecuritySubject::new("test-subject"),
        tenant: TenantId::new("test-tenant"),
    }
}

fn exact_edit_registry(invoked_paths: Arc<Mutex<Vec<String>>>) -> SealedToolRegistry {
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(ExactEditTool::new(invoked_paths)))
        .unwrap();
    registry.seal()
}

#[tokio::test]
async fn authority_free_tool_runs_without_approval() {
    let ex = ToolExecutor::new(
        registry(),
        Arc::new(DenyAll), // authority-free work never reaches approval
        Arc::new(WsRoot),
        Arc::new(SystemClock),
        10_000,
        ConflictPolicy::ScopeOverlap,
        empty_security_config(),
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
            .contains("approval declined")
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
            .contains("approval declined")
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

#[tokio::test]
async fn prepared_edit_authorizes_the_exact_canonical_path() {
    let invoked = Arc::new(Mutex::new(Vec::new()));
    let resources = Arc::new(Mutex::new(Vec::new()));
    let executor = ToolExecutor::new(
        exact_edit_registry(invoked.clone()),
        Arc::new(AllowAll),
        Arc::new(WsRoot),
        Arc::new(SystemClock),
        10_000,
        ConflictPolicy::ScopeOverlap,
        recording_approval_security(resources.clone()),
    );

    let output = executor
        .execute(
            &[call("exact_edit", "c1", json!({"path": "src/lib.rs"}))],
            &RequestId::new("r"),
            &SessionId::new("s1"),
            &Cancellation::new(),
            Deadline::never(),
        )
        .await;

    assert!(!output[0].is_error);
    assert_eq!(
        resources.lock().expect("resources poisoned").as_slice(),
        [SecurityResource::filesystem(
            "/ws",
            vec!["src".into(), "lib.rs".into()]
        )]
    );
    assert_eq!(
        invoked.lock().expect("invoked paths poisoned").as_slice(),
        ["/ws/src/lib.rs"]
    );
}

#[tokio::test]
async fn edited_approval_revalidates_reprepares_and_reauthorizes() {
    let invoked = Arc::new(Mutex::new(Vec::new()));
    let resources = Arc::new(Mutex::new(Vec::new()));
    let approvals = Arc::new(Mutex::new(Vec::new()));
    let executor = ToolExecutor::new(
        exact_edit_registry(invoked.clone()),
        Arc::new(EditThenAllow {
            calls: AtomicUsize::new(0),
            seen: approvals.clone(),
            edited_arguments: json!({"path": "src/edited.rs"}),
        }),
        Arc::new(WsRoot),
        Arc::new(SystemClock),
        10_000,
        ConflictPolicy::ScopeOverlap,
        recording_approval_security(resources.clone()),
    );

    let output = executor
        .execute(
            &[call("exact_edit", "c1", json!({"path": "src/original.rs"}))],
            &RequestId::new("r"),
            &SessionId::new("s1"),
            &Cancellation::new(),
            Deadline::never(),
        )
        .await;

    assert!(!output[0].is_error);
    let observed = approvals.lock().expect("approval observations poisoned");
    assert_eq!(observed.len(), 2);
    assert_ne!(observed[0].fingerprint(), observed[1].fingerprint());
    assert_eq!(
        observed[0].resource(),
        &SecurityResource::filesystem("/ws", vec!["src".into(), "original.rs".into()])
    );
    assert_eq!(
        observed[1].resource(),
        &SecurityResource::filesystem("/ws", vec!["src".into(), "edited.rs".into()])
    );
    drop(observed);
    assert_eq!(
        resources.lock().expect("resources poisoned").as_slice(),
        [
            SecurityResource::filesystem("/ws", vec!["src".into(), "original.rs".into()]),
            SecurityResource::filesystem("/ws", vec!["src".into(), "edited.rs".into()])
        ]
    );
    assert_eq!(
        invoked.lock().expect("invoked paths poisoned").as_slice(),
        ["/ws/src/edited.rs"]
    );
}

#[tokio::test]
async fn approval_observes_cancellation_and_deadline() {
    let invoked = Arc::new(Mutex::new(Vec::new()));
    let deadline_executor = ToolExecutor::new(
        exact_edit_registry(invoked.clone()),
        Arc::new(HangingApproval),
        Arc::new(WsRoot),
        Arc::new(SystemClock),
        10_000,
        ConflictPolicy::ScopeOverlap,
        recording_approval_security(Arc::new(Mutex::new(Vec::new()))),
    );
    let deadline_output = tokio::time::timeout(
        Duration::from_secs(2),
        deadline_executor.execute(
            &[call("exact_edit", "deadline", json!({"path": "a.rs"}))],
            &RequestId::new("r-deadline"),
            &SessionId::new("s1"),
            &Cancellation::new(),
            Deadline::after(&SystemClock, 30),
        ),
    )
    .await
    .expect("approval deadline must preempt an unresponsive policy");
    assert!(deadline_output[0].is_error);
    assert!(
        deadline_output[0].content[0]
            .as_text()
            .unwrap()
            .contains("approval timed out")
    );

    let cancel_executor = ToolExecutor::new(
        exact_edit_registry(invoked.clone()),
        Arc::new(HangingApproval),
        Arc::new(WsRoot),
        Arc::new(SystemClock),
        10_000,
        ConflictPolicy::ScopeOverlap,
        recording_approval_security(Arc::new(Mutex::new(Vec::new()))),
    );
    let cancel = Cancellation::new();
    let cancel_call = [call("exact_edit", "cancel", json!({"path": "b.rs"}))];
    let request = RequestId::new("r-cancel");
    let session = SessionId::new("s1");
    let run = cancel_executor.execute(&cancel_call, &request, &session, &cancel, Deadline::never());
    let trigger = async {
        tokio::time::sleep(Duration::from_millis(30)).await;
        cancel.cancel(agent_runtime_core::cancel::CancelReason::UserRequested);
    };
    let (cancel_output, ()) =
        tokio::time::timeout(Duration::from_secs(2), async { tokio::join!(run, trigger) })
            .await
            .expect("turn cancellation must preempt an unresponsive approval policy");
    assert!(cancel_output[0].is_error);
    assert!(
        cancel_output[0].content[0]
            .as_text()
            .unwrap()
            .contains("approval cancelled")
    );
    assert!(
        invoked.lock().expect("invoked paths poisoned").is_empty(),
        "neither timed-out nor cancelled approval may invoke the tool"
    );
}

#[tokio::test]
async fn unavailable_approval_is_distinct_from_explicit_decline() {
    let invoked = Arc::new(Mutex::new(Vec::new()));
    let executor = ToolExecutor::new(
        exact_edit_registry(invoked.clone()),
        Arc::new(UnavailableApproval),
        Arc::new(WsRoot),
        Arc::new(SystemClock),
        10_000,
        ConflictPolicy::ScopeOverlap,
        recording_approval_security(Arc::new(Mutex::new(Vec::new()))),
    );
    let output = executor
        .execute(
            &[call("exact_edit", "c1", json!({"path": "src/lib.rs"}))],
            &RequestId::new("r"),
            &SessionId::new("s1"),
            &Cancellation::new(),
            Deadline::never(),
        )
        .await;
    assert!(output[0].is_error);
    assert!(
        output[0].content[0]
            .as_text()
            .unwrap()
            .contains("approval unavailable")
    );
    assert!(invoked.lock().expect("invoked paths poisoned").is_empty());
}

#[tokio::test]
async fn invalid_prepared_authority_fails_closed_before_invocation() {
    for (mode, expected) in [
        (
            InvalidPreparation::TamperedFingerprint,
            "fingerprint mismatch",
        ),
        (
            InvalidPreparation::ExceedsPermissionBound,
            "exceed the tool descriptor upper bound",
        ),
        (
            InvalidPreparation::MissingWriteEffect,
            "no matching write effect",
        ),
        (
            InvalidPreparation::MismatchedWriteResource,
            "not covered by the authorized resource",
        ),
    ] {
        let invoked = Arc::new(AtomicBool::new(false));
        let mut registry = ToolRegistry::new();
        registry
            .register(Arc::new(InvalidPreparedTool {
                mode,
                invoked: invoked.clone(),
            }))
            .unwrap();
        let executor = ToolExecutor::new(
            registry.seal(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            empty_security_config(),
        );

        let output = executor
            .execute(
                &[call("invalid_prepared", "c1", json!({}))],
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(output[0].is_error, "{mode:?} must fail");
        assert!(
            output[0].content[0].as_text().unwrap().contains(expected),
            "{mode:?} returned {:?}",
            output[0].content
        );
        assert!(
            !invoked.load(Ordering::SeqCst),
            "{mode:?} must fail before invocation"
        );
    }
}

/// Reads one exact file outside the workspace: prepares a host-mounted
/// resource so authorization sees the escape.
#[derive(Debug)]
struct EscapedReadTool;

#[async_trait]
impl Tool for EscapedReadTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            "escaped_read",
            "reads one exact file outside the workspace",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            ToolEffects::read_only(),
        )
        .with_permission_upper_bound(PermissionSet::single(Permission::FsRead))
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        Ok(PreparedToolCall::new(
            ctx.call_id.clone(),
            "escaped_read",
            arguments,
            PermissionSet::single(Permission::FsRead),
            SecurityResource::filesystem("/", vec!["etc".into(), "hosts".into()]),
            ToolEffects::read_only(),
            ToolCallDisplay::new("Read /etc/hosts"),
        ))
    }

    async fn invoke(
        &self,
        _prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::text("escaped content"))
    }
}

/// Allows everything it covers without requiring approval.
#[derive(Debug)]
struct UnattendedAllowCheck {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
}

#[async_trait]
impl SecurityCheck for UnattendedAllowCheck {
    fn id(&self) -> &SecurityCheckId {
        &self.id
    }
    fn revision(&self) -> &SecurityCheckRevision {
        &self.revision
    }
    async fn evaluate(
        &self,
        _request: &AuthorizationRequest,
        _cancel: &Cancellation,
    ) -> SecurityCheckOutcome {
        SecurityCheckOutcome::Allow {
            constraints: GrantConstraints::unconstrained(),
        }
    }
}

#[tokio::test]
async fn an_out_of_workspace_resource_is_never_allowed_unattended() {
    let mut reg = ToolRegistry::new();
    reg.register(Arc::new(EscapedReadTool)).unwrap();

    let mut builder =
        SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
    builder.register(
        Arc::new(UnattendedAllowCheck {
            id: SecurityCheckId::new("allow-everything"),
            revision: SecurityCheckRevision::new("v1"),
        }),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        ActionClass::new("test"),
    );
    let security = SecurityConfig {
        check_set: Arc::new(builder.seal().unwrap()),
        subject: SecuritySubject::new("test-subject"),
        tenant: TenantId::new("test-tenant"),
    };

    let ex = ToolExecutor::new(
        reg.seal(),
        // A permissive approval policy is irrelevant: the check set answered
        // `Allow`, so no approval decision exists to sanction the escape.
        Arc::new(AllowAll),
        Arc::new(WsRoot),
        Arc::new(SystemClock),
        10_000,
        ConflictPolicy::ScopeOverlap,
        security,
    );
    let out = ex
        .execute(
            &[call("escaped_read", "c1", json!({}))],
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
            .contains("approval decision"),
        "an unattended allow must not sanction a workspace escape: {:?}",
        out[0].content
    );
}

#[tokio::test]
async fn an_out_of_workspace_resource_runs_only_through_approval() {
    let mut reg = ToolRegistry::new();
    reg.register(Arc::new(EscapedReadTool)).unwrap();

    let resources = Arc::new(Mutex::new(Vec::new()));
    let mut builder =
        SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
    builder.register(
        Arc::new(RecordingApprovalCheck {
            id: SecurityCheckId::new("recording-approval"),
            revision: SecurityCheckRevision::new("v1"),
            resources: resources.clone(),
        }),
        SecurityCheckMode::Authoritative,
        PermissionSet::single(Permission::FsRead),
        ActionClass::new("test"),
    );
    let security = SecurityConfig {
        check_set: Arc::new(builder.seal().unwrap()),
        subject: SecuritySubject::new("test-subject"),
        tenant: TenantId::new("test-tenant"),
    };

    let ex = ToolExecutor::new(
        reg.seal(),
        Arc::new(AllowAll),
        Arc::new(WsRoot),
        Arc::new(SystemClock),
        10_000,
        ConflictPolicy::ScopeOverlap,
        security,
    );
    let out = ex
        .execute(
            &[call("escaped_read", "c1", json!({}))],
            &RequestId::new("r"),
            &SessionId::new("s1"),
            &Cancellation::new(),
            Deadline::never(),
        )
        .await;

    assert!(!out[0].is_error, "{:?}", out[0].content);
    assert_eq!(out[0].content[0].as_text().unwrap(), "escaped content");
    assert_eq!(
        resources.lock().expect("resources poisoned").as_slice(),
        [SecurityResource::filesystem(
            "/",
            vec!["etc".into(), "hosts".into()]
        )],
        "approval-routed authorization must have seen the escaped resource"
    );
}
