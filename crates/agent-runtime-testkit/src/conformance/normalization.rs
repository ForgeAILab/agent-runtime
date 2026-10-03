//! Host-neutral normalization, approval-edit, and strict-schema consumer gates.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agent_runtime::core::check_set::ActionClass;
use agent_runtime::core::grant::{GrantConstraints, SecurityCheckOutcome};
use agent_runtime::core::security::{AuthorizationRequest, PermissionSet, SecurityResource};
use agent_runtime::prelude::*;
use agent_runtime::registry::Permission;
use async_trait::async_trait;
use serde_json::{Value, json};

use crate::{MemoryWorkspace, RecordingObserver, scenarios};

fn canonical_schema() -> Value {
    json!({"type":"object", "properties":{"path":{"type":"string"}}, "required":["path"], "additionalProperties":false})
}

#[derive(Debug, Default)]
struct EnvelopeWrite {
    normalized: AtomicUsize,
    prepared: AtomicUsize,
    invoked: Mutex<Vec<PreparedToolCall>>,
}

#[async_trait]
impl Tool for EnvelopeWrite {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            "canonical_write",
            "strict canonical writer",
            canonical_schema(),
            ToolEffects::new(vec![]).with_write("/ws"),
        )
    }
    fn normalize_arguments(&self, mut arguments: Value) -> Result<Value, RuntimeError> {
        self.normalized.fetch_add(1, Ordering::SeqCst);
        Ok(arguments
            .as_object_mut()
            .and_then(|object| object.remove("parameters"))
            .unwrap_or(arguments))
    }
    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        let path = arguments["path"]
            .as_str()
            .expect("validated canonical path");
        let absolute = format!("{}/{}", ctx.workspace.root(), path);
        Ok(PreparedToolCall::new(
            ctx.call_id.clone(),
            "canonical_write",
            arguments.clone(),
            PermissionSet::single(Permission::FsWrite),
            SecurityResource::filesystem(
                ctx.workspace.root(),
                path.split('/').map(str::to_owned).collect(),
            ),
            ToolEffects::new(vec![]).with_write(absolute),
            ToolCallDisplay::new("Write exact fixture file"),
        ))
    }
    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.invoked.lock().unwrap().push(prepared.clone());
        Ok(ToolOutcome::text("written"))
    }
}

#[derive(Debug)]
struct RecordingAuthority {
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
    seen: Mutex<Vec<AuthorizationRequest>>,
}
#[async_trait]
impl SecurityCheck for RecordingAuthority {
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
        self.seen.lock().unwrap().push(request.clone());
        SecurityCheckOutcome::RequireApproval {
            constraints: GrantConstraints::unconstrained(),
        }
    }
}

#[derive(Debug, Default)]
struct EditOnce {
    seen: Mutex<Vec<PreparedToolCall>>,
}
#[async_trait]
impl ApprovalPolicy for EditOnce {
    async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
        let mut seen = self.seen.lock().unwrap();
        seen.push(request.prepared().clone());
        if seen.len() == 1 {
            ApprovalDecision::Edit {
                arguments: json!({"parameters":{"path":"edited.txt"}}),
            }
        } else {
            ApprovalDecision::Allow
        }
    }
}

/// Proves an approval edit is normalized, validated, prepared, and authorized
/// again, while model history and advertisements retain their original bytes.
pub async fn assert_approval_edits_are_normalized_and_reauthorized() {
    let tool = Arc::new(EnvelopeWrite::default());
    let approval = Arc::new(EditOnce::default());
    let authority = Arc::new(RecordingAuthority {
        id: SecurityCheckId::new("fixture.authority"),
        revision: SecurityCheckRevision::new("1"),
        seen: Mutex::new(Vec::new()),
    });
    let raw = json!({"parameters":{"path":"original.txt"}});
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "canonical_write",
        &raw,
        "done",
    ));
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider.clone())
        .tool(tool.clone())
        .workspace(Arc::new(MemoryWorkspace::new("/ws")))
        .security_check(
            authority.clone(),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::FsWrite),
            ActionClass::new("fixture"),
        )
        .approval(approval.clone())
        .observer(observer.clone())
        .build()
        .unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("write")).await.unwrap();
    assert_eq!(tool.normalized.load(Ordering::SeqCst), 2);
    assert_eq!(tool.prepared.load(Ordering::SeqCst), 2);
    let approvals = approval.seen.lock().unwrap();
    assert_eq!(approvals.len(), 2);
    assert_ne!(approvals[0].fingerprint(), approvals[1].fingerprint());
    assert_eq!(approvals[0].arguments(), &json!({"path":"original.txt"}));
    assert_eq!(approvals[1].arguments(), &json!({"path":"edited.txt"}));
    let checked = authority.seen.lock().unwrap();
    assert_eq!(checked.len(), 2);
    for (authorization, prepared) in checked.iter().zip(approvals.iter()) {
        assert_eq!(&authorization.resource, prepared.resource());
        assert_eq!(
            &authorization.evidence.content_guard_digest,
            prepared.fingerprint()
        );
    }
    let invoked = tool.invoked.lock().unwrap();
    assert_eq!(invoked.len(), 1);
    assert_eq!(invoked[0].fingerprint(), approvals[1].fingerprint());
    assert!(
        session
            .history()
            .iter()
            .flat_map(|message| &message.content)
            .any(|part| matches!(part, ContentPart::ToolCall(call) if call.arguments == raw))
    );
    assert_eq!(
        provider.requests()[0].tools[0].input_schema,
        canonical_schema()
    );
    let serialized = serde_json::to_string(&observer.events()).unwrap();
    for content in ["original.txt", "edited.txt"] {
        assert!(!serialized.contains(content));
    }
    super::event_schema::assert_versioned_and_roundtrips(&observer.events());
}

/// Proves explicit normalization does not bypass workspace or policy denial
/// and does not widen the strict canonical schema advertised to the provider.
pub async fn assert_opt_in_normalization_preserves_denial_and_schema() {
    for path in ["allowed.txt", "../escape.txt"] {
        let tool = Arc::new(EnvelopeWrite::default());
        let provider = Arc::new(scenarios::fake_tool_then_text(
            "canonical_write",
            &json!({"parameters":{"path":path}}),
            "done",
        ));
        let observer = RecordingObserver::shared();
        let runtime = RuntimeBuilder::new(ModelId::new("fake"))
            .model_profile(scenarios::fake_model_profile())
            .provider(provider.clone())
            .tool(tool.clone())
            .workspace(Arc::new(MemoryWorkspace::new("/ws")))
            .legacy_approval_authority()
            .approval(Arc::new(DenyAll))
            .observer(observer.clone())
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        session.run(UserInput::text("write")).await.unwrap();
        assert_eq!(tool.normalized.load(Ordering::SeqCst), 1);
        assert_eq!(tool.prepared.load(Ordering::SeqCst), 1);
        assert!(tool.invoked.lock().unwrap().is_empty());
        assert_eq!(
            provider.requests()[0].tools[0].input_schema.to_string(),
            canonical_schema().to_string()
        );
        let history = session.history();
        let result = history
            .iter()
            .flat_map(|message| &message.content)
            .find_map(|part| match part {
                ContentPart::ToolResult(result) => Some(result),
                _ => None,
            })
            .unwrap();
        assert!(result.is_error);
        let text = result.content[0].as_text().unwrap();
        assert!(text.contains(if path == "allowed.txt" {
            "approval declined"
        } else {
            "workspace violation"
        }));
        super::event_schema::assert_versioned_and_roundtrips(&observer.events());
    }
}

#[derive(Debug, Default)]
struct LegacyCanonical(AtomicUsize);
#[async_trait]
impl LegacyTool for LegacyCanonical {
    fn name(&self) -> &str {
        "legacy_canonical"
    }
    fn description(&self) -> &str {
        "legacy strict canonical tool"
    }
    fn input_schema(&self) -> Value {
        canonical_schema()
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::new(vec![])
    }
    async fn invoke_legacy(
        &self,
        _arguments: Value,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::text("invoked"))
    }
}

/// Proves legacy identity normalization rejects wrapped arguments while a
/// seeded ephemeral session retains subscribe-before-send event delivery.
pub async fn assert_legacy_identity_with_seeded_history_and_subscription() {
    use futures_util::StreamExt;
    let tool = Arc::new(LegacyCanonical::default());
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "legacy_canonical",
        &json!({"parameters":{"path":"out.txt"}}),
        "done",
    ));
    let observer = RecordingObserver::shared();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .model_profile(scenarios::fake_model_profile())
        .provider(provider.clone())
        .tool(tool.clone())
        .observer(observer.clone())
        .build()
        .unwrap();
    let seeded = vec![
        Message::user("seed question"),
        Message::text(Role::Assistant, "seed answer"),
    ];
    let session = runtime
        .start_session(StartSession::new().with_history(seeded.clone()))
        .await
        .unwrap();
    let mut subscription = session.subscribe();
    let turn = session.send(UserInput::text("write")).unwrap();
    turn.completed().await;
    let mut events = Vec::new();
    while let Some(envelope) = subscription.next().await {
        let terminal = matches!(envelope.payload, RuntimeEvent::TurnCompleted { .. });
        events.push(envelope);
        if terminal {
            break;
        }
    }
    assert!(
        events
            .iter()
            .any(|e| matches!(e.payload, RuntimeEvent::TurnStarted))
    );
    assert!(matches!(
        events.last().unwrap().payload,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            ..
        }
    ));
    assert_eq!(&session.history()[..seeded.len()], &seeded);
    assert_eq!(tool.0.load(Ordering::SeqCst), 0);
    assert!(
        session
            .history()
            .iter()
            .flat_map(|m| &m.content)
            .any(|part| matches!(part, ContentPart::ToolResult(result) if result.is_error))
    );
    assert_eq!(
        provider.requests()[0].tools[0].input_schema,
        canonical_schema()
    );
    super::event_schema::assert_versioned_and_roundtrips(&events);
}
