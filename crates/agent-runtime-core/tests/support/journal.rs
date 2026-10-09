//! Object assembler/read-only transport for reader fixtures. It has no native
//! writer, durability, migration or collector implementation.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use agent_runtime_core::checkpoint::*;
use agent_runtime_core::clock::{Deadline, Timestamp};
use agent_runtime_core::content::*;
use agent_runtime_core::error::{RuntimeError, UnsupportedCapability};
use agent_runtime_core::event::*;
use agent_runtime_core::ids::*;
use agent_runtime_core::interaction::*;
use agent_runtime_core::journal::*;
use agent_runtime_core::provider::*;
use agent_runtime_core::security::{PermissionSet, SecurityResource};
use agent_runtime_core::store::*;
use agent_runtime_core::tool::{PreparedToolCall, ToolCallDisplay, ToolEffects, ToolOutcome};
use agent_runtime_core::usage::*;
use agent_runtime_registry::{Fingerprint, RegistryRevision};
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;

pub(super) const LIMITS: JournalReadLimits = JournalReadLimits {
    objects: 100_000,
    encoded_bytes: 128_000_000,
    entries: 10_000,
};

#[derive(Debug)]
pub(super) struct ReadFixture {
    pub(super) objects: BTreeMap<JournalObjectId, Vec<u8>>,
    pub(super) head: JournalHead,
    pub(super) reads: AtomicUsize,
}

impl ReadFixture {
    pub(super) fn checkpoint(checkpoint: &TurnCheckpoint) -> Self {
        let mut objects = Assembler::default();
        let roots = objects.snapshot(&checkpoint.snapshot);
        let state = objects.state(&checkpoint.state);
        let refs = ReferencedTurnCheckpoint {
            schema_version: 4,
            transition_revision: 4,
            session: checkpoint.session.clone(),
            turn: checkpoint.turn.clone(),
            state_revision: checkpoint.state_revision,
            operation_fingerprint: checkpoint.operation_fingerprint.clone(),
            active_history_start: checkpoint.active_history_start,
            internal_input: checkpoint
                .internal_input
                .as_ref()
                .map(|input| objects.add(JournalObjectKind::InternalInput, input)),
            visible_output: checkpoint.visible_output,
            state,
            snapshot: roots.clone(),
            deadline: checkpoint.deadline,
            watermark: checkpoint.watermark.clone(),
            updated: checkpoint.updated,
        };
        let checkpoint_id = objects.add(JournalObjectKind::Checkpoint, &refs);
        let batch = JournalBatch {
            session: checkpoint.session.clone(),
            sequence: 1,
            predecessor: None,
            expected_revision: None,
            writer_epoch: 1,
            operation: JournalOperationId::new("read-fixture"),
            boundary: JournalBoundary {
                version: 1,
                session: checkpoint.session.clone(),
                turn: Some(checkpoint.turn.clone()),
                checkpoint_sequence: checkpoint.watermark.checkpoint_sequence,
                planned_step_count: roots.planned_step_count,
            },
            snapshot: roots.clone(),
            checkpoint: Some(checkpoint_id.clone()),
        };
        let batch_id = objects.add(JournalObjectKind::Batch, &batch);
        let head = JournalHead {
            schema_version: 4,
            storage_id: JournalStorageId::new("protected-fixture"),
            role: JournalRole::Protected,
            session: checkpoint.session.clone(),
            revision: JournalRevision(1),
            writer_epoch: 1,
            batch_sequence: 1,
            batch: batch_id,
            snapshot: roots,
            checkpoint: Some(checkpoint_id),
            superseded_by: None,
            retirement: None,
            created: Timestamp::ZERO,
            updated: checkpoint.updated,
        };
        Self {
            objects: objects.objects,
            head,
            reads: AtomicUsize::new(0),
        }
    }

    pub(super) fn snapshot(snapshot: &SessionSnapshot) -> Self {
        // Assemble only this ordinary view: no protected state is available in
        // its object namespace, even if a caller knows an exact object's digest.
        let mut objects = Assembler::default();
        let roots = objects.snapshot(snapshot);
        let batch = JournalBatch {
            session: snapshot.id.clone(),
            sequence: 1,
            predecessor: None,
            expected_revision: None,
            writer_epoch: 1,
            operation: JournalOperationId::new("read-fixture"),
            boundary: JournalBoundary {
                version: 1,
                session: snapshot.id.clone(),
                turn: None,
                checkpoint_sequence: 0,
                planned_step_count: roots.planned_step_count,
            },
            snapshot: roots.clone(),
            checkpoint: None,
        };
        let batch_id = objects.add(JournalObjectKind::Batch, &batch);
        let head = JournalHead {
            schema_version: 4,
            storage_id: JournalStorageId::new("ordinary-fixture"),
            role: JournalRole::Ordinary,
            session: snapshot.id.clone(),
            revision: JournalRevision(1),
            writer_epoch: 1,
            batch_sequence: 1,
            batch: batch_id,
            snapshot: roots,
            checkpoint: None,
            superseded_by: None,
            retirement: None,
            created: Timestamp::ZERO,
            updated: snapshot.updated,
        };
        Self {
            objects: objects.objects,
            head,
            reads: AtomicUsize::new(0),
        }
    }

    pub(super) fn object(&self, id: &JournalObjectId) -> JournalObject {
        JournalObject {
            id: id.clone(),
            bytes: self.objects.get(id).unwrap().clone(),
        }
    }

    pub(super) fn replace<T: Serialize>(
        &mut self,
        kind: JournalObjectKind,
        value: &T,
    ) -> JournalObjectId {
        let object = JournalObject::encode(kind, value).unwrap();
        self.objects.insert(object.id.clone(), object.bytes);
        object.id
    }

    pub(super) fn repair_batch_refs(&mut self) {
        let mut batch: JournalBatch = self
            .object(&self.head.batch)
            .decode(JournalObjectKind::Batch)
            .unwrap();
        batch.snapshot = self.head.snapshot.clone();
        batch.checkpoint = self.head.checkpoint.clone();
        self.head.batch = self.replace(JournalObjectKind::Batch, &batch);
    }

    pub(super) fn lease(&self) -> JournalLease {
        JournalLease::new(
            self.head.storage_id.clone(),
            self.head.session.clone(),
            Some(self.head.clone()),
            None,
            Arc::new(42u64),
        )
    }
}

#[async_trait]
impl SessionJournal for ReadFixture {
    fn storage_id(&self) -> JournalStorageId {
        self.head.storage_id.clone()
    }
    async fn open(&self, _: &SessionId, _: JournalOpen) -> Result<JournalLease, RuntimeError> {
        Err(RuntimeError::unsupported_capability(
            UnsupportedCapability::SessionJournal,
        ))
    }
    async fn read(
        &self,
        lease: &JournalLease,
        id: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        assert_eq!(lease.storage_id(), &self.head.storage_id);
        assert_eq!(lease.session(), &self.head.session);
        assert_eq!(lease.handle::<u64>(), Some(&42));
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.objects
            .get(id)
            .cloned()
            .ok_or_else(|| RuntimeError::not_found("missing fixture object"))
    }
    async fn commit(
        &self,
        _: &JournalLease,
        _: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        panic!("reader attempted native write");
    }
    async fn renew(&self, _: &JournalLease) -> Result<JournalLease, RuntimeError> {
        panic!("reader attempted renewal");
    }
    async fn close(&self, _: JournalLease) -> Result<(), RuntimeError> {
        panic!("reader attempted close");
    }
    async fn retire(
        &self,
        _: &SessionId,
        _: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        panic!("reader attempted retirement");
    }
    async fn collect(&self, _: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        panic!("reader attempted collection");
    }
}

#[derive(Default)]
struct Assembler {
    objects: BTreeMap<JournalObjectId, Vec<u8>>,
}
impl Assembler {
    fn add<T: Serialize>(&mut self, kind: JournalObjectKind, value: &T) -> JournalObjectId {
        let object = JournalObject::encode(kind, value).unwrap();
        self.objects.insert(object.id.clone(), object.bytes);
        object.id
    }
    fn sequence(&mut self, items: Vec<JournalObjectId>) -> JournalSequence {
        if items.is_empty() {
            return JournalSequence::default();
        }
        let mut trees: Vec<_> = items
            .chunks(JOURNAL_FANOUT)
            .map(|items| JournalSequence {
                root: Some(self.add(
                    JournalObjectKind::Sequence,
                    &JournalSequenceNode::Leaf {
                        items: items.to_vec(),
                    },
                )),
                len: items.len() as u64,
                height: 0,
            })
            .collect();
        while trees.len() > 1 {
            trees = trees
                .chunks(JOURNAL_FANOUT)
                .map(|children| JournalSequence {
                    root: Some(self.add(
                        JournalObjectKind::Sequence,
                        &JournalSequenceNode::Branch {
                            children: children.to_vec(),
                        },
                    )),
                    len: children.iter().map(|tree| tree.len).sum(),
                    height: children[0].height + 1,
                })
                .collect();
        }
        trees.pop().unwrap()
    }
    fn map(&mut self, entries: Vec<JournalMapEntry>) -> JournalMap {
        if entries.is_empty() {
            return JournalMap::default();
        }
        let mut trees: Vec<_> = entries
            .chunks(JOURNAL_FANOUT)
            .map(|entries| JournalMapChild {
                first_key: entries[0].key.clone(),
                tree: JournalMap {
                    root: Some(self.add(
                        JournalObjectKind::Map,
                        &JournalMapNode::Leaf {
                            entries: entries.to_vec(),
                        },
                    )),
                    len: entries.len() as u64,
                    height: 0,
                },
            })
            .collect();
        while trees.len() > 1 {
            trees = trees
                .chunks(JOURNAL_FANOUT)
                .map(|children| JournalMapChild {
                    first_key: children[0].first_key.clone(),
                    tree: JournalMap {
                        root: Some(self.add(
                            JournalObjectKind::Map,
                            &JournalMapNode::Branch {
                                children: children.to_vec(),
                            },
                        )),
                        len: children.iter().map(|child| child.tree.len).sum(),
                        height: children[0].tree.height + 1,
                    },
                })
                .collect();
        }
        trees.pop().unwrap().tree
    }
    fn snapshot(&mut self, snapshot: &SessionSnapshot) -> JournalSnapshotRoots {
        let ids: Vec<_> = snapshot
            .history
            .iter()
            .map(|message| self.add(JournalObjectKind::Message, message))
            .collect();
        let history_chain = journal_history_chain(&ids);
        let history = self.sequence(ids);
        let ids = snapshot
            .usage
            .records()
            .iter()
            .map(|record| self.add(JournalObjectKind::UsageRecord, record))
            .collect();
        let usage = self.sequence(ids);
        let ids = snapshot
            .manifests
            .iter()
            .map(|manifest| self.add(JournalObjectKind::Manifest, manifest))
            .collect();
        let manifests = self.sequence(ids);
        let entries = snapshot
            .extension_state
            .iter()
            .map(|(key, value)| JournalMapEntry {
                key: key.clone(),
                value: self.add(JournalObjectKind::Extension, value),
            })
            .collect();
        let extensions = self.map(entries);
        JournalSnapshotRoots {
            history,
            history_chain,
            usage,
            extensions,
            manifests,
            planned_step_count: snapshot.manifests.len() as u64 + 77,
            identity: snapshot.identity.clone(),
            updated: snapshot.updated,
        }
    }
    fn request(&mut self, request: &ProviderRequest) -> JournalObjectId {
        let ids = request
            .messages
            .iter()
            .map(|message| self.add(JournalObjectKind::Message, message))
            .collect();
        let messages = self.sequence(ids);
        let ids = request
            .tools
            .iter()
            .map(|schema| self.add(JournalObjectKind::ToolSchema, schema))
            .collect();
        let tools = self.sequence(ids);
        let mut settings = request.clone();
        settings.messages.clear();
        settings.tools.clear();
        let settings = self.add(JournalObjectKind::RequestSettings, &settings);
        self.add(
            JournalObjectKind::Request,
            &ReferencedProviderRequest {
                messages,
                tools,
                settings,
            },
        )
    }
    fn state(&mut self, state: &TurnState) -> JournalObjectId {
        let Value::Object(mut fields) = serde_json::to_value(state).unwrap() else {
            unreachable!()
        };
        let tag = fields.remove("state").unwrap().as_str().unwrap().to_owned();
        let mut refs = BTreeMap::new();
        for (name, value) in fields {
            let reference = if tag == "calling_model" && name == "request" {
                JournalStateValue::Request {
                    object: self.request(&serde_json::from_value(value).unwrap()),
                }
            } else if let Value::Array(values) = value {
                let ids = values
                    .iter()
                    .map(|value| self.add(JournalObjectKind::StateValue, value))
                    .collect();
                JournalStateValue::Sequence {
                    sequence: self.sequence(ids),
                }
            } else {
                JournalStateValue::Object {
                    object: self.add(JournalObjectKind::StateValue, &value),
                }
            };
            refs.insert(name, reference);
        }
        self.add(
            JournalObjectKind::TurnState,
            &ReferencedTurnState { tag, fields: refs },
        )
    }
}

pub(super) fn snapshot() -> SessionSnapshot {
    let mut usage = UsageLedger::new();
    usage.record(UsageRecord {
        source: UsageSource::ProviderAttempt,
        provenance: Provenance {
            failed: true,
            ..Default::default()
        },
        delta: UsageDelta::new(),
    });
    SessionSnapshot {
        id: SessionId::new("session-1"),
        history: vec![Message::user("hello")],
        usage,
        identity: SessionIdentityState {
            turn: 1,
            request: 4,
            attempt: 3,
            event: 8,
            tool_call: 2,
            steer: 1,
            event_seq: 9,
        },
        manifests: Vec::new(),
        extension_state: [(
            "private.fixture".to_owned(),
            VersionedSessionState::new(
                RegistryRevision::new("v1"),
                serde_json::json!({"secret":"raw", "negative_zero":-0.0}),
            ),
        )]
        .into(),
        updated: Timestamp(100),
    }
}

pub(super) fn accepted() -> TurnCheckpoint {
    TurnCheckpoint::accepted(
        TurnId::new("turn-1"),
        UserInput::text("hello"),
        snapshot(),
        0,
        Deadline::at(Timestamp(123456)),
        1,
        9,
        Timestamp(100),
    )
    .unwrap()
}

pub(super) fn call() -> ToolCall {
    ToolCall {
        id: ToolCallId::new("call-1"),
        name: "read".to_owned(),
        arguments: serde_json::json!({"raw": "credential", "negative_zero":-0.0}),
    }
}
pub(super) fn prepared(call: &ToolCall) -> PreparedToolCall {
    PreparedToolCall::new(
        call.id.clone(),
        call.name.clone(),
        call.arguments.clone(),
        PermissionSet::new(),
        SecurityResource::other("tool", &call.name),
        ToolEffects::default(),
        ToolCallDisplay::new("Read"),
    )
}
pub(super) fn result(call: &ToolCall) -> ToolResultBlock {
    ToolResultBlock {
        call_id: call.id.clone(),
        name: call.name.clone(),
        content: vec![ContentPart::text("done")],
        is_error: false,
    }
}
pub(super) fn model_request() -> ProviderRequest {
    let mut request = ProviderRequest::new(
        ModelId::new("fake"),
        vec![
            Message::system("projected summary (not a canonical history prefix)"),
            Message {
                role: Role::Assistant,
                content: vec![ContentPart::Reasoning {
                    text: "thought\né\u{2028}".to_owned(),
                    redacted: false,
                    signature: Some("sig+/=\0\né".to_owned()),
                    producer: None,
                }],
            },
            Message::user("final transformed prompt"),
        ],
    );
    request.tools = vec![ToolSchema {
        name: "read".to_owned(),
        description: "Read exactly".to_owned(),
        input_schema: serde_json::json!({"type":"object","properties":{"raw":{"type":"string"}}}),
    }];
    request.sampling.temperature = Some(-0.0);
    request.sampling.top_p = Some(0.9);
    request.reasoning = Some(ReasoningConfig {
        effort: Some("high".to_owned()),
        max_tokens: Some(128),
    });
    request.max_output_tokens = Some(256);
    request.stop = vec!["stop\0\n".to_owned()];
    request.vendor_extensions =
        serde_json::json!({"continuation":"opaque+/=\0", "number":1.2345678901234567e-100});
    request.cache_identity = Some(CacheIdentity::legacy(
        Fingerprint::of("profile"),
        "fake",
        ModelId::new("fake"),
        Vec::new(),
        PromptCacheControl::Implicit,
    ));
    request.cache_boundary = Some(ProviderCacheBoundary {
        stable_tool_count: 1,
        stable_system_block_count: 1,
        stable_message_count: 2,
    });
    request
}

pub(super) fn states() -> Vec<TurnState> {
    let request_id = RequestId::new("request-a");
    let call = call();
    let prepared = prepared(&call);
    let input = InternalTurnInput::new(
        "private instruction",
        InternalTurnSource {
            kind: "goal".to_owned(),
            id: "fixture".to_owned(),
            revision: RegistryRevision::new("v1"),
            sensitivity: InternalTurnSensitivity::Sensitive,
            goal: None,
        },
    )
    .unwrap();
    let interaction = InteractionRequest::questionnaire(
        InteractionRequestId::new("questionnaire-1"),
        InteractionOrigin::new(
            SessionId::new("session-1"),
            TurnId::new("turn-1"),
            call.id.clone(),
        ),
        Questionnaire::new(vec![
            Question::new(QuestionId::new("question-1"), "Choice", "Private prompt")
                .allow_free_form(true),
        ])
        .unwrap(),
        Deadline::at(Timestamp(123456)),
        InteractionSensitivity::Sensitive,
    )
    .unwrap();
    let operation = CacheOperationCheckpoint {
        operation: CacheOperationId::new("cache-1"),
        request: Some(request_id.clone()),
        attempt: None,
        identity: model_request().cache_identity.unwrap(),
        purpose: ProviderAttemptPurpose::CacheKeepalive,
        fingerprint: "a".repeat(64),
        expected_read_tokens: Some(10),
        preflight_rejection: None,
    };
    let started = CacheOperationCheckpoint {
        attempt: Some(AttemptId::new("attempt-1")),
        ..operation.clone()
    };
    let cache_result = CacheOperationResultCheckpoint {
        outcome: CacheOperationOutcome::Completed,
        state: CacheState::WarmObserved,
        evidence: None,
        metrics: [("cache_read_tokens".to_owned(), 10)].into(),
        rejection_reason: None,
        terminal_reason: None,
    };
    let response = AssembledModelResponse {
        attempt: AttemptId::new("attempt-1"),
        text: "visible".to_owned(),
        reasoning: model_request().messages[1].content.clone(),
        tool_calls: vec![call.clone()],
        advertised_tools: vec![call.name.clone()],
        finish: FinishReason::ToolCalls,
    };
    vec![
        TurnState::Accepted {
            input: UserInput::text("hello"),
        },
        TurnState::InternalAccepted { input },
        TurnState::LocalActionAccepted {
            request_id: request_id.clone(),
            call: call.clone(),
        },
        TurnState::LocalActionPrepared {
            request_id: request_id.clone(),
            call: call.clone(),
            prepared: prepared.clone(),
        },
        TurnState::LocalActionExecuting {
            request_id: request_id.clone(),
            call: call.clone(),
            prepared: prepared.clone(),
        },
        TurnState::LocalActionOutcomeReady {
            request_id: request_id.clone(),
            call: call.clone(),
            outcome: ToolOutcome::json(
                serde_json::json!({"exact": "unbounded raw", "negative_zero":-0.0}),
            ),
        },
        TurnState::LocalActionResultReady {
            request_id: request_id.clone(),
            call: call.clone(),
            result: result(&call),
        },
        TurnState::Planning { step: 0 },
        TurnState::CallingModel {
            request_id: request_id.clone(),
            request: model_request(),
            step: 0,
        },
        TurnState::ModelResponseReady {
            request_id: request_id.clone(),
            response,
            step: 0,
        },
        TurnState::AwaitingApproval {
            request_id: request_id.clone(),
            source_calls: vec![call.clone()],
            slots: vec![ToolSlotCheckpoint::Prepared(prepared.clone())],
            step: 0,
        },
        TurnState::AwaitingInteraction {
            request_id: request_id.clone(),
            source_calls: vec![call.clone()],
            slots: vec![ToolSlotCheckpoint::Prepared(prepared.clone())],
            completed: Vec::new(),
            interaction_index: 0,
            request: interaction,
            response: Some(InteractionResponse::declined(InteractionRequestId::new(
                "questionnaire-1",
            ))),
            step: 0,
        },
        TurnState::ToolOutcomeReady {
            request_id: request_id.clone(),
            source_calls: vec![call.clone()],
            slots: vec![ToolSlotCheckpoint::Prepared(prepared.clone())],
            completed: Vec::new(),
            outcome_index: 0,
            outcome: ToolOutcome::json(serde_json::json!({"exact":"raw", "negative_zero":-0.0})),
            step: 0,
        },
        TurnState::ExecutingTools {
            request_id,
            source_calls: vec![call.clone()],
            slots: vec![ToolSlotCheckpoint::Prepared(prepared)],
            completed: Vec::new(),
            step: 0,
        },
        TurnState::Completing {
            finish: TurnFinish::Completed,
            visible_output: true,
            provider_error_kind: None,
        },
        TurnState::PublishingTerminal {
            finish: TurnFinish::Completed,
            visible_output: true,
        },
        TurnState::Terminal {
            finish: TurnFinish::Completed,
            visible_output: true,
        },
        TurnState::CacheOperationPrepared { operation },
        TurnState::CacheOperationStarted {
            operation: started.clone(),
        },
        TurnState::CacheOperationResultReady {
            operation: started.clone(),
            result: cache_result.clone(),
        },
        TurnState::CacheOperationTerminal {
            operation: started,
            result: cache_result,
        },
    ]
}

pub(super) fn checkpoint_for_state(state: &TurnState) -> TurnCheckpoint {
    let mut checkpoint = accepted();
    match state {
        TurnState::InternalAccepted { input } => {
            checkpoint = TurnCheckpoint::internal_accepted(
                checkpoint.turn.clone(),
                input.clone(),
                snapshot(),
                0,
                checkpoint.deadline,
                1,
                9,
                Timestamp(100),
            )
            .unwrap();
        }
        TurnState::LocalActionAccepted { request_id, call }
        | TurnState::LocalActionPrepared {
            request_id, call, ..
        }
        | TurnState::LocalActionExecuting {
            request_id, call, ..
        }
        | TurnState::LocalActionOutcomeReady {
            request_id, call, ..
        }
        | TurnState::LocalActionResultReady {
            request_id, call, ..
        } => {
            checkpoint = TurnCheckpoint::local_action(
                checkpoint.turn.clone(),
                request_id.clone(),
                call.clone(),
                snapshot(),
                checkpoint.deadline,
                1,
                9,
                Timestamp(100),
            )
            .unwrap();
        }
        TurnState::CacheOperationPrepared { operation }
        | TurnState::CacheOperationStarted { operation }
        | TurnState::CacheOperationResultReady { operation, .. }
        | TurnState::CacheOperationTerminal { operation, .. } => {
            let mut prepared = operation.clone();
            prepared.attempt = None;
            checkpoint = TurnCheckpoint::cache_operation(
                checkpoint.turn.clone(),
                prepared,
                snapshot(),
                checkpoint.deadline,
                1,
                9,
                Timestamp(100),
            )
            .unwrap();
        }
        _ => {}
    }
    // Build legal trajectories up to each actual state; the only exceptional
    // seeded later cache phases retain the same validated operation envelope.
    if checkpoint.state == *state {
        return checkpoint;
    }
    let path: Vec<_> = match state {
        TurnState::LocalActionPrepared { .. } => vec![state.clone()],
        TurnState::LocalActionExecuting { .. } => vec![states()[3].clone(), state.clone()],
        TurnState::LocalActionOutcomeReady { .. } => {
            vec![states()[3].clone(), states()[4].clone(), state.clone()]
        }
        TurnState::LocalActionResultReady { .. } => vec![
            states()[3].clone(),
            states()[4].clone(),
            states()[5].clone(),
            state.clone(),
        ],
        TurnState::CacheOperationStarted { .. } => vec![state.clone()],
        TurnState::CacheOperationResultReady { .. } => vec![states()[18].clone(), state.clone()],
        TurnState::CacheOperationTerminal { .. } => {
            vec![states()[18].clone(), states()[19].clone(), state.clone()]
        }
        _ => {
            let all = states();
            let mut path = vec![all[7].clone()];
            if !matches!(state, TurnState::Planning { .. }) {
                path.push(all[8].clone());
            }
            if !matches!(
                state,
                TurnState::Planning { .. } | TurnState::CallingModel { .. }
            ) {
                path.push(all[9].clone());
            }
            match state {
                TurnState::AwaitingApproval { .. } => path.push(state.clone()),
                TurnState::AwaitingInteraction { .. } => {
                    path.push(all[10].clone());
                    path.push(all[13].clone());
                    let mut pending = state.clone();
                    if let TurnState::AwaitingInteraction { response, .. } = &mut pending {
                        *response = None;
                    }
                    path.push(pending);
                    path.push(state.clone());
                }
                TurnState::ExecutingTools { .. } => {
                    path.push(all[10].clone());
                    path.push(state.clone());
                }
                TurnState::ToolOutcomeReady { .. } => {
                    path.push(all[10].clone());
                    path.push(all[13].clone());
                    path.push(state.clone());
                }
                TurnState::Completing { .. } => path.push(state.clone()),
                TurnState::PublishingTerminal { .. } => {
                    path.push(all[14].clone());
                    path.push(state.clone());
                }
                TurnState::Terminal { .. } => {
                    path.push(all[14].clone());
                    path.push(all[15].clone());
                    path.push(state.clone());
                }
                _ => {}
            }
            path
        }
    };
    for next in path {
        let visible = checkpoint.visible_output
            || matches!(&next, TurnState::ModelResponseReady { response, .. } if !response.text.is_empty());
        checkpoint = checkpoint
            .transition_with_progress(
                next,
                checkpoint.snapshot.clone(),
                checkpoint.active_history_start,
                visible,
                checkpoint.watermark.event_sequence + 1,
                Timestamp(checkpoint.updated.0 + 1),
            )
            .unwrap();
    }
    assert_eq!(&checkpoint.state, state);
    checkpoint
}
