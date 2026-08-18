//! Admission over a protected non-terminal checkpoint.
//!
//! A turn that ends without a durable terminal boundary -- for example after
//! a failed protected write, or a restart that left its checkpoint dormant --
//! must not wedge every later admission behind
//! "cannot accept a new turn over a non-terminal checkpoint". Newly admitted
//! work finalizes the interrupted turn as an explicit `Failed` terminal
//! without replaying its indeterminate outcome and then proceeds through
//! ordinary acceptance.

use std::sync::{Arc, Mutex};

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint, TurnState};
use agent_runtime::core::clock::{Deadline, Timestamp};
use agent_runtime::core::content::{Message, UserInput};
use agent_runtime::core::error::RuntimeError;
use agent_runtime::core::event::{EventEnvelope, RuntimeEvent, TurnFinish};
use agent_runtime::core::ids::{SessionId, TurnId};
use agent_runtime::core::observer::EventObserver;
use agent_runtime::core::provider::{Capabilities, FinishReason, ModelId, ProviderStreamEvent};
use agent_runtime::core::store::{SessionIdentityState, SessionSnapshot, SessionStore};
use agent_runtime::core::usage::UsageLedger;
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream};
use agent_runtime::runtime::{CheckpointRecoveryPolicy, Runtime, RuntimeBuilder, StartSession};
use async_trait::async_trait;

#[derive(Debug, Default)]
struct MemorySessionStore {
    snapshots: Mutex<Vec<SessionSnapshot>>,
}

#[async_trait]
impl SessionStore for MemorySessionStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .snapshots
            .lock()
            .expect("session store lock")
            .iter()
            .rev()
            .find(|snapshot| snapshot.id == *id)
            .cloned())
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.snapshots
            .lock()
            .expect("session store lock")
            .push(snapshot.clone());
        Ok(())
    }
}

/// A checkpoint store that records every accepted save and can fail one
/// save by index, simulating the durable-boundary failure that strands a
/// turn's checkpoint short of a terminal state.
#[derive(Debug, Default)]
struct RecordingCheckpointStore {
    latest: Mutex<Option<TurnCheckpoint>>,
    saved: Mutex<Vec<TurnCheckpoint>>,
    fail_next_completing: Mutex<bool>,
}

impl RecordingCheckpointStore {
    /// Fails the next save of a `Completing` checkpoint, stranding that turn
    /// one boundary short of its terminal state.
    fn fail_next_completing_save(&self) {
        *self.fail_next_completing.lock().expect("fault slot lock") = true;
    }

    fn latest(&self) -> Option<TurnCheckpoint> {
        self.latest.lock().expect("checkpoint store lock").clone()
    }

    fn saved(&self) -> Vec<TurnCheckpoint> {
        self.saved.lock().expect("checkpoint store lock").clone()
    }

    fn seed(&self, checkpoint: TurnCheckpoint) {
        *self.latest.lock().expect("checkpoint store lock") = Some(checkpoint);
    }

    /// One-shot: trips only for the first `Completing` save after arming.
    fn trip_fault(&self, checkpoint: &TurnCheckpoint) -> bool {
        let mut armed = self.fail_next_completing.lock().expect("fault slot lock");
        if *armed && matches!(checkpoint.state, TurnState::Completing { .. }) {
            *armed = false;
            return true;
        }
        false
    }
}

#[async_trait]
impl CheckpointStore for RecordingCheckpointStore {
    async fn load_latest(
        &self,
        _session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self.latest.lock().expect("checkpoint store lock").clone())
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        if self.trip_fault(checkpoint) {
            return Err(RuntimeError::conflict(
                "scripted protected checkpoint write failure",
            ));
        }
        *self.latest.lock().expect("checkpoint store lock") = Some(checkpoint.clone());
        self.saved
            .lock()
            .expect("checkpoint store lock")
            .push(checkpoint.clone());
        Ok(())
    }
}

#[derive(Debug, Default)]
struct EnvelopeObserver {
    events: Mutex<Vec<EventEnvelope>>,
}

impl EnvelopeObserver {
    fn events(&self) -> Vec<EventEnvelope> {
        self.events.lock().expect("observer lock").clone()
    }

    fn last_finish(&self) -> Option<TurnFinish> {
        self.events
            .lock()
            .expect("observer lock")
            .iter()
            .rev()
            .find_map(|event| match &event.payload {
                RuntimeEvent::TurnCompleted { finish, .. } => Some(finish.clone()),
                _ => None,
            })
    }
}

impl EventObserver for EnvelopeObserver {
    fn observe(&self, event: &EventEnvelope) {
        self.events
            .lock()
            .expect("observer lock")
            .push(event.clone());
    }
}

fn runtime(
    provider: Arc<FakeProvider>,
    sessions: Arc<MemorySessionStore>,
    checkpoints: Arc<RecordingCheckpointStore>,
    observer: Arc<EnvelopeObserver>,
) -> Runtime {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .observer(observer)
        .build()
        .expect("runtime builds")
}

fn text_reply_script(text: &str) -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta { text: text.into() },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])
}

/// A failed protected write leaves the interrupted turn's checkpoint
/// non-terminal; the next turn must be admitted anyway, finalizing the
/// interrupted turn as an explicit `Failed` terminal first.
#[tokio::test]
async fn new_turn_finalizes_interrupted_checkpoint_instead_of_wedging() {
    let session_id = SessionId::new("interrupted-admission");
    let sessions = Arc::new(MemorySessionStore::default());
    let checkpoints = Arc::new(RecordingCheckpointStore::default());
    // Fail the first turn's Completing boundary: the turn's own terminal
    // publication dies non-durably and strands its checkpoint.
    checkpoints.fail_next_completing_save();

    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![
            text_reply_script("first reply"),
            text_reply_script("second reply"),
        ],
    ));
    let observer = Arc::new(EnvelopeObserver::default());
    let runtime = runtime(
        provider.clone(),
        sessions.clone(),
        checkpoints.clone(),
        observer.clone(),
    );
    let session = runtime
        .start_session(StartSession::new().with_id(session_id.clone()))
        .await
        .expect("session starts");

    session
        .run(UserInput::text("first question"))
        .await
        .expect("a non-durable turn failure still commits its handle");
    let interrupted = checkpoints
        .latest()
        .expect("the interrupted turn checkpointed");
    assert!(
        !interrupted.state.is_terminal(),
        "the scripted protected write failure strands the checkpoint: {:?}",
        interrupted.state
    );
    assert_eq!(
        provider.requests().len(),
        1,
        "the turn failed while publishing its terminal boundary, mid-flight"
    );

    // Before reconciliation this admission failed with
    // "cannot accept a new turn over a non-terminal checkpoint".
    session
        .run(UserInput::text("second question"))
        .await
        .expect("the next turn is admitted over the interrupted checkpoint");

    let saved = checkpoints.saved();
    let finalized = saved
        .iter()
        .find(|checkpoint| {
            checkpoint.turn == interrupted.turn
                && matches!(
                    checkpoint.state,
                    TurnState::Terminal {
                        finish: TurnFinish::Failed,
                        ..
                    }
                )
        })
        .expect("the interrupted turn reached an explicit Failed terminal");
    let admitted = saved
        .iter()
        .find(|checkpoint| {
            checkpoint.turn != interrupted.turn
                && matches!(checkpoint.state, TurnState::Accepted { .. })
        })
        .expect("the new turn has an acceptance checkpoint");
    assert_eq!(
        admitted.watermark.checkpoint_sequence,
        finalized.watermark.checkpoint_sequence + 1,
        "the new turn continues the reconciled checkpoint watermark"
    );
    assert!(
        matches!(observer.last_finish(), Some(TurnFinish::Completed)),
        "the admitted turn completes: {:?}",
        observer.last_finish()
    );

    let events = observer.events();
    let finalize_error = events
        .iter()
        .position(|event| {
            event.turn.as_ref() == Some(&interrupted.turn)
                && matches!(event.payload, RuntimeEvent::Error { .. })
        })
        .expect("the finalization is attributed an error event");
    let second_start = events
        .iter()
        .position(|event| {
            event.turn.as_ref() != Some(&interrupted.turn)
                && matches!(event.payload, RuntimeEvent::TurnStarted)
        })
        .expect("the new turn starts");
    assert!(
        finalize_error < second_start,
        "the interrupted turn is finalized before the new turn starts"
    );
}

/// A restart that deliberately leaves an interrupted checkpoint dormant
/// (the defer recovery policy) must still admit new work: the dormant turn
/// is finalized as failed, never resumed by the admission path.
#[tokio::test]
async fn dormant_interrupted_checkpoint_is_finalized_on_new_work() {
    let session_id = SessionId::new("dormant-interrupted");
    let sessions = Arc::new(MemorySessionStore::default());
    let checkpoints = Arc::new(RecordingCheckpointStore::default());
    let snapshot = SessionSnapshot {
        id: session_id.clone(),
        history: vec![Message::user("dormant question")],
        usage: UsageLedger::new(),
        identity: SessionIdentityState::default(),
        manifests: Vec::new(),
        extension_state: Default::default(),
        updated: Timestamp::ZERO,
    };
    let seeded = TurnCheckpoint::accepted(
        TurnId::new("seeded-turn"),
        UserInput::text("dormant question"),
        snapshot,
        0,
        Deadline::never(),
        1,
        2,
        Timestamp::ZERO,
    )
    .expect("seeded acceptance checkpoint");
    checkpoints.seed(seeded.clone());

    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![text_reply_script("fresh reply")],
    ));
    let observer = Arc::new(EnvelopeObserver::default());
    let runtime = runtime(
        provider.clone(),
        sessions.clone(),
        checkpoints.clone(),
        observer.clone(),
    );
    let session = runtime
        .start_session(
            StartSession::new()
                .with_id(session_id.clone())
                .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
        )
        .await
        .expect("session starts with the checkpoint dormant");
    assert_eq!(
        provider.requests().len(),
        0,
        "the deferred policy must not resume the dormant turn"
    );

    session
        .run(UserInput::text("new question"))
        .await
        .expect("new work is admitted over the dormant interrupted checkpoint");

    let saved = checkpoints.saved();
    assert!(
        saved.iter().any(|checkpoint| {
            checkpoint.turn == seeded.turn
                && matches!(
                    checkpoint.state,
                    TurnState::Terminal {
                        finish: TurnFinish::Failed,
                        ..
                    }
                )
        }),
        "the dormant turn is finalized as failed: {:?}",
        saved
            .iter()
            .map(|checkpoint| &checkpoint.state)
            .collect::<Vec<_>>()
    );
    assert!(
        matches!(observer.last_finish(), Some(TurnFinish::Completed)),
        "the admitted turn completes: {:?}",
        observer.last_finish()
    );
}
