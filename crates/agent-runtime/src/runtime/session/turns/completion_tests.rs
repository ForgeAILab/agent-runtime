use super::*;
use std::future::{Future, poll_fn};
use std::task::{Context, Wake, Waker};

use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::content::{InternalTurnSensitivity, InternalTurnSource};
use agent_runtime_core::provider::ModelId;
use agent_runtime_provider::fake::FakeProvider;
use agent_runtime_registry::RegistryRevision;

use crate::runtime::{RuntimeBuilder, StartSession};

struct AdmissionOnWake {
    session: SessionHandle,
    parent: Waker,
    observations: Arc<Mutex<Vec<(usize, bool)>>>,
}

impl Wake for AdmissionOnWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // Capture admission synchronously at notification, before the
        // completing task can continue. This deterministically models a
        // host waking on another worker and immediately doing idle work.
        let active = self.session.inner.turns.lock().unwrap().count;
        let gate_available = self.session.inner.turn_gate.try_lock().is_ok();
        self.observations
            .lock()
            .unwrap()
            .push((active, gate_available));
        self.parent.wake_by_ref();
    }
}

async fn session_with_history(history: Vec<Message>) -> SessionHandle {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("done")))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(8_192, 8_192, 1_024),
        ))
        .build()
        .unwrap()
        .start_session(StartSession::new().with_history(history))
        .await
        .unwrap()
}

async fn session() -> SessionHandle {
    session_with_history(Vec::new()).await
}

async fn assert_completion_releases_admission(
    session: &SessionHandle,
    turn: TurnHandle,
    finish: Option<TurnFinish>,
) {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let completed = turn.completed();
    tokio::pin!(completed);
    poll_fn(|context| {
        let waker = Waker::from(Arc::new(AdmissionOnWake {
            session: session.clone(),
            parent: context.waker().clone(),
            observations: observations.clone(),
        }));
        completed.as_mut().poll(&mut Context::from_waker(&waker))
    })
    .await;
    assert_eq!(
        *observations.lock().unwrap(),
        vec![(0, true)],
        "completion must release active-turn admission and the turn gate before waking the host"
    );
    assert_eq!(turn.outcome().await.0, finish, "retain the turn outcome");
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn user_completion_releases_admission_before_waking() {
    let session = session().await;
    let turn = session.send(UserInput::text("hello")).unwrap();
    assert_completion_releases_admission(&session, turn, Some(TurnFinish::Completed)).await;
}

#[tokio::test]
async fn internal_completion_releases_admission_before_waking() {
    let session = session().await;
    let source = InternalTurnSource {
        kind: "fixture".into(),
        id: "completion-test".into(),
        revision: RegistryRevision::new("v1"),
        sensitivity: InternalTurnSensitivity::Public,
        goal: None,
    };
    let InternalTurnAdmission::Accepted(turn) = session
        .try_send_internal_if_idle(InternalTurnInput::new("continue", source).unwrap())
        .unwrap()
    else {
        panic!("idle internal turn must be accepted");
    };
    assert_completion_releases_admission(&session, turn, Some(TurnFinish::Completed)).await;
}

#[tokio::test]
async fn resumed_completion_releases_admission_before_waking() {
    // The engine restores the accepted history before spawning recovery.
    let session = session_with_history(vec![Message::user("resume me")]).await;
    let checkpoint = TurnCheckpoint::accepted(
        TurnId::new("completion-resume"),
        UserInput::text("resume me"),
        session.snapshot(),
        0,
        Deadline::never(),
        1,
        2,
        Timestamp::ZERO,
    )
    .unwrap();
    let turn = session.spawn_checkpoint_resume(checkpoint).unwrap();
    assert_completion_releases_admission(&session, turn, Some(TurnFinish::Completed)).await;
}

#[tokio::test]
async fn aborted_completion_releases_admission_before_waking() {
    let session = session().await;
    let turn = session
        .send(UserInput::text("abort before polling"))
        .unwrap();
    // The guard is captured before spawning, so even an unpolled task
    // must release admission and notify its completion waiter on abort.
    session.inner.turns.lock().unwrap().aborts[0].abort();
    assert_completion_releases_admission(&session, turn, None).await;
}
