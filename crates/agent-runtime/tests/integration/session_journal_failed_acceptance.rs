//! A turn whose protected acceptance checkpoint fails.
//!
//! The runtime appends the turn's input and the injected content it drains
//! to live history before the acceptance checkpoint commits. What it may do
//! with that speculative state depends on what the protected store proves:
//!
//! * proven absent: the input and injections leave live history (the
//!   injections return to the queue), so a later ordinary save cannot publish
//!   history past the protected terminal checkpoint;
//! * proven committed (a lost acknowledgement the store resolves): the turn
//!   is accepted and runs;
//! * unresolved: the input may be durable, so it stays and the session's
//!   persistence is fenced; a reopen recovers whichever boundary the
//!   protected store holds.
//!
//! No ordinary save may observe the speculative state while the outcome is
//! open.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint};
use agent_runtime::core::content::{
    InternalTurnInput, InternalTurnSensitivity, InternalTurnSource, Message, Role, UserInput,
};
use agent_runtime::core::error::RuntimeError;
use agent_runtime::core::ids::SessionId;
use agent_runtime::core::journal::{
    IdentityJournalProjection, InMemorySessionJournal, JournalCommit, JournalCommitFailurePoint,
    JournalGcReport, JournalGcRequest, JournalHead, JournalLease, JournalObjectId, JournalOpen,
    JournalOpenIntent, JournalOperationId, JournalPinId, JournalReadLimits, JournalReader,
    JournalRetirement, JournalRevision, JournalRootSelection, JournalStorageId, SessionJournal,
};
use agent_runtime::core::provider::{Capabilities, FinishReason, ModelId, ProviderStreamEvent};
use agent_runtime::core::store::{SessionSnapshot, SessionStore};
use agent_runtime::provider::fake::{FakeProvider, ScriptedStream, usage_event};
use agent_runtime::registry::RegistryRevision;
use agent_runtime::runtime::{
    InjectedContent, InjectionRecovery, InternalTurnAdmission, Runtime, RuntimeBuilder,
    SessionHandle, StartSession, TurnHandle,
};
use async_trait::async_trait;
use tokio::sync::Notify;

const INJECTION: &str = "queued child result";

fn injected_failure(what: &str) -> RuntimeError {
    RuntimeError::internal(format!("injected {what}")).retryable()
}

fn replies(count: usize) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        (0..count)
            .map(|index| {
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta {
                        text: format!("reply {index}"),
                    },
                    usage_event(6, 3),
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ])
            })
            .collect(),
    ))
}

fn builder(provider: Arc<FakeProvider>) -> RuntimeBuilder {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(32_000, 32_000, 4_096),
        ))
}

fn count(history: &[Message], text: &str) -> usize {
    history
        .iter()
        .filter(|message| message.joined_text() == text)
        .count()
}

/// Runs one user turn and requires it to end with an assistant reply.
async fn complete(session: &SessionHandle, text: &str) -> Result<(), String> {
    let before = session.history().len();
    session
        .run(UserInput::text(text))
        .await
        .map_err(|error| format!("{error:?}"))?;
    let history = session.history();
    match history.last() {
        Some(last) if history.len() > before && last.role == Role::Assistant => Ok(()),
        _ => Err(format!("turn `{text}` did not complete: {history:?}")),
    }
}

// ---- legacy stores ----

#[derive(Debug, Default)]
struct Sessions(Mutex<Option<SessionSnapshot>>);

impl Sessions {
    fn history_len(&self) -> Option<usize> {
        self.0
            .lock()
            .unwrap()
            .as_ref()
            .map(|snapshot| snapshot.history.len())
    }
}

#[async_trait]
impl SessionStore for Sessions {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .clone()
            .filter(|snapshot| snapshot.id == *id))
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        *self.0.lock().unwrap() = Some(snapshot.clone());
        Ok(())
    }
}

/// How the next legacy checkpoint save fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyFault {
    /// Nothing is stored.
    Rejected,
    /// The checkpoint is stored and the acknowledgement lost; reading it
    /// back works.
    LostAck,
    /// The checkpoint is stored and the acknowledgement lost; the read-back
    /// fails too.
    LostAckUnreadable,
    /// Waits for `release`, then fails without storing anything.
    BlockThenReject,
    /// Nothing is stored and the read-back fails: the outcome is unresolved
    /// although the write never landed.
    RejectedUnreadable,
}

#[derive(Debug, Default)]
struct Checkpoints {
    stored: Mutex<Option<TurnCheckpoint>>,
    fault: Mutex<Option<LegacyFault>>,
    fail_next_load: AtomicBool,
    entered: Notify,
    release: Notify,
}

impl Checkpoints {
    fn arm(&self, fault: LegacyFault) {
        *self.fault.lock().unwrap() = Some(fault);
    }

    fn stored(&self) -> TurnCheckpoint {
        self.stored
            .lock()
            .unwrap()
            .clone()
            .expect("a stored checkpoint")
    }
}

#[async_trait]
impl CheckpointStore for Checkpoints {
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        if self.fail_next_load.swap(false, Ordering::SeqCst) {
            return Err(injected_failure("checkpoint read failure"));
        }
        Ok(self
            .stored
            .lock()
            .unwrap()
            .clone()
            .filter(|checkpoint| checkpoint.session == *session))
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        let fault = self.fault.lock().unwrap().take();
        match fault {
            None => {
                *self.stored.lock().unwrap() = Some(checkpoint.clone());
                Ok(())
            }
            Some(LegacyFault::Rejected) => Err(injected_failure("checkpoint save failure")),
            Some(LegacyFault::RejectedUnreadable) => {
                self.fail_next_load.store(true, Ordering::SeqCst);
                Err(injected_failure("checkpoint save failure"))
            }
            Some(LegacyFault::BlockThenReject) => {
                self.entered.notify_one();
                self.release.notified().await;
                Err(injected_failure("checkpoint save failure"))
            }
            Some(fault) => {
                *self.stored.lock().unwrap() = Some(checkpoint.clone());
                if fault == LegacyFault::LostAckUnreadable {
                    self.fail_next_load.store(true, Ordering::SeqCst);
                }
                Err(injected_failure("lost checkpoint acknowledgement"))
            }
        }
    }
}

fn legacy_runtime(
    provider: Arc<FakeProvider>,
    sessions: &Arc<Sessions>,
    checkpoints: &Arc<Checkpoints>,
) -> Runtime {
    builder(provider)
        .session_store(sessions.clone())
        .checkpoint_store(checkpoints.clone())
        .build()
        .unwrap()
}

// ---- native journals ----

/// How the protected journal's next commit fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeFault {
    /// Waits for `release`, then fails before its visibility point.
    BlockThenReject,
    /// Lands, loses its acknowledgement, and the writer's renew fails.
    LostAckRenewFails,
    /// Lands, loses its acknowledgement, and the same-operation replay fails.
    LostAckReplayFails,
    /// Fails before its visibility point, and the writer's renew fails, so
    /// the outcome is unresolved although nothing landed.
    RejectRenewFails,
}

#[derive(Debug)]
struct FaultyJournal {
    inner: Arc<InMemorySessionJournal>,
    fault: Mutex<Option<NativeFault>>,
    fail_renew: AtomicBool,
    fail_replay: AtomicBool,
    entered: Notify,
    release: Notify,
}

impl FaultyJournal {
    fn new(inner: InMemorySessionJournal) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(inner),
            fault: Mutex::new(None),
            fail_renew: AtomicBool::new(false),
            fail_replay: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
        })
    }

    fn arm(&self, fault: NativeFault) {
        *self.fault.lock().unwrap() = Some(fault);
    }
}

#[async_trait]
impl SessionJournal for FaultyJournal {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }
    async fn open(
        &self,
        session: &SessionId,
        request: JournalOpen,
    ) -> Result<JournalLease, RuntimeError> {
        self.inner.open(session, request).await
    }
    async fn read(
        &self,
        lease: &JournalLease,
        object: &JournalObjectId,
    ) -> Result<Vec<u8>, RuntimeError> {
        self.inner.read(lease, object).await
    }
    async fn commit(
        &self,
        lease: &JournalLease,
        batch: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        let fault = self.fault.lock().unwrap().take();
        match fault {
            Some(NativeFault::BlockThenReject) => {
                self.entered.notify_one();
                self.release.notified().await;
                self.inner
                    .fail_next_commit_at(JournalCommitFailurePoint::AfterObjects)?;
                self.inner.commit(lease, batch).await
            }
            Some(NativeFault::LostAckRenewFails) => {
                self.inner.commit(lease, batch).await?;
                self.fail_renew.store(true, Ordering::SeqCst);
                Err(injected_failure("lost commit acknowledgement"))
            }
            Some(NativeFault::RejectRenewFails) => {
                self.inner
                    .fail_next_commit_at(JournalCommitFailurePoint::AfterObjects)?;
                let rejected = self.inner.commit(lease, batch).await;
                self.fail_renew.store(true, Ordering::SeqCst);
                rejected
            }
            Some(NativeFault::LostAckReplayFails) => {
                self.inner.commit(lease, batch).await?;
                self.fail_replay.store(true, Ordering::SeqCst);
                Err(injected_failure("lost commit acknowledgement"))
            }
            None => {
                if self.fail_replay.swap(false, Ordering::SeqCst) {
                    return Err(injected_failure("replay failure"));
                }
                self.inner.commit(lease, batch).await
            }
        }
    }
    async fn renew(&self, lease: &JournalLease) -> Result<JournalLease, RuntimeError> {
        if self.fail_renew.swap(false, Ordering::SeqCst) {
            return Err(injected_failure("renew failure"));
        }
        self.inner.renew(lease).await
    }
    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError> {
        self.inner.close(lease).await
    }
    async fn retire(
        &self,
        session: &SessionId,
        expected: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        self.inner.retire(session, expected).await
    }
    async fn collect(&self, request: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        self.inner.collect(request).await
    }
}

#[derive(Debug)]
struct NativeOrdinary(Arc<dyn SessionJournal>);

#[async_trait]
impl SessionStore for NativeOrdinary {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.0.clone())
    }
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        Err(RuntimeError::conflict(
            "native session wrote a legacy record",
        ))
    }
}

#[derive(Debug)]
struct NativeProtected(Arc<dyn SessionJournal>);

#[async_trait]
impl CheckpointStore for NativeProtected {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.0.clone())
    }
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        Err(RuntimeError::conflict(
            "native session wrote a legacy record",
        ))
    }
}

struct Native {
    ordinary: Arc<InMemorySessionJournal>,
    protected: Arc<FaultyJournal>,
}

impl Native {
    fn new(label: &str) -> Self {
        Self {
            ordinary: Arc::new(InMemorySessionJournal::ordinary(
                JournalStorageId::new(format!("failed-acceptance-ordinary-{label}")),
                Arc::new(IdentityJournalProjection),
            )),
            protected: FaultyJournal::new(InMemorySessionJournal::protected(
                JournalStorageId::new(format!("failed-acceptance-protected-{label}")),
            )),
        }
    }

    fn runtime(&self, provider: Arc<FakeProvider>) -> Runtime {
        builder(provider)
            .session_store(Arc::new(NativeOrdinary(self.ordinary.clone())))
            .checkpoint_store(Arc::new(NativeProtected(self.protected.clone())))
            .build()
            .unwrap()
    }

    async fn ordinary_head(&self, session: &SessionId) -> SessionSnapshot {
        let journal = self.ordinary.as_ref();
        let lease = journal.open(session, read_only()).await.unwrap();
        let snapshot = JournalReader::new(journal, &lease, limits())
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .expect("an ordinary head");
        journal.close(lease).await.unwrap();
        snapshot
    }

    async fn protected_head(&self, session: &SessionId) -> TurnCheckpoint {
        let journal = self.protected.inner.as_ref();
        let lease = journal.open(session, read_only()).await.unwrap();
        let checkpoint = JournalReader::new(journal, &lease, limits())
            .unwrap()
            .checkpoint()
            .await
            .unwrap()
            .expect("a protected head");
        journal.close(lease).await.unwrap();
        checkpoint
    }
}

fn read_only() -> JournalOpen {
    JournalOpen {
        root: JournalRootSelection::Latest,
        expected_revision: None,
        operation: JournalOperationId::new("failed-acceptance-read"),
        pin: JournalPinId::new("failed-acceptance-read"),
        intent: JournalOpenIntent::ReadOnly,
    }
}

fn limits() -> JournalReadLimits {
    JournalReadLimits {
        objects: 1_000_000,
        encoded_bytes: 1 << 30,
        entries: 1_000_000,
    }
}

// ---- finding 1: ordinary persistence racing the acceptance commit ----

/// A host `persist()` issued while the acceptance commit is in flight must
/// not capture the speculative input: when that commit then fails, the
/// published ordinary head would be ahead of the protected terminal
/// checkpoint and the session would never resume. The race is driven
/// deterministically: the protected commit blocks, the persist future is
/// polled once (enough to snapshot history when nothing holds it off), and
/// only then does the commit fail.
#[tokio::test]
async fn a_persist_racing_a_failing_acceptance_commit_publishes_no_speculative_history() {
    let native = Native::new("race");
    let id = SessionId::new("failed-acceptance-race");
    let session = native
        .runtime(replies(4))
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    complete(&session, "first turn").await.unwrap();
    session
        .inject(InjectedContent::text(INJECTION).must_deliver())
        .unwrap();
    native.protected.arm(NativeFault::BlockThenReject);
    let turn = session.send(UserInput::text("cut turn")).unwrap();
    native.protected.entered.notified().await;

    let persist = session.persist();
    tokio::pin!(persist);
    let early = futures_util::poll!(persist.as_mut());
    native.protected.release.notify_one();
    turn.completed().await;
    match early {
        Poll::Ready(result) => result.unwrap(),
        Poll::Pending => persist.await.unwrap(),
    }

    let protected = native.protected_head(&id).await;
    let ordinary = native.ordinary_head(&id).await;
    assert!(protected.state.is_terminal(), "{:?}", protected.state);
    assert_eq!(
        ordinary.history.len(),
        protected.snapshot.history.len(),
        "the racing persist published speculative history"
    );
    complete(&session, "next turn").await.unwrap();
    assert_eq!(count(&session.history(), INJECTION), 1);
    assert_eq!(count(&session.history(), "cut turn"), 0);
    session.shutdown().await.unwrap();

    let resumed = native
        .runtime(replies(1))
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    complete(&resumed, "resumed turn").await.unwrap();
    resumed.shutdown().await.unwrap();
}

// ---- finding 2: an error does not prove the checkpoint absent ----

/// The acceptance commit lands but its acknowledgement is lost and the
/// writer cannot resolve it (renew or replay fails). The input may be
/// durably accepted, so it must stay in the live session, the session must
/// publish nothing more, and a reopen must recover the accepted turn with
/// its input and the injection exactly once.
#[tokio::test]
async fn an_unresolved_native_acceptance_fences_the_session_and_reopen_recovers_it() {
    for fault in [
        NativeFault::LostAckRenewFails,
        NativeFault::LostAckReplayFails,
    ] {
        let native = Native::new(&format!("{fault:?}"));
        let id = SessionId::new(format!("failed-acceptance-{fault:?}"));
        let session = native
            .runtime(replies(4))
            .start_session(StartSession::create(id.clone(), Vec::new()))
            .await
            .unwrap();
        complete(&session, "first turn").await.unwrap();
        let ordinary_before = native.ordinary_head(&id).await;
        session
            .inject(InjectedContent::text(INJECTION).must_deliver())
            .unwrap();
        native.protected.arm(fault);
        session.run(UserInput::text("cut turn")).await.unwrap();
        let unresolved = session
            .take_unresolved_injections()
            .expect("the fenced session holds the drained batch");

        assert_eq!(
            count(&session.history(), "cut turn"),
            1,
            "{fault:?}: possibly accepted input was rolled back"
        );
        assert!(
            session.persist().await.is_err(),
            "{fault:?}: an unresolved session published"
        );
        assert!(session.shutdown().await.is_err(), "{fault:?}");
        assert_eq!(
            native.ordinary_head(&id).await,
            ordinary_before,
            "{fault:?}: the ordinary head moved"
        );
        let protected = native.protected_head(&id).await;
        assert!(!protected.state.is_terminal(), "{fault:?}");

        let resumed = native
            .runtime(replies(3))
            .start_session(StartSession::resume(id))
            .await
            .unwrap_or_else(|error| panic!("{fault:?}: {error:?}"));
        assert_eq!(
            resumed.restore_unresolved_injections(unresolved).unwrap(),
            InjectionRecovery::AlreadyAccepted,
            "{fault:?}"
        );
        complete(&resumed, "after reopen")
            .await
            .unwrap_or_else(|error| panic!("{fault:?}: {error}"));
        let history = resumed.history();
        assert_eq!(count(&history, "cut turn"), 1, "{fault:?}: {history:?}");
        assert_eq!(count(&history, INJECTION), 1, "{fault:?}: {history:?}");
        resumed.shutdown().await.unwrap();
    }
}

/// A legacy store reports only success or an error, so a failed acceptance
/// save is read back. The written checkpoint proves the write landed: the
/// turn is accepted and runs. An unreadable store proves nothing: the
/// session is fenced and a reopen recovers the accepted turn.
#[tokio::test]
async fn lost_legacy_acceptance_acknowledgements_are_resolved_by_reading_back() {
    // Read-back resolves the lost acknowledgement.
    let sessions = Arc::new(Sessions::default());
    let checkpoints = Arc::new(Checkpoints::default());
    let id = SessionId::new("failed-acceptance-legacy-lost-ack");
    let session = legacy_runtime(replies(4), &sessions, &checkpoints)
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    complete(&session, "first turn").await.unwrap();
    checkpoints.arm(LegacyFault::LostAck);
    complete(&session, "cut turn")
        .await
        .expect("a landed acceptance runs its turn");
    session.persist().await.unwrap();
    assert!(checkpoints.stored().state.is_terminal());
    assert_eq!(
        sessions.history_len(),
        Some(checkpoints.stored().snapshot.history.len())
    );
    session.shutdown().await.unwrap();
    let resumed = legacy_runtime(replies(1), &sessions, &checkpoints)
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    complete(&resumed, "after reopen").await.unwrap();
    assert_eq!(count(&resumed.history(), "cut turn"), 1);
    resumed.shutdown().await.unwrap();

    // The read-back fails too: unresolved.
    let sessions = Arc::new(Sessions::default());
    let checkpoints = Arc::new(Checkpoints::default());
    let id = SessionId::new("failed-acceptance-legacy-unreadable");
    let session = legacy_runtime(replies(4), &sessions, &checkpoints)
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    complete(&session, "first turn").await.unwrap();
    let ordinary_before = sessions.history_len();
    session
        .inject(InjectedContent::text(INJECTION).must_deliver())
        .unwrap();
    checkpoints.arm(LegacyFault::LostAckUnreadable);
    session.run(UserInput::text("cut turn")).await.unwrap();
    let unresolved = session
        .take_unresolved_injections()
        .expect("the fenced session holds the drained batch");
    assert_eq!(
        count(&session.history(), "cut turn"),
        1,
        "possibly accepted input was rolled back"
    );
    assert!(
        session.persist().await.is_err(),
        "an unresolved session published"
    );
    assert!(session.shutdown().await.is_err());
    assert_eq!(sessions.history_len(), ordinary_before);
    assert!(!checkpoints.stored().state.is_terminal());
    let resumed = legacy_runtime(replies(3), &sessions, &checkpoints)
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert_eq!(
        resumed.restore_unresolved_injections(unresolved).unwrap(),
        InjectionRecovery::AlreadyAccepted
    );
    complete(&resumed, "after reopen").await.unwrap();
    let history = resumed.history();
    assert_eq!(count(&history, "cut turn"), 1, "{history:?}");
    assert_eq!(count(&history, INJECTION), 1, "{history:?}");
    resumed.shutdown().await.unwrap();
}

// ---- every acceptance site rolls back a proven failure ----

#[derive(Debug, Clone, Copy)]
enum Site {
    User,
    Internal,
    #[cfg(feature = "external-agent")]
    ExternalUser,
    #[cfg(feature = "external-agent")]
    ExternalInternal,
}

fn sites() -> Vec<Site> {
    #[allow(unused_mut)]
    let mut sites = vec![Site::User, Site::Internal];
    #[cfg(feature = "external-agent")]
    sites.extend([Site::ExternalUser, Site::ExternalInternal]);
    sites
}

#[cfg(feature = "external-agent")]
mod external {
    use agent_runtime::agent::external::{
        ExternalAgentBackend, ExternalAgentEvent, ExternalTurnRequest, ExternalTurnStream,
    };
    use agent_runtime::core::error::RuntimeError;
    use async_trait::async_trait;

    /// Answers every turn with one text reply.
    #[derive(Debug)]
    pub(super) struct Replying;

    #[async_trait]
    impl ExternalAgentBackend for Replying {
        async fn run_turn(
            &self,
            _request: ExternalTurnRequest,
        ) -> Result<ExternalTurnStream, RuntimeError> {
            Ok(Box::pin(futures_util::stream::iter([
                ExternalAgentEvent::Text {
                    text: "external reply".to_owned(),
                },
                ExternalAgentEvent::Completed,
            ])))
        }
    }
}

fn site_runtime(site: Site, sessions: &Arc<Sessions>, checkpoints: &Arc<Checkpoints>) -> Runtime {
    let builder = builder(replies(4))
        .session_store(sessions.clone())
        .checkpoint_store(checkpoints.clone());
    match site {
        Site::User | Site::Internal => builder.build().unwrap(),
        #[cfg(feature = "external-agent")]
        Site::ExternalUser | Site::ExternalInternal => builder
            .external_agent(Arc::new(external::Replying))
            .build()
            .unwrap(),
    }
}

/// Starts the cut turn of `site` without waiting for it.
fn start_site_turn(site: Site, session: &SessionHandle) -> TurnHandle {
    let internal = || {
        InternalTurnInput::new(
            "continue internally",
            InternalTurnSource {
                kind: "fixture".into(),
                id: "failed-acceptance.internal".into(),
                revision: RegistryRevision::new("v1"),
                sensitivity: InternalTurnSensitivity::Public,
                goal: None,
            },
        )
        .unwrap()
    };
    let internal_turn = || {
        let InternalTurnAdmission::Accepted(turn) =
            session.try_send_internal_if_idle(internal()).unwrap()
        else {
            panic!("internal admission was not accepted");
        };
        turn
    };
    match site {
        Site::User => session.send(UserInput::text("cut turn")).unwrap(),
        #[cfg(feature = "external-agent")]
        Site::ExternalUser => session.send(UserInput::text("cut turn")).unwrap(),
        Site::Internal => internal_turn(),
        #[cfg(feature = "external-agent")]
        Site::ExternalInternal => internal_turn(),
    }
}

/// At every acceptance site, a host `persist()` issued while the acceptance
/// save is in flight must not capture the speculative state: the save then
/// fails, and a published snapshot would sit ahead of the protected terminal
/// checkpoint. The persist future is polled once while the save blocks.
#[tokio::test]
async fn a_persist_racing_a_failing_acceptance_at_every_site_publishes_no_speculative_history() {
    let mut failing = Vec::new();
    for site in sites() {
        let sessions = Arc::new(Sessions::default());
        let checkpoints = Arc::new(Checkpoints::default());
        let id = SessionId::new(format!("failed-acceptance-race-{site:?}"));
        let session = site_runtime(site, &sessions, &checkpoints)
            .start_session(StartSession::create(id.clone(), Vec::new()))
            .await
            .unwrap();
        complete(&session, "first turn").await.unwrap();
        session
            .inject(InjectedContent::text(INJECTION).must_deliver())
            .unwrap();
        checkpoints.arm(LegacyFault::BlockThenReject);
        let turn = start_site_turn(site, &session);
        checkpoints.entered.notified().await;
        let persist = session.persist();
        tokio::pin!(persist);
        let early = futures_util::poll!(persist.as_mut());
        checkpoints.release.notify_one();
        turn.completed().await;
        match early {
            Poll::Ready(result) => result.unwrap(),
            Poll::Pending => persist.await.unwrap(),
        }
        let heads_agree = checkpoints.stored().state.is_terminal()
            && sessions.history_len() == Some(checkpoints.stored().snapshot.history.len());
        session.shutdown().await.unwrap();
        println!("FAILED-ACCEPTANCE-RACE {site:?} heads_agree={heads_agree}");
        if !heads_agree {
            failing.push(format!("{site:?}"));
        }
    }
    assert!(
        failing.is_empty(),
        "racing persist published speculative state at {failing:?}"
    );
}

/// At each of the four acceptance sites (direct and external, user and
/// internal turns), a failed acceptance the store proves absent returns the
/// live session to its committed boundary: the drained must-deliver
/// injection goes back to the queue, a persist publishes the boundary the
/// protected head holds, and the next turn delivers the injection once.
#[tokio::test]
async fn every_acceptance_site_rolls_back_a_proven_failure_and_requeues_must_deliver_content() {
    let mut failing = Vec::new();
    for site in sites() {
        let sessions = Arc::new(Sessions::default());
        let checkpoints = Arc::new(Checkpoints::default());
        let id = SessionId::new(format!("failed-acceptance-site-{site:?}"));
        let session = site_runtime(site, &sessions, &checkpoints)
            .start_session(StartSession::create(id.clone(), Vec::new()))
            .await
            .unwrap();
        complete(&session, "first turn").await.unwrap();
        session
            .inject(InjectedContent::text(INJECTION).must_deliver())
            .unwrap();
        let before = session.history();
        checkpoints.arm(LegacyFault::Rejected);
        start_site_turn(site, &session).completed().await;
        let rolled_back = session.history() == before;
        session.persist().await.unwrap();
        let heads_agree = checkpoints.stored().state.is_terminal()
            && sessions.history_len() == Some(checkpoints.stored().snapshot.history.len());
        let next = complete(&session, "next turn").await;
        let delivered = count(&session.history(), INJECTION);
        session.shutdown().await.unwrap();
        let resumed = site_runtime(site, &sessions, &checkpoints)
            .start_session(StartSession::resume(id))
            .await;
        let resumed_ok = resumed.is_ok();
        if let Ok(resumed) = resumed {
            resumed.shutdown().await.unwrap();
        }
        println!(
            "FAILED-ACCEPTANCE-SITE {site:?} rolled_back={rolled_back} heads_agree={heads_agree} \
             next={next:?} delivered={delivered} resumed={resumed_ok}"
        );
        if !rolled_back || !heads_agree || next.is_err() || delivered != 1 || !resumed_ok {
            failing.push(format!("{site:?}"));
        }
    }
    assert!(failing.is_empty(), "failing sites {failing:?}");
}

// ---- an unresolved acceptance that did not commit keeps its injections ----

/// The acceptance write never landed, but the store cannot prove it (native:
/// rejected commit, then a failed renew; legacy: rejected save, then a failed
/// read-back). The fenced session cannot deliver the drained must-deliver
/// batch and a reopen loads the previous boundary with an empty queue, so
/// the batch is handed to the host and restored on the reopened session,
/// which requeues it because its protected head proves the acceptance
/// absent. It is delivered exactly once, and survives another reopen once.
#[tokio::test]
async fn an_unresolved_acceptance_that_did_not_commit_restores_its_injections_once() {
    // Native, direct user turn.
    let native = Native::new("reject-renew-fails");
    let id = SessionId::new("failed-acceptance-reject-renew-fails");
    let session = native
        .runtime(replies(4))
        .start_session(StartSession::create(id.clone(), Vec::new()))
        .await
        .unwrap();
    complete(&session, "first turn").await.unwrap();
    session
        .inject(InjectedContent::text(INJECTION).must_deliver())
        .unwrap();
    native.protected.arm(NativeFault::RejectRenewFails);
    session.run(UserInput::text("cut turn")).await.unwrap();
    let unresolved = session
        .take_unresolved_injections()
        .expect("the fenced session holds the drained batch");
    assert_eq!(unresolved.len(), 1);
    assert!(
        session.persist().await.is_err(),
        "a fenced session published"
    );
    assert!(session.shutdown().await.is_err());
    assert!(native.protected_head(&id).await.state.is_terminal());
    let resumed = native
        .runtime(replies(2))
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    assert_eq!(count(&resumed.history(), INJECTION), 0);
    assert_eq!(
        resumed.restore_unresolved_injections(unresolved).unwrap(),
        InjectionRecovery::Requeued { items: 1 }
    );
    complete(&resumed, "after reopen").await.unwrap();
    assert_eq!(count(&resumed.history(), INJECTION), 1);
    assert_eq!(count(&resumed.history(), "cut turn"), 0);
    resumed.shutdown().await.unwrap();
    let again = native
        .runtime(replies(1))
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert_eq!(count(&again.history(), INJECTION), 1);
    again.shutdown().await.unwrap();

    // Legacy, every acceptance site.
    let mut failing = Vec::new();
    for site in sites() {
        let sessions = Arc::new(Sessions::default());
        let checkpoints = Arc::new(Checkpoints::default());
        let id = SessionId::new(format!("failed-acceptance-unresolved-{site:?}"));
        let session = site_runtime(site, &sessions, &checkpoints)
            .start_session(StartSession::create(id.clone(), Vec::new()))
            .await
            .unwrap();
        complete(&session, "first turn").await.unwrap();
        session
            .inject(InjectedContent::text(INJECTION).must_deliver())
            .unwrap();
        checkpoints.arm(LegacyFault::RejectedUnreadable);
        start_site_turn(site, &session).completed().await;
        let unresolved = session.take_unresolved_injections();
        let fenced = session.persist().await.is_err();
        let _ = session.shutdown().await;
        let resumed = site_runtime(site, &sessions, &checkpoints)
            .start_session(StartSession::resume(id))
            .await
            .unwrap();
        let recovery = unresolved.map(|batch| resumed.restore_unresolved_injections(batch));
        let next = complete(&resumed, "after reopen").await;
        let delivered = count(&resumed.history(), INJECTION);
        resumed.shutdown().await.unwrap();
        println!(
            "UNRESOLVED-NOT-COMMITTED {site:?} fenced={fenced} recovery={recovery:?} \
             next={next:?} delivered={delivered}"
        );
        if !fenced
            || !matches!(recovery, Some(Ok(InjectionRecovery::Requeued { items: 1 })))
            || next.is_err()
            || delivered != 1
        {
            failing.push(format!("{site:?}"));
        }
    }
    assert!(failing.is_empty(), "failing sites {failing:?}");
}
