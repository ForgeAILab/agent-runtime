//! Bounded, resumable collection.
//!
//! One call holds the write gate for at most a cooperative budget of bounded
//! work units. Progress lives in a private, process-local, disposable
//! [`CollectionCursor`] shared by the engine's clones. Durable dirty marks,
//! session generations, pins/catalog, membership and acknowledged writes
//! remain the restart source of truth: losing the cursor loses progress, not
//! safety.
//!
//! Safety across yields:
//! - A session releases nothing until its complete live union (head closure,
//!   resolved pin/catalog roots and their closures, live lease roots) has been
//!   marked under one stored fence, and its entire membership has been
//!   inventoried under that same fence.
//! - Every resumed call re-loads the session record and discards the mark if
//!   its fence changed (every root, pin, staging, retirement and dependency
//!   change writes the session). Work on other sessions never restarts it.
//! - Every release batch is one atomic fenced apply. After an acknowledged
//!   collector-owned apply the cursor adopts exactly the fence it wrote; any
//!   other writer makes the next apply fail its fence instead.
//! - Lease roots are re-checked under the gate before every apply: a lease
//!   registered after the mark protects its whole closure from that batch.
//!   Close, renew and revocation only remove protection.
use super::controls::CollectionFailures;
use super::persistence::{CatalogMetadata, Metadata, PinMetadata, decode, invalid_state};
use super::*;
use crate::error::UnsupportedCapability;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Additional cursor workspace per engine (owner decision, separate from memo).
pub(super) const CURSOR_CAP_BYTES: usize = 16 * 1024 * 1024;
/// Dirty-session ids per discovery page.
const DISCOVERY_PAGE: usize = 32;
/// Membership releases per atomic apply. One batch per session keeps physical
/// receipts identical to the unbounded collector (pack dead-byte thresholds
/// are evaluated per apply); the preflight splits it when storage cannot fit.
const RELEASE_BATCH: usize = 20_000;
/// Estimated cursor bytes per retained id (64-hex id, node and vector overhead).
const ID_CHARGE: usize = 128;

#[derive(Debug, Clone, Copy)]
enum Unit {
    Discover,
    Load,
    PinPage,
    PinChain,
    Trace,
    Leases,
    Inventory,
    Dependency,
    Release,
}
const UNIT_KINDS: usize = 9;

#[derive(Debug, Default)]
pub(super) struct CollectionState {
    pub(super) cursor: Option<CollectionCursor>,
    pub(super) cap: usize,
    // Largest charge of a mark skipped over the cap (test observability of
    // how far one unit can overshoot before the in-unit check stops it).
    pub(super) peak_skip_charge: usize,
}
impl CollectionState {
    pub(super) fn new() -> Self {
        Self {
            cursor: None,
            cap: CURSOR_CAP_BYTES,
            peak_skip_charge: 0,
        }
    }
    #[cfg(feature = "test-support")]
    pub(super) fn charge(&self) -> usize {
        self.cursor
            .as_ref()
            .and_then(|cursor| cursor.mark.as_ref())
            .map_or(0, SessionMark::charge)
    }
}

#[derive(Debug)]
pub(super) struct CollectionCursor {
    // Highest in-order session finished in this pass.
    after: Option<SessionId>,
    queue: VecDeque<SessionId>,
    // Sessions whose mark restarted; retried after the current page so one
    // churning session never stalls the pass.
    deferred: VecDeque<SessionId>,
    // Consecutive calls that started no unit.
    zero_work: u64,
    // A session of this pass stopped at the release budget with garbage
    // left (durable dirty flag only): the pass ends with `has_more`.
    partial: bool,
    mark: Option<SessionMark>,
    costs: [Duration; UNIT_KINDS],
    // Sessions skipped by a failure earlier in this pass; their retained
    // hints do not by themselves request another pass.
    failed: BTreeSet<SessionId>,
}

#[derive(Debug, Clone)]
enum Frontier {
    Edge(JournalObjectId, ExpectedObject),
    PinRoot(JournalObjectId),
}

#[derive(Debug)]
struct PinChain {
    writer_epochs: BTreeSet<u64>,
    pending: Vec<JournalObjectId>,
    roots: BTreeSet<JournalObjectId>,
    dependencies: DependencyRoots,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Pins,
    Trace,
    Leases,
    Inventory,
    Dependencies,
    Release,
}

#[derive(Debug)]
struct SessionMark {
    session: SessionId,
    stored: Option<JournalSessionRecord>,
    fence: JournalFence,
    phase: Phase,
    // Validated (id, expected kind) pairs; their ids are live.
    visited: BTreeMap<JournalObjectId, Vec<ExpectedObject>>,
    // Validated non-head pin roots and live lease roots.
    extra: BTreeSet<JournalObjectId>,
    frontier: Vec<Frontier>,
    dependencies: BTreeSet<SessionId>,
    pins_done: bool,
    pin_after: Option<JournalCollectionContinuation>,
    pins: VecDeque<JournalPinRecord>,
    pin_bytes: usize,
    chain: Option<PinChain>,
    catalog: BTreeMap<JournalObjectId, Option<CatalogMetadata>>,
    members_started: bool,
    member_after: Option<JournalCollectionContinuation>,
    live_members: usize,
    garbage: VecDeque<JournalObjectId>,
    dep_queue: VecDeque<SessionId>,
    // This session's lease ids when its lease roots were added to the mark.
    leases: Option<BTreeSet<u64>>,
}

impl SessionMark {
    fn new(session: SessionId, stored: Option<JournalSessionRecord>) -> Self {
        let fence = stored
            .as_ref()
            .map_or(JournalFence::default(), JournalSessionRecord::fence);
        Self {
            session,
            stored,
            fence,
            phase: Phase::Pins,
            visited: BTreeMap::new(),
            extra: BTreeSet::new(),
            frontier: Vec::new(),
            dependencies: BTreeSet::new(),
            pins_done: false,
            pin_after: None,
            pins: VecDeque::new(),
            pin_bytes: 0,
            chain: None,
            catalog: BTreeMap::new(),
            members_started: false,
            member_after: None,
            live_members: 0,
            garbage: VecDeque::new(),
            dep_queue: VecDeque::new(),
            leases: None,
        }
    }
    fn charge(&self) -> usize {
        let chain = self
            .chain
            .as_ref()
            .map_or(0, |chain| chain.pending.len() + chain.roots.len());
        (self.visited.len()
            + self.extra.len()
            + self.frontier.len()
            + self.catalog.len()
            + self.garbage.len()
            + self.dep_queue.len()
            + self.dependencies.len()
            + chain)
            * ID_CHARGE
            + self.visited.values().map(Vec::len).sum::<usize>() * 8
            + self.pin_bytes
    }
    fn live(&self, id: &JournalObjectId) -> bool {
        self.visited.contains_key(id) || self.extra.contains(id)
    }
    fn live_count(&self) -> usize {
        self.visited.len()
            + self
                .extra
                .iter()
                .filter(|id| !self.visited.contains_key(*id))
                .count()
    }
}

// Per-call accounting and budget.
struct Call {
    started: Instant,
    target: Duration,
    max_units: u64,
    unit_items: usize,
    cap: usize,
    units: u64,
    released: u64,
    restarted: bool,
    peak_skip_charge: usize,
    // Sessions restarted in this call; a second restart skips them.
    restarts: BTreeSet<SessionId>,
    over_capacity: BTreeSet<SessionId>,
}
impl Call {
    fn admit(&self, kind: Unit, costs: &[Duration; UNIT_KINDS]) -> bool {
        if self.units >= self.max_units {
            return false;
        }
        // Reserve the measured cost of the next unit of this kind. Once the
        // gate is held the first unit always starts, even if the gate wait
        // consumed the target, so a call under sustained contention still
        // progresses; its hold is then that one bounded unit.
        self.units == 0 || self.started.elapsed() + costs[kind as usize] <= self.target
    }
}

// Restores the hints a call took if its future is dropped mid-call.
struct HintGuard<'a> {
    dirty: &'a Mutex<BTreeSet<SessionId>>,
    taken: Option<BTreeSet<SessionId>>,
}
impl Drop for HintGuard<'_> {
    fn drop(&mut self) {
        if let Some(taken) = self.taken.take() {
            if let Ok(mut dirty) = self.dirty.lock() {
                dirty.extend(taken);
            }
        }
    }
}

// Reserve a decaying maximum of a unit kind's observed cost, so one cheap
// unit does not hide the cost of an occasional large one of the same kind.
fn observe(costs: &mut [Duration; UNIT_KINDS], kind: Unit, elapsed: Duration) {
    let slot = &mut costs[kind as usize];
    *slot = elapsed.max(*slot * 3 / 4);
}

enum Step {
    Continue,
    Yield,
    Complete,
}

fn unsupported() -> RuntimeError {
    RuntimeError::unsupported_capability(UnsupportedCapability::JournalCollection)
}

// A plain state conflict from a fence-bound page under an unchanged fence:
// the adapter rejected a continuation after a physical change.
pub(super) fn page_restart(error: &RuntimeError) -> bool {
    !error.retryable
        && error.metadata.get("journal_invalid_state").is_none()
        && matches!(
            error.class,
            crate::error::FailureClass::StateConflict { .. }
        )
}

fn over_capacity() -> RuntimeError {
    let mut error = unsupported();
    error
        .metadata
        .insert("journal_collection_cursor_capacity", true);
    error
}

fn contract_violation() -> RuntimeError {
    let mut error = unsupported();
    error.metadata.insert("journal_collection_contract", true);
    error
}

impl<S: JournalStorage> JournalEngine<S> {
    pub(super) async fn collect_inner(
        &self,
        request: JournalGcRequest,
    ) -> Result<JournalGcReport, RuntimeError> {
        crate::journal_profile_scope!("rt.controls.collect_inner", {
            let started = Instant::now();
            let budget = request.work_budget;
            let target = Duration::from_millis(
                budget
                    .target_millis
                    .clamp(1, JournalGcBudget::DEFAULT_TARGET_MILLIS),
            );
            let _gate = self.write_gate().await;
            #[cfg(feature = "test-support")]
            let _profile_gate = crate::journal::profile::timer("rt.gate.hold");
            {
                let local = self.local.lock()?;
                if !local.collection_enabled {
                    return Err(unsupported());
                }
                if request.storage_id != self.domain.storage_id
                    || request.expected_root_epoch != local.root_epoch
                {
                    return Err(conflict());
                }
            }
            if self.storage.collection_capability() == JournalCollectionCapability::Unsupported {
                return Err(unsupported());
            }
            if self
                .local
                .lock()?
                .leases
                .values()
                .any(|lease| lease.dependencies.unresolved)
            {
                return Err(unsupported());
            }
            let mut report = JournalGcReport {
                root_epoch: request.expected_root_epoch,
                examined: 0,
                deleted: 0,
                reclaimed_bytes: 0,
                unreadable_sessions: Vec::new(),
                progress: Some(JournalGcProgress::Yielded),
                has_more: true,
                work_units: 0,
                zero_work_calls: 0,
                over_capacity_sessions: Vec::new(),
            };
            let (mut kept, cap) = {
                let mut state = self.collection.lock().map_err(|_| host_failure())?;
                (state.cursor.take(), state.cap)
            };
            let mut call = Call {
                started,
                target,
                max_units: budget.max_work_units.max(1),
                unit_items: budget
                    .max_unit_items
                    .clamp(1, JournalGcBudget::MAX_UNIT_ITEMS) as usize,
                cap,
                units: 0,
                released: 0,
                restarted: false,
                peak_skip_charge: 0,
                restarts: BTreeSet::new(),
                over_capacity: BTreeSet::new(),
            };
            let cursor = kept.get_or_insert_with(|| CollectionCursor {
                after: None,
                queue: VecDeque::new(),
                deferred: VecDeque::new(),
                zero_work: 0,
                partial: false,
                mark: None,
                costs: [Duration::ZERO; UNIT_KINDS],
                failed: BTreeSet::new(),
            });
            // Close and revocation do no I/O. Drain their local dirty hints under
            // the registry lock order; concurrent changes insert fresh hints and
            // therefore cannot be erased by this call. A dropped call restores them.
            let taken = std::mem::take(&mut *self.dirty.lock().map_err(|_| host_failure())?);
            let mut guard = HintGuard {
                dirty: &self.dirty,
                taken: Some(taken.clone()),
            };
            let mut local_pending = taken.clone();
            if let Some(mark) = &cursor.mark {
                local_pending.remove(&mark.session);
            }
            let mut failures = CollectionFailures::default();
            let result = self
                .collect_steps(
                    &request,
                    cursor,
                    &mut call,
                    &mut report,
                    &mut local_pending,
                    &mut failures,
                )
                .await;
            guard.taken = None;
            {
                let mut dirty = self.dirty.lock().map_err(|_| host_failure())?;
                // Keep even local-only and dependency-discovered hints, including
                // conflicts. A later abort must preserve these as well as the input.
                dirty.extend(failures.skipped.iter().cloned());
                dirty.extend(local_pending);
                if let Some(mark) = &cursor.mark {
                    dirty.insert(mark.session.clone());
                }
                match &result {
                    Ok(true) => {
                        report.progress = Some(JournalGcProgress::Complete);
                        // A session stopped by the release budget with garbage
                        // left keeps its durable dirty flag only; ask for a pass.
                        report.has_more = cursor.partial
                            || dirty.iter().any(|s| {
                                !failures.skipped.contains(s) && !cursor.failed.contains(s)
                            });
                        // A completed pass keeps only its epoch and measured unit costs.
                        cursor.after = None;
                        cursor.queue.clear();
                        cursor.mark = None;
                        cursor.deferred.clear();
                        cursor.failed.clear();
                        cursor.partial = false;
                    }
                    Ok(false) => {
                        cursor.failed.extend(failures.skipped.iter().cloned());
                        report.progress = Some(if call.restarted {
                            JournalGcProgress::Restarted
                        } else {
                            JournalGcProgress::Yielded
                        });
                        report.has_more = true;
                    }
                    Err(_) => {
                        // Speculative progress is disposable; durable state is not.
                        dirty.extend(taken);
                        kept = None;
                    }
                }
            }
            if let Some(cursor) = kept.as_mut() {
                cursor.zero_work = if call.units == 0 {
                    cursor.zero_work + 1
                } else {
                    0
                };
                report.zero_work_calls = cursor.zero_work;
            }
            {
                let mut state = self.collection.lock().map_err(|_| host_failure())?;
                state.cursor = kept;
                state.peak_skip_charge = state.peak_skip_charge.max(call.peak_skip_charge);
            }
            result?;
            report.work_units = call.units;
            report.unreadable_sessions = failures.unreadable.iter().cloned().collect();
            report.over_capacity_sessions = call.over_capacity.iter().cloned().collect();
            Ok(report)
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn collect_steps(
        &self,
        request: &JournalGcRequest,
        cursor: &mut CollectionCursor,
        call: &mut Call,
        report: &mut JournalGcReport,
        local_pending: &mut BTreeSet<SessionId>,
        failures: &mut CollectionFailures,
    ) -> Result<bool, RuntimeError> {
        crate::journal_profile_scope!("rt.controls.collect_sessions", {
            // A resumed mark is authenticated against the stored fence first.
            // This prerequisite read is not a unit of new progress, so a
            // one-unit budget still advances; the gate wait already passed the
            // time check.
            if cursor.mark.is_some() {
                let session = cursor.mark.as_ref().expect("mark").session.clone();
                let started = Instant::now();
                let loaded = self.storage.load(&self.domain, &session).await;
                observe(&mut cursor.costs, Unit::Load, started.elapsed());
                match failures.isolate(&session, loaded)? {
                    Some(stored) => {
                        let fence = stored
                            .as_ref()
                            .map_or(JournalFence::default(), JournalSessionRecord::fence);
                        let mark = cursor.mark.as_ref().expect("mark");
                        // Only this session's own changes invalidate its mark:
                        // a write (fence) or a lease opened/closed since its
                        // lease roots were taken. Added leases are also
                        // filtered before every apply; a closed one would only
                        // keep garbage until the next pass.
                        let leases_changed = mark.leases.as_ref().is_some_and(|taken| {
                            self.local
                                .lock()
                                .is_ok_and(|local| Self::lease_ids(&local, &session) != *taken)
                        });
                        if fence != mark.fence || leases_changed {
                            cursor.mark = None;
                            Self::restart(cursor, call, failures, session);
                        }
                    }
                    None => {
                        cursor.mark = None;
                        Self::advance(cursor, session);
                    }
                }
            }
            loop {
                let step = if cursor.mark.is_some() {
                    self.mark_step(request, cursor, call, report, failures)
                        .await?
                } else {
                    self.next_session(request, cursor, call, local_pending, failures)
                        .await?
                };
                if let Some(mark) = &cursor.mark {
                    if mark.charge() > call.cap {
                        let session = mark.session.clone();
                        Self::skip_over_capacity(cursor, call, failures, session);
                    }
                }
                match step {
                    Step::Continue => {}
                    Step::Yield => return Ok(false),
                    Step::Complete => return Ok(true),
                }
            }
        })
    }

    // Discover and start the next dirty session of this pass.
    async fn next_session(
        &self,
        request: &JournalGcRequest,
        cursor: &mut CollectionCursor,
        call: &mut Call,
        local_pending: &mut BTreeSet<SessionId>,
        failures: &mut CollectionFailures,
    ) -> Result<Step, RuntimeError> {
        if cursor.queue.is_empty() {
            if let Some(session) = cursor.deferred.pop_front() {
                cursor.queue.push_back(session);
                return Ok(Step::Continue);
            }
            if !call.admit(Unit::Discover, &cursor.costs) {
                return Ok(Step::Yield);
            }
            let started = Instant::now();
            call.units += 1;
            let page = self
                .storage
                .dirty_sessions(&self.domain, cursor.after.as_ref(), DISCOVERY_PAGE)
                .await?;
            observe(&mut cursor.costs, Unit::Discover, started.elapsed());
            if page.len() > DISCOVERY_PAGE {
                return Err(contract_violation());
            }
            let last = (page.len() == DISCOVERY_PAGE)
                .then(|| page.last().cloned())
                .flatten();
            let mut sessions: BTreeSet<_> = page
                .into_iter()
                .filter(|s| cursor.after.as_ref().is_none_or(|after| s > after))
                .collect();
            // Merge process-local changes in the same session-id order as the
            // persistent cursor. Local-only sessions beyond a full page wait.
            sessions.extend(
                local_pending
                    .iter()
                    .filter(|s| cursor.after.as_ref().is_none_or(|after| *s > after))
                    .filter(|s| last.as_ref().is_none_or(|end| *s <= end))
                    .cloned(),
            );
            if sessions.is_empty() {
                return Ok(Step::Complete);
            }
            cursor.queue.extend(sessions);
            return Ok(Step::Continue);
        }
        // Never read a later session once the membership-release budget has
        // been spent. Its persistent/local dirty marker survives.
        if call.released == request.max_objects && call.released != 0 {
            return Ok(Step::Yield);
        }
        if !call.admit(Unit::Load, &cursor.costs) {
            return Ok(Step::Yield);
        }
        let session = cursor.queue.pop_front().expect("queued session");
        local_pending.remove(&session);
        // A dependency-index read may already have failed for this session.
        // Do not load it again within the same call.
        if failures.skipped.contains(&session) {
            Self::advance(cursor, session);
            return Ok(Step::Continue);
        }
        let started = Instant::now();
        call.units += 1;
        let read = async {
            let stored = self.storage.load(&self.domain, &session).await?;
            let mut mark = SessionMark::new(session.clone(), stored);
            if let Some(stored) = &mark.stored {
                let metadata: Metadata = decode(&stored.engine)?;
                if let Some(head) = &stored.head {
                    if metadata.dependencies.unresolved {
                        return Err(unsupported());
                    }
                    mark.dependencies.extend(metadata.dependencies.sessions);
                    mark.frontier.push(Frontier::Edge(
                        head.id.clone(),
                        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Head)),
                    ));
                }
            }
            Ok(mark)
        }
        .await;
        observe(&mut cursor.costs, Unit::Load, started.elapsed());
        match failures.isolate(&session, read)? {
            Some(mark) => cursor.mark = Some(mark),
            None => Self::advance(cursor, session),
        }
        Ok(Step::Continue)
    }

    // The discovery position only moves forward; a deferred (restarted)
    // session finishing later never re-opens earlier pages.
    fn advance(cursor: &mut CollectionCursor, session: SessionId) {
        if cursor.after.as_ref().is_none_or(|after| &session > after) {
            cursor.after = Some(session);
        }
    }

    fn finish_session(cursor: &mut CollectionCursor) {
        if let Some(mark) = cursor.mark.take() {
            Self::advance(cursor, mark.session);
        }
    }

    // Discard a stale speculative mark and retry the session after the
    // current page. A second restart within one call skips it for the call
    // (its dirty state and hint survive).
    fn restart(
        cursor: &mut CollectionCursor,
        call: &mut Call,
        failures: &mut CollectionFailures,
        session: SessionId,
    ) {
        call.restarted = true;
        if call.restarts.insert(session.clone()) {
            cursor.deferred.push_back(session);
        } else {
            failures.skipped.insert(session.clone());
            Self::advance(cursor, session);
        }
    }

    // A mark that cannot fit the cursor cap skips only its own session: its
    // durable and local dirty state survive and it is reported, while other
    // sessions keep progressing.
    fn skip_over_capacity(
        cursor: &mut CollectionCursor,
        call: &mut Call,
        failures: &mut CollectionFailures,
        session: SessionId,
    ) {
        if let Some(mark) = cursor.mark.take() {
            call.peak_skip_charge = call.peak_skip_charge.max(mark.charge());
        }
        failures.skipped.insert(session.clone());
        call.over_capacity.insert(session.clone());
        Self::advance(cursor, session);
    }

    async fn mark_step(
        &self,
        request: &JournalGcRequest,
        cursor: &mut CollectionCursor,
        call: &mut Call,
        report: &mut JournalGcReport,
        failures: &mut CollectionFailures,
    ) -> Result<Step, RuntimeError> {
        let phase = cursor.mark.as_ref().expect("mark").phase;
        let kind = match phase {
            Phase::Pins => {
                let mark = cursor.mark.as_ref().expect("mark");
                if mark.chain.is_some() || !mark.pins.is_empty() {
                    Unit::PinChain
                } else {
                    Unit::PinPage
                }
            }
            Phase::Trace => Unit::Trace,
            Phase::Leases => Unit::Leases,
            Phase::Inventory => Unit::Inventory,
            Phase::Dependencies => Unit::Dependency,
            Phase::Release => Unit::Release,
        };
        if !call.admit(kind, &cursor.costs) {
            return Ok(Step::Yield);
        }
        // No epoch check here: close, renew and revocation (the only changes
        // possible under the held gate) only remove protection, any write to
        // this session fails the batch's fence, and the lease filter runs
        // before every apply.
        let started = Instant::now();
        call.units += 1;
        let session = cursor.mark.as_ref().expect("mark").session.clone();
        let result = match phase {
            Phase::Pins => self.pin_unit(cursor, call).await,
            Phase::Trace => self.trace_unit(cursor, call).await,
            Phase::Leases => self.lease_unit(cursor),
            Phase::Inventory => self.inventory_unit(cursor, call, report).await,
            Phase::Dependencies => self.dependency_unit(cursor, failures).await,
            Phase::Release => self.release_unit(request, cursor, call, report).await,
        };
        observe(&mut cursor.costs, kind, started.elapsed());
        match result {
            Err(error) if error.metadata.get("journal_collection_contract").is_some() => Err(error),
            Err(error)
                if error
                    .metadata
                    .get("journal_collection_cursor_capacity")
                    .is_some() =>
            {
                Self::skip_over_capacity(cursor, call, failures, session);
                Ok(Step::Continue)
            }
            // A page conflict under an unchanged fence is a physical change
            // (repack/compaction) that rejected the continuation: restart the
            // session's mark rather than skipping it for the pass.
            Err(error)
                if matches!(phase, Phase::Pins | Phase::Inventory) && page_restart(&error) =>
            {
                cursor.mark = None;
                Self::restart(cursor, call, failures, session);
                Ok(Step::Continue)
            }
            result => {
                if failures.isolate(&session, result)?.is_none() {
                    cursor.mark = None;
                    Self::advance(cursor, session);
                }
                Ok(Step::Continue)
            }
        }
    }

    async fn page(
        &self,
        mark: &SessionMark,
        kind: JournalCollectionPageKind,
        after: Option<JournalCollectionContinuation>,
        max_items: usize,
    ) -> Result<JournalCollectionPage, RuntimeError> {
        let max_items = u32::try_from(max_items).unwrap_or(u32::MAX);
        let page = self
            .storage
            .collection_page(
                &self.domain,
                &mark.session,
                JournalCollectionPageRequest {
                    kind,
                    fence: mark.fence,
                    after,
                    max_items,
                },
            )
            .await?;
        let kind_matches = matches!(
            (kind, &page.items),
            (
                JournalCollectionPageKind::Members,
                JournalCollectionItems::Members(_)
            ) | (
                JournalCollectionPageKind::Pins,
                JournalCollectionItems::Pins(_)
            )
        );
        if page.fence != mark.fence || page.items.len() > max_items as usize || !kind_matches {
            return Err(contract_violation());
        }
        Ok(page)
    }

    async fn pin_unit(
        &self,
        cursor: &mut CollectionCursor,
        call: &Call,
    ) -> Result<(), RuntimeError> {
        let mark = cursor.mark.as_mut().expect("mark");
        if mark.chain.is_none() {
            if let Some(pin) = mark.pins.pop_front() {
                let metadata: PinMetadata = decode(&pin.engine)?;
                let mut pending = pin.roots.clone();
                if let Some(record) = &mark.stored {
                    if metadata.writer_epochs.contains(&record.writer_epoch) {
                        if let Some(head) = &record.head {
                            // Issuing an epoch at open does not mean it published.
                            // Only follow that epoch's own tip; selected roots are
                            // already recorded in the pin.
                            if head
                                .decode::<JournalHead>(JournalObjectKind::Head)
                                .map_err(invalid_state)?
                                .writer_epoch
                                == record.writer_epoch
                            {
                                pending.push(head.id.clone());
                            }
                        }
                    }
                }
                mark.chain = Some(PinChain {
                    writer_epochs: metadata.writer_epochs,
                    pending,
                    roots: BTreeSet::new(),
                    dependencies: DependencyRoots {
                        sessions: metadata.dependencies.sessions,
                        unresolved: metadata.dependencies.unresolved,
                        ..Default::default()
                    },
                });
                return Ok(());
            }
            if mark.pins_done {
                mark.phase = Phase::Trace;
                return Ok(());
            }
            let after = mark.pin_after.take();
            let page = self
                .page(
                    mark,
                    JournalCollectionPageKind::Pins,
                    after,
                    call.unit_items,
                )
                .await?;
            let mark = cursor.mark.as_mut().expect("mark");
            mark.pin_bytes += usize::try_from(page.bytes).unwrap_or(usize::MAX);
            if let JournalCollectionItems::Pins(pins) = page.items {
                mark.pins.extend(pins);
            }
            mark.pin_after = page.next;
            mark.pins_done = mark.pin_after.is_none();
            return Ok(());
        }
        // Resolve compact pin roots, including an active writer's published
        // chain: at most `unit_items` catalog lookups per unit.
        let mut steps = 0;
        loop {
            let mark = cursor.mark.as_mut().expect("mark");
            let chain = mark.chain.as_mut().expect("chain");
            let Some(root) = chain.pending.pop() else {
                let chain = mark.chain.take().expect("chain");
                if chain.dependencies.unresolved {
                    return Err(unsupported());
                }
                mark.dependencies.extend(chain.dependencies.sessions);
                mark.frontier
                    .extend(chain.roots.into_iter().map(Frontier::PinRoot));
                return Ok(());
            };
            if !chain.roots.insert(root.clone()) {
                continue;
            }
            if steps == call.unit_items {
                chain.roots.remove(&root);
                chain.pending.push(root);
                return Ok(());
            }
            if !mark.catalog.contains_key(&root) {
                steps += 1;
                let session = mark.session.clone();
                let entry = self.storage.catalog(&self.domain, &session, &root).await?;
                let mark = cursor.mark.as_mut().expect("mark");
                mark.catalog.insert(
                    root.clone(),
                    entry.map(|entry| decode(&entry.engine)).transpose()?,
                );
            }
            let mark = cursor.mark.as_mut().expect("mark");
            let chain = mark.chain.as_mut().expect("chain");
            if let Some(entry) = &mark.catalog[&root] {
                chain.dependencies.extend(&entry.dependencies);
                if entry
                    .writer_epoch
                    .is_some_and(|epoch| chain.writer_epochs.contains(&epoch))
                {
                    chain.pending.extend(entry.previous.iter().cloned());
                    chain.pending.extend(entry.staged_roots.iter().cloned());
                }
            }
        }
    }

    // One bounded batch of typed edges from the explicit frontier. Each new
    // (id, expected kind) is validated exactly as the full tracer does.
    async fn trace_unit(
        &self,
        cursor: &mut CollectionCursor,
        call: &Call,
    ) -> Result<(), RuntimeError> {
        let mark = cursor.mark.as_mut().expect("mark");
        let mut batch = Vec::new();
        let mut ids = BTreeSet::new();
        while batch.len() < call.unit_items {
            let Some(item) = mark.frontier.pop() else {
                break;
            };
            let fresh = match &item {
                Frontier::Edge(id, expected) => mark
                    .visited
                    .get(id)
                    .is_none_or(|seen| !seen.contains(expected)),
                Frontier::PinRoot(id) => {
                    !mark.extra.contains(id)
                        && mark.visited.get(id).is_none_or(|seen| {
                            !seen.contains(&ExpectedObject::Leaf(ExpectedLeaf::Kind(
                                JournalObjectKind::Head,
                            )))
                        })
                }
            };
            if fresh {
                let id = match &item {
                    Frontier::Edge(id, _) | Frontier::PinRoot(id) => id.clone(),
                };
                ids.insert(id);
                batch.push(item);
            }
        }
        if batch.is_empty() {
            mark.phase = Phase::Leases;
            return Ok(());
        }
        let ids: Vec<_> = ids.into_iter().collect();
        let session = mark.session.clone();
        let bodies = self.storage.objects(&self.domain, &session, &ids).await?;
        if bodies.len() != ids.len() {
            return Err(conflict());
        }
        let mut objects = BTreeMap::new();
        for (id, body) in ids.into_iter().zip(bodies) {
            objects.insert(id, body.ok_or_else(conflict)?);
        }
        let mark = cursor.mark.as_mut().expect("mark");
        for item in batch {
            match item {
                Frontier::Edge(id, expected) => {
                    if mark
                        .visited
                        .get(&id)
                        .is_some_and(|seen| seen.contains(&expected))
                    {
                        continue;
                    }
                    let object = JournalObject {
                        id: id.clone(),
                        bytes: objects[&id].to_vec(),
                    };
                    if !expected_matches(expected, object.kind().map_err(invalid_state)?) {
                        return Err(invalid_state(conflict()));
                    }
                    let links = object_links(&object, expected).map_err(invalid_state)?;
                    mark.frontier
                        .extend(links.into_iter().map(|(id, e)| Frontier::Edge(id, e)));
                    mark.visited.entry(id).or_default().push(expected);
                    // Bound workspace inside the unit too: one batch of wide
                    // sequence nodes can add far more than the cap.
                    if mark.charge() > call.cap {
                        return Err(over_capacity());
                    }
                }
                Frontier::PinRoot(id) => {
                    let object = JournalObject {
                        id: id.clone(),
                        bytes: objects[&id].to_vec(),
                    };
                    let kind = object.kind().map_err(invalid_state)?;
                    if kind == JournalObjectKind::Head {
                        mark.frontier.push(Frontier::Edge(
                            id,
                            ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Head)),
                        ));
                    } else {
                        validate_leaf(&object, kind).map_err(invalid_state)?;
                        mark.extra.insert(id);
                    }
                }
            }
        }
        Ok(())
    }

    fn lease_ids(local: &super::transition::State, session: &SessionId) -> BTreeSet<u64> {
        local
            .leases
            .iter()
            .filter(|(_, lease)| &lease.session == session)
            .map(|(id, _)| *id)
            .collect()
    }

    fn lease_unit(&self, cursor: &mut CollectionCursor) -> Result<(), RuntimeError> {
        let mark = cursor.mark.as_mut().expect("mark");
        let local = self.local.lock()?;
        mark.leases = Some(Self::lease_ids(&local, &mark.session));
        for lease in local
            .leases
            .values()
            .filter(|lease| lease.session == mark.session)
        {
            for layer in &lease.reachable.layers {
                mark.extra.extend(layer.iter().cloned());
            }
        }
        mark.phase = Phase::Inventory;
        Ok(())
    }

    // One fence-bound membership page. Garbage is computed per page; the
    // subset check runs once the end marker is reached.
    async fn inventory_unit(
        &self,
        cursor: &mut CollectionCursor,
        call: &Call,
        report: &mut JournalGcReport,
    ) -> Result<(), RuntimeError> {
        let mark = cursor.mark.as_mut().expect("mark");
        if mark.members_started && mark.member_after.is_none() {
            if mark.live_members != mark.live_count() {
                return Err(conflict());
            }
            mark.dep_queue = std::mem::take(&mut mark.dependencies).into_iter().collect();
            mark.phase = Phase::Dependencies;
            return Ok(());
        }
        mark.members_started = true;
        let after = mark.member_after.take();
        let page = self
            .page(
                mark,
                JournalCollectionPageKind::Members,
                after,
                call.unit_items,
            )
            .await?;
        let mark = cursor.mark.as_mut().expect("mark");
        if let JournalCollectionItems::Members(ids) = page.items {
            report.examined += ids.len() as u64;
            for id in ids {
                if mark.live(&id) {
                    mark.live_members += 1;
                } else {
                    mark.garbage.push_back(id);
                }
            }
        }
        mark.member_after = page.next;
        Ok(())
    }

    // Before a source becomes clean, persist it as a candidate on every
    // referenced session; one target per unit.
    async fn dependency_unit(
        &self,
        cursor: &mut CollectionCursor,
        failures: &mut CollectionFailures,
    ) -> Result<(), RuntimeError> {
        crate::journal_profile_scope!("rt.collection.dependencies", {
            let mark = cursor.mark.as_mut().expect("mark");
            let source = mark.session.clone();
            let Some(target) = mark.dep_queue.pop_front() else {
                mark.phase = Phase::Release;
                return Ok(());
            };
            if target == source {
                return Ok(());
            }
            if !failures.skipped.contains(&target) {
                let result = Box::pin(self.index_retirement_dependency(&source, &target)).await;
                failures.isolate(&target, result)?;
            }
            if failures.skipped.contains(&target) {
                // Without the candidate index, this source cannot safely
                // become clean. Preserve its own state and retry hint too.
                failures.skipped.insert(source.clone());
                if failures.unreadable.contains(&target) {
                    failures.unreadable.insert(source.clone());
                }
                cursor.mark = None;
                Self::advance(cursor, source);
            }
            Ok(())
        })
    }

    async fn release_unit(
        &self,
        request: &JournalGcRequest,
        cursor: &mut CollectionCursor,
        call: &mut Call,
        report: &mut JournalGcReport,
    ) -> Result<(), RuntimeError> {
        let mark = cursor.mark.as_mut().expect("mark");
        // Lease roots selected since the mark are live: never release them.
        {
            let local = self.local.lock()?;
            let leases: Vec<_> = local
                .leases
                .values()
                .filter(|lease| lease.session == mark.session)
                .collect();
            if !leases.is_empty() {
                mark.garbage.retain(|id| {
                    !leases
                        .iter()
                        .any(|lease| lease.reachable.layers.iter().any(|l| l.contains(id)))
                });
            }
        }
        let limit = usize::try_from(request.max_objects - call.released).unwrap_or(usize::MAX);
        let remaining = mark.garbage.len();
        let mut take = remaining.min(limit).min(RELEASE_BATCH);
        let Some(stored) = mark.stored.clone() else {
            if remaining != 0 {
                return Err(invalid_state(conflict()));
            }
            let session = mark.session.clone();
            self.local.lock()?.sessions.entry(session).or_default();
            Self::finish_session(cursor);
            return Ok(());
        };
        let max_units = match self.storage.collection_capability() {
            JournalCollectionCapability::Bounded(limits) => limits.max_apply_units,
            JournalCollectionCapability::Unsupported => return Err(unsupported()),
        };
        let session = mark.session.clone();
        let plan = loop {
            let release: Vec<_> = mark.garbage.iter().take(take).cloned().collect();
            let complete = take == remaining;
            let mut record = stored.clone();
            let expect = record.fence();
            record.generation = expect.generation.checked_add(1).ok_or_else(conflict)?;
            record.collection_dirty = !complete;
            // Inclusive edge: revision -64 survives, -65 may be pruned. Storage
            // also preserves this writer epoch's last operation. Object
            // retention does not retain replay rows.
            let replay_floor = JournalRevision(record.revision.0.saturating_sub(64));
            let plan = JournalCollectionPlan {
                write: JournalWrite {
                    domain: self.domain.clone(),
                    session: session.clone(),
                    kind: JournalWriteKind::Collect,
                    expect,
                    record,
                    objects: JournalObjectBatch::default(),
                    operation: None,
                    pins: vec![],
                    catalog: release
                        .iter()
                        .cloned()
                        .map(JournalCatalogChange::Remove)
                        .collect(),
                    release,
                    prune_operations_before: Some(replay_floor),
                    inject: None,
                },
                max_units,
            };
            match self.storage.prepare_collection_apply(&plan).await? {
                JournalCollectionPreflight::Fits { units } if units <= max_units => break plan,
                JournalCollectionPreflight::Fits { .. } => return Err(contract_violation()),
                JournalCollectionPreflight::Split { max_release }
                    if (max_release as usize) < take && max_release > 0 =>
                {
                    take = max_release as usize;
                }
                _ => return Err(unsupported()),
            }
        };
        let complete = take == remaining;
        let written = plan.write.record.clone();
        let release = plan.write.release.clone();
        let receipt = self.apply_collection(plan, &session).await?.receipt;
        report.deleted += receipt.reclaimed_objects;
        report.reclaimed_bytes += receipt.reclaimed_bytes;
        let mark = cursor.mark.as_mut().expect("mark");
        mark.fence = written.fence();
        mark.stored = Some(written);
        mark.garbage.drain(..take);
        let mut local = self.local.lock()?;
        local.sessions.entry(session.clone()).or_default();
        if !release.is_empty() {
            if call.released == 0 {
                local.advance_root_epoch()?;
            }
            if let Some(record) = local.sessions.get_mut(&session) {
                for id in &release {
                    record.retained_heads.remove(id);
                }
            }
            call.released += release.len() as u64;
        }
        drop(local);
        if complete || take == limit {
            // Only a budget that released something asks for another pass.
            cursor.partial |= !complete && take > 0;
            Self::finish_session(cursor);
        }
        Ok(())
    }

    async fn apply_collection(
        &self,
        plan: JournalCollectionPlan,
        session: &SessionId,
    ) -> Result<JournalCollectionReceipt, RuntimeError> {
        crate::journal_profile_scope!("rt.engine.storage_apply", {
            let release = plan.write.release.clone();
            self.guarded_apply(session, &release, self.storage.apply_collection(plan))
                .await
        })
    }
}
