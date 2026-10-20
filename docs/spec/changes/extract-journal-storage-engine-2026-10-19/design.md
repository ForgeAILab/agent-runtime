---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-20T03:38:23Z
---

# Design: Storage-generic journal engine and physical disk reduction (R1–R7)

All references are to agent-runtime `main` at `1de6d7d`, read-only. Forge
references are to `next/v0.14` at `78702e7c` (written `forge:<path>:<line>`).
Forge's adoption design is
`forge:docs/spec/changes/adopt-session-journal-2026-10-19/design.md`
(written `Forge D<n>`). The U9 design is
`docs/spec/changes/add-session-journal-2026-10-07/design.md` (written
`U9 design:<line>`). The original design used estimates; task 1 now replaces D8.4 with physical
SQLite measurements. D8.2 retains the historical logical-byte decomposition;
D8.4 is authoritative for physical size and manifest-sharing conclusions.

## Evidence on main

| Fact | File:line |
| --- | --- |
| `SessionJournal` has eight required methods and no storage abstraction. | `crates/agent-runtime-core/src/journal/mod.rs:36-88` |
| The whole engine is private to `InMemorySessionJournal`: one `std::sync::Mutex<State>` holds objects, sessions, leases, pins, epoch, grants and durable pins. | `crates/agent-runtime-core/src/journal/memory.rs:383-394`, `:518-526` |
| Per-session durable metadata lives in `SessionRecord`: current head, last writer epoch, active writer, operation index, retained-head catalog, fast-path snapshot/encoding caches, checkpoint floor, tombstone, dependency index. | `memory.rs:113-130` |
| Leases carry a full reachable-object set; durable pins carry a reachable set, a retirement-blocking flag and dependencies. | `memory.rs:132-148`, `:301-326` |
| A commit clones state, stages objects, checkpoint, batch, pin, head and idempotency in that order (the seven `JournalCommitFailurePoint` cuts) and swaps at one visibility point. | `memory.rs:1238-1509`, `:65-105` |
| A protected commit after a writer change, or without a cached seed, runs full materialization (`materialize_staged_snapshot`). | `memory.rs:1282-1290`, `:1426-1451` |
| Every open traces the selected root's full closure. | `memory.rs:1162-1165`, `:3130-3163` |
| Collection traces every session's current root and every lease/pin in the domain, then deletes by global set difference. | `memory.rs:1636-1707` |
| Collection, retirement, pins, opens, commits, renew, close and revoke each advance the root epoch exactly once. | `memory.rs:406-409`, `:593`, `:603`, `:663`, `:690`, `:737`, `:1211`, `:1505`, `:1529`, `:1558`, `:1618`, `:1690` |
| `close` and `revoke_writer` touch only lease/pin/writer bookkeeping. | `memory.rs:586-595`, `:1543-1560` |
| `SessionPersistence::drop` closes through `now_or_never` and spawns only if close suspends. | `crates/agent-runtime/src/runtime/journal.rs:596-628` |
| `SessionPersistence::open` opens both writer domains, then always loads the legacy pair. | `runtime/journal.rs:297-331`, `:639-657` |
| Operation IDs are fingerprints of role, expected revision, roots and checkpoint; the writer retries only its own just-failed commit after `renew` shows the head advanced. | `runtime/journal.rs:942-1001` |
| CAS binds expected revision, batch sequence and predecessor batch digest, so a replay whose head has advanced can never apply. | `memory.rs:833-865` |
| Generic sequences (history, usage, manifests, request lists) use 64-wide leaves of 64-hex IDs and a fixed 32-level binary spine. | `memory.rs:1728-1767`, `runtime/journal.rs:1276-1320` |
| Structured JSON arrays (the cache record and manifests) store one `JsonInline` object per element, two elements per `jv` node and a binary Z85 `jb` spine. | `memory.rs:1769-1856`, `:1994-2007` |
| Object envelopes are canonical JSON `{"encoding","kind","schema_version","value"}`; references inside values are 64-hex strings (Z85 only in `jb`). | `crates/agent-runtime-core/src/journal/encoding.rs:329-373`, `model.rs:205-262` |
| `JournalReader` exposes `snapshot()` and `checkpoint()` only; no single-extension read. | `crates/agent-runtime-core/src/journal/reader.rs:709-744` |
| The testkit suites take a `SessionJournalConformanceFixture` (journal + failure injector + writer revoker) and a `JournalCollectionHost`. | `crates/agent-runtime-testkit/src/conformance/journal.rs:50-111`, `journal_gc.rs:20-53`, `:106-130` |
| The workspace already builds `rusqlite` 0.32 `bundled` (optional, `agent-runtime-obs`). | `crates/agent-runtime-obs/Cargo.toml:21`, `:30` |
| Forge's legacy row holds a sealed checkpoint and, only when its digest differs, a sealed ordinary snapshot; a checkpoint save NULLs the snapshot columns. | `forge:crates/agent-host/src/protected_store.rs:1795-1897` |

Measured U9 evidence used by the model (task 2.7, N=4000, plain fixture,
`/Volumes/Data/tmp/refactor/u9b-4/task-2.7-current-final.log`):

| Commit | New objects | New object bytes |
| --- | ---: | ---: |
| Protected bootstrap `accepted` (4,000 messages, history tree) | 4,161 | 905,425 |
| Protected bootstrap `model_response_ready` (structured cache record) | 8,032 | 1,626,408 |
| Ordinary bootstrap (messages, cache record, two manifests) | 20,241 | 4,207,410 |
| Legacy protected checkpoint (`accepted`, materialized JSON) | n/a | 763,847 |
| Legacy ordinary snapshot | n/a | 1,772,321 |

So one 85-byte seed message costs about 226 logical bytes as a message
object plus its share of a history leaf, and each 113-byte cache fragment
costs about 406 logical bytes (one `JsonInline` object, half a `jv` node and
a `jb` share). The ordinary domain repeats all of it, plus about 4,000
spine objects per retained manifest.

## Goals / Non-Goals

- Goals:
  - Extract the commit, projection, validation, reachability, dependency,
    lease and collection engine from `InMemorySessionJournal` into a public
    `JournalEngine<S: JournalStorage>`. Durable hosts implement only storage,
    transactions and fences. Semantics stay in the runtime.
  - `InMemorySessionJournal` becomes the engine over volatile storage. Every
    existing in-memory, runtime and testkit suite passes unchanged.
  - Meet Forge R1–R6 (Forge D2) and R7 (Forge Owner Decisions, Q2).
  - Make physical bytes measurable before any reduction lever is built.
- Non-Goals:
  - A Forge SQLite adapter. The runtime ships a reference packed storage for
    measurement and conformance only.
  - Changing `SessionJournal`, `SessionStore`, `CheckpointStore`, the
    materialized schema-3 checkpoint or the v3 import.
  - U10 (LCM content copy) and the per-commit O(N) CPU of policy and
    checkpoint validation (U9 design:171-178).
  - Global cross-session deduplication. Storage is session-scoped (D8.4);
    a FromIndex/Continue child stores its own copy of shared bodies.

## Decisions

### D1. Engine and storage split

What moves where (no behavior change in this step):

| Today in `memory.rs` | After |
| --- | --- |
| `ObjectAssembler`, `DraftReader`, `StagedClosure`, `materialize_staged_*`, `object_links`, `*_closure`, `validate_leaf`, `DependencyRoots`, `SnapshotEncodingCache`, CAS/checkpoint-floor checks, projection | `journal/engine/` (private modules), unchanged logic |
| `State.objects`, `SessionRecord.{current, last_writer_epoch, operations, retained_heads, checkpoint_floor, retirement, dependencies}`, `State.durable_pins`, `State.retirement_authorizations` | **durable**: persisted through `JournalStorage` (D2) |
| `State.leases`, `active_pins`, `SessionRecord.active_writer`, `root_epoch`, `next_handle`, `collection_enabled`, fast-path snapshot/encoding caches | **process-local** engine registry (D3) |
| `ObjectStore` copy-on-write layers, `compact`, `object_count`, `object_bytes` | `VolatileJournalStorage` |
| `InMemorySessionJournal` | a newtype over `JournalEngine<VolatileJournalStorage>` with the same public items |

Rationale: the process-local split is exactly Forge D4's "root/pin epoch
lives in engine memory" and R2. Everything a restarted process needs to
refuse a stale writer, answer a replay, keep a pin or honor a tombstone is
durable.

### D2. The storage trait (R1)

New module `agent-runtime-core::journal::storage`, re-exported from
`journal`. Signatures are normative for names, arguments and return types;
documentation wording is implementation detail.

```rust
/// One protection domain served by a storage. A single storage instance MAY
/// serve several domains (Forge serves ordinary and protected from one file)
/// and MAY share identical object bodies between them physically.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JournalDomain { pub storage_id: JournalStorageId, pub role: JournalRole }

/// Versioned engine metadata the storage persists verbatim and never parses
/// (checkpoint floor, dependency index, retirement grant, pin root metadata).
#[derive(Clone, PartialEq, Eq)]           // Debug prints version + length only
pub struct JournalEngineState { pub version: u32, pub bytes: Vec<u8> }

/// Durable per-(domain, session) record. `revision`/`writer_epoch` are the
/// fence fields every write re-checks inside the storage transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalSessionRecord {
    pub revision: JournalRevision,          // 0 = nothing published
    pub writer_epoch: u64,                  // last durable writer fence issued
    pub head: Option<JournalObject>,        // current head object (id + canonical bytes)
    pub has_checkpoint: bool,               // host-queryable; false when head is None
    pub retirement: Option<JournalRetirement>, // tombstone; survives collection
    pub collection_dirty: bool,             // roots/pins changed since last full sweep
    pub engine: JournalEngineState,
}

/// One immutable object handed to storage. `kind` lets the host cluster
/// objects; it is redundant with the envelope and never trusted for reads.
#[derive(Clone, PartialEq, Eq)]           // Debug: kind + length only
pub struct JournalStoredObject { pub id: JournalObjectId, pub kind: JournalObjectKind, pub bytes: Arc<[u8]> }

/// All new objects of one write, delivered together so a host can compress
/// and seal them as one unit (R7, lever S1). Order: canonical id order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JournalObjectBatch { pub objects: Vec<JournalStoredObject> }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalOperationRecord {
    pub operation: JournalOperationId,
    pub digest: JournalDigest,
    pub revision: JournalRevision,          // revision the operation published
    pub result: JournalHead,                // original result, returned on identical replay
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalPinRecord {
    pub pin: JournalPinId,
    pub blocks_retirement: bool,            // transfer/staging vs. retained root
    pub roots: Vec<JournalObjectId>,        // heads/leaves whose closures it protects
    pub engine: JournalEngineState,         // dependency index
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalCatalogEntry { pub head: JournalObjectId, pub engine: JournalEngineState }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalPinChange { Put(JournalPinRecord), Remove(JournalPinId) }
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalCatalogChange { Put(JournalCatalogEntry), Remove(JournalObjectId) }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JournalWriteKind { OpenWriter, Commit, Retire, Authorize, Pin, ReleasePin, Stage, Collect }

/// One serialized write transaction. The storage applies it entirely or not
/// at all. SQLite maps it to one `BEGIN IMMEDIATE ... COMMIT`.
#[derive(Debug, Clone, PartialEq)]
pub struct JournalWrite {
    pub domain: JournalDomain,
    pub session: SessionId,
    pub kind: JournalWriteKind,
    /// The stored record's (revision, writer_epoch) MUST equal this, else the
    /// write fails with StateConflict and changes nothing. An absent record
    /// matches (0, 0).
    pub expect: (JournalRevision, u64),
    pub record: Option<JournalSessionRecord>,   // replacement; None = unchanged
    pub objects: JournalObjectBatch,            // added to (domain, session) membership
    pub operation: Option<JournalOperationRecord>,
    pub pins: Vec<JournalPinChange>,
    pub catalog: Vec<JournalCatalogChange>,
    /// Collection: drop these objects from (domain, session) membership. A
    /// body loses read authority immediately; physical removal follows rule 4.
    pub release: Vec<JournalObjectId>,
    /// Replay-window pruning (D8.6). Only set when the owner approves Q1.
    pub prune_operations_before: Option<JournalRevision>,
    /// Test-only commit cut (see below). Production engines leave it None.
    pub inject: Option<JournalCommitFailurePoint>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum JournalBodySharing { PerSession, #[default] Global }

#[async_trait]
pub trait JournalStorage: Send + Sync + fmt::Debug + 'static {
    fn sharing(&self) -> JournalBodySharing { JournalBodySharing::Global }
    /// Optional canonical retained bytes in this domain's session sharing scopes,
    /// including deferred dead bodies; None keeps the membership fallback.
    async fn retained_bytes(&self, domain: &JournalDomain)
        -> Result<Option<u64>, RuntimeError> { Ok(None) }
    async fn load(&self, domain: &JournalDomain, session: &SessionId)
        -> Result<Option<JournalSessionRecord>, RuntimeError>;
    /// Bodies by id, restricted to (domain, session) membership. `None` for a
    /// non-member. Hosts may prefetch a session's packs on the first call.
    async fn objects(&self, domain: &JournalDomain, session: &SessionId, ids: &[JournalObjectId])
        -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError>;
    /// Every object id in (domain, session) membership (collection only).
    async fn members(&self, domain: &JournalDomain, session: &SessionId)
        -> Result<Vec<JournalObjectId>, RuntimeError>;
    async fn operation(&self, domain: &JournalDomain, session: &SessionId, operation: &JournalOperationId)
        -> Result<Option<JournalOperationRecord>, RuntimeError>;
    async fn pins(&self, domain: &JournalDomain, session: &SessionId)
        -> Result<Vec<JournalPinRecord>, RuntimeError>;
    async fn catalog(&self, domain: &JournalDomain, session: &SessionId, head: &JournalObjectId)
        -> Result<Option<JournalCatalogEntry>, RuntimeError>;
    /// Sessions with `collection_dirty`, in session-id order after `after`.
    async fn dirty_sessions(&self, domain: &JournalDomain, after: Option<&SessionId>, limit: usize)
        -> Result<Vec<SessionId>, RuntimeError>;
    /// Returns physical body copies removed by this write, including deferred
    /// dead bodies; canonical encoded lengths, before compression/sealing.
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError>;
    /// Physical compaction only (repack, VACUUM). Never changes membership.
    async fn compact(&self, domain: &JournalDomain) -> Result<(), RuntimeError>;
}
```

Rules the trait documents (and the storage suite in D11 checks):

1. **Atomicity.** `apply` is all-or-nothing. A precondition mismatch is
   `StateConflict` at `PreProvider`/`Checkpoint` with no effect. Any other
   error is *ambiguous*: the engine drops its cached record for that
   (domain, session) and reloads it before the next use.
2. **Statement order and cuts.** A storage executes a commit in the order
   objects (non-control kinds) → `Checkpoint` object → `Batch` object →
   pins and catalog → session record → operation record, then commits.
   `inject = Some(point)` aborts and rolls back after the named step;
   `AfterCommit` commits and then returns an error. This is how
   `JournalCommitFailurePoint` cuts reach a real transaction (Forge D5.1).
   `JournalCommitFailurePoint` keeps its seven variants; the engine passes the
   armed point through, so `fail_next_commit_at` works on every storage.
3. **Fences inside the transaction.** The `expect` comparison runs inside the
   storage transaction (`WHERE revision = ? AND writer_epoch = ?` in SQL), not
   only in the engine's registry. A write from a fenced-out writer cannot land
   even if the engine's cache is stale.
4. **Membership and sharing (Q4 authoritative).** Objects are added to
   `(domain, session)` membership. An id already present with different bytes
   in the sharing scope is `StateConflict`. `JournalStorage::sharing()` declares
   `JournalBodySharing::PerSession` or `Global`. `Global` stores one physical
   copy across domains and sessions; `PerSession` stores independent copies
   across sessions and may share across that session's domains or keep a copy
   per domain. The additive default is `Global`, preserving existing adapters'
   behavior. The packed reference declares `PerSession`; volatile defaults to
   `Global` and also offers a per-session configuration.

   Every body is readable through every membership holding it until that
   membership is released. Non-members never read it, including after its last
   membership in scope is released. Releasing A's last membership can reclaim
   A's copy under `PerSession` even when B holds the same id; a `Global` copy
   survives until its last holder releases it.

   `apply` returns `JournalApplyReceipt { reclaimed_objects, reclaimed_bytes }`
   counting exactly physical body copies removed by that transaction, with
   bytes measured as their canonical encoded lengths before compression and
   sealing (the existing receipt unit). Clearing membership bits alone counts
   zero. Retained live bodies moved during a rewrite do not count as reclaimed.

   Physical deletion may be deferred inside a sealed pack below a documented
   dead-byte threshold of at most **25% of canonical body bytes**. The apply
   reaching the threshold rewrites that pack; dead bytes are never served.
   An empty sharing scope and collection of a retired/deleted session always
   purge its dead bytes. Receipts count deferred bytes when physically removed,
   including removals triggered by a later apply, not at membership release.
   Immediate deletion is also conforming. The reference uses a 25% threshold;
   it rewrites only the affected packs and bounded small-pack merges.
   `JournalStorage::retained_bytes(domain)` is an additive optional diagnostic:
   deferred adapters return canonical bytes physically retained in the sharing
   scopes containing the domain's sessions, including dead bytes. The default
   `None` keeps existing adapters' membership-based engine diagnostic. The
   packed adapter supplies it so the unchanged GC byte-evidence assertions
   still compare retained-byte differences with physical receipts. This
   diagnostic never controls liveness and is not compressed SQLite file size.

5. **Consistent reads.** Each read method returns a state that some completed
   `apply` produced. Within one engine instance, every read that feeds a write
   happens under that engine's write gate (D3), and objects are immutable, so
   no multi-call read transaction is required.
6. **No awaits but storage.** The engine never holds a storage transaction:
   a transaction exists only inside one `apply` call.
7. **Isolation is not the digest.** Storage never serves an object to a
   domain or session that is not a member, even if the id is known.

The engine is public:

```rust
pub struct JournalEngine<S: JournalStorage> { /* storage, domain, policy, registry, caches */ }
pub struct JournalEngineConfig {
    pub validated_root_memo_bytes: usize,   // R4 memo budget, default 256 MiB per engine
    pub collection_enabled: bool,           // default false: retire/collect stay unsupported
}
impl<S: JournalStorage> JournalEngine<S> {
    pub fn protected(storage: Arc<S>, storage_id: JournalStorageId, config: JournalEngineConfig) -> Self;
    pub fn ordinary(storage: Arc<S>, storage_id: JournalStorageId,
                    projection: Arc<dyn JournalProjectionPolicy>, config: JournalEngineConfig) -> Self;
    pub fn fail_next_commit_at(&self, point: JournalCommitFailurePoint) -> Result<(), RuntimeError>;
    pub fn revoke_writer(&self, session: &SessionId) -> Result<(), RuntimeError>;          // I/O-free
    pub fn enable_collection(&self) -> Result<(), RuntimeError>;                            // I/O-free
    pub fn root_epoch(&self) -> Result<u64, RuntimeError>;                                  // I/O-free
    pub async fn authorize_retirement(&self, session: &SessionId, expected: JournalRevision,
                                      retired_at: Timestamp) -> Result<(), RuntimeError>;
    pub async fn pin_transfer(&self, lease: &JournalLease) -> Result<(), RuntimeError>;
    pub async fn retain_root(&self, lease: &JournalLease) -> Result<(), RuntimeError>;
    pub async fn release_pin(&self, session: &SessionId, pin: &JournalPinId,
                             expected_root_epoch: u64) -> Result<(), RuntimeError>;
    pub async fn stage_leaves(&self, lease: &JournalLease, pin: JournalPinId,
                              objects: Vec<JournalObject>) -> Result<(), RuntimeError>;
    pub async fn compact(&self) -> Result<(), RuntimeError>;
}
impl<S: JournalStorage> SessionJournal for JournalEngine<S> { /* the eight methods */ }
```

`JournalProjectionPolicy` and `IdentityJournalProjection` move to the engine
module unchanged and stay re-exported at the same paths.

### D3. Process-local state, leases, locking and epochs (R2)

- **Registry** (`std::sync::Mutex`, never held across an await): leases with
  their reachable sets, active pins, active writer per session, `next_handle`,
  `root_epoch`, `collection_enabled`. `close`, lease drop and `revoke_writer`
  touch only the registry, so they never await storage and
  `SessionPersistence::drop`'s `now_or_never` close always completes.
- **Write gate** (one async mutex per engine instance, i.e. per domain; it
  never hands a released lock to a parked waiter, so `futures_util::lock::Mutex<()>`
  and not `tokio::sync::Mutex`, whose handoff can leave the lock owned by a
  task queued in the releasing worker's unstealable LIFO slot; each
  acquisition first consumes one unit of Tokio's cooperative budget): held from the first read that feeds a write until the registry is
  updated after `apply`. Taken by writer open, commit, retire,
  `authorize_retirement`, pin changes, staging and collection. Read-only opens
  also take it briefly, so a collection cannot run between "select root" and
  "register lease". `read` never takes it: objects are immutable and the lease
  set only grows under the gate.
- **Writer open** loads the record, then `apply(OpenWriter)` with
  `expect = (revision, writer_epoch)` writing `writer_epoch + 1` (creating the
  revision-0 reservation for an absent head). A second writer in the same
  process is refused from the registry before any I/O; a writer in another
  process (Forge forbids it with `runtime.lock`) fails the durable fence.
- **Root epoch**: advanced at exactly today's operations (Evidence table), by
  the registry, after the operation's `apply` succeeds. The volatile instance
  therefore reports the same epochs to every existing GC suite. A collection
  checks `expected_root_epoch` under the gate before it reads anything.
- **Projection policy** runs under the write gate, as today under the lock; a
  policy must still not call back into the same journal.
- `InMemorySessionJournal`'s synchronous host controls keep their signatures.
  `VolatileJournalStorage` never suspends, so the newtype drives the engine's
  async controls to completion with `now_or_never`, retrying with
  `std::thread::yield_now` only while the write gate is held by another thread
  (a holder never suspends inside the volatile storage, and the gate is only
  ever owned by a task that is running).

### D4. Session-scoped incremental collection (R3)

Review follow-ups (N3/N1, 2026-10-19): before clearing a source session's
dirty flag, collection adds its id to each dependency target's opaque session
metadata (`retirement_dependents`). These are append-only candidates, not
authoritative blockers. Retirement reads the target's candidates plus pending
dirty sources from storage and checks their current heads and pins; stale
candidates do not block. A failed/interrupted indexing write leaves the source
dirty, so a restart still discovers it. Index writes preserve the target's
dirty flag and use the existing fenced `JournalWrite`; no trait change or
object/membership read of clean targets is needed. Completed-fork children
are loaded by id. Retirement and diagnostics no longer hydrate every session
through a clone of the whole process registry.

Durable pins store head/leaf roots, session dependencies, an unresolved bit
and pinned writer epochs. Each retained-head catalog entry adds its previous
head id, writer epoch, and the delta of validated leaves staged since the
writer's preceding commit (recorded once, including unreferenced staging in
the writer's lease closure). Collection resolves a pinned writer's chain from
its current head and memoizes catalog entries and closures per root for the
sweep. The next writer open or retirement adds the prior writer's final root
to affected pins before changing/removing that head; ordinary commits never
write pins. Explicit pin controls compare old/new records and write only
changes. Only a head published by the pinned writer epoch becomes its dynamic
tip; retained reader pins do not follow later writer opens. These links are
engine metadata, not new object-format fields, and
preserve the original transfer/retention closure and epoch semantics.

- The membership invariant: for every (domain, session), membership ⊇ the
  closure of the current head, of every durable pin's roots, of the session's
  catalog heads that are still selectable, and of staged leaves. Commits add
  their whole admitted set (including objects first stored by another session
  or the other domain); this is what makes per-session sweeps safe.
- Every commit, retirement, pin change and staging change sets
  `collection_dirty` in the same `apply`.
- `collect(request)`: under the gate, check the epoch, then page
  `dirty_sessions`. For each session: live = closure(current head) ∪
  closures(durable pin roots) ∪ the reachable sets of that session's live
  leases (registry) ∪ staged roots. Trace with the engine's closure walk over
  `storage.objects`, memoized by root (D5). `release` = members − live, up to
  the remaining `max_objects`; catalog entries whose head is released are
  removed in the same write; `collection_dirty` is cleared only when the
  session's sweep released everything it found. Retired sessions have no
  current root, so only their pins survive.
- Cost: proportional to the members of dirty sessions, never to the domain.
- Shared objects stay correct because physical deletion is storage-level
  within the declared sharing scope (D2 rule 4): a FromIndex child or the other
  domain keeps its own membership. `examined` counts membership entries in
  swept sessions; `max_objects` bounds membership releases, as the engine's
  existing `released` counter does. `deleted` and `reclaimed_bytes` sum physical
  copies/canonical bytes from `JournalApplyReceipt`, including earlier deferred
  bodies removed by this collection. The volatile single-session N=1000 byte
  evidence (570,121 / 404,529 / 0) is unchanged; deferred adapters include dead
  copies in their retained-byte diagnostic until physical removal.
- Unknown dependency revisions and `unresolved` indexes keep returning typed
  unsupported (today's domain-wide check becomes a check over the dependency
  records the sweep loads plus live leases).

### D5. Validated-root memo (R4)

The engine and its clones share an LRU keyed by `(session, head object id)`;
the domain is fixed by the engine. Each immutable entry holds the exact head
closure (`Arc<BTreeSet<JournalObjectId>>`), shared immutable object layers,
decoded snapshot, optional encoding cache, and dependency roots. Only a
root-checked successful commit or a complete materialization can create an
entry. Cold opens validate once; memoized opens reuse the closure and seed
without object reads or full materialization, including across writer epochs.
Unreplayable protected deltas still fall back to full validation.

`validated_root_memo_bytes` defaults to 256 MiB per engine. The conservative
charge includes stored encoded bodies and their id/index overhead, canonical
snapshot bytes, closure ids, encoding-cache ids/keys, and entry overhead;
shared data is charged to every entry. Least recently used entries are evicted
until the new entry fits. An oversized entry is not retained; zero disables
retention. This budget covers the memo, independently of live writer working
sets and the pre-existing session state caches. No storage trait change is
needed.

Exact-closure construction also caches parsed object links by object id and
expected kind. New or differently typed edges run the original checks; reused
immutable edges avoid JSON reparsing. Missing bodies still conflict. Parsed
links are charged to the memo budget. A new writer trims cache-only bodies and
links from the previous writer, preserving the selected exact closure and all
durable memberships; reader/GC roots are unchanged. Tests prove edge reuse,
wrong-kind rejection, missing-body rejection and unchanged membership on trim.

The immutable body cache also mitigates N2: commit starts from the memoized
head's shared object layers and fetches only missing admitted lease objects
(e.g. separately staged leaves), rather than reloading the whole lease closure.
Renew reuses the same validated closure. Public reads can reuse immutable
memoized bodies after validating the live lease and its admitted reachability;
bytes retain the ordinary hash/kind check and the lease is checked again before
return. Missing cache bodies fall back to membership-scoped storage reads.
This also avoids rereading a full closure while a per-turn runtime restores a
warm snapshot. Storage still supplies durable fences, current records, pins and
operation results for every write.

Collection evicts historical entries whose closure intersects released
membership, and trims released cache-only bodies from unaffected entries.
Thus current-root validation survives ordinary collection, and a cached body
can never suppress re-admission of an object whose membership was released.
A failed storage apply clears the session's memo conservatively.

The two named existing writer-change/advisory-delta tests have no full-count
assertion requiring a change; all their assertions stay unchanged. New tests
cover true LRU pressure, oversized/zero-budget misses, one full validation
following eviction, and zero storage object calls for open, renew and commit
of a 100-message memoized head in both domains, after collection between writer
epochs. Evidence: `rt-e6/r4-core-final.log`; each tested commit calls load, pins,
operation and apply once, and objects zero times. The future-size gate remains
16,384 bytes (largest observed: commit_inner, 4,944 bytes).

### D6. Native-first open (R5)

`SessionPersistence::open` opens and validates the configured native domains
first. It loads the legacy pair only when neither domain has a head, or when
legacy import is enabled, both store/journal capabilities are present, the
protected head exists and the ordinary head is absent (the supported repair).
Headed sessions otherwise skip both `load` and `load_latest`. Capability-loss,
retirement, absent-head, native authority and import fences are unchanged.

The shared group-D consumer assertion in
`conformance/journal_consumers/recovery.rs` changes `loads + 2` → `loads`,
executed by the Smith, Nyx and Open Forge fixtures. A new runtime regression
checks a complete import and a protected-first partial import: import/repair
loads the legacy pair once (one call per store); five subsequent native
reopens make zero legacy calls and no provider calls. Evidence:
`rt-e6/r5-runtime-focused.log`. No other existing assertion changes for R5.

### D7. Bounded single-extension read (R6)

`JournalReader::extension(&mut self, namespace: &str)
-> Result<Option<VersionedSessionState>, RuntimeError>` validates the selected
head object and its batch, then descends the extension map by `first_key`
separators. It validates fanout, ordering, inherited bounds, subtree heights and
counts along the path, and reads one opaque `Extension` value. It never reads
history, usage, manifests or the checkpoint. Stored sensitivity and numeric
bits are preserved; absent heads, empty maps and missing namespaces return
`None`. Object, byte and entry limits apply as for the other reader methods.

For a nonempty map, height measured in visited levels is
`JournalMap.height + 1` (the descriptor uses zero for a leaf). Exact opaque
read counts are **head + batch + map levels + value**: 4 reads at 10 namespaces
and 6 at 10,000 namespaces, each with a 4,000-message history and the unrelated
closures absent from the test transport. Evidence:
`rt-e6/r6-readers-valid-fixture.log`. This clarifies the height convention in
the bounded-read scenario; no additional off-path object is read.

A structured `ExtensionReference` at `runtime.core.previous_cache` is
materialized through its JSON tree and is not bounded by that opaque count;
reader limits still apply. Structured objects at other namespaces conflict.
The storage trait is unchanged. Tests also cover Sensitive values, missing
keys, empty/absent heads, all three limits, malformed separators, and the
structured namespace's additional reads and limit failure.

### D8. Physical disk (R7)

#### D8.1 Metric and baseline

- **Metric**: physical bytes of one live session after collection:
  `page_count × page_size` after `wal_checkpoint(TRUNCATE)` and `VACUUM` of a
  file holding that session only, minus the same file with the empty schema.
  It includes encryption overhead, compression, row, index and page overhead.
- **Legacy baseline L**: the same measurement of Forge's legacy row after the
  same session's last completed turn, idle. Whether that row holds one sealed
  blob (the checkpoint, with the ordinary save skipped because its digest
  matches) or two (checkpoint plus ordinary snapshot) depends on save order
  and digest; the harness reports which. Estimates below show both readings:
  **L1** (one blob) is the stricter reported baseline; the later Owner
  Decision gates against the observed **L2** idle row.
- **Gate** (later Owner Decision): native ≤ 2.0 × L2 at N=4000 on the
  realistic fixture B; aim ≤ 1.5 ×. Report N=100/1000/4000 for both
  fixtures (D10). L1 remains diagnostic.

#### D8.2 Where the bytes go today

Per message, logical bytes per domain (fixture A, from the evidence table):

| Component | Logical bytes per message | Comes from |
| --- | ---: | --- |
| Message object (85 B JSON + 72 B envelope) | ~157 | one object per message |
| History leaf reference (quoted 64-hex) and leaf/spine share | ~69 | `Leaf { items }` |
| Cache fragment `JsonInline` (113 B + 65 B envelope) | ~178 | one object per element |
| `jv` node share (two 64-hex refs per node) | ~146 | binary bottom nodes |
| `jb` spine share (two Z85 refs per node) | ~76 | binary spine |
| Per retained manifest (ordinary only): its own `jv`/`jb` spine | ~220 each | not shared with the cache record in the measured run |

What turns logical bytes into physical bytes:

- **Object count.** About 3 objects per message in protected and 5 in
  ordinary (fixture A). Any per-object row costs a key, a nonce, a tag, row
  and index overhead regardless of body size.
- **Small objects never compress.** Almost every object is below Forge's
  1 KiB threshold (Forge D3.4), so per-object compression does nothing; the
  envelope repeats verbatim in every row.
- **Per-row AEAD**: 24-byte nonce + 16-byte tag + 9-byte envelope per body.
- **SQLite rows and indexes** (Forge D4): a body row ≈ 106 B plus a 41 B
  primary-key index entry; a membership row ≈ 84 B plus an 83 B secondary
  index entry; random keys leave B-tree pages ~70% full. ≈ 170 B per body and
  ≈ 240 B per membership, two memberships per shared body.
- **Reference entropy.** Each reference is a random 32-byte digest (64 hex or
  40 Z85 chars). Compression cannot shrink it below ~33 bytes. Legacy has no
  references at all.
- **Cross-domain duplication.** Two engine instances store two copies unless
  storage shares bodies.
- **Superseded objects before collection.** About 80 KB logical per plain
  turn (U9 task 2.7: 49 KB protected + 30 KB ordinary), mostly heads, batches
  and rewritten spine paths, until a sweep.
- **Operation records.** One per commit, retained for the session lifetime
  today, each with the original head (~1 KB; 0.6–1.2 KB sealed per Forge D4).
  About 10 commits per turn across both domains, so they grow with turns, not
  messages, and collection never removes them.

Legacy, by contrast, compresses one JSON document as a whole: the envelope
and key names disappear, repeated cache fragments become long matches, and
only content and one copy of each fragment hash remain (≈ 42 B per fixture-A
message, ≈ 256 B per fixture-B message at L1).

#### D8.3 Levers

Storage-level (no format change):

- **S1 Commit packs.** `apply` receives each write's new objects as one
  batch, then partitions admission at transaction cut points into sealed packs
  capped at **1 MiB canonical body bytes**, including imports. Each immutable
  object must fit that cap; oversized objects are rejected atomically. There
  is no journal format change. Operation packs also stay within 1 MiB of their
  serialized logical records; full segments leave binary carry and stay sealed.
- **S2 Session repack with cross-domain sharing.** One independent pack set
  per session serves both domains. Admission deduplicates within that session
  only; no cross-session lookup or open-time decoding of all packs occurs.
  Session indexes are loaded lazily by `owner`, with an SQLite owner index.
  `data_version` invalidates caches after another connection commits.
  Collection rewrites a pack once **at least 25% of its canonical body bytes
  are dead**, even when optional repacking is off. Retired-session collection
  purges every dead byte. Membership bits hide dead bodies immediately.
  Base repack selects only packs smaller than **64 KiB** and packs that apply
  already purged; stable larger packs stay sealed. Outputs respect the 1 MiB
  cap. Packs containing deferred dead bodies stay sealed until purge, so
  repartitioning cannot concentrate dead bytes above the bound. The explicit
  terminal hook merges equal size tiers (minimum 4 KiB)
  only when their combined logical bytes fit the cap. Each admitted byte has
  a constant number of size tiers up to that fixed cap; threshold purges are
  charged to the bytes that died, avoiding whole-history rewrites every turn.
- **S3 Compact ids at the boundary.** The id → (pack, offset) index lives
  inside each sealed pack as sorted 4-byte id prefixes plus varint offsets;
  a read verifies the full id by hashing the bytes (the reader already
  re-verifies ids, `encoding.rs:394-404`). No per-object SQLite row or index
  remains. This needs session-scoped reads, which the trait has.
- **S4 Op-record packing.** Operation records of a session live in its packs,
  so successive heads compress against each other.
- **S5 Bounded replay window** (contract change, owner Q1). Keep operation
  records at or above the inclusive current −64 floor per domain, plus the
  current writer epoch’s own last operation. Older pruned identical replays return
  `StateConflict`, which CAS already guarantees because every commit binds
  expected revision, sequence and predecessor (`memory.rs:833-865`).

Format-level (journal schema 5; D9):

- **F1 Chunked JSON array leaves.** Structured JSON arrays store up to 64
  elements (≤ 8 KiB canonical) inline per leaf object, with fixed chunk
  boundaries from index 0 and a minimum-height branch spine. Prefix-identical
  arrays (the cache record and every manifest's cache identity) share all full
  chunks. Removes ~3 objects and ~2 references per element.
- **F2 Chunked history.** History leaves store up to 64 messages (≤ 16 KiB)
  inline instead of 64 message IDs. `history_chain` keeps its definition over
  virtual message object IDs (computed, not stored).
- **F3 Minimum-height generic sequences.** Removes the 20–26 unary spine nodes
  each changed sequence rewrites per commit (pre-collection garbage only).
- **F4 Binary references in envelopes.** Rejected: after compression a 64-hex
  or Z85 id already costs ~33 bytes, so it saves ~0 physical bytes for a
  format break.

#### D8.4 Measured physical bytes through the reference adapter (step 7 / task 4.5)

Measured 2026-10-20 UTC on the Q4-aligned PerSession reference with 1 MiB
logical packs, 25% dead-byte purges and bounded small-pack merges. Both domains
share within one session; separate sessions own independent copies. The
explicit geometric terminal hook still runs after terminal publication.

Physical bytes are `page_count × page_size` after truncated WAL checkpoints
and VACUUM, minus the empty schema. File length and `dbstat` agree. All
head/fence/engine metadata, pins, catalogs, memberships and object/operation
packs are included. Idle legacy rows contain two sealed blobs (L2), because
`identity.event` and `identity.event_seq` advance after the terminal checkpoint.
Sealing models a 24-byte nonce and 16-byte tag over real zstd level-3 bytes.

Collection once at the end (the original 13 cases):

| Fixture | N | Turn | L1 bytes | L2 bytes | Before bytes | Final bytes | Before/L2 | Final/L2 | Cumulative pack writes | Largest collection write |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 100 | plain | 0 | 8192 | 90112 | 57344 | 11.00000 | 7.00000 | 415555 | 86181 |
| A | 100 | tool | 0 | 8192 | 118784 | 61440 | 14.50000 | 7.50000 | 541851 | 91181 |
| A | 1000 | plain | 20480 | 53248 | 344064 | 311296 | 6.46154 | 5.84615 | 750200 | 27274 |
| A | 1000 | tool | 24576 | 53248 | 356352 | 307200 | 6.69231 | 5.76923 | 1103115 | 129458 |
| A | 4000 | plain | 90112 | 204800 | 1208320 | 1179648 | 5.90000 | 5.76000 | 1365409 | 27200 |
| A | 4000 | tool | 94208 | 208896 | 1224704 | 1179648 | 5.86275 | 5.64706 | 1522276 | 32148 |
| B | 100 | plain | 20480 | 40960 | 135168 | 102400 | 3.30000 | 2.50000 | 415920 | 61242 |
| B | 100 | tool | 20480 | 45056 | 155648 | 94208 | 3.45455 | 2.09091 | 802616 | 161358 |
| B | 1000 | plain | 159744 | 335872 | 622592 | 577536 | 1.85366 | 1.71951 | 842386 | 33636 |
| B | 1000 | tool | 163840 | 339968 | 679936 | 622592 | 2.00000 | 1.83133 | 1494382 | 288266 |
| B | 4000 | plain | 655360 | 1359872 | 2387968 | 2183168 | 1.75602 | 1.60542 | 3053696 | 314148 |
| B | 4000 | tool | 655360 | 1392640 | 2719744 | 2494464 | 1.95294 | 1.79118 | 3533286 | 323123 |
| C | 1000 | tool | 454656 | 942080 | 11993088 | 1228800 | 12.73043 | 1.30435 | 106631979 | 2257327 |

Collection after every turn, plus a final sweep after shutdown:

| Fixture | N | Turn | L2 bytes | Final bytes | Final/L2 | Cumulative pack writes | Largest collection write |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| B | 1000 | plain | 335872 | 577536 | 1.71951 | 845627 | 33297 |
| B | 1000 | tool | 339968 | 622592 | 1.83133 | 1491335 | 288771 |
| B | 4000 | plain | 1359872 | 2183168 | 1.60542 | 3057083 | 313815 |
| B | 4000 | tool | 1392640 | 2494464 | 1.79118 | 3540392 | 323123 |
| C | 1000 | tool | 942080 | 1204224 | 1.27826 | 144922993 | 368800 |

B/N=4000 passes the required **≤2.0× L2** gate: collect-at-end = **1.60542× plain / 1.79118× tool**; per-turn = **1.60542× plain / 1.79118× tool**. All four miss the optional 1.5× aim. Group 5 stays unbuilt; journal schema 4 is unchanged.

Cumulative per-turn B pack writes grow **3.615× plain / 2.374× tool** when seeded N grows 4× (1000 → 4000). C/1000 contributes 250 turn samples in the raw JSON. **Write verdict: O(N) in admitted bytes for these bounded turn shapes; no O(N²) whole-history pack rewrites.** Fixed 1 MiB packs bound merge tiers; stable large packs stay sealed. At a 25% dead threshold, survivor rewrites are charged to at most three live bytes per dead byte. Replay-window maintenance is bounded. This verdict concerns pack payload/nonce writes; the chosen metric excludes metadata, SQLite page overhead and VACUUM.

Supplemental C/N=4000, collection at the end (one release run):

| Fixture | N | Turn | L2 bytes | Final bytes | Final/L2 | Cumulative pack writes | Largest collection write |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| C | 4000 | tool | 3977216 | 4886528 | 1.22863 | 487612606 | 9208323 |

Its native/L2 ratio is 1.22863×. It is reported without gating.

`/Volumes/Data/tmp/refactor/rt-e7/measurements.json` is authoritative. The
`cumulative_pack_bytes_written` meter uses transactional TEMP triggers for
object/operation pack INSERT/UPDATE payload + nonce lengths, including
intermediate rewrites. `max_collection_pack_bytes_written` is the largest
meter delta for one engine collection call (one domain); final collection is
included. Rollback restores the meter. The raw JSON retains every turn's
cumulative sample and actual retained-operation counts. Simulator fields stay
counterfactual diagnostics; former projected-after fields alias actual sizes.

#### D8.5 Decision rule

1. Build the measurement harness first (task 1). It reports L1/L2, the D4
   layout and S1, S1+S2, S1+S2+S3 for both fixtures, plus a grown fixture C
   (D10) for operation records.
2. Build S1–S4 in the reference packed storage after extraction (tasks 3–4).
3. If S1–S4 (+S5 if approved) measure ≤ 2.0× L2 on fixture B at N=4000, stop:
   no format change. Report fixture A as the lower bound.
4. Otherwise, or if the owner requires the 1.5× aim or fixture A (Q2), build
   F1, re-measure, then F2. F3 is built only together with F1 or F2, since it
   needs the same format version.

#### D8.6 Replay window (S5)

- Pruning runs in collection (`prune_operations_before`), never in commit.
- The window is per domain/session, with an inclusive floor:
  `revision >= current.saturating_sub(64)` (current −64 kept, −65 pruned),
  plus the current writer epoch's own last operation. This edge and
  last-operation rule follow the explicit step-5 owner instruction; they
  replace the earlier strict-floor/all-current-epoch wording. At a current
  revision of 70 the floor is 6, retaining 65 records including the current
  result. Storage never prunes another domain/session's records.
- A pruned identical replay returns the same `StateConflict` as a stale CAS;
  it can never apply. A pruned different-digest replay also conflicts.
- `assert_journal_gc_replay_fence_conformance` replays after one advance and
  one sweep and stays green; a new conformance case checks the window edge.
- Released contract text ("returns its original result even after head
  advance", `mod.rs:63-65`) becomes "for at least the latest 64 revisions and
  the current writer epoch's last operation". This adopted pre-1.0 semantic
  break is recorded in the Q1 decision below.

### D9. Journal schema 5 (only if F1/F2 are gated in)

- `JOURNAL_SCHEMA_VERSION` stays 4 for v4 objects; v5 objects and heads carry
  `schema_version: 5`, which the object hash already binds.
- Readers accept 4 and 5 for at least one full release after the first v5
  writer release (Q3). Materialized checkpoints stay schema 3.
- No silent rewrite: a v4 head is continued by the writer in v4. New sessions
  and v3 imports start in v5 only when the host opts in on the engine config
  (`JournalEngineConfig::format`, added with F1). Upgrading an existing v4
  session is an explicit host call that publishes one full-replacement commit
  (`JournalTransitionDelta::bootstrap` over the existing head, operation
  `runtime-format-upgrade-v5-*`) and is counted in the report.
- New object kinds are added to the `#[non_exhaustive]`-less
  `JournalObjectKind`: a declared break, listed in CHANGELOG.
- The cold-encoding oracle, equivalence harness and every conformance suite
  run against both formats.

### D10. Measurement harness

- New `publish = false` crate `crates/agent-runtime-journal-bench` (keeps
  `rusqlite` and `zstd` out of every published crate). Dependencies: core,
  runtime, testkit, `rusqlite` 0.32 `bundled` (already in the workspace),
  `zstd`. Sealing is modeled exactly by size (24-byte nonce + 16-byte tag per
  sealed unit), not by real encryption, because SQLite sizes depend only on
  lengths.
- The original task-1 harness used the temporary core `journal-measurement`
  feature and `measurement_export`. Task 4.3 removes both and runs the same
  fixtures end to end over `JournalEngine<PackedSqliteJournalStorage>`.
  Bench-local observation records first admissions and operation counts;
  membership and bodies are read through `JournalStorage`. The adapter's real
  SQLite file, including actual opaque engine metadata, is measured in place.
- Fixtures:
  - **A**: the task-2.7 fixture (`session_journal_equivalence.rs:1925-2024`),
    plain and tool.
  - **B**: fixture A's shape (N seeded messages, cache-enabled provider,
    two turns) with realistic content from a seeded deterministic generator
    (user text 100–800 B, assistant 200–2,000 B, tool calls and results
    0.5–8 KB of JSON, English-like word lists so zstd ratios are realistic).
    B at N=4000 is the R7 gate (Q2).
  - **C** (report only): B's content grown by turns to N=1000 (250 turns),
    to count operation records per turn and the retained manifest window.
- Authoritative native sizes are the actual S1–S5 adapter file before and
  after collection, with root-only pins and real operation pruning. Legacy uses Forge's row shape/save rules (`FORGE-SESSION\0`,
  zstd 3, modeled sealing, checkpoint NULLs snapshot, digest skip). D4,
  isolated S1/S2/S3 and the old S4/S5 layouts remain counterfactual SQL
  diagnostics over the real stored object sets. Task 4.5 replaces the earlier
  S5 projections with measurements; no F lever is built. The supplemental C
  script grows to N=4000 (1000 writer epochs) for the step-6 ratio table.
- Output: one JSON report plus a Markdown table with N=100/1000/4000, both
  baselines, per-kind breakdown (objects, logical, compressed, row/index
  overhead), pre- and post-collection bytes, and op-record bytes per turn.
  `#[ignore]` full run; a non-ignored N=100 smoke keeps it compiling.

### D11. Conformance

- Existing suites run unchanged against `InMemorySessionJournal` (engine over
  volatile storage): core unit tests, runtime equivalence/import/crash
  matrices, `assert_session_journal_conformance`,
  `assert_ordinary_session_journal_conformance`, every collection suite and
  the consumer gates. That is the proof that extraction preserved semantics.
- New testkit module `conformance::journal_storage`:
  - `assert_journal_storage_conformance(factory)`: the factory returns a fresh
    `Arc<S>`; the suite runs the existing protected, ordinary and collection
    suites over `JournalEngine<S>`, plus storage-level cases: atomic `apply`
    at every `inject` point, precondition fences, membership-restricted
    reads, declared Global/PerSession copies, deletion only at scope-local last
    membership within the declared scope, bounded deferred removal,
    `dirty_sessions` paging, and a restart (a fresh engine over
    the same storage) that must see exactly the acknowledged state.
  - Broken storages that the suite MUST reject, each failing a named case:
    **torn transaction** (persists objects but drops the record at a cut),
    **lost write** (acknowledges without persisting; detected on restart),
    **reordered commit** (makes write n+1 visible without write n; detected by
    predecessor/sequence on restart), **stale epoch** (ignores
    `expect.writer_epoch`; a revoked writer's commit lands),
    **shared object deleted while referenced**, exercised under both sharing
    declarations (deletes a body when another
    domain's or session's membership still holds it; detected by the other
    session's open failing before a lease).
- `JournalEngine` exposes the same `fail_next_commit_at`, `revoke_writer` and
  host controls, so `SessionJournalConformanceFixture::new` and a
  `JournalCollectionHost` impl for `JournalEngine<S>` (added in the testkit)
  cover any storage with one line.
- Sharing receipt cases branch on `storage.sharing()`: Global retains its
  original zero-until-last-holder receipts; PerSession independently reclaims
  session copies and permits either shared or independent domain copies.
  Both modes check every remaining holder's readability and released-member
  isolation. A low-dead-share case accepts immediate or deferred removal and
  checks exact cumulative receipts when the scope empties. The sixth faulty
  adapter serves released bodies; it fails membership isolation in both modes.
- Volatile runs the complete suite in both modes via
  `VolatileJournalStorage::new(JournalBodySharing::PerSession)` or the existing
  Global default. The packed adapter passes with optional repacking off, on,
  and a connection restart; all configurations declare PerSession.

### D12. Compatibility, migration and CHANGELOG

Unchanged (source-compatible): `SessionJournal`, `SessionStore::journal`,
`CheckpointStore::journal`, every DTO in `model.rs`, `JournalReader`,
`JournalProjectionPolicy`, `IdentityJournalProjection`,
`JournalCommitFailurePoint`, and every `InMemorySessionJournal` item:
`protected`, `ordinary`, `fail_next_commit_at`, `revoke_writer`,
`enable_collection`, `root_epoch`, `authorize_retirement`, `pin_transfer`,
`retain_root`, `release_pin`, `stage_leaves`, `compact`, `object_count`,
`object_bytes`, `Clone`, `Debug`. Testkit: `SessionJournalConformanceFixture`,
`SessionJournalCollectionFixture::{reference, reference_ordinary}`, the
`JournalCollectionHost` impl for `InMemorySessionJournal`, and every
`assert_*` function keep their signatures and behavior.

Added (all additive):

- core `journal::storage`: `JournalStorage`, `JournalDomain`,
  `JournalEngineState`, `JournalSessionRecord`, `JournalStoredObject`,
  `JournalObjectBatch`, `JournalOperationRecord`, `JournalPinRecord`,
  `JournalCatalogEntry`, `JournalPinChange`, `JournalCatalogChange`,
  `JournalWriteKind`, `JournalWrite`, `JournalApplyReceipt`,
  `VolatileJournalStorage`, `JournalBodySharing`.
- Q4 additions: `JournalStorage::sharing()` (default Global), optional
  `JournalStorage::retained_bytes(domain)` (default None), and
  `VolatileJournalStorage::new(sharing)`.
- core `journal` re-exports: `JournalEngine<S>`, `JournalEngineConfig`, and
  `JournalEngine::retained_bytes`.
- `JournalReader::extension`.
- testkit `conformance::journal_storage`: `assert_journal_storage_conformance`,
  `JournalCollectionHost for JournalEngine<S>`, broken storages
  (test-only, not exported).
- the bench crate (not published).

The implemented pre-1.0 break is S5's bounded replay window: collection may
prune results below the inclusive current −64 floor, preserving the current
writer epoch's own last operation. `SessionPersistence::open` also skips unused
legacy loads. The storage abstraction and extension reader are source-compatible
additions. Schema 5 is not built because task 4.5 passes the L2 gate.

Migration: none for existing users. `InMemorySessionJournal` callers and
testkit consumers recompile without edits. Forge adopts by implementing
`JournalStorage` and wrapping it in two `JournalEngine`s sharing one storage;
Forge D4's per-object tables are replaced by pack tables (Forge's spec change,
not this one).

CHANGELOG `Unreleased` text (applied in the last task):

- `### Added` — "A storage-generic session-journal engine. `JournalEngine<S>`
  runs the complete commit, projection, validation, lease, retirement and
  collection semantics over any `JournalStorage`, which supplies only atomic
  write transactions, fences and object membership. `InMemorySessionJournal`
  is now this engine over `VolatileJournalStorage`; its API and behavior are
  unchanged. Storage receives each write's new objects as one batch so a host
  can compress and seal them together. Collection is incremental per session.
  A validated-root memo lets a new writer reuse a head this process already
  validated. `JournalReader::extension` reads one opaque extension in O(log n)
  objects. `assert_journal_storage_conformance` checks a storage, including
  torn, lost, reordered and stale-fenced writes and shared-object deletion.
  `JournalStorage::sharing()` declares Global or PerSession copies, defaulting
  to Global for compatibility. Sealed packs may defer physical removal below
  25% dead canonical bytes; membership reads stop immediately, and receipts
  count copies/bytes at physical removal. The optional retained-byte storage
  diagnostic includes deferred bodies; volatile offers both sharing modes."
- `### Changed` — "Opening a session whose ordinary and protected journals
  both have a native head no longer loads the legacy v3 records."
- `### Breaking` (pre-1.0) — "After collection a journal may reject replays
  below the inclusive current −64 revision floor with `StateConflict`. The
  current writer epoch's own last operation is preserved; pruned operations
  never apply again."
- If schema 5 ships, `### Changed` **Breaking (pre-1.0)** — "Journal schema 5
  stores small JSON array elements and messages in chunked leaves. Readers
  accept schema 4 and 5; writers continue existing schema-4 sessions in
  schema 4 until the host upgrades them explicitly. `JournalObjectKind` gains
  variants."

## Risks / Trade-offs

- **Extraction regressions.** Mitigation: move code in steps whose only diff
  is location, run the full existing suites after each, and keep the volatile
  storage's epoch and error points identical (D3).
- **Async gate vs. synchronous controls.** The volatile newtype's spin on gate
  contention is acceptable only because volatile storage never suspends;
  documented on the type.
- **Per-session copies.** FromIndex/Continue children no longer share bodies
  physically with their parents. Forge uses only `ForkSeed::Summary`
  (Forge D1 facts). Correctness is unchanged; the collection suite's
  shared-suffix cases still pass because each session's membership holds its
  closure.
- **S5 projection versus engine.** S1–S5 now run through the actual packed
  adapter and pass storage conformance. D8.4 records physical measurements
  after actual pruning; archived payload projections do not select a format
  change. Measured B/N=4000 passes the L2 gate.
- **Memo memory.** The R4 memo holds decoded snapshots; bounded by
  `validated_root_memo_bytes`.
- **CPU stays O(N) per commit** for policy and checkpoint validation (U9
  design:171-178); R4 removes only the per-writer full validation.

## Open Questions (owner)

- **Q1. Replay window (S5).** Keep operation results for the latest 64
  revisions per domain plus the current writer epoch, instead of the session
  lifetime? Recommendation: **yes.** CAS already prevents any old operation
  from applying; the runtime retries only its own last commit; lifetime
  records grow ~2.5 MB per 1,000 turns packed (6–12 MB as Forge D4 rows),
  which alone breaks R7 for long sessions.
- **Q2. Which fixture gates R7?** Recommendation: gate on the realistic
  fixture B at N=4000 against the one-blob baseline (≤ 2.0×, aim 1.5×);
  report fixture A as the worst-case lower bound without gating on it.
- **Q3. Schema-4 reader window if F1/F2 ship.** Recommendation: v4 readers
  stay for one full release after the first schema-5 writer release, and
  existing v4 sessions are only upgraded by an explicit host call. No product
  has a durable v4 writer today (Forge pins a pre-journal runtime; Nyx and
  Smith have not adopted).
- **Q4. Accept per-session physical copies** (no cross-session body sharing)
  as the price of packs? Recommendation: **yes** for Forge, which forks only
  with `ForkSeed::Summary`; a host that needs shared FromIndex suffixes can
  still deduplicate inside its own storage.

## Owner Decisions (2026-10-19)

All four open questions were decided as recommended:

- **Q1: replay window adopted.** Operation results are kept for the latest
  64 revisions plus the current writer epoch, not for the session's lifetime.
  This is a pre-1.0 contract break; CHANGELOG records it as `Breaking`.
  Task 4.4 is in scope.
- **Q2: the realistic fixture gates.** The gate is fixture B at N=4000
  against L1: at most 2.0×, aiming for 1.5×. Fixture A is reported as the
  worst case and does not gate.
- **Q3: schema-4 readers stay for one release.** If journal schema 5 is
  needed, schema-4 readers ship for one full release after the first
  schema-5 writer. Schema-4 sessions upgrade only on an explicit host request.
- **Q4: per-session copies accepted.** Identical bodies are not shared
  physically across sessions.

### Owner decision after measurement (2026-10-19)

Measurement (task 1.4) showed that an idle Forge legacy session holds two
sealed blobs (L2), not one. The owner chose to gate R7 against today's real
on-disk bytes: fixture B at N=4000 after collection must be at most 2.0× L2,
aiming for 1.5×.

| Lever | Plain | Tool |
| --- | ---: | ---: |
| S1–S5 (measured) | 1.49× L2 | 1.62× L2 |

Group 4 builds S1–S5. The format levers F1 and F2 (journal schema 5) are not
built, because storage-only levers meet the gate. They stay documented as the
path if a later gate tightens. Fixture A and the L1 ratios remain reported,
but they do not gate.

## Step 6 final verification evidence

All Cargo checks used `/Volumes/Data/tmp/refactor/c.sh`, explicit timeouts,
foreground supervision and this worktree's single `target` directory.

| Check | Result | Evidence under `rt-e6/` |
| --- | --- | --- |
| Formatting | PASS | `v3-fmt.log` |
| Clippy all targets/features, warnings denied | PASS; zero warnings | `v3-clippy.log` |
| Core all features | 314 passed | `warm-read-core.log` |
| Runtime all features | 677 passed | `v3-runtime.log` |
| Testkit all features | 441 passed | `v3-testkit.log` |
| LCM all features | 62 passed | `v3-lcm.log` |
| Bench | 10 passed; two reports ignored in debug | `v3-bench.log` |
| Primary release report | PASS; 13 cases | `v3-release-report.log` |
| C/N=4000 supplement | NOT MET: timeout | `final-c4000-attempt2.log` |
| Rust 1.86 core/runtime build | PASS | `v3-msrv.log` |
| Dependency boundaries | PASS; six commands, 13-package bench-leaf check | `dependency-boundaries.json` |
| Existing expectation integrity | PASS; one authorized change | `test-integrity.json` |
| Strict spec validation | PASS; 2 delta files, 13 requirements | `strict-spec-final.log` |

Normal commands total **1,504 passed, zero failed**. Release reports add
**1 passing tests**. Every primary before/after physical value is
identical to the pre-R4 S5 run. C/N=1000 prunes 4,002 results to 130; C/N=4000 remains NOT MET: supplement exceeded the reserved measurement time. All 13 primary cases pass.

The memo is keyed by session/head id, bounded by the existing 256 MiB estimated
budget, with LRU eviction, oversize rejection and zero-budget disablement.
Charges include bodies, decoded snapshot estimates, closure ids, parsed links
and encoding metadata. Exact closures remain eager. Parsed links are cached by
id and expected kind; missing bodies/wrong kinds still fail. New writers trim
cache-only data without changing membership. Both domains' 100-message tests
prove zero storage object calls at memoized open, warm snapshot, renew and
commit, after collection/writer change. Tested commits call load/pins/operation/
apply once each and perform zero full snapshot validations. Eviction causes one
full validation, then a hit. The unchanged future-size gate passes: largest
`commit_inner` = **4,944 / 16,384 bytes**, read = 104 bytes.

The only existing expectation change is
`crates/agent-runtime-testkit/src/conformance/journal_consumers/recovery.rs:249`,
`loads + 2` → `loads`, explicitly authorized for R5 and shared by three consumer
fixtures. Both named memory tests' assertions are unchanged (comment only).
Original GC/replay conformance, reviewer tests and the storage trait remain
byte-identical. The only new public API is `JournalReader::extension`.

The first C/N=4000 attempt was intentionally stopped to bound unused fake-provider
request/call diagnostics. An initial supervisor interruption left its child
running; cleanup retired only its disposable fixture session, and the temporary
control helper was removed. The revised runner forwards cancellation to its own
child group (timeout self-test passes). Per-turn fresh fakes use the same scripts,
capabilities and explicit cache endpoint; all 13 physical sizes remain identical.
No production data, schema, dependency or storage-trait signature changed.

NOT MET: C/N=4000 physical ratio is unavailable within the reserved budget. The B gate and all 13 primary release cases pass; B/tool also misses the optional 1.5× aim.

## Step 7 final verification evidence

Q4 now governs D2 rule 4: the storage declares Global or PerSession body
sharing (Global is the additive compatibility default). The packed reference
uses PerSession copies and lazy owner-scoped indexes. Dead-body reads stop at
membership release; pack bytes may remain below a 25% canonical dead-byte
threshold until a bounded rewrite. Physical copy/encoded-byte receipts count
actual removal. An optional storage retained-byte diagnostic preserves the
existing GC byte-evidence assertions without changing them.

All commands used `/Volumes/Data/tmp/refactor/c.sh`, individual timeouts and
this worktree's single target directory. Implementation remained unchanged during final
package verification and both release reports; later edits were documentation. No memo/R4–R6 paths were
refactored; the only engine change selects the optional storage byte diagnostic.

| Check | Result | Evidence under `rt-e7/` |
| --- | --- | --- |
| Formatting | PASS | `final-fmt.log` |
| Clippy all targets/features, warnings denied | PASS | `final-clippy.log` |
| Core all features | 314 passed | `final-core.log` |
| Runtime all features | 677 passed | `final-runtime.log` |
| Testkit all features | 444 passed | `final-testkit.log` |
| Bench | 16 passed; two reports ignored in debug | `final-bench.log` |
| Primary release report | PASS; 18 cases | `release-report.log` |
| C/N=4000 release supplement | PASS; one attempt, 5,257.4 seconds including build | `release-c4000.log` |
| Future-size gate | PASS; commit_inner 4,944 / 16,384 bytes | `final-future-sizes.log` |
| Rust 1.86 core/runtime build | PASS | `final-msrv.log` |
| Dependency boundaries | PASS; six commands and 13-package leaf check | `dependency-boundaries.json` |
| Existing-expectation integrity | PASS; 31 checks, one Q4 parameterized site | `test-integrity.json` |
| Strict spec validation | PASS; 2 deltas, 13 requirements | `strict-spec.log` |

Final package commands total **1,451 passed, zero failed**; release reports add
**two passing tests and 19 completed measured cases**. The supplemental
C/N=4000 result is **1.22863× L2**, with 16,002 → 130 retained operation
results. D8.4 and task 4.5 contain its physical bytes and write counters.
The unchanged GC source and existing packed test bodies remain byte-identical.
Only `journal_storage.rs:601` parameterizes the membership receipt expectation:
Global keeps its old values; PerSession independently accounts for session
copies and permits either domain-sharing policy. New tests reject premature
shared deletion and serving dead bodies in both modes.

NOT MET: the optional B/N=4000 1.5× L2 aim is missed under both collection
schedules. The required 2.0× gate passes; group 5 remains intentionally unbuilt.
The earlier Step-6 C/N=4000 timeout remains historical evidence; Step 7 now
supplies the completed measurement. Full report and raw JSON are
`/Volumes/Data/tmp/refactor/rt-e7/report.md` and `measurements.json`.
