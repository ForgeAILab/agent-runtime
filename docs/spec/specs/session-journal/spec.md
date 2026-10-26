# session-journal Specification

## Purpose
Native session persistence through immutable, content-addressed journal objects:
fenced writers publish complete heads and referenced checkpoints, a
storage-generic engine runs the journal semantics over host storage, and
collection reclaims unreachable content without breaking pinned readers or
concurrent root ownership.

## Requirements
### Requirement: Native journal capability is explicit

SessionStore and CheckpointStore SHALL add defaulted journal() discovery returning None, without changing or removing existing methods. An additive defaulted require_journal() SHALL return the discovered capability or explicit typed UnsupportedCapability::SessionJournal evidence. A native SessionJournal SHALL provide storage_id, open, read, commit, renew, close, retire and collect with the signatures and fenced semantics in the archived change's design (`docs/spec/changes/archive/2026-10-26-add-session-journal-2026-10-07/design.md`). Existing implementations MUST continue compiling; a legacy SnapshotJournal adapter MUST NOT claim native CAS, GC safety or delta-write performance.

#### Scenario: Existing store implements only current methods

- **WHEN** an unchanged SessionStore/CheckpointStore pair is composed
- **THEN** it compiles and selects materialized legacy persistence
- **AND** explicit native capability requests fail with typed unsupported evidence
- **AND** schema guards requiring migration are reported by the coordinated consumer gate

#### Scenario: Native data loses its adapter

- **WHEN** a session has a native head but the configured store no longer supplies its journal capability
- **THEN** resume fails explicitly before provider or tool work
- **AND** no stale legacy snapshot is used as fallback

### Requirement: Immutable deltas publish one complete boundary

A native commit MUST atomically publish an expected-revision, writer-fenced head and referenced checkpoint only after all immutable objects and transition data are durable. Operation IDs SHALL bind SHA-256 batch digests, exact duplicates SHALL return their original result while the operation record is within the retained replay window, and different-input reuse or backwards checkpoint revisions MUST conflict. Unchanged durable history, usage prefixes, request bodies and diagnostic leaves SHALL NOT be rewritten after bootstrap.

#### Scenario: Head publication acknowledgement is lost

- **WHEN** a committed batch is retried with the same operation and digest
- **THEN** the original committed boundary is returned without another transition
- **AND** a committed provider or tool outcome is reused

#### Scenario: History length grows with a fixed append

- **WHEN** N=100, 1000 and 4000 histories each receive the same one-message append
- **THEN** new-body bytes are independent of unchanged-prefix length
- **AND** counted writes separately report reference paths, changing diagnostics and opaque extension replacements
- **AND** an attempted N=10000 case is reported as inapplicable while the 4096-entry cache-identity history limit rejects it before provider I/O

### Requirement: Requests and protected state are referenced exactly

Referenced checkpoints SHALL retain the exact final request, prepared invocations, committed raw outcomes, history/usage/extensions, deadlines, identity counters, active history index and scoped event watermarks through protected object references. Reconstruction MUST validate their complete closure and current operation/transition checks without replanning or deriving execution content from redacted records. Ordinary projected objects SHALL remain subject to their existing sensitivity policy and MUST NOT expose protected objects by digest.

#### Scenario: Projected request contains a summary and signed reasoning

- **WHEN** CallingModel is restored from references
- **THEN** the final ordered messages, schemas, reasoning signatures, settings and cache boundaries equal the frozen request
- **AND** current planner or summary-model policy is not run to reconstruct it

#### Scenario: Host preserves opaque JSON map insertion order

- **WHEN** existing operation or preparation fingerprints depend on opaque JSON map order
- **THEN** journal-json-1 binds and restores that order as explicit metadata while payload object keys stay canonical
- **AND** a reader unable to represent the recorded order fails closed before execution instead of changing fingerprints

#### Scenario: Redacted terminal history differs in bytes

- **WHEN** ordinary and protected terminal records have compatible existing boundary evidence but different content digests
- **THEN** ordinary completed history remains host-policy canonical history
- **AND** only compatible protected extension namespaces are overlaid under existing identity, history-role, usage, manifest-counter and revision checks

### Requirement: Journal corruption cannot repeat external work

Readers MUST validate framing, object digests, sequence, predecessor evidence and published-root closure. Unpublished torn tails MAY be quarantined or trimmed only under a writer fence; corruption within a published boundary MUST fail closed as StateConflict and require verified host repair, rather than rolling back execution progress.

#### Scenario: Process dies halfway through the last unpublished frame

- **WHEN** the next writer opens the journal
- **THEN** the last published complete boundary is returned
- **AND** repair trims or quarantines only the unpublished tail before idempotent retry

#### Scenario: Published tool outcome object is missing

- **WHEN** checkpoint reconstruction detects the missing object
- **THEN** recovery fails before tool/provider I/O
- **AND** it does not replay the tool from an older checkpoint

### Requirement: Collection respects concurrent root ownership

Opening a root SHALL pin its transitive closure atomically; commits, retirement, durable transfer intents and collection SHALL share a root/pin epoch fence. Collection MUST recheck reachability before deletion and retain every live, retained, staged, migration, child or pending-fork root. Supersession alone MUST NOT authorize deletion and unsupported collection MUST retain content.

#### Scenario: Retired native session retains stale v3 records

- **WHEN** an imported session is retired with its original v3 records still present
- **THEN** opening its latest root with either read-only or writer intent fails explicitly as StateConflict, before and after collection
- **AND** resume, fork, read-only views, session-exists, child recovery/rebind and watermark refresh propagate that error without loading legacy records
- **AND** existing pinned readers and explicitly retained historical roots remain readable while their complete closure is available

#### Scenario: Fork or resume races with sweep

- **WHEN** a collector marks an object and a resume or FromIndex fork acquires its root
- **THEN** either the pin is established before deletion or open fails before any dereference
- **AND** a successful pin or committed child prevents that object's deletion

#### Scenario: Summary fork retires the parent

- **WHEN** the successor seed and supersession are durable, all transfer/read pins close and host retention authorizes retirement
- **THEN** objects reachable only from the retired parent may be collected
- **AND** live child and LCM summary-source objects remain reachable

### Requirement: Journal semantics run over a host storage trait

The runtime SHALL provide a public `JournalEngine<S: JournalStorage>` that implements `SessionJournal` with the commit, projection, validation, reachability, dependency-index, lease, retirement and collection semantics of the reference journal. A host MUST be able to obtain a conforming native journal by implementing only `JournalStorage`: membership-scoped object reads, session records, operation records, durable pins, the retained-root catalog, dirty-session paging, physical compaction and one atomic `apply` per write. `InMemorySessionJournal` SHALL be this engine over `VolatileJournalStorage`, with unchanged public items and behavior.

#### Scenario: Existing reference suites run over the extracted engine

- **WHEN** the existing core, runtime and testkit journal suites run against `InMemorySessionJournal`
- **THEN** every suite passes without any edited expectation
- **AND** root epochs, error classes and commit cuts are observed at the same operations as before extraction

#### Scenario: A host supplies only storage

- **WHEN** a host wraps its `JournalStorage` in a protected and an ordinary `JournalEngine`
- **THEN** both engines pass the protected, ordinary and collection conformance suites
- **AND** the host implements no projection, validation, reachability or dependency logic

### Requirement: Every journal write is one fenced storage transaction

Each commit, writer open, retirement, retirement authorization, durable pin change, staging change and collection batch SHALL be submitted as exactly one `JournalWrite` and applied by the storage entirely or not at all. The storage MUST compare the stored revision, writer epoch and control generation with the write's `JournalFence` expectation inside the same transaction and MUST reject a mismatch as `StateConflict` without effect. An absent record MUST match the zero fence. Every apply MUST replace the session record with generation exactly one greater than expected; skipped generations and overflow MUST conflict without effect. Revision and writer epoch semantics MUST remain unchanged. The engine MUST NOT hold a storage transaction outside a single `apply` call. All new immutable objects of one write SHALL be delivered to the storage as one batch.

#### Scenario: A fenced-out writer's commit cannot land

- **WHEN** a writer's lease was revoked and a successor opened with a higher writer epoch
- **AND** the stale writer submits a commit whose expectation names the old epoch
- **THEN** the storage rejects the write and nothing it carried becomes visible

#### Scenario: A stale control write cannot overwrite another engine's control state

- **WHEN** one engine prepares a write and another engine releases a pin, authorizes retirement, records a retirement-dependent candidate or collects that session
- **AND** the stored revision and writer epoch remain unchanged
- **THEN** the first engine's stale generation conflicts without changing any record, object, membership, pin, catalog or operation
- **AND** a caller retry reloads the new fence; clones of one engine serialize preparation through apply

#### Scenario: A commit cut leaves no partial state

- **WHEN** a write is cut after objects, checkpoint, batch, pins, record or operation record
- **THEN** a fresh engine over the same storage sees the prior head and no new membership, pin or operation record
- **AND** a cut after the transaction commits is resolved by the operation record on retry

### Requirement: Leases and close are process-local

Lease, reader-pin and writer-slot registries and the root/pin epoch SHALL live in engine memory. `close`, lease drop and `revoke_writer` MUST complete without storage I/O. A writer open MUST advance the durable writer epoch in a storage write before returning a lease.

#### Scenario: Close completes while a write is in progress

- **WHEN** a session's persistence handle is dropped while another task holds the engine's write gate
- **THEN** its `close` completes synchronously under `now_or_never`
- **AND** the session's writer slot is free for the next writer open

#### Scenario: A restarted process cannot reuse an old writer

- **WHEN** a process exits with a writer lease open and a new process opens the same session as writer
- **THEN** the new lease carries a strictly greater writer epoch
- **AND** no write carrying the old epoch can be applied

### Requirement: Collection is incremental and safe for shared objects

Collection SHALL examine only sessions whose roots, pins or staging changed since their last complete sweep, and its cost MUST be proportional to those sessions' members. A session's live set SHALL be the closure of its current head, its durable pins, its live process leases and its staged leaves. Storage MUST declare `PerSession` or `Global` physical body sharing. It MUST preserve reads through every remaining membership and MUST never serve a non-member. `PerSession` copies MAY be reclaimed independently across sessions; a `Global` body MUST survive until its last membership is released. Apply receipts SHALL count canonical bytes of physical body copies removed, at physical removal time. Dead bytes MAY remain in sealed packs below a documented threshold of at most 25% of canonical body bytes; reaching the threshold, emptying a sharing scope, or collecting a retired session MUST purge the affected dead bytes.

#### Scenario: Clean sessions are not read

- **WHEN** one of eleven sessions in a domain committed since the last sweep
- **THEN** a collection reads members and objects of that session only

#### Scenario: An object shared across domains or sessions survives

- **WHEN** an object is a member of the protected and ordinary domains of a session, or of two sessions
- **AND** collection releases it from one of them
- **THEN** the other domain or session still reads it through a pinned lease


#### Scenario: Deferred dead bodies are unreadable and eventually counted

- **WHEN** a membership is released while its dead body remains below a pack's rewrite threshold
- **THEN** reads through that membership return no body immediately
- **AND** the receipt counts the body only when a later apply rewrites or deletes that pack
- **AND** emptying its sharing scope removes and counts all remaining dead copies

### Requirement: A validated root is not validated again in the same process

The engine SHALL memoize, per process and within a configured byte budget, each head it fully validated, keyed by session and head object id, together with its closure and decoded snapshot. A writer opening a memoized head MUST use the memoized closure without object reads and MUST commit on the fast path even after a writer change. A memo miss SHALL fall back to one full validation. Before returning a memo-served reader operation, the engine MUST confirm through storage once for the operation that its exact root is still the stored current head or is still held by a durable historical pin. A mismatch MUST discard the speculative result and retry through membership-scoped storage with the same reader limits. Successful commits SHALL drop unheld predecessor proofs. LRU touches MUST NOT scan all entries.

#### Scenario: A new runtime per turn reuses the validated head

- **WHEN** a host builds a new runtime for each turn of the same session in one process
- **THEN** only the first writer open performs a full validation
- **AND** every committed root still equals a cold encoding of its materialized state

#### Scenario: Another engine releases a warm reader's membership

- **WHEN** another engine advances the stored head or releases the durable historical pin and collects the selected closure
- **THEN** a warm direct, snapshot or extension read falls back to storage and conflicts exactly as a cold reader does
- **AND** an unchanged held root needs one metadata check per reader operation and zero body reads

#### Scenario: A busy session does not retain every superseded proof

- **WHEN** one session repeatedly commits while another session's current proof fits alongside its current proof
- **THEN** unheld predecessor proofs are removed before admitting the new proof
- **AND** the other session's current proof survives without being touched

### Requirement: Native-first open

`SessionPersistence::open` SHALL load the legacy v3 records only when no native head exists in any configured domain or when a partial import repair is possible. When both native heads exist it MUST NOT call the legacy `load` or `load_latest`.

#### Scenario: An imported session is reopened

- **WHEN** a session imported from v3 is opened again with both journal capabilities
- **THEN** no legacy load is performed
- **AND** a session whose ordinary import publication is missing still loads the v3 pair once for repair

### Requirement: Bounded single-extension read

`JournalReader::extension(namespace)` SHALL return one opaque extension value of the selected head by reading the head, its batch, the extension map nodes on the path to the namespace and the value object only. It MUST NOT read history, usage, manifests or the checkpoint.

#### Scenario: Probe a large session

- **WHEN** a host reads the LCM component extension of a session with 4,000 messages and 10,000 extension namespaces
- **THEN** the number of object reads is at most head + batch + map height + 1 (height counted in map levels; a nonempty descriptor has `JournalMap.height + 1` levels)

### Requirement: Physical retained bytes are measured before they are reduced

The repository SHALL provide a measurement harness that writes real SQLite files and reports, for N=100, 1000 and 4000, the physical bytes of a live session after collection (page count times page size after WAL checkpoint and VACUUM) for the legacy row, the per-object-row layout and each storage-level lever, with a per-object-kind breakdown and operation-record bytes per commit. No reduction lever SHALL be built before the harness reports its baseline. A storage-level lever MUST be preferred over a journal format change when it meets the gate.

#### Scenario: Gate evaluation

- **WHEN** the harness runs the realistic fixture at N=4000 after the storage-level levers are built
- **THEN** the report states native physical bytes as a multiple of the observed two-blob legacy baseline (L2)
- **AND** a journal format change is built only if that multiple exceeds 2.0 or the owner requires more

### Requirement: Storage receives each write's objects as one batch

The engine SHALL hand all new immutable objects of a write to the storage in one `JournalObjectBatch`, with each object's kind, so a host can compress and seal them as one unit and index them inside it. Storage MUST verify a read object's full id against its bytes before returning it when it indexes objects by a shortened id.

#### Scenario: A packed storage serves reads

- **WHEN** a storage stores each write as one compressed, sealed pack with an internal short-id index
- **THEN** it passes the storage conformance suite
- **AND** a read of a missing or mismatching id fails instead of returning another object's bytes

### Requirement: Storage conformance rejects broken storages

The testkit SHALL export `assert_journal_storage_conformance`, which runs the existing journal suites over `JournalEngine<S>` plus storage-level and restart cases. A torn transaction, a lost write, a reordered commit, a storage ignoring the writer epoch and a storage deleting a body still held by another membership MUST each fail a named case.

#### Scenario: A lost write is detected

- **WHEN** a storage acknowledges a commit without persisting it
- **THEN** the restart case of the suite fails because a fresh engine does not see the acknowledged head

### Requirement: Replay results are kept for a bounded window

A journal SHALL keep each operation's original result for at least the latest 64 published revisions of its domain and for the current writer epoch's own last operation, and MAY prune older operation records during collection. The inclusive replay floor MUST be `current_revision.saturating_sub(64)`: revision −64 is retained and revision −65 may be pruned. An identical replay of a pruned operation MUST fail with `StateConflict` and MUST NOT apply.

#### Scenario: A replay beyond the window

- **WHEN** an operation committed 65 revisions ago is retried with the same digest after a collection
- **THEN** the commit fails with `StateConflict`
- **AND** the head and history are unchanged


#### Scenario: The replay window edge is inclusive

- **WHEN** a session reaches revision 70 and collection runs
- **THEN** the operation that published revision 6 still returns its original result
- **AND** the operation that published revision 5 fails with `StateConflict` without applying
- **AND** the current writer epoch's last operation still returns its original result

### Requirement: Isolated session collection failures

Collection SHALL silently skip state/fence conflicts and isolate other
non-retryable failures in a session's collection
reads or its own atomic apply, preserving its membership, retirement, replay rows
and durable dirty mark, and SHALL continue remaining sessions. The report SHALL
contain sorted unique session ids whose state could not be read or validated
in `unreadable_sessions`, excluding conflicts, without object ids or content.
Local dirty hints SHALL survive both conflict skips and isolation so repair can be retried next
call, with at most one failing load per session in that call. An unreadable
retirement dependency SHALL leave its source dirty and uncollected as well.
Retryable failures and domain-wide discovery, epoch or poisoned engine lock
failures SHALL abort;
previously acknowledged session transactions need not roll back.

#### Scenario: An unreadable session does not stop collection

- **GIVEN** three dirty sessions and a non-retryable, non-conflict load failure for the middle one
- **WHEN** the shared engine collects the domain
- **THEN** both readable sessions are collected and the middle one is unchanged and reported
- **AND** another collection retries the middle session once and collects new garbage elsewhere

#### Scenario: A session apply fails without effect

- **WHEN** a session's atomic collection apply returns a non-retryable, non-conflict error
- **THEN** its objects, replay rows and dirty mark remain unchanged
- **AND** it is reported while collection continues with later sessions

#### Scenario: Retryable storage failure

- **WHEN** a session load or apply returns a retryable error before any session is acknowledged
- **THEN** collection returns that error without changing durable session state
- **AND** local dirty hints remain available for a later retry

#### Scenario: Session changed between load and apply

- **WHEN** a session read or apply returns a non-retryable state/fence conflict
- **THEN** collection skips it without listing it in `unreadable_sessions`
- **AND** its durable state and retry hint survive while unrelated collection continues
- **AND** a dependency conflict also leaves its source dirty without reporting either as unreadable

#### Scenario: Packed SQLite writer contention

- **GIVEN** a second SQLite connection holds a write lock
- **WHEN** packed apply encounters BUSY or LOCKED, including an extended code
- **THEN** it returns a retryable error and collection aborts for retry
- **AND** a failed transaction that did not commit has no durable effect
- **AND** only ambiguous COMMIT I/O errors receive retryability on account of I/O

#### Scenario: Local engine lock poisoned during collection

- **WHEN** a session read or apply encounters a poisoned engine registry or memo lock
- **THEN** collection returns the domain-wide error rather than isolating a session
- **AND** unacknowledged session state and retry hints survive

#### Scenario: Unreadable dependency candidate

- **WHEN** collection cannot read a dependency target with a non-retryable, non-conflict error
- **THEN** the target and its source are reported and kept dirty without releasing source membership
- **AND** independent sessions can still be collected

#### Scenario: Unknown stored transfer-index revision leaks safely

- **GIVEN** a closed session whose stored transfer-index revision is unknown and whose orphan bodies could otherwise be collected
- **WHEN** collection visits that session and later independent garbage
- **THEN** it reports the unknown session and preserves all of its bytes while reclaiming only the independent garbage
- **AND** a repeated call reports the unknown session without releasing more bytes
- **AND** retirement of that session and a possible transfer target remains refused, with both current closures readable

### Requirement: Fail-closed domain diagnostics and retirement

Retained-byte diagnostics SHALL return exact accounted bytes or an error rather
than silently omit unreadable sessions. Retirement SHALL reject an unreadable
candidate because its dependency metadata may block the requested retirement.
Domain-wide storage discovery, retained-byte accounting and physical compaction
errors SHALL remain errors because these interfaces cannot identify a safe
per-session partial result.

#### Scenario: An unreadable candidate might depend on a retirement target

- **WHEN** retirement encounters an unreadable dependency candidate
- **THEN** it fails without retiring the target

#### Scenario: Retained-byte diagnostic cannot account for a session

- **WHEN** storage cannot enumerate or read a session's member bodies for the diagnostic
- **THEN** the engine returns the error rather than a successful partial byte count

### Requirement: Storage authority and identity conformance

The reusable storage suite SHALL reject a cache that bypasses membership checks
on hits, a lookup that substitutes another id sharing a four-byte prefix, and
`members()` that includes released or never-admitted ids. Each defect SHALL have
a test-only adapter that fails its corresponding named case. Immediate physical
removal of released bodies SHALL remain permitted.

#### Scenario: Warm cache after release

- **WHEN** a member body is read to warm the cache and its membership is released
- **THEN** repeated reads through that session and a never-admitted session return no body

#### Scenario: Four-byte prefix collision

- **GIVEN** two valid different object ids sharing their first four bytes
- **WHEN** both are stored and requested in different orders including duplicates
- **THEN** each request returns exactly its own body

#### Scenario: Exact membership enumeration

- **WHEN** a body is held elsewhere but released from a session or never admitted there
- **THEN** that session's members exclude it across roles and storage domains
