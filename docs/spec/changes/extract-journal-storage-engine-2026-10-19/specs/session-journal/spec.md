## ADDED Requirements

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

Each commit, writer open, retirement, retirement authorization, durable pin change, staging change and collection batch SHALL be submitted as exactly one `JournalWrite` and applied by the storage entirely or not at all. The storage MUST compare the stored revision and writer epoch with the write's expectation inside the same transaction and MUST reject a mismatch as `StateConflict` without effect. The engine MUST NOT hold a storage transaction outside a single `apply` call. All new immutable objects of one write SHALL be delivered to the storage as one batch.

#### Scenario: A fenced-out writer's commit cannot land

- **WHEN** a writer's lease was revoked and a successor opened with a higher writer epoch
- **AND** the stale writer submits a commit whose expectation names the old epoch
- **THEN** the storage rejects the write and nothing it carried becomes visible

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

Collection SHALL examine only sessions whose roots, pins or staging changed since their last complete sweep, and its cost MUST be proportional to those sessions' members. A session's live set SHALL be the closure of its current head, its durable pins, its live process leases and its staged leaves. Storage MUST delete an object body only when no membership in any domain or session still holds it, in the transaction that removes the last membership.

#### Scenario: Clean sessions are not read

- **WHEN** one of eleven sessions in a domain committed since the last sweep
- **THEN** a collection reads members and objects of that session only

#### Scenario: An object shared across domains or sessions survives

- **WHEN** an object is a member of the protected and ordinary domains of a session, or of two sessions
- **AND** collection releases it from one of them
- **THEN** the other domain or session still reads it through a pinned lease

### Requirement: A validated root is not validated again in the same process

The engine SHALL memoize, per process and within a configured byte budget, each head it fully validated, keyed by session and head object id, together with its closure and decoded snapshot. A writer opening a memoized head MUST use the memoized closure without object reads and MUST commit on the fast path even after a writer change. A memo miss SHALL fall back to one full validation.

#### Scenario: A new runtime per turn reuses the validated head

- **WHEN** a host builds a new runtime for each turn of the same session in one process
- **THEN** only the first writer open performs a full validation
- **AND** every committed root still equals a cold encoding of its materialized state

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
- **THEN** the number of object reads is at most head + batch + map height + 1

### Requirement: Physical retained bytes are measured before they are reduced

The change SHALL provide a measurement harness that writes real SQLite files and reports, for N=100, 1000 and 4000, the physical bytes of a live session after collection (page count times page size after WAL checkpoint and VACUUM) for the legacy row, the per-object-row layout and each storage-level lever, with a per-object-kind breakdown and operation-record bytes per commit. No reduction lever SHALL be built before the harness reports its baseline. A storage-level lever MUST be preferred over a journal format change when it meets the gate.

#### Scenario: Gate evaluation

- **WHEN** the harness runs the realistic fixture at N=4000 after the storage-level levers are built
- **THEN** the report states native physical bytes as a multiple of the one-blob legacy baseline
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

Only if the owner approves design question Q1: a journal SHALL keep each operation's original result for at least the latest 64 published revisions of its domain and for the current writer epoch's own last operation, and MAY prune older operation records during collection. The inclusive replay floor MUST be `current_revision.saturating_sub(64)`: revision −64 is retained and revision −65 may be pruned. An identical replay of a pruned operation MUST fail with `StateConflict` and MUST NOT apply.

#### Scenario: A replay beyond the window

- **WHEN** an operation committed 65 revisions ago is retried with the same digest after a collection
- **THEN** the commit fails with `StateConflict`
- **AND** the head and history are unchanged


#### Scenario: The replay window edge is inclusive

- **WHEN** a session reaches revision 70 and collection runs
- **THEN** the operation that published revision 6 still returns its original result
- **AND** the operation that published revision 5 fails with `StateConflict` without applying
- **AND** the current writer epoch's last operation still returns its original result
