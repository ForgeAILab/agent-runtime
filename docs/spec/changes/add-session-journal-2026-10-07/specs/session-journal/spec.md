## ADDED Requirements

### Requirement: Native journal capability is explicit

SessionStore and CheckpointStore SHALL add defaulted journal() discovery returning None, without changing or removing existing methods. An additive defaulted require_journal() SHALL return the discovered capability or explicit typed UnsupportedCapability::SessionJournal evidence. A native SessionJournal SHALL provide storage_id, open, read, commit, renew, close, retire and collect with the signatures and fenced semantics in design.md. Existing implementations MUST continue compiling; a legacy SnapshotJournal adapter MUST NOT claim native CAS, GC safety or delta-write performance.

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

A native commit MUST atomically publish an expected-revision, writer-fenced head and referenced checkpoint only after all immutable objects and transition data are durable. Operation IDs SHALL bind SHA-256 batch digests, exact duplicates SHALL return their original result, and different-input reuse or backwards checkpoint revisions MUST conflict. Unchanged durable history, usage prefixes, request bodies and diagnostic leaves SHALL NOT be rewritten after bootstrap.

#### Scenario: Head publication acknowledgement is lost

- **WHEN** a committed batch is retried with the same operation and digest
- **THEN** the original committed boundary is returned without another transition
- **AND** a committed provider or tool outcome is reused

#### Scenario: History length grows with a fixed append

- **WHEN** N=100, 1000 and 10000 histories each receive the same one-message append
- **THEN** new-body bytes are independent of unchanged-prefix length
- **AND** counted writes separately report reference paths, changing diagnostics and opaque extension replacements

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

#### Scenario: Fork or resume races with sweep

- **WHEN** a collector marks an object and a resume or FromIndex fork acquires its root
- **THEN** either the pin is established before deletion or open fails before any dereference
- **AND** a successful pin or committed child prevents that object's deletion

#### Scenario: Summary fork retires the parent

- **WHEN** the successor seed and supersession are durable, all transfer/read pins close and host retention authorizes retirement
- **THEN** objects reachable only from the retired parent may be collected
- **AND** live child and LCM summary-source objects remain reachable
