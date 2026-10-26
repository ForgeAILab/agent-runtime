## ADDED Requirements

### Requirement: Canonical LCM content requires an explicit protected capability

Canonical LCM sessions SHALL use LcmCanonicalJournal::journal and commit_history with the signatures in design.md over the same exact protection/transaction domain as CheckpointStore.journal(). Existing LcmReader/LcmWriter methods SHALL remain unchanged; no default or blanket implementation MAY claim combined checkpoint atomicity. Every lookup and mutation MUST pass unchanged LcmViewAuthority/authorize_view and current owner-generation checks. Missing capability MUST fail before work when canonical mode is explicitly configured.

#### Scenario: Legacy LCM adapter has no companion capability

- **WHEN** a host keeps its existing LCM adapter without canonical configuration
- **THEN** it still compiles and uses U9 or legacy persistence
- **AND** explicit canonical configuration without the exact capability fails before provider admission

#### Scenario: Grant is revoked after opening a journal

- **WHEN** the scoped journal next reads, pins, mutates or collects
- **THEN** it rejects the operation before looking up opaque content
- **AND** neither a digest nor a saved owner epoch supplies replacement authority

### Requirement: Source and checkpoint share one publication boundary

Canonical commit_history SHALL atomically publish only the new immutable LCM entries, their integrity/source metadata and the referring exact journal head/checkpoint under expected journal/DAG revisions and writer/owner fences. Canonical Message bodies MUST exist once in the protected domain and SHALL be reused by history/checkpoint and byte-identical request references. Reusing an operation with different inputs MUST conflict; identical retry MUST return its original result.

#### Scenario: Source commit acknowledgement is lost

- **WHEN** the identical source/checkpoint transaction is retried
- **THEN** the original complete boundary is returned
- **AND** no duplicate entry, checkpoint revision or Message body is written

#### Scenario: Crash between physical entry and head writes

- **WHEN** the process restarts before the atomic publication point
- **THEN** readers see the prior complete source/checkpoint boundary
- **AND** staged content cannot appear as accepted conversation or external execution progress

### Requirement: Active and terminal source cutoffs remain distinct

Version-5 timeline-backed roots SHALL retain exact durable_len, last-terminal completed_len, source integrity and checkpoint-specific projection roots. Protected active recovery MUST include the referenced unfinished suffix; ordinary completed views SHALL use their validated terminal cutoff and redaction policy. A summary crossing the selected cutoff MUST NOT disclose or substitute incomplete source coverage. Public materialized history and planner authority SHALL remain.

#### Scenario: Active suffix contains a committed tool outcome

- **WHEN** exact recovery selects a nonterminal checkpoint
- **THEN** the canonical suffix through durable_len is restored and the tool is not rerun
- **AND** ordinary terminal readers remain on their prior completed view

#### Scenario: Current summary spans beyond an ordinary cutoff

- **WHEN** an ordinary reader selects an older completed boundary
- **THEN** it uses that boundary's retained complete projection
- **AND** it neither slices summary text nor exposes the newer raw active suffix

### Requirement: Existing LCM durability and lossless retention remain

Canonical storage SHALL preserve U6 identity/tunable validation and durable pending-summary successor proofs, U7 claim/frontier checks and U8 fork/repair semantics. Collection MUST retain all live DAG source and projection closures plus session/checkpoint/request/reader/import/transfer pins under U9 epoch fences. A live summary MUST NOT authorize deletion of its canonical sources; supersession alone MUST NOT authorize retired timeline collection.

#### Scenario: Summary DAG CAS succeeds before checkpoint successor

- **WHEN** exact recovery resumes the protected pending summary intent
- **THEN** it adopts only the verified committed successor without another summarizer call
- **AND** no below-frontier source truncation occurs

#### Scenario: Continue fork races with garbage collection

- **WHEN** ownership transfers before the child's exact seed is published
- **THEN** durable intent and transfer pins retain source closure and the parent stays fenced
- **AND** only the same authorized fork repairs the child before supersession
