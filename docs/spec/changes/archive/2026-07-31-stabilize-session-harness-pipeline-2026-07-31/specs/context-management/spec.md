## ADDED Requirements

### Requirement: Conversation placement is independent of classification

The context planner SHALL use fragment kind for accounting and compaction
policy without using it to reorder canonical conversation messages. Messages
within the conversation lane MUST reach the provider in their original
sequence.

#### Scenario: Tool continuation is planned
- **GIVEN** canonical history contains a user message, an assistant tool call,
  and its tool result in that order
- **WHEN** the next provider request is planned
- **THEN** the provider request retains exactly that role and message order
- **AND** accounting still classifies user input, history, and tool results
  separately

### Requirement: Active tool exchanges are atomic

The planner and compactor SHALL represent every assistant tool-call message and
all matching results as one exchange supporting multiple call IDs. Every
message from the latest user input through the active continuation MUST remain
required until the turn reaches a terminal state.

#### Scenario: Assistant requests parallel tools
- **GIVEN** one assistant message contains several tool calls
- **AND** matching results are appended in canonical order
- **WHEN** context pressure requires compaction
- **THEN** the complete assistant message and all matching results survive
  together or are removed together as part of an older completed turn
- **AND** the active-turn exchange is never compacted

### Requirement: Planning state is session scoped

Mutable planning metadata SHALL belong to one session execution context,
including prior cache plans, compaction state, and activation revisions.
Planning in one session MUST NOT affect cache or compaction outcomes in another
session.

#### Scenario: Two sessions share one runtime
- **GIVEN** two sessions execute interleaved turns through one runtime
- **WHEN** one session compacts and changes its cache prefix
- **THEN** the other session observes neither outcome
- **AND** each plan is compared only with its own preceding plan

### Requirement: Compaction results are returned atomically

A compactor SHALL return compacted fragments and their outcome as one owned
result. A plan that did not invoke compaction MUST receive a fresh no-op
outcome and MUST NOT read metadata through shared mutable side channels.

#### Scenario: A compacted plan is followed by a fitting plan
- **GIVEN** one plan invoked compaction
- **WHEN** a later plan already fits without compaction
- **THEN** the later plan records no compaction outcome
- **AND** no prior session or turn outcome is reused

### Requirement: Structural and semantic compaction are distinct

The deterministic context package SHALL perform only structural selection,
bounding, provenance validation, and budget enforcement. Model-assisted
semantic summaries MUST be coordinated above it and re-enter planning as
explicit provenance-carrying summary fragments.

#### Scenario: Old history receives a semantic summary
- **GIVEN** a harness coordinator selects complete old turn groups
- **WHEN** it stores originals and obtains a model-generated summary
- **THEN** the deterministic planner validates the summary's coverage,
  sensitivity, hash, and budget
- **AND** the context package performs no provider or network call itself
