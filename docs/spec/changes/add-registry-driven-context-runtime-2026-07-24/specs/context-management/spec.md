## ADDED Requirements

### Requirement: Versioned context fragments

Every source of provider context SHALL contribute a versioned fragment with a
stable identity, source, kind, priority, required status, content revision,
dependency/pairing metadata, sensitivity classification, and cache class.
Contributors MUST NOT append unplanned content directly to provider requests.

#### Scenario: Ability contributes tool instructions

- **GIVEN** an activated ability supplies instructions and a tool schema
- **WHEN** the turn is prepared
- **THEN** each contribution is represented by a versioned fragment
- **AND** changing either revision changes the resulting context fingerprint

### Requirement: Authoritative immutable context plan

The context planner SHALL be the sole authority for canonical ordering,
provider messages, active tool schemas, input counts, output/reasoning reserves,
compaction, and cache hints. Provider adapters MAY serialize a context plan but
MUST NOT add uncounted context.

#### Scenario: Provider request is constructed

- **GIVEN** the runtime has resolved a model profile and activated abilities
- **WHEN** it calls the provider adapter
- **THEN** the request is derived from one immutable context plan
- **AND** every serialized context-bearing field is represented in that plan

### Requirement: Complete preflight accounting

Context accounting SHALL include message framing, roles, tool schemas, tool
calls/results, multimodal content, continuation state, provider adapter
overhead, and configured output/reasoning reserve. Counts MUST identify their
tokenizer/request-sizer revision and exact or estimated confidence.

#### Scenario: Large tool schema exceeds the budget

- **GIVEN** selected tool schemas plus required messages and output reserve
  exceed the model input limit
- **WHEN** the context plan is compiled
- **THEN** planning fails or invokes approved compaction before network I/O
- **AND** the budget report attributes tokens to the tool-schema category

### Requirement: Semantic context compaction

Compaction SHALL preserve required system/developer constraints, the current
user request, unresolved decisions, required ability instructions, and valid
tool-call/result pairs. It MAY remove expired optional fragments, bound tool
results, or summarize older history, but every summary MUST retain provenance,
covered identifiers, policy revision, sensitivity, content hash, and token
count.

#### Scenario: Old history is summarized

- **GIVEN** required content and recent turns approach the configured high
  watermark
- **WHEN** older eligible history is compacted
- **THEN** the plan targets the configured lower watermark
- **AND** the summary records exactly which messages it replaces
- **AND** no unmatched tool call or result remains

#### Scenario: Required content cannot fit

- **GIVEN** required fragments and output/reasoning reserve exceed the model
  limits even after permitted compaction
- **WHEN** planning completes
- **THEN** it fails with a structured cannot-fit report
- **AND** no required fragment is silently discarded

### Requirement: Cache-aware stable planning

The planner SHALL distinguish local compiled-context caching from provider
prompt caching, order stable fragments deterministically where the provider
contract permits, and fingerprint all inputs that affect tokenization,
serialization, activation, compaction, or cache semantics.

#### Scenario: Only current user input changes

- **GIVEN** the model profile, tokenizer, adapter, stable instructions, and
  activated tool schemas are unchanged
- **WHEN** a new user message is planned
- **THEN** the cache plan identifies the longest unchanged stable prefix
- **AND** only downstream changed blocks receive new fingerprints

#### Scenario: Tool schema revision changes

- **GIVEN** an activated tool publishes a new schema revision
- **WHEN** the next execution phase is planned
- **THEN** the activation and context fingerprints change
- **AND** the cache plan does not claim reuse beyond the changed schema block

### Requirement: Context-budgeted capability activation

The context planner SHALL provide an explicit budget for capability schemas and
instructions, and the capability resolver MUST respect that budget before
activation. The runtime MUST NOT advertise an unbounded registry or silently
truncate selected schemas to make them fit.

#### Scenario: Many relevant capabilities are installed

- **GIVEN** more relevant capability schemas exist than the model budget allows
- **WHEN** the initial activation plan is selected
- **THEN** the resolver chooses a bounded dependency-complete set
- **AND** the remaining entries stay discoverable through bounded registry
  search without entering the provider context
