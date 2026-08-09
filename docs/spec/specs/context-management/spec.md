# context-management Specification

## Purpose
TBD - created by archiving change add-registry-driven-context-runtime. Update Purpose after archive.
## Requirements
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

Every planned request SHALL carry a complete budget report: token counts per
category, the total counted input tokens, the enforced input budget, and the
output/reasoning reserves, produced by a versioned sizer whose confidence is
recorded. The planning event SHALL report the counted consumption
(`input_tokens`) and the enforced budget (`input_budget_tokens`) as distinct
values that match the budget report.

#### Scenario: Telemetry separates consumption from the enforced budget

- **GIVEN** a plan whose counted input is below its enforced budget
- **WHEN** the planning event is emitted
- **THEN** `input_tokens` equals the counted consumption
- **AND** `input_budget_tokens` equals the enforced budget rather than the
  consumption

### Requirement: Semantic context compaction

Compaction SHALL reclaim tokens in cost order, treating retained reasoning
from turns before the last user message as the cheapest reclaim: a first
stage SHALL remove such reasoning parts from message fragments before any
fragment eviction, truncation, elision, or summarization runs, while
reasoning at or after the last user message MUST be preserved for the
provider's same-turn continuation contract. Bounded truncation SHALL treat
reasoning parts like text parts.

#### Scenario: Prior-turn reasoning is reclaimed first

- **GIVEN** an over-budget history whose older assistant messages retain
  reasoning parts
- **WHEN** compaction runs
- **THEN** the prior-turn reasoning parts are removed before other content is
  evicted or summarized
- **AND** the containing messages and their other parts survive

#### Scenario: Current-turn reasoning survives compaction

- **GIVEN** an over-budget history whose assistant reasoning follows the last
  user message
- **WHEN** compaction runs
- **THEN** that reasoning is preserved

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

### Requirement: Semantic summarization responds to context pressure

The semantic summary coordinator SHALL decide to summarize from observed input
usage measured against a configured input budget, not from a count of completed
turns. A configured minimum completed-turn count SHALL remain as an eligibility
floor, and reaching it MUST NOT by itself cause summarization. Growth MUST be
measured relative to the session's opening input cost so that a larger stable
prefix does not advance the trigger. Only provider-attempt usage MAY inform the
decision; the coordinator's own summary spend MUST be excluded.

#### Scenario: A long session of small turns is not summarized
- **GIVEN** a session past the minimum turn floor
- **AND** input usage well below the configured share of the budget
- **WHEN** a turn commits
- **THEN** no summary is produced
- **AND** no summary model call is made

#### Scenario: A single large tool result triggers summarization
- **GIVEN** a session past the minimum turn floor
- **WHEN** one turn's input usage crosses the configured share of the budget
- **THEN** a summary is produced at that commit

#### Scenario: The floor protects a young session
- **GIVEN** a session below the minimum completed-turn floor
- **AND** input usage above the configured share of the budget
- **WHEN** a turn commits
- **THEN** no summary is produced

#### Scenario: A larger prefix does not advance the trigger
- **GIVEN** two sessions whose conversation bodies cost identically
- **AND** one begins with a substantially larger opening input cost
- **WHEN** both commit the same number of equivalent turns
- **THEN** neither summarizes before the other

#### Scenario: Summary spend does not feed the trigger
- **GIVEN** a session that has already produced a semantic summary
- **WHEN** the decision is evaluated again
- **THEN** the separately attributed summary usage is excluded from it

#### Scenario: A policy without an input budget is rejected
- **GIVEN** a policy whose input budget is zero
- **WHEN** it is validated
- **THEN** validation fails
- **AND** the trigger is not silently disabled

### Requirement: Comparable provider-cache expectation

The cache planner SHALL distinguish a plan with no prior provider-request
baseline from a plan compared with a predecessor. It SHALL expose an absent
expected-read value for the first request, zero when a predecessor exists but
no prefix survives under the same cache identity, and the preserved-prefix
token count when reuse is comparable. The expectation MUST retain the
planner's token-count confidence.

#### Scenario: First eligible request has no read expectation

- **GIVEN** a cache-capable session has not yet sent a provider request
- **WHEN** its first context and cache plan are built
- **THEN** the stable prefix remains eligible for future reuse
- **AND** the current request has no expected cache-read value

#### Scenario: Unchanged prefix has a comparable expectation

- **GIVEN** a prior provider request used cache plan A
- **AND** the next plan retains 42,000 tokens under the same cache identity
- **WHEN** the next request is prepared
- **THEN** its expected cache read is 42,000 tokens
- **AND** the expectation identifies the new plan and its confidence

#### Scenario: Model identity changes

- **GIVEN** a prior provider request used one model identity
- **WHEN** the next plan resolves a different provider or model identity
- **THEN** its comparable expected cache read is zero
- **AND** no token from the prior identity is represented as a missed read

### Requirement: Opaque exact provider cache identity

The context/runtime planner SHALL construct one immutable opaque cache identity
for each provider context plan. The identity MUST include provider identity, a
host-supplied CacheEndpointIdentity digest/revision, adapter partition
revision, model/profile, tokenizer and request-adapter revisions, cache
control/key/breakpoint identity, optional opaque CacheResourceIdentity, stable
provider-prefix fragment IDs and hashes, ordered stable tool
names/descriptions/schemas, registry
snapshot/view/activation revisions, and ordered stable history IDs and hashes.
The changing conversation tail MUST remain outside CacheIdentity. Consumers
MUST use the Runtime identity and MUST NOT reconstruct it from prompt text.

#### Scenario: Registry activation changes

- **GIVEN** two plans have identical visible messages and model profile
- **AND** the registry view or activation revision changes
- **WHEN** Runtime builds the second cache plan
- **THEN** it produces a different opaque cache identity
- **AND** the previous provider-cache baseline is not comparable

#### Scenario: Raw prompt content is not exposed

- **GIVEN** a host observes a cache plan or lifecycle event
- **WHEN** Runtime projects the identity
- **THEN** it exposes only the opaque digest and bounded redaction-safe
  components
- **AND** raw system, history, tool-schema, endpoint, tenant, credential, or
  provider-key content is absent

#### Scenario: Invalid identity components fail before projection

- **GIVEN** a profile, fragment, tool, endpoint, or revision contributes an
  unsafe or oversized public identity component
- **WHEN** the planner completes the cache identity for a context plan
- **THEN** planning returns a structured cache-identity error
- **AND** it attaches no cache plan or identity to the returned plan
- **AND** no manifest, lifecycle event, or provider adapter receives the
  invalid identity

### Requirement: Explicit provider boundaries are exact or absent

The context planner SHALL derive explicit provider cache boundaries beside the
canonical request rendering. When provider lane reordering would place a
changing tool or system block before a nominally stable later lane, Runtime
MUST emit no explicit marker and MUST suppress the provider read expectation
for that request. An unmarked request MUST NOT establish a provider-cache
baseline for a later request. Runtime MUST NOT attach a full CacheIdentity to
evidence for a smaller marked prefix. Stable content that the selected adapter
cannot represent on its provider wire MUST likewise suppress the marker,
expectation, and future provider baseline rather than silently counting
dropped content.

#### Scenario: Changing tool precedes stable system on provider wire

- **GIVEN** a structural plan has a stable system/history prefix and a changing
  tool schema
- **AND** the provider renders every tool before system and history
- **WHEN** Runtime derives the explicit cache boundary
- **THEN** the request carries no cache marker
- **AND** structural local reuse remains separate from an absent provider read
  expectation

#### Scenario: Unmarked request cannot seed the next expectation

- **GIVEN** request A suppresses its explicit marker because its stable prefix
  is not exactly representable on the provider wire
- **AND** request B later removes the changing or unsupported material and can
  represent an exact boundary
- **WHEN** Runtime plans request B
- **THEN** request A is not treated as a provider-cache predecessor
- **AND** request B carries no positive read expectation derived from request A

### Requirement: Structural reuse and provider lease state remain distinct

The context engine SHALL keep local compiled-context reuse, structural stable
prefix comparison, provider cache evidence, lease status, expiry, and
synthetic maintenance as separate projections. A structural prefix match MUST
NOT establish provider warmth, retention, or a cache guarantee.

#### Scenario: Local key matches after provider expiry

- **GIVEN** two plans compile to the same local context key
- **AND** the provider reports expiry for the prior cache identity
- **WHEN** Runtime reduces the second plan
- **THEN** local structural reuse remains available
- **AND** provider cache state is expired or suspended rather than warm

#### Scenario: First request has no baseline

- **GIVEN** a session has not crossed a provider preflight boundary
- **WHEN** Runtime builds its first cache plan
- **THEN** the plan has no comparable provider expectation
- **AND** planning alone does not seed the persisted predecessor

### Requirement: Cache identity changes retire comparable expectations

The planner SHALL compare provider cache plans only when every fixed opaque
identity component matches. Changes to CacheEndpointIdentity, adapter
partition, provider/model profile, adapter, tokenizer, cache control, provider
key/breakpoint/resource identity, an already-sealed stable-prefix fragment,
stable tools, registry/view/activation, or an already-sealed ordered stable
history entry MUST produce an absent or zero expectation according to the
existing first-request/comparable-baseline rules. An append-only promotion of
the prior changing tail into ordered stable history MAY preserve the prior
identity as a prefix-compatible baseline, but only the exact previously sealed
prefix is reusable. Changes limited to the current conversation tail MUST not
change CacheIdentity.

#### Scenario: Tool schema order changes

- **GIVEN** the same tool names are present with a different schema order
- **WHEN** the next provider plan is built
- **THEN** the cache identity changes
- **AND** the prior preserved-prefix expectation is not reused

#### Scenario: Same identity appends a user message

- **GIVEN** all stable identity inputs are unchanged
- **AND** only the conversation tail appends a new user message
- **WHEN** the next plan is built
- **THEN** the stable preserved-prefix expectation remains comparable
- **AND** the changed tail is represented separately from identity

#### Scenario: Prior tail becomes append-only stable history

- **GIVEN** the next plan preserves every fixed identity component and every
  previously sealed history entry
- **AND** it appends the prior changing tail to ordered stable history
- **WHEN** Runtime compares the plans
- **THEN** it may reuse only the previously sealed prefix expectation
- **AND** editing, removing, or reordering any sealed entry retires the
  comparable baseline
