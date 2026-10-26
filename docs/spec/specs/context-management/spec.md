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
values that match the budget report. The default offline sizer SHALL count a
reasoning part's opaque signature, at its own configurable
characters-per-token ratio, because providers bill the continuation state
they hand back.

#### Scenario: Telemetry separates consumption from the enforced budget

- **GIVEN** a plan whose counted input is below its enforced budget
- **WHEN** the planning event is emitted
- **THEN** `input_tokens` equals the counted consumption
- **AND** `input_budget_tokens` equals the enforced budget rather than the
  consumption

#### Scenario: A replayed search step is counted

- **GIVEN** history holding a Gemini search step stored as a redacted
  reasoning part with a 46,704-character signature
- **WHEN** the default sizer prices that part
- **THEN** it counts the signature at six characters per token, 7,784 tokens,
  rather than zero

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

### Requirement: Image parts are projected per request

The context planner SHALL project image parts before sizing and before
computing cache identity. Canonical history SHALL remain unchanged. A
projected image SHALL become a text part
`[image <id>: <mime>[ <w>×<h>] not included in this request]`, where `<id>`
is the first 8 lowercase hex digits of the SHA-256 digest of its data URL.
Dimensions SHALL be included when the PNG, JPEG, GIF, or WebP header
decodes. When the profile's `input_modalities` lacks `image`, every image
part SHALL be projected. Otherwise, when `ContextPolicy` carries an
`ImageWindow { inline: K }`:

- Every image in the newest user message SHALL stay inline and SHALL NOT be
  counted.
- The remaining images, numbered `0..T` in order, SHALL stay inline exactly
  when `i >= K * floor(max(T - K, 0) / K)`.

The plan SHALL report how many images it projected, separately for text-only
and window reasons.

#### Scenario: Text-only profile

- **GIVEN** a profile with `input_modalities = [text]` and history holding a
  tool-result image
- **WHEN** a plan is built
- **THEN** no planned message contains an image part
- **AND** the plan reports one image projected as text-only

#### Scenario: Stable prefix between advances

- **GIVEN** `K = 10` and 14 window-counted images in history
- **WHEN** a 15th image is appended and a new plan is built
- **THEN** all 15 stay inline
- **AND** the planned messages before the new one are identical to the
  previous plan's

#### Scenario: Window advance

- **GIVEN** `K = 10` and 20 window-counted images
- **WHEN** a plan is built
- **THEN** images 10 through 19 are inline and 0 through 9 are placeholders

#### Scenario: Newest user message is exempt

- **GIVEN** `K = 3` and a newest user message with 6 images
- **WHEN** a plan is built
- **THEN** all 6 are inline

### Requirement: First changed cache-prefix fragment is observable

Cache plans and CachePlanChanged SHALL expose optional first_changed_fragment
evidence from their committed predecessor comparison. The diagnostic MUST name
the earliest changed ID, content hash, or cache class within an established
stable prefix in plan-segment order (Instructions before Capabilities), not
provider wire byte order. When IDs differ and the predecessor ID is absent
from the current leading Stable segments, it MUST name that removed
predecessor ID, including when a neighbour or tail occupies its old position.
It SHALL otherwise use the current ID, or the predecessor ID when no current
item exists. It MUST NOT change cache identity, fingerprints, provider
wire output, or canonical cache metrics.

#### Scenario: A stable instruction changes

- **GIVEN** two eligible requests share the same non-fragment identity partition
- **WHEN** the second changes one stable instruction's content
- **THEN** first_changed_fragment names that instruction at the first differing position
- **AND** downstream unchanged fragments are not reported as the cause

#### Scenario: Prefix segment is removed or changes class

- **WHEN** a comparable request removes the final stable-prefix segment or makes it ephemeral
- **THEN** the diagnostic names the removed predecessor ID or the changed current ID respectively
- **AND** no sensitive fragment content is emitted

#### Scenario: Prefix deletion leaves a neighbour or ephemeral tail

- **GIVEN** a comparable predecessor has stable segments a, b, c followed by an Ephemeral user tail
- **WHEN** the current plan removes the final, middle, or first stable segment
- **THEN** the diagnostic names c, b, or a respectively, even if a neighbour or user tail occupies the removed segment's old position
- **AND** changing or retaining the tail does not change the deletion attribution

#### Scenario: New tail does not invalidate an established prefix

- **WHEN** the prior stable prefix remains identical and only an appended or newly promoted tail is added
- **THEN** first_changed_fragment is absent
- **AND** existing prefix metrics keep their current semantics

#### Scenario: No fragment cause is attributable

- **WHEN** a request has no usable predecessor, a suppressed/unsupported boundary, or a changed non-fragment identity partition
- **THEN** first_changed_fragment is absent even if aggregate invalidation is nonzero
- **AND** the runtime does not fabricate a fragment cause

#### Scenario: Fragment ID is unsafe for telemetry

- **WHEN** a changed ID violates the new diagnostic field's bounded safe-ID rules
- **THEN** the event omits first_changed_fragment
- **AND** the planner's fragment identity and all cache behavior remain unchanged

### Requirement: Authorized timeline ownership
LcmWriter SHALL expose defaulted claim(view, owner, generation). The default
records nothing: after authority validation it SHALL return Claimed for an empty
timeline and Unsupported for a populated one. A fresh session MUST reject
populated or owned timelines of a claim-aware store with typed TimelineOwned
unless the host selects Adopt, Fork or Retire. Replacement and continuation
bindings MUST pass unchanged LcmViewAuthority checks.

#### Scenario: Populated timeline policies
- **WHEN** a new owner resolves a timeline with committed nodes
- **THEN** default construction fails with TimelineOwned
- **AND** Adopt claims the next generation first and then seeds canonical history from the claimed timeline projection
- **AND** Fork claims a fresh timeline with a summary seed
- **AND** Retire claims an empty replacement while preserving old immutable data

#### Scenario: Adopt never discards a seed
- **WHEN** a host requests Adopt with non-empty seed history
- **THEN** construction returns typed Conflict and no timeline is claimed

#### Scenario: Restart after leaf commit
- **WHEN** a new session identity encounters a committed leaf
- **THEN** it requires explicit policy and never starts appending at sequence zero

#### Scenario: Claim-unaware store
- **WHEN** a store uses the default claim method
- **THEN** a session creates and runs normally on an empty timeline
- **AND** the session that holds the binding continues on its populated timeline, live and after resume
- **AND** a fresh binding of a populated timeline forks to a resolver-supplied replacement with a summary seed
- **AND** explicit Adopt returns typed ForkRequired, so no timeline is adopted without fencing

#### Scenario: Pre-existing unclaimed timeline
- **WHEN** a session whose validated state predates ownership resumes with Adopt
- **THEN** it claims generation one, rebuilds its state against the claimed timeline and continues
- **AND** without a policy the resume fails with TimelineOwned and no owner

#### Scenario: Stale ownership epoch
- **WHEN** another owner acquires a newer claim generation
- **THEN** the claim bumps the store revision
- **AND** the old owner receives TimelineOwned before provider admission or a timeline commit
- **AND** a claim-aware store rejects any mutation whose view fence is not the current owner with TimelineOwned

#### Scenario: Ephemeral LCM
- **WHEN** an ephemeral session runs with LCM configured
- **THEN** its LCM uses a volatile in-memory timeline
- **AND** the host's durable timeline is neither claimed nor written

#### Scenario: Bounded fork summary
- **WHEN** a Fork policy renders the projected summary of a timeline
- **THEN** the rendering is capped by a configurable bound and keeps the newest tail
- **AND** sources classified Secret never enter it

### Requirement: Frontier-safe reconcile
Reconciliation MUST never truncate below max(active_node.range.end)+1. Divergence
below that frontier SHALL return typed LcmDivergence { frontier, at }, with the
same Adopt, Fork or Retire policy options at a host recovery boundary.

#### Scenario: Divergent summarized source
- **WHEN** canonical history differs below the active summary frontier
- **THEN** no truncation occurs and LcmDivergence identifies frontier and at
- **AND** committed entries and nodes remain unchanged

#### Scenario: Concurrent leaf commit
- **WHEN** a leaf advances the frontier after the runtime check
- **THEN** the store atomically rejects truncation with typed RangeOverlap

### Requirement: Shared LCM Request Sizing
By default LCM SHALL measure immutable messages and projected summary messages
with the planner RequestSizer, including reasoning, tool arguments/results and
framing. A host-supplied LcmSizer MUST be kept as given; builder composition
MUST NOT replace it.

#### Scenario: Tool and reasoning parity
- **WHEN** the planner and LCM measure the same message using the shared sizer
- **THEN** the measured counts MUST match, including structured tool content.

#### Scenario: Host sizer kept
- **WHEN** a host configures its own LcmSizer and composes LCM through the builder
- **THEN** LCM MUST size entries with the host sizer, and only the default sizer is replaced by the planner RequestSizer.

### Requirement: Working Set Budget and Pressure
An optional host WorkingSetPolicy SHALL cap planner input at the lesser of the
resolved input window and hard_tokens. LCM SHALL evaluate pressure against
target_tokens minus measured fixed overhead from the last successful plan, but
never against less than 25% of target_tokens. When the floor applies LCM MUST
emit a typed OverheadExceedsTarget diagnostic; overhead MUST be re-measured at
the next plan.

#### Scenario: Independent target
- **WHEN** a large provider window is configured with a smaller working set
- **THEN** LCM MUST compact according to the working-set target and the planner MUST enforce the hard cap.

#### Scenario: Overhead at or above target
- **WHEN** measured fixed overhead leaves less than the floor, including a stale persisted overhead
- **THEN** LCM MUST use the floor, MUST NOT refuse the turn for pressure alone, and MUST emit the typed diagnostic.

#### Scenario: Absent policy
- **WHEN** no working set is configured
- **THEN** existing provider-budget selection MUST remain available.

### Requirement: Bounded Reclaiming Summaries
LCM SHALL separate leaf source targets from summary output caps, default the
summary maximum ratio to 0.25, and escalate when measured reclaim is below
min_reclaim_ratio. Every accepted model summary MUST meet both ratios and strict
shrink. The deterministic fallback MUST strictly shrink within
min(deterministic_token_cap, source - 1) and is not ratio-capped. Leaf selection
MUST permit a full turn pair and MUST widen a leaf the fallback cannot shrink
through the next turn. A summary that still cannot fit under hard pressure MUST
end the round as a structured cannot_fit. Hard pressure SHALL derive rounds with
one shared formula from overage and expected reclaim, fixed once per admission
epoch and never above MAX_DERIVED_HARD_ROUNDS (64).

#### Scenario: Weak reclaim escalates
- **WHEN** a model response is smaller than its source but violates a ratio
- **THEN** LCM MUST escalate rather than commit it.

#### Scenario: Tiny oldest turn
- **WHEN** the oldest turn is smaller than the deterministic fallback's framing and later turns exceed the leaf target
- **THEN** hard compaction MUST widen that leaf and commit progress rather than fail on it at every admission.

#### Scenario: Derived bound is fixed
- **WHEN** hard pressure derives its round bound and later rounds run in the same admission epoch
- **THEN** the bound MUST NOT grow, and the structured cannot_fit MUST report it as max_rounds.

#### Scenario: Boundary compaction
- **WHEN** a host opts into soft_on_turn_boundary and a turn completes
- **THEN** runtime SHALL attempt soft compaction after TurnCompleted through existing idle admission and persistence.

### Requirement: Neutral Provider Summary Mechanism
A feature-gated provider summary adapter SHALL use host-supplied instructions,
compact tool-aware rendering, the authoritative planner for every request,
LCM purpose usage accounting, cancellation and deadlines. Oversized leaves
SHALL use bounded map-reduce without exceeding the summarizer input budget.
Every provider call MUST have its own deadline (default 60 s) within an
operation budget scaled by the planned calls and capped (default 10 min); both
MUST be configurable. Cancellation MUST be scoped to one summary operation.
The attempt purpose MUST be idle compaction only for idle-boundary summaries.

#### Scenario: Oversized tool leaf
- **WHEN** a source leaf exceeds the summarizer window
- **THEN** multiple measured map calls and bounded reduction MUST preserve tool evidence and aggregate usage.

#### Scenario: Per-operation cancellation
- **WHEN** a host cancels the scope of one summary operation
- **THEN** that operation MUST fail without provider I/O, and the next operation MUST run normally.

#### Scenario: Deterministic tool evidence
- **WHEN** model escalation reaches fallback
- **THEN** bounded rendering MUST include tool names, truncated arguments and results.

### Requirement: Durable ordinary-store hard admission

The runtime SHALL persist each pending hard-compaction response in the Sensitive LCM extension of full canonical state before committing its DAG mutation when SessionStore is configured and CheckpointStore is absent. Legacy persistence SHALL retain full SessionSnapshot saves; native SessionJournal persistence MAY retain the same complete materializable state through atomically published roots and referenced objects. It MUST retain existing authorization, identity, body, source-plan and revision checks, and MUST persist the exact intent or proven successor before provider admission. Representation changes MUST NOT accept missing or host-redacted required intent as exact-successor proof.

#### Scenario: Crash before the staged mutation lands

- **WHEN** the ordinary intent snapshot or equivalent complete native root is saved but its DAG CAS has not landed
- **THEN** resume validates and discards the uncommitted intent before new work
- **AND** retains complete canonical history and already charged summary usage

#### Scenario: Crash after the staged mutation lands

- **WHEN** its DAG CAS lands before terminal ordinary persistence
- **THEN** resume adopts exactly the proven successor without duplicate nodes
- **AND** persists repaired state before exposing a live session handle

#### Scenario: Fully saved turn

- **WHEN** terminal persistence succeeds
- **THEN** normal resume retains history and the committed DAG without replay

#### Scenario: Failed staging save

- **WHEN** SessionStore or its native journal fails to save the hard intent
- **THEN** admission surfaces an error and does not commit that DAG mutation
- **AND** provider I/O does not proceed

#### Scenario: Other host compositions

- **WHEN** CheckpointStore is configured or neither store is configured
- **THEN** hard admission and recovery retain their existing behavior

#### Scenario: Required Sensitive intent was omitted by ordinary policy

- **WHEN** native root materialization cannot recover the required validated intent body
- **THEN** exact-successor recovery fails closed before provider or summary-model I/O
- **AND** a root digest alone does not attest the omitted body

### Requirement: Deterministic Fallback Rendering
The deterministic fallback SHALL render its source so that the stored text
cannot be read as a reply form. It MUST keep tool names, truncated arguments
and truncated results as evidence, MUST render each tool call and tool result
as a bracketed note that is not attributed to the assistant role, MUST NOT
render a tool call as `call name(args)`, and MUST omit reasoning parts. It MUST
begin with a fixed label saying the text is an excerpt and not a summary
whenever the label fits inside the fallback target, and MUST fall back to the
unlabelled cut when it does not. The rendering used to build a summary prompt
is not changed by this requirement.

#### Scenario: Deterministic tool evidence
- **WHEN** model escalation reaches fallback for a source holding a tool call and its result
- **THEN** the stored text MUST include the tool name, its truncated arguments and its truncated result
- **AND** it MUST NOT contain `call name(args)` or an assistant-attributed tool call line.

#### Scenario: Reasoning is left out
- **WHEN** the fallback source holds a reasoning part
- **THEN** the stored text MUST NOT contain that reasoning.

#### Scenario: Label on a tiny source
- **WHEN** the fallback target is smaller than the label
- **THEN** the fallback MUST still strictly shrink the source, without the label.

### Requirement: Resume Disowns an Uncheckpointed Tail
LCM resume validation SHALL recover a checkpoint whose store holds entries
past the checkpointed frontier that were appended by a turn which never
reached a terminal checkpoint. The recovery MUST apply only when the
checkpoint carries no pending summary, the checkpointed history prefix matches
canonical history, the store's active DAG equals the DAG the checkpoint
recorded, and every entry below the frontier matches canonical history. It
MUST then truncate the store from the frontier and re-anchor the checkpoint to
the resulting revision. It MUST NOT change any summary node, and any other
disagreement MUST remain a conflict.

#### Scenario: Process killed between provider steps
- **WHEN** a host with only a session store is killed after a turn appended several provider steps and before the session was saved
- **THEN** resuming the saved session MUST succeed
- **AND** the timeline MUST hold no entry of the killed turn
- **AND** the next turn MUST complete.

#### Scenario: Recovery interrupted after its truncation
- **WHEN** a recovery truncated the tail and the process died before the repaired checkpoint was saved
- **THEN** the next resume of the unchanged saved session MUST succeed.

#### Scenario: Store without truncation
- **WHEN** the store does not support truncating its tail
- **THEN** resume MUST fail with the original conflict and the store MUST be unchanged.

### Requirement: Flat semantic summary is an opt-in alternative to LCM

A host SHALL be able to compose the flat semantic-summary coordinator instead
of LCM through `RuntimeBuilder::semantic_summary`. A runtime MUST have at most
one semantic compaction strategy. Building MUST fail when LCM and a flat
summary are both configured, whether the summary was added through the
dedicated method or as a component with id `harness.semantic_summary`.
Building MUST also fail when the flat summary is combined with
`soft_on_turn_boundary(true)`.

#### Scenario: Host composes the flat summary
- **GIVEN** a runtime built with `RuntimeBuilder::semantic_summary` and no LCM
- **WHEN** a turn commits under context pressure
- **THEN** the coordinator stores the summarized originals, records the
  summary under `harness.semantic_summary`, and projects it into later
  requests
- **AND** no LCM component is present

#### Scenario: Both strategies are configured
- **GIVEN** a builder with `RuntimeBuilder::lcm` and a flat summary, by method
  or by component id
- **WHEN** the runtime is built
- **THEN** building fails with a configuration error naming both strategies

#### Scenario: Soft turn-boundary compaction with the flat summary
- **GIVEN** a builder with the flat summary and `soft_on_turn_boundary(true)`
- **WHEN** the runtime is built
- **THEN** building fails with a configuration error

### Requirement: Idle semantic compaction returns the protected summary

`SessionHandle::try_idle_semantic_compaction` SHALL run, with the flat summary
configured, the coordinator's idle compaction under the
`cache_idle_compaction` purpose. It SHALL return the protected summary, any
fallback reason, and the usage, and SHALL record that usage with source
`SemanticSummary`. LCM's `try_idle_compaction` result MUST be unchanged.

#### Scenario: Idle compaction under the flat summary
- **GIVEN** an idle session with the flat summary configured and eligible old
  turns
- **WHEN** the host calls `try_idle_semantic_compaction`
- **THEN** the result carries the new protected summary and its usage
- **AND** the usage ledger attributes that usage to the semantic summary

#### Scenario: LCM idle compaction is unchanged
- **GIVEN** a runtime configured with LCM
- **WHEN** the host calls `try_idle_compaction`
- **THEN** the result carries no summary body

### Requirement: Persisted flat summary state resumes under the flat summary

Persisted `harness.semantic_summary` schema-v1 state SHALL resume as live
state when the flat summary is configured, and MUST NOT be imported into LCM
or rejected. A host whose ordinary session store omits the state SHALL be
able to restore it through `restore_semantic_summary_if_absent`. That restore
MUST fail closed unless the session is idle and the component revision,
source session, canonical history prefix and summary revision all match. It
MUST never overwrite state the runtime already restored. State written by the
flat coordinator MUST remain importable by the LCM legacy decoder.

#### Scenario: Resume keeps the summary
- **GIVEN** a session persisted with flat summary state
- **WHEN** it resumes under a runtime configured with the flat summary
- **THEN** the next request projects the persisted summary
- **AND** the namespace is still present and no LCM state is created

#### Scenario: Host restores omitted state
- **GIVEN** a resumed idle session without the namespace and a matching
  protected copy held by the host
- **WHEN** the host calls `restore_semantic_summary_if_absent`
- **THEN** it returns true and the summary is projected on the next request

#### Scenario: Restore does not overwrite
- **GIVEN** a session that already holds the namespace
- **WHEN** the host calls `restore_semantic_summary_if_absent`
- **THEN** it returns false and the state is unchanged

#### Scenario: LCM imports flat summary state
- **GIVEN** a session whose flat summary state was written by the restored
  coordinator
- **WHEN** it resumes under a runtime configured with LCM
- **THEN** the legacy import succeeds as it does for pin-line state

### Requirement: Flat summary fallback is reported as a downgrade

The runtime SHALL emit `HarnessEvent::SemanticSummaryFallback` when the flat
coordinator cannot produce a summary and falls back to unsummarized history.
The event is observable as `RuntimeEvent::Downgrade` with capability
`semantic_summary`, and the turn SHALL continue.

#### Scenario: Summary model fails
- **GIVEN** a session under pressure whose summary model returns an error
- **WHEN** the turn commits
- **THEN** a `Downgrade` event with capability `semantic_summary` is emitted
- **AND** the turn's result is committed without a new summary

### Requirement: Manual semantic compaction may repeat within an idle interval

`SessionHandle::try_manual_semantic_compaction` SHALL run the same flat
semantic compaction as `try_idle_semantic_compaction` for an explicit host
request. An earlier attempt in the current idle interval MUST NOT refuse it.
It MUST apply every other admission rule of idle compaction: no running or
pending turn, no compaction in flight, a safe checkpoint, a configured flat
summary component, and the same persistence and rollback. After a manual
attempt the interval SHALL remain attempted for `try_idle_semantic_compaction`.

#### Scenario: Retry after an idle attempt

- **GIVEN** an idle session whose current interval already had a semantic
  compaction attempt
- **WHEN** the host calls `try_idle_semantic_compaction`
- **THEN** it is refused as busy
- **WHEN** the host calls `try_manual_semantic_compaction`
- **THEN** the compaction is admitted
- **AND** a later `try_idle_semantic_compaction` in the same interval is still
  refused as busy

#### Scenario: Retry after a failed summary

- **GIVEN** an idle interval whose semantic compaction failed with a fallback
  reason
- **WHEN** the host calls `try_manual_semantic_compaction`
- **THEN** the summary model is called again and its result is returned

#### Scenario: Manual compaction while another is in flight

- **GIVEN** an idle semantic compaction that is still running
- **WHEN** the host calls `try_manual_semantic_compaction`
- **THEN** it is refused as busy
- **AND** the running compaction completes normally

### Requirement: Reused LCM accounting preserves decisions and authority

Reusing LCM accounting SHALL produce the same pressure totals, configured
budget decisions, and observable outcomes as full computation under the same
versioned policy and source evidence. Reuse MUST NOT weaken view authorization,
revision checks, source validation, checked arithmetic, protected commit
ordering, or explicit conflict behavior. Loss of reusable accounting MUST
affect performance only and MUST NOT reset persisted state or infer authority.

#### Scenario: Warm append and cold recomputation agree

- **GIVEN** the same authorized history and versioned sizer/policy
- **WHEN** pressure is evaluated after a validated append using reused accounting and using full computation
- **THEN** both yield identical token totals and pressure/compaction decisions
- **AND** provider usage and configured limits are unchanged

#### Scenario: Authority is revoked after a prior successful evaluation

- **WHEN** the host revokes an LCM view grant before the next evaluation
- **THEN** previously computed accounting cannot authorize another lookup or result
- **AND** the existing denial behavior remains fail-closed

#### Scenario: Revision or canonical prefix changes

- **WHEN** store, policy, sizer, classifier, guard, DAG, binding evidence, or canonical history no longer satisfies existing validation
- **THEN** reusable accounting cannot override the mismatch
- **AND** the existing full validation, conflict, and recovery policy remain authoritative

#### Scenario: Old protected LCM state resumes cold

- **WHEN** a supported legacy checkpoint resumes without process-local accounting
- **THEN** its existing schema, component revision, history fingerprint, and canonical state are validated as before
- **AND** accounting is rebuilt without a state reset or additional authority
