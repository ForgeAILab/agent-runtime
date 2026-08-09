## ADDED Requirements

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
