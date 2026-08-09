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

The planner SHALL compare provider cache plans only when the complete opaque
identity matches. Changes to CacheEndpointIdentity, adapter partition,
provider/model profile, adapter, tokenizer, cache control, provider
key/breakpoint/resource identity, stable-prefix fragments, stable tools,
registry/view/activation, or ordered stable history MUST produce an absent or
zero expectation according to the existing first-request/comparable-baseline
rules. Changes limited to the conversation tail MUST not change CacheIdentity.

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
