## MODIFIED Requirements

### Requirement: Versioned run manifest

Every turn SHALL reference a versioned run manifest containing registry
snapshot/view fingerprints, resolved model profile, capability resolver and
activation revisions, tokenizer and adapter revisions, context/compaction/cache
policy revisions, permission-vocabulary revision, ordered security-check
identities/modes/revisions and composed-set fingerprint, content-guard
revisions, isolation backend/profile/configuration revisions, endpoint/path
policy revisions, ordered segment identifiers and hashes, token counts, and
context/cache fingerprints. The manifest MUST contain no raw content, secret,
credential, reusable grant, or isolated tool state.

#### Scenario: Audit a completed turn

- **GIVEN** a completed turn used automatic capability routing, content guards,
  compaction, authorization, and an isolated tool
- **WHEN** an operator inspects its persisted manifest
- **THEN** the exact registry, model, activation, tokenizer, context, check-set,
  guard, isolation backend, and profile revisions are identifiable
- **AND** the manifest explains decisions without requiring raw sensitive
  content or reusable authority

### Requirement: Privacy-safe context telemetry

Default planning events and manifests SHALL store identifiers,
classifications, hashes, revisions, counts, and decisions rather than raw
credentials, secrets, or sensitive fragment content. Hosts MAY persist raw
content only through an explicit storage policy and sensitivity-aware
contract; that opt-in MUST NOT extend to secret-class content, credential
material, or quarantined/rejected content-guard output, which MUST remain
excluded from persisted raw content regardless of host storage policy.

#### Scenario: Tool result contains a secret

- **GIVEN** a sensitive tool result participates in context planning
- **WHEN** planning metrics and the run manifest are emitted
- **THEN** they contain its bounded identifier, classification, hash, and token
  count
- **AND** they do not contain the raw secret value

#### Scenario: Host opt-in cannot capture secret-class or quarantined content

- **GIVEN** a host has configured an explicit storage policy that persists raw
  fragment content
- **WHEN** a fragment is classified as secret-class or has been
  quarantined/rejected by a content guard
- **THEN** the persisted record excludes its raw content regardless of the
  storage policy
- **AND** only identifiers, classifications, hashes, revisions, and decisions
  are stored for that fragment

### Requirement: Revision-safe persistence and replay

Session persistence SHALL retain enough versioned manifest data to resolve the
same registry view, model profile, activation set, context decisions, and
security-check-set/isolation/content-guard revisions during equivalent replay.
Missing or changed required revisions MUST fail explicitly unless the host
opts into a labeled non-equivalent replay. The labeled non-equivalent replay
opt-in MUST NOT apply to a security-check-set, isolation backend/profile,
content-guard, or permission-vocabulary revision mismatch: those mismatches
MUST hard-fail replay unconditionally, with no host opt-in able to force a
non-equivalent security replay.

#### Scenario: Required skill revision is unavailable

- **GIVEN** a persisted turn references a specific skill revision
- **AND** only a different revision is installed during replay
- **WHEN** equivalent replay is requested
- **THEN** replay fails with a structured revision-mismatch result
- **AND** it does not silently substitute the installed revision

#### Scenario: Security revision mismatch cannot be waived

- **GIVEN** a persisted turn references an older security-check-set, isolation
  backend/profile, or content-guard revision
- **AND** the host has configured a labeled non-equivalent replay opt-in for
  content/skill revisions
- **WHEN** equivalent replay is requested under a different security-relevant
  revision
- **THEN** replay fails with a structured revision-mismatch result regardless
  of the host's non-equivalent replay opt-in
- **AND** no isolated execution, credential resolution, or side effect occurs

## ADDED Requirements

### Requirement: Observable security lifecycle

The runtime SHALL emit versioned neutral events for per-check and composed
authorization decisions, approval decisions, isolation start/finish/termination,
denied host operations, endpoint/path denials, opaque credential use, leak
detection, and content-guard outcomes. Events MUST use stable reason codes and
bounded classifications and MUST NOT expose raw credentials, sensitive
headers/bodies, quarantined content, isolated tool memory, or reusable grants.

#### Scenario: Isolated egress is denied

- **GIVEN** an isolated tool requests an endpoint outside its active grant
- **WHEN** the egress broker denies the request
- **THEN** event consumers receive the contributing check identities/revisions,
  composed-set fingerprint, action/resource class, grant fingerprint, and stable
  denial code
- **AND** receive no request body, credential value, or undisclosed allowlist
  entry

### Requirement: Security-safe replay

Equivalent replay SHALL verify ordered security-check identities/modes/revisions,
the composed-set fingerprint, permission vocabulary, content-guard, isolation
backend/profile/configuration, endpoint, and path revisions. Replay MUST NOT
reuse an expired capability grant, resolve a credential, run an isolated tool,
or repeat any side effect unless the host initiates a new authorized execution
explicitly.

#### Scenario: Security or isolation revision changed

- **GIVEN** a persisted turn references an older check set, isolation backend,
  profile, or endpoint policy revision
- **WHEN** equivalent replay is requested under a different revision
- **THEN** replay fails with a structured revision mismatch
- **AND** no isolated execution, credential resolution, or network request occurs
