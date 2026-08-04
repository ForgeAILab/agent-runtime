## ADDED Requirements

### Requirement: Adapters declare how they drive a prompt cache

Every provider adapter SHALL declare, per model, how it drives a provider-side
prompt cache: not at all, implicitly by keeping a stable keyed prefix, or
explicitly by marking segments. The declaration MUST map onto the neutral cache
classes so a context plan reports what the serving adapter can honor rather than
assuming it can honor nothing.

#### Scenario: A plan reports the serving adapter's real capability
- **GIVEN** an adapter that drives a prompt cache implicitly
- **WHEN** a context plan containing cache-stable segments is built for it
- **THEN** the plan does not report the stable class as unsupported

#### Scenario: An adapter without a prompt cache is honest about it
- **GIVEN** an adapter that cannot cache
- **WHEN** a plan containing cache-stable segments is built for it
- **THEN** the plan reports the stable class as unsupported

### Requirement: A prompt cache is keyed by session, not by request

An adapter driving an implicit prompt cache SHALL key it to the session rather
than to the request or attempt. A request id changes every turn, so keying by it
would place each turn in a different cache partition and defeat the reuse the
stable prefix exists to enable.

#### Scenario: Two turns of one session share a cache key
- **GIVEN** a session that issues two provider requests
- **WHEN** each request is serialized
- **THEN** both carry the same prompt cache key

#### Scenario: Separate sessions do not share a cache key
- **GIVEN** two sessions on the same model
- **WHEN** each issues a request
- **THEN** their prompt cache keys differ

### Requirement: Explicit adapters mark the stable request prefix

An adapter that marks cache segments explicitly SHALL mark the tool
declarations and the trailing system instructions, which the planner classifies
cache-stable and which every turn of a session repeats verbatim. It MUST NOT
exceed the number of breakpoints it declared.

#### Scenario: Tools and system instructions are marked
- **GIVEN** a request carrying tool declarations and system instructions
- **WHEN** an explicit adapter serializes it
- **THEN** the serialized request marks both for caching

#### Scenario: A request with neither is unmarked
- **GIVEN** a request with no tools and no system instructions
- **WHEN** an explicit adapter serializes it
- **THEN** no cache breakpoint is emitted
