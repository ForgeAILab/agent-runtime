## ADDED Requirements

### Requirement: Isolated session collection failures

Collection SHALL silently skip state/fence conflicts and isolate other
non-retryable failures in a session's collection
reads or its own atomic apply, preserving its membership, retirement, replay rows
and durable dirty mark, and SHALL continue remaining sessions. The report SHALL
contain sorted unique session ids whose state could not be read or validated
in `unreadable_sessions`, excluding conflicts, without object ids or content.
Local dirty hints SHALL survive both conflict skips and isolation so repair can be retried next
call, with at most one failing load per session in that call. An unreadable
retirement dependency SHALL leave its source dirty and uncollected as well.
Retryable failures and domain-wide discovery, epoch or poisoned engine lock
failures SHALL abort;
previously acknowledged session transactions need not roll back.

#### Scenario: An unreadable session does not stop collection

- **GIVEN** three dirty sessions and a non-retryable, non-conflict load failure for the middle one
- **WHEN** the shared engine collects the domain
- **THEN** both readable sessions are collected and the middle one is unchanged and reported
- **AND** another collection retries the middle session once and collects new garbage elsewhere

#### Scenario: A session apply fails without effect

- **WHEN** a session's atomic collection apply returns a non-retryable, non-conflict error
- **THEN** its objects, replay rows and dirty mark remain unchanged
- **AND** it is reported while collection continues with later sessions

#### Scenario: Retryable storage failure

- **WHEN** a session load or apply returns a retryable error before any session is acknowledged
- **THEN** collection returns that error without changing durable session state
- **AND** local dirty hints remain available for a later retry

#### Scenario: Session changed between load and apply

- **WHEN** a session read or apply returns a non-retryable state/fence conflict
- **THEN** collection skips it without listing it in `unreadable_sessions`
- **AND** its durable state and retry hint survive while unrelated collection continues
- **AND** a dependency conflict also leaves its source dirty without reporting either as unreadable

#### Scenario: Packed SQLite writer contention

- **GIVEN** a second SQLite connection holds a write lock
- **WHEN** packed apply encounters BUSY or LOCKED, including an extended code
- **THEN** it returns a retryable error and collection aborts for retry
- **AND** a failed transaction that did not commit has no durable effect
- **AND** only ambiguous COMMIT I/O errors receive retryability on account of I/O

#### Scenario: Local engine lock poisoned during collection

- **WHEN** a session read or apply encounters a poisoned engine registry or memo lock
- **THEN** collection returns the domain-wide error rather than isolating a session
- **AND** unacknowledged session state and retry hints survive

#### Scenario: Unreadable dependency candidate

- **WHEN** collection cannot read a dependency target with a non-retryable, non-conflict error
- **THEN** the target and its source are reported and kept dirty without releasing source membership
- **AND** independent sessions can still be collected

#### Scenario: Unknown stored transfer-index revision leaks safely

- **GIVEN** a closed session whose stored transfer-index revision is unknown and whose orphan bodies could otherwise be collected
- **WHEN** collection visits that session and later independent garbage
- **THEN** it reports the unknown session and preserves all of its bytes while reclaiming only the independent garbage
- **AND** a repeated call reports the unknown session without releasing more bytes
- **AND** retirement of that session and a possible transfer target remains refused, with both current closures readable

### Requirement: Fail-closed domain diagnostics and retirement

Retained-byte diagnostics SHALL return exact accounted bytes or an error rather
than silently omit unreadable sessions. Retirement SHALL reject an unreadable
candidate because its dependency metadata may block the requested retirement.
Domain-wide storage discovery, retained-byte accounting and physical compaction
errors SHALL remain errors because these interfaces cannot identify a safe
per-session partial result.

#### Scenario: An unreadable candidate might depend on a retirement target

- **WHEN** retirement encounters an unreadable dependency candidate
- **THEN** it fails without retiring the target

#### Scenario: Retained-byte diagnostic cannot account for a session

- **WHEN** storage cannot enumerate or read a session's member bodies for the diagnostic
- **THEN** the engine returns the error rather than a successful partial byte count

### Requirement: Storage authority and identity conformance

The reusable storage suite SHALL reject a cache that bypasses membership checks
on hits, a lookup that substitutes another id sharing a four-byte prefix, and
`members()` that includes released or never-admitted ids. Each defect SHALL have
a test-only adapter that fails its corresponding named case. Immediate physical
removal of released bodies SHALL remain permitted.

#### Scenario: Warm cache after release

- **WHEN** a member body is read to warm the cache and its membership is released
- **THEN** repeated reads through that session and a never-admitted session return no body

#### Scenario: Four-byte prefix collision

- **GIVEN** two valid different object ids sharing their first four bytes
- **WHEN** both are stored and requested in different orders including duplicates
- **THEN** each request returns exactly its own body

#### Scenario: Exact membership enumeration

- **WHEN** a body is held elsewhere but released from a session or never admitted there
- **THEN** that session's members exclude it across roles and storage domains
