---
created_at: 2026-10-20T12:43:20Z
updated_at: 2026-10-20T13:13:52Z
---

## Why

One unreadable session currently aborts the shared engine's dirty-session sweep,
including when a process-local hint reintroduces a session omitted by storage.
Forge cannot collect later healthy sessions without violating the shared-engine
lease boundary. Storage conformance also misses warm-cache authority, short-id
lookup collisions and inaccurate membership enumeration.

## What Changes

- Isolate non-retryable per-session collection read and apply failures. Preserve
  that session's durable state and dirty mark; continue unrelated sessions.
  Retryable and domain-wide discovery/mark failures still abort. Collection is
  incremental: an abort does not roll back already acknowledged sessions.
- **BREAKING**: add default-deserialized `unreadable_sessions: Vec<SessionId>` to
  public `JournalGcReport`. Only session ids are reported, sorted and unique;
  hosts can deduplicate logs by id. Keep local dirty hints so key restoration or
  storage repair can recover without another write. At most one failed load per
  session per call, even when dependency indexing encounters that session.
- Isolate unknown stored transfer-index revisions as authorized by the integrator;
  retain the affected session's orphan bodies and possible retirement targets,
  while independent sessions continue collection.
- Preserve fail-closed retirement: unreadable dependency candidates can protect
  another session, so skipping them could allow unsafe retirement. Retained-byte
  diagnostics remain exact-or-error (a scalar cannot signal an undercount).
  Storage-native retained-byte accounting and physical compact are delegated,
  not engine session sweeps; their domain-wide errors remain errors.
- Add named conformance cases for unreadable-session collection, warm-cache
  membership checks, exact full-id lookup with a four-byte prefix collision, and
  exact membership enumeration, plus broken adapters for the three storage bugs.

## Impact

- Capability: `session-journal`; builds on the still-active
  `extract-journal-storage-engine-2026-10-19` change without editing it.
- Code: core journal controls/report and tests, testkit storage conformance,
  CHANGELOG. No object format or required storage method changes.
- Failed non-retryable applies rely on storage's atomic no-effect error contract;
  ambiguous lost acknowledgements must be marked retryable. Conservative
  dependency candidate additions may precede a source's failed collection;
  source membership, replay rows and dirty state remain unchanged.
