# Review decisions

D1: A session state/fence conflict is silently skipped, keeps its local retry
hint and durable state, and is retried on the next call. It is not unreadable.
Only non-retryable, non-conflict session failures are reported. Dependency
failures conservatively block their source, with the same reporting distinction.
Known invalid stored data must remain distinguishable from concurrent changes.
The engine recognizes `ErrorKind::Conflict` or `FailureClass::StateConflict`.
Legacy metadata/object validation uses those same classifications, so proven
invalid data carries the private redaction-safe `journal_invalid_state: true`
metadata flag while preserving its public kind/class. Missing bodies, response
counts and membership mismatches can race a collector and remain conflicts.
Packed malformed framing/duplicate decoded entries use `Serialization`, while
both its `data_version` checks and transaction fences use `StateConflict`.
Separate skipped and unreadable sets preserve hints and suppress repeated
failed dependency reads without turning conflicts into diagnostics.

D2: Packed SQLite BUSY and LOCKED (including extended codes) are retryable at
every SQL boundary. I/O failures are retryable at COMMIT, where the durable
outcome can be ambiguous. Other proven no-effect failures stay non-retryable.

D4: Poisoned engine registry, memo and hint locks are domain-wide failures and
abort collection even when encountered inside a session read or apply.
The existing poison-only `host_failure()` constructor adds the private
`journal_lock_poisoned: true` flag; ordinary storage HostComponent failures
remain eligible for session isolation. Retryability is not changed by this
flag. Tests poison registry and memo locks while a collection load is paused,
then clear poisoning only in the fixture to verify retry recovery.

D5: Order-dependent conservative blocking is accepted. If a target was already
collected before a later source discovers an unreadable dependency, earlier
acknowledged work is not rolled back. A source discovered before its blocked
target remains dirty and uncollected. Candidate additions already acknowledged
on other targets may remain. This trades extra retention for safe incremental
collection; no global preflight or dependency-order traversal is required.
