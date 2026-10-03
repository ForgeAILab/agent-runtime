---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-01T21:25:57Z
completed_at:
---

# Tasks: Add runtime failure classes

## 1. Contract and Conversion

- [x] 1.1 Approve class/stage vocabulary; add defaulted, redaction-safe classes and lenient optional timing/recovery fields without changing existing constructors or coarse fields.
- [x] 1.2 Preserve every provider error field in conversion; verify duration versus absolute-time units and local preflight stage overrides.
- [x] 1.3 Preserve typed planner/harness errors through a private driver carrier; set LCM classes at typed matches and genuine concurrent revision reads without exposing store text.

## 2. Compatibility Proof

- [x] 2.1 Add frozen legacy RuntimeError/Error-event/ChildFailed JSON and future or malformed-class fixtures to testkit; assert old/new reader behavior and exact zero/absent timing values.
- [x] 2.2 Add Smith contributor, Forge authorized/revoked LCM view, and Nyx seeded-preflight cases in the three consumer suites described in proposal.md.
- [x] 2.3 Verify deterministic schema/policy classes, input versus capability budget failures, unknown host failures, and unchanged retry admission/event/TurnFinish behavior.

## 3. Audit Corrections

- [x] 3.1 Make diagnostic evidence fail-open for record loading and make the new stage/component enums source- and wire-forward-compatible.
- [x] 3.2 Preserve class, timing, reset, and credential evidence through delegated child failures.
- [x] 3.3 Use direct `Option<u32>` context-overflow counts and restore driver rejection messages.
- [x] 3.4 Remove blanket LCM prose-site classification; retain typed mappings and genuine concurrent revision conflicts, with expansion MissingSource classified as RequestRejected.
- [x] 3.5 Replace the opaque checkpoint fixture with a persisted ChildFailed fixture and update migration/release notes.

## 4. Release Gates

- [ ] 4.1 Compile all three actual consumer integrations; document and coordinate error literals/destructuring changes before claiming source compatibility. — Not run: actual consumer repositories are outside this job; the Rust migration is documented.
- [ ] 4.2 Run formatting, all-feature Clippy, workspace/doc/schema tests, all three named consumer targets, dependency/license checks, and declared MSRV gates. — This job runs only the touched-crate checks and narrow tests required by its brief; the orchestrator owns the remaining release gates.
- [ ] 4.3 Document additive wire fields and any required Rust migration; publish only after all gates pass and consumers can pin an immutable landed version/revision. — Documentation is complete; publication and immutable consumer pins remain orchestrator/release work.
