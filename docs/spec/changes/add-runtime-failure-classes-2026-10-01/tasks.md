---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-01T00:00:00Z
completed_at:
---

# Tasks: Add runtime failure classes

## 1. Contract and Conversion

- [ ] 1.1 Approve class/stage vocabulary; add defaulted, redaction-safe classes and optional timing/recovery fields without changing existing constructors or coarse fields.
- [ ] 1.2 Preserve every provider error field in conversion; verify duration versus absolute-time units and local preflight stage overrides.
- [ ] 1.3 Preserve typed planner/harness errors through a private driver carrier; set LCM classes before conversion without exposing store text.

## 2. Compatibility Proof

- [ ] 2.1 Add frozen legacy RuntimeError/Error-event/checkpoint JSON and future unknown-class fixtures to testkit; assert old/new reader behavior and exact zero/absent timing values.
- [ ] 2.2 Add Smith contributor, Forge authorized/revoked LCM view, and Nyx seeded-preflight cases in the three consumer suites described in proposal.md.
- [ ] 2.3 Verify deterministic schema/policy classes, input versus capability budget failures, unknown host failures, and unchanged retry admission/event/TurnFinish behavior.

## 3. Release Gates

- [ ] 3.1 Compile all three actual consumer integrations; document and coordinate error literals/destructuring changes before claiming source compatibility.
- [ ] 3.2 Run formatting, all-feature Clippy, workspace/doc/schema tests, all three named consumer targets, dependency/license checks, and declared MSRV gates.
- [ ] 3.3 Document additive wire fields and any required Rust migration; publish only after all gates pass and consumers can pin an immutable landed version/revision.
