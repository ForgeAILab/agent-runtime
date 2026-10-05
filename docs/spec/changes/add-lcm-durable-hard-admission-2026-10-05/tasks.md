---
created_at: 2026-10-05T00:00:00Z
updated_at: 2026-10-05T07:20:00Z
completed_at: 2026-10-05T07:20:00Z
---

## 1. Implementation
- [x] 1.1 Inspect U7/U8 and record ordering, crash windows and compatibility.
- [x] 1.2 Persist ordinary-only hard intent before admission retry.
- [x] 1.3 Validate/discard uncommitted ordinary intent and retain exact adoption.

## 2. Verification and Delivery
- [x] 2.1 Add deterministic crash matrix, save failures and unchanged-host checks.
- [x] 2.2 Run focused conformance, fmt, workspace clippy, tests and repo gates.
- [x] 2.3 Update changelog, inspect disk diff and report exact results/status.

## Verification results

Final fmt, workspace clippy, workspace all-feature tests, standalone doc tests,
workspace all-feature build, 1.86 default/all-feature production builds and 1.88
MCP/CLI all-feature build all passed. Workspace tests: 1598 passed, 0 failed,
4 existing ignored; standalone docs: 7 passed, 0 failed, 1 existing ignored.
All 12 new fault-matrix tests pass, network-free. Strict spec validation and
git diff --check passed. cargo deny check initially could not lock the read-only
home advisory cache; a temporary copy of deny.toml changes only db-path to
/private/tmp/lcm-hard-advisory-dbs. cargo deny --config
/private/tmp/lcm-hard-deny.toml check passed all four policy groups, with no
allow-list or policy changes. Initial focused fixture compile/assertion failures
were corrected before the final runs.
