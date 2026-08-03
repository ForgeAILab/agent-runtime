---
created_at: 2026-08-03T21:05:00Z
updated_at: 2026-08-03T21:05:00Z
completed_at: null
---

## 1. Policy

- [x] 1.1 Rename `trigger_turns` to `min_turns`; add `trigger_percent` and
      `input_budget_tokens` to `SemanticSummaryPolicy`.
- [x] 1.2 Extend `validate` to reject a zero budget, an out-of-range percent,
      and a retain count that does not fit under the floor.

## 2. Decision

- [x] 2.1 Derive the baseline and latest input cost from provider-attempt
      records in the committed usage ledger.
- [x] 2.2 Gate `after_commit` on the floor and then on measured pressure.
- [x] 2.3 Exclude `UsageSource::SemanticSummary` records from the decision.

## 3. Tests

- [x] 3.1 Small turns past the floor do not summarize; one large turn does.
- [x] 3.2 The floor protects a young session.
- [x] 3.3 A larger opening cost does not advance the trigger.
- [x] 3.4 Summary spend is excluded.
- [x] 3.5 A zero budget fails validation.

## 4. Validation

- [x] 4.1 `cargo fmt --check`, `cargo clippy` warnings-as-errors, workspace tests.
