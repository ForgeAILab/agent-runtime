---
created_at: 2026-10-04T00:00:00Z
updated_at: 2026-10-04T22:10:00Z
completed_at: 2026-10-04T22:10:00Z
---

## 1. Implementation
- [x] 1.1 Split strict identity and tunables; rebuild authorized state on tuning changes.
- [x] 1.2 Add request-sizer adapter and measured planner overhead state.
- [x] 1.3 Add builder working-set budget and pressure integration.
- [x] 1.4 Add non-wedging leaf/round behavior and opt-in boundary compaction.
- [x] 1.5 Separate source/output targets, enforce reclaim ratios and improve fallback.
- [x] 1.6 Add feature-gated planner-backed provider summarizer with bounded map-reduce.

## 2. Verification and Delivery
- [x] 2.1 Add testkit revision, map-reduce, ratios, working-set and sizing conformance.
- [x] 2.2 Run changed-crate tests, testkit conformance and consumer fixtures.
- [x] 2.3 Run final fmt, workspace clippy, docs, MSRV and deny gates once; rerun failures only.
- [x] 2.4 Update Unreleased changelog and record API/adoption/results in journal.

## 3. Audit fix round
- [x] 3.1 Deterministic fallback keeps min(cap, source - 1); widen unsummarizable leaves; structured cannot_fit for summary cannot-fit under hard pressure.
- [x] 3.2 Floor the working-set pressure budget at 25% of target with a typed OverheadExceedsTarget diagnostic.
- [x] 3.3 Roll back idle batch memory when the exact idle checkpoint fails.
- [x] 3.4 Keep a host LcmSizer; share the planner sizer only in place of the default.
- [x] 3.5 One shared derived-rounds formula, fixed per admission epoch, capped at 64.
- [x] 3.6 Provider summary per-call/per-operation limits, trigger purpose and per-operation cancellation.
- [x] 3.7 Changelog consumer-break list; pending checks route every tunable mismatch through source identity.
