---
created_at: 2026-10-20T12:43:20Z
updated_at: 2026-10-20T13:13:52Z
completed_at: 2026-10-20T13:13:52Z
---

## 1. Implementation

- [x] 1.1 Add report field, per-session isolation and retry hints; document other sweeps.
- [x] 1.2 Add core regressions for non-retryable reads/applies, repeated hints and retryable aborts.
- [x] 1.3 Add the unreadable-session case through the testkit collection host.
- [x] 1.4 Add M2/M11/M14 conformance cases and broken adapters proving rejection.
- [x] 1.5 Update Unreleased changelog, run requested verification and write report.

## 2. Integrator follow-up

- [x] 2.1 Update the shared unknown-transfer conformance assertions for isolation, retained bytes, possible-target retirement and independent collection.
- [x] 2.2 Stage accepted Message leaves in the host-facing unreadable-session case.
- [x] 2.3 Rerun every requested check, record changed assertions, complete tasks and replace the report.

## Acceptance status

Complete. The integrator-authorized unknown-transfer isolation assertions retain
orphan bytes, reject retirement of the unknown session and a possible target,
and collect independent garbage. The host-facing fixture now stages accepted
Message leaves. The full testkit run passes, including both shared-body-deletion
broken adapters, both volatile full-suite wrappers and the transparent wrapper.
All three packed-storage conformance wrappers and every requested verification
command pass. Both resumed problems passed on their first implementation attempt.
