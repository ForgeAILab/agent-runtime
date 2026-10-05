---
created_at: 2026-10-04T00:00:00Z
updated_at: 2026-10-04T00:00:00Z
completed_at:
status: proposed
---

Status: **PROPOSED — awaiting repository-owner review. No implementation tasks completed.**

## 1. Contract review

- [ ] 1.1 Approve explicit scheme selection and the config-field versus provider-builder compatibility choice.
- [ ] 1.2 Confirm OAuth beta flag, auth-header conflict rules, and supported host credential use.

## 2. Implementation after approval

- [ ] 2.1 Add Anthropic source/target, clock, minimum-validity configuration, and static-source compatibility.
- [ ] 2.2 Validate request/auth configuration before acquisition; enforce cancellation, deadline, and lease validity before transport.
- [ ] 2.3 Inject the selected authentication header; merge OAuth beta flags exactly once without overriding host flags.
- [ ] 2.4 Classify pre-output transport and SSE auth failures; invalidate the exact revision and emit the existing recovery disposition without an internal retry.
- [ ] 2.5 Retain safe Debug/error/event/persistence boundaries using existing redaction helpers.

## 3. Conformance and delivery after approval

- [ ] 3.1 Cover static API key, renewable API key, renewable Bearer, and static setup-token source leases; header conflicts and beta deduplication.
- [ ] 3.2 Cover acquisition/invalidation cancellation and deadlines, insufficient validity, static/stale invalidation, and concurrent revisions.
- [ ] 3.3 Verify two visible attempts on one eligible rejection, no third attempt on replacement rejection, total-attempt exhaustion, and no recovery after semantic output.
- [ ] 3.4 Verify canary absence from compact/pretty Debug, errors, events, manifests, snapshots, and checkpoints.
- [ ] 3.5 Update README/CHANGELOG and run fmt, Clippy, workspace/all-feature tests, doc tests, and provider Rust 1.86 validation.
- [ ] 3.6 Record conformance evidence and coordinate a separate Nyx migration review before retiring its legacy adapter.
