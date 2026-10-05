---
created_at: 2026-10-04T00:00:00Z
updated_at: 2026-10-04T00:00:00Z
completed_at:
status: proposed
---

Status: **PROPOSED — awaiting repository-owner review. No implementation tasks completed.**

## 1. Design review

- [ ] 1.1 Approve declarative profile versus hook/wrapper and config-field versus provider-builder compatibility.
- [ ] 1.2 Approve profile-aware input accounting/cache identity, output-limit treatment, and lease account-header precedence.
- [ ] 1.3 Confirm host-supplied backend fixtures, default instructions, synthetic text, effort mappings, and unsupported-field mask.

## 2. Implementation after approval

- [ ] 2.1 Add a bounded validated profile with standard behavior by default; redact prompt/default text and show safe settings in Debug.
- [ ] 2.2 Integrate fallback/synthetic content into preflight planning and profile-aware sizing/cache identity before immutable plan admission.
- [ ] 2.3 Project ordered system content to top-level instructions and insert only planner-accounted fallback/synthetic input.
- [ ] 2.4 Add explicit wire tuning omission without weakening model/output/reasoning budget validation.
- [ ] 2.5 Add explicit-effort, host budget-mapping, and default-effort precedence plus configured reasoning summary.
- [ ] 2.6 Project the acquired lease account into a validated configured header; reject missing required accounts, invalid values, and conflicting static headers.
- [ ] 2.7 Revalidate the final profile payload, preserve stateless restrictions and bounds, and leave stream normalization/recovery unchanged.

## 3. Conformance and delivery after approval

- [ ] 3.1 Verify byte-shape fixtures for default standard behavior, multiple/no system messages, system-only/empty conversations, multimodal input, and tool/reasoning continuations.
- [ ] 3.2 Verify rejected tuning fields are absent and policy cannot restore them through vendor overrides.
- [ ] 3.3 Verify explicit effort precedence, mapping boundaries/zero/unmapped budgets, defaults, summary, and model-advertised extended effort labels.
- [ ] 3.4 Verify account/token coherence across refresh, concurrent attempts, one recovery replay, missing accounts, case-insensitive conflicts, and header injection rejection.
- [ ] 3.5 Verify profile fallback/synthetic input budget rejection and cache identity changes before credential/transport I/O.
- [ ] 3.6 Verify canary non-disclosure in Debug, errors, events, manifests, snapshots, and checkpoints.
- [ ] 3.7 Update README/CHANGELOG and run fmt, Clippy, workspace/all-feature tests, doc tests, and provider Rust 1.86 validation.
- [ ] 3.8 Publish profile conformance evidence; coordinate any consumer migration separately after repository-owner approval.
