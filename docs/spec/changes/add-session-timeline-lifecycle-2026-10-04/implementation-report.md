# U7/U8 implementation report

All work is uncommitted on `feat/u7-u8-session-timeline`. No git metadata,
desktop or visible browser was changed. The dated spec was written and strictly
validated before implementation. Runtime code remains host-neutral.

## Files and current lines

| File | Changed lines |
| --- | --- |
| [CHANGELOG.md](/Volumes/Data/codes/ai/agent-runtime-u78/CHANGELOG.md:12) | 12–52 |
| [crates/agent-runtime-core/src/checkpoint/mod.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-core/src/checkpoint/mod.rs:541) | 541–542 |
| [crates/agent-runtime-core/src/checkpoint/validation.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-core/src/checkpoint/validation.rs:144) | 144–194 |
| [crates/agent-runtime-core/src/error.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-core/src/error.rs:179) | 179–204, 245–261, 302, 306–316 |
| [crates/agent-runtime-core/src/lib.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-core/src/lib.rs:132) | 132 |
| [crates/agent-runtime-core/src/provider.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-core/src/provider.rs:1620) | 1620 |
| [crates/agent-runtime-lcm/src/lib.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-lcm/src/lib.rs:62) | 62–64 |
| [crates/agent-runtime-lcm/src/store.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-lcm/src/store.rs:50) | 50–63, 392–401, 406–423 |
| [crates/agent-runtime-lcm/src/testing.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-lcm/src/testing.rs:39) | 39, 281–315 |
| [crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:1043) | 1043, 1115, 1207, 1295, 1489, 1593, 1751, 1832, 1920, 2016 |
| [crates/agent-runtime-testkit/src/conformance/delegation/support.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/src/conformance/delegation/support.rs:166) | 166–172, 190 |
| [crates/agent-runtime-testkit/src/conformance/history.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/src/conformance/history.rs:145) | 145–154, 373, 441 |
| [crates/agent-runtime-testkit/src/conformance/manifests.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/src/conformance/manifests.rs:267) | 267, 333, 536 |
| [crates/agent-runtime-testkit/src/conformance/manifests/tests.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:64) | 64, 126, 165, 217, 298, 318, 358, 382, 409, 494, 529, 650, 747 |
| [crates/agent-runtime-testkit/src/conformance/mod.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/src/conformance/mod.rs:27) | 27–28 |
| [crates/agent-runtime-testkit/src/consumers/nyx.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/src/consumers/nyx.rs:55) | 55–59 |
| [crates/agent-runtime-testkit/src/consumers/open_forge.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/src/consumers/open_forge.rs:70) | 70–79 |
| [crates/agent-runtime-testkit/src/consumers/smith.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/src/consumers/smith.rs:35) | 35–44 |
| [crates/agent-runtime-testkit/src/stores.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/src/stores.rs:146) | 146–154 |
| [crates/agent-runtime-testkit/tests/conformance.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/tests/conformance.rs:14) | 14–16 |
| [crates/agent-runtime-testkit/tests/conformance/goal_conformance.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/tests/conformance/goal_conformance.rs:585) | 585 |
| [crates/agent-runtime-testkit/tests/conformance/lcm_working_set.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/tests/conformance/lcm_working_set.rs:271) | 271, 291 |
| [crates/agent-runtime-testkit/tests/conformance/runtime_conformance/interaction.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/tests/conformance/runtime_conformance/interaction.rs:364) | 364, 391, 431, 524 |
| [crates/agent-runtime-testkit/tests/conformance/runtime_conformance/local_action.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/tests/conformance/runtime_conformance/local_action.rs:304) | 304, 328 |
| [crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:27) | 27, 70, 150, 350, 425, 511, 533, 567, 665, 740, 883, 952, 990, 1108, 1206, 1412, 1433, 1490, 1504, 1590 |
| [crates/agent-runtime-testkit/tests/consumer_nyx.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/tests/consumer_nyx.rs:53) | 53 |
| [crates/agent-runtime-testkit/tests/consumer_open_forge.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/tests/consumer_open_forge.rs:81) | 81–91 |
| [crates/agent-runtime/src/agent/driver/mod.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/agent/driver/mod.rs:325) | 325–327, 568–573 |
| [crates/agent-runtime/src/agent/driver/provider.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/agent/driver/provider.rs:408) | 408–418 |
| [crates/agent-runtime/src/delegation/lifecycle.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/delegation/lifecycle.rs:1147) | 1147–1156, 1162–1164 |
| [crates/agent-runtime/src/delegation/monitor.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/delegation/monitor.rs:593) | 593, 601 |
| [crates/agent-runtime/src/harness/lcm.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/harness/lcm.rs:145) | 145–162, 171–193, 515–516, 524, 584, 599–923, 1493, 1742–1762, 1790–1810, 2087–2093, 3310–3315, 3968–3973, 4361–4363, 4383, 4417, 4876, 4897–4899, 4907, 4912, 4925–4927, 4935–4953, 4981–4983, 5342–5352, 6159 |
| [crates/agent-runtime/src/harness/mod.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/harness/mod.rs:43) | 43–44 |
| [crates/agent-runtime/src/lib.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/lib.rs:198) | 198–201 |
| [crates/agent-runtime/src/runtime/command.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/runtime/command.rs:13) | 13–14, 56–68, 71, 76–78, 81, 83–86, 105, 107–108, 121–154, 162, 167 |
| [crates/agent-runtime/src/runtime/engine.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/runtime/engine.rs:25) | 25–27, 161–166, 294, 341–356, 363–387, 601, 628, 651–654, 698–711, 723–991, 997–1006, 1020–1021, 1028, 1037–1046, 1061, 1063–1066, 1075–1084, 1197–1236, 1238–1239, 1252–1264, 1278–1280, 1358, 1405–1410, 1412–1426 |
| [crates/agent-runtime/src/runtime/mod.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/runtime/mod.rs:12) | 12, 25–27, 30 |
| [crates/agent-runtime/src/runtime/session/lifecycle.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/runtime/session/lifecycle.rs:32) | 32–48, 207 |
| [crates/agent-runtime/src/runtime/session/mod.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/runtime/session/mod.rs:111) | 111, 127–128 |
| [crates/agent-runtime/src/runtime/session/turns.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/runtime/session/turns.rs:13) | 13, 27–62 |
| [crates/agent-runtime/tests/integration/cache_evidence.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/tests/integration/cache_evidence.rs:3491) | 3491, 3561, 3606, 3855 |
| [crates/agent-runtime/tests/integration/interrupted_turn_admission.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/tests/integration/interrupted_turn_admission.rs:334) | 334, 456, 487, 505, 535, 555, 576, 636 |
| [crates/agent-runtime/tests/integration/lcm_expansion.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/tests/integration/lcm_expansion.rs:234) | 234–244, 401, 410, 436 |
| [crates/agent-runtime/tests/integration/lcm_failed_turn_recovery.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/tests/integration/lcm_failed_turn_recovery.rs:317) | 317, 379–382, 409 |
| [crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:407) | 407–417, 1167, 1284, 1312, 1347, 1386, 1433, 1453, 1534, 1654, 1884, 2018 |
| [crates/agent-runtime/tests/integration/lcm_unsigned_reasoning.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/tests/integration/lcm_unsigned_reasoning.rs:282) | 282 |
| [crates/agent-runtime/src/runtime/fork.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime/src/runtime/fork.rs:1) | new file |
| [crates/agent-runtime-testkit/src/conformance/session_timeline.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/src/conformance/session_timeline.rs:1) | new file |
| [crates/agent-runtime-testkit/tests/conformance/session_timeline.rs](/Volumes/Data/codes/ai/agent-runtime-u78/crates/agent-runtime-testkit/tests/conformance/session_timeline.rs:1) | new file |
| [docs/migrations/session-timeline-lifecycle.md](/Volumes/Data/codes/ai/agent-runtime-u78/docs/migrations/session-timeline-lifecycle.md:1) | new file |

## Public API

- `COMMAND_SCHEMA_VERSION = 2`; `StartSessionMode::{Create, Resume, Ephemeral}`;
  `StartSession::{create(id, seed), resume(id), ephemeral(history)}` and
  `with_lcm_policy`. Wire `mode` is required; `seed` replaces `initial_history`.
  No implicit upsert or silent resume seed overwrite remains.
- `SessionHandle::{resumed(), superseded_by()}`; `Runtime::fork_session(ForkSession)`;
  `ForkSeed::{Summary, FromIndex, Empty}` and `ForkLcm::{NewTimeline, Continue}`.
- Defaulted `LcmWriter::claim(view, owner, generation)`, `LcmClaimResult::{Claimed, Fork}`;
  `LcmRecoveryPolicy::{Adopt, Fork, Retire}`; defaulted resolver `new_timeline` and
  `continue_timeline` hooks. Unsupported claims/hooks fail closed. Claims are
  rechecked before provider admission and timeline commits.
- `LcmError::{TimelineOwned, LcmDivergence, ForkRequired}`; `RuntimeError.lcm` is
  `Option<Box<LcmFailure>>`, with `lcm_failure()` for typed borrowing. The allocation
  keeps the error under Clippy's large-error limit and does not change JSON shape.
  RangeOverlap/EntryConflict retain Conflict kind and typed evidence through the
  harness and driver.
- `TurnCheckpoint::{session_boundary, is_session_boundary}` protect exact seeds
  using existing Terminal wire data, initial sequence/revision one. Protected
  adapters with an initial-admission-only filter must additionally admit this
  narrowly validated boundary. Checkpoint schema and transition versions remain.

## Test and fixture edits

The ledger below records every edited test/fixture and its reason. Initial line
references describe pre-format source; the file map above gives current diff
locations. No failure expectation was weakened or removed. Representation-only
assertion edits borrow the same typed evidence after boxing.

# Test and fixture edits (before final tests)

- crates/agent-runtime/tests/integration/cache_evidence.rs:3491: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/cache_evidence.rs:3561: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/cache_evidence.rs:3606: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/cache_evidence.rs:3855: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_failed_turn_recovery.rs:317: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_failed_turn_recovery.rs:405: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_unsigned_reasoning.rs:282: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/interrupted_turn_admission.rs:335: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/interrupted_turn_admission.rs:458: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/interrupted_turn_admission.rs:489: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/interrupted_turn_admission.rs:507: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/interrupted_turn_admission.rs:538: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/interrupted_turn_admission.rs:559: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/interrupted_turn_admission.rs:581: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/interrupted_turn_admission.rs:642: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:1156: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:1273: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:1301: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:1336: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:1375: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:1422: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:1442: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:1523: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:1643: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:1873: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs:2007: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:1043: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:1115: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:1207: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:1295: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:1489: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:1593: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:1751: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:1832: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:1920: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/adaptive_cache.rs:2016: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests.rs:267: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests.rs:333: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests.rs:536: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:64: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:126: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:165: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:217: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:298: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:318: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:358: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:382: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:409: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:494: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:529: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:650: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/manifests/tests.rs:747: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/history.rs:364: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/src/conformance/history.rs:435: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:27: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:70: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:150: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:350: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:427: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:515: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:537: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:571: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:669: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:744: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:887: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:956: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:994: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:1112: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:1210: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:1416: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:1437: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:1494: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:1508: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/recovery.rs:1594: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/interaction.rs:365: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/interaction.rs:392: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/interaction.rs:432: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/interaction.rs:525: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/local_action.rs:304: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/runtime_conformance/local_action.rs:328: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/goal_conformance.rs:585: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/lcm_working_set.rs:271: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime-testkit/tests/conformance/lcm_working_set.rs:291: replaced implicit named startup with explicit resume; fixture already has saved state. No assertions changed.
- crates/agent-runtime/src/harness/lcm.rs: added explicit authorized claim support to agent_runtime_lcm::LcmWriter for TestStore; existing fixture focuses on projection/conflict/import/accounting, not ownership (new reference-store conformance covers ownership).
- crates/agent-runtime/tests/integration/lcm_legacy_resume_integration.rs: added explicit authorized claim support to LcmWriter for LcmFixtureStore; existing fixture focuses on projection/conflict/import/accounting, not ownership (new reference-store conformance covers ownership).
- crates/agent-runtime/tests/integration/lcm_expansion.rs: added explicit authorized claim support to LcmWriter for ExpansionStore; existing fixture focuses on projection/conflict/import/accounting, not ownership (new reference-store conformance covers ownership).
- crates/agent-runtime-testkit/tests/consumer_open_forge.rs: added explicit authorized claim support to LcmWriter for ConflictingStore; existing fixture focuses on projection/conflict/import/accounting, not ownership (new reference-store conformance covers ownership).
- crates/agent-runtime-testkit/src/conformance/history.rs: added explicit authorized claim support to LcmWriter for CountingStore; existing fixture focuses on projection/conflict/import/accounting, not ownership (new reference-store conformance covers ownership).
- crates/agent-runtime/tests/integration/lcm_failed_turn_recovery.rs: seeded orphan tail now records the same session owner before append, matching its unfinished-turn origin; healing assertions retained.
- crates/agent-runtime-testkit/src/consumers/nyx.rs: added nyx lifecycle migration request helper; product policy remains in fixture.
- crates/agent-runtime-testkit/src/consumers/smith.rs: added smith lifecycle migration request helper; product policy remains in fixture.
- crates/agent-runtime-testkit/src/consumers/open_forge.rs: added open_forge lifecycle migration request helper; product policy remains in fixture.
- crates/agent-runtime/tests/integration/lcm_expansion.rs: authorize construction then revoke before expansion; unknown-node branch uses a new store after permanent revocation. Preserves denied-expansion assertions under new fail-closed claim boundary.
- crates/agent-runtime-testkit/src/stores.rs: reference protected store admits validated initial idle session boundaries via TurnCheckpoint::is_session_boundary; ordinary admission and successor validation remain unchanged. Required to protect fork seeds before first turn.
- crates/agent-runtime/src/harness/lcm.rs: test-only LcmState literal adds default claim_generation=0 to match the additive state field; existing expectations preserved.
- crates/agent-runtime-testkit/tests/consumer_nyx.rs: seeded-history fixture uses consumers::nyx::start_request (ephemeral); existing preflight/terminal assertions retained.
- crates/agent-runtime-testkit/src/conformance/session_timeline.rs: new reusable authorized multi-timeline fixture, with durable/idempotent replacement and continuation hooks.
- crates/agent-runtime-testkit/tests/conformance/session_timeline.rs: new U7/U8 tests cover policy selection, leaf restart, immutable divergence, ownership authority, all session modes, consumer forks, exact seed redaction recovery, unfinished parent refusal, pending repair, unsupported default and end-to-end typed errors.
- crates/agent-runtime-testkit/src/conformance/mod.rs and tests/conformance.rs: register new fixture and conformance module.


# Edits after workspace-test failure (2026-10-04)

- Production: TurnCheckpoint::session_boundary and is_session_boundary now use checkpoint_sequence=1, preserving the existing sequence invariant. Fork tests exposed the zero sequence; no assertion was loosened.
- crates/agent-runtime-testkit/src/conformance/delegation/support.rs: durable_parent_session now reads the deterministic stores and selects create for first construction or resume for saved state. This helper is used by both scenarios; all 22 failing delegation assertions are unchanged.
- crates/agent-runtime/tests/integration/lcm_failed_turn_recovery.rs: diverged_lcm_timeline_heals_on_the_next_completed_turn now uses create rather than resume because its session/checkpoint stores are empty; the separately seeded same-owner LCM orphan tail and all healing assertions are retained.
- crates/agent-runtime-testkit/tests/conformance/session_timeline.rs: unfinished_parent_checkpoint_refuses_fork_without_provider_work seeds its accepted checkpoint with checkpoint_sequence=1 rather than the invalid 0. The busy/no-provider assertions are unchanged.

All 29 initial failed names are recorded in failed-tests.json; reruns select those exact names. No pass is claimed solely from changing expected results.

## Corrections after first targeted rerun

- Production: the initial idle Terminal boundary now uses state_revision=1 (checkpoint sequence also remains 1). This satisfies the existing core validator without relaxing its ordinary accepted-at-zero invariant. Updated the public field doc to explain the additive idle boundary.
- crates/agent-runtime-testkit/src/stores.rs: first-write filtering now exempts only a validated is_session_boundary record from the ordinary revision-zero/admission test. The helper recognizes the reserved session-boundary ID, Terminal shape, positive revision, initial sequence and full core validation. Required for five failing fork tests; assertions unchanged.
- First failed-only rerun: integration 1 passed/0 failed; conformance 23 passed/5 failed. Only the remaining five names are selected next.

## Additional recovery audit coverage

- Added transferred_claim_fences_old_owner_before_provider_admission and frontier_divergence_after_restart_can_recover_with_new_timeline_fork to crates/agent-runtime-testkit/tests/conformance/session_timeline.rs. These assert two previously untested U7/U8 edges: an old owner must stop at admission after a generation transfer, and a below-frontier startup error must not prevent the host from choosing a fresh-timeline fork. Existing tests/assertions are unchanged. Their first run is separate; only failures will be rerun.

## Fixes for additional recovery audit failures

- Both new audit tests failed before the fix: an old generation was admitted, and deferred fork-source loading required the divergent provider projection to resume.
- Production now rechecks claim epochs at provider admission, completed-turn commits and idle compaction. It claims legacy imports before source writes. No test assertion was changed.
- Fork source loading validates strict LCM identity and view authority while withholding the old projection from provider work; normal resume remains strict. Live fork sources get the same checks. NewTimeline/Continue bindings still validate source authority and replacement emptiness. This permits explicit host recovery after typed divergence, rather than reseeding existing resume snapshots in place.

## Additional redaction audit coverage

- Added ValueRedactingStore and policy_fork_seed_is_protected_before_first_turn_even_with_newer_redacted_values in the new session_timeline conformance module. Existing redaction coverage dropped Sensitive namespaces; this test also covers a newer ordinary snapshot that keeps the namespace but redacts its value, and the projection summary created by U7 Fork before the first turn. No existing fixture or assertion was weakened.

- Redaction audit fixture correction after its first failure: ValueRedactingStore now redacts the immutable U8 string/request namespaces in place and drops other Sensitive namespaces. Replacing structured CacheSessionState wholesale with a string violated its schema and masked the intended seed check. The expected original summary and exact-checkpoint assertions remain unchanged.

- Redaction audit after the schema-preserving fixture correction still failed: the resumed summary was the newer ordinary redacted value, not the exact seed. Production now protects U7 Fork seeds before the first turn and always overlays the compatible exact immutable summary/pending-fork namespaces. Other mutable namespace merge rules are unchanged. The expected original seed and exact-checkpoint assertions are unchanged.

## Clippy correction

- Initial clippy failed on clippy::result_large_err at 39 core sites (duplicated for lib/lib-test) because RuntimeError grew to 136 bytes. Production boxes only optional LCM evidence, keeps the wire shape unchanged, and adds RuntimeError::lcm_failure() for allocation-independent typed borrowing.
- crates/agent-runtime-testkit/tests/conformance/session_timeline.rs: payload assertions/patterns now borrow via lcm_failure() (and dereference the borrowed frontier count); expected kinds, owners, generations, frontier and serialized evidence are unchanged. This is a representation-only test edit after a lint failure, not a relaxed assertion.

- Clippy rerun had one remaining clippy::let_and_return in Runtime::fork_session. Return the async block directly; reservation lifetime and behavior are unchanged. No test/fixture/assertion edit.

## Commands and results

All Cargo commands used `CARGO_BUILD_JOBS=6`,
`CARGO_TARGET_DIR=$PWD/target`, and `TMPDIR=/Volumes/Data/tmp`.
Long output is under `/Volumes/Data/tmp/refactor/u78/`.

| Command / stage | Exit | Passed / failed / ignored | FAILED names |
| --- | ---: | --- | --- |
| `cargo test --workspace --all-features --no-fail-fast` | 101 | 1480 / 29 / 4 | All 29 listed below |
| Exact failed integration test | 0 | 1 / 0 / 0 (135 filtered) | None |
| Exact 28 failed conformance tests | 101 | 23 / 5 / 0 (152 filtered) | Five fork tests listed below |
| Exact remaining five conformance tests | 0 | 5 / 0 / 0 (175 filtered) | None |
| New ownership/restart audit, first run | 101 | 0 / 2 / 0 | Two audit tests listed below |
| Exact two audit failures | 0 | 2 / 0 / 0 | None |
| New value-redaction audit, first run | 101 | 0 / 1 / 0 | Value-redaction test below (fixture schema error) |
| Exact value-redaction failure after fixture correction | 101 | 0 / 1 / 0 | Same test (real exact-seed recovery failure) |
| Exact value-redaction failure after production fix | 0 | 1 / 0 / 0 | None |
| `cargo test -p agent-runtime-testkit --all-features --test conformance --no-fail-fast` | 0 | 183 / 0 / 0 | None |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings`, initial | 101 | 0 / 1 gate | `clippy::result_large_err` (39 unique core sites, duplicated for lib/test) |
| Same clippy command, first failed-gate rerun | 101 | 0 / 1 gate | `clippy::let_and_return` in `Runtime::fork_session` |
| Same clippy command, final failed-gate rerun | 0 | 1 / 0 gates; 0 warnings | None |
| `cargo fmt --all -- --check` | 0 | 1 / 0 gates | None; formatter reapplied after fixes |
| `cargo deny check`, initial | 1 | Policy checks not run | Advisory database lock was on a read-only path |
| `cargo deny --config /Volumes/Data/tmp/refactor/u78/deny-writable.toml check --show-stats` | 0 | 4 / 0 policy checks | None; 14 warnings (13 duplicate-version, 1 license) |
| Strict spec validation, initial and final changed-delta validation | 0 | 1 / 0 change checks each | None (`Valid`) |

The temporary deny config changes only `advisories.db-path`; dependency bans,
licenses, sources and advisory rules are identical to repository deny.toml.
The workspace command was not repeated wholesale. Its 29 failures were resolved
with exact-name reruns. Together with the three additional regression tests,
covered workspace tests total 1512 passing and 4 ignored; this is combined
coverage, not a claim that the original workspace command exited zero.
The full requested testkit gate passed 183 tests. Clippy compiles all current
workspace test targets, including the final boxed evidence representation.

Initial workspace FAILED names:

```text
lcm_failed_turn_recovery::diverged_lcm_timeline_heals_on_the_next_completed_turn
delegation_conformance::calling_model_checkpoint_refuses_resume_without_constructing_a_provider
delegation_conformance::child_completion_cursor_replays_without_reinjection_after_restart
delegation_conformance::child_completion_acceptance_failure_races_public_persist
delegation_conformance::child_completion_persists_before_crash_without_an_explicit_flush
delegation_conformance::interrupted_child_requires_explicit_idempotent_resume
delegation_conformance::ephemeral_child_outcomes_do_not_poison_parent_restart
delegation_conformance::follow_up_after_parent_restart_reuses_child_session_and_history
delegation_conformance::follow_up_bind_save_failure_rolls_back_dormant_state
delegation_conformance::durable_child_ownership_and_policy_fail_closed
delegation_conformance::follow_up_persists_ready_removal_before_send
delegation_conformance::expired_durable_child_remains_non_resumable
delegation_conformance::resume_bind_save_failure_rolls_back_dormant_state
delegation_conformance::restored_child_catalog_rejects_limit_mismatch
delegation_conformance::restored_child_catalog_rejects_duplicate_child_ids
delegation_conformance::restored_child_outcome_cursor_rejects_completed_turn_splice
delegation_conformance::restored_child_outcome_cursor_rejects_unknown_children
delegation_conformance::restored_child_outcome_cursor_rejects_variant_mismatch
delegation_conformance::restored_child_catalog_rejects_workspace_mismatch
delegation_conformance::restored_child_outcome_cursor_rejects_duplicates
delegation_conformance::stopped_durable_child_remains_terminal_after_restart
delegation_conformance::returned_child_input_survives_parent_restart_without_provider_work
delegation_conformance::terminal_artifacts_recover_after_parent_ledger_failure
session_timeline::continue_claims_new_owner_and_preserves_projection
session_timeline::partial_continue_fork_is_fenced_and_same_request_repairs_after_restart
session_timeline::protected_summary_survives_redacting_store_and_parent_resume_is_denied
session_timeline::forge_rotation_is_bounded_cacheable_and_supersedes_parent
session_timeline::smith_empty_and_history_index_forks_have_explicit_seeds
session_timeline::unfinished_parent_checkpoint_refuses_fork_without_provider_work
```

First exact conformance rerun FAILED names:

```text
session_timeline::protected_summary_survives_redacting_store_and_parent_resume_is_denied
session_timeline::partial_continue_fork_is_fenced_and_same_request_repairs_after_restart
session_timeline::smith_empty_and_history_index_forks_have_explicit_seeds
session_timeline::forge_rotation_is_bounded_cacheable_and_supersedes_parent
session_timeline::continue_claims_new_owner_and_preserves_projection
```

Additional recovery audit FAILED names:

```text
session_timeline::frontier_divergence_after_restart_can_recover_with_new_timeline_fork
session_timeline::transferred_claim_fences_old_owner_before_provider_admission
```

Additional redaction audit FAILED names:

```text
session_timeline::policy_fork_seed_is_protected_before_first_turn_even_with_newer_redacted_values
```


## Forge 3.6 adoption

Implement atomic claims and durable, idempotent authorized resolver hooks;
admit protected initial idle session boundaries. Configure U6 WorkingSetPolicy
and keep host-owned instructions, guard/authority checks, ordinary redaction and
exact protected state. At topic genesis/handoff call fork_session with a fresh
ID, Summary seed and NewTimeline; resume that ID on later turns. Continue retains
existing projection and is not a bounded topic reset. Match lcm_failure() instead
of parsing error messages. Explicit fork can archive an idle divergent source
on restart while keeping strict identity and authority validation.

Remove V149 timeline retirement and stale-LCM-state deletion for tunable changes
once claims and topic forks are deployed. Strict store/binding/classifier/guard
identity mismatches still require explicit migration. Smith /new uses an Empty
fork; Nyx reconstructed per-call history uses ephemeral. The full consumer-break
list is in CHANGELOG.md, with examples in docs/migrations/session-timeline-lifecycle.md.

## U9/U10 and release follow-ups

U9 adds SessionJournal and referenced checkpoints with a v3 compatibility adapter.
U10 makes the claimed LCM timeline canonical and removes the second history copy.
Those changes are not implemented here. Independent builds of the external
consumer repositories remain a coordinated release gate; all in-repo adapter
fixtures ran in the workspace gate. Unsupported stores need claim support before
LCM reuse; host binding decisions and confidentiality remain host responsibilities.
