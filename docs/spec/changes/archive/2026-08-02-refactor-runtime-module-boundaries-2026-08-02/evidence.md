# Implementation Evidence

## Baseline — 2026-08-02T16:34:02Z

The working tree contained only this untracked change directory when Stage 2
started. CodeGraph reported an up-to-date index with 168 files, 6,178 nodes,
and 19,894 edges.

### Stable module paths

- `agent_runtime::agent` and its crate-private `agent::driver` module
- `agent_runtime::delegation`
- `agent_runtime::harness`
- `agent_runtime::tool`
- `agent_runtime_core::check_set`
- `agent_runtime_core::checkpoint`
- `agent_runtime_testkit::conformance::delegation`

### Source inventory

| File | Baseline lines |
| --- | ---: |
| `crates/agent-runtime/src/agent/driver.rs` | 4,951 |
| `crates/agent-runtime/src/delegation/mod.rs` | 2,953 |
| `crates/agent-runtime-testkit/tests/runtime_conformance.rs` | 4,983 |
| `crates/agent-runtime-core/src/checkpoint.rs` | 1,991 |
| `crates/agent-runtime-testkit/src/conformance/delegation.rs` | 2,130 |
| `crates/agent-runtime/src/harness/live_abilities.rs` | 1,482 |
| `crates/agent-runtime-core/src/check_set.rs` | 2,471 |
| `crates/agent-runtime/src/tool/executor.rs` | 2,452 |

The selected files contained 19 `clippy::too_many_arguments` suppressions:
15 in the driver, three in checkpoint construction, and one in live-ability
sealing.

The runtime integration target contained 64 registered tests. Reusable
delegation conformance exposed 23 `assert_*` entry points, and the delegation
integration target registered the same 23 scenarios.

The aggregate SHA-256 over sorted JSON/snapshot fixture hashes was
`9aa576c993addd43a5bd43eb70350a95effd633aa2dca6bda9d882313f24d160`.
Individual event-envelope fixture hashes were also captured in the Stage 2
command log for versions 1 and 3 through 11.

### Green focused baseline

- `agent-runtime-core checkpoint`: 10 passed
- `agent-runtime-core check_set`: 29 passed (including three filtered grant
  tests selected by the name filter)
- `agent-runtime` library: 145 passed
- `runtime_conformance`: 64 passed
- `delegation_conformance`: 23 passed
- `goal_conformance`: 9 passed

All commands used `--all-features`; there were no failures or ignored tests.

## Final Evidence

Completed 2026-08-02T17:02:37Z.

### Resulting ownership

- Direct driver: five files; the largest is `tools.rs` at 1,436 lines, with
  `turn.rs` at 1,329 and the stable root at 780.
- Delegation runtime: six files; the largest is `lifecycle.rs` at 810 lines and
  the stable root is 98.
- Runtime conformance: a 12-line target root, six shared/scenario files, and
  exactly 64 registered tests. The recovery family is the largest at 1,614
  lines because the crash-boundary scenarios intentionally remain together.
- Checkpoint core: five files; the exhaustive transition relation is one
  482-line file, validation is 694 lines, and the store contract is 19 lines.
- Delegation conformance: six files; the largest is shared support at 631
  lines, with all 23 public `assert_*` entry points retained.
- Live abilities: six files; restoration/rebasing, activation,
  search/staging, session state, and tests are separate, and the largest file
  is 407 lines.
- `check_set.rs` production is 1,018 lines with 1,431 lines of private tests;
  `tool/executor.rs` production is 1,114 lines with 1,335 lines of private
  tests. Both production pipelines remain centralized.

The private `TurnMachineContext` removes repetitive construction/recovery
plumbing. Targeted `too_many_arguments` suppressions decreased from 19 to 16;
none were added.

### Compatibility comparison

- Runtime integration inventory: 64 before, 64 after.
- Public delegation assertions/tests: 23 before, 23 after.
- Aggregate fixture hash:
  `9aa576c993addd43a5bd43eb70350a95effd633aa2dca6bda9d882313f24d160`
  before and after.
- No `Cargo.toml` or `Cargo.lock` changed.
- Only the approved source hotspots and this change directory changed.

### Final gates

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  passed.
- `cargo test --workspace --all-features`: passed, including runtime,
  steering, goal, delegation, checkpoint/replay, event-schema, security, and
  Smith/Nyx/Open Forge consumer adapters.
- `cargo +1.86.0 check --workspace --all-targets --all-features`: passed.
- Strict spec validation: passed.
- `git diff --check`: passed.
