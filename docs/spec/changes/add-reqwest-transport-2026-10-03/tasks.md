---
created_at: 2026-10-03T00:00:00Z
updated_at: 2026-10-03T09:13:27Z
completed_at:
---

## 1. Proposal

- [x] 1.1 Read repository rules, canonical CLI transport and consumer references.
- [x] 1.2 Validate proposal and record authorized approval before implementation.

## 2. Implementation

- [x] 2.1 Add optional dependencies, required policy, origin pin and timeout API.
- [x] 2.2 Move canonical transport checks, bounds, status mapping and streaming.
- [x] 2.3 Cover loopback/all-answer DNS checks, redaction, redirects, streaming,
  cancellation and response bounds; retain existing CLI assertions.
- [x] 2.4 Migrate CLI to a shared PublicHttps transport without policy changes.

## 3. Documentation and gates

- [x] 3.1 Add compile-checked usage, README package/usage, CHANGELOG and provenance.
- [x] 3.2 Verify optional-feature Rust 1.86 and default dependency isolation.
- [x] 3.3 Run all requested formatting, Clippy, workspace/doc, MSRV and deny gates.
- [x] 3.4 Validate final spec, tick tasks and record outcomes.
- [x] 3.5 Commit approval, implementation and docs in repository style; do not push.
- [x] 3.6 Add `ConfiguredOrigin` (one user-configured origin on any address class) for
      self-hosted model servers on the local network; mark the policy enum non-exhaustive.

Commit limitation: the proposal was committed as `fc58db7` (`spec: propose
shared policy-explicit reqwest transport`). The authorized approval commit
failed when the sandbox denied creating the worktree `index.lock`; per the
owner’s explicit fallback, all remaining work is left uncommitted. Task 3.5
therefore remains unchecked. Approval is recorded in proposal/meta before code.

## Verification

All required gates passed against the final implementation:

- `cargo fmt --all`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`
- `cargo test --workspace --doc`
- `cargo +1.86.0 build -p agent-runtime-registry -p agent-runtime-core -p agent-runtime-ability -p agent-runtime-provider -p agent-runtime-context -p agent-runtime-lcm -p agent-runtime-obs -p agent-runtime`
- `cargo +1.88.0 build -p agent-runtime-mcp -p agent-runtime-cli --all-features`
- `cargo +1.86.0 build -p agent-runtime-provider --features reqwest-transport`
- `cargo deny check`

`cargo deny check` initially could not populate the user's read-only Cargo
cache with a missing Windows-target crate. Re-running the same check with
`CARGO_HOME=/Volumes/Data/tmp/agent-runtime-reqwest-cargo-home` passed all four
checks (advisories, bans, licenses, sources), without changing `deny.toml` or
suppressing checks. Existing duplicate-version/unmatched-license warnings
remain warnings. Dependency versions did not change.

Focused provider tests and the unchanged CLI suites passed too. The shared
module retains all six original transport test assertion sets and adds six
policy/redaction/header tests; seven local-server integration tests cover
streaming, localhost resolution, redirect refusal, bounded error reads,
pending-request cancellation, stream drop, and invalid-header redaction.
The public usage example is a feature-enabled compiled doctest.

`cargo tree -p agent-runtime-provider --no-default-features -e normal`
contains no reqwest, url, rustls, or hyper. Strict spec validation and
`git diff --check` passed. No donor repositories were modified, no CodeGraph
index was created, and no commits were pushed. Gate logs are under
`/tmp/reqwest-gate-*.log` (the successful deny retry uses
`/tmp/reqwest-gate-deny-writable-cache.log`).
