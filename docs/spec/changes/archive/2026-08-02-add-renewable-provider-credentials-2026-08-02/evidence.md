# Compatibility Evidence

Verified at `2026-08-02T18:53:44Z` against Agent Runtime base revision
`39ba8319207a8b51a6be84e2ad60a18edf2a5fc8` with the implementation present
as uncommitted working-tree changes.

## Verification

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features --no-fail-fast`
- `cargo +1.86.0 build -p agent-runtime-registry -p agent-runtime-core -p agent-runtime-ability -p agent-runtime-provider -p agent-runtime-context -p agent-runtime-obs -p agent-runtime --all-features`
- `python3 /Users/mai1015/.codex/skills/spec-toolkit/scripts/spec_toolkit.py validate add-renewable-provider-credentials --type change --strict`
- `git diff --check`

The workspace tests include provider-source acquisition, proactive refresh,
minimum-validity rejection, cancellation, timeout, exact-revision
invalidation, stale-invalidation races, one visible pre-output authentication
replay, no replay after semantic output, terminal replacement rejection,
attempt ceilings, OpenAI-compatible adapter conformance, consumer
compatibility fixtures, and active-secret canary checks.

## Schema and Compatibility

No new `RuntimeEvent` variant was introduced. Existing v5 through v11 schema
fixtures and all workspace schema tests pass. The direct API-key constructor
remains supported through the static credential-source compatibility adapter.

## Smith Handoff State

Smith may develop and test locally against this verified working tree. An
exact committed or released dependency revision is intentionally not claimed:
publication requires separate commit/release authorization. Task 5.4 remains
open until such a revision exists and is pinned by Smith.
