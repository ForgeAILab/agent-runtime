# Provenance

This repository's reusable provider, agent-loop, and tool mechanisms were seeded
from the **Nyx** project. This file records the donor, revision, path mappings,
retained notices, and material refactors so any transferred module can be
audited.

## Donor

| Field | Value |
| --- | --- |
| Source repository | Nyx (`gitea@git.mai1015.com:nyx/nyx.git`) |
| Exact revision | `7f51ccd4e073940d1d6f10c5eeb1efac4c0a35ca` (branch `master`) |
| Donor license | `MIT OR Apache-2.0` |
| Use here | Under the **MIT** option; upstream copyright notices retained (see `LICENSE`) |
| Minimum Rust | 1.86 (matches this workspace) |

## Transfer method

The donor working repository was read at the exact revision above and was not
rewritten. A temporary repository received a path-filtered `git fast-export`
containing the approved provider, agent-loop, tool, message, and security
sources. That 167-commit filtered history was imported into this repository and
joined to the shared-runtime root through a history-only merge. Its filtered
tip is `d827da0e10ff6ccc3ee1320425497298f72153dd`.

The implementation was then **neutral re-encoded** in the destination paths
below: reusable mechanisms were retained while Nyx product policy and domain
types were removed. The history-only merge deliberately keeps the donor paths
out of the current working tree while making every retained revision
reviewable. For example:

```sh
git log --follow -- crates/nyx-provider/src/openai.rs
git show d827da0:crates/nyx-provider/src/openai.rs
```

Once a component appears below, this repository is its canonical owner; the
Nyx copy must not evolve independently, and a consumer that adopts a shared
release deletes its superseded copy in the same consumer change (see
`CONTRIBUTING.md`).

## Path map (retained / refactored)

| Donor path (at revision) | Destination | Disposition |
| --- | --- | --- |
| `crates/nyx-provider/src/sse.rs` | `crates/agent-runtime/src/provider/sse.rs` | Retained: SSE frame parsing (newline normalization, blank-line framing, multi-`data:` join). |
| `crates/nyx-provider/src/openai.rs` (payload build, `OpenAiStreamState`, `OpenAiToolCallAccumulator`) | `crates/agent-runtime/src/provider/openai.rs`, `crates/agent-runtime/src/agent/assembler.rs` | Refactored: request-build and chunk mapping neutralized; **streamed tool calls are now surfaced** as validated calls (donor discarded them); network access moved behind an injected `HttpTransport`; no `reqwest`, no Nyx catalog. |
| `crates/nyx-provider/src/retry.rs` (`is_retryable`, `pick_delay`, backoff) | `crates/agent-runtime/src/provider/retry.rs` | Refactored: exponential backoff + rate-limit floor + `retry-after` honoring retained; vendor error-substring matching dropped. |
| `crates/nyx-tools/src/registry.rs` | `crates/agent-runtime/src/tool/registry.rs` | Retained: name-conflict fail-closed registration and insertion-order preservation; sealing. |
| `crates/nyx-agent/src/agent/engine.rs` (`ToolLoopEngine::run`) | `crates/agent-runtime/src/agent/driver.rs` | Refactored: control flow (assemble → stream → tools-or-done → append results → repeat) retained; **removed** all Nyx product policy (hard-coded prompts, product names, "FINAL STEP" text, presentation strings); **added** capability validation/downgrade, per-attempt retry recording, explicit turn deadline, and fail-closed approval. |
| `crates/nyx-tools/src/core.rs` (`Tool`, `ToolResult`, `ContentBlock`) | `crates/agent-runtime-core/src/tool.rs`, `crates/agent-runtime-core/src/content.rs` | Refactored: `Tool` gains a declared `effects()`; tool result content model retained; tool calls are first-class content (donor round-tripped them as a JSON string). |
| `crates/nyx-core/src/message.rs`, `crates/nyx-provider/src/lib.rs` (message/content/usage/request types) | `crates/agent-runtime-core/src/content.rs`, `crates/agent-runtime-core/src/provider.rs`, `crates/agent-runtime-core/src/usage.rs` | Refactored: role/content/message shapes retained; `UsageMetadata` generalized to **disjoint counters with per-record provenance**. |
| `crates/nyx-security/src/secret.rs` (`Secret` redaction) | `crates/agent-runtime-core/src/store.rs` (`Secret`) | Retained: `Debug`/`Display` render `[redacted]`; explicit `expose()`. |
| `crates/nyx-security/src/lib.rs` (`Sandbox::validate_*_path`), `os_sandbox.rs` (containment) | `crates/agent-runtime-core/src/workspace.rs` (`Workspace`) | Refactored: reduced to a neutral `contains`/`resolve` boundary; defaults made fail-closed (donor default was permissive identity). |

## Nyx product policy intentionally NOT transferred

Chat/Discord/Telegram adapters, memory/summarization, cron, workflows, skills,
system prompts (`NYX_HARNESS`, `NYX_SAFETY`, …), the provider catalog (client
IDs, `originator: "nyx"`), cost/budget subsystem, `nyx-security` env-var/keyring
branding, and all `Bot`/`Actor`/`DispatchKind`/`Channel` domain types. These
remain Nyx product policy and are out of scope for the neutral runtime.

## Greenfield in this repository (no donor source)

The following had no donor equivalent and were designed here: the reason-carrying
`Cancellation`; injectable `Clock`/`Deadline`; the unified `RuntimeError`
(`kind` + `retryable` + redaction); the **versioned event envelope** and
canonical `RuntimeEvent` vocabulary; the disjoint `UsageLedger` with provenance;
the neutral `ApprovalPolicy` (fail-closed); capability descriptors + explicit
downgrade; side-effect-aware tool scheduling; and the embeddable runtime facade
(`RuntimeBuilder` / `Runtime` / `SessionHandle`) with its event emitter and the
`agent-runtime-testkit` conformance suites.

## Consumer migrations

Adopting this runtime in Nyx, Smith, or Open Forge — and deleting each
consumer's superseded implementation — requires a **separate approved proposal
in that consumer's repository**. This change does not modify any consumer.
