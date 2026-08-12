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
| `crates/nyx-provider/src/sse.rs` | `crates/agent-runtime-provider/src/sse.rs` | Retained: SSE frame parsing (newline normalization, blank-line framing, multi-`data:` join). |
| `crates/nyx-provider/src/openai.rs` (payload build, `OpenAiStreamState`, `OpenAiToolCallAccumulator`) | `crates/agent-runtime-provider/src/openai.rs`, `crates/agent-runtime/src/agent/assembler.rs` | Refactored: request-build and chunk mapping neutralized; **streamed tool calls are now surfaced** as validated calls (donor discarded them); network access moved behind an injected `HttpTransport`; no `reqwest`, no Nyx catalog. |
| `crates/nyx-provider/src/retry.rs` (`is_retryable`, `pick_delay`, backoff) | `crates/agent-runtime-provider/src/retry.rs` | Refactored: exponential backoff + rate-limit floor + `retry-after` honoring retained; vendor error-substring matching dropped. |
| `crates/nyx-agent/src/system_prompt.rs` (`SectionBuilder`, `SystemPromptBuilder`, `StaticSection`/`FileSection`/`BudgetedFileSection`/`FnSection`, `budgeted_content`, token stats) | `crates/agent-runtime-context/src/prompt.rs` | Refactored: the composable section-assembly mechanism and budgeting retained; **removed** all Nyx product prompt text (`NYX_HARNESS`, `NYX_SAFETY`, component blurbs, phase prompts, workspace-bootstrap builders) and the `SkillsSection`; `nyx_core::PromptSection` replaced by a neutral local type; renders to core `Message`s. Landed first as the standalone `agent-runtime-prompt` crate, then **folded into `agent-runtime-context`**: the donor's token-stats estimator was dropped in favor of `SystemPromptBuilder::into_fragments`, which turns named sections into versioned `ContextFragment`s sized by the one authoritative `RequestSizer`. |
| `crates/nyx-obs/src/{lib,fanout,file,stdout}.rs` (`EventSink`, `FanoutSink`, file/stdout sinks) | `crates/agent-runtime-obs/src/{lib,fanout,file,cli}.rs` | Refactored: the async `EventSink` + fanout + sink pattern retained; sinks operate on the runtime's canonical `EventEnvelope` instead of the donor's parallel `Event` enum (dropped); **added** the `SinkObserver` observer-hook bridge, the `drive` stream pump, the `ObsRow` SQL projection, and an opt-in `SqliteSink`; donor OpenTelemetry sink not transferred. |
| `crates/nyx-tools/src/registry.rs` | `crates/agent-runtime-registry/src/collection.rs`, `crates/agent-runtime/src/tool/registry.rs` | Refactored: the name-conflict fail-closed registration, insertion-order preservation, and sealing were generalized into a neutral `Registry<T: Named>`/`Sealed<T>`. Landed first in `agent-runtime-ability`, then **moved into the registry kernel** as a generic primitive (re-exported from `agent-runtime-ability` for compatibility); the tool registry is now a thin specialization over it, holding tools via a local `ToolEntry` wrapper (the kernel's `Named` cannot be implemented directly for the foreign `Arc<dyn Tool>`), that keeps the JSON-schema validation. |
| `crates/nyx-skills/src/manifest.rs` (`Skill`, `SkillSource`) | `crates/agent-runtime-ability/src/skill.rs` | Refactored: reduced to the neutral core of a skill — name, routing description, inline-or-file instructions, supporting files, and free-form metadata. **Removed** all Nyx product policy: `SKILL.md` frontmatter parsing, trust levels, requirement/OS checks, npm/package discovery, routing-description lint, guard scanning, and the skill taxonomy. |
| `crates/nyx-agent/src/agent/engine.rs` (`ToolLoopEngine::run`) | `crates/agent-runtime/src/agent/driver.rs` | Refactored: control flow (assemble → stream → tools-or-done → append results → repeat) retained; **removed** all Nyx product policy (hard-coded prompts, product names, "FINAL STEP" text, presentation strings); **added** capability validation/downgrade, per-attempt retry recording, explicit turn deadline, and fail-closed approval. |
| `crates/nyx-tools/src/core.rs` (`Tool`, `ToolResult`, `ContentBlock`) | `crates/agent-runtime-core/src/tool.rs`, `crates/agent-runtime-core/src/content.rs` | Refactored: `Tool` gains a declared `effects()`; tool result content model retained; tool calls are first-class content (donor round-tripped them as a JSON string). |
| `crates/nyx-core/src/message.rs`, `crates/nyx-provider/src/lib.rs` (message/content/usage/request types) | `crates/agent-runtime-core/src/content.rs`, `crates/agent-runtime-core/src/provider.rs`, `crates/agent-runtime-core/src/usage.rs` | Refactored: role/content/message shapes retained; `UsageMetadata` generalized to **disjoint counters with per-record provenance**. |
| `crates/nyx-security/src/secret.rs` (`Secret` redaction) | `crates/agent-runtime-core/src/store.rs` (`Secret`) | Retained: `Debug`/`Display` render `[redacted]`; explicit `expose()`. |
| `crates/nyx-security/src/lib.rs` (`Sandbox::validate_*_path`), `os_sandbox.rs` (containment) | `crates/agent-runtime-core/src/workspace.rs` (`Workspace`) | Refactored: reduced to a neutral `contains`/`resolve` boundary; defaults made fail-closed (donor default was permissive identity). |

## Nyx product policy intentionally NOT transferred

Chat/Discord/Telegram adapters, memory/summarization, cron, workflows, the Nyx
skills product policy (`SKILL.md` frontmatter, trust levels, requirement/OS
checks, package discovery, routing lint, guard scanning — only a neutral `Skill`
data type is retained), system prompts (`NYX_HARNESS`, `NYX_SAFETY`, …), the
provider catalog (client
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

## Lossless Context Memory transfer (implemented; consumer adoption pending)

The implemented Lossless Context Memory transfer is based on Nyx revision
`9614842d8f614d7d41e00d8e73ed3d042764d451` (`chore(runtime): close phase 1
merge gates`). The donor is `MIT OR Apache-2.0`; this repository uses the MIT
option. Any transferred substantial source must retain the donor
`LICENSE-MIT` notice, `Copyright (c) 2025-2026 Nyx Contributors`, alongside the
destination `LICENSE` and this provenance record.

The neutral transfer is limited to the summary-DAG invariants, active-node and
frontier projection, token-targeted compaction selection, three-stage
strict-shrink escalation, and their conformance tests. The canonical
destinations are the `crates/agent-runtime-lcm/src/{summarize,planning,
pressure,projection,store}.rs` modules plus
`crates/agent-runtime/src/harness/lcm.rs` for turn/checkpoint integration. The
full source-to-destination map, donor behavior/test inventory, historical
pre-cutover baseline, and consumer binding rows are in
[`docs/spec/changes/add-lossless-context-memory-2026-08-11/transfer-baseline.md`](docs/spec/changes/add-lossless-context-memory-2026-08-11/transfer-baseline.md).

This records source transfer only; it does not claim final validation, a
release gate, or consumer adoption. No Nyx, Smith, or Open Forge repository has
been changed here. Each consumer adoption remains a separate approved change
with its own binding, store conformance, and recovery gate. Nyx channel
identity, Smith persistent agent-session identity, and Open Forge Room +
AgentIdentity authorization remain host policy. Nyx SQLite migrations,
settlement/vector-memory policy, provider prompts/catalog, scheduler policy,
and product memory authority are not transferred.
