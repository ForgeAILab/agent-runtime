---
created_at: 2026-10-04T00:00:00Z
updated_at: 2026-10-04T00:00:00Z
status: proposed
---

Status: **PROPOSED — awaiting repository-owner review. Not approved or implemented.**

## Why

Nyx reports that its legacy adapter supplies the request shape required by the
ChatGPT Codex backend (`.../codex/responses`): top-level instructions, fallback
instructions without system content, a synthetic user input for system-only
conversations, omission of rejected tuning fields, reasoning defaults and
token-budget-to-effort mapping, and a lease-bound account header. This is
consumer-provided endpoint evidence; no Nyx repository was inspected.

The current shared adapter already has a ChatGPT preset and static account
header helper (`crates/agent-runtime-provider/src/responses.rs:198`,
`crates/agent-runtime-provider/src/responses.rs:209`), but its encoder still
puts system messages into `input` (`responses.rs:649`), emits temperature and
max output tokens (`responses.rs:391`), emits only explicitly named reasoning
effort (`responses.rs:403`), and rejects reasoning token budgets
(`responses.rs:592`). Here and below abbreviated provider paths are under
`crates/agent-runtime-provider/src/`. At the transport boundary it uses the
lease secret but never `lease.account()` (`responses.rs:1892`), despite the
existing lease account contract
(`crates/agent-runtime-core/src/provider_credential.rs:115`). A reusable,
explicit mechanism needs review before embedding further endpoint policy.

## What Changes

- Propose a bounded, host-supplied declarative request profile on Responses
  configuration, with the current encoding as the default and no URL-based
  activation. Compare this with a `RequestShaper`, lease-carried arbitrary
  header projection, and a host wrapper `Provider` in `design.md`.
- Permit configured top-level instruction projection, host-supplied fallback
  and system-only user content, tuning-field omission, reasoning default
  effort/summary, and a host-defined bounded token-budget mapping.
- Reuse `ProviderCredentialLease::account()` for an optional validated
  host-selected account header, bound to the same attempt's token. Do not add
  product-specific fields or unrestricted headers to the core lease.
- Require profile-aware preflight accounting and cache identity for any added
  context; preserve output/reasoning budgets even when their wire controls
  cannot be sent.
- Preserve stateless Responses restrictions and existing streaming/recovery
  behavior; add standard-versus-profile conformance fixtures.

## Impact

- Affected specs: `provider-runtime`, `provider-credentials`,
  `context-management` (ADDED requirements; existing truth is not edited).
- Affected future code: Responses config, validation, history/payload
  projection, lease-to-header construction, planning integration and tests;
  README and CHANGELOG. No new dependencies are proposed.
- Public API: a profile field on public `ResponsesConfig` breaks exact Rust
  struct literals; builders/default constructors retain existing behavior.
  A provider-builder alternative is available if the owner requires literal
  compatibility. No event or persisted credential schema changes are needed.
- Product policy stays with the host: endpoint selection, instruction text,
  synthetic text, omission mask, supported efforts, default effort/summary,
  mapping thresholds, header name, and whether account identity is required.
- Adding a backend profile does not change `TerminalOutputPolicy`: the existing
  ChatGPT preset already selects `UsageOnly` (`responses.rs:203`). It does not
  authorize provider history, hosted tools, login ceremony, or a Nyx migration.

Existing scoped work: the named-effort proposal
`docs/spec/changes/expand-responses-reasoning-efforts-2026-09-09/` addresses label
acceptance, which is already reflected in `responses.rs:599`. This proposal
preserves those bounded labels and adds mapping/default behavior rather than
reintroducing a global effort enumeration. The native Responses proposal
`docs/spec/changes/add-native-xai-responses-provider-2026-08-03/` remains the
stateless protocol baseline.
