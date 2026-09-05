---
created_at: 2026-08-29T18:20:37Z
updated_at: 2026-08-29T18:44:11Z
completed_at: 2026-08-29T18:44:11Z
---

## 0. Approval and active-change reconciliation

- [x] 0.1 Confirm this proposal is approved before editing production code.
- [x] 0.2 Reconcile the approved provider-preset and runtime-security-boundary
  contracts; record any constructor/conformance adjustments in this proposal
  before implementation.
- [x] 0.3 Capture the baseline worktree and preserve the unrelated web-fetch
  changes without modification.

## 1. Isolated CLI package

- [x] 1.1 Add `crates/agent-runtime-cli` as a workspace member with a library
  target and `agent-runtime` binary target.
- [x] 1.2 Keep Clap, Reqwest, signal, and terminal dependencies local to the
  CLI package; add a dependency-boundary assertion proving existing package
  graphs are unchanged.
- [x] 1.3 Implement the thin Clap entry point, `run` subcommand, typed provider
  and output enums, explicit model-limit flags, and actionable help text.

## 2. Configuration and provider composition

- [x] 2.1 Implement positional-or-piped prompt resolution, terminal/empty-input
  rejection, and model-limit validation before provider I/O.
- [x] 2.2 Resolve provider-specific credential environment variables and the
  `--api-key-env` override directly into `Secret` without exposing values in
  debug output or errors.
- [x] 2.3 Implement provider construction for OpenAI, OpenAI-compatible,
  Anthropic, xAI Responses, and Gemini Interactions using the existing public
  adapters and explicit `ResolvedModelProfile` limits.
- [x] 2.4 Implement the CLI-owned Reqwest transport with HTTPS/userinfo/fragment
  validation, disabled redirects, per-request DNS/IP restrictions, typed status
  mapping, bounded error bodies, TLS validation, and cancellation propagation.

## 3. Runner, streaming, and lifecycle

- [x] 3.1 Compose `RuntimeBuilder` without product prompts, tools, stores, or a
  daemon; subscribe before turn admission and execute exactly one user turn.
- [x] 3.2 Implement text rendering with assistant content only on stdout and
  diagnostics only on stderr.
- [x] 3.3 Implement JSONL rendering with exactly one canonical event envelope
  per line and no prose on stdout.
- [x] 3.4 Implement typed completion/failure outcomes, Ctrl-C turn interruption,
  bounded session shutdown, and binary-owned `0`/`1`/`2`/`130` exit mapping.

## 4. Verification

- [x] 4.1 Add parser and validation tests for every provider/output value,
  prompt source, empty input, base URL rule, credential variable, and model
  limit invariant.
- [x] 4.2 Add fake-provider runner tests proving subscription-before-send,
  text/JSONL stdout contracts, stderr separation, event ordering, completion,
  failure, cancellation, and shutdown.
- [x] 4.3 Add adversarial transport tests for redirects, DNS rebinding/address
  classes, IPv4-mapped IPv6, URL userinfo/fragments, HTTP status taxonomy,
  bounded bodies, and credential non-disclosure without public network access.
- [x] 4.4 Run `cargo fmt --check`, package tests, workspace tests,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`, MSRV
  verification, and dependency/license checks.
- [x] 4.5 Run supported-consumer compatibility checks and confirm no consumer
  source changes are required for this additive package.

## 5. Documentation

- [x] 5.1 Add CLI documentation covering installation, the exact command
  contract, provider environment variables, explicit limits, stdin, output,
  exit codes, and security restrictions.
- [x] 5.2 Update the root README and changelog with a minimal one-turn example
  and an explicit list of non-goals.
