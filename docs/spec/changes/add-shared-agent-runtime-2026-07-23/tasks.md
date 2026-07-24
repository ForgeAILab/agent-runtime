---
created_at: 2026-07-24T00:11:37Z
updated_at: 2026-07-24T06:13:44Z
completed_at:
---

## 0. Approval and Identity

- [x] 0.1 Approve the independent-repository model, shared mechanism/product
  policy boundary, package layout, migration order, and explicit non-goals.
- [ ] 0.2 Confirm the permanent repository name, remote location, and public
  package names before publication; `agent-runtime*` remains a working name.
  _(Still deferred, deliberately. `v0.1.0` is tagged under the working
  `agent-runtime*` names with `repository = "https://example.invalid/..."`. A
  git tag claims nothing publicly, so the naming decision stays open; this task
  gates **crates.io publication**, not the tag. Renaming before publication is
  still expected — see `docs/migration-0.1.md`.)_
- [x] 0.3 Confirm MIT licensing and record the exact Nyx source revision,
  contributor notices, and path mappings before importing implementation.
  _(MIT chosen; donor revision `7f51ccd` and path map recorded in
  `PROVENANCE.md`.)_

## 1. Workspace and Quality Baseline

- [x] 1.1 Create the Rust 2024 workspace with `agent-runtime-core`,
  `agent-runtime`, and `agent-runtime-testkit`, using Rust 1.86 as the minimum.
- [x] 1.2 Add formatting, Clippy-as-error, unit/doc tests, dependency/license
  checks, macOS/Linux CI, and public dependency-boundary tests.
- [x] 1.3 Add `PROVENANCE.md`, `CHANGELOG.md`, MIT license text, contribution
  guidance, and a policy forbidding consumer-domain dependencies.
- [x] 1.4 Document tagged dependency use and an uncommitted sibling path
  override for local cross-repository development.

## 2. Core Runtime Contracts

- [x] 2.1 Implement neutral IDs, messages/content, structured errors,
  cancellation, deadlines, redaction-safe metadata, and versioned events.
- [x] 2.2 Implement provider, tool, approval, workspace, session-store,
  secret-store, event-observer, and clock traits without consumer-domain types.
- [x] 2.3 Implement disjoint usage counters and per-counter provenance suitable
  for provider attempts, retries, tool loops, and consumer rollups.
- [x] 2.4 Add serialization fixtures and API-boundary tests that prevent Smith,
  Nyx, or Forge dependencies from entering the core package.
  _(Committed v1 event-envelope golden JSON plus exact representation tests;
  boundary enforced by `deny.toml` bans.)_

## 3. Provider Runtime

- [x] 3.1 Implement capability and model descriptors, normalized provider
  request types, vendor extension data, and explicit downgrade events.
- [x] 3.2 Implement typed streaming events for text, reasoning, tool-call
  assembly, finish state, errors, usage, and cache observations.
- [x] 3.3 Implement deterministic fake and configurable OpenAI-compatible
  adapters with cancellation, malformed-stream, and usage fixtures.
- [x] 3.4 Implement retry wrappers that record each attempt and never hide
  usage or retryability metadata.

## 4. Tool and Agent Execution

- [x] 4.1 Implement a deterministic tool registry, invocation context, typed
  results, declared effects, and name-conflict validation.
- [x] 4.2 Implement host approval and workspace enforcement before side
  effects, including deadlines, cancellation, and bounded output.
- [x] 4.3 Implement the direct streaming provider/tool loop with validated tool
  calls, canonical tool results, configured limits, and finalization.
- [x] 4.4 Implement side-effect-aware scheduling and deterministic ordering for
  overlapping writes.

## 5. Embeddable Runtime Facade

- [x] 5.1 Implement `RuntimeBuilder`, `Runtime`, and `SessionHandle` with
  injected host services and no required daemon.
- [x] 5.2 Implement versioned runtime commands/events, concurrent subscriber
  behavior, cancellation propagation, and bounded shutdown.
- [x] 5.3 Prove equivalent canonical behavior through headless and embedded
  test hosts using the same fake-provider scenario.

## 6. Source Transfer and Provenance

- [x] 6.1 Create temporary filtered source histories from the approved Nyx
  revision without modifying the Nyx working repository.
  _(A read-only path-filtered export was imported through a temporary
  repository; the 167-commit retained history and filtered tip are recorded in
  `PROVENANCE.md` → "Transfer method".)_
- [x] 6.2 Transfer reusable provider, agent-loop, and tool implementation into
  the approved neutral packages while retaining notices and commit provenance.
  _(Implementations transferred and upstream license notices retained;
  filtered commit ancestry and destination path mappings are retained.)_
- [x] 6.3 Remove Nyx product policy from public shared contracts and record each
  retained, refactored, or deferred source path in `PROVENANCE.md`.
- [ ] 6.4 Stop independent feature work on transferred copies and prepare the
  separate Nyx migration proposal that deletes superseded code.
  _(Ownership/no-independent-edit policy documented in `CONTRIBUTING.md` and
  `PROVENANCE.md`; the Nyx migration proposal is a separate consumer change —
  see section 8 — outside this proposal's approval boundary.)_

## 7. Conformance and Release Candidate

- [x] 7.1 Add reusable provider, tool, runtime, cancellation, event-schema, and
  shutdown conformance suites to `agent-runtime-testkit`.
- [x] 7.2 Add consumer adapter fixtures for Smith, Nyx, and Open Forge without
  importing their domain types into production packages.
- [x] 7.3 Establish pre-release compatibility CI and document how an explicitly
  breaking release coordinates consumer proposals.
- [x] 7.4 Tag the first `0.1.0` candidate only after the shared suite and all
  available consumer compatibility fixtures pass.
  _(Tagged `v0.1.0` after the combined validation: fmt, Clippy-as-error, 409
  workspace tests, doc tests, MSRV 1.86 across all seven production packages,
  the dependency-boundary checks, and the Smith/Nyx/Open Forge compatibility
  suites. Held until the dependent `add-registry-driven-context-runtime-2026-07-24`
  scope was also complete, per that change's task 0.2.)_

## 8. Consumer Handoffs

_All of section 8 is deferred: the approval boundary of this proposal does not
authorize modifying Nyx, Smith, or Open Forge. Each requires a separate approved
proposal in that consumer's repository._

- [ ] 8.1 Prepare a Nyx proposal to adopt the release and delete transferred
  implementations in the same change.
- [ ] 8.2 Rewrite the Smith proposal around one terminal-host package rather
  than parallel `smith-*` runtime crates.
- [ ] 8.3 Prepare an Open Forge proposal for a Forge-owned executor adapter;
  keep Forge task, event, database, and workspace policy in Forge.

## 9. Review Remediation

- [x] 9.1 Redact HTTP request diagnostics and make OpenAI byte streaming
  cancellation-, deadline-, usage-trailer-, and UTF-8-safe.
- [x] 9.2 Serialize all normalized OpenAI request options and preserve provider
  finish reasons through attempt and turn terminals.
- [x] 9.3 Serialize session turns, track every turn during shutdown, and apply
  one absolute shutdown timeout.
- [x] 9.4 Mint restart-safe session IDs and persist monotonic turn, request,
  attempt, tool-call, event-id, and event-sequence counters.
- [x] 9.5 Cap attempt deadlines, make retry backoff cancellation-aware, and emit
  structured provider-attempt exhaustion.
- [x] 9.6 Apply one aggregate tool-result budget to rich content and runtime
  error messages.
- [x] 9.7 Validate registered tool arguments against JSON Schema before
  exposure or invocation and mark post-stream validation failures in usage.
- [x] 9.8 Add explicit command schema versioning, committed event-schema golden
  fixtures, and formatting/conformance regression coverage.
