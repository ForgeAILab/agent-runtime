---
created_at: 2026-08-29T21:16:28Z
updated_at: 2026-08-29T21:46:53Z
completed_at: 2026-08-29T21:46:53Z
---

## 0. Approval and baseline

- [x] 0.1 Strictly validate this change and record the user's authorization to
  continue into Stage 2.
- [x] 0.2 Reconcile the active CLI, MCP, and runtime-security contracts and
  preserve the unrelated web-fetch worktree changes.
- [x] 0.3 Capture focused CLI/MCP package baselines before production edits.

## 1. Versioned CLI configuration

- [x] 1.1 Add strict version-1 TOML types for optional run defaults and named
  stdio MCP server definitions, rejecting unknown/invalid fields before I/O.
- [x] 1.2 Add explicit `--config`, merge file defaults below command flags,
  preserve flag-only compatibility, and keep prompt/secret values out of the
  file.
- [x] 1.3 Resolve config-relative paths, bare executables, and `env_from`
  references into a redaction-safe typed configuration with deterministic
  server ordering.
- [x] 1.4 Add parser/config tests for versioning, precedence, validation,
  relative resolution, missing environment sources, and no-I/O rejection.

## 2. Stdio MCP authority and inspection

- [x] 2.1 Add static `mcp inspect` output that exposes exact command metadata,
  environment names/mappings, tool allowlists, and bounds without spawning.
- [x] 2.2 Add repeatable server and exact tool approval flags; reject every
  unconfigured, duplicate, unselected, or non-allowlisted approval before
  spawn.
- [x] 2.3 Clear ambient variables in the shared stdio client and redact
  transport debug output; add regression tests for both guarantees.
- [x] 2.4 Resolve child variables only from named environment sources and pass
  the MCP package a minimal explicit environment.

## 3. Runtime composition and lifecycle

- [x] 3.1 Make run preparation asynchronous, connect selected servers in
  deterministic order, bind only exact approved tools, and convert them to
  `McpTool`s.
- [x] 3.2 Register MCP tools through `RuntimeBuilder`, an authoritative
  CLI-owned external-service security check, and an exact allowlist approval
  policy; do not use server annotations to narrow authority or `AllowAll`.
- [x] 3.3 Preserve live connections through the turn and add bounded cleanup
  after normal completion, startup rollback, failure, and Ctrl-C.
- [x] 3.4 Implement required-server failure and optional-server diagnostic
  behavior without contaminating text/JSONL stdout.
- [x] 3.5 Add hermetic composition and lifecycle tests covering no-consent,
  partial consent, conservative effects, failure isolation, cancellation, and
  cleanup.

## 4. Package and compatibility gates

- [x] 4.1 Add the CLI's stdio-only `agent-runtime-mcp` and TOML/path-resolution
  dependencies, declare CLI Rust 1.88, and keep remote HTTP disabled.
- [x] 4.2 Extend dependency-boundary tests for CLI leaf isolation, absent MCP
  HTTP dependencies, and unchanged embeddable package graphs.
- [x] 4.3 Run formatting, CLI/MCP tests, workspace tests, warning-denied
  all-feature Clippy, and package-specific MSRV checks.
- [x] 4.4 Run strict spec validation and `git diff --check`, then record any
  unavailable consumer/live gates honestly.

Verification: the focused CLI/MCP suites and the full all-feature workspace
suite pass; all-feature Clippy is warning-free; embeddable packages check on
Rust 1.86 and the MCP/CLI packages check on Rust 1.88; strict change validation
and `git diff --check` pass. Neutral in-workspace consumer gates pass. No live
provider or third-party MCP server was invoked; those remain environment-owned
integration gates rather than hermetic repository tests.

## 5. Documentation

- [x] 5.1 Document the version-1 file, flag precedence, explicit consent,
  `env_from`, inspection, failure behavior, examples, and exit/output contracts.
- [x] 5.2 Update README/changelog/development guidance and state the deferred
  remote MCP, OAuth, ambient discovery, persistent trust, and Smith-specific
  settings explicitly.
