---
created_at: 2026-10-04T00:00:00Z
updated_at: 2026-10-05T00:18:20Z
completed_at: 2026-10-05T00:18:20Z
---

# Tasks: Validate declared tool write scopes

## 1. Implementation

- [x] 1.1 Share the invocation containment check and validate frozen specs at build with typed tool/scope/root evidence only for configured workspaces.
- [x] 1.2 Document workspace-wide declarations, no-workspace behavior, and migration under Unreleased Breaking.
- [x] 1.3 Test rejected static scopes, accepted root/child scopes, workspace-dependent absolute paths, no-workspace builds, and unchanged argument-dependent invocation rejection.
- [x] 1.4 Audit declarations throughout the workspace and fix any newly invalid configured-workspace fixtures or examples.

The audit covered `with_write`, direct `Effect::Write`/`WriteScope`, and
`WriteTool` declarations in Rust and Markdown throughout the workspace,
including examples, CLI, harness, testkit, and doc code blocks. Existing
configured-workspace declarations are contained; no declaration fix was needed.

## 2. Gates

- [x] 2.1 Strictly validate both change folders and keep U.2 proposal-only.
- [x] 2.2 Run formatting, all-target/all-feature Clippy, workspace all-feature tests, workspace doc tests, and all three named consumer tests.

Verification: all five requested commands exited 0. Workspace all-feature
tests: 1,503 passed, 0 failed, 4 ignored (including doctests). Workspace doc
tests: 7 passed, 0 failed, 1 ignored. Named consumer tests: Nyx 6, Open Forge 5,
Smith 8 passed. Formatting and all-target/all-feature Clippy passed. Both
change folders passed strict spec validation. U.2 has no implementation.
The embeddable `agent-runtime-core` and `agent-runtime` packages also built
successfully with Rust 1.86.0.
