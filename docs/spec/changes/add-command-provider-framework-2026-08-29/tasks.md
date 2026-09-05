---
created_at: 2026-08-30T02:33:01Z
updated_at: 2026-08-30T03:06:49Z
completed_at: 2026-08-30T03:06:49Z
---

## 0. Approval and baseline

- [x] 0.1 Strictly validate this proposal and record approval before Stage 2.
- [x] 0.2 Capture provider/facade dependency trees and focused test baselines;
  preserve the unrelated CLI/MCP and web-fetch worktree changes.
- [x] 0.3 Confirm the `../tui` provider composition seam without editing the
  consumer repository.

## 1. Public command-provider contracts

- [x] 1.1 Add an opt-in command-provider module with redaction-safe immutable
  process configuration, validated resource bounds, and typed construction
  errors.
- [x] 1.2 Add the trusted adapter/attempt-decoder contracts for model
  descriptors, capabilities, request validation, per-attempt argv/stdin, stdout
  normalization, and optional preflight/version parsing.
- [x] 1.3 Implement `CommandProvider` over the existing `Provider` trait and
  reject unsupported or unrepresented request semantics before process I/O.
- [x] 1.4 Document public invariants and add compile-level examples showing a
  consumer-owned named adapter.

## 2. Secure process mechanism

- [x] 2.1 Spawn only an absolute executable with direct argv, an exact cwd,
  cleared ambient environment, explicit environment entries, piped streams,
  bounded stdin, and no shell.
- [x] 2.2 Stream framed stdout through the attempt decoder while separately
  draining and withholding bounded stderr.
- [x] 2.3 Enforce per-frame/aggregate output bounds, exactly one valid terminal,
  no post-terminal semantics, exit-status mapping, and malformed-stream errors.
- [x] 2.4 Tie process-group termination to cancellation, deadline, setup
  rollback, decoder failure, normal completion, and stream drop with bounded
  cleanup.
- [x] 2.5 Add explicit optional preflight/version probing through the same
  constrained process path without spawning during construction or inspection.

## 3. Package and compatibility boundaries

- [x] 3.1 Add `command-provider` feature gates to the provider package and
  facade without changing default native-provider graphs or the Rust 1.86
  embeddable baseline.
- [x] 3.2 Select and audit any safe process-group dependency for MSRV, license,
  and cross-platform behavior; update dependency policy files if required.
- [x] 3.3 Add architecture tests proving consumer neutrality, feature isolation,
  no shell dependency, and no dependency on Smith, Nyx, or Open Forge.

## 4. Verification

- [x] 4.1 Add hermetic fixture executables/codecs for text, reasoning, tool-call,
  usage, terminal, and explicit version-probe success.
- [x] 4.2 Cover unsupported settings, invalid frames, oversized input/output,
  stderr redaction, non-zero exits, timeouts, cancellation, early stream drop,
  and descendant-process cleanup.
- [x] 4.3 Run focused provider/runtime tests, the full all-feature workspace,
  warning-denied Clippy, formatting, dependency policy, and Rust 1.86 gates.
- [x] 4.4 Run strict spec validation and `git diff --check`; record unavailable
  platform/live-consumer gates honestly.

Verification: focused command-provider conformance, provider tests, the full
all-feature workspace suite, warning-denied all-target Clippy, rustdoc warnings,
formatting, dependency policy, and `git diff --check` pass. The provider and
facade—including `command-provider`—check and test on Rust 1.86. Library
cross-checks pass for installed Windows MSVC and Linux GNU targets; process
lifecycle tests executed on macOS/Unix, not on live Windows or Linux hosts. No
live model CLI/provider or `../tui` product gate was run because concrete named
adapters and consumer edits are explicitly deferred to the next Smith change.

## 5. Documentation and consumer handoff

- [x] 5.1 Document native-versus-command provider composition, portable versus
  adapter-specific settings, capability failures, authentication/environment
  handling, probing, lifecycle, and diagnostics.
- [x] 5.2 Add a concise `../tui` handoff describing the future named provider
  kind/config mapping while explicitly deferring edits to that repository.
