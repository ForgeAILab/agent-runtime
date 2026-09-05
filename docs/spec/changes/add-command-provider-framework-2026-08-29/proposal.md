---
created_at: 2026-08-30T02:33:01Z
updated_at: 2026-08-30T03:06:49Z
---

## Why
Adopters such as Smith should be able to select either a native API adapter or
an installed model CLI without replacing Agent Runtime's provider, context,
tool, security, event, and lifecycle layers. The normalized `Provider` boundary
already supports that composition, but the workspace does not yet provide a
reusable, security-bounded subprocess mechanism for a product-owned CLI
protocol adapter.

## What Changes
- Add an opt-in command-provider mechanism to `agent-runtime-provider`, with a
  facade feature in `agent-runtime`, while keeping every existing default
  dependency graph and provider API compatible.
- Add a trusted adapter contract that declares model capabilities, translates
  one normalized `ProviderRequest` into a bounded command invocation, and
  normalizes machine-readable stdout into `ProviderStreamEvent`s.
- Add a reusable process host that uses an exact executable without a shell,
  clears the ambient environment, accepts only explicit environment values and
  a fixed working directory, bounds input/output and time, and terminates the
  process tree on cancellation, deadline, stream drop, or failure.
- Add optional explicit availability/version probing through the same bounded
  process path. Loading or inspecting configuration never probes or spawns.
- Validate capabilities and adapter representation before spawn; reject silent
  loss of tools, reasoning controls, structured output, or other request
  semantics.
- Add hermetic fixture-command conformance covering event ordering, tool-call
  fragments, malformed output, non-zero exits, limits, cancellation, cleanup,
  environment isolation, redaction, and version compatibility.
- Document the consumer handoff for `../tui`: Smith remains responsible for
  named provider kinds, layered settings, executable trust, secret resolution,
  and concrete Codex/Claude/other CLI protocol adapters.
- Keep autonomous CLI agents, CLI-owned MCP/tool loops, persistent hidden CLI
  sessions, shell command templates, and built-in vendor CLI adapters outside
  this framework change.

## Impact
- Affected specs: `command-provider`, `package-architecture`
- Affected code: `crates/agent-runtime-provider`, `crates/agent-runtime`,
  provider conformance fixtures/tests, workspace documentation
- Consumer impact: additive and opt-in; `../tui` is inspected for compatibility
  but is not modified by this change
