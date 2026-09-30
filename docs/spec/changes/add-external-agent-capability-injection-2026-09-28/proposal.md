---
created_at: 2026-09-28T22:30:00Z
updated_at: 2026-09-28T22:30:00Z
---

## Why

`ExternalAgentBackend` can hand a whole turn to an installed coding CLI, but the
host cannot give that CLI anything: not the session's skills, not its MCP
servers, and not the runtime-owned tools a direct turn would have. An external
turn is therefore a weaker agent than a direct one, and every adopter who wants
parity must learn each CLI's config surface and write it into the user's global
config -- the one place they must not touch.

A lab run against the installed CLIs (`evidence/` in this change)
shows every surveyed CLI accepts per-run skills and MCP servers without editing
global config, and that headless denial of an unapproved tool never hangs. The
mechanisms differ per CLI, so the runtime needs one neutral description of
"what to inject" and per-CLI adapters that realize it.

## What Changes

- Add a neutral `ExternalCapabilities` value carried on `ExternalTurnRequest`:
  skill bundles (a directory with `SKILL.md`), MCP server definitions
  (stdio/http), and a tool policy (allowlist of qualified tool names, default
  deny, never prompt).
- Add a runtime-hosted **tool bridge**: an MCP server (stdio, spawned by the CLI
  as a child that connects back to the runtime over a per-turn local socket)
  exposing the session's runtime-owned tools. A bridge call is a
  runtime-dispatched call: it traverses prepare -> authorize -> approve ->
  invoke and is recorded in the canonical tool path. External CLI-native tools
  remain observation-only.
- Add an opt-in `agent-runtime-agent-cli` package with first-party backends:
  - **Claude Code** (verified live): `--plugin-dir` for skills,
    `--mcp-config` + `--strict-mcp-config`, `--setting-sources ''`,
    `--tools`/`--allowedTools`, `--permission-mode dontAsk`, stream-json
    normalization, `--resume` with capabilities re-passed.
  - **Codex** (verified live): skills via a session `CODEX_HOME/skills`
    (credential handling is an open decision),
    `-c mcp_servers.<name>.*` including per-tool `approval_mode`,
    `--ignore-user-config --strict-config`, `--ask-for-approval never`,
    `exec --json` normalization, `exec resume` with capabilities re-passed.
- Document Gemini CLI and Cursor Agent recipes as deferred adapters (mechanisms
  verified structurally; live model runs blocked on credentials).

## Impact

- Affected specs: `agent-execution`, `runtime-api`, `package-architecture`.
- Affected code: `crates/agent-runtime/src/agent/external.rs` (request type),
  external turn driver (bridge lifetime), `crates/agent-runtime-mcp` (server
  side of the bridge behind a feature), new `crates/agent-runtime-agent-cli`.
- Compatibility: additive. `ExternalCapabilities::default()` is empty and a
  backend that ignores it behaves as today. The default dependency graph does
  not grow.
- Security: injected MCP servers and skills execute with host authority inside
  the CLI. Only the bridge brings tool calls back under runtime approval; the
  proposal states that boundary rather than implying the CLI is sandboxed.

## Non-Goals

- Proxying the CLI's own built-in tools (shell, edit) through runtime approval.
- Interactive approval round-trips into the CLI's native permission prompt.
  v1 is static allowlist + deny; runtime approval happens inside the bridge.
- Isolating CLI credentials. Adapters reuse the user's login and never copy
  credential files. Where a CLI can only discover injected skills through a
  relocated home (Codex), the adapter requires an explicit credential or an
  opt-in home strategy -- see design.md, Open Questions.
- Shipping Gemini/Cursor adapters in this change.
