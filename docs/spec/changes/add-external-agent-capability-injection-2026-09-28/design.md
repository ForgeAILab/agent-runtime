## Context

`ExternalAgentBackend` (change `add-external-agent-backend`) runs one whole turn
in a CLI and normalizes its events. This change lets the session give that CLI
skills, MCP servers, and runtime tools. Evidence for every CLI mechanism below
is in `target/cli-injection-lab/reports/{claude,codex,gemini,cursor}.md`
(fixture: stdio MCP server `lab_nonce` + skill `lab-greeting`).

## Goals / Non-Goals

- Goals: one neutral injection value; per-CLI realization with no global
  config writes; runtime tools reachable from the CLI under runtime approval;
  deterministic headless behaviour (never wait on a prompt).
- Non-Goals: see proposal.

## Evidence summary

| | Claude Code 2.1.284 | Codex 0.158 | Gemini 0.58 | Cursor 2026.07 |
|---|---|---|---|---|
| Live model run | VERIFIED | VERIFIED | blocked (account tier) | blocked (not logged in) |
| Per-run MCP | `--mcp-config` json/file | `-c mcp_servers.<n>.{command,args,env}` | project `.gemini/settings.json` | project `.cursor/mcp.json` (no flag) |
| Exclude user MCP | `--strict-mcp-config` | `--ignore-user-config` (+ session home) | `--allowed-mcp-server-names` | isolated `CURSOR_CONFIG_DIR`/`HOME` only |
| Tool allowlist | `--tools` + `--allowedTools` | `enabled_tools` + per-tool `approval_mode` | `--policy` TOML (deny `*`, allow exact) | `.cursor/cli.json` `Mcp(srv:tool)` |
| Headless deny | `dontAsk`: error tool_result + `permission_denials` | `never`: `mcp_tool_call status:failed` | tool removed; `tool_not_registered` | source-only: `permissionDenied` |
| Skills | `--plugin-dir` (namespaced `plugin:skill`) | `$CODEX_HOME/skills`, project `.codex/skills` | project `.gemini/skills` | project `.cursor/skills`, `--plugin-dir` |
| Model tool name | `mcp__srv__tool` | event `{server,tool}` | `mcp_srv_tool` | UNVERIFIED |
| Resume keeps injection | no -- re-pass | no -- re-pass | no -- re-pass | no -- re-pass |

Universal findings: (1) injection is launch-scoped on every CLI, so the
adapter re-materializes it on each turn, including resumed ones; (2) no CLI
hung on a denied tool in headless mode; (3) relocating a CLI's config home
loses keychain/OAuth logins (Claude, Cursor, Gemini) -- isolate by flags.

## Decisions

- **Neutral value, per-CLI realization.** `ExternalCapabilities { skills,
  mcp_servers, tool_policy, bridge }` lives in `agent::external`; each backend
  maps it to argv/env/files. The runtime never renders CLI-specific config.
- **Per-turn materialization dir.** The adapter writes generated files (plugin
  dir, mcp json, policy) under a runtime-owned session directory, never in the
  user's workspace or global config, and re-creates them for every turn.
- **Tool bridge as an MCP server.** Runtime tools reach the CLI as one injected
  MCP server named `runtime`. The CLI spawns a tiny stdio shim which connects to
  a per-turn Unix socket (named pipe on Windows) owned by the turn driver; the
  socket path and a one-turn token are passed via env. A call enters the
  ordinary tool pipeline (authorize/approve/invoke) and its result is recorded
  canonically. Alternatives: streamable-HTTP loopback server (works for all
  four CLIs but needs a TCP listener and TLS-less auth token); rejected as the
  default, kept as fallback for CLIs without stdio MCP.
- **Approval bridging.** The CLI-side policy auto-allows the bridge's tools so
  the CLI never prompts; the real approval happens runtime-side inside the
  bridge call, which may block until the host answers. Third-party injected
  MCP servers get CLI-side allow/deny only and remain observation-only.
- **Headless posture.** Adapters always select the CLI's non-prompting mode
  (Claude `dontAsk`, Codex `--ask-for-approval never`, Gemini default + policy,
  Cursor no `--force`) and map CLI denials to `ToolCompleted { ok: false }`.
- **Failure detection.** Terminal status comes from the CLI's typed terminal
  (`result.is_error`/`terminal_reason`, `turn.completed`/`turn.failed`), not
  exit code or `subtype`; non-fatal warning items are not failures.
- **Package.** `agent-runtime-agent-cli`, opt-in, depends on `agent-runtime`
  (feature `external-agent`) and `agent-runtime-mcp` (feature `bridge`). Each
  CLI is a cargo feature (`claude`, `codex`). This reverses the earlier
  non-goal "adapters are the adopter's" for these two CLIs only; Smith may keep
  its own.

## Risks / Trade-offs

- CLI flag drift: adapters pin a supported version range and run a `--version`
  preflight; mismatches fail the turn with a clear message.
- Codex baseline prompt is ~79k input tokens per turn (mostly cached);
  reported via usage, not mitigated here.
- Skill namespacing differs (Claude `plugin:skill`); instructions that name a
  skill must use the adapter-reported name.
- The bridge gives the CLI a live channel into runtime tools for the turn's
  lifetime; token + socket are per-turn and torn down at the terminal event.

## Migration Plan

Additive. Existing `run_turn` implementations compile unchanged if
`ExternalTurnRequest` gains a field with `Default` and backends constructed via
struct literal are updated (crate-internal only).

## Open Questions

1. Codex skills need `CODEX_HOME/skills` (or the workspace's `.codex/skills`).
   Options: (a) session `CODEX_HOME` + require `CODEX_API_KEY`/`OPENAI_API_KEY`;
   (b) session `CODEX_HOME` with `auth.json` symlinked to the user's (token
   refresh may replace the link); (c) inline skill bodies via `AGENTS.md`-style
   instructions instead of native skills. Recommended: (a) when a key is
   configured, else (c).
2. Should third-party MCP servers also be routed through the bridge (proxy) so
   their calls get runtime approval and canonical records? Deferred; costs a
   second hop and re-implements server lifecycles.
