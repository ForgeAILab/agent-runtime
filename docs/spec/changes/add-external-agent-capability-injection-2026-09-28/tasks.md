---
created_at: 2026-09-28T22:30:00Z
updated_at: 2026-09-29T03:30:00Z
completed_at: 2026-09-29T03:30:00Z
---

## 1. Neutral contract
- [x] 1.1 Add `ExternalCapabilities`, `SkillBundle`, `ExternalMcpServer` (stdio/http), `ExternalToolPolicy` to `agent::external`; validation per runtime-api scenarios.
- [x] 1.2 Carry capabilities on `ExternalTurnRequest`; builder/session API to attach them; default empty.

## 2. Runtime tool bridge
- [x] 2.1 Add the `agent-runtime` feature `external-agent-bridge` (which implies
  `external-agent` and Tokio `net`/`io-util`) and a minimal streamable-HTTP MCP
  server bound to loopback on an ephemeral port. Authenticate every request
  with a per-turn bearer token, bound request bodies, support initialize,
  initialized notifications, tools/list, tools/call, ping, 405 for GET, and
  minimal `Mcp-Session-Id` handling.
- [x] 2.2 Project the sealed runtime tool registry into MCP tool descriptors and
  dispatch `tools/call` through the existing prepare/authorize/approve/invoke
  executor path. Emit the ordinary runtime tool and approval lifecycle events,
  return MCP text results (including structured output serialized as JSON),
  map denials/failures to `isError: true`, and never append bridge exchanges to
  canonical history because the external CLI owns that conversation.
- [x] 2.3 Start the bridge before `ExternalAgentBackend::run_turn`, put its
  `ExternalToolBridge { url, bearer_token, tools }` description on the request,
  allow bounded concurrent calls, and invalidate/stop the listener when the
  backend refuses or the external turn completes, fails, or is cancelled.

## 3. `agent-runtime-agent-cli` package
- [x] 3.1 Scaffold crate, features `claude`/`codex`, and a shared supervised process runner (own process group killed on cancel/terminal, stdin=/dev/null, bounded stdout lines and stderr tail) plus owner-only per-turn directories.
- [x] 3.2 Claude adapter: materialize plugin dir + mcp json; argv per design; stream-json -> `ExternalAgentEvent` (tool_use/tool_result, `permission_denials`, `result.is_error`); `--session-id`/`--resume`.
- [x] 3.3 Codex adapter: session home + skills, `-c mcp_servers.*` incl. per-tool approval, `exec --json` -> events (`mcp_tool_call`, `command_execution`, `turn.completed.usage`); `exec resume`. Resolve design Open Question 1 first.
- [x] 3.4 Version preflight for both.

## 4. Verification
- [x] 4.1 Unit tests: argv/file materialization snapshots; event decoders against captured transcripts from `target/cli-injection-lab/work/*/` (checked in as trimmed fixtures).
- [x] 4.2 Bridge tests with a scripted backend acting as the CLI over loopback: approval required/denied, teardown refuses late connections.
- [x] 4.3 Ignored-by-default live tests (`--ignored`, env-gated) reproducing the lab scenarios for Claude and Codex.
- [x] 4.4 Docs: `docs/external-agents.md` with per-CLI recipes incl. deferred Gemini/Cursor notes.

## Verification log (2026-09-29)
- Offline: `cargo test -p agent-runtime --features external-agent-bridge`, `cargo test -p agent-runtime-agent-cli --all-features`, workspace clippy `--all-features -D warnings`, `cargo +1.86 check` of both packages.
- Live (Claude Code 2.1.284, haiku; Codex 0.158.0, gpt-6-luna): skill + injected MCP tool per CLI, and a runtime tool called back through the bridge per CLI -- all pass.
