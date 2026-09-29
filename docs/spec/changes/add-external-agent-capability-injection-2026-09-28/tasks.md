---
created_at: 2026-09-28T22:30:00Z
updated_at: 2026-09-28T22:30:00Z
completed_at:
---

## 1. Neutral contract
- [ ] 1.1 Add `ExternalCapabilities`, `SkillBundle`, `ExternalMcpServer` (stdio/http), `ExternalToolPolicy` to `agent::external`; validation per runtime-api scenarios.
- [ ] 1.2 Carry capabilities on `ExternalTurnRequest`; builder/session API to attach them; default empty.

## 2. Runtime tool bridge
- [ ] 2.1 Add the `agent-runtime` feature `external-agent-bridge` (which implies
  `external-agent` and Tokio `net`/`io-util`) and a minimal streamable-HTTP MCP
  server bound to loopback on an ephemeral port. Authenticate every request
  with a per-turn bearer token, bound request bodies, support initialize,
  initialized notifications, tools/list, tools/call, ping, 405 for GET, and
  minimal `Mcp-Session-Id` handling.
- [ ] 2.2 Project the sealed runtime tool registry into MCP tool descriptors and
  dispatch `tools/call` through the existing prepare/authorize/approve/invoke
  executor path. Emit the ordinary runtime tool and approval lifecycle events,
  return MCP text results (including structured output serialized as JSON),
  map denials/failures to `isError: true`, and never append bridge exchanges to
  canonical history because the external CLI owns that conversation.
- [ ] 2.3 Start the bridge before `ExternalAgentBackend::run_turn`, put its
  `ExternalToolBridge { url, bearer_token, tools }` description on the request,
  allow bounded concurrent calls, and invalidate/stop the listener when the
  backend refuses or the external turn completes, fails, or is cancelled.

## 3. `agent-runtime-agent-cli` package
- [ ] 3.1 Scaffold crate, features `claude`/`codex`, process supervision reuse from command-provider (process group, stdin=/dev/null, bounded output).
- [ ] 3.2 Claude adapter: materialize plugin dir + mcp json; argv per design; stream-json -> `ExternalAgentEvent` (tool_use/tool_result, `permission_denials`, `result.is_error`); `--session-id`/`--resume`.
- [ ] 3.3 Codex adapter: session home + skills, `-c mcp_servers.*` incl. per-tool approval, `exec --json` -> events (`mcp_tool_call`, `command_execution`, `turn.completed.usage`); `exec resume`. Resolve design Open Question 1 first.
- [ ] 3.4 Version preflight for both.

## 4. Verification
- [ ] 4.1 Unit tests: argv/file materialization snapshots; event decoders against captured transcripts from `target/cli-injection-lab/work/*/` (checked in as trimmed fixtures).
- [ ] 4.2 Bridge tests with the testkit fake: approval required/denied, teardown refuses late connections.
- [ ] 4.3 Ignored-by-default live tests (`--ignored`, env-gated) reproducing the lab scenarios for Claude and Codex.
- [ ] 4.4 Docs: `docs/external-agents.md` with per-CLI recipes incl. deferred Gemini/Cursor notes.
