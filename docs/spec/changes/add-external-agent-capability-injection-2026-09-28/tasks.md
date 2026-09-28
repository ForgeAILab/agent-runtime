---
created_at: 2026-09-28T22:30:00Z
updated_at: 2026-09-28T22:30:00Z
completed_at:
---

## 1. Neutral contract
- [ ] 1.1 Add `ExternalCapabilities`, `SkillBundle`, `ExternalMcpServer` (stdio/http), `ExternalToolPolicy` to `agent::external`; validation per runtime-api scenarios.
- [ ] 1.2 Carry capabilities on `ExternalTurnRequest`; builder/session API to attach them; default empty.

## 2. Runtime tool bridge
- [ ] 2.1 `agent-runtime-mcp` feature `bridge`: MCP server over a per-turn local socket that lists the session's tools and dispatches calls through the ordinary tool pipeline.
- [ ] 2.2 Stdio shim binary/entrypoint the CLI spawns; connects with socket path + one-turn token from env.
- [ ] 2.3 Turn driver owns bridge lifetime; teardown on terminal/cancel; canonical recording of bridge calls.

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
