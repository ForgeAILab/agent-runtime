# External agents with injected capabilities

An external agent backend runs a whole turn inside an installed coding CLI
(see `ExternalAgentBackend`). This document covers giving that CLI the
session's skills, MCP servers, and runtime-owned tools, and the two
first-party backends in `agent-runtime-agent-cli`.

```toml
[dependencies]
agent-runtime = { git = "https://github.com/ForgeAILab/agent-runtime.git", rev = "<sha>", features = ["external-agent-bridge"] }
agent-runtime-agent-cli = { git = "https://github.com/ForgeAILab/agent-runtime.git", rev = "<sha>", features = ["claude", "codex"] }
```

## What the host describes

`ExternalCapabilities` is CLI-neutral and validated at `RuntimeBuilder::build`:

| Field | Meaning |
|---|---|
| `skills` | `SkillBundle { name, dir }`: an absolute folder holding `SKILL.md` |
| `mcp_servers` | `ExternalMcpServer { name, transport }`: stdio command or streamable-HTTP url |
| `tool_policy.allow` | `AllowedTool { server, tool }`; `tool = "*"` allows the whole server |
| `runtime_tools` | expose the runtime's own tools through the bridge (feature `external-agent-bridge`) |

```rust
let runtime = RuntimeBuilder::new(model)
    .provider(provider)                // still required for model identity
    .tool(my_tool)
    .external_agent(Arc::new(ClaudeCodeBackend::new(ClaudeCodeConfig {
        cwd: workspace,
        model: Some("haiku".into()),
        ..Default::default()
    })))
    .external_capabilities(ExternalCapabilities {
        skills: vec![SkillBundle::new("release-notes", "/abs/skills/release-notes")],
        mcp_servers: vec![/* ... */],
        tool_policy: ExternalToolPolicy {
            allow: vec![AllowedTool::new(RUNTIME_BRIDGE_SERVER_NAME, "*")],
        },
        runtime_tools: true,
    })
    .build()?;
```

## Authority

- **Bridge tools are runtime-dispatched.** When `runtime_tools` is on, the
  turn starts a loopback streamable-HTTP MCP server named `runtime`
  (127.0.0.1, ephemeral port, per-turn bearer token). A call from the CLI goes
  through the ordinary prepare → authorize → approve → invoke pipeline and
  emits `ToolCallRequested`/`ToolCallCompleted`. Its exchange is not added to
  canonical history: for an external turn the CLI owns the conversation. The
  token is refused after the turn's terminal event.
- **Everything else is the CLI's.** Built-in CLI tools and tools from injected
  MCP servers run under the CLI's own policy. The runtime only observes them
  (`ExternalToolInvoked`/`ExternalToolCompleted`) and cannot approve them.
- **Nothing prompts.** Backends run the CLI non-interactively. A tool outside
  the allowlist is refused by the CLI and reported as a failed tool; the turn
  continues. Bridge tools are allowed CLI-side because their real approval
  happens in the runtime.
- **No global config is written.** Generated files live in a per-turn,
  owner-only directory that is removed when the CLI exits. Every turn,
  including a resumed one, re-applies the capabilities: no CLI surveyed keeps
  launch-scoped config across `--resume`.

## Claude Code (`feature = "claude"`, supports >=2.1.0 <3)

| Capability | Mechanism |
|---|---|
| Skills | generated plugin `--plugin-dir <turn>/plugin` with `skills/<name>/`; Claude names them `runtime-skills:<name>` |
| MCP servers | `--mcp-config <turn>/mcp.json --strict-mcp-config` (the user's own servers are excluded) |
| User settings | `--setting-sources ""` |
| Policy | `--permission-mode dontAsk` + `--allowedTools=mcp__<server>__<tool>,…` (`mcp__<server>` for `*`, plus `Skill`) |
| Continuation | `--session-id <uuid>` then `--resume <id>` |

Built-in tools (Bash, Edit, …) stay visible but are not pre-approved, so
`dontAsk` refuses them. The adapter uses the user's normal login; do not set
`CLAUDE_CONFIG_DIR` to isolate it, because that loses keychain authentication.
Failure is read from `result.is_error`/`terminal_reason`, never `subtype`
(an authentication failure still reports `"success"`).

## Codex (`feature = "codex"`, supports >=0.158.0 <0.200.0)

| Capability | Mechanism |
|---|---|
| MCP servers | `-c mcp_servers.<name>.{command,args,env}` or `{url,http_headers}` |
| Policy | `--ask-for-approval never`; per server `default_tools_approval_mode="prompt"` (refused under `never`) and `tools.<tool>.approval_mode="approve"` for allowed tools |
| Bridge | `mcp_servers.runtime.url` + `bearer_token_env_var` (the token is passed in the environment, not argv) |
| User config | `exec --ignore-user-config --strict-config` |
| Skills with `api_key` | a per-session `CODEX_HOME` holding `skills/<name>/`; the key is passed as `OPENAI_API_KEY` |
| Skills without `api_key` | the user's `CODEX_HOME` is kept; a skill catalog (name, description, path to a copied `SKILL.md`) is passed as `developer_instructions`, and the model reads a skill on demand |
| Continuation | `exec resume <thread-id>` with every flag re-passed |

Credential files are never copied. A per-session `CODEX_HOME` persists across
resumed turns; call `CodexBackend::cleanup_session_homes` when retiring the
session. Skill bodies are deliberately not pasted into `developer_instructions`:
a body written as a standing order ("reply with exactly this line") then
governs every request and suppressed unrelated MCP calls in live runs.
`--skip-git-repo-check` is on by default because the host chooses `cwd`.

## Verifying against the installed CLIs

Live tests are ignored by default and spend model tokens:

```sh
AGENT_RUNTIME_LIVE_CLAUDE=1 cargo test -p agent-runtime-agent-cli --features claude --test claude -- --ignored
AGENT_RUNTIME_LIVE_CODEX=1 AGENT_RUNTIME_CODEX_MODEL=<model> cargo test -p agent-runtime-agent-cli --features codex --test codex -- --ignored
AGENT_RUNTIME_LIVE_BRIDGE=claude cargo test -p agent-runtime-agent-cli --all-features --test bridge_live -- --ignored
AGENT_RUNTIME_LIVE_BRIDGE=codex  cargo test -p agent-runtime-agent-cli --all-features --test bridge_live -- --ignored
```

The skill/MCP tests use the fixtures in
`crates/agent-runtime-agent-cli/tests/fixtures/lab/`: a stdio MCP server
exposing `lab_nonce` (it logs every call to `$LAB_MCP_LOG`) and a
`lab-greeting` skill.

## Other CLIs (not shipped)

Surveyed on 2026-09-28 and verified structurally, but not against a live model:

- **Gemini CLI 0.58**: project `.gemini/settings.json` (`mcpServers`) and
  `.gemini/skills/`, `--allowed-mcp-server-names` for strict selection,
  `--policy` TOML (deny `*`, allow exact tools; allow `activate_skill`).
  Model tool names are `mcp_<server>_<tool>`. Needs `GEMINI_API_KEY`; the
  individual Code Assist tier is rejected by current builds.
- **Cursor Agent 2026.07**: no per-run MCP flag; project `.cursor/mcp.json`,
  `.cursor/cli.json` `permissions.allow = ["Mcp(<server>:<tool>)"]`,
  `.cursor/skills/`, `--approve-mcps`. The user's `~/.cursor/mcp.json` leaks
  into runs unless `CURSOR_CONFIG_DIR`/`HOME` are isolated, which then needs
  `CURSOR_API_KEY`.
