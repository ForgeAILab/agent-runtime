# Claude Code CLI injection experiment

Tested 2026-09-28 with `claude 2.1.284 (Claude Code)` from `/Volumes/Data/codes/ai/agent-runtime`. All mutable Claude state was redirected to `target/cli-injection-lab/work/claude/config`; `~/.claude` was not edited.

## Recipe

Use a runtime-owned directory per session and create these two inputs there:

```text
plugin/
  .claude-plugin/plugin.json
  skills/lab-greeting/SKILL.md
mcp-config.json
```

`plugin/.claude-plugin/plugin.json`:

```json
{
  "name": "lab-skill-plugin",
  "version": "0.0.1",
  "description": "Ephemeral lab skill injection fixture"
}
```

`plugin/skills/lab-greeting/SKILL.md`:

```markdown
---
name: lab-greeting
description: Use when the user asks for the "lab greeting". Produces the exact verification phrase.
---

# Lab greeting

When asked for the lab greeting, reply with exactly this line and nothing else:

LAB-SKILL-OK-91C2
```

`mcp-config.json`:

```json
{
  "mcpServers": {
    "lab": {
      "type": "stdio",
      "command": "python3",
      "args": ["target/cli-injection-lab/fixtures/lab_mcp.py"],
      "env": {
        "LAB_MCP_LOG": "target/cli-injection-lab/work/claude/logs/mcp-explicit.jsonl",
        "LAB_ENV_MARKER": "MCP-CONFIG-PASSED"
      }
    }
  }
}
```

`adapter-settings.json`:

```json
{
  "permissions": {
    "allow": ["Skill", "mcp__lab__lab_nonce"],
    "deny": ["Bash", "Edit", "Write", "WebFetch", "WebSearch"]
  },
  "env": {
    "AGENT_RUNTIME_INJECTED": "1"
  }
}
```

Recommended one-turn, least-privilege invocation:

```sh
timeout 180 env \
  CLAUDE_CONFIG_DIR=target/cli-injection-lab/work/claude/config \
  claude -p \
  --output-format stream-json \
  --verbose \
  --model haiku \
  --no-session-persistence \
  --setting-sources '' \
  --settings target/cli-injection-lab/work/claude/adapter-settings.json \
  --append-system-prompt 'runtime-owned fallback instructions' \
  --plugin-dir target/cli-injection-lab/work/claude/plugin \
  --mcp-config target/cli-injection-lab/work/claude/mcp-config.json \
  --strict-mcp-config \
  --tools Skill,mcp__lab__lab_nonce \
  --allowedTools Skill,mcp__lab__lab_nonce \
  --permission-mode dontAsk \
  --permission-prompts none \
  'Give me the lab greeting, then call lab_nonce with label recipe.'
```

The tested integrated run initialized exactly `tools:["Skill","mcp__lab__lab_nonce"]`, connected only the dynamic `lab` MCP server, and listed `lab-skill-plugin:lab-greeting`. See [`raw/20-final-adapter-recipe.stream.jsonl`](../work/claude/raw/20-final-adapter-recipe.stream.jsonl).

For resumable sessions, remove `--no-session-persistence`, add `--session-id <UUID>` on the first turn, and use `--resume <UUID>` later. Re-pass `--plugin-dir`, `--mcp-config`, `--strict-mcp-config`, `--tools`, `--allowedTools`, permission flags, and any explicit `--settings` on every resumed process. The experiment proves that MCP and plugin injection do not survive a resume launch by themselves.

In a Rust `Command`, pass the empty setting-source value as two argv entries, `"--setting-sources", ""`; the shell spelling above is `--setting-sources ''`.

Credential caveat: the isolated `CLAUDE_CONFIG_DIR` reported `loggedIn:false` and `apiKeySource:"none"`, even after copying the user's small state file into it. An adapter using an isolated config directory must also supply a usable API/provider credential through its environment or an isolated credential helper. With OAuth/keychain-only authentication, isolation needs separate product support; copying state is insufficient.

## Result matrix

| Mechanism | Result in 2.1.284 |
|---|---|
| `--mcp-config <file>` | Verified: dynamic server connected and tool exposed before the API call. |
| `--mcp-config '<json>'` | Verified: inline JSON also connected; server name changed the tool prefix. |
| `--strict-mcp-config` | Verified against a competing project MCP source: only the flag-supplied server started. |
| `--allowedTools` / `--disallowedTools` | Configuration accepted; denied tools disappeared from the init tool list. A live authorization decision was not reached. |
| `--permission-mode dontAsk` + `--permission-prompts none` | Init reported `dontAsk`; installed help says prompt-requiring calls are denied automatically. Actual denied-call event is **UNVERIFIED**. |
| `--dangerously-skip-permissions` | Verified init mode `bypassPermissions`; this is the all-tools alternative. |
| `--plugin-dir` with `skills/` | Verified: namespaced skill registered; direct slash invocation expanded the exact `SKILL.md` body. |
| `--add-dir <root>` with `<root>/.claude/skills` | Negative: the skill was not discovered. The same root used as the primary cwd did discover it. |
| `--settings <file>` | Verified: permission denies affected the tool list and settings env reached the MCP subprocess. |
| `--append-system-prompt` | Verified in the persisted `prompt_snapshot`. Model response effect is **UNVERIFIED**. |
| `--setting-sources user` / empty | Verified by an env marker inherited by MCP with `user`, and `null` with the empty value. |
| `--session-id` / `--resume` | Verified same session ID and history file; ephemeral MCP/plugin flags had to be passed again. |

## 1. MCP injection

### File form, tool name, environment, and strict isolation

The strict run used:

```sh
claude -p --output-format stream-json --verbose --model haiku \
  --no-session-persistence --setting-sources '' \
  --settings "$WORK/settings.json" \
  --mcp-config "$WORK/mcp-config.json" --strict-mcp-config \
  --allowedTools mcp__lab__lab_nonce \
  --disallowedTools Bash Edit Write WebFetch WebSearch \
  --permission-mode dontAsk --permission-prompts none \
  'Call the lab_nonce tool with label strict, then reply with only its result.'
```

Trimmed `system/init` output:

```json
{"session_id":"1012f7cd-61af-4e33-85cc-6a86059af2e6","tools":["...","mcp__lab__lab_nonce"],"mcp_servers":[{"name":"lab","status":"connected","source":"dynamic"}],"permissionMode":"dontAsk"}
```

The model/stream tool name is therefore `mcp__lab__lab_nonce`. The corresponding MCP log proves process start, handshake, list request, and env propagation:

```json
{"pid":30802,"event":"start","env_marker":"MCP-CONFIG-PASSED"}
{"pid":30802,"event":"rpc","method":"initialize"}
{"pid":30802,"event":"rpc","method":"notifications/initialized"}
{"pid":30802,"event":"rpc","method":"tools/list"}
```

The cwd also contained a project `.mcp.json` server named `ambient`, enabled by the explicit settings file. With strict mode, init contained only `lab` and the ambient log did not yet exist. The otherwise-equivalent non-strict run produced:

```json
{"tools":["...","mcp__ambient__lab_nonce","mcp__lab__lab_nonce"],"mcp_servers":[{"name":"ambient","status":"connected","source":"project"},{"name":"lab","status":"connected","source":"dynamic"}]}
```

Thus strict mode excludes non-flag MCP sources. A real user-scoped server was deliberately not created or changed, but installed help states that strict mode ignores “all other MCP configurations”; the competing project source demonstrates that behavior without touching user config.

Evidence: [strict stream](../work/claude/raw/01-mcp-strict.stream.jsonl), [non-strict stream](../work/claude/raw/02-mcp-nonstrict.stream.jsonl), and [MCP log summary](../work/claude/raw/21-mcp-log-summary-final.txt).

### Inline JSON form

This also worked:

```sh
--mcp-config '{"mcpServers":{"inline":{"type":"stdio","command":"python3","args":["target/cli-injection-lab/fixtures/lab_mcp.py"],"env":{"LAB_MCP_LOG":"target/cli-injection-lab/work/claude/logs/mcp-inline.jsonl","LAB_ENV_MARKER":"INLINE-JSON-PASSED"}}}}' \
--strict-mcp-config
```

Init exposed `mcp__inline__lab_nonce`, and the server log contained `env_marker:"INLINE-JSON-PASSED"`. Evidence: [inline JSON stream](../work/claude/raw/08-mcp-inline-json.stream.jsonl).

### Was the tool invoked?

**UNVERIFIED.** Every model turn stopped at authentication with `Not logged in · Please run /login`. All MCP logs were searched for both a `tools/call` RPC and the fixture's `event:"call"`; neither existed. The positive evidence stops at connection and `tools/list`, so this report does not claim a nonce result was obtained through Claude.

An attempt to exercise the CLI against a deterministic loopback Anthropic-compatible server also failed because the sandbox rejected `bind(127.0.0.1, 38123)` with `PermissionError: [Errno 1] Operation not permitted`. Evidence: [local mock failure](../work/claude/raw/15-local-mock-bind-failure.txt).

## 2. Headless tool approval

`--allowedTools` is an approval allowlist, not an availability allowlist. To expose only the injected MCP tool, also use `--tools`:

```sh
--tools mcp__lab__lab_nonce \
--allowedTools mcp__lab__lab_nonce \
--permission-mode dontAsk \
--permission-prompts none
```

That run's complete init tool list was:

```json
"tools":["mcp__lab__lab_nonce"]
```

When skills are injected too, retain the built-in skill loader:

```sh
--tools Skill,mcp__lab__lab_nonce \
--allowedTools Skill,mcp__lab__lab_nonce
```

The integrated run showed exactly those two tools. Separately, `--disallowedTools Bash Edit Write WebFetch WebSearch` removed those names from init while leaving the remaining default tools visible.

For all-tools automatic approval, the tested switch was:

```sh
--dangerously-skip-permissions
```

Init then reported `"permissionMode":"bypassPermissions"` and the full default tool set.

What happens to an unallowed live tool call is **UNVERIFIED** because no model call occurred. The locally installed `--help` text is explicit that `--permission-prompts none` means nobody answers and “anything that would prompt is denied automatically”; therefore the non-hanging headless recipe is `dontAsk` plus `none`. No denied stream event was observed, so its exact event shape must not be assumed.

Evidence: [allow-only stream](../work/claude/raw/09-permissions-allow-only.stream.jsonl), [bypass-all stream](../work/claude/raw/10-permissions-bypass-all.stream.jsonl), and [integrated stream](../work/claude/raw/20-final-adapter-recipe.stream.jsonl).

## 3. Skill injection and prompt fallback

### `--plugin-dir`: positive

The plugin passed `claude plugin validate` (only a non-fatal missing-author warning). With `--plugin-dir "$WORK/plugin"`, init contained:

```json
{"slash_commands":["...","lab-skill-plugin:lab-greeting","..."],"skills":["...","lab-skill-plugin:lab-greeting","..."],"plugins":[{"name":"lab-skill-plugin","path":".../work/claude/plugin","source":"lab-skill-plugin@inline","version":"0.0.1"}]}
```

A direct command prompt, `/lab-skill-plugin:lab-greeting`, caused Claude to expand the skill locally before authentication. The persisted companion message was:

```json
{"type":"user","isMeta":true,"turnCompanion":true,"message":{"role":"user","content":[{"type":"text","text":"Base directory for this skill: .../plugin/skills/lab-greeting\n\n# Lab greeting\n\nWhen asked for the lab greeting, reply with exactly this line and nothing else:\n\nLAB-SKILL-OK-91C2\n"}]}}
```

This proves registration and explicit load. Automatic model selection from the description “Use when the user asks for the lab greeting” is **UNVERIFIED** because Haiku never ran.

Evidence: [plugin stream](../work/claude/raw/03-plugin-skill.stream.jsonl), [direct command stream](../work/claude/raw/14-direct-skill-command.stream.jsonl), and [skill companion excerpt](../work/claude/raw/14-skill-companion.jsonl).

### `--add-dir` with `.claude/skills`: negative

From a neutral cwd, this did not register the skill:

```sh
--add-dir target/cli-injection-lab/work/claude/add-dir-root
```

The added root contained `.claude/skills/lab-greeting/SKILL.md`, but init had no `lab-greeting` in either `skills` or `slash_commands`. As a control, running Claude with that same root as the primary cwd did list unnamespaced `lab-greeting`. Therefore the file layout was valid; 2.1.284 simply did not discover skills through `--add-dir`.

Evidence: [add-dir stream](../work/claude/raw/04-add-dir-skill.stream.jsonl) and [cwd control stream](../work/claude/raw/04b-cwd-skill-control.stream.jsonl).

### `--append-system-prompt`: verified fallback

The first persisted session used:

```sh
--append-system-prompt 'APPEND-MARKER-5E77: when asked for the append marker reply APPEND-OK-5E77.'
```

Its `prompt_snapshot.systemPrompt[-1]` was exactly that string:

```json
{"type":"attachment","attachment_type":"prompt_snapshot","appended":"APPEND-MARKER-5E77: when asked for the append marker reply APPEND-OK-5E77."}
```

This is a reliable per-session instruction fallback. The requested response could not be observed without authentication. Evidence: [prompt snapshot excerpt](../work/claude/raw/11-append-prompt-snapshot.jsonl).

## 4. Settings and setting-source control

The explicit `--settings "$WORK/settings.json"` file contained permission rules and:

```json
{"env":{"LAB_ENV_MARKER":"SETTINGS-FLAG-PASSED"}}
```

Even with `--setting-sources ''`, the explicit settings file still applied: denied built-ins disappeared from init and the MCP subprocess logged:

```json
{"event":"start","env_marker":"SETTINGS-FLAG-PASSED"}
```

An isolated user settings file at `$CLAUDE_CONFIG_DIR/settings.json` set `LAB_ENV_MARKER=USER-SETTING-SOURCE-PASSED`. With `--setting-sources user`, MCP inherited that value. With `--setting-sources ''`, an otherwise equivalent MCP process logged `env_marker:null`. Thus the flag controls standard user/project/local sources but does not suppress an explicitly passed `--settings` file.

Evidence: [settings stream](../work/claude/raw/05-settings-flag.stream.jsonl), [user source stream](../work/claude/raw/06-setting-sources-user.stream.jsonl), and [no sources stream](../work/claude/raw/07-setting-sources-none.stream.jsonl).

## 5. Streaming events

The output is JSONL. The useful observed event shapes were:

MCP connection, tool exposure, skill registration, and session ID (`system/init`):

```json
{"type":"system","subtype":"init","session_id":"b24f20e0-b6d8-40e9-8efc-3cd6a46ad72e","tools":["Skill","mcp__lab__lab_nonce"],"mcp_servers":[{"name":"lab","status":"connected","source":"dynamic"}],"skills":["deep-research","lab-skill-plugin:lab-greeting","..."],"permissionMode":"dontAsk"}
```

Usage and terminal status (`result`):

```json
{"type":"result","subtype":"success","session_id":"b24f20e0-b6d8-40e9-8efc-3cd6a46ad72e","total_cost_usd":0,"usage":{"input_tokens":0,"output_tokens":0,"cache_creation_input_tokens":0,"cache_read_input_tokens":0},"permission_denials":[],"terminal_reason":"api_error","is_error":true,"result":"Not logged in · Please run /login"}
```

Skill load itself was visible in the persisted session as the `isMeta:true`, `turnCompanion:true` message shown above; the stream's init event only lists the skill.

MCP tool invocation and tool result stream events are **UNVERIFIED**. There was no live API response asking for a tool, and the MCP audit logs prove no `tools/call` occurred. No example is fabricated here.

Adapter warning: an authentication failure still emitted `subtype:"success"` on the final result wrapper. Treat `is_error:true`, `terminal_reason:"api_error"`, and/or nonzero process exit as failure; do not trust `subtype` alone.

## 6. Resume behavior

The first persistent run used explicit session ID `22222222-2222-4222-8222-222222222222` together with the plugin and MCP flags. Init showed both injections and a JSONL history file was created under the isolated config.

A second process used only:

```sh
--resume 22222222-2222-4222-8222-222222222222
```

It kept the same session ID, but init showed `mcp_servers:[]`, no `mcp__lab__lab_nonce`, and no `lab-skill-plugin:lab-greeting`. A third resume that re-passed `--plugin-dir`, `--mcp-config`, `--strict-mcp-config`, `--tools`, and `--allowedTools` restored both.

Conclusion: conversation history persists, but launch-scoped MCP and plugin configuration must be supplied on every resume. The first turn's appended prompt was recorded in the default system-prompt snapshot; its actual reuse by a model on resume is **UNVERIFIED** because every request failed authentication.

Evidence: [first session stream](../work/claude/raw/11-session-first.stream.jsonl), [resume without injection](../work/claude/raw/12-resume-without-injection.stream.jsonl), and [resume with reinjection](../work/claude/raw/13-resume-with-reinjection.stream.jsonl).

## 7. Isolation, files, cleanup, and process lifetime

Claude wrote only under the isolated work tree during model experiments:

- `$CLAUDE_CONFIG_DIR/.claude.json` and timestamped backups.
- `$CLAUDE_CONFIG_DIR/projects/<cwd-key>/<session-id>.jsonl` for the two persistent session tests.
- `$CLAUDE_CONFIG_DIR/telemetry/1p_failed_events.*.json` after authentication failures.
- The requested `--debug-file` under `work/claude/logs/`.
- No project-session JSONL files for runs using `--no-session-persistence`.

The main global state file remained byte-identical across the experiment check: size 1085 and SHA-256 `bf02873576f4b10fda30e55e32ff594ed7944b398311584ff2a6ce6257392a8e`. All mutation-capable Claude runs set `CLAUDE_CONFIG_DIR` to the lab work path. Read-only `--help`, `--version`, and `auth status` were the only direct global-config invocations.

Every observed MCP PID returned false to `kill -0` after its owning Claude process exited. The server received no explicit shutdown RPC in the fixture log; it terminated when the stdio client closed/exited. Evidence: [MCP process lifetime](../work/claude/raw/16-mcp-process-lifetime.txt).

## Gotchas

- `--strict-mcp-config` is the MCP isolation control. `--setting-sources ''` controls settings sources, not a substitute for strict MCP mode.
- `--allowedTools` grants approval; it does not by itself hide other tools. Use `--tools` for the exposed surface and `--allowedTools` for the automatic approval surface.
- Keep `Skill` in `--tools` when injected skills should be selected by the model.
- Plugin skills are namespaced as `<plugin-name>:<skill-name>`; primary-cwd `.claude/skills` are unnamespaced.
- `--add-dir` did not make nested `.claude/skills` discoverable in this version.
- Dynamic MCP processes start and receive environment values before model authentication succeeds. Failed turns can therefore still have subprocess side effects.
- `--no-session-persistence` prevents resume and project transcript creation, but failed-event telemetry files were still written inside the isolated config.
- A copied config state file did not carry OAuth/keychain authentication into an isolated `CLAUDE_CONFIG_DIR`.
- Live nonce invocation, exact MCP tool-use/result stream shapes, automatic description-based skill triggering, and an actual denied-call event remain **UNVERIFIED** because there was no usable credential and the sandbox also prohibited a loopback mock server.

Raw CLI help, version, auth status, streams, fixture logs, and extracted evidence are under [`target/cli-injection-lab/work/claude`](../work/claude/).

## Addendum — live verification (orchestrator, normal auth, no CLAUDE_CONFIG_DIR)

Same recipe minus `CLAUDE_CONFIG_DIR`/`--settings`; isolation kept by `--setting-sources '' --strict-mcp-config --no-session-persistence`.
Raw: `work/claude/raw/90-live.jsonl`, `work/claude/raw/91-deny.jsonl`.

- Skill auto-selected by description: `tool_use Skill {"skill":"lab-skill-plugin:lab-greeting"}` → output `LAB-SKILL-OK-91C2`. VERIFIED.
- MCP call: `tool_use mcp__lab__lab_nonce {"label":"live"}` → `tool_result NONCE-7F3A-live`; fixture log has one `event:"call"`. VERIFIED.
- Un-allowlisted tool under `--permission-mode dontAsk` (no `--allowedTools`): no hang; `tool_result is_error:true` "Permission to use mcp__lab__lab_nonce has been denied…", and final `result.permission_denials:[{tool_name,tool_use_id,tool_input}]`. VERIFIED.
- Gotcha: without stdin redirect the CLI waits 3s and warns; adapter must spawn with stdin=/dev/null (or pipe the prompt).
- Auth conclusion: do NOT isolate via `CLAUDE_CONFIG_DIR` for keychain/OAuth users; isolate via flags instead.
