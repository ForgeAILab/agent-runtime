# Cursor Agent CLI injection experiment

Tested 2026-09-28 with `cursor-agent 2026.07.16-899851b` from
`~/.local/bin/cursor-agent`. Scratch state stayed under
`target/cli-injection-lab/work/cursor`. The user's `~/.cursor` files were read
only and were not edited.

This report uses three evidence labels:

- **VERIFIED**: observed in a command or fixture log.
- **SOURCE-VERIFIED**: confirmed in the exact installed 2026.07.16 bundle, but
  not exercised through a model turn.
- **UNVERIFIED**: the sandbox prevented the required model-backed run.

## Recipe

Cursor Agent has no `--mcp-config` or strict-MCP flag. The clean adapter recipe
is therefore an ephemeral primary project that is its own Git root, plus an
isolated Cursor profile. Stage the runtime-owned MCP and skill files in that
project, and provide an explicit API credential because profile isolation does
not preserve the tested OAuth/keychain login.

Assume:

```sh
ROOT=/Volumes/Data/codes/ai/agent-runtime
RUN="$ROOT/target/cli-injection-lab/work/cursor/session"
PROJECT="$RUN/project"
```

Make `$PROJECT` a distinct Git root. This matters when it lives below another
repository: Cursor resolves project configuration from the repository root.

```sh
mkdir -p "$PROJECT/.cursor/skills/lab-greeting" \
  "$RUN/config" "$RUN/data" "$RUN/home" "$RUN/node-cache" "$RUN/cache"
git -C "$PROJECT" init --template=
```

Create `$PROJECT/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "lab": {
      "command": "python3",
      "args": [
        "target/cli-injection-lab/fixtures/lab_mcp.py"
      ],
      "env": {
        "LAB_MCP_LOG": "target/cli-injection-lab/work/cursor/session/mcp-calls.jsonl",
        "LAB_ENV_MARKER": "CURSOR-PROJECT-MARKER"
      }
    }
  }
}
```

Create `$PROJECT/.cursor/cli.json` to auto-allow only the injected MCP tool:

```json
{
  "permissions": {
    "allow": ["Mcp(lab:lab_nonce)"],
    "deny": []
  }
}
```

Copy the fixture skill to
`$PROJECT/.cursor/skills/lab-greeting/SKILL.md`. Native project skills at that
path are supported by the installed bundle. A per-launch alternative is a local
plugin containing `.cursor-plugin/plugin.json` and
`skills/lab-greeting/SKILL.md`, passed with `--plugin-dir "$RUN/plugin"`.

Run with `$PROJECT` as the process working directory:

```sh
timeout 180 env \
  HOME="$RUN/home" \
  CURSOR_CONFIG_DIR="$RUN/config" \
  CURSOR_DATA_DIR="$RUN/data" \
  NODE_COMPILE_CACHE="$RUN/node-cache" \
  XDG_CACHE_HOME="$RUN/cache" \
  AGENT_CLI_CREDENTIAL_STORE=file \
  CURSOR_API_KEY="$CURSOR_API_KEY" \
  cursor-agent -p \
    --output-format stream-json \
    --trust \
    --approve-mcps \
    --model "$CHEAP_MODEL" \
    'Give me the lab greeting, then call lab_nonce with label recipe.'
```

`$CHEAP_MODEL` should come from `cursor-agent models` for the authenticated
account. Model listing required authentication here, so no model name was
guessed. Omit `--model` if the adapter cannot resolve one safely.

Do not add `--force` to the least-privilege recipe. `--approve-mcps` handles
server loading; `.cursor/cli.json` handles tool execution. `--force` is the
broad auto-approve alternative and still respects explicit deny rules.

For resume, use the same working directory and isolated directories, keep the
staged files present, and append `--resume <session-id>`. Re-pass
`--approve-mcps`, `--trust`, `--model`, `--plugin-dir` if used, and `--force` if
the broad mode was deliberately selected.

The recipe is structurally supported and its MCP discovery portion was
**VERIFIED**. A complete model-backed recipe run is **UNVERIFIED** because this
sandbox could neither access Cursor credentials/network nor bind a loopback
mock server.

## Result matrix

| Mechanism | Result in 2026.07.16 |
|---|---|
| Project `.cursor/mcp.json` | **VERIFIED**: discovered, launched, initialized, and listed `lab_nonce`. |
| MCP environment | **VERIFIED**: fixture logged `CURSOR-PROJECT-MARKER`. |
| Actual `lab_nonce` call | **UNVERIFIED**: no `tools/call` or fixture `event:"call"` occurred. |
| Global `~/.cursor/mcp.json` | **VERIFIED**: its two servers were discovered by default. |
| Strict MCP isolation | **VERIFIED** with empty `CURSOR_CONFIG_DIR`; there is no strict-MCP CLI flag. |
| `cursor-agent mcp enable lab` | **VERIFIED** in the temp project and redirected `CURSOR_DATA_DIR`. |
| `--approve-mcps` | Help/source support it for headless chat; a model-backed chat is **UNVERIFIED**. It does not change `mcp list` behavior. |
| `.cursor/cli.json` `Mcp(lab:lab_nonce)` | **SOURCE-VERIFIED** exact allowlist syntax; live call **UNVERIFIED**. |
| `--force` | Help/source show headless auto-approval except explicit denies; live call **UNVERIFIED**. |
| Native `.cursor/skills/**/SKILL.md` | **SOURCE-VERIFIED** discovery; description-triggered use **UNVERIFIED**. |
| `--plugin-dir` skill | Help/source support local plugins and their `skills/`; live activation **UNVERIFIED**. |
| `.cursor/rules/*.mdc` / `AGENTS.md` | **SOURCE-VERIFIED** discovery; response effect **UNVERIFIED**. |
| `stream-json` event schema | **SOURCE-VERIFIED**; no successful live stream was produced. |
| `--resume <id>` | Parser/session ID handling **VERIFIED**; resumed model turn and retained injection **UNVERIFIED**. |

## 1. MCP injection

### Project configuration, process start, and environment

The test project contained the exact MCP file shown in the Recipe, with the log
path changed to `work/cursor/project-allow/mcp-calls.jsonl`. With all mutable
state redirected into `work/cursor`, a fresh project first reported:

```sh
cursor-agent mcp list
```

```text
lab: not loaded (needs approval)
```

After approving only this project server:

```sh
cursor-agent mcp enable lab
cursor-agent mcp list
cursor-agent mcp list-tools lab
```

```text
✓ Enabled and approved MCP server: lab
lab: ready
Tools for lab (1):
- lab_nonce (label)
```

Evidence: [unapproved list](../work/cursor/raw/10-mcp-unapproved.txt),
[enable](../work/cursor/raw/11-mcp-enable.txt),
[ready list](../work/cursor/raw/12-mcp-ready.txt), and
[tool list](../work/cursor/raw/13-mcp-tools.txt).

The fixture log proves the subprocess received its injected environment and
completed the MCP handshake:

```json
{"pid":38785,"event":"start","env_marker":"CURSOR-PROJECT-MARKER"}
{"pid":38785,"event":"rpc","method":"initialize"}
{"pid":38785,"event":"rpc","method":"notifications/initialized"}
{"pid":38785,"event":"rpc","method":"tools/list"}
```

Each `enable`, `list`, or `list-tools` command started a fresh fixture process.
All observed PIDs had exited when checked after the commands.

The approval was persisted only below the redirected data directory:

```json
[
  "lab-d18d1afed7a08dd0"
]
```

at
`work/cursor/data/projects/Volumes-Data-codes-ai-agent-runtime-target-cli-injection-lab-work-cursor-project-allow/mcp-approvals.json`.

### Tool identity and invocation

`mcp list-tools lab` exposes the tool as bare `lab_nonce`. The exact installed
code represents MCP calls with separate `providerIdentifier:"lab"` and
`toolName:"lab_nonce"` fields, and permission rules address it as
`Mcp(lab:lab_nonce)`. A successful live stream was not obtained, so the fully
serialized model/stream name is **UNVERIFIED**; in particular, this report does
not claim a Claude-style `mcp__lab__lab_nonce` name.

The fixture log contains no `tools/call` RPC and no `event:"call"`. Therefore
actual tool invocation and the `NONCE-7F3A-...` result are **UNVERIFIED**. The
positive evidence stops at server start, initialization, tool discovery, and
environment propagation.

### Does the user's global MCP configuration leak?

Yes. The read-only `~/.cursor/mcp.json` had two configured server names,
`mcp-router` and `codegraph`. No secrets were printed. In a temp project with
the default config location, both names were discovered. To prevent their
processes from running during the check, `mcp disable` was used only with a
redirected `CURSOR_DATA_DIR`; this produced a temp-project file containing:

```json
["mcp-router", "codegraph"]
```

and then `mcp list` printed:

```text
mcp-router: disabled
codegraph: disabled
```

SHA-1 hashes of `~/.cursor/mcp.json`, `~/.cursor/cli-config.json`, and
`~/.cursor/agent-cli-state.json` were identical before and after. Evidence:
[global leak and hashes](../work/cursor/raw/20-global-leak.txt).

There is no `--strict-mcp-config` equivalent in help. Pointing
`CURSOR_CONFIG_DIR` at an empty temp directory excluded the global file:

```text
No MCP servers configured (expected in .cursor/mcp.json or ~/.cursor/mcp.json)
```

Evidence: [isolated list](../work/cursor/raw/21-strict-empty.txt). For strict
project isolation, the primary workspace must also be an ephemeral project
without the user's `.cursor/mcp.json`. Isolating `HOME` additionally prevents
user-level skills and rules from leaking.

## 2. Headless tool approval

Server approval and tool-call approval are separate:

- `cursor-agent mcp enable lab` persists one server approval in the redirected
  project data.
- `--approve-mcps` auto-approves all discovered MCP servers during a headless
  chat launch. It did not auto-approve an `mcp list` subcommand in a fresh data
  directory; that command still said `needs approval`.
- `.cursor/cli.json` with `"allow":["Mcp(lab:lab_nonce)"]` is the exact
  least-privilege tool-call rule.
- `--force` selects broad headless auto-approval. Help describes it as “Force
  allow commands unless explicitly denied.”

The installed execution-policy code checks explicit MCP deny/allow patterns and
uses the `providerIdentifier:toolName` pair. The installed headless provider is
an immediate deny provider by default:

```js
requestApproval(_) { return Promise.resolve({approved: false}); }
```

When headless auto-approval is active, the corresponding provider returns
`{approved:true}`. Thus an otherwise unallowed approval-requiring tool call is
rejected rather than left waiting for terminal input; an explicit permission
denial is represented internally as `permissionDenied`. This is
**SOURCE-VERIFIED** but a real denied stream event is **UNVERIFIED** because no
model produced a tool call.

An explicit deny such as this remains the safety backstop even with `--force`:

```json
{
  "permissions": {
    "allow": [],
    "deny": ["Mcp(lab:lab_nonce)"]
  }
}
```

The broad `--force` and narrow allowlist runs both failed before a model turn
with `Failed to reach the Cursor API`, so no live approval result is claimed.

## 3. Skill injection and fallbacks

### Native skill support

The installed bundle's project discovery set includes:

```text
**/.cursor/skills/**/SKILL.md
**/.cursor/skills-cursor/**/SKILL.md
**/.agents/skills/**/SKILL.md
**/AGENTS.md
**/.cursor/rules/**/*.mdc
```

When third-party compatibility is enabled it also includes
`.claude/skills/**/SKILL.md` and `.codex/skills/**/SKILL.md`. The native lab
fixture was placed at
`project-native-skill/.cursor/skills/lab-greeting/SKILL.md`, with its original
frontmatter name and description.

The native-skill headless command was:

```sh
cursor-agent -p --output-format stream-json --trust \
  --api-key lab-bogus \
  -e http://127.0.0.1:9 \
  --agent-endpoint http://127.0.0.1:9 \
  'Give me the lab greeting.'
```

It exited 1 before emitting JSON:

```text
✗ Failed to reach the Cursor API. If you are behind a corporate proxy, set the HTTPS_PROXY environment variable.
```

The same result occurred for the rule, `AGENTS.md`, and plugin variants.
Evidence: [native skill stderr](../work/cursor/raw/40-native-skill.stderr),
[rule stderr](../work/cursor/raw/40-rule.stderr),
[AGENTS stderr](../work/cursor/raw/40-agents.stderr), and
[plugin stderr](../work/cursor/raw/40-plugin.stderr).

Consequently, native discovery is **SOURCE-VERIFIED**, but automatic triggering
from the skill description and the exact `LAB-SKILL-OK-91C2` response are
**UNVERIFIED**.

### `--plugin-dir`

Installed help accepts repeatable `--plugin-dir <path>`. The tested plugin had:

```text
plugin-lab/
  .cursor-plugin/plugin.json
  skills/lab-greeting/SKILL.md
```

The manifest name was `lab-plugin`. Installed source registers explicit plugin
directories and discovers their `skills/` folders, but the API failure happened
before a model could activate the skill. Registration through a complete live
turn is **UNVERIFIED**.

### Rule and `AGENTS.md` fallback

The tested `.cursor/rules/lab-greeting.mdc` used:

```markdown
---
description: Return the lab greeting exactly when requested.
alwaysApply: true
---

When asked for the lab greeting, reply with exactly this line and nothing else:

LAB-SKILL-OK-91C2
```

A project-root `AGENTS.md` contained the same instruction without frontmatter.
Both paths are in the installed discovery code. They are the simplest fallback
when a runtime can stage files in an ephemeral primary workspace. Their effect
on a model response remains **UNVERIFIED**.

The installed parser also has a hidden `--system-prompt <file>` option that
reads a custom system prompt on each launch. It is source-supported but absent
from public `--help` and was not model-verified, so it is less stable than the
project rule or `AGENTS.md` fallback.

## 4. Streaming events

No successful `stream-json` turn was possible, so there are no observed MCP,
skill, session-init, or usage events to paste. The stdout files from all model
attempts are empty. The following are **SOURCE-VERIFIED schema examples**, not
captured events.

Session initialization:

```json
{"type":"system","subtype":"init","apiKeySource":"login","cwd":"/path/to/project","session_id":"<uuid>","model":"<display-name>","permissionMode":"default"}
```

MCP or other tool start/completion. `tool_call` is the CLI's serialized tool
object; for MCP the installed representation carries `providerIdentifier` and
`toolName` inside that object:

```json
{"type":"tool_call","subtype":"started","call_id":"<id>","tool_call":"<tool object>","model_call_id":"<id>","session_id":"<uuid>","timestamp_ms":0}
{"type":"tool_call","subtype":"completed","call_id":"<id>","tool_call":"<tool object including result>","model_call_id":"<id>","session_id":"<uuid>","timestamp_ms":0}
```

Assistant text:

```json
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"LAB-SKILL-OK-91C2"}]},"session_id":"<uuid>"}
```

Final result and usage:

```json
{"type":"result","subtype":"success","duration_ms":0,"duration_api_ms":0,"is_error":false,"result":"<text>","session_id":"<uuid>","request_id":"<id>","usage":{"inputTokens":0,"outputTokens":0,"cacheReadTokens":0,"cacheWriteTokens":0}}
```

There is no dedicated `skill_loaded` event in the installed serializer. A
skill read through a tool would appear as an ordinary `tool_call`; otherwise
only the eventual assistant output is visible. Therefore stream output alone
cannot reliably prove automatic skill selection unless the accompanying tool
payload or persisted state identifies the skill.

## 5. Resume

The isolated command:

```sh
timeout 4 cursor-agent create-chat
```

printed:

```text
d9da02a7-46a3-4f63-b700-f3feb159d072
```

but did not exit before the four-second timeout (exit 124). Evidence:
[create-chat stdout](../work/cursor/raw/30-create-chat.txt) and
[exit code](../work/cursor/raw/30-create-chat.exit).

Passing that ID to `--resume` was accepted by argument/session resolution and
then failed at the Cursor API (exit 1), with no JSON output. Evidence:
[resume stderr](../work/cursor/raw/31-resume-offline.stderr) and
[exit code](../work/cursor/raw/31-resume-offline.exit).

Installed source resolves the resume ID, then rebuilds workspace resources for
the new process. Injection is therefore launch state, not session state:

- project MCP, native skills, rules, and `AGENTS.md` are rediscovered only if
  the same workspace/files are still present;
- `--plugin-dir` must be passed again;
- `CURSOR_CONFIG_DIR`, `CURSOR_DATA_DIR`, and isolated `HOME` must be reused;
- `--approve-mcps`, `--force`, `--trust`, and the chosen model should be passed
  again as needed;
- a prior `mcp enable` approval survives only when the same
  `CURSOR_DATA_DIR` is reused.

A successful second model turn preserving conversation history is
**UNVERIFIED**.

## 6. Isolation and cleanup

The normal credential path failed in this sandbox:

```text
ERROR: SecItemCopyMatching failed -50
exit=139
```

With `AGENT_CLI_CREDENTIAL_STORE=file` and isolated config/data directories,
`cursor-agent status` cleanly returned `Not logged in`; `--list-models` then
reported authentication required. Evidence: [keychain status](../work/cursor/raw/22-status-keychain.txt),
[isolated status](../work/cursor/raw/23-status-isolated.txt), and
[isolated model list](../work/cursor/raw/24-models-isolated.txt).

Attempts to use a deterministic local model endpoint were also blocked because
the sandbox rejected `bind(127.0.0.1, 0)` with
`PermissionError: [Errno 1] Operation not permitted`. The installed hidden
`--authless` switch rejected the production executable with
`--authless can only be used with agent-cli-local`. These constraints explain
why model-dependent checks are unverified rather than failed product behavior.

Observed writes under the isolated work directory were:

- `CURSOR_CONFIG_DIR/cli-config.json`;
- `CURSOR_DATA_DIR/projects/<project>/mcp-approvals.json` after `mcp enable`;
- `CURSOR_DATA_DIR/projects/<project>/mcp-disabled.json` after temp-project
  `mcp disable`;
- Node compile-cache files below the redirected `NODE_COMPILE_CACHE`.

A successful chat would also use the redirected data directory for project and
session state, but that write path is **UNVERIFIED** here. MCP fixture processes
started by `mcp` subcommands were all gone after each command. A successful
chat-lifetime subprocess was not observed.

## Gotchas

- `.cursor/mcp.json` is project-root configuration, not an arbitrary-cwd file;
  use a distinct Git root for a nested temporary project.
- `--approve-mcps` approves every discovered MCP server. Pair it with strict
  config/home isolation before using it with runtime-owned servers.
- `--approve-mcps` applies to chat startup, not `cursor-agent mcp list`.
- `mcp enable` is persistent state, even when safely redirected. Prefer the
  per-launch flag when cleanup simplicity matters.
- `--force` is much broader than MCP approval and is unsuitable for the narrow
  adapter recipe.
- `CURSOR_CONFIG_DIR` is the effective strict-global-MCP control, but profile
  isolation loses the tested login. Supply a scoped API/auth token instead of
  copying or editing `~/.cursor`.
- Isolate `HOME` as well as Cursor config/data if user skills, rules, and other
  home-scoped prompt inputs must be excluded.
- There is no successful model event in this experiment: do not treat schema
  inspection, `tools/list`, or the presence of a skill file as proof of a tool
  call or description-triggered skill use.

## Addendum — orchestrator

`cursor-agent status` under the real profile also reports `Not logged in`; live verification needs `cursor-agent login` or `CURSOR_API_KEY`. All model-backed rows remain UNVERIFIED.
