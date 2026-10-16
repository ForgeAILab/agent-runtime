# Codex CLI 0.158 injection experiment

Tested on 2026-09-28 with `codex-cli 0.158.0`. Scratch artifacts and raw transcripts are under `target/cli-injection-lab/work/codex/`.

The central result is positive for configuration injection: MCP command, arguments, environment, tool allowlisting, and per-tool approval policy can all be supplied as one-run `-c` session flags. An isolated `CODEX_HOME` supplies skills from `skills/<name>/SKILL.md`, while `--ignore-user-config` excludes the isolated home's base `config.toml`. The real model request could not cross this environment's network boundary, so model-driven MCP calling, description-triggered skill execution, approval prompting, successful usage events, and the exact model-facing qualified tool name remain **UNVERIFIED**.

## Recipe

### Recommended adapter layout

Use a fresh directory for each adapter session:

```text
target/cli-injection-lab/work/codex/home/
  auth.json                         # temporary; copied only if account auth is needed
  skills/
    lab-greeting/
      SKILL.md                      # copied verbatim from the fixture
```

Files used in this experiment:

```text
MCP server: target/cli-injection-lab/fixtures/lab_mcp.py
Skill source: target/cli-injection-lab/fixtures/skills/lab-greeting/SKILL.md
Isolated home: target/cli-injection-lab/work/codex/home
Turn cwd: target/cli-injection-lab/work/codex/session
MCP evidence log: target/cli-injection-lab/work/codex/mcp-allow.log
JSONL output: target/cli-injection-lab/work/codex/mcp-allow.jsonl
```

The setup used was equivalent to:

```sh
mkdir -p target/cli-injection-lab/work/codex/home/skills
mkdir -p target/cli-injection-lab/work/codex/session
cp -R target/cli-injection-lab/fixtures/skills/lab-greeting \
  target/cli-injection-lab/work/codex/home/skills/

# Needed here because OPENAI_API_KEY was unset. This was the only file copied
# from ~/.codex, and all temporary auth.json copies were deleted after testing.
cp ~/.codex/auth.json \
  target/cli-injection-lab/work/codex/home/auth.json
```

### Exact one-turn command

Shared options such as `--sandbox` and `--ask-for-approval` must be placed **before** `exec` in this build:

```sh
env CODEX_HOME=target/cli-injection-lab/work/codex/home \
timeout 180 codex \
  --model gpt-6-luna \
  -c 'model_reasoning_effort="low"' \
  --sandbox read-only \
  --ask-for-approval never \
  --disable apps \
  -C target/cli-injection-lab/work/codex/session \
  -c 'mcp_servers.lab.command="python3"' \
  -c 'mcp_servers.lab.args=["target/cli-injection-lab/fixtures/lab_mcp.py"]' \
  -c 'mcp_servers.lab.env={LAB_MCP_LOG="target/cli-injection-lab/work/codex/mcp-allow.log",LAB_ENV_MARKER="ENV-OK-C0D3"}' \
  -c 'mcp_servers.lab.enabled_tools=["lab_nonce"]' \
  -c 'mcp_servers.lab.default_tools_approval_mode="prompt"' \
  -c 'mcp_servers.lab.tools.lab_nonce.approval_mode="approve"' \
  exec \
  --json \
  --strict-config \
  --ignore-user-config \
  'Call the lab nonce tool with label adapter, then return only its result.' \
  > target/cli-injection-lab/work/codex/mcp-allow.jsonl \
  2> target/cli-injection-lab/work/codex/mcp-allow.stderr.txt
```

This policy means:

- only `lab_nonce` is exposed from server `lab`;
- that tool has MCP approval mode `approve`;
- any other MCP tool on `lab` falls back to `prompt` if exposed later;
- global approval policy is `never`, so the headless process will not wait for human input;
- shell commands are read-only sandboxed;
- connected Apps are disabled for the run;
- `$CODEX_HOME/config.toml` is ignored, while auth still comes from that isolated home.

For a non-resumable turn, add `--ephemeral`. Do not add it when the adapter needs `exec resume`.

After the session, delete the temporary isolated home or at minimum its copied `auth.json`. The copies created during this experiment were removed; `find target/cli-injection-lab/work/codex -name auth.json` returned no paths.

### Exact resume form

Re-pass all MCP session flags and keep the same isolated home/skills directory:

```sh
env CODEX_HOME=target/cli-injection-lab/work/codex/home \
timeout 180 codex \
  --model gpt-6-luna \
  -c 'model_reasoning_effort="low"' \
  --sandbox read-only \
  --ask-for-approval never \
  --disable apps \
  -C target/cli-injection-lab/work/codex/session \
  -c 'mcp_servers.lab.command="python3"' \
  -c 'mcp_servers.lab.args=["target/cli-injection-lab/fixtures/lab_mcp.py"]' \
  -c 'mcp_servers.lab.env={LAB_MCP_LOG="target/cli-injection-lab/work/codex/mcp-resume-repassed.log",LAB_ENV_MARKER="RESUME-REPASSED"}' \
  -c 'mcp_servers.lab.enabled_tools=["lab_nonce"]' \
  -c 'mcp_servers.lab.default_tools_approval_mode="prompt"' \
  -c 'mcp_servers.lab.tools.lab_nonce.approval_mode="approve"' \
  exec resume \
  --json \
  --strict-config \
  --ignore-user-config \
  01a0e9ec-82b3-7b20-89eb-3119fe68d408 \
  'Call the lab nonce tool with label resumed.'
```

## Test boundary

The installed binary accepted the copied account auth and selected `gpt-6-luna` at low reasoning effort. Every real `codex exec --json` model request then failed during transport setup:

```json
{"type":"thread.started","thread_id":"01a0e9ec-82b3-7b20-89eb-3119fe68d408"}
{"type":"turn.started"}
{"type":"error","message":"Reconnecting... 2/5 (workspace routing discovery failed)"}
{"type":"item.completed","item":{"id":"item_0","type":"error","message":"Falling back from WebSockets to HTTPS transport. workspace routing discovery failed"}}
{"type":"turn.failed","error":{"message":"workspace routing discovery failed"}}
```

`--no-daemon` did not change that result. A local mock Responses endpoint was also unavailable because this outer sandbox disallowed binding a listening socket. Consequently, dry configuration checks and the CLI app-server's direct MCP client RPC were used where noted. A direct app-server MCP RPC proves transport/filter behavior, but it bypasses model selection and MCP approval review; it is not evidence of a model-driven call.

## 1. MCP injection

### One-run configuration: VERIFIED

The exact `-c` flags in the recipe were accepted by both `codex exec` and the app server. A `config/read` RPC reported this effective value:

```json
{
  "mcp_servers": {
    "lab": {
      "command": "python3",
      "args": ["target/cli-injection-lab/fixtures/lab_mcp.py"],
      "env": {
        "LAB_ENV_MARKER": "CONFIG-READ-LIVE",
        "LAB_MCP_LOG": "target/cli-injection-lab/work/codex/mcp-config-read-live.log"
      },
      "environment_id": "local",
      "enabled": true,
      "default_tools_approval_mode": "prompt",
      "enabled_tools": ["lab_nonce"],
      "tools": {"lab_nonce": {"approval_mode": "approve"}}
    }
  }
}
```

Every injected field's origin was `{"type":"sessionFlags"}`. Raw evidence: `app-server-config-read-live.jsonl`. This shows the flags are a per-process layer rather than a mutation of a TOML file.

`codex mcp get lab --json` also returned the injected command, args, env, and enabled tools. That management output omitted approval modes, while `config/read` retained them.

### Server startup and environment: VERIFIED

The real `codex exec --json` process started and initialized the fixture before its model request failed:

```json
{"t":1790630989.478487,"pid":31075,"event":"start","argv":["target/cli-injection-lab/fixtures/lab_mcp.py"],"env_marker":"ENV-OK-C0D3"}
{"t":1790630989.478601,"pid":31075,"event":"rpc","method":"initialize"}
{"t":1790630989.478731,"pid":31075,"event":"rpc","method":"notifications/initialized"}
{"t":1790630989.479187,"pid":31075,"event":"rpc","method":"tools/list"}
```

Thus `command`, `args`, and `env` all work, and `LAB_ENV_MARKER` reaches the MCP child.

### Tool invocation: partially VERIFIED

The CLI app-server MCP client directly called the configured server:

```json
{"id":4,"result":{"content":[{"type":"text","text":"NONCE-7F3A-direct"}]}}
```

The fixture independently logged:

```json
{"t":1790632124.994964,"pid":6647,"event":"rpc","method":"tools/call"}
{"t":1790632124.995072,"pid":6647,"event":"call","tool":"lab_nonce","label":"direct"}
```

Raw evidence: `app-server-direct-call.jsonl` and `mcp-direct-call.log`.

- CLI-to-MCP stdio invocation and result: **VERIFIED**.
- Model choosing and invoking the MCP tool during `codex exec`: **UNVERIFIED** because the model transport failed first.

### Model/stream tool name: UNVERIFIED

The MCP inventory exposes server `lab` and raw tool `lab_nonce`. `codex features list` reports `non_prefixed_mcp_tool_names` as under development and `false`, which suggests a model-facing qualified name such as `mcp__lab__lab_nonce`. No model request or `exec --json` MCP item was captured, so that exact qualified name is **UNVERIFIED**. The app-server RPC represents it as separate fields (`server: "lab"`, `tool: "lab_nonce"`).

### Excluding the user's MCP servers: VERIFIED for user config

An isolated home with a decoy server in `config.toml` was tested twice:

- without `--ignore-user-config`, `userdecoy` started and logged `USER-DECOY`;
- with `--ignore-user-config` plus a session-flag `strictlab`, the decoy log was absent and only `strictlab` started.

Evidence:

```text
decoy_log_present_after_ignore=no
```

An empty isolated home also produced `[]` from `codex mcp list --json` before injection.

This is strict against the user's `$CODEX_HOME/config.toml`. Managed/system config layers can still exist and cannot necessarily be overridden by an adapter. A trusted project `.codex/config.toml` is another layer; use a neutral cwd or an isolated home with no project trust if it must be excluded. `--disable apps` prevents the normal Apps feature from adding its MCP path for the turn.

## 2. Headless tool approval

### Auto-allow only the injected tool

Use all three MCP settings:

```toml
mcp_servers.lab.enabled_tools = ["lab_nonce"]
mcp_servers.lab.default_tools_approval_mode = "prompt"
mcp_servers.lab.tools.lab_nonce.approval_mode = "approve"
```

The effective configuration and its `sessionFlags` origin are **VERIFIED**. `enabled_tools` behavior is also **VERIFIED**: with `enabled_tools=[]`, inventory returned `"tools":{}`, and a direct call failed before reaching the fixture:

```json
{"error":{"code":-32603,"message":"tool 'lab_nonce' is disabled for MCP server 'lab'"},"id":4}
```

The fixture log contained initialization and `tools/list` but no `tools/call`.

For auto-allowing all tools on the injected server, set `default_tools_approval_mode="approve"` and omit `enabled_tools`, or keep a deliberate allowlist if only some tools should be exposed. Do not use `--dangerously-bypass-approvals-and-sandbox` merely to allow MCP; that flag disables both approval checks and sandboxing globally.

### What happens to an unapproved model tool call: UNVERIFIED

The headless recipe uses `--ask-for-approval never`; the persisted turn context confirms:

```json
{"approval_policy":"never","approvals_reviewer":"user","sandbox_policy":{"type":"read-only"},"model":"gpt-6-luna","effort":"low"}
```

CLI help says `never` does not ask and returns execution failures to the model. Official MCP documentation describes `approve`, `prompt`, and other per-tool approval modes. However, the model could not issue a call in this environment, so no denied `exec --json` item and no hang/non-hang observation was captured. The exact headless event for a `prompt`-mode MCP tool is therefore **UNVERIFIED**.

A diagnostic direct `mcpServer/tool/call` succeeded even when the server default was `prompt` and the thread approval policy was `never`. That proves the direct app-server RPC bypasses the model approval path and must not be used to test approval behavior.

### Global flags observed

- `--sandbox read-only|workspace-write|danger-full-access` controls model-generated shell commands. The read-only test still allowed the configured MCP process to start and write its own log; sandbox mode is not an MCP allowlist.
- `--ask-for-approval on-request|never` works when placed before `exec`.
- `--approve-for-me` produced `approval_policy="on-request"`, `approvals_reviewer="auto_review"`, and `sandbox_mode="workspace-write"` in `config/read`. It routes approval requests through automatic review; it is not a tool allowlist and may require model connectivity.
- `--dangerously-bypass-approvals-and-sandbox` is accepted and documented as disabling both protections. Its end-to-end MCP behavior was **UNVERIFIED** because the model request was blocked.
- `--full-auto` is not accepted by this 0.158 binary, before or after `exec`; both forms exited 2 with `unexpected argument '--full-auto'`. This conflicts with current documentation that mentions it as a compatibility path, so adapters should feature-detect the installed binary.

## 3. Skill injection

### Isolated `CODEX_HOME/skills`: discovery VERIFIED

Copying the fixture to:

```text
target/cli-injection-lab/work/codex/home/skills/lab-greeting/SKILL.md
```

made this entry appear in `codex debug prompt-input`:

```text
- lab-greeting: Use when the user asks for the "lab greeting". Produces the exact verification phrase. (file: r0/lab-greeting/SKILL.md)
```

The skill root mapping was:

```text
r0 = target/cli-injection-lab/work/codex/home/skills
```

This is the best per-session adapter mechanism. The skill directory must remain present for every new or resumed process. `--ignore-user-config` only says it suppresses `$CODEX_HOME/config.toml`; it does not disable `$CODEX_HOME/skills` discovery.

### Description trigger and exact response: UNVERIFIED

The description was present in the model prompt catalog, but the actual prompt `Give me the lab greeting` failed at model transport setup. Therefore a description-triggered read of `SKILL.md` and the expected final text `LAB-SKILL-OK-91C2` are **UNVERIFIED**.

The prompt contains skill metadata, not every skill's full instructions. The model is expected to choose a skill and read its file when applicable. No dedicated `skill.loaded` JSON event was found in this test.

### Project-level skill and `AGENTS.md`: prompt injection VERIFIED

Inside the experimental project, both of these were discovered:

```text
project/.codex/skills/lab-greeting/SKILL.md
project/AGENTS.md
```

`codex debug prompt-input` showed the project skill catalog entry and injected the exact `AGENTS.md` body:

```text
# AGENTS.md instructions for .../work/codex/project
When the user asks for the project marker, reply with exactly this line and nothing else:

LAB-AGENTS-OK-42D1
```

Discovery occurred in the prompt-input diagnostic even without project trust. Model compliance is **UNVERIFIED** due to the same transport failure.

Project MCP configuration behaved differently: `project/.codex/config.toml` did not start its server until the isolated home persisted:

```toml
[projects."target/cli-injection-lab/work/codex/project"]
trust_level = "trusted"
```

After that, `codex exec` started `projectlab` and its fixture log contained `PROJECT-ENV-OK`. A one-run `-c projects."<path>".trust_level="trusted"` did not unlock project config, apparently because trust is resolved before session flags. `codex mcp get projectlab` also did not see the project layer even when `exec` did.

### Other skill mechanisms checked

- `codex features list` reports `skill_search` and `skill_mcp_dependency_install` as stable/enabled; `plugins` is also stable/enabled.
- `--add-dir` is documented as adding writable directories, not skill search roots.
- `skills.config=[{path=...,enabled=true}]` did not add an otherwise undiscovered fixture, whether `path` named the directory or `SKILL.md`. It is an enablement override, not a per-run add-directory mechanism.
- In this 0.158 build, `enabled=false` matched and removed the catalog entry only when `path` named the exact `SKILL.md`; the directory path did not remove it. This differs from the current config-reference wording and should be version-tested.
- The isolated home still received built-in `.system` skills, and this host injected another skill root under `~/.mirasim/skills`. Enabling the experimental `skip_host_skill_discovery` feature did not remove that root here. Isolated `CODEX_HOME` is therefore strong config/MCP isolation, not complete skill isolation in this host environment.

## 4. Streaming events

### Session ID: VERIFIED

Observed `codex exec --json` event:

```json
{"type":"thread.started","thread_id":"01a0e9ec-82b3-7b20-89eb-3119fe68d408"}
```

Use `thread_id` as the value passed to `codex exec resume`.

### MCP invocation and result in `exec --json`: UNVERIFIED

No model-driven MCP item was emitted because transport failed before inference. Thus there is no honest `codex exec --json` MCP invocation/result example from this experiment.

The separate app-server diagnostic returned this verified result:

```json
{"id":4,"result":{"content":[{"type":"text","text":"NONCE-7F3A-direct"}]}}
```

The generated 0.158 app-server schema defines `McpServerToolCallParams`/`McpServerToolCallResponse`, and its item representation uses server and tool fields. That protocol is not interchangeable with the simpler `codex exec --json` event stream.

### Skill load: UNVERIFIED

No dedicated skill-load stream event was captured. The only verified evidence is the pre-turn prompt catalog entry shown above. A successful model turn would need to show file/tool activity or the expected response, neither of which was reached.

### Usage: UNVERIFIED

The failed run ended with:

```json
{"type":"turn.failed","error":{"message":"workspace routing discovery failed"}}
```

It never emitted a successful `turn.completed` usage object, so a real usage example is **UNVERIFIED**. The official non-interactive-mode documentation describes `thread.started`, turn/item events, and successful completion/usage, but this report does not substitute a documentation example for observed output.

Raw stream: `work/codex/mcp-allow.jsonl`.

## 5. Resume

MCP session flags must be supplied again: **VERIFIED at process startup**.

The initial MCP log contained four lines (start, initialize, initialized notification, tools/list). Resuming the captured thread without any MCP `-c` flags left the count unchanged:

```text
before: 4
after:  4
```

Resuming the same ID with the flags re-passed created a new MCP process:

```json
{"t":1790631820.396929,"pid":2938,"event":"start","argv":["target/cli-injection-lab/fixtures/lab_mcp.py"],"env_marker":"RESUME-REPASSED"}
{"t":1790631820.397027,"pid":2938,"event":"rpc","method":"initialize"}
{"t":1790631820.397843,"pid":2938,"event":"rpc","method":"tools/list"}
```

Both resume attempts retained the same thread ID. The resumed model turn itself timed out on the blocked network, so a resumed tool call is **UNVERIFIED**.

Skills are external filesystem discovery. Keep the same `CODEX_HOME/skills` or project cwd on resume; the prior turn does not embed a one-run skill directory into the resume command. `--ephemeral` should not be used when resume is required.

## 6. Isolation and cleanup

### Files Codex wrote

Even with an isolated `CODEX_HOME`, Codex 0.158 wrote or materialized:

```text
installation_id
.sandbox_migration
state_5.sqlite{,-shm,-wal}
thread_history_1.sqlite{,-shm,-wal}
queue_1.sqlite{,-shm,-wal}
logs_2.sqlite{,-shm,-wal}
memories_1.sqlite{,-shm,-wal}
goals_1.sqlite{,-shm,-wal}
sessions/YYYY/MM/DD/rollout-*.jsonl
shell_snapshots/*.sh
thread-writer-locks/.coordination.lock
tmp/arg0-*/...
skills/.system/*
```

`--ephemeral` prevented rollout/session persistence for the ephemeral thread, but the home still received initialization/state/temp files and materialized system skills. Help-only and debug commands can also initialize parts of the home.

The global `~/.codex` directory was never edited. Its existing `auth.json` was read and temporarily copied into isolated homes because no API key was present; all copies under `work/codex` were deleted at the end. The original remained `~/.codex/auth.json`, 4194 bytes, mode `0600`.

### MCP process lifetime: VERIFIED for sampled processes

MCP children were started on demand and initialized over stdio. This sandbox rejected `ps` with `operation not permitted`, so each of the nine fixture PIDs recorded across the experiments was checked afterward with `kill -0` (signal 0, which does not mutate the process). All nine returned `not-running`, including the main `exec` PID `31075`, direct-call PIDs `6647`/`7456`, and resume PID `2938`. Evidence: `mcp-pid-liveness.txt`.

This supports process cleanup when the owning `exec`/app-server transport closes. The fixture has no explicit exit event, and PID reuse remains a general caveat for any after-the-fact numeric PID check.

Calling the app-server status/inventory RPC can start an additional MCP process; the direct-call log contains two PIDs because both a thread and a status inventory path initialized the server. An adapter should treat MCP server lifetime as tied to each Codex process/transport and make its runtime-owned MCP server safe for more than one concurrent connection.

## `--profile` experiment

The supported profile file is `$CODEX_HOME/<name>.config.toml`. The experiment created `home/lab.config.toml` with the MCP server and ran:

```sh
env CODEX_HOME=target/cli-injection-lab/work/codex/home \
codex --profile lab mcp get profilelab --json
```

It returned the expected command, args, env, and `enabled_tools`; an `exec` using the profile started the server with `PROFILE-ENV-OK`.

Adding `exec --ignore-user-config` prevented the selected profile server from starting in this build. Therefore:

- profile files work and are a useful isolated-home alternative;
- profiles are file-based rather than purely one-run injection;
- do not combine a required profile with `--ignore-user-config` on 0.158;
- direct `-c` flags plus `--ignore-user-config` are the stronger adapter recipe.

## CLI/version gotchas

1. The binary is exactly `codex-cli 0.158.0`.
2. `codex exec --json` and `codex exec resume --json` are the machine-readable interfaces.
3. Shared `-a/--ask-for-approval` flags parse before `exec`; after `exec`, 0.158 exits 2 even though `exec --help` repeats some shared options.
4. `--full-auto` exits 2 in both positions in the tested binary.
5. `--strict-config` is supported by `exec`/app-server but not every debug subcommand.
6. `--ignore-user-config` suppresses `$CODEX_HOME/config.toml` and, in practice here, an explicitly selected profile; auth still uses the isolated home.
7. Read-only shell sandboxing does not stop a configured MCP process from writing its own log.
8. Direct app-server MCP calls bypass model approval review and are only transport/filter diagnostics.
9. Project config needs persisted trust in the isolated home; session-flag trust was too late in this test.
10. Current host-provided skill roots can remain visible despite an isolated home.
11. The actual model request was blocked by `workspace routing discovery failed`; model-driven claims are marked **UNVERIFIED** throughout.

## Evidence index

All paths below are relative to `target/cli-injection-lab/work/codex/`:

| Evidence | File(s) |
|---|---|
| Version and CLI flags | `version.txt`, `help.txt`, `exec-help.txt`, `exec-resume-help.txt` |
| Feature list | `features-list.txt` |
| Real JSONL failure and MCP startup | `mcp-allow.jsonl`, `mcp-allow.stderr.txt`, `mcp-allow.log` |
| Effective session-flag config/origins | `app-server-config-read-live.jsonl` |
| Direct MCP call and fixture proof | `app-server-direct-call.jsonl`, `mcp-direct-call.log` |
| Allowlist rejection | `app-server-disabled-call.jsonl`, `mcp-disabled-call.log` |
| Approval bypass diagnostic | `app-server-prompt-call.jsonl`, `mcp-prompt-call.log` |
| Isolated-user-config test | `strict-ignore-decoy-check.txt`, `mcp-strictlab.log`, `mcp-userdecoy.log` |
| Profile | `home/lab.config.toml`, `mcp-get-profile.json`, `mcp-profile.log` |
| Skills and AGENTS prompt | `prompt-input-skill-codex-home.json`, `prompt-input-project-no-trust.json`, `project/AGENTS.md` |
| Project config/trust | `project/.codex/config.toml`, `home-project/config.toml`, `mcp-project.log` |
| Resume | `resume-no-injection*.txt`, `resume-no-injection.jsonl`, `resume-repassed.jsonl`, `mcp-resume-repassed.log` |
| MCP child cleanup | `mcp-pid-liveness.txt` |
| Parser quirks | `full-auto-*.txt`, `exec-short-approval-help.txt`, `global-long-approval-help.txt` |
| Generated app-server protocol | `app-server-schema/` |

## Official references consulted

- [Codex configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference)
- [Codex MCP documentation](https://learn.chatgpt.com/docs/extend/mcp)
- [Codex non-interactive mode](https://learn.chatgpt.com/docs/non-interactive-mode)
- [Codex approvals and security](https://learn.chatgpt.com/docs/agent-approvals-security)
- [Codex app-server protocol](https://learn.chatgpt.com/docs/app-server)

Where those pages and the installed 0.158 binary differed, this report records the binary's observed behavior.

## Addendum — live verification (orchestrator, real network)

Recipe as above (isolated CODEX_HOME with temporary auth.json copy, deleted after). Raw: `work/codex/live/out.jsonl`, `work/codex/live/deny.jsonl`.

- Skill: model read `$CODEX_HOME/skills/lab-greeting/SKILL.md` via a `command_execution` (`cat .../skills/.system/../lab-greeting/SKILL.md`) and answered `LAB-SKILL-OK-91C2`. VERIFIED (skill load is a shell read, not a dedicated event).
- MCP: `item.started`/`item.completed` `{"type":"mcp_tool_call","server":"lab","tool":"lab_nonce","arguments":{"label":"live"},"result":{"content":[{"type":"text","text":"NONCE-7F3A-live"}]},"status":"completed"}`; fixture log shows one `call`. VERIFIED.
- Usage: `turn.completed.usage {input_tokens:79446, cached_input_tokens:68608, cache_write_input_tokens:0, output_tokens:261, reasoning_output_tokens:44}`. Note ~79k input tokens of baseline prompt per turn.
- Tool needing approval (`default_tools_approval_mode="prompt"`) under `--ask-for-approval never`: no hang; `mcp_tool_call status:"failed", error.message:"MCP tool call requires approval, but approval policy is never"`; server never received `tools/call`. VERIFIED.
- Gotcha: stream contains non-fatal `item.completed {type:"error"}` warnings (e.g. hook-trust notice); only `turn.failed`/`error` top-level events are terminal.
- Gotcha: isolated CODEX_HOME accumulates sqlite state (sessions, thread_history, memories) — delete per session or keep for resume.
