# Gemini CLI 0.58 injection lab

Tested on 2026-09-28 with `gemini 0.58.0` at
`~/.nvm/versions/node/v24.19.0/bin/gemini`.

## Recipe

The cleanest adapter recipe is a fresh per-session directory plus a fresh
Gemini-specific home. This keeps settings, extension metadata, project registry,
sessions, and history out of the user's `~/.gemini`.

Assume:

```sh
RUN=/absolute/path/to/session
PROJECT="$RUN/project"
PROFILE="$RUN/profile"
```

Create `$PROFILE/.gemini/settings.json`:

```json
{
  "security": {
    "auth": { "selectedType": "gemini-api-key" },
    "folderTrust": { "enabled": false }
  },
  "privacy": { "usageStatisticsEnabled": false }
}
```

Create `$PROJECT/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "lab": {
      "command": "python3",
      "args": ["/absolute/path/to/lab_mcp.py"],
      "env": {
        "LAB_MCP_LOG": "$LAB_MCP_LOG",
        "LAB_ENV_MARKER": "$LAB_ENV_MARKER"
      },
      "trust": false
    }
  },
  "skills": { "enabled": true }
}
```

Put native skills at
`$PROJECT/.gemini/skills/<skill-name>/SKILL.md`. A project `GEMINI.md` is loaded
as always-on context. Additional context roots can be added with
`--include-directories`.

Because `--allowed-mcp-server-names lab` both filters to `lab` and grants a
server-wide allow rule, use a higher-priority policy when only selected tools
from that server should run. The following was tested as `$RUN/policy.toml`:

```toml
[[rule]]
mcpName = "lab"
toolName = "*"
decision = "deny"
priority = 400
denyMessage = "Only selected runtime tools are allowed"

[[rule]]
mcpName = "lab"
toolName = "lab_nonce"
decision = "allow"
priority = 500

[[rule]]
toolName = "activate_skill"
decision = "allow"
priority = 500
```

Run from `$PROJECT`:

```sh
env \
  GEMINI_CLI_HOME="$PROFILE" \
  GEMINI_API_KEY="$GEMINI_API_KEY" \
  LAB_MCP_LOG="$RUN/mcp.jsonl" \
  LAB_ENV_MARKER="per-turn-marker" \
  timeout 180 \
  gemini \
    -m gemini-2.5-flash \
    -p "$PROMPT" \
    --output-format stream-json \
    --skip-trust \
    --approval-mode default \
    --allowed-mcp-server-names lab \
    --policy "$RUN/policy.toml"
```

Important recipe details:

- `GEMINI_CLI_HOME` is a base directory; the CLI uses
  `$GEMINI_CLI_HOME/.gemini/...` for its files.
- `--allowed-mcp-server-names lab` is the strict server-selection mechanism. It
  prevented an isolated user-scope `user-decoy` server from even starting.
- MCP model-facing names are `mcp_{serverName}_{toolName}`; this experiment saw
  `mcp_lab_lab_nonce`.
- Headless skill activation needs an allow rule for `activate_skill`.
- `--allowed-tools mcp_lab_lab_nonce,activate_skill` also works, but 0.58 prints
  that it is deprecated; `--policy` is the forward-looking choice.
- `--approval-mode yolo` and `--yolo` approve everything. They are unsuitable
  for least privilege but both were verified.
- If an existing OAuth login must be reused, omitting `GEMINI_CLI_HOME` lets the
  CLI read it, while project settings plus `--allowed-mcp-server-names` still
  avoid changing the user's MCP configuration. The tradeoff is that normal
  Gemini session/history files are then written under the user's profile.
- `GEMINI_CLI_SYSTEM_SETTINGS_PATH=/absolute/settings.json` is another verified
  per-run settings injection route. It has system/admin semantics, so the
  project file is less surprising for ordinary runtime injection.

### Extension alternative

An extension directory may contain `gemini-extension.json`, `GEMINI.md`,
`skills/`, `commands/`, and MCP definitions. In 0.58, `--extensions` accepts
installed/linked extension names, not an arbitrary directory path. Stage it in
the isolated profile first:

```sh
env GEMINI_CLI_HOME="$PROFILE" \
  gemini extensions link "$RUN/lab-extension" --consent

env GEMINI_CLI_HOME="$PROFILE" GEMINI_API_KEY="$GEMINI_API_KEY" \
  gemini -m gemini-2.5-flash -p "$PROMPT" \
    --output-format stream-json --skip-trust \
    --extensions lab-extension \
    --allowed-mcp-server-names ext-lab \
    --policy "$RUN/policy.toml"
```

The linked extension is represented by metadata beneath
`$PROFILE/.gemini/extensions/`; linking is therefore safe only when that profile
is isolated or when the user explicitly wants a persistent install.

## Test basis and hosted-model limitation

No `GEMINI_API_KEY` or `GOOGLE_API_KEY` was present. A disposable copy of the
existing OAuth files was tried inside the lab home, never in the real profile.
The debug run failed before a model request because sandbox DNS was blocked:

```text
Cached credentials are not valid: request to https://oauth2.googleapis.com/token failed,
reason: getaddrinfo ENOTFOUND oauth2.googleapis.com
...
EXIT=41
```

Evidence: `work/gemini/logs/02-baseline-auth-debug.txt`.

To continue testing the actual CLI pipeline, all successful headless runs used
0.58's hidden test flag `--fake-responses-non-strict` with deterministic model
responses. Gemini CLI itself still performed settings/extension/skill discovery,
policy filtering, MCP process startup, MCP RPC, streamed event generation,
session persistence, and resume. The fake flag is a lab aid and is not part of
the production recipe.

Therefore:

- CLI injection, filtering, approval, event, persistence, and cleanup findings
  below are VERIFIED.
- Whether Google's hosted flash model spontaneously chooses the skill or MCP
  tool from its description is **UNVERIFIED**. The captured request proves that
  the descriptions and tool declarations reach the model, but the lab's model
  choice was deterministic.
- Hosted-model token accounting and latency are **UNVERIFIED**. The event schema
  is verified, but the sample counts came from fake responses.

The temporary OAuth/account copies were deleted after the test.

## 1. MCP injection and strict server isolation

### Project-scoped settings work

From `work/gemini/project`, with the MCP definition only in
`.gemini/settings.json`:

```text
Configured MCP servers:

✓ lab: python3 .../fixtures/lab_mcp.py (stdio) - Connected
```

Evidence: `work/gemini/logs/05-mcp-list.txt` and
`work/gemini/logs/project-list-mcp.jsonl`.

The explicit environment mapping was expanded and delivered to the child:

```json
{"pid":93082,"event":"start","env_marker":"PROJECT-LIST-MARKER"}
```

### `--allowed-mcp-server-names` is strict

The isolated user settings contained a second server named `user-decoy`. The
workspace contained `lab`. This invocation selected only `lab`:

```sh
gemini -m gemini-2.5-flash -p 'Call the lab nonce tool ...' \
  --output-format stream-json --skip-trust \
  --allowed-mcp-server-names lab \
  --fake-responses-non-strict fakes/10-mcp-allowed.jsonl
```

Observed stream:

```json
{"type":"tool_use","tool_name":"mcp_lab_lab_nonce","parameters":{"label":"allowed"}}
{"type":"tool_result","status":"success","output":"NONCE-7F3A-allowed"}
```

Observed MCP log:

```json
{"pid":93854,"event":"start","env_marker":"STRICT-PROJECT-MARKER"}
{"pid":93854,"event":"call","tool":"lab_nonce","label":"allowed"}
```

`work/gemini/logs/user-decoy-mcp.jsonl` remained absent. Evidence:
`work/gemini/logs/06-mcp-strict-allowed.jsonl` and
`work/gemini/logs/strict-lab-mcp.jsonl`.

This also shows that `--allowed-mcp-server-names` is not just a discovery
filter: the call succeeded under default approval without a separate
`--allowed-tools`, so the selected server received a server-wide allow rule.

### Settings-path environment overrides work

With an otherwise empty isolated profile and bare cwd:

```sh
GEMINI_CLI_SYSTEM_SETTINGS_PATH=work/gemini/system-settings.json \
  gemini mcp list
```

produced:

```text
✓ system-lab: python3 .../fixtures/lab_mcp.py (stdio) - Connected
```

A headless call then exposed `mcp_system-lab_lab_nonce` and returned
`NONCE-7F3A-system`. Evidence:
`work/gemini/logs/19-system-settings-mcp-list.txt` and
`work/gemini/logs/21-system-settings-mcp-call.jsonl`.

Setting only `GEMINI_CLI_HOME=work/gemini/env-home` selected
`work/gemini/env-home/.gemini/settings.json` and exposed `home-lab`; its child
logged `GEMINI-CLI-HOME-MARKER`. Evidence:
`work/gemini/logs/20-gemini-cli-home-mcp-list.txt` and
`work/gemini/logs/home-mcp.jsonl`.

## 2. Headless approval behavior

### Unallowed call: denied immediately, no hang

With `trust:false`, `--approval-mode default`, and no allow rule, the MCP server
started and answered `tools/list`, but the tool was filtered out of the active
registry. A forced call produced:

```json
{"type":"tool_result","status":"error","output":"Tool \"mcp_lab_lab_nonce\" not found. ...","error":{"type":"tool_not_registered"}}
```

There was no `tools/call` entry in the fixture log, and the process did not
wait for interactive input. Evidence:
`work/gemini/logs/07-mcp-default-denied.jsonl` and
`work/gemini/logs/default-denied-mcp.jsonl`.

In a normal model request the denied tool is absent from the declarations, so a
hosted model ordinarily cannot select it. The fake response deliberately called
the absent name to reveal the failure mode.

### Deprecated exact allowlist works

Adding:

```text
--allowed-tools mcp_lab_lab_nonce
```

made the same call succeed and generated a real fixture `tools/call`. The CLI
also printed:

```text
Warning: --allowed-tools cli argument and tools.allowed in settings.json are
deprecated and will be removed in 1.0: Migrate to Policy Engine
```

Evidence: `work/gemini/logs/08-mcp-exact-allowed.jsonl` and
`work/gemini/logs/exact-allowed-mcp.jsonl`.

### Policy Engine is the tested least-privilege route

A policy allowing only `lab.lab_nonce` was tested with two configured servers.
The `lab` call succeeded while `other.lab_nonce` was not registered, and only
the `lab` fixture received `tools/call`:

```json
{"type":"tool_result","status":"success","output":"NONCE-7F3A-policy-lab"}
{"type":"tool_result","status":"error","error":{"type":"tool_not_registered"}}
```

Evidence: `work/gemini/logs/28-policy-only-one-tool.jsonl`,
`work/gemini/logs/policy-only-lab.jsonl`, and
`work/gemini/logs/policy-other.jsonl`.

The stricter recipe above was also tested against one selected server exposing
two tools. With `--allowed-mcp-server-names lab`, the wildcard deny at priority
400, and the exact allow at 500, `mcp_lab_lab_nonce` succeeded and
`mcp_lab_other_nonce` was unregistered. The MCP process logged only the
`lab_nonce` call. Evidence:
`work/gemini/logs/33-strict-server-exact-tool-fixed.jsonl` and
`work/gemini/logs/strict-exact-mcp-2.jsonl`.

0.58 gotcha: a rule containing only `mcpName = "lab"` was rejected because
`toolName` was missing. `toolName = "*"` was required for the server-wide deny.
The rejected run is preserved in
`work/gemini/logs/32-strict-server-exact-tool.jsonl`.

### Allow-all modes work

Both `--approval-mode yolo` and `--yolo`, without `--allowed-tools`, executed
the MCP call and printed `YOLO mode is enabled. All tool calls will be
automatically approved.` Evidence:
`work/gemini/logs/09-mcp-approval-yolo.jsonl` and
`work/gemini/logs/10-mcp-short-yolo.jsonl`.

## 3. Skills, extensions, and context injection

### Native skills exist in 0.58

The CLI exposes `gemini skills list/install/link/enable/disable/uninstall`.
Placing the fixture directly at
`.gemini/skills/lab-greeting/SKILL.md` yielded:

```text
Discovered Agent Skills:

lab-greeting [Enabled]
  Description: Use when the user asks for the "lab greeting". Produces the exact verification phrase.
  Location: .../project/.gemini/skills/lab-greeting/SKILL.md
```

Evidence: `work/gemini/logs/04-skills-list.txt`.

The captured model request contained the actual discovery metadata:

```text
<name>lab-greeting</name>
<description>Use when the user asks for the "lab greeting". Produces the exact verification phrase.</description>
```

Evidence: `work/gemini/logs/12-request-dump-include-default.txt`.

With an allow rule for `activate_skill`, activation streamed as:

```json
{"type":"tool_use","tool_name":"activate_skill","parameters":{"name":"lab-greeting"}}
{"type":"tool_result","status":"success","output":"Skill **lab-greeting** activated. Resources loaded from `.../.gemini/skills/lab-greeting`..."}
{"type":"message","role":"assistant","content":"LAB-SKILL-OK-91C2","delta":true}
```

The saved session contains the full body inside
`<activated_skill name="lab-greeting"><instructions>...LAB-SKILL-OK-91C2...`.
Evidence: `work/gemini/logs/11-native-skill-activate.jsonl` and the corresponding
session under `work/gemini/clean-home/.gemini/tmp/project/chats/`.

Without the allow rule, a forced headless activation returned
`tool_not_registered`; it did not prompt or hang. Evidence:
`work/gemini/logs/29-skill-default-headless.jsonl`.

The description is demonstrably supplied to the model, and activation works.
Whether the hosted flash model triggers it from the phrase “lab greeting” is
**UNVERIFIED** because hosted access was blocked.

### `GEMINI.md` and `--include-directories` work together

This command included an extra directory:

```text
--include-directories work/gemini/include-context
```

The recorded request contained both:

```text
--- Context from: .../include-context/GEMINI.md ---
... LAB-CONTEXT-INCLUDE-6A27 ...

--- Context from: .../project/GEMINI.md ---
... LAB-CONTEXT-PROJECT-4D11 ...
```

This worked without setting `context.loadMemoryFromIncludeDirectories`; that
setting was absent in the tested project. Evidence:
`work/gemini/logs/12-request-dump-include-default.txt`.

Thus `GEMINI.md` is the simple always-on fallback when on-demand skill
activation is unnecessary. Hosted-model compliance with those instructions is
**UNVERIFIED**, but their presence in the request is verified.

### Extensions are functional but need installation/link staging

Both constructed extension directories passed `gemini extensions validate`.
Linking with `--consent` reported:

```text
Extension "lab-extension" linked successfully and enabled.
Extension "decoy-extension" linked successfully and enabled.
```

`gemini extensions list` showed the lab extension's context file, `ext-lab` MCP
server, and `lab-extension-greeting` skill. Evidence:
`work/gemini/logs/13-extension-link-lab.txt` and
`work/gemini/logs/14-extension-link-decoy.txt`.

Per-run selection was strict. With `--extensions lab-extension`, the captured
request had these counts:

```text
LAB-EXT-CONTEXT-83E5 => present
lab-extension-greeting => present
mcp_ext-lab_lab_nonce => present
DECOY-EXT-CONTEXT-177C => absent
decoy-extension-skill => absent
```

Evidence: `work/gemini/logs/15-extension-filter-request.txt`.

The extension command `/lab-marker` expanded before the model call to:

```text
Reply exactly EXT-COMMAND-OK-581A.
```

Evidence: `work/gemini/logs/16-extension-command-request.txt`.

The extension skill activated successfully and returned
`LAB-EXT-SKILL-OK-63B9`; the extension MCP tool streamed as
`mcp_ext-lab_lab_nonce` and returned `NONCE-7F3A-extension`. Evidence:
`work/gemini/logs/17-extension-skill-activate.jsonl` and
`work/gemini/logs/18-extension-mcp-call.jsonl`.

## 4. Streaming event shapes

`--output-format stream-json` produced newline-delimited JSON. Representative
events from the verified MCP run follow.

Session ID and requested model:

```json
{"type":"init","session_id":"85ebe5c6-36e2-4515-b126-a27e88e6a812","model":"gemini-2.5-flash"}
```

MCP invocation and result:

```json
{"type":"tool_use","tool_name":"mcp_lab_lab_nonce","tool_id":"mcp_lab_lab_nonce__mcp_lab_lab_nonce_1790631309953_0","parameters":{"label":"allowed"}}
{"type":"tool_result","tool_id":"mcp_lab_lab_nonce__mcp_lab_lab_nonce_1790631309953_0","status":"success","output":"NONCE-7F3A-allowed"}
```

Skill load is represented as ordinary tool events; there is no separate
`skill_load` event:

```json
{"type":"tool_use","tool_name":"activate_skill","parameters":{"name":"lab-greeting"}}
{"type":"tool_result","status":"success","output":"Skill **lab-greeting** activated. Resources loaded from `...`"}
```

Final usage/result event:

```json
{"type":"result","status":"success","stats":{"total_tokens":58,"input_tokens":50,"output_tokens":8,"cached":0,"duration_ms":24,"tool_calls":1,"models":{"gemini-3.5-flash":{"total_tokens":58,"input_tokens":50,"output_tokens":8,"cached":0,"input":50}}}}
```

The fake-response harness caused the result's model bucket to say
`gemini-3.5-flash` while `init.model` reflected the requested
`gemini-2.5-flash`; do not treat that model-bucket value as a hosted-model
finding.

Evidence: `work/gemini/logs/06-mcp-strict-allowed.jsonl` and
`work/gemini/logs/11-native-skill-activate.jsonl`.

## 5. Resume

A first turn used a manual session ID and selected only `lab-extension`:

```text
--session-id resume-lab-58
--extensions lab-extension
--allowed-mcp-server-names ext-lab
```

It successfully activated `lab-extension-greeting`, called
`mcp_ext-lab_lab_nonce`, and returned `NONCE-7F3A-resume-first`. Evidence:
`work/gemini/logs/22-resume-first.jsonl`.

The next invocation used the same cwd and:

```text
--resume resume-lab-58 --extensions decoy-extension
```

The stream kept `session_id: "resume-lab-58"`, but current capabilities were
rebuilt from the new invocation:

```json
{"type":"tool_result","status":"error","error":{"type":"invalid_tool_params","message":"params/name must be equal to one of the allowed values"}}
{"type":"tool_result","status":"error","error":{"type":"tool_not_registered","message":"Tool \"mcp_ext-lab_lab_nonce\" not found. ..."}}
```

Evidence: `work/gemini/logs/23-resume-without-injection.jsonl`.

Re-running `--resume resume-lab-58` with `--extensions lab-extension`, the
skill allow rule, and the MCP server allow flag restored both successful calls.
Evidence: `work/gemini/logs/24-resume-restored.jsonl`.

Conclusion: resume preserves conversation/history, including the prior
`<activated_skill>` payload in the session JSON, but it does not preserve the
live extension selection, tool registry, server process, or approval flags.
Those must be supplied again on every resumed CLI process. Project-scoped files
that remain on disk are rediscovered automatically, but per-run flags still need
to be repeated.

## 6. Isolation, files written, and MCP lifetime

### Files written

Within the isolated Gemini homes, 0.58 wrote:

- `.gemini/projects.json` and `.project_root` registry markers;
- `.gemini/history/<project>/`;
- `.gemini/tmp/<project>/chats/session-*.jsonl`;
- `.gemini/installation_id`;
- extension metadata such as `extension_integrity.json`,
  `extension-enablement.json`, and `.gemini-extension-install.json`;
- `gemini-credentials.json` in the isolated API-key test profile;
- several `projects.json.<uuid>.tmp` files after early help/auth failures.

The complete path/size inventory is in
`work/gemini/logs/31-isolated-artifacts.txt`.

`gemini skills list` also initialized configured MCP servers before printing
the skills. A probe intended only to enumerate skills can therefore start MCP
processes. Evidence: `work/gemini/logs/04-skills-list.txt` and the first process
in `work/gemini/logs/project-list-mcp.jsonl`.

`gemini --list-sessions` attempted to generate missing session summaries with a
model call and retried when the network was unavailable; it did not behave as a
purely local listing in this test. Evidence:
`work/gemini/logs/25-list-sessions.txt`.

### MCP process lifetime

A copied fixture with signal/exit logging showed that Gemini terminated the
stdio server immediately after the headless result:

```json
{"pid":85655,"event":"call","tool":"lab_nonce","label":"lifetime"}
{"pid":85655,"event":"signal","signal":15}
{"pid":85655,"event":"exit"}
```

Evidence: `work/gemini/logs/26-lifetime-stream.jsonl` and
`work/gemini/logs/lifetime-mcp.jsonl`.

### Real profile remained untouched

The selected real-profile files had the same size and modification time before
and after the experiment:

```text
~/.gemini/settings.json       307 bytes  2026-07-11T18:52:26-0400
~/.gemini/oauth_creds.json   1811 bytes  2026-08-25T21:52:40-0400
~/.gemini/projects.json      1541 bytes  2026-09-10T16:11:21-0400
~/.gemini/google_accounts.json 87 bytes  2026-06-18T17:05:27-0400
```

Evidence: the initial inventory in the experiment transcript and
`work/gemini/logs/30-real-profile-final-stat.txt`. The disposable credential
copies under `work/gemini/home/.gemini/` were removed.

## Gotchas

- Hosted flash execution and semantic tool/skill selection remain
  **UNVERIFIED** because the sandbox could not resolve Google's OAuth endpoint.
- `--allowed-mcp-server-names` is simultaneously a strict discovery filter and
  a server-wide auto-approval rule. Add a higher-priority wildcard deny plus
  exact allows when the selected server exports more than the desired tools.
- In the tested 0.58 policy parser, a server-wide MCP rule needed
  `toolName = "*"`; omitting `toolName` caused a schema error and the invalid
  policy was ignored.
- `--allowed-tools` works but is explicitly deprecated in 0.58.
- Default headless denial removes a tool from the registry. Forced calls report
  `tool_not_registered`, not an interactive denial event, and do not hang.
- A turn can still end with process exit 0 and `result.status: "success"` after
  an individual `tool_result.status: "error"` if the model continues. Adapters
  must inspect tool events, not only exit status or the final result.
- `--extensions` selects linked/installed names. There is no observed CLI flag
  that directly mounts an arbitrary extension directory for one invocation.
- Resume reloads history but rebuilds active tools, skills, extensions, MCP
  processes, and policies from current files/flags.
- Warnings such as terminal capability messages and policy warnings were on
  stderr. The lab merged stderr with stdout for raw transcripts; an adapter
  should keep stderr separate if it requires a pure JSONL stdout stream.
- The full skill instructions are in the model-facing function response and
  saved session, while the streamed `tool_result.output` is only a concise
  activation/resource summary.

## Addendum — live attempt (orchestrator, real network, user OAuth)

Recipe run from `work/gemini/live/project` with the user's own profile (no `GEMINI_CLI_HOME`).
Result: exit 1 before any model request — `IneligibleTierError: This client is no longer supported
for Gemini Code Assist for individuals`. The user's OAuth tier cannot drive the CLI; a live
verification needs `GEMINI_API_KEY` (or a Vertex project). Hosted-model skill/tool selection remains UNVERIFIED.
Adapter implication: surface this auth failure as a terminal `Failed` with the CLI's message.
