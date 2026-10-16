# CLI injection lab — shared brief

Repo: /Volumes/Data/codes/ai/agent-runtime (Rust). It has an `ExternalAgentBackend`
(crates/agent-runtime/src/agent/external.rs) that runs one whole turn inside an installed
coding-agent CLI and normalizes its streamed events. We want the runtime to INJECT, per
session/turn and WITHOUT mutating the user's global CLI config:
  (a) skills (SKILL.md folders), (b) MCP servers, (c) runtime-owned tools (plan: expose them
  to the CLI as an MCP server the runtime hosts, so (c) reduces to (b) + approval routing).

Fixtures (do not modify; copy if you need variants):
- MCP server (stdio, python3, no deps): target/cli-injection-lab/fixtures/lab_mcp.py
  tool `lab_nonce(label)` -> "NONCE-7F3A-<label>". Every process start/RPC/call is appended as
  JSON lines to $LAB_MCP_LOG (default fixtures/mcp_calls.log). Use a per-experiment
  LAB_MCP_LOG path so you can PROVE invocation. It also logs env var LAB_ENV_MARKER (tests
  whether env passes through to MCP servers).
- Skill: target/cli-injection-lab/fixtures/skills/lab-greeting/SKILL.md
  asking for "the lab greeting" should produce exactly `LAB-SKILL-OK-91C2`.

Rules:
- Run the CLI headless/non-interactive with machine-readable streaming output (JSON/JSONL).
- Use cheap/fast models and tiny prompts; cap each run with a timeout (e.g. `timeout 180`).
- NEVER edit the user's global config (~/.claude, ~/.codex, ~/.gemini, ~/.cursor, etc.).
  Prefer flags / env / temporary dirs under target/cli-injection-lab/work/<cli>/.
  If the only way requires an isolated HOME/config dir, test that and note what auth breaks.
- Work dir for scratch: target/cli-injection-lab/work/<cli>/. Save raw transcripts there.
- If network/model calls are blocked by your sandbox, say so explicitly and still document
  mechanisms from `--help`, docs, and dry runs.

Questions to answer with EVIDENCE (commands run + trimmed output):
1. MCP injection: exact flag/env/file to add an MCP server for one run only. Can user's own
   MCP servers be excluded (strict mode)? Does the tool get invoked (check the log)? Tool
   name as the model/stream sees it (e.g. mcp__lab__lab_nonce)? Does LAB_ENV_MARKER pass?
2. Tool approval headlessly: how to auto-allow ONLY the injected tool (allowlist) vs all.
   What happens to an un-allowed tool call in headless mode (denied event? hang?).
3. Skill injection: exact way to make lab-greeting discoverable for one run only (plugin dir,
   add-dir, skills dir, CODEX_HOME/skills, extensions, rules...). Does it trigger by
   description? Fallback: system-prompt append / AGENTS.md-style file in cwd.
4. Streaming events: what JSON events show MCP tool invocation + result, skill load, session
   id, usage. Paste one example of each.
5. Resume: does `--resume <id>` (or equivalent) keep injected MCP/skills, or must they be
   re-passed each turn?
6. Isolation/cleanup: files the CLI writes (session logs etc.), MCP process lifetime.
Write the final report to target/cli-injection-lab/reports/<cli>.md with a top
"Recipe" section (exact argv/env/files for the adapter), then the evidence per question,
then gotchas. Keep it factual; mark anything unverified as UNVERIFIED.
