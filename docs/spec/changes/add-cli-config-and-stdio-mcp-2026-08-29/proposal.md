---
created_at: 2026-08-29T21:16:28Z
updated_at: 2026-08-29T21:46:53Z
---

## Why

The new `agent-runtime` executable can run one real turn, but every invocation
must repeat provider/model flags and the host cannot install any of the
framework's existing MCP tool adapters. Smith proves both capabilities are
useful, but importing Smith's configuration model would couple a neutral
reference host to one product's prompts, memory, skills, credential rotation,
and session policy.

The right first step is a small CLI-owned configuration contract and trusted
local MCP composition. It should exercise the existing `agent-runtime-mcp`
mechanism without silently executing commands from a project file, treating
server-authored annotations as authority, inheriting the host environment, or
introducing remote endpoint/OAuth policy before the CLI has the necessary
egress hardening.

## What Changes

- Add a versioned, strict TOML configuration file loaded only through an
  explicit `--config <PATH>`. The first schema stores defaults for the run
  settings the CLI already supports and local stdio MCP server definitions.
  Command-line values override file values; no user/project discovery occurs.
- Add static `agent-runtime mcp inspect --config <PATH> [SERVER]` output that
  shows the resolved command, arguments, working directory, environment
  variable names, configured tool allowlist, and bounds without spawning a
  process or resolving secret values.
- Add repeatable `--allow-mcp-server <SERVER>` and
  `--allow-mcp-tool <SERVER/TOOL>` run flags. A config file alone grants no
  process or tool authority. A selected server must be explicitly authorized
  for that run, and only the intersection of its configured allowlist and exact
  per-run tool approvals is registered.
- Resolve child environment values only from named host environment variables,
  clear the child's ambient environment, canonicalize the executable and
  working directory before computing server identity, and keep all values
  redacted from debug/error/inspection output.
- Connect and bind selected servers before runtime construction, inject their
  `McpTool`s through `RuntimeBuilder::tools`, and authorize them through an
  exact CLI-owned security check and approval policy. The existing conservative
  external read/write, endpoint network, and data-egress floor remains in force.
- Keep MCP connections alive for the turn and perform bounded cleanup after
  completion, failure, or Ctrl-C. Required-server startup failures fail the
  command; optional-server failures are safe stderr diagnostics and contribute
  no tools.
- Raise only `agent-runtime-cli`'s declared Rust version to 1.88 because its
  direct `agent-runtime-mcp`/`rmcp` dependency requires it. The embeddable
  packages and workspace baseline remain Rust 1.86.

## Impact

- Affected specs: `cli-configuration`, `cli-mcp-hosting`,
  `package-architecture`
- Affected code: `crates/agent-runtime-cli`, the stdio launch boundary in
  `crates/agent-runtime-mcp`, workspace lockfile, CLI documentation, README,
  and tests
- Compatibility: additive command/config surface; existing flag-only `run`
  invocations remain valid
- Security: introduces local process execution under explicit per-run consent,
  strict tool selection, minimal environment, conservative tool authority, and
  bounded lifecycle handling
- Consumers: no Smith, Nyx, Open Forge, or sibling-repository edits are
  authorized

## Shared-Code Admission

The CLI is itself a host, so its TOML schema, flag precedence, inspection
output, consent flags, and stderr policy belong in the CLI leaf package. The
protocol connection, descriptor, tool, effect, and output-bounding mechanisms
remain in `agent-runtime-mcp`. The only shared-package correction is clearing
the ambient environment and redacting transport debug output, both required by
the already-approved policy-mediated stdio transport contract for every host.

Smith remains a behavioral reference rather than a dependency. This change
adopts the portable subset—run defaults, MCP definitions, tool filters,
timeouts, output caps, and explicit authority—without adopting Smith-specific
agents, prompts, skills, memory, persistence, credential setup, or background
policy.

## Related Active Changes

- `add-cli-runner-2026-08-29` established the leaf CLI, current flag contract,
  output channels, and lifecycle behavior. This change extends that host
  without replacing its one-turn execution path.
- `add-mcp-capability-source-2026-08-07` supplies the already-implemented MCP
  client and tool adapter while deliberately leaving configuration and trust UX
  to hosts.
- `add-runtime-security-boundary-2026-07-24` requires stdio MCP launch to be an
  authorized process spawn with an explicit minimal environment. This change
  supplies the CLI policy and completes the shared `env_clear` enforcement
  needed by that stdio path; remote MCP transport remains gated.
- Other active `package-architecture` deltas are orthogonal. This proposal adds
  a package-specific CLI MSRV rule and does not change their package contracts.
- Existing web-fetch work in the worktree is unrelated and must remain
  untouched.

## Non-Goals

- No automatic discovery of user, workspace, or project configuration.
- No persisted trust database, credential store, OAuth, interactive approval,
  or non-interactive blanket `--yes` switch.
- No streamable HTTP/SSE MCP server, redirect policy, proxy policy, remote
  bearer token, or OAuth flow.
- No MCP resources, prompts, sampling, server installation, package download,
  or command mutation.
- No Smith-specific prompt, skill, memory, child-agent, session, or credential
  configuration.
- No persisted conversation, REPL, daemon, or background server supervision.

## Approval Boundary

The user has authorized Stage 2 after this proposal passes strict validation.
That approval covers the CLI-owned versioned config, static inspection,
explicit per-run stdio MCP authorization, standard runtime tool injection,
minimal child environment correction, docs, and hermetic tests in this
repository. It does not authorize remote MCP, consumer edits, package
publication, ambient config discovery, or persistent trust/credential state.
