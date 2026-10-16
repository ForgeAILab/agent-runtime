---
created_at: 2026-08-29T21:16:28Z
updated_at: 2026-08-29T21:16:28Z
---

# Design: CLI configuration and trusted stdio MCP

## Context

`agent-runtime-cli` already resolves process arguments and environment values
into a `ResolvedRunConfig`, constructs a provider and model profile, builds the
public runtime, subscribes before turn admission, and maps terminal events to
stable output and exit codes. `agent-runtime-mcp` already turns a fully resolved
`McpServerConfig` into conservative `McpTool` implementations. The missing
layer is host policy: durable input, process consent, tool approval, secret
resolution, and connection lifetime.

## Decision 1: one explicit, versioned CLI-owned TOML document

The initial shape is:

```toml
version = 1

[run]
provider = "anthropic"
model = "claude-example"
context_tokens = 200000
max_input_tokens = 180000
max_output_tokens = 20000
api_key_env = "ANTHROPIC_API_KEY"
output = "text"

[mcp.servers.github]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-github"]
cwd = "."
env_from = { PATH = "PATH", GITHUB_TOKEN = "GITHUB_TOKEN" }
allow_tools = ["search_issues", "get_issue"]
required = true
startup_timeout_ms = 10000
request_timeout_ms = 60000
max_output_bytes = 65536
```

`version` is mandatory and must equal `1`. Unknown fields, duplicate server
names, invalid names, empty commands/tool lists, zero bounds, missing source
environment variables, and invalid run settings fail before any provider I/O or
child spawn. Relative config paths are resolved from the process; relative
server working directories and path-shaped commands are resolved from the
config file's directory. Bare commands are resolved to an absolute executable
using the host's current `PATH` before the child's environment is cleared.

The config file never stores prompt text or secret values. `env_from` maps a
child variable name to a source environment variable name. Inspection shows
only that mapping, never its resolved value.

Run-setting precedence is:

```text
explicit command flag > explicit config value > existing safe CLI default
```

Provider, model, and all three model limits must still resolve explicitly.
`--config` is optional for flag-only compatibility. There is no ambient search
order, include directive, profile inheritance, or environment interpolation in
this slice.

## Decision 2: config describes; command flags authorize

Merely loading a file never starts a server. A run starts a configured server
only when its name appears in `--allow-mcp-server`. Every tool registered from
that server must also appear both in its `allow_tools` config list and an exact
`--allow-mcp-tool <server>/<tool>` argument. Unknown, duplicate, cross-server,
or non-allowlisted approvals fail before spawn.

This deliberately avoids a persistent trust database in the first slice. It
also ensures a checked-in file cannot grant itself process or invocation
authority. Shell history records only non-secret server/tool identities.

`agent-runtime mcp inspect` is static and side-effect free. It gives the user a
redaction-safe view of exactly what a subsequent run flag would authorize. It
does not list live tools, because doing so would already require executing the
configured command.

## Decision 3: clear ambient child state at the shared stdio boundary

The MCP client calls `env_clear()` before applying `McpServerConfig`'s explicit
environment map. The CLI resolves a bare executable through the host `PATH`
first and passes the absolute path in the resolved config. If the child itself
needs `PATH`, `HOME`, a credential, or any other variable, the file must grant
it through `env_from`.

`McpTransport` receives a custom redacted `Debug` representation: command,
arguments, cwd, and environment names may be shown; environment/header values
may not. `McpServerConfig::identity` continues to exclude secret values and now
observes the CLI-resolved absolute executable and working directory.

This is a shared safety correction rather than CLI-specific behavior. A caller
that truly needs environment inheritance must enumerate the values it grants.

## Decision 4: selected tools use the ordinary security pipeline

For each successfully connected server, the CLI constructs `McpTool`s only for
the exact per-run approvals and registers them with `RuntimeBuilder::tools`.
It supplies:

- an authoritative CLI security check covering the selected tools' external
  read/write, exact endpoint network, and data-egress permissions; and
- an approval policy that allows only the exact model-facing tool names
  produced by those selected bindings and denies every other request.

Both decisions are bound to the resolved tool registration. Server annotations
cannot lower the `agent-runtime-mcp` conservative floor. The CLI does not opt a
server into a reviewed narrower floor and does not use `AllowAll`.

The process-spawn decision occurs before dialing: exact server-name consent,
strict config validation, resolved command presentation through inspection, and
minimal-environment construction are all complete before `McpClient::connect`.
The client remains mechanism-only and continues to require the caller to make
that decision.

## Decision 5: preparation owns connections; runner owns final cleanup

Preparing an MCP-enabled run is asynchronous:

1. resolve and validate file, flags, environment references, paths, and
   approvals;
2. connect selected servers in deterministic name order;
3. bind and filter tools;
4. build the runtime with those tools and exact policies;
5. retain one shared connection per server through the turn.

If a required server fails, preparation shuts down every connection already
opened and returns exit `1`. An optional failure becomes a safe stderr
diagnostic and contributes no tools. Binding rejections are diagnostics; a
selected server that yields none of its approved tools is a required failure or
an optional omission rather than silently running with a different surface.

After the turn stops accepting tool work, the runner shuts down the runtime
session, then every MCP connection with a bounded aggregate deadline. Ctrl-C
interrupts the turn first. MCP diagnostics remain on stderr, preserving both
text and JSONL stdout contracts.

## Decision 6: isolate the dependency and higher MSRV in the CLI leaf

`agent-runtime-cli` directly depends on `agent-runtime-mcp` with default
features only. Therefore it compiles stdio and not HTTP. It declares Rust 1.88,
matching `rmcp`; the workspace and embeddable package baseline stays 1.86.

This is more honest than hiding MCP behind a default-off feature while an
all-feature CLI build still requires 1.88. No existing package may depend on
the CLI, so embedders remain unaffected.

## Risks / Trade-offs

- Per-run approval flags are intentionally verbose. They are auditable,
  non-persistent, and safe for the first headless host; a persistent trust UX
  can be proposed after real usage.
- Clearing the environment can break existing MCP callers that accidentally
  depended on inheritance. That behavior conflicts with the approved security
  contract; migration is to enumerate required variables explicitly.
- A config allowlist and CLI approval bind tool names, not the semantics of a
  third-party executable. The conservative authority floor and explicit server
  consent remain necessary even when the schema appears read-only.
- Optional server failure means a run may proceed with fewer tools. The stderr
  diagnostic and deterministic required/optional choice keep this observable.

## Verification

- Config/parser tests cover precedence, versioning, unknown fields, relative
  resolution, missing env sources, redaction, and no-I/O failure.
- CLI policy tests prove a file alone spawns nothing and a tool requires both
  config allowlisting and exact per-run approval.
- MCP client tests prove ambient variables do not reach a child and `Debug`
  does not expose values.
- In-process MCP fixtures prove binding, ordinary prepared invocation,
  conservative effects, optional/required behavior, cancellation, and bounded
  shutdown without a public network connection.
- Dependency tests prove stdio is enabled, HTTP is absent, the CLI is a leaf,
  and embeddable package graphs/MSRV remain unchanged.
