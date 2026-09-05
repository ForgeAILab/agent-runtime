# Agent Runtime CLI

`agent-runtime` is the framework's isolated reference command-line host. It
executes one in-process runtime turn and exits; no daemon or consumer product
is required.

## Install

From this checkout:

```sh
cargo install --path crates/agent-runtime-cli
```

The CLI requires Rust 1.88 because its local MCP client uses the official
protocol SDK. The embeddable runtime packages retain their Rust 1.86 baseline.

The package is deliberately separate from the `agent-runtime` library. A Rust
host that depends on the runtime facade does not acquire Clap, Reqwest, signal,
or process-level dependencies.

## Run one turn

```text
agent-runtime run \
  --provider <openai|openai-compatible|anthropic|xai|gemini> \
  --model <MODEL> \
  --context-tokens <N> \
  --max-input-tokens <N> \
  --max-output-tokens <N> \
  [--base-url <HTTPS_URL>] \
  [--api-key-env <ENV_NAME>] \
  [--output <text|jsonl>] \
  [--config <PATH>] \
  [--allow-mcp-server <SERVER>]... \
  [--allow-mcp-tool <SERVER/TOOL>]... \
  [PROMPT]
```

Provider, model, and all three model limits must resolve from command flags or
an explicit config file. The CLI does not guess a context window for an
unknown model. `max-input-tokens` and `max-output-tokens` must be non-zero and
no larger than `context-tokens`.

First-party providers use their canonical endpoint unless `--base-url` is
supplied. `openai-compatible` has no safe universal endpoint and therefore
requires `--base-url`.

Example with placeholders that must be replaced by limits for the selected
model:

```sh
export OPENAI_API_KEY='...'

agent-runtime run \
  --provider openai \
  --model '<MODEL>' \
  --context-tokens <CONTEXT_TOKENS> \
  --max-input-tokens <MAX_INPUT_TOKENS> \
  --max-output-tokens <MAX_OUTPUT_TOKENS> \
  'Summarize the trade-offs in this design.'
```

## Credentials

Credential values are read only from environment variables:

| Provider | Default variable |
| --- | --- |
| `openai`, `openai-compatible` | `OPENAI_API_KEY` |
| `anthropic` | `ANTHROPIC_API_KEY` |
| `xai` | `XAI_API_KEY` |
| `gemini` | `GEMINI_API_KEY` |

`--api-key-env NAME` selects a different variable name. There is no API-key
value flag, credential file, saved login, or credential import. Missing values
are reported by variable name only. Credentials, request headers, prompts, and
provider response bodies are excluded from CLI debug/error rendering.

## Versioned configuration

`--config PATH` loads exactly one strict version-1 TOML file. The CLI never
searches a home directory, workspace, or project for ambient configuration.
Unknown fields and unsupported versions fail before provider I/O or child
process launch.

The file can provide defaults for the existing run settings:

```toml
version = 1

[run]
provider = "anthropic"
model = "<MODEL>"
context_tokens = 200000
max_input_tokens = 180000
max_output_tokens = 20000
api_key_env = "ANTHROPIC_API_KEY"
output = "text"
```

An explicit command flag overrides the corresponding file value. File values
override only documented defaults such as text output. Prompt text never comes
from the file; it remains positional or piped stdin. Literal provider or MCP
secret fields are not part of the schema.

```sh
agent-runtime run \
  --config ./agent-runtime.toml \
  --model '<COMMAND_LINE_OVERRIDE>' \
  'Summarize this change.'
```

## Trusted local MCP tools

Version 1 supports local stdio MCP servers only:

```toml
version = 1

[run]
provider = "anthropic"
model = "<MODEL>"
context_tokens = 200000
max_input_tokens = 180000
max_output_tokens = 20000

[mcp.servers.example]
command = "/absolute/path/to/mcp-server"
args = ["--stdio"]
cwd = "."
env_from = { PATH = "PATH", SERVICE_TOKEN = "EXAMPLE_SERVICE_TOKEN" }
allow_tools = ["search", "get_record"]
required = true
startup_timeout_ms = 10000
request_timeout_ms = 60000
max_output_bytes = 65536
```

Startup timeouts are limited to 300,000 ms, request timeouts to 600,000 ms,
and one tool result to 1,048,576 bytes; all three values must be non-zero.

`env_from` maps a variable visible to the child to the name of a source
variable in the CLI process. Values are resolved only for selected servers and
are never shown in inspection or diagnostics. The child receives a cleared
environment plus exactly these mappings. If a server or launcher needs
`PATH`, `HOME`, or another variable, grant it explicitly.

A file describes a server but cannot authorize itself. Inspect the static,
redaction-safe definition without launching it:

```sh
agent-runtime mcp inspect --config ./agent-runtime.toml example
```

Then authorize both the process and each exact tool for one run:

```sh
agent-runtime run \
  --config ./agent-runtime.toml \
  --allow-mcp-server example \
  --allow-mcp-tool example/search \
  --allow-mcp-tool example/get_record \
  'Find the relevant records and summarize them.'
```

Only tools present in both the file's `allow_tools` and the per-run flags are
registered. They use the runtime's ordinary prepare, authorization, approval,
invocation, cancellation, and outcome path. Server risk annotations cannot
lower the conservative external read/write, endpoint-network, and data-egress
authority assigned to an unreviewed MCP tool.

Servers start in deterministic name order. A `required = true` startup or
binding failure exits `1` before the provider turn. An optional failure removes
that server's tools, writes a safe warning to stderr, and lets the turn
continue. Connections receive bounded cleanup after completion, failure, or
Ctrl-C.

## Prompt input

Pass a positional prompt or omit it and pipe stdin to EOF:

```sh
printf '%s\n' 'Explain the public runtime lifecycle.' | \
  agent-runtime run \
    --provider openai \
    --model '<MODEL>' \
    --context-tokens <CONTEXT_TOKENS> \
    --max-input-tokens <MAX_INPUT_TOKENS> \
    --max-output-tokens <MAX_OUTPUT_TOKENS>
```

If no positional prompt is present and stdin is a terminal, the command exits
with guidance. It never silently starts an interactive session. Empty input is
rejected before provider I/O.

## Output

`--output text` is the default. Stdout contains only streamed visible assistant
text and one final newline on success. Diagnostics use stderr.

`--output jsonl` writes exactly one canonical `EventEnvelope` per stdout line,
including schema version, monotonic sequence, session/turn identity, timestamp,
and the redaction-safe runtime payload. No prose is mixed into stdout:

```sh
agent-runtime run \
  --provider xai \
  --model '<MODEL>' \
  --context-tokens <CONTEXT_TOKENS> \
  --max-input-tokens <MAX_INPUT_TOKENS> \
  --max-output-tokens <MAX_OUTPUT_TOKENS> \
  --output jsonl \
  'Return a short status.' | jq -c '.payload'
```

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Turn completed and the session shut down cleanly |
| `1` | Provider, MCP setup, runtime, output serialization, I/O, or shutdown failure |
| `2` | Argument or resolved configuration error |
| `130` | Ctrl-C interrupted the accepted turn and bounded shutdown completed |

## Network boundary

The concrete CLI transport is bound to the configured provider origin. It
allows HTTPS only, rejects URL userinfo and fragments, ignores environment
proxy settings, disables redirects, resolves DNS for every request, and denies
loopback, private, link-local, multicast, documentation, unspecified, and
other reserved address classes—including IPv4-mapped IPv6 forms. A rejected
destination receives neither credentials nor a request body.

MCP version 1 does not add a remote network boundary. It compiles the local
stdio transport only; streamable HTTP/SSE, redirects, proxies, bearer-token
handling, and OAuth are deliberately deferred.

## Deliberate non-goals

The CLI has no interactive REPL/TUI, persisted sessions, credentials or trust
database, OAuth flow, remote MCP, ambient config discovery, MCP resources/
prompts/sampling, server installer, built-in filesystem/shell tools, plugin
loading, daemon, REST API, or product-specific prompt/policy. Smith-specific
agents, skills, memory, prompts, credential setup, and background policy remain
in Smith rather than entering this neutral host.
