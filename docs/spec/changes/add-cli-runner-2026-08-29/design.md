## Context

The runtime already exposes every mechanism needed for a one-turn command:
`RuntimeBuilder`, explicit `ResolvedModelProfile` limits, `StartSession`,
`SessionHandle::send`, canonical event subscription, turn interruption, and
bounded session shutdown. The missing layer is a process host that translates
arguments/environment/stdin into those public contracts and translates the
event stream into stdout/stderr and an exit status.

Open Forge has two useful reference shapes. `forge-ctl` keeps Clap parsing thin
and delegates commands to library modules with explicit output formats and
terminal-state exit codes. Its native agent host implements the runtime's
injected HTTP transport, including typed status errors, disabled redirects,
DNS/IP validation, and redacted diagnostics. The framework CLI should reuse
those shapes without depending on Forge or copying its product commands.

## Goals / Non-Goals

- Goals:
  - Make one real runtime turn invocable from a shell or CI process.
  - Preserve the runtime's public admission, event, accounting, cancellation,
    and shutdown paths.
  - Provide deterministic human and machine output contracts.
  - Keep credentials and network behavior fail-closed.
  - Keep all concrete CLI/HTTP dependencies out of existing library graphs.
- Non-Goals:
  - Interactive chat, TUI behavior, durable sessions, background daemons,
    product policy, tool discovery, OAuth, or a configuration-file ecosystem.

## Decisions

### Decision 1: Add a leaf host package and one thin binary

Create `crates/agent-runtime-cli` with a library target and an
`agent-runtime` binary target. The binary parses arguments, selects process I/O,
and delegates to library-owned `run` logic. The library exposes internal seams
for argument/config resolution, runtime construction, event rendering, and
exit classification so tests do not need a network or subprocess.

The new package depends on `agent-runtime`; no existing package depends on the
CLI package. Clap, Reqwest, signal handling, and any terminal helpers are
declared only in this package.

Alternatives considered:

- A binary target inside `agent-runtime` was rejected because it would mix
  process policy into the facade package and make dependency isolation harder
  to audit.
- A CLI parser library without an installable binary was rejected because it
  would not solve the zero-host-code adoption gap.

### Decision 2: Start with one explicit `run` command

The initial contract is:

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
  [PROMPT]
```

If `PROMPT` is absent, stdin must be piped and is read to EOF. If stdin is a
terminal, the command fails with guidance instead of silently starting an
interactive session. Empty prompts fail before runtime construction. The
three model limits are required and validated (`max_input_tokens <=
context_tokens`, non-zero output budget) because the runtime intentionally
refuses unknown-model guesses.

Provider presets supply canonical base URLs. `openai-compatible` requires an
explicit base URL; a base URL override remains available for first-party
providers but passes the same validation. No default system prompt or tool set
is installed.

### Decision 3: Resolve credentials by name, never by value

The default environment variable is provider-specific:

| Provider | Default variable |
| --- | --- |
| `openai`, `openai-compatible` | `OPENAI_API_KEY` |
| `anthropic` | `ANTHROPIC_API_KEY` |
| `xai` | `XAI_API_KEY` |
| `gemini` | `GEMINI_API_KEY` |

`--api-key-env` changes only the variable name. There is no `--api-key` flag,
credential file, saved login, or credential echo. Missing/empty variables are
reported by variable name only, and resolved values enter the provider's
`Secret` boundary immediately.

Alternatives considered:

- A hidden terminal prompt conflicts with prompt-on-stdin composition and adds
  terminal policy beyond the non-interactive MVP.
- Persisted credentials and OAuth require a host-specific protected store and
  ceremony; they are separate capabilities.

### Decision 4: The CLI owns a restrictive production transport

The CLI implements the injected provider `HttpTransport` with Reqwest. It
normalizes and validates the configured endpoint, permits HTTPS only, rejects
userinfo/fragments, disables redirects, resolves DNS for each request, denies
loopback/private/link-local/multicast/documentation/reserved addresses, and
connects only to the validated address set. HTTP status values are converted
to the runtime's typed provider errors without returning unbounded or
credential-bearing response bodies.

This is host code, consistent with the active security design that makes DNS,
dialing, pooling, redirect, and TLS behavior a host-transport conformance
obligation. It does not become a default transport in `agent-runtime-provider`.

### Decision 5: stdout is a stable data channel

`--output text` is the default. It writes assistant `TextDelta` content to
stdout as received, adds one final newline on success, and sends lifecycle
diagnostics/errors to stderr. It does not print debug representations of
events or configuration.

`--output jsonl` writes exactly one serialized canonical runtime event envelope
per line, preserving schema version, sequence, session/turn attribution, and
redaction. It emits no prose on stdout. JSON serialization failure is terminal
rather than falling back to human text.

The runner subscribes before turn admission so no initial event is lost and
waits for the accepted turn's terminal event/handle before shutdown.

### Decision 6: Process outcomes map from structured runtime boundaries

- Completed turn: exit `0` after bounded session shutdown.
- Argument or configuration error: Clap/config exit `2`, before provider I/O.
- Provider/runtime/serialization/shutdown failure: exit `1` with a safe stderr
  diagnostic.
- Ctrl-C: interrupt the accepted turn, perform bounded shutdown, and exit
  `130`.

The runner does not call `process::exit` from reusable command modules. They
return a typed outcome to the thin binary, which owns the final process code.

### Decision 7: Test through injected seams, not real services

Parser/config/output/exit tests use in-memory I/O and a fake runtime/provider
composition. Transport tests use deterministic URL/DNS/status fixtures and
must cover redirect denial, restricted addresses (including IPv4-mapped IPv6),
credential redaction, bounded error excerpts, cancellation, and no-I/O config
rejection. The default suite opens no public network connection and requires
no provider credential.

## Risks / Trade-offs

- Requiring explicit model limits is more verbose than consumer catalogs, but
  it preserves the framework's fail-closed model contract and avoids stale CLI
  guesses.
- A one-turn command is less convenient than a REPL, but it gives shell/CI
  users a stable primitive without prematurely choosing history persistence or
  terminal UX policy.
- The new package has a heavier graph due to Clap and Reqwest. Isolation in a
  leaf package prevents that cost from reaching embedders.
- A concrete network transport expands the security surface. Exact-destination
  validation, disabled redirects, redacted failures, and adversarial tests are
  release gates rather than follow-up hardening.
- Active provider/security proposals may change constructor or conformance
  details. Stage 2 must reconcile those approved contracts without silently
  weakening this proposal.

## Migration Plan

1. Add the leaf package, parser, typed configuration, and injectable runner
   seams with no live provider I/O.
2. Add provider construction and the restrictive CLI transport.
3. Add text/JSONL rendering, signal handling, shutdown, and exit mapping.
4. Add hermetic/adversarial tests and user documentation.
5. Run workspace, dependency-boundary, MSRV, and consumer compatibility gates.

Existing consumers require no migration. A later separately approved change
may let Open Forge replace its equivalent transport with shared code only if
the ownership boundary and conformance contract justify that move.

## Open Questions

- None for the proposed MVP. Interactive sessions, persisted configuration,
  OAuth, and tool/MCP loading require separate proposals because each changes
  product or authority policy.
