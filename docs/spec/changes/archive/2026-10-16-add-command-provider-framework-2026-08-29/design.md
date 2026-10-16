## Context

Agent Runtime already reduces every model backend to `ProviderRequest`,
`Capabilities`, and `ProviderStreamEvent`. Smith already resolves provider
names and open adapter-kind strings before composing `Arc<dyn Provider>`. That
means a CLI backend does not require a parallel runtime or a consumer-specific
contract: it needs a process-backed implementation of the existing provider
trait.

The sibling Open Forge repository confirms that real model CLIs need distinct
protocol adapters and careful process-group cleanup. Its CLI adapters are
autonomous coding executors, however, so their agent-loop ownership and broad
environment inheritance cannot be copied into a model-provider mechanism.

## Goals / Non-Goals

- Goals:
  - Let a host install a command-backed `Provider` wherever it can install a
    native provider.
  - Reuse one process, cancellation, deadline, output-bound, and redaction
    implementation across concrete CLI protocol adapters.
  - Keep the runtime authoritative for canonical history, tools/MCP, approvals,
    retries, usage aggregation, and event semantics.
  - Give consumer configuration a small, typed construction surface and an
    explicit preflight/version hook.
- Non-Goals:
  - Implement Smith configuration or a Smith provider kind in this repository.
  - Ship Codex, Claude, Gemini, Cursor, or other vendor CLI adapters here.
  - Treat an autonomous coding-agent CLI as a transparent model provider.
  - Permit shell strings, ambient executable discovery, ambient environment
    inheritance, or config-read spawning.
  - Persist or resume hidden CLI sessions in version 1.

## Decisions

### Feature-gated provider module

The mechanism will live under an opt-in `command-provider` feature in
`agent-runtime-provider`; `agent-runtime` will expose a same-named passthrough
feature and its existing provider re-export will carry the API. This keeps the
process dependency graph out of native-only and library-only consumers without
creating another crate for one provider implementation.

### Trusted codec, constrained process authority

A `CommandProvider` will combine host-owned immutable process configuration
with a trusted `CommandAdapter`. The process configuration owns the absolute
executable, fixed arguments, exact working directory, explicit environment,
and resource bounds. The adapter may add per-attempt arguments and bounded
stdin, create one attempt-local stdout decoder, and declare model descriptors
and capabilities; it may not substitute the executable, invoke a shell,
inherit the environment, or change lifecycle policy.

Adapter settings remain typed host code rather than opaque TOML or generic
argument templates in the framework. This lets Smith namespace its own CLI
settings while the shared package remains product-neutral.

### One process per visible provider attempt

Each `Provider::stream` call spawns at most one process, after all static and
request-specific validation succeeds. The process receives the canonical full
request selected by Runtime. A command adapter may speak a multi-message
protocol within that process, but it may not hide another model attempt,
retry, tool loop, or persistent conversation behind the same `AttemptId`.

This preserves canonical history and retry accounting. A future autonomous
agent/executor backend is a separate interface, not a special mode of
`CommandProvider`.

### Runtime owns tools and capability truth

The adapter publishes exact capabilities through the existing provider
contract. `CommandProvider` checks `Capabilities::unsupported_for` before
encoding or spawning, and the adapter performs any stricter representation
check before spawn. Tool schemas and tool-call fragments use the existing
request/event types; a text-only CLI advertises `tools = false`. The framework
never enables or delegates to a CLI's own MCP or built-in tool loop.

Non-null vendor extensions are rejected unless the concrete adapter explicitly
validates and consumes them. Unsupported settings are never silently dropped.

### Machine stdout and redaction-safe stderr

Stdout is the sole semantic channel. The framework incrementally frames it
under per-frame and aggregate byte bounds, and an attempt-local decoder maps
frames to normalized events. It enforces no events after a terminal, exactly
one successful terminal, complete tool-call fragments through existing runtime
validation, and a malformed-stream error for premature EOF or invalid frames.

Stderr is drained under a separate bound to prevent child blockage but is not
copied into events, errors, logs, or `Debug`. A concrete adapter can derive a
bounded redaction-safe classification only from an explicit machine-readable
error on stdout. Process configuration `Debug` exposes environment names but
never values or stdin.

### Explicit process and probe lifecycle

The host supplies an already-resolved absolute executable and directory.
Spawning uses direct argv, clears the environment, pipes standard streams,
closes stdin after the bounded payload, and places the child in a killable
process group using a safe dependency with acceptable license and MSRV. The
attempt deadline and cancellation token are authoritative. Dropping the stream
also initiates process-tree termination; cleanup has a bounded grace period and
cannot detach a known live child silently.

An optional adapter-defined version probe uses the same executable, environment,
directory, and resource controls with separate small bounds. It runs only when
the host explicitly calls preflight. Static construction and inspection do not
touch the process or authentication state.

## Risks / Trade-offs

- Real CLIs expose incompatible and unstable output protocols. Keeping codecs
  consumer-owned avoids pretending that one generic argv template is portable,
  but Smith must maintain compatibility tests for each named adapter.
- Some agentic CLIs cannot expose raw provider semantics or external tool calls.
  Such CLIs must advertise a reduced capability set or use a future executor
  interface; adapting them lossy would corrupt runtime behavior.
- Process-tree termination is platform-sensitive. The implementation must use
  a tested safe abstraction and include Unix plus compile-time cross-platform
  coverage rather than relying only on `kill_on_drop` for the direct child.
- An explicit minimal environment can require more setup for logged-in CLIs.
  That friction is intentional: the consumer chooses which variables and home
  paths cross the trust boundary.

## Migration Plan

1. Add the opt-in framework and fixture-based conformance without changing any
   native provider or default feature graph.
2. Publish construction and adapter examples for a consumer-owned mock CLI.
3. In a separate `../tui` change, add one named provider kind and codec, map
   Smith's resolved model settings into the framework, and run live/version
   gates for the exact supported CLI.
4. Add more named adapters independently; never broaden one adapter based on a
   different CLI's behavior.

## Open Questions

None for the framework boundary. Concrete vendor protocol, authentication, and
configuration choices intentionally belong to the later `../tui` change.
