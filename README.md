# Agent Runtime

Agent Runtime is a neutral, embeddable Rust runtime shared by three
independently released products:

- Smith, a terminal-first coding-agent client
- Nyx, a self-hosted agent platform
- Open Forge, an agent workflow and orchestration product

It owns reusable **mechanism** — provider normalization, one checkpointable
provider/tool turn machine, prepared-invocation security, cancellation, usage
accounting, session-scoped capability activation, and deterministic testing —
while each consumer keeps its own **policy**: authored prompts, configuration,
presentation, persistence implementations, approval defaults, and domain
types.

## Packages

| Package | Role |
| --- | --- |
| `agent-runtime-registry` | The dependency-light registry kernel: namespaced identities, revisions, provenance, layered sealing, scoped views, fingerprints, and the generic `Named`/`Registry<T>`/`Sealed<T>` collection. Std-only by default. |
| `agent-runtime-core` | Host-neutral contracts: IDs, messages/content, structured errors, cancellation, deadlines, redaction-safe metadata, versioned events, disjoint usage counters, and the provider/tool/approval/workspace/store/observer/clock traits. |
| `agent-runtime-ability` | Descriptor-first abilities on the registry kernel: bounded `AbilityDescriptor`s, dependency/conflict/readiness metadata, lazy policy-checked activation, and the unified `Ability`/`AbilityKind` view. Registry-only by default; `tool` bridges the runtime's `Tool`. |
| `agent-runtime-provider` | Provider mechanism: injectable HTTP transport, SSE normalization, a configurable OpenAI-compatible adapter, a deterministic fake, and the attempt-recording retry/backoff classifier. |
| `agent-runtime-context` | The authoritative context engine: versioned and positioned `ContextFragment`s (including composable system-prompt sections), complete token accounting (`RequestSizer`/`CharRatioSizer`), deterministic structural compaction, and cache-aware planning through `ContextPlanner`. Deterministic and network-free. |
| `agent-runtime-obs` | Observability facade over the event envelope: an async `EventSink`, `FanoutSink`, a `SinkObserver` bridge, an event-stream pump, an `ObsRow` SQL projection, and feature-gated CLI/file/SQLite sinks. |
| `agent-runtime` | The embeddable runtime: session-scoped registry views and activation epochs, the checkpointable direct turn machine, prepared tool execution, host interaction, delegation, and reusable harness components for todos, memory, artifacts, and semantic summaries. Re-exports `registry`, `ability`, `provider`, and `context`, and `obs` behind an opt-in feature. |
| `agent-runtime-testkit` | Deterministic fakes, clocks, event recorders, reusable conformance suites, and neutral consumer adapter fixtures. |

All crates except `agent-runtime-testkit` are intended as production
dependencies; pick just the mechanism each consumer needs. Minimum supported
Rust version: **1.86** (edition 2024). License: **MIT**.

## Quick start

```rust
use std::sync::Arc;
use agent_runtime::core::prelude::*;
use agent_runtime::provider::fake::FakeProvider;
use agent_runtime::runtime::{RuntimeBuilder, StartSession};

# async fn run() -> Result<(), RuntimeError> {
let runtime = RuntimeBuilder::new(ModelId::new("fake"))
    .provider(Arc::new(FakeProvider::text_reply("hello")))
    // Required: every request is planned against declared limits, and the
    // runtime refuses to guess a context window.
    .model_profile(ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(128_000, 128_000, 4_096),
    ))
    .build()?;

let session = runtime.start_session(StartSession::new()).await?;
let turn = session.run(UserInput::text("hi")).await?;
println!("completed {}", turn.id());
# Ok(())
# }
```

`SessionHandle::send` returns a turn-local `TurnHandle`; interrupting it does
not permanently cancel the session. `cancel_session` is reserved for terminal
teardown. Hosts that need crash recovery can inject separate `SessionStore`
and protected `CheckpointStore` implementations. The ordinary store receives
completed session state; the protected store records exact versioned
mid-turn states and pending interactions.

Delegated children become durable when the host's `ChildRuntimeFactory`
provides both stores. The parent snapshot then carries a bounded,
redaction-safe child catalog while each child keeps its canonical history and
exact checkpoint under a stable child session ID. Restoring a parent only
rebinds metadata. After constructing its coordinator, a durable host calls
`coordinator.recover().await` to reconcile the authoritative protected child
checkpoints and any returned interactions without constructing providers.
`follow_up` starts a new turn on an idle child, while `resume` explicitly
continues one safe interrupted checkpoint. Neither path silently spawns a
replacement, and a checkpoint at indeterminate provider I/O is deliberately
non-resumable.

Live capability routing is opt-in through
`RuntimeBuilder::live_ability_routing()`. The runtime always retains the
protected, authority-free `registry.search` bootstrap, derives a scoped view
per session, and advertises only the current activation epoch. Reusable
behavior above the neutral kernel is composed through ordered, phase-specific
harness components rather than an unrestricted mutable middleware chain.

## Development

```sh
cargo test --workspace --all-features
cargo clippy --all-targets --all-features -- -D warnings

# MSRV 1.86. Every production package is listed: a partial list is how an MSRV
# violation reaches a consumer, since newer syntax compiles fine on stable.
cargo +1.86.0 build \
  -p agent-runtime-registry -p agent-runtime-core -p agent-runtime-ability \
  -p agent-runtime-provider -p agent-runtime-context -p agent-runtime-obs \
  -p agent-runtime

# Dependency boundaries are contracts, not preferences.
cargo tree -p agent-runtime-registry --no-default-features -e normal  # std-only
cargo tree -p agent-runtime-ability  --no-default-features -e normal  # kernel only
```

See [`docs/development.md`](docs/development.md) for the tagged-dependency and
uncommitted local-override workflows, and [`CONTRIBUTING.md`](CONTRIBUTING.md)
for the neutrality boundary rule.

## Status and provenance

This runtime seeds its reusable provider, agent-loop, and tool mechanisms from
the Nyx project, with all product policy removed. The donor revision, path
mappings, and retained notices are recorded in [`PROVENANCE.md`](PROVENANCE.md).

The current stabilization and durable-child specifications live under
[`docs/spec/changes/stabilize-session-harness-pipeline-2026-07-31/`](docs/spec/changes/stabilize-session-harness-pipeline-2026-07-31/)
and
[`docs/spec/changes/add-resumable-child-sessions-2026-07-31/`](docs/spec/changes/add-resumable-child-sessions-2026-07-31/).
Adopting a breaking runtime revision in Nyx, Smith, or Open Forge requires a
separate coordinated consumer change.
