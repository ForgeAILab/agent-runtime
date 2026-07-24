# Agent Runtime

Agent Runtime is a neutral, embeddable Rust runtime shared by three
independently released products:

- Smith, a terminal-first coding-agent client
- Nyx, a self-hosted agent platform
- Open Forge, an agent workflow and orchestration product

It owns reusable **mechanism** — provider normalization, one direct
provider/tool loop, cancellation, usage accounting, and deterministic testing —
while each consumer keeps its own **policy**: prompts, configuration, presentation,
persistence, approval, and domain types.

## Packages

| Package | Role |
| --- | --- |
| `agent-runtime-registry` | The dependency-light registry kernel: namespaced identities, revisions, provenance, layered sealing, scoped views, fingerprints, and the generic `Named`/`Registry<T>`/`Sealed<T>` collection. Std-only by default. |
| `agent-runtime-core` | Host-neutral contracts: IDs, messages/content, structured errors, cancellation, deadlines, redaction-safe metadata, versioned events, disjoint usage counters, and the provider/tool/approval/workspace/store/observer/clock traits. |
| `agent-runtime-ability` | Descriptor-first abilities on the registry kernel: bounded `AbilityDescriptor`s, dependency/conflict/readiness metadata, lazy policy-checked activation, and the unified `Ability`/`AbilityKind` view. Registry-only by default; `tool` bridges the runtime's `Tool`. |
| `agent-runtime-provider` | Provider mechanism: injectable HTTP transport, SSE normalization, a configurable OpenAI-compatible adapter, a deterministic fake, and the attempt-recording retry/backoff classifier. |
| `agent-runtime-context` | The authoritative context engine: versioned `ContextFragment`s (including folded-in composable system-prompt sections via `SystemPromptBuilder`), complete token accounting (`RequestSizer`/`CharRatioSizer`), semantic compaction, and cache-aware planning through `ContextPlanner`. Deterministic and network-free. |
| `agent-runtime-obs` | Observability facade over the event envelope: an async `EventSink`, `FanoutSink`, a `SinkObserver` bridge, an event-stream pump, an `ObsRow` SQL projection, and feature-gated CLI/file/SQLite sinks. |
| `agent-runtime` | The embeddable runtime: the registry hub, capability retrieval/activation, the direct agent loop, the tool registry/executor, and the `RuntimeBuilder` / `Runtime` / `SessionHandle` facade. Re-exports `registry`, `ability`, `provider`, and `context`, and `obs` behind an opt-in feature. |
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
session.run(UserInput::text("hi")).await;
# Ok(())
# }
```

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

The specification for this work lives under
[`docs/spec/`](docs/spec/changes/add-shared-agent-runtime-2026-07-23/). Adopting
the runtime in Nyx, Smith, or Open Forge requires a separate approved proposal in
each consumer repository; this repository does not modify any consumer.
