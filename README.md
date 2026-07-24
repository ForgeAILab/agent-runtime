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
| `agent-runtime-core` | Host-neutral contracts: IDs, messages/content, structured errors, cancellation, deadlines, redaction-safe metadata, versioned events, disjoint usage counters, and the provider/tool/approval/workspace/store/observer/clock traits. |
| `agent-runtime` | The embeddable runtime: provider adapters (deterministic fake + configurable OpenAI-compatible), the direct agent loop, the tool registry/executor, and the `RuntimeBuilder` / `Runtime` / `SessionHandle` facade. |
| `agent-runtime-testkit` | Deterministic fakes, clocks, event recorders, reusable conformance suites, and neutral consumer adapter fixtures. |

Only `agent-runtime-core` and `agent-runtime` are intended as production
dependencies. Minimum supported Rust version: **1.86** (edition 2024). License:
**MIT**.

## Quick start

```rust
use std::sync::Arc;
use agent_runtime::core::prelude::*;
use agent_runtime::provider::fake::FakeProvider;
use agent_runtime::runtime::{RuntimeBuilder, StartSession};

# async fn run() -> Result<(), RuntimeError> {
let runtime = RuntimeBuilder::new(ModelId::new("fake"))
    .provider(Arc::new(FakeProvider::text_reply("hello")))
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
cargo build -p agent-runtime-core -p agent-runtime   # MSRV 1.86
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
