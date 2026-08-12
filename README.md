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
| `agent-runtime-provider` | Provider mechanism: injectable HTTP transport, SSE normalization, configurable OpenAI-compatible, native Responses, and native Gemini Interactions adapters, a deterministic fake, and the attempt-recording retry/backoff classifier. |
| `agent-runtime-context` | The authoritative context engine: versioned and positioned `ContextFragment`s (including composable system-prompt sections), complete token accounting (`RequestSizer`/`CharRatioSizer`), deterministic structural compaction, and cache-aware planning through `ContextPlanner`. Deterministic and network-free. |
| `agent-runtime-lcm` | Lossless Context Memory: immutable logical timelines, transactional hierarchical summary DAGs, deterministic tool-safe compaction planning, convergence-guaranteed summarization, and bounded expansion. Store- and provider-neutral. |
| `agent-runtime-obs` | Observability facade over the event envelope: an async `EventSink`, `FanoutSink`, a `SinkObserver` bridge, an event-stream pump, an `ObsRow` SQL projection, and feature-gated CLI/file/SQLite sinks. |
| `agent-runtime` | The embeddable runtime: session-scoped registry views and activation epochs, the checkpointable direct turn machine, prepared tool execution, host interaction, delegation, and reusable harness components for todos, memory, artifacts, and LCM. Re-exports `registry`, `ability`, `provider`, `context`, and `lcm`, and `obs` behind an opt-in feature. |
| `agent-runtime-testkit` | Deterministic fakes, clocks, event recorders, reusable conformance suites, and neutral consumer adapter fixtures. |

All crates except `agent-runtime-testkit` are intended as production
dependencies; pick just the mechanism each consumer needs. Minimum supported
Rust version: **1.86** (edition 2024). License: **MIT**.

## Renewable provider credentials

Direct adapters can use a host-owned renewable credential source without
teaching Agent Runtime how login or token storage works:

```rust
let provider = OpenAiProvider::with_credential_source(
    transport,
    OpenAiConfig::new("https://openrouter.ai/api/v1", "model-id"),
    ProviderCredentialTarget::new("openrouter")?,
    credential_source,
)?
.with_credential_minimum_validity_ms(30_000);
```

`ProviderCredentialSource` acquires a lease with optional expiry and an opaque
revision, under the provider attempt's cancellation and deadline. A classified
pre-output authentication rejection invalidates that exact revision and may
produce one visible replacement attempt, subject to the normal total-attempt
ceiling. Existing `OpenAiConfig::api_key` callers remain supported through the
non-expiring static source path.

The host owns refresh policy, protected access/refresh-token storage, and any
browser, callback, authorization-code, or device-code ceremony. Lease secrets,
revisions, expiry, raw authentication bodies, and source references are absent
from runtime events, snapshots, checkpoints, manifests, errors, and debug
renderings. This provider contract does not turn consumer-subscription login
credentials into provider API keys.

## Native Gemini Interactions

`GeminiInteractionsProvider` implements Google's native Interactions REST/SSE
protocol without a Google SDK or OpenAI-compatibility translation. Hosts supply
an absolute reviewed API-version base URL, one resolved model/capability
profile, and either a static API key or `ProviderCredentialSource`:

```rust
let mut config = GeminiInteractionsConfig::new(
    "https://generativelanguage.googleapis.com/v1beta",
    "gemini-3.6-flash",
)
.with_supported_thinking_levels(["minimal", "low", "medium", "high"]);
config.capabilities = resolved_capabilities;

let provider = GeminiInteractionsProvider::with_credential_source(
    transport,
    config,
    ProviderCredentialTarget::new("google")?,
    credential_source,
)?;
```

The adapter always sends `stream=true` and `store=false`, injects the key only
as `x-goog-api-key`, rejects provider storage/background/hosted-tool overrides,
and reconstructs complete input steps from canonical local history. Signed
thought blocks—including signature-only thoughts—remain opaque, durable
continuation content across tool loops and later local replay. Endpoint choice,
model catalogs, defaults, credential persistence, and provider UX remain host
policy. Vertex AI, provider-hosted tools, and `previous_interaction_id` are not
supported by this adapter.

## Native Responses / xAI Grok

`ResponsesProvider` implements the stateless OpenAI Responses wire protocol
over the same injected transport. The first fixture-verified deployment is
xAI's Grok Responses endpoint:

```rust
let mut config = ResponsesConfig::new("https://api.x.ai/v1", "grok-4.5");
config.api_key = Some(Secret::new("xai-api-key"));
let provider = ResponsesProvider::new(transport, config)?;
```

Every request sends complete local history with `stream=true`, `store=false`,
`include=["reasoning.encrypted_content"]`, and a session-derived
`prompt_cache_key`. Signed reasoning summaries and encrypted-only reasoning
items remain ordered continuation content around function calls. Provider
storage, `previous_response_id`, background responses, and hosted tools are
rejected before credentials or network I/O. Model limits and reasoning effort
availability remain host/catalog policy; the adapter does not embed Grok Build
defaults.

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

Eligible provider-backed work can accept additional real-user input without
starting a later whole turn:

```rust
let receipt = session.steer_current_turn(
    Some(turn.id()),
    UserInput::text("also cover the cancellation race"),
)?;
```

Acceptance is process-local. The input becomes canonical only when the event
stream emits the matching `TurnSteerCommitted`; `TurnSteerDiscarded` means the
host still owns any locally retained draft. Rejections are typed and retain
the exact `UserInput`. Steering never mutates an in-flight provider request:
the driver drains FIFO input only at a protected provider/tool boundary and
continues under the same `TurnId`.

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

Persistent goals are an opt-in reusable harness component. Hosts register
`get_goal`, `create_goal`, and `update_goal`, the `GoalComponent` phases, and
at most one process-scoped `GoalController` per eligible session. Automatic
continuations use `try_send_internal_if_idle`: they carry typed provenance,
create no user-role history message, and lose atomically to real user input.
Interactive hosts with a separate process-local input queue can attach a
`GoalAdmissionGate` to the controller and disable idle-only admission until
their real-user turn is admitted.
Goal state is durable in versioned extension state, while scheduling remains
strictly process-scoped—there is no daemon or restart-time execution.

LCM is opt-in: a host supplies its transactional `LcmReader`/`LcmWriter`
adapter, one shared `LcmViewAuthority`, timeline resolver, summary model, and
policy, then attaches the coordinator with `RuntimeBuilder::lcm`. Soft work is
admitted only by `SessionHandle::try_idle_compaction`; hard work completes
before provider admission. The context planner remains authoritative for final
budgeting and provider serialization. Hosts may attach a summary-body
`ContentGuard` with `LcmCoordinator::with_content_guard`; guard identity and
revision are protected compatibility inputs. Authorized callers can inspect a
bounded source page through `SessionHandle::expand_lcm` without receiving or
supplying an authority grant. See
[`docs/migration-0.1.md`](docs/migration-0.1.md#17-lossless-context-memory) for
the direct-package and facade composition examples.

## Development

```sh
cargo test --workspace --all-features
cargo clippy --all-targets --all-features -- -D warnings

# MSRV 1.86. Every production package is listed: a partial list is how an MSRV
# violation reaches a consumer, since newer syntax compiles fine on stable.
cargo +1.86.0 build \
  -p agent-runtime-registry -p agent-runtime-core -p agent-runtime-ability \
  -p agent-runtime-provider -p agent-runtime-context -p agent-runtime-lcm \
  -p agent-runtime-obs \
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
