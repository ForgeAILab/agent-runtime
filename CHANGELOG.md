# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project uses
pre-1.0 semantic versioning: while the major version is `0`, minor releases may
contain breaking changes and are coordinated with consumer proposals.

## [Unreleased]

### Breaking

See [`docs/migration-0.1.md`](docs/migration-0.1.md) for the full migration.

- `RuntimeBuilder::build()` now **requires** a resolvable model profile, via
  `model_profile(..)` or `model_catalog(..)`, and fails otherwise. There is no
  default context window: a runtime that cannot state its model's limits cannot
  enforce a budget, and guessing one is how uncounted context reaches a
  provider.
- Provider requests are derived from an immutable `ContextPlan` instead of being
  assembled from the system prompt, full history, and every registered tool.
  Every context-bearing field is counted before the request is sent, and a turn
  that cannot fit fails before any network I/O rather than at the provider.
- `agent-runtime-prompt` was folded into `agent-runtime-context` and removed;
  its `TokenEstimator`/`CharBasedEstimator` are superseded by `RequestSizer`.
- `Named`/`Registry<T>`/`Sealed<T>` moved to `agent-runtime-registry`. A `Named`
  impl on a foreign type (e.g. `Arc<dyn YourTrait>`) is now an orphan impl and
  needs a local newtype.
- Event `SCHEMA_VERSION` is `2`, adding nine planning-lifecycle variants.
  Existing variants are unchanged and the v1 golden fixture still guards the v1
  wire representation.

### Changed
- Split the monolithic `agent-runtime` crate into focused, single-responsibility
  crates so consumers (Nyx, Open Forge, Smith) can depend on just the mechanism
  they need. Provider adapters moved from `agent-runtime::provider` into the new
  `agent-runtime-provider` crate; `agent_runtime::provider::*` paths still
  resolve via a re-export, so this is source-compatible.
- The tool registry is now a thin, schema-validating specialization of the
  shared `agent-runtime-registry` collection mechanism, held via a local
  `Named` wrapper; its public API is unchanged.
- Generic registry primitives (`Named`, `Registry<T>`, `Sealed<T>`) moved from
  `agent-runtime-ability` into `agent-runtime-registry`, which owns every
  registry mechanism now; `agent-runtime-ability` re-exports them for
  compatibility. `agent-runtime-ability` is now descriptor-first: bounded
  `AbilityDescriptor`s with affordances/dependencies/conflicts/readiness/risk,
  and lazy policy-checked activation, built on the registry kernel's
  namespaced `RegistryId` identity.
- Folded the standalone `agent-runtime-prompt` crate into `agent-runtime-context`
  before its first release, so the workspace has exactly one token-budget and
  provider-context assembly path. `SystemPromptBuilder::into_fragments` turns
  named prompt sections into versioned `ContextFragment`s (revision, priority,
  and cache class carried through to the authoritative `ContextPlan`); the
  standalone crate's separate `TokenEstimator`/`CharBasedEstimator` was
  dropped in favor of `agent-runtime-context`'s `RequestSizer`/`CharRatioSizer`.

### Added
- Agent delegation (`add-agent-delegation-runtime`): a neutral
  `DelegationCoordinator` spawns children as full runtime sessions built by a
  host `ChildRuntimeFactory`, with spawn/list/follow-up/wait/result/stop
  addressed by stable `ChildId`. Depth-one is enforced fail-closed (child
  views lose delegation tools; a child session cannot construct a
  coordinator), spawn/follow-up/stop pass the composed authorization path
  under the host-covered `agent.delegate` permission, per-parent and shared
  capacity are reject-by-default with an explicit queue policy, and children
  stop with their parent or process and never resume. Attributed child
  lifecycle events (`ChildSpawned` … `ChildFailed`) join the event vocabulary
  (`SCHEMA_VERSION` is now `4`); the completed event carries the child's
  final result so coalescing can never drop it.
- Safe-boundary content injection: `SessionHandle::inject` queues bounded
  host content (`RuntimeBuilder::injection_queue_limit`, default 64) that the
  driver introduces only at provider/tool boundaries — never mid-stream —
  with structured overflow for coalescable items and guaranteed delivery for
  must-deliver items (e.g. final child results).
- Testkit: a delegation conformance suite (lifecycle ordering, depth
  rejection, fail-closed coverage, capacity, scoped views, stop/teardown
  cancellation propagation) and safe-boundary injection integration tests.
- Reasoning preservation: the driver retains streamed reasoning as
  `ContentPart::Reasoning` history parts for the turn that produced it
  (merging consecutive same-`redacted` deltas, placed ahead of visible text
  and tool calls) and sheds prior-turn reasoning when the next user turn
  starts, dropping reasoning-only assistant messages rather than sending them
  empty. The OpenAI-compatible adapter serializes non-redacted reasoning as
  `reasoning_content` on assistant wire messages — required by
  OpenAI-compatible thinking models (e.g. Z.AI GLM) during tool-call
  continuations — and never serializes redacted reasoning. Compaction strips
  prior-turn reasoning as its cheapest first stage and truncates reasoning
  parts like text.
- `ContextPlanned` gains `input_tokens` (the counted consumption,
  `serde(default)` for journals written before the field existed) and
  `ContextPlan::input_budget()` exposes the enforced budget.
- `TurnCompleted` gains `visible_output`: `false` flags a reasoning-only
  completion so hosts can react instead of showing nothing. Serialized only
  when `false`; ordinary turns and old journals keep the previous wire shape.
- `ContentPart::Reasoning` gains an optional `signature` for providers that
  sign thinking blocks; absent from the wire when unset, and dropped by
  tool-output truncation whenever the signed text is altered.
- Provider conformance now covers reasoning: adapters must normalize
  streamed reasoning identically and accept continuation requests carrying
  reasoning history back (`assert_normalized_reasoning_stream`), and the
  OpenAI adapter's wire echo of `reasoning_content` is asserted end to end.
- `agent-runtime-registry`: the dependency-light registry kernel — namespaced
  `RegistryId`/`RegistryDomain` identity, `RegistryRevision`/`RegistrySource`/
  `EntryProvenance`, layered sealing with deterministic conflict/override
  rules, bounded searchable `RegistryCard`s, scoped `RegistryView`s, stable
  `Fingerprint`s, and the generic `Named`/`Registry<T>`/`Sealed<T>` collection.
  Std-only by default; `serde` adds (de)serialization.
- `agent-runtime-provider`: the provider mechanism (injectable HTTP transport,
  SSE normalization, OpenAI-compatible adapter, deterministic fake, and the
  retry/backoff classifier) as its own crate depending only on
  `agent-runtime-core`.
- `agent-runtime-context`: the authoritative context engine — versioned
  `ContextFragment`s, complete provider-wire token accounting via
  `RequestSizer`/`CharRatioSizer`, semantic compaction, cache-aware planning,
  and the immutable `ContextPlan` that is the exclusive source of provider
  messages/tools/reserves/counts. Includes the folded-in composable
  system-prompt mechanism (`SystemPromptBuilder` and its section types).
  Deterministic and network-free.
- `agent-runtime-obs`: an observability facade over the neutral event envelope —
  an async `EventSink` trait, `FanoutSink`, a `SinkObserver` bridge onto the
  runtime's observer hook, a `drive` pump for the async event stream, an
  `ObsRow` SQL projection, and feature-gated `CliSink` (default), `FileSink`
  (JSONL), and `SqliteSink` (opt-in) sinks.
- `agent-runtime` re-exports `registry`, `ability`, `provider`, and `context`
  directly, and `obs` behind an opt-in feature, for one-stop consumption.
- Rust 2024 workspace with `agent-runtime-core`, `agent-runtime`, and
  `agent-runtime-testkit` (minimum supported Rust version 1.86).
- Host-neutral core contracts: neutral IDs, messages/content, structured
  errors, cancellation, deadlines, redaction-safe metadata, versioned events,
  and disjoint usage counters with per-counter provenance.
- Host adapter traits: `Provider`, `Tool`, `ApprovalPolicy`, `Workspace`,
  `SessionStore`, `SecretStore`, `EventObserver`, and `Clock`.
- Provider runtime: capability/model descriptors, normalized requests, typed
  streaming events, a deterministic fake adapter, a configurable
  OpenAI-compatible adapter over an injectable HTTP transport, and an
  attempt-recording retry wrapper.
- Tool + agent execution: deterministic tool registry with name-conflict
  validation, fail-closed approval and workspace enforcement, side-effect-aware
  scheduling, and one canonical direct provider/tool loop with configured
  limits.
- Embeddable runtime facade: `RuntimeBuilder`, `Runtime`, and `SessionHandle`
  with injected host services, versioned commands/events, concurrent
  subscribers, cancellation propagation, and bounded shutdown.
- `agent-runtime-testkit`: fake clock, event recorder, temporary workspace, and
  reusable conformance suites (provider, tool, runtime, cancellation,
  event-schema, shutdown) plus neutral consumer adapter fixtures.

### Fixed
- `ContextPlanned::input_budget_tokens` now reports the enforced input budget
  it was always documented as, instead of the counted consumption (which
  moved to the new `input_tokens` field).

### Provenance
- Reusable provider, agent-loop, and tool mechanisms were adapted from the Nyx
  project. See `PROVENANCE.md` for the donor revision and path mappings.
