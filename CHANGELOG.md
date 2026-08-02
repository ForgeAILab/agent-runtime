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
- Event `SCHEMA_VERSION` is now `11`. Since the registry-driven v2 baseline,
  tool-call argument projection, delegation, attempt-scoped streaming,
  metadata-only host interaction, lossless child `needs_input`, and
  durability-aligned `PlanUpdated`, and durable-child recovery/resume each
  advanced the vocabulary. Golden fixtures retain the compatible v5 through
  v11 wire forms; pre-v5
  unattributed output deltas are intentionally rejected.
- `SessionHandle::send` and `run` return `Result<TurnHandle, RuntimeError>`.
  `TurnHandle` owns turn-local interruption and completion; use
  `cancel_session` only for terminal session teardown. The compatibility
  `cancel` alias retains terminal semantics.
- `Tool` now separates `spec`, argument/resource `prepare`, and exact
  `invoke(PreparedToolCall, ..)`. `LegacyTool` remains as a conservative
  migration adapter, but cannot claim invocation-specific authority.

### Changed
- The direct loop is a versioned, checkpointable turn machine. Mutable
  planning/cache/activation/extension state is session-owned, and completed
  turns are saved before `TurnCompleted` becomes the durable terminal
  boundary.
- Conversation classification no longer determines provider-wire placement.
  Chronology is preserved within one conversation lane, complete parallel
  tool exchanges are atomic, and the active-turn continuation is required
  during compaction.
- Deterministic compaction is named `StructuralCompactor` and no longer
  fabricates semantic summaries. The asynchronous semantic-summary
  coordinator stores originals, calls a purpose-attributed model, and submits
  provenance-bearing history projection back through deterministic planning.
- Live ability routing derives a scoped view and activation epochs per
  session. `registry.search` stages an authorized, dependency-complete bundle
  transactionally and exposes it only after the canonical search result
  commits. Hosts attaching after session startup can inspect the current
  immutable epoch through `SessionHandle::activation_epoch`; live event
  subscriptions cover events emitted after subscription, while persisted
  journals remain authoritative for earlier events and delivery gaps.
- A valid persisted provider-cache baseline is now discarded and rebuilt when
  a resumed session changes model profile or provider cache contract; malformed
  or unknown cache-state schemas still fail closed. Ready terminal hooks can
  record an explicit cancellation without converting it into a failed turn,
  while pending hooks remain cancellation- and deadline-bounded.
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
- Typed active-turn steering: `SessionHandle::steer_current_turn` admits
  bounded FIFO `UserInput` against an optional expected `TurnId`, returns a
  stable `SteerReceipt`, and retains caller input in structured rejection.
  Inputs commit only at protected provider/tool boundaries and continue under
  the same logical turn; metadata-only `TurnSteerCommitted` and
  `TurnSteerDiscarded` events make disposition explicit without exposing raw
  content. Atomic drain-or-close prevents acceptance after a terminal fence,
  while cancellation discards before `TurnCompleted`.
- `GoalAdmissionGate` lets an interactive host defer idle-only automatic goal
  continuation while process-local real-user work is pending. It does not
  interrupt or pause an already-serving goal turn.
- Protected `CheckpointStore` records for accepted input, assembled model
  responses, pending approvals/interactions, raw tool outcomes, every
  canonical tool result, and terminal publication. Recovery never implicitly
  replays an indeterminate provider call or tool side effect.
- Invocation-specific prepared authority: canonical arguments, exact
  `SecurityResource`, typed permission bounds, scheduler effects, approval
  display, and a preparation fingerprint all describe the same immutable
  action. Edited approval input restarts preparation and authorization.
- Phase-specific ordered harness contracts for tool views, context, history
  projection, model options, tool output, and turn commits. Components receive
  immutable views, return explicit patches, and are bounded by turn
  cancellation/deadlines.
- Standard harness components: typed checkpointed todos with `PlanUpdated`,
  descriptor-first lazily verified skills, bounded memory contribution,
  session-private artifact offloading plus authorized paginated
  `artifact.read`, structured questionnaire interaction, and
  purpose-attributed semantic summary coordination. Persistent goals add
  descriptor-first `get_goal`/`create_goal`/`update_goal`, optimistic typed
  host controls, provider-evidence accounting, and a process-scoped conditional
  continuation controller with no synthetic user history.
- Lossless delegated task outcomes, including typed child `needs_input`
  handoff without a root broker, deterministic multi-child delivery, and
  follow-up reuse of the same child session.
- Agent delegation (`add-agent-delegation-runtime`): a neutral
  `DelegationCoordinator` spawns children as full runtime sessions built by a
  host `ChildRuntimeFactory`, with spawn/list/follow-up/wait/result/stop
  addressed by stable `ChildId`. Depth-one is enforced fail-closed (child
  views lose delegation tools; a child session cannot construct a
  coordinator), spawn/follow-up/stop pass the composed authorization path
  under the host-covered `agent.delegate` permission, per-parent and shared
  capacity are reject-by-default with an explicit queue policy. Hosts that do
  not provide both child stores retain process-ephemeral behavior. With both
  stores, bounded parent-owned records retain stable child/session identity,
  cumulative limits, policy fingerprints, and safe checkpoint watermarks;
  restored children remain dormant until an explicit new-turn `follow_up` or
  exact-checkpoint `resume`. Unsafe in-flight provider checkpoints fail closed,
  competing in-process coordinators are rejected, and host lifecycle leases
  remain the cross-process boundary. The provider-free `recover()` pass
  reconciles a protected child checkpoint newer than its parent catalog after
  abrupt process loss before child commands are accepted. Returned child
  questionnaires live in protected extension state and can be re-queued after
  restart without provider work. Attributed child
  lifecycle events (`ChildSpawned` … `ChildFailed`) join the event vocabulary
  (`SCHEMA_VERSION` is now `10`); the completed event carries the child's
  final result so coalescing can never drop it.
- Event schema v10 adds metadata-only `InternalTurnStarted` and
  durability-aligned `GoalUpdated` projections. Checkpoint schema/revision v2
  records attributed internal accepted input while retaining ordinary user
  turn compatibility.
- Event schema v11 adds metadata-only active-turn steering dispositions and a
  persisted steer identity floor. Existing snapshot reads default the new
  counter safely; consumers matching `RuntimeEvent` exhaustively must handle
  both disposition variants.
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
  `RequestSizer`/`CharRatioSizer`, structural compaction, cache-aware planning,
  and the immutable `ContextPlan` that is the exclusive source of provider
  messages/tools/reserves/counts. Includes the folded-in composable
  system-prompt mechanism (`SystemPromptBuilder` and its section types).
  Deterministic and network-free; semantic summarization is coordinated above
  it by the runtime harness.
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
