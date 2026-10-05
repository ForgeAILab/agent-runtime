# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project uses
pre-1.0 semantic versioning: while the major version is `0`, minor releases may
contain breaking changes and are coordinated with consumer proposals.

## [Unreleased]

### Breaking

- U7/U8 (explicit session and timeline lifecycle). Full consumer-break list:
  - `COMMAND_SCHEMA_VERSION` is 2. `StartSession` now requires explicit `mode`
    on the wire; `initial_history` becomes `seed`. Schema 1/unknown legacy
    fields are rejected. Use `create(id, seed)`, `resume(id)` or
    `ephemeral(history)`. `StartSession::with_id` is removed, so a former
    `new().with_id(id)` resume fails to compile; `new()` is an unnamed create.
    `StartSession::from_json(value)` decodes wire payloads and rejects another
    schema version with a typed `Config` error (plain serde decoding still
    fails with a serde error). Resume with any seed returns Conflict; missing
    resume state returns NotFound; create over an ordinary snapshot or protected
    checkpoint returns Conflict. Resume identity floors require resume mode.
    Checkpoint recovery policies remain.
  - Ephemeral disables session/checkpoint loading and writes even when stores
    were configured. Its LCM runs on a volatile in-memory timeline and never
    claims or writes the host timeline bound to its id. `SessionHandle::resumed()`
    reports actual persisted loading. Nyx's per-call history reconstruction maps
    to `ephemeral`.
  - `LcmWriter::claim` is defaulted. The default records nothing: it returns
    `Claimed` for an empty timeline and `LcmClaimResult::Unsupported` for a
    populated one. With it, LCM stays usable: sessions run normally, a session
    resuming its own snapshot continues unfenced, and a fresh binding of a
    populated timeline forks to the resolver's `new_timeline` with a summary
    seed (typed `ForkRequired` without that hook). Explicit Adopt of a fresh
    binding on such a store returns `ForkRequired`. Claim-aware stores must update ownership
    atomically, bump their revision on every ownership change, and reject a
    mutation whose `LcmView::owner()` fence is not the recorded owner (an
    unfenced write to an owned timeline included) with `TimelineOwned`.
    `LcmView` gains `with_owner`/`owner`; the runtime fences every write.
  - Fresh owners cannot silently bind populated timelines; select explicit
    Adopt, Fork or Retire. Adopt claims the next generation before it reads, and
    Adopt with a non-empty seed returns Conflict. A pre-U7 session on an
    unclaimed timeline fails resume with `TimelineOwned { owner: None, .. }`;
    `resume(id).with_lcm_policy(Adopt)` claims generation 1 and continues.
    Default resolver replacement and continuation hooks fail closed; hosts
    opting into rotations must implement durable, idempotent, authorized
    bindings. The Fork and Retire policies durably rebind through `new_timeline`
    before validating the replacement. NewTimeline requires an empty store. An
    unauthorized LCM view now fails at session construction, not first use.
  - The U7 Fork summary is capped by the new public
    `LcmCoordinatorPolicy::fork_summary_max_chars` (default
    `DEFAULT_FORK_SUMMARY_MAX_CHARS`, 16 384), keeps the newest tail, omits
    Secret-classified sources and passes the content guard. Exhaustive
    `LcmCoordinatorPolicy` literals must add the field.
  - `agent-runtime-lcm`: the `testing` module is now `memory` and always
    compiled, and the `test-support` feature is removed; import
    `agent_runtime_lcm::InMemoryLcmStore` or `memory::InMemoryLcmStore`. It
    remains a volatile reference store, not a persistence backend.
  - `LcmError` adds TimelineOwned, LcmDivergence and ForkRequired; update exhaustive
    matches. `RuntimeError` adds optional typed `lcm: Option<Box<LcmFailure>>`; add it to
    Rust literals/destructures; `lcm_failure()` borrows the evidence. RangeOverlap/EntryConflict now retain Conflict kind
    through provider admission, with typed evidence rather than Config strings.
    Below-frontier reconcile fails without truncation. Serialized errors remain
    additive and legacy/future optional LCM evidence is readable.
  - `Runtime::fork_session(ForkSession)` accepts Summary/FromIndex/Empty and
    NewTimeline/Continue. It requires idle durable sources and supersedes parents.
    The resolver hook, replacement emptiness, Continue claim support and the seed
    are validated before any pending intent is written. A later failure rolls the
    intent back while the successor owns nothing durable; otherwise retry the same
    request. `Runtime::abort_fork(parent)` clears an intent a crash left behind
    under the same rule. Default resume refuses pending or superseded parents.
    With LCM, Continue accepts only `Empty` or `FromIndex(0)` (its adopted history);
    a Summary or a later suffix returns Conflict. Use NewTimeline plus Summary for
    bounded topic rotation. Smith `/new` maps to an Empty fork; Forge
    genesis/handoff maps to a Summary/NewTimeline fork.
  - Protected adapters that restrict their first save to admission states must
    additionally accept `TurnCheckpoint::is_session_boundary()` for exact idle
    seed protection. The constructor uses existing Terminal wire data; checkpoint
    schema/transition versions and store trait signatures are unchanged. Ordinary
    Sensitive-state redaction and exact protected retention remain mandatory.
    Compatible exact immutable seeds/pending intents override even newer ordinary
    redacted values; generated U7 Fork seeds are protected before the first turn,
    on the create and the resume path.
    Persisted LCM state adds a defaulted claim generation; U6 tunable rebuilds remain.
  - Forge can remove V149 timeline retirement and stale-LCM-state deletion for
    tunable changes after adopting claims and authorized session/topic forks.
    See `docs/migrations/session-timeline-lifecycle.md` for migration and U9/U10.

- U6 (LCM working sets, one sizer, provider summaries). Consumer-break list:
  - `LCM_ALGORITHM_REVISION` is now `agent-runtime-lcm-2`; persisted LCM state
    from `-1` rebuilds its derived metadata once on resume (see below).
  - `LcmEscalationPolicy::target_tokens` is replaced by
    `leaf_source_target_tokens`, `summary_max_ratio` (default 0.25) and
    `min_reclaim_ratio` (default 0.5). Update struct literals or use
    `..Default::default()`. The struct lost `Eq` (ratio fields are `f64`);
    remove downstream `Eq` bounds. Serialized escalation settings have no
    serde default or alias: old JSON without the new fields fails to
    deserialize and must be migrated. `policy_revision` defaults to
    `lcm-summary-policy-2`.
  - Ratio enforcement: model summaries must meet `summary_max_ratio` and
    `min_reclaim_ratio` at every escalation level, so fewer model outputs are
    accepted than before. The deterministic fallback keeps the base target,
    `min(deterministic_token_cap, source_tokens - 1)`, and is not ratio-capped.
    At the defaults the 0.25 ratio is stricter than the 0.5 reclaim floor, so
    `min_reclaim_ratio` only binds when a host raises `summary_max_ratio` above
    `1 - min_reclaim_ratio`.
  - A leaf the deterministic fallback cannot strictly shrink (one short turn,
    smaller than the fallback's own framing) is widened through the next turn
    instead of wedging compaction (on the hard path up to the active turn;
    idle compaction never widens into the retained recent entries). A summary
    that still cannot fit under hard
    pressure now ends as the structured `cannot_fit` limit error (with
    `summary_attempts` and summary usage metadata) instead of an unstructured
    "cannot fit after escalation" limit.
  - `LcmSummaryAttemptOutcome::NonShrinking` is now reported before
    `OverBudget`: under ratio caps every non-shrinking output is also over
    budget, so the old order made `NonShrinking` unreachable.
  - `LcmPressurePolicy::max_rounds` defaults to 0 (was 3) and `validate`
    accepts 0, which derives the bound. The one shared formula is
    `derive_hard_rounds` (overage above the soft threshold divided by
    `expected_leaf_reclaim_tokens`, plus two), fixed once per admission epoch
    and never above `MAX_DERIVED_HARD_ROUNDS` (64). Positive values remain
    explicit limits. The structured `cannot_fit` error's `max_rounds` now
    reports the effective bound, not the configured 0. Added
    `decide_pressure_with_reclaim`, `derive_hard_rounds`,
    `expected_leaf_reclaim_tokens`, `MAX_DERIVED_HARD_ROUNDS` and
    `LcmEscalationPolicy::expected_reclaim_tokens`.
  - Default sizer: `LcmCoordinatorPolicy::default().sizer` is now the shared
    `RequestSizerAdapter` over the default request sizer, and
    `RuntimeBuilder::lcm` replaces exactly that default with the planner's
    `RequestSizer`. A host-supplied `LcmSizer` is kept as given; it is never
    discarded. With a host sizer and a `WorkingSetPolicy`, measured overhead is
    the planner total minus the host-sized history, so the host sizer should
    approximate the request sizer (or omit it to share). The LCM
    `CharRatioSizer` now counts all content parts (revision
    `char-ratio-content-2`), and non-default context `CharRatioSizer` settings
    get distinct revisions; the default revision is unchanged.
  - `RuntimeBuilder::lcm` registers the coordinator's history projector and
    turn-commit hook at `build()` instead of at call time, because the
    coordinator is rebuilt with the final request sizer and working set.
    Execution order is unchanged: the sealed pipeline orders components by
    declared constraints and then component id, never by registration order.
  - `LcmLifecycleReason` adds `OverheadExceedsTarget`. Exhaustive matches need
    the new arm, and readers of stored events must accept the new
    `overhead_exceeds_target` value. `LcmLifecycleMetadata` adds optional
    `overhead_tokens` and `target_tokens` (serde default, omitted when `None`,
    so stored events still read); struct literals without `..Default::default()`
    and exhaustive destructuring must include them.
  - Revision bump and tuning: LCM tunable policy, sizer, model and algorithm
    changes rebuild derived state from authorized active nodes instead of
    failing with a conflict. Timeline, binding, store schema, classifier and
    guard mismatches remain fail-closed. A protected pending response from
    older tunables is accepted only after source-identity validation against
    canonical history; one from current tunables is validated by recomputing
    its exact plan. No store migration or stale-state deletion is needed.
  - Persistence: private LCM state adds `fixed_overhead_tokens` and
    `hard_round_limit` (both `serde(default)`). Idle LCM batches write the
    redaction-safe extension namespace `runtime.lcm.idle_boundary` (revision
    `lcm-idle-boundary-1`, a fingerprint of the predecessor usage ledger) and
    refresh the exact terminal checkpoint; the namespace is kept, not removed.
    `merge_terminal_checkpoint_snapshot` now accepts a usage ledger skew when
    that marker proves the protected ledger extends the ordinary one with
    semantic-summary records only, and takes the protected LCM state when the
    ordinary copy has a different revision but the same identity fields.
    A failed exact idle checkpoint rolls back the batch's extension state,
    usage records and marker; once the exact checkpoint is saved, an ordinary
    save failure keeps the progress (the checkpoint is its saved counterpart).
- U6 additive API: `LcmCoordinator::with_summary_policy`,
  `ContextPlanner::with_input_cap` and `RunPlanner::with_input_cap`,
  `lcm::RequestSizerAdapter`, `lcm::summarize::render_summary_source`,
  `lcm::LCM_SUMMARY_PURPOSE` (now defined in the LCM crate and re-exported by
  `harness`), `LcmEscalatingSummarizer::can_always_summarize`, and the
  derived-rounds helpers listed above.
- Add `WorkingSetPolicy { target_tokens, hard_tokens }` through
  `RuntimeBuilder::working_set_policy`, measured `fixed_overhead_tokens`, and
  opt-in `soft_on_turn_boundary(bool)`. The planner enforces
  `min(resolved input, hard)`; pressure uses `target - measured overhead`, but
  never less than 25% of the target. When that floor applies the admission
  pass emits `LcmLifecycle { reason: OverheadExceedsTarget }` with the clamped
  thresholds and the new optional `LcmLifecycleMetadata::overhead_tokens`
  (measured overhead) and `target_tokens` (working-set target) fields. A persisted overhead is trusted only until the next plan
  re-measures it. Without a working set, existing budgets remain.
- Add feature `provider-summary` and `ProviderLcmSummaryModel<P: Provider>`
  (re-exported through `harness` and `lcm`) with host-supplied instructions,
  planner-admitted tool-aware map-reduce and aggregate usage.
  `ProviderSummaryLimits` sets a per-call deadline (default 60 s) and an
  operation ceiling (default 10 min); the operation budget is the per-call
  deadline times the calls planned so far, up to the ceiling.
  `with_operation_scope` supplies the host's cancellation and deadline per
  summary operation (`ProviderSummaryScope`), so cancelling one operation never
  poisons the next; `with_clock` replaces the clock. Calls are attributed
  `IdleCompaction` only for idle-boundary summaries, `Ordinary` otherwise.
  Deterministic fallback preserves bounded tool arguments and results. Forge
  can replace its private summarizer and tuning-state deletion; timeline
  ownership/V149 retirement still depends on U7.

See [`docs/migration-0.1.md`](docs/migration-0.1.md) for the full migration.

- `RuntimeError` adds `class`, `retry_after_ms`, `limit_resets_at_ms`, and
  `credential_recovery`. Rust struct literals must supply the new fields;
  exhaustive destructuring must include them or use `..`. Existing JSON stays
  readable in both directions, and context-overflow counts are `Option<u32>`.
  `FailureStage` and `FailureComponent` are non-exhaustive, and
  `FailureComponent` adds `Unknown`, so enum matches need a wildcard arm.
  Actual consumer builds remain a release gate.
- `CachePlan` and `RuntimeEvent::CachePlanChanged` add an optional
  `first_changed_fragment`. Rust struct literals must supply it (`None` is
  fine) and field-exact patterns must bind it or use `..`. JSON without the
  field stays readable, and it is omitted when unknown. The pre-hook tool
  error text now says "before tool normalization" instead of "before tool
  preparation".
- Session manifest retention is now a finite recent window (default 32
  planned steps). `snapshot.manifests` remains an ordered `Vec<TurnManifest>`;
  `RuntimeBuilder::manifest_window(NonZeroUsize)` and
  `SessionHandle::recent_manifests()` expose configuration and the same recent
  suffix. Hosts requiring lifetime audit/replay must archive records. New
  protected checkpoints omit diagnostic manifests and preserve exact execution
  state using a RedactionSafe planned-step boundary record. Legacy JSON remains
  readable; older binaries can reject a new terminal checkpoint paired with
  nonempty ordinary diagnostics. Roll-forward repairs an opaque pre-U3
  under-count when the longer legacy list still proves it, and nonterminal
  crash recovery retains diagnostics from a lagging ordinary snapshot. The
  namespace is exported as `runtime::MANIFEST_BOUNDARY_NAMESPACE`; additive
  fields in its value remain readable. See the retention and downgrade
  migration.
- The removed session-scoped rolling-summary contract is replaced by Lossless
  Context Memory (LCM). Hosts bind an authorized logical timeline and compose
  `LcmCoordinator`; when `.lcm` is configured, resume automatically imports
  valid schema-v1 state only when the coordinator has the legacy protected
  `ArtifactStore` and the runtime has a durable `SessionStore`, validates
  canonical history/artifact/binding identity, and persists the replacement
  before accepting turns. There is no public/manual restore alias or second
  semantic-compaction path.
- `RunManifest` is now manifest schema v2 with redaction-safe lossless LCM
  records and fingerprint semantics. `RunManifest::check_replay_as` returns a
  typed `ReplayMismatch` report covering revision, lossless-record, and
  assembled-context differences, even for a labeled non-equivalent replay;
  strict equivalent replay rejects every such difference. LCM equivalent
  replay must use `check_replay_with_lossless_context` or
  `check_replay_as_with_lossless_context` with the restored lossless records
  and assembled context fingerprint; the revision-only entry points do not
  establish LCM equivalence.
- The old idle semantic-summary API is replaced by
  `SessionHandle::try_idle_compaction()`, which returns metadata only through
  `IdleCompactionAdmission::Accepted { changed, fallback_reason, usage }`
  (or `Busy`/`Shutdown`). No summary body crosses the runtime facade.
- LCM integrations may attach a summary-body `ContentGuard`; its ID/revision
  is a strict checkpoint compatibility boundary and guarded historical state
  fails closed if the guard is removed. `SessionHandle::expand_lcm` provides
  bounded, read-only inspection through the coordinator's host-authorized
  timeline binding and emits metadata-only lifecycle events.

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
- Event `SCHEMA_VERSION` is now `15`. Since the registry-driven v2 baseline,
  tool-call argument projection, delegation, attempt-scoped streaming,
  metadata-only host interaction, lossless child `needs_input`, and
  durability-aligned `PlanUpdated`, durable-child recovery/resume, and
  attempt-attributed prompt-cache evidence each advanced the vocabulary.
  Provider `CacheObservation` read/write fields are now independently
  presence-aware, canonical observations carry request/attempt/cache-plan
  attribution, and `CacheStateChanged` reports the comparable expectation,
  provider observation, saturating shortfall, and confidence. Exhaustive
  provider/event matches must handle the new shapes. Legacy numeric cache
  observations remain readable without fabricating attribution or a miss.
  Version 14 adds the canonical cache-operation lifecycle; version 15 adds the
  metadata-only `LcmLifecycle` event for pressure, admission, escalation,
  commits, fallback, import, expansion, and failure. Golden fixtures retain
  the compatible v5-v11 and v13-v15 wire forms; pre-v5 unattributed output
  deltas are intentionally rejected.
- `SessionHandle::send` and `run` return `Result<TurnHandle, RuntimeError>`.
  `TurnHandle` owns turn-local interruption and completion; use
  `cancel_session` only for terminal session teardown. The compatibility
  `cancel` alias retains terminal semantics.
- `Tool` now separates `spec`, argument/resource `prepare`, and exact
  `invoke(PreparedToolCall, ..)`. `LegacyTool` remains as a conservative
  migration adapter, but cannot claim invocation-specific authority.

### Changed

- History and LCM accounting do less repeated work per provider call.
  Private history captures share immutable generations instead of copying
  every message (about 4x fewer message copies per call in the benchmark),
  and LCM keeps authorized, revision-bound, process-local token totals so an
  unchanged or appended history only accounts for the new entries. Public
  owned history, request and checkpoint types, serde formats, persisted LCM
  revisions and fingerprints are unchanged. A DAG revision change between
  checkpoint and pressure accounting now fails the turn with the existing
  revision-conflict error instead of accounting against the newer DAG.
  Truncating the stored tail now drops the cached totals, so a later append
  cannot reuse counts for removed entries.
- Integration tests build as fewer binaries: the 20 `agent-runtime` test
  targets are one `integration` target and the four testkit conformance
  targets are one `conformance` target. Every case is kept; the consumer
  targets `consumer_open_forge`, `consumer_smith` and `consumer_nyx` keep their
  names. Run a moved case with `--test integration <module>::` or
  `--test conformance <module>::`.
- `ProviderAttemptFinished` now carries optional zero-based attempt position,
  configured attempt total, and the effective delay before an actually
  admitted retry. Legacy journals deserialize the fields as absent, while
  consumers can distinguish retryable classification from a scheduled retry
  without duplicating runtime backoff or deadline policy.
- The direct loop is a versioned, checkpointable turn machine. Mutable
  planning/cache/activation/extension state is session-owned, and completed
  turns are saved before `TurnCompleted` becomes the durable terminal
  boundary.
- Conversation classification no longer determines provider-wire placement.
  Chronology is preserved within one conversation lane, complete parallel
  tool exchanges are atomic, and the active-turn continuation is required
  during compaction.
- Deterministic structural compaction remains network-free. Persisted semantic
  history compaction now uses LCM's immutable timeline and transactional
  hierarchical summary DAG; committed active nodes are projected back through
  the authoritative context planner with lossless source pointers.
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

- Cache diagnostics: `first_changed_fragment` names the first plan segment
  (in plan-segment order) that differs from the committed predecessor,
  including a removed segment, without changing fingerprints, cache
  identities or provider requests.
- `Tool::normalize_arguments`: an identity-default hook that runs before full
  schema validation in both executor paths and on approval edits, so a host
  can accept a model's wrapped arguments while advertising the canonical
  schema. Edited arguments are re-normalized and re-authorized; checkpointed
  prepared actions resume without normalization. For opt-in tools the
  execution-facing call carries the normalized arguments; history keeps the
  raw ones.
- The sealed tool registry reuses each tool's compiled validator instead of
  recompiling it on every call.

- Neutral `FailureClass`, `FailureStage`, and fixed `FailureComponent`
  evidence on runtime errors. Legacy, future, and malformed classification
  evidence cannot make a record unreadable; stages/components have `Unknown`
  fallbacks, invalid optional timing/recovery evidence becomes absent, and zero
  remains explicit. Delegated child failures retain all four evidence fields.
  Provider conversion preserves timing and credential recovery, while private
  request construction retains planner/harness evidence and LCM typed or
  genuine concurrent-revision classifications with the existing coarse kind,
  retryability, error channel, and terminal outcome.
  Classification and timing hints never admit a retry. Frozen error/event/
  child-failure fixtures and all three neutral consumer suites cover the
  contract.
- External agent capability injection: `ExternalCapabilities` (skills, MCP
  servers, tool allowlist, runtime tools) attached with
  `RuntimeBuilder::external_capabilities` and delivered on every
  `ExternalTurnRequest`. Feature `external-agent-bridge` serves the session's
  runtime tools to the CLI as a turn-scoped loopback MCP server whose calls go
  through ordinary authorization and approval.
- `agent-runtime-agent-cli`: `ClaudeCodeBackend` (`claude` feature) and
  `CodexBackend` (`codex` feature) realize those capabilities with each CLI's
  launch-scoped flags, never touching global CLI config. See
  `docs/external-agents.md`.
- Responses adapters now forward bounded model-advertised reasoning efforts,
  including Codex `xhigh`, `max`, and `ultra`, instead of imposing the legacy
  `low`/`medium`/`high` allowlist.
- `agent-runtime-provider/command-provider`, an opt-in process-bounded
  implementation of the canonical provider contract for trusted,
  consumer-owned model CLI codecs. It provides exact capability validation,
  shell-free direct argv, canonical executable/cwd resolution, a cleared and
  explicit child environment, redacted config/attempt diagnostics, bounded
  stdin/stdout/stderr, typed machine-output decoding, explicit compatibility
  probing, and cancellation/deadline/drop-safe process-group cleanup. Runtime
  retains canonical history, tools/MCP, approvals, retries, and events; named
  Codex/Claude/other adapters and Smith configuration remain consumer work.
- `agent-runtime-cli`, an isolated reference host that installs the
  `agent-runtime run` command for one in-process turn. It supports the
  first-party provider adapters, explicit model limits, positional or piped
  prompts, credential-by-environment configuration, assistant-text or
  canonical JSONL streaming, exact-origin HTTPS transport with restricted
  address and redirect denial, Ctrl-C interruption, and stable process exits.
  It now accepts one explicit strict version-1 TOML file for existing run
  defaults and trusted local stdio MCP definitions. Static inspection,
  per-run server consent, exact per-run tool approval, minimal child
  environment mapping, conservative tool authority, optional/required failure
  handling, and bounded connection shutdown keep a file from authorizing its
  own process or tools. The CLI package declares Rust 1.88 for the official MCP
  SDK while embeddable packages retain Rust 1.86. Interactive chat,
  persistence, remote MCP/OAuth, ambient config discovery, daemons, and
  consumer policy remain outside this command.
- `agent-runtime-lcm`, a store- and provider-neutral package for immutable
  logical timelines, transactional leaf/condensed summary DAGs, deterministic
  tool-exchange-safe planning, bounded expansion, soft/hard pressure decisions,
  and three-stage convergence-guaranteed summarization. The runtime facade
  re-exports it as `agent_runtime::lcm`.
- Native stateless OpenAI Responses provider, first fixture-verified against
  xAI Grok: bounded input-item encoding, session-keyed implicit prompt caching,
  encrypted reasoning replay, function-call streaming, structured output,
  usage/cache normalization, terminal fencing, and renewable bearer
  credentials. Provider-side storage, background responses, and hosted tools
  remain rejected before I/O.
- Native Google Gemini Interactions adapter over injected `HttpTransport`, with
  stateless `store=false` history replay, renewable `x-goog-api-key`
  credentials, bounded native request/stream types, function and multimodal
  result translation, structured output, usage/cache normalization, and exact
  signed-thought continuation. Vertex AI, hosted tools, provider storage, and
  live-network tests remain outside the shared runtime.
- Host-injected renewable provider credentials through
  `ProviderCredentialSource`, with optional lease expiry, opaque exact-revision
  invalidation, static API-key compatibility, cancellation/deadline bounds,
  and one attempt-visible pre-output authentication recovery replay. OAuth
  ceremony and credential persistence remain host policy, and credential
  material is excluded from runtime observability and persistence.
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
  `artifact.read`, structured questionnaire interaction, and Lossless Context
  Memory coordination. Persistent goals add
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
  and tool calls), sheds prior-turn unsigned reasoning when the next user turn
  starts, and retains signed provider continuation—including signature-only
  blocks—across serialization and replay. The OpenAI-compatible adapter serializes non-redacted reasoning as
  `reasoning_content` on assistant wire messages — required by
  OpenAI-compatible thinking models (e.g. Z.AI GLM) during tool-call
  continuations — and never serializes redacted reasoning. Compaction strips
  prior-turn unsigned reasoning as its cheapest first stage but never
  truncates signed continuation content.
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
- LCM hard compaction no longer wedges a session behind one oversized agentic
  turn. Leaf planning only cuts at user boundaries, so a single turn whose
  tool loop outgrew `leaf_target_tokens` (many tool rounds, no user message
  inside) could never be planned: every admission backed up to the frontier
  and failed with "LCM context cannot fit after bounded hard compaction",
  forever. The same wedge hit a turn that reached into the
  `retain_recent_entries` tail. When the oldest raw turn cannot be cut, it is
  now taken whole as one oversized leaf.
- A tool call that breaks a registered tool's schema is now answered instead
  of fatal. The stream boundary rejected it as a malformed stream, which
  failed the whole turn, while the executor -- which already validates the
  same arguments -- would have returned a canonical tool error naming the
  offending property. A model that makes the mistake deterministically (an
  extra discriminator property on every attempt, say) could never get past
  its first tool call. Arguments that are not an object at all, where the
  schema requires one, remain a malformed stream.
- Shedding a prior turn's unsigned reasoning is now a projection onto the
  model-facing request instead of a rewrite of canonical history. Rewriting
  history between turns diverged it from the immutable LCM entries and the
  protected checkpoint that fingerprinted them, so the second turn of any
  session whose provider streams unsigned reasoning (every OpenAI-compatible
  thinking endpoint, z.ai GLM among them) failed closed with "LCM canonical
  history no longer matches its protected checkpoint" and stayed wedged.
  Messages are projected one for one, so an assistant message that carried
  nothing but shed reasoning arrives empty; the OpenAI-compatible wire drops
  it rather than sending a blank assistant line.
- The release lock now resolves `rustls` 0.23.45 and `rustls-webpki` 0.103.15,
  clearing RUSTSEC-2026-0285 before publication.
- `ContextPlanned::input_budget_tokens` now reports the enforced input budget
  it was always documented as, instead of the counted consumption (which
  moved to the new `input_tokens` field).

### Provenance
- Reusable provider, agent-loop, and tool mechanisms were adapted from the Nyx
  project. See `PROVENANCE.md` for the donor revision and path mappings.
