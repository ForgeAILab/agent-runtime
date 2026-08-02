# Migrating to the registry-driven context runtime

This release replaces direct prompt/history/all-tools request assembly with a
prepared **context plan**, and replaces process-global mutable registration
with sealed, run-scoped registry views. Both are breaking, and both are
deliberate: they are the changes that make token accounting authoritative and
capability visibility fail-closed.

Everything below is source-level. No consumer needs to change its behavior,
only how it declares what the runtime is allowed to send.

## 1. A model profile is now required

`RuntimeBuilder::build()` fails if it cannot resolve the target model's limits.

There is no default context window and no fallback estimate. A runtime that
cannot state its model's limits cannot enforce a budget, and a guessed window
is how uncounted context reaches a provider — so the builder refuses rather
than assume.

```rust
use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};

let runtime = RuntimeBuilder::new(ModelId::new("gpt-x"))
    .provider(provider)
    .model_profile(ResolvedModelProfile::explicit(
        "openai",
        ModelId::new("gpt-x"),
        ModelLimits::new(128_000, 128_000, 16_000),
    ))
    .build()?;
```

Hosts with more than a couple of models should register a catalog instead, and
let precedence resolve the profile:

```rust
use agent_runtime::core::catalog::{CatalogSource, LayeredModelCatalog, StaticSource};

let catalog = LayeredModelCatalog::new()
    .with_source(Arc::new(embedded_metadata))       // known-good defaults
    .with_source(Arc::new(provider_local_config));  // wins over embedded

let runtime = RuntimeBuilder::new(model)
    .provider(provider)
    .provider_name("openai")
    .model_catalog(Arc::new(catalog))
    .build()?;
```

Two resolution rules are worth knowing before you compose sources:

- Sources at the **same** precedence layer that disagree about a field fail
  with `SourceConflict`. Registration order never decides.
- A model no source declares fails with `UnknownModel`; a model with no
  resolvable limits fails with `MissingLimits`. Neither is silently defaulted.

## 2. Requests are derived from a `ContextPlan`

The loop no longer assembles a request from the system prompt, the full
history, and every registered tool. It builds versioned `ContextFragment`s,
compiles them into an immutable `ContextPlan`, and derives the request from
that plan.

The practical consequences for a host:

- Every context-bearing field of the request was counted against the model's
  budget first. A provider adapter may *serialize* a plan; it may not add to it.
- A turn that cannot fit fails **before** any network I/O, with a
  `BudgetFailure` event naming the responsible category — not a provider-side
  error after the request was already paid for.
- Sampling, reasoning, and `max_output_tokens` are request *options*, not
  context, and are still set on the builder as before.

Tune reserves and the capability sub-budget through `ContextPolicy`:

```rust
use agent_runtime::context::budget::ContextPolicy;

let runtime = RuntimeBuilder::new(model)
    .provider(provider)
    .model_profile(profile)
    .context_policy(
        ContextPolicy::new(RegistryRevision::new("host-policy-1"), 4_096, 0)
            .with_capability_budget(8_000),
    )
    .build()?;
```

Without a compactor attached, an over-budget plan fails rather than being
silently reduced. Attach `StructuralCompactor` to opt into deterministic
prior-turn reasoning removal, optional-fragment eviction, and unpaired
tool-result/history bounding under configured watermarks. It deliberately
does not invent semantic summaries or drop an old tool exchange merely to fit;
use the runtime-level `SemanticSummaryCoordinator` when stored originals and a
purpose-attributed summary model are available.

## 3. `agent-runtime-prompt` folded into `agent-runtime-context`

The standalone prompt crate is gone. The workspace deliberately has exactly one
token-budget and provider-context assembly path; two would drift.

- `agent_runtime_prompt::SystemPromptBuilder` → `agent_runtime::context::SystemPromptBuilder`
- `SystemPromptBuilder::into_fragments()` turns named sections into versioned
  `ContextFragment`s, so their tokens, revisions, priority, and cache class
  reach the authoritative plan.
- `TokenEstimator` / `CharBasedEstimator` are **removed**. Use
  `agent_runtime::context::sizing::{RequestSizer, CharRatioSizer}`, which sizes
  message framing, tool schemas, and tool calls rather than raw string length.
  A sizer reports its own revision and whether its counts are exact or
  estimated.

## 4. Generic registry primitives moved to the kernel

`Named`, `Registry<T>`, and `Sealed<T>` now live in `agent-runtime-registry`.
`agent-runtime-ability` re-exports them, so existing paths keep resolving.

If you implemented `Named` for a type you do not own (e.g. `Arc<dyn YourTrait>`),
that impl is now an orphan and will not compile. Wrap it in a local newtype —
the same fix the ability crate uses for `AbilityEntry`/`ToolEntry`.

## 5. Session snapshots carry run manifests

`SessionSnapshot` gained `manifests: Vec<TurnManifest>`, recording per-turn
registry, model, resolver, tokenizer, adapter, context, compaction, and cache
revisions and fingerprints.

This is a **migration, not a breaking read**: the field is `#[serde(default)]`,
so a snapshot persisted before manifests existed still loads with an empty
list.

Manifests store identifiers, classifications, content hashes, token counts, and
decisions — never raw fragment content, credentials, or secrets. There is no
constructor path that accepts raw content, so a sensitive tool result can only
ever be recorded as `{id, classification, hash, tokens}`.

For replay, `RunManifest::check_replay(&available)` requires every recorded
revision to be present and identical; a missing or changed revision fails
explicitly rather than substituting what happens to be installed. A host that
wants to proceed anyway must opt in explicitly via
`check_replay_as(&available, ReplayMode::LabeledNonEquivalent)`.

## 6. Event schema version 8

`SCHEMA_VERSION` is now `9`. The current vocabulary includes registry sealing,
scoped-view derivation, model resolution, capability retrieval and activation,
context planning and compaction, cache-plan changes, budget failures,
attempt-scoped speculative output, metadata-only interaction lifecycle,
lossless child `needs_input`, and durability-aligned `PlanUpdated`.

Committed v5 through v9 fixtures guard the compatible wire representations.
Pre-v5 output deltas are intentionally not accepted because they lack the
request/attempt identity needed to discard retry output safely. A consumer
that matches exhaustively on `RuntimeEvent` must handle the new variants.

## 7. Turns are explicit and interruption is turn-local

`send` and `run` now return a structured handle:

```rust
let turn = session.send(UserInput::text("inspect the project"))?;
turn.interrupt(CancelReason::UserRequested); // this turn only
turn.completed().await;
```

`SessionHandle::interrupt_current_turn` is the equivalent session-addressed
operation. `cancel_session` permanently cancels the session and is appropriate
for shutdown or revocation; the old `cancel` name remains a terminal
compatibility alias. Submission after shutdown returns `RuntimeError` instead
of minting an unusable turn ID.

## 8. Exact recovery uses a protected checkpoint store

`SessionStore` remains the host-policy view of completed session state.
`CheckpointStore` is a separate protected record of exact, versioned mid-turn
state. Inject both when crash recovery is required:

```rust
let runtime = RuntimeBuilder::new(model)
    .provider(provider)
    .model_profile(profile)
    .session_store(session_store)
    .checkpoint_store(checkpoint_store)
    .build()?;
```

Checkpoints may contain prepared actions, pending interaction content, or raw
tool outcomes and therefore require stronger storage policy than an ordinary
redacted journal. Recovery never reconstructs these values from observability
events and never implicitly replays an indeterminate provider call or tool
side effect.

`SessionSnapshot` now carries namespaced `extension_state`. Each value declares
its schema revision and sensitivity. Unknown component state can be preserved;
an incompatible revision fails explicitly.

## 9. Tools prepare exact authority before approval

New tools implement `Tool` directly:

```rust
#[async_trait]
impl Tool for EditTool {
    fn spec(&self) -> ToolSpec { /* conservative upper bound */ }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        // Canonicalize the exact path/resource and required permissions.
        # todo!()
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        # todo!()
    }
}
```

The runtime verifies that the prepared permission set is covered by
`ToolSpec::permission_upper_bound`, authorizes and displays that exact action,
and invokes the same fingerprinted object. If approval edits arguments, schema
validation, preparation, authorization, and approval run again. `LegacyTool`
is available during migration but maps unspecified reads/writes/process/network
effects conservatively.

## 10. Live abilities and harness components are opt-in

Call `RuntimeBuilder::live_ability_routing()` (or register an ability/descriptor
override) to replace the fixed all-tools-visible request surface with
session-scoped retrieval and activation. The protected `registry.search`
bootstrap is always retained. Its results are authorized, dependency-complete,
and staged until the canonical tool result commits.

Reusable behavior is composed with phase-specific builder methods such as
`context_contributor`, `model_interceptor`, `tool_output_processor`, and
`turn_commit_hook`. Components have stable IDs/revisions and ordering
constraints, receive immutable views, and return explicit patches. Do not port
middleware that mutates shared request or session dictionaries.

The standard questionnaire is activated only when host interaction is ready.
Large tool output can be moved to a session-private `ArtifactStore` and read
back in bounded pages through `artifact.read`. Todos, memory, artifacts, and
semantic summaries remain generic mechanism; hosts still own their sources,
trust policy, persistence implementations, and presentation.

## 11. Durable child sessions require both stores

Delegation remains host-composed. A `ChildRuntimeFactory` that exposes both a
`SessionStore` and a protected `CheckpointStore` creates durable child
sessions; omitting either retains the compatible process-ephemeral behavior.
The coordinator stores only bounded child identity, policy, status, limits,
and checkpoint-watermark metadata in the parent session's redaction-safe
`extension_state`. Exact task content remains in the child snapshot and
checkpoint.

After the parent is restored, construct `DelegationCoordinator`, then call
`recover().await` before listing or accepting child operations. Recovery
reconciles a catalog that may lag its protected child checkpoint after abrupt
process loss, restores returned interactions, and constructs no providers.
Then use:

- `follow_up(child, input)` for a new task turn on an idle retained child;
- `resume(child)` only for an interrupted exact checkpoint;
- `spawn(spec)` only when a new child identity is intended.

These operations never fall back to one another. Resume refuses missing,
terminal, regressed, policy-incompatible, or unsafe checkpoints. In
particular, `TurnState::CallingModel` is non-resumable because process loss
cannot prove whether the provider completed the request; replay could duplicate
provider work. Hosts must hold an exclusive lifecycle lease for the parent
session across processes. The runtime additionally rejects a second
coordinator for the same live `SessionHandle`.

`RuntimeEvent` schema v9 adds child `recovered`, `interrupted`, and
`resume_started` progress phases. Consumers matching `ChildPhase`
exhaustively must handle them. The v5-v9 golden fixtures retain older readable
wire forms.

A child questionnaire returned to its parent is saved as sensitive
session-extension state in the child's protected checkpoint. The same
`recover().await` pass reloads and re-queues the exact attributed request;
ordinary redacted snapshots may omit it. The narrower
`recover_returned_interactions().await` method remains available to hosts that
have already reconciled checkpoint metadata separately.

## 12. Persistent goals and internal turns are opt-in

Register the three standard goal tools and the same `GoalComponent` as a
context contributor, model interceptor, tool-output processor, and turn-commit
hook. Use `SessionHandle::control_goal` for typed host mutations and
`SessionHandle::goal` for the bounded projection. A host that wants automatic
work may attach one `GoalController`; its continuations are admitted only by
`try_send_internal_if_idle`, carry an `InternalTurnSource`, and append no
user-role message.

Goal token accounting charges provider-reported uncached input plus output and
labels missing evidence unknown. A budgeted goal stops when required evidence
is unavailable; observed budgets are post-response limits and may overshoot by
one request. Controller lifetime is process-scoped. Restoring an active goal
does not create work until a host explicitly attaches a controller, and no
daemon, fork inheritance, or remote scheduler is implied.

Event consumers must handle schema-v10 `InternalTurnStarted` and `GoalUpdated`.
Protected checkpoint implementations must accept `TurnState::InternalAccepted`
as a revision-zero successor to a terminal checkpoint.

## 13. Active-turn steering has explicit disposition

Hosts that previously called `send` while a turn was serving must now choose
whether the input is a future whole turn or a correction to current work. Use
`steer_current_turn` for the latter:

```rust
match session.steer_current_turn(Some(serving_turn.id()), input) {
    Ok(receipt) => pending.insert(receipt.id, local_draft),
    Err(rejection) => restore(rejection.input),
}
```

Do not append canonical UI history at acceptance. Wait for schema-v11
`TurnSteerCommitted`; remove or restore local state on
`TurnSteerDiscarded`. Raw steer content is deliberately absent from both
events. A `TurnMismatch` rejection reports the current identity and whether a
single retry is eligible. `NoActiveTurn`, `NonSteerable`, close-fence, size,
depth, cumulative-byte, and shutdown outcomes remain distinct.

Steering is bounded by `LoopConfig::steer_limits`. Safe-boundary ordering is
tool result, generic host injection, then real-user steer. Ordinary and
attributed internal provider turns are eligible; local-tool-only work and
returned interactions are not. Protected checkpoint stores contain a steer
only after its commit boundary.

Automatic goal controllers may receive a host-owned `GoalAdmissionGate` via
`GoalControllerConfig::with_admission_gate`. Disable it while an interactive
client owns pending real-user work, admit that work at the terminal boundary,
then re-enable it. The gate affects only future idle admission.

## Checklist

- [ ] Declare a `model_profile` or `model_catalog` on every `RuntimeBuilder`.
- [ ] Replace `agent_runtime_prompt` imports with `agent_runtime::context`.
- [ ] Replace `TokenEstimator`/`CharBasedEstimator` with a `RequestSizer`.
- [ ] Wrap any foreign-type `Named` impl in a local newtype.
- [ ] Handle the new `RuntimeEvent` variants if you match exhaustively.
- [ ] Distinguish future whole-turn input from active-turn steering and retain
      local drafts until a committed/discarded disposition.
- [ ] Decide a `ContextPolicy` reserve, and whether to attach a compactor.
- [ ] Update `send`/`run` call sites to handle `Result<TurnHandle, _>`.
- [ ] Use turn interruption for normal user interrupts; reserve
      `cancel_session` for terminal teardown.
- [ ] Migrate authority-bearing tools to `prepare` + exact `invoke`.
- [ ] Provide a protected `CheckpointStore` if mid-turn recovery is required.
- [ ] Decide whether the host opts into live ability routing and which harness
      components/sources it trusts.
- [ ] Decide whether delegated children are durable; if so, provide both
      stores and an exclusive cross-process parent-session lifecycle lease.
- [ ] Keep child follow-up and exact interrupted-turn resume as separate user
      operations; never replace a failed lookup with spawn.
