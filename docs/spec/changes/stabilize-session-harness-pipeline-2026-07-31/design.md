## Context

The intended architecture already separates a neutral runtime from
product-owned policy, but the live path currently concentrates mutable state in
the shared `Driver`:

```text
RuntimeShared
  └── Driver
      ├── RunPlanner(previous_cache)
      ├── SemanticCompactor(last_outcome)
      ├── sealed static ToolRegistry
      └── direct provider/tool loop
          └── many SessionHandle values
```

That shape contradicts the `RunPlanner` documentation saying it is one per
session. `RuntimeBuilder::build()` creates one planner, wraps it in the shared
driver, and every session uses it. Conversation fragments are also sorted by
`FragmentKind::order_rank()`, so the category used for accounting can move a
user message after the assistant tool call and result that followed it.

The corrected shape is:

```text
RuntimeShared (immutable, reusable)
  ├── providers and model catalogs
  ├── sealed registries and component pipeline
  ├── tool factories/executors and security checks
  └── host stores/brokers

SessionExecutionContext (one per session)
  ├── RunPlanner + prior cache plan
  ├── structural compactor policy
  ├── scoped ability view + activation epochs
  ├── extension/component state namespaces
  └── current TurnMachine

TurnMachine (one active turn)
  ├── turn cancellation
  ├── request/attempt state
  ├── prepared tool calls
  ├── pending approval/question
  └── checkpoint watermark
```

## Goals / Non-Goals

### Goals

- Make the provider request preserve canonical conversation order exactly.
- Make every mutable planning and activation value belong to one session.
- Make retry output correct for live views, journal replay, and final
  `visible_output`.
- Authorize and approve the exact action that will execute.
- Resume without losing manifests or duplicating committed model/tool work.
- Expose one generic harness path that current built-ins exercise before MCP or
  third-party extensions.
- Support structured agent-to-user clarification without conflating it with
  security approval.
- Preserve deterministic, network-free context validation.

### Non-Goals

- Replace the direct loop with arbitrary graph execution.
- Give hooks mutable access to all runtime/session state.
- Put Smith prompts, skill source precedence, storage paths, or TUI behavior in
  the shared runtime.
- Treat an observability journal as an exact checkpoint.
- Infer narrow shell authority by parsing arbitrary command text.
- Add nested child agents or restart ephemeral children after process exit.

## Decisions

### Classification and placement are independent

`FragmentKind` remains a budget/accounting category. A new placement value
controls wire order:

```rust
pub struct ContextPosition {
    pub lane: ContextLane,
    pub sequence: u64,
}

pub enum ContextLane {
    Instructions,
    Capabilities,
    Memory,
    Conversation,
    TailContext,
}
```

All conversation messages use `ContextLane::Conversation` and retain their
canonical sequence. The planner may order stable non-conversation lanes for
cache efficiency, but it MUST NOT reorder messages inside the conversation
lane.

The runtime groups conversation content by turn. An assistant message may own
many tool-call IDs, and every matching result belongs to the same atomic
exchange:

```rust
pub struct ToolExchange {
    pub assistant: Message,
    pub call_ids: BTreeSet<ToolCallId>,
    pub results: Vec<Message>,
}
```

Every message from the latest user input through the active continuation is
required. Older completed turn groups are optional and may be compacted only
as complete groups.

### Compaction returns one owned result

The compactor contract returns its fragments and outcome together:

```rust
pub struct CompactionResult {
    pub fragments: Vec<ContextFragment>,
    pub outcome: CompactionOutcome,
}

pub trait Compactor {
    fn compact(
        &self,
        fragments: &[ContextFragment],
        report: &BudgetReport,
        budget: &ContextBudget,
    ) -> Result<Option<CompactionResult>, CompactionError>;
}
```

There is no `last_outcome` mutex. A plan that does not compact receives a
fresh no-op outcome. The existing deterministic implementation becomes
`StructuralCompactor`: it may select, bound, and validate content but does not
claim to understand or summarize meaning.

Model-assisted semantic summaries are coordinated above the context crate.
The coordinator stores originals in the session-private artifact store,
requests a summary through a separately attributed model purpose, validates
coverage/provenance, and submits explicit `Summary` fragments back to the
deterministic planner.

### Turn submission and cancellation are explicit

`SessionHandle::send` returns `Result<TurnHandle, RuntimeError>`. A
`TurnHandle` exposes its `TurnId`, completion, and turn-local interruption.
The active-turn registry retains each turn cancellation handle.

`interrupt_current_turn(UserRequested)` cancels only the currently serving
turn. `cancel_session(Shutdown|Revoked|...)` permanently cancels the root
session token and is reserved for terminal lifecycle. Submitting while
shutdown is in progress returns an error and does not mint an orphan turn ID.

### Provider output is speculative until the attempt commits

Text and reasoning deltas carry request and attempt identity. Consumers may
render them immediately, but must buffer them as speculative state. Every
attempt ends with exactly one of:

```text
ProviderAttemptOutputCommitted
ProviderAttemptOutputDiscarded
```

A retryable failure discards its accumulated visible and reasoning output
before the next attempt begins. Only committed output changes canonical
assistant history or the turn's `visible_output` value. Usage and diagnostics
for failed attempts remain observable.

### Tool calls are prepared before authority is evaluated

The tool contract becomes a two-stage operation:

```rust
pub struct PreparedToolCall {
    pub call_id: ToolCallId,
    pub tool: String,
    pub canonical_arguments: Value,
    pub required_permissions: PermissionSet,
    pub resource: SecurityResource,
    pub effects: ToolEffects,
    pub display: ToolCallDisplay,
    pub preparation_fingerprint: Fingerprint,
}

#[async_trait]
pub trait Tool {
    fn spec(&self) -> ToolSpec;

    async fn prepare(
        &self,
        call: ToolCall,
        ctx: &PreparationContext<'_>,
    ) -> Result<PreparedToolCall, RuntimeError>;

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError>;
}
```

The executor validates schema, canonicalizes arguments/resources, verifies the
prepared permissions are within the descriptor upper bound, authorizes,
requests approval if required, verifies the preparation fingerprint, and
invokes the prepared action. Editing approval arguments starts preparation and
authorization again.

Static effects remain a conservative descriptor upper bound and readiness hint.
They are not used as the final invocation resource. Shell declares broad
workspace write, process, and applicable network authority because arbitrary
commands cannot be narrowed safely.

Approval waiting selects over turn cancellation and deadline. Approval is a
security decision over a prepared action and cannot change arguments,
permissions, or resource.

### The direct loop becomes a serializable state machine

The runtime retains a direct, auditable transition table:

```rust
pub enum TurnState {
    Accepted { input: UserInput },
    Planning,
    CallingModel { request: PlannedRequest },
    ModelResponseReady { response: AssembledResponse },
    AwaitingApproval { calls: Vec<PreparedToolCall> },
    AwaitingInteraction { request: InteractionRequest },
    ExecutingTools {
        calls: Vec<PreparedToolCall>,
        completed: Vec<ToolResultBlock>,
    },
    Completing,
    Terminal { finish: TurnFinish },
}
```

Transitions are idempotent by session, turn, state revision, and operation
fingerprint. The runtime checkpoints after input acceptance, assembled model
response, prepared pending actions, each committed tool result, and terminal
completion. The first implementation may ship completed-turn persistence
before mid-turn checkpoints, but the state schema is chosen once.

`CheckpointStore` is distinct from `EventObserver` and `SessionStore` summary
snapshots. It stores exact state required for resumption and is protected by
host policy. The event journal remains redacted, bounded observability and
links to a checkpoint through a watermark.

### Agent questions are not approvals

The generic harness supplies an `ask_user`/questionnaire ability backed by a
host-injected `InteractionBroker`. A request contains origin session/turn/call
identity, stable question IDs, one-to-three bounded questions, optional
mutually exclusive choices, optional free-form answers, a deadline,
cancellation, and content sensitivity.

The turn enters `AwaitingInteraction`; the exact pending request is
checkpointed. An answer resumes the same turn and becomes the canonical result
of the questionnaire ability. Decline, timeout, cancellation, and unavailable
host support are structured results.

This channel MUST NOT:

- authorize a tool, widen a permission, or resolve a security grant;
- masquerade as a new independent user turn;
- wait forever when no interactive host exists;
- expose raw sensitive answers in default events or manifests.

Interactive readiness controls whether the ability is activated. A
non-interactive host may omit it or provide an explicit protocol; absence
returns `interaction_unavailable` rather than hanging.

The neutral runtime permits host policy to decide whether a child session can
activate the interaction ability. It does not silently route a child prompt to
the root user or parent session.

### Ability activation is part of session execution

At session creation the runtime seals a registry snapshot, derives the
policy-scoped view, retrieves an initial dependency-complete bundle, authorizes
activation, creates epoch zero, and emits the corresponding lifecycle events.
At each provider boundary the current epoch is frozen and materialized into
tool schemas and instruction fragments.

An intent miss may invoke a small protected capability-search bootstrap,
activate an additional authorized bundle at a safe boundary, advance the
epoch, and replan. Existing Smith-like fixed tools are the first conformance
fixture; MCP and third-party extensions are later sources, not the first test.

### Harness extensions use ordered typed phases

The generic harness exposes narrow component traits such as
`ContextContributor`, `ToolViewResolver`, `ModelInterceptor`, and
`TurnCommitHook`. Components declare stable ID/revision and before/after
constraints. Build time topologically sorts them, rejects missing dependencies
and cycles, protects security/context phases, and fingerprints the final
pipeline.

Hooks receive immutable views and return typed patches. Component state is
namespaced and session-scoped. Product-authored prompt text and source policy
remain outside the runtime.

### Artifacts make offloading recoverable

Large tool content may be represented as:

```rust
pub enum ToolContent {
    Inline(Vec<ContentPart>),
    Artifact {
        preview: Vec<ContentPart>,
        reference: ArtifactRef,
        media_type: String,
        byte_length: u64,
    },
}
```

`ArtifactStore` is orthogonal to workspace access and session-private by
default. `artifact.read` is bounded, paginated, permission-checked, and fully
attributed. Structural compaction and semantic summaries may refer to artifacts
without placing their raw content in the project workspace.

### Standard harness components remain optional composition

Todo state, skill loading, memory contribution, artifact offloading, semantic
summary coordination, standard delegation adaptation, capability search, and
questionnaire support live in `agent_runtime::harness` initially. A separate
crate is created only after a second consumer demonstrates independent reuse.

## Risks / Trade-offs

- This is a breaking pre-1.0 event, tool, and session API change. A coordinated
  consumer migration and schema fixtures are required.
- Mid-turn checkpoints contain more sensitive material than audit journals;
  hosts must supply protection and retention policy.
- Speculative streaming adds reducer complexity but avoids corrupt transcripts
  without sacrificing latency.
- Exact preparation is conservative for shell and other opaque operations; the
  correct result is broad declared authority, not false precision.
- Live activation makes runtime composition more complex. Freezing epochs at
  safe boundaries preserves determinism.
- Semantic summarization adds provider cost and failure modes. It is optional
  and never weakens deterministic context validation.

## Migration Plan

1. Land provider-request capture and the release-gate regression tests before
   changing implementation.
2. Introduce placement/turn-group types and per-session execution context
   behind the existing facade; restore manifests and remove the compactor side
   channel.
3. Add turn handles and attempt-scoped events, then migrate reducers before
   removing legacy event forms.
4. Land prepared invocation alongside the active security change and migrate
   built-in tool fixtures first.
5. Introduce `TurnMachine` and completed-turn persistence, then add
   boundary-level checkpoints and recovery.
6. Add the interaction broker/questionnaire and durable approval states.
7. Wire ability activation and typed harness phases using current built-in
   tools as the integration fixture.
8. Add artifacts, summaries, todos, skills, and memory; only then enable new
   extension sources.
9. Release only after Agent Runtime and Smith compatibility suites pass against
   the same pinned revisions.

## Active-change reconciliation

This change is explicitly rebased on the landed Phase A portions of
`add-runtime-security-boundary-2026-07-24`. Prepared invocations reuse
`agent_runtime_registry::Permission`,
`agent_runtime_core::security::{PermissionSet, SecurityResource,
AuthorizationRequest}`, and the existing composed `SecurityCheckSet`; they do
not introduce a second permission vocabulary or authorization path.
`ToolEffects` remains the conservative descriptor/scheduling upper bound, but
its static resource derivation is superseded as the final invocation authority
by the concrete prepared action. The still-open credential/stdio/clock/random,
native trust, broker, and isolation tasks in the security change remain
independent and are not marked complete here.

The completed-but-unarchived delegation and reasoning-preservation changes are
also explicitly rebased into this implementation. Child sessions receive
independent session execution contexts, remain depth-one and ephemeral, and
retain parent attribution. Successful-attempt reasoning continuation remains
canonical while failed-attempt reasoning becomes speculative and discardable.
No shared capability requirement is weakened or duplicated.

## Open Questions

- The shared runtime defines checkpoint confidentiality requirements and
  redaction hooks; each host still chooses an encrypted database, protected
  file, or ephemeral implementation. Smith's concrete choice is decided in
  its coordinated proposal.
- Whether a future bidirectional headless protocol supports answering
  questionnaires is a host decision. The runtime contract supports it, but the
  first non-interactive consumer may return `interaction_unavailable`.
