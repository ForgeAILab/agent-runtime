## Context

Nyx's LCM implementation combines an immutable `conversation_turns` ledger with transactional `summary_nodes` / `summary_edges`, active-node derivation through supersession, deterministic pointer annotations, token-targeted leaf selection, fanout condensation, soft/hard pressure thresholds, and three-stage summarization escalation. A later Nyx change settles aged DAG summaries into semantic vector memory without deleting the original turns.

Agent Runtime already owns the adjacent neutral mechanisms: canonical history and
session persistence, protected artifacts/checkpoints, context fragments,
structural compaction, cache-aware planning, run manifests, and typed runtime
events. The pre-cutover semantic-summary implementation was deliberately
narrower: one session-scoped summary replaced one prefix identified by
`omit_prefix`, while the original prefix was stored as a protected artifact. It
did not provide a timeline independent of a runtime session, hierarchical
summaries, stable expansion pointers, or guaranteed convergence.

The implemented cutover transfers the reusable LCM mechanism into Agent Runtime
and removes the flat coordinator from the public surface. Product memory policy
remains in consumers.

## Goals / Non-Goals

### Goals

- Publish a host-neutral LCM package usable without the provider adapters or runtime facade.
- Preserve every compacted source entry behind a stable, authorized expansion path.
- Keep the context planner as the only authority for provider-visible ordering and budgeting.
- Support a logical timeline that survives runtime-session replacement.
- Guarantee bounded compaction progress without trusting a model to honor a size request.
- Make DAG mutation, checkpoint recovery, replay, and concurrent compaction idempotent.
- Preserve sensitivity, trust, content-guard, model, policy, and source provenance through summaries.
- Transfer one canonical implementation from Nyx and supply consumer conformance gates.

### Non-Goals

- A general vector database or embedding service.
- Product-level memory ACL, authority, visibility, publication, or lifecycle rules.
- A scheduler, daemon, or unattended wake policy.
- Provider-specific summarization prompts or model selection defaults.
- Deleting or rewriting a host's authoritative raw history.
- Cross-timeline summary nodes or automatic sharing between agents.

## Decisions

### Independently reusable package

`agent-runtime-lcm` is a new production package rather than another large module in the facade. It is independently justified by three consumers and owns a coherent mechanism with its own store contracts and conformance surface. It MUST NOT depend on Smith, Nyx, Open Forge, a concrete database, a provider adapter, an HTTP client, or a scheduler.

The package may depend on the neutral core, registry, and context contracts. `agent-runtime` re-exports its supported composition API and owns the turn/checkpoint integration.

Direct consumers implement `LcmReader` and `LcmWriter` over their own
transactional store and run the shared `agent-runtime-testkit` LCM conformance
suite. The host creates one `LcmViewAuthority` at its authorization boundary,
shares it with the adapter, and issues views only for an authorized binding.
Facade consumers provide the same host store, a `LcmSummaryModel`, a
`LcmCoordinatorPolicy`, and a `LcmTimelineBinding` through a resolver, then
attach exactly one coordinator with `RuntimeBuilder::lcm`.

### Timeline identity is not runtime-session identity

LCM introduces an opaque `LcmTimelineId`. A host binds one runtime session to
exactly one timeline at session construction. Nyx can bind an authorized
channel, Smith can bind a persistent agent session, and Open Forge can bind an
authorized Room/AgentIdentity context. Replacing a backend `SessionId` does not
require replacing the timeline. These are consumer binding designs only; this
change does not edit those repositories.

The timeline identifier grants no read authority. Hosts authorize a binding before construction, and every later expansion is checked through the same host-owned store/view rather than trusting an ID supplied by model text.

### Immutable entries and derived DAG

`LcmStore` exposes append/read-range operations for immutable, ordered timeline entries and compare-and-swap DAG commits. A production implementation may map these operations onto an existing append-only conversation store; it need not duplicate content into a second database.

Nodes are `leaf` or `condensed`. A leaf references a contiguous, fingerprinted entry span. A condensed node references active child nodes from the same timeline. Active state is derived from nodes without a superseding parent. No mutable "active context" row exists.

Every successful node commit atomically writes the node, its typed edges, and child supersession updates under an expected timeline/DAG revision. Gaps, overlap, cross-timeline edges, stale revisions, missing children, and already-superseded children fail without partial mutation.

### Stable source identities and lossless expansion

LCM uses opaque entry and node identifiers rather than Nyx integer channel-turn
IDs. Each summary records its exact source fingerprint, covered sequence range,
child identities, policy/algorithm/sizer revisions, token count, source
classifications, and its own producer metadata. A model-produced node retains
that model's identity, revision, escalation level, and purpose; deterministic
fallback nodes retain the algorithm revision and no fabricated model purpose.
A leaf remains expandable to the original entries; a condensed node remains
recursively expandable to leaves and then originals.

Provider-visible annotations are generated by the runtime from validated node metadata. Summary-model output cannot author or alter an LCM pointer. Expansion accepts a validated opaque reference and returns bounded content through a host-authorized `LcmView`; receiving or repeating a reference never grants access.

### Context projection remains below the planner

LCM assembles candidate active nodes followed by the recent raw suffix, preserving canonical order and complete tool-call/result exchanges. The runtime maps these candidates to versioned context fragments. The existing context planner remains solely responsible for token accounting, final inclusion, structural compaction, provider serialization, and cache identity.

LCM MUST NOT append content directly to a provider request or maintain another
token-budget authority. Binding/authorization, store schema, store-view
authorization, classifier, node, algorithm, policy, model, and sizer revisions
remain separate values. Manifest records carry the replay-relevant binding,
store, view, node, policy, model, and sizer values; protected LCM state also
binds the classifier revision.

### Soft and hard pressure use checkpointed runtime operations

Policy defines a soft pressure threshold, hard pressure threshold, leaf token target, condensation fanout, retained recent-turn floor, maximum rounds, and escalation budgets. Thresholds are evaluated from the planner's resolved input budget and observed growth, excluding summary-model usage.

Above the soft threshold, the runtime records the decision at turn commit and
admits at most one idempotent operation only when the host explicitly claims the
session's protected idle boundary with `try_idle_compaction`. The completed user
turn never waits for that model work. Above the hard threshold, the next
external turn performs bounded protected compaction in the pre-provider hook,
before provider admission. A hard compaction is not an uncheckpointed model
call inside the deterministic context planner.

Compaction ownership is serialized by expected DAG revision and operation fingerprint, not only an in-process mutex. Concurrent processes may compute candidates, but only one compatible mutation commits. A stale loser reloads state and does not repeat provider work automatically.

### Guaranteed-convergence escalation

The shared engine ports Nyx's three stages: a detail-preserving model request, a stricter reduced-budget model request, then deterministic bounded head/tail reduction with explicit elision metadata. Provider failure, empty output, over-budget output, non-shrinking output, or invalid provenance advances the escalation level.

Every committed replacement MUST be strictly smaller under the same versioned request sizer than the source it replaces. A configured maximum round count bounds hard-pressure latency. If eligible content still cannot fit, the runtime returns the existing structured cannot-fit result rather than silently deleting required material.

### Security classification is joined, not rewritten

Summary sensitivity is the most-sensitive classification of all covered
sources. Summary trust is the least-trusted classification, and
guarded/sanitized inputs retain their guard and transformation revisions.
Summary text is re-guarded before commit where the active context-security
contract requires it. Lifecycle events and manifests contain only bounded
opaque IDs, hashes, revisions, classifications, counts, usage, and stable
reasons; they never contain source/summary bodies, artifacts, credentials, or
authority grants.

Secret-class content is never sent to a summary model or stored in normal summary bodies. Ineligible required content remains raw or causes a structured cannot-fit result. Events and manifests contain bounded identities, revisions, classifications, counts, hashes, and reasons, never raw summaries or source content.

### Replace the flat coordinator

`LcmCoordinator` is the canonical persisted semantic-history component; the old
implementation, public aliases, and independent state machine are removed.
Existing usage attribution, protected-state validation, artifact integrity
checks, and checkpoint ordering are retained behind the LCM API where they
remain applicable.

On resume, when `RuntimeBuilder::lcm` is configured, the runtime automatically
detects valid schema-v1 state. The host must provide a durable `SessionStore`,
and the coordinator must have the legacy protected `ArtifactStore` configured.
The cutover verifies the canonical history/source fingerprint, exact artifact
bytes and provenance, and host-authorized timeline binding, then appends the
immutable history, commits one equivalent leaf, and persists the replacement
checkpoint before accepting turns. The old namespace is removed only after
that durable boundary. Missing/malformed/incompatible state or artifact fails
closed without partial timeline or DAG mutation; there is no public/manual
restore alias.

### Episodic and semantic memory stay separate

LCM is the episodic/context layer. Vector retrieval remains a host concern through `MemorySource`. A later independent change may define a neutral settlement sink that indexes aged LCM nodes, but settlement cannot delete the DAG or raw timeline and is not required for this extraction.

### Source transfer and consumer migration

The donor is Nyx revision `9614842d8f614d7d41e00d8e73ed3d042764d451`. The transfer covers the summary DAG invariants, compaction selection/control logic, escalating summarizer, active-context assembly, and their tests. Agent Runtime's provenance file records exact source/destination paths and material changes.

Nyx, Smith, and Open Forge adoption occurs in separate approved changes. Once Nyx adopts the shared package, it deletes its superseded LCM mechanism in the same consumer change. Fixes during the bounded transfer window land canonically here and are backported only with an explicit reference.

## API Shape

The implemented public shape establishes the ownership boundaries:

```rust
pub struct LcmTimelineId(/* opaque */);
pub struct LcmEntryId(/* opaque */);
pub struct LcmNodeId(/* opaque */);
pub struct LcmSequence(u64);

pub struct LcmEntry { /* id, sequence, content fingerprint, protected content metadata */ }
pub struct LcmNode { /* kind, range, revision, classifications, source fingerprint */ }
pub enum LcmEdge { Entry(LcmEntryId), Node(LcmNodeId) }

#[async_trait]
pub trait LcmReader: Send + Sync + Debug {
    fn store_revision(&self) -> RegistryRevision;
    fn authorize_view(&self, view: &LcmView) -> Result<(), LcmError>;
    async fn current_revision(&self, view: &LcmView) -> Result<LcmRevision, LcmError>;
    async fn load_range(/* view + bounded range */) -> Result<Vec<LcmEntry>, LcmError>;
    async fn active_nodes(/* authorized view */) -> Result<Vec<LcmNode>, LcmError>;
    async fn node(/* authorized view + opaque id */) -> Result<LcmNode, LcmError>;
    async fn expand(/* bounded authorized reference */) -> Result<LcmExpansion, LcmError>;
}

#[async_trait]
pub trait LcmWriter: LcmReader {
    async fn append(/* idempotent immutable entries */) -> Result<AppendResult, LcmError>;
    async fn commit_leaf(/* expected revision + typed edges */) -> Result<CommitResult, LcmError>;
    async fn commit_condensation(/* expected revision + active children */)
        -> Result<CommitResult, LcmError>;
}

pub trait LcmStore: LcmReader + LcmWriter {}
```

Hosts mint `LcmViewAuthority` once and share it with their adapter; the adapter
validates that authority and its host binding on every method. The default
graph has no concrete backend. A facade host supplies a resolver-backed
`LcmTimelineBinding`, `Arc<dyn LcmStore>`, `Arc<dyn LcmSummaryModel>`, and
`LcmCoordinatorPolicy` to `LcmCoordinator::new`, then calls
`RuntimeBuilder::lcm(Arc::new(coordinator))`. The `StartSession` id must match
the binding's runtime session id.

## Risks / Trade-offs

- The cutover replaces a working flat-summary path and touches session
  recovery, context planning, and event schemas. Mitigation: preserve the
  applicable accounting/redaction tests, keep schema-v1 decoding internal to
  automatic resume, and gate consumer adoption separately.
- A host store without transactional compare-and-swap cannot safely claim production LCM support. Mitigation: make conformance mandatory and provide only an in-memory reference implementation for tests.
- Timeline persistence can duplicate data if a host adapts it naively over snapshots. Mitigation: allow adapters over existing immutable history and document that LCM owns references and derived nodes, not necessarily a second content copy.
- Summary DAGs increase metadata and implementation complexity. Mitigation: high fanout, bounded expansion, active-node derivation, and deterministic pruning of derived caches only; raw history is never pruned by LCM.
- Compaction changes provider-cache prefixes. Mitigation: active node revisions participate in cache identity, and compaction happens at explicit checkpoint boundaries.
- The active runtime-security proposal changes trust/content-guard vocabulary. Mitigation: keep this delta additive and rebase the runtime adapter onto the approved security types before implementation approval.

## Migration Plan / Current Cutover

1. The package, neutral types, store contracts, algorithms, and conformance
   fixtures are implemented without changing consumer repositories.
2. Runtime facade integration performs the validated schema-v1 cutover on
   resume when `.lcm` is configured with a durable `SessionStore` and the
   coordinator's protected legacy `ArtifactStore`, then persists the
   replacement leaf and checkpoint before accepting turns.
3. The flat coordinator and its public aliases are removed; facade exports,
   events, fixtures, README, changelog, and provenance describe LCM.
4. Neutral Smith, Nyx, and Open Forge gates remain release/adoption work; no
   final consumer validation or adoption is claimed by this repository change.
5. Each consumer lands a separate approved proposal: Nyx as donor/canonical
   owner cutover, then Smith and Open Forge as their bindings require.

Before publication, rollback removes the new package/integration from the
unreleased change. After a host persists LCM DAG state, rollback requires a
declared non-equivalent session restart or a host-owned export; it must not
silently discard the timeline or expose a public restore alias.

## Resolved Implementation Decisions

- The least-authority API uses separate `LcmReader` and `LcmWriter` traits, with an `LcmStore` convenience trait for hosts that implement both. Authorized views are host-created values passed to reads; an opaque timeline or node ID is never sufficient authority.
- The first release exposes bounded expansion through the Rust API and deterministic provider-visible pointer annotations only. It does not register an `lcm.expand` model ability; a later change may add one after defining its host authorization envelope.
- Timeline entries retain the neutral structured `Message` plus stable metadata. This keeps tool-call/result validation and exact replay possible without introducing consumer-domain envelopes.
- Classification reuses the repository's neutral `agent_runtime_registry::TrustClass`, `agent_runtime_context::Sensitivity`, and content-guard revision contracts. The LCM package does not create a parallel security taxonomy; it carries optional guard/transformation provenance and leaves host guard invocation at the runtime boundary.
