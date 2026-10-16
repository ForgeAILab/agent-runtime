# Lossless Context Memory transfer baseline

Status: the neutral extraction and Agent Runtime cutover are implemented and
validated in this working tree; this document retains the historical donor and
pre-cutover baseline used to audit the transfer. The neutral `consumer_nyx`,
`consumer_smith`, and `consumer_open_forge` contract gates pass against the
exact baselines recorded below. That is not consumer adoption: no Nyx, Smith,
or Open Forge repository was changed, and each adoption remains a separate
approved change pinned to the eventual Agent Runtime commit.

## Donor identity, license, and notice

| Field | Baseline value |
| --- | --- |
| Donor repository | Nyx (`gitea@git.mai1015.com:nyx/nyx.git`) |
| Exact donor revision | `9614842d8f614d7d41e00d8e73ed3d042764d451` |
| Donor revision subject | `chore(runtime): close phase 1 merge gates` (2026-08-10) |
| Donor license | `MIT OR Apache-2.0` (the donor root `Cargo.toml` and `README.md`) |
| License used in Agent Runtime | MIT, as recorded by the destination `LICENSE` |
| Donor notice | `LICENSE-MIT`: `Copyright (c) 2025-2026 Nyx Contributors` |
| Notice action | Keep the donor MIT notice with any transferred substantial source, and retain the destination provenance entry. Do not copy Nyx product branding or policy as if it were neutral runtime behavior. |

The LCM code first landed in donor commit
`007a3e356987a83e923390c4867b69051848c321`; this baseline is taken from the
specified final revision so later donor fixes are included. The donor's V7/V8
summary-DAG migrations and V9 removal of `conversation_summaries` are part of
the observed final state, not a request to copy Nyx's SQLite schema into Agent
Runtime. The complete attribution and in-progress status are also recorded in
[`PROVENANCE.md`](../../../../PROVENANCE.md).

## Exact source-to-destination map

The destination is split between the independent `agent-runtime-lcm` package
(neutral mechanism) and the Agent Runtime facade (turn admission, checkpoint,
and context integration). The map deliberately names source slices where a
donor file also contains Nyx-only fallback or dispatch policy.

| Donor path at `9614842d8f614d7d41e00d8e73ed3d042764d451` | Destination path | Transfer disposition |
| --- | --- | --- |
| `crates/nyx-agent/src/compression/escalating.rs` | `crates/agent-runtime-lcm/src/summarize.rs` | Transfer the three-stage escalation contract and strict deterministic fallback. Replace Nyx `LlmProvider`, message, prompt, and model-selection types with neutral package contracts; preserve no provider prompt defaults. |
| `crates/nyx-runtime/src/dispatch/compaction.rs` | `crates/agent-runtime-lcm/src/planning.rs` and `crates/agent-runtime-lcm/src/pressure.rs` | Transfer token-targeted leaf selection, tool-pair boundary handling, active-node fanout condensation, threshold calculations, and operation planning. Remove Nyx service lookup and dispatch execution. |
| `crates/nyx-runtime/src/dispatch/steps/load_history.rs` (`assemble_dag_history`, summary pointer rendering, frontier cursor) | `crates/agent-runtime-lcm/src/projection.rs` | Transfer ordered active-node plus raw-suffix projection and stable pointer metadata. The provider-facing result becomes runtime context fragments, not a Nyx `Message` assembled directly into a request. |
| `crates/nyx-runtime/src/dispatch/steps/load_history.rs` (history boundary and hard-pressure admission) | `crates/agent-runtime/src/harness/lcm.rs` | Re-express the read-only projection and hard-checkpoint adapter against `LcmTimelineId`, `LcmView`, and Agent Runtime turn gates. Do not transfer Nyx recent-window layering as a second budget authority. |
| `crates/nyx-runtime/src/dispatch/steps/trigger_compaction.rs` | `crates/agent-runtime/src/harness/lcm.rs` | Re-express soft-pressure post-commit/idle admission and checkpointed operation identity. The donor's detached task and process-local per-channel mutex are not the package contract. |
| LCM policy portions of `crates/nyx-runtime/src/dispatch/foundation/config.rs` and `crates/nyx-runtime/src/dispatch/foundation/mod.rs` | `crates/agent-runtime-lcm/src/pressure.rs` | Transfer named policy inputs (soft/hard ratios, leaf target, fanout, retained suffix, escalation and round limits) as neutral revisioned policy. Nyx config resolution stays consumer-owned. |
| `crates/nyx-core/src/services/traits.rs` (`SummaryNode`, `SummaryEdge`, `SummaryDagService`, `SessionService::load_turns_after`) | `crates/agent-runtime-lcm/src/store.rs` | Replace string channel IDs and integer turn IDs with opaque timeline, entry, node, and sequence types plus least-authority reader/writer/store contracts. Preserve atomic mutation and frontier semantics. |
| `crates/nyx-core/src/services/testing.rs` (`InMemorySummaryDag`) | `crates/agent-runtime-lcm/src/testing.rs` and `crates/agent-runtime-testkit/src/conformance/lcm.rs` | Rebuild the reference store fixture against the neutral traits. Keep validation coverage; do not expose a Nyx service registry or `KernelError`. |
| LCM methods in `crates/nyx-store/src/session_store.rs` | `crates/agent-runtime-lcm/src/store.rs` and `crates/agent-runtime-testkit/src/conformance/lcm.rs` | Preserve behavioral invariants (contiguous leaves, same-timeline children, no active overlap, atomic supersession, active/frontier reads) as backend-neutral contracts. No SQLite implementation is transferred. |
| LCM-focused tests in `crates/nyx-store/src/sqlite_tests.rs` | `crates/agent-runtime-testkit/src/conformance/lcm.rs` | Port the invariant cases to an in-memory/reference store. Migration-specific assertions remain consumer adapter tests. |
| `crates/nyx-store/src/migration.rs` V7/V8/V9 | No destination in `agent-runtime-lcm`; consumer-owned adapter migrations | V7 summary tables and V8 `settled_at` document donor persistence behavior. V9 dropping `conversation_summaries` is a Nyx migration decision; it must not be copied into the package. |
| Nyx wiring in `crates/nyx/src/subsystems/session/{service.rs,mod.rs}` and `crates/nyx/src/runtime/integration_tests.rs` | No direct destination; `consumer_nyx` adoption gate | Keep channel/session wiring and end-to-end tests in Nyx until a separate approved consumer change binds its host identity to `LcmTimelineId`. |
| Nyx settlement/vector-memory changes (`crates/nyx-memory/**`, `settled_at` policy and memory tools) | No destination | Episodic LCM transfer stops at lossless expansion. Settlement, embeddings, memory search, ACL/visibility, and lifecycle policy are out of scope. |

No path in this table authorizes a second implementation in a consumer. During
the bounded transfer window, fixes to the neutral mechanism land in
`agent-runtime-lcm`; consumer copies are removed only in their separately
approved adoption changes.

## Donor behavior at the baseline revision

### Timeline, DAG, and persistence

- Nyx uses a channel string as the timeline key and integer conversation-turn
  IDs as source identities. A leaf summary covers a contiguous, ordered span of
  turns. A condensed summary covers at least two active child summaries.
- A node is active when no other node supersedes it; there is no mutable active
  context row. Active nodes are ordered by `(start_turn_id, id)`, and the
  frontier is the maximum covered end turn across all nodes.
- Leaf writes and condensation are transactional. The store rejects missing or
  gapped leaf turns, active overlap, missing/cross-channel children,
  already-superseded children, and invalid child cardinality without partial
  mutation. Condensation inserts the parent, edges it to the children, and
  marks all children superseded in one transaction.
- The final donor schema has `summary_nodes` and `summary_edges` (V7), a
  `settled_at` field for a later tiered-memory flow (V8), and drops the old
  `conversation_summaries` table (V9). Raw conversation turns remain the
  authoritative source; LCM does not delete them.
- The donor uses a process-local in-flight guard per channel for asynchronous
  soft compaction. The neutral extraction replaces that limitation with
  expected revision and operation-fingerprint ownership so concurrent hosts
  cannot commit conflicting DAG mutations.

### Projection and pressure control

- History assembly renders active nodes first, followed by turns after the
  frontier. Each pointer is generated from validated metadata in the form
  `[summary #<id> | turns <start>–<end> | <kind>]`, then the stored summary
  text. The model cannot author or modify the pointer.
- Turn reconstruction preserves multimodal content and tool calls/results.
  Boundary repair drops orphan tool results, synthesizes missing tool results,
  and reorders interleaved or multi-step tool chains so a provider never sees a
  broken tool exchange.
- Donor leaf selection targets roughly 4,096 raw tokens and a 512-token leaf
  replacement. It expands a selected block to include the matching tool result
  when a tool call would otherwise be split. Once active nodes exceed the
  configured fanout (normally 8), the oldest group of at least two active nodes
  is condensed.
- A soft threshold can schedule one asynchronous compaction after persistence.
  A hard threshold blocks the next persistent history load and performs bounded
  compaction rounds until the estimate is below the hard threshold or no work
  remains. Non-persistent dispatches do not perform blocking compaction.
- Projection is lossless only because pointers refer to stable stored nodes and
  expansion can walk condensed nodes back to leaves and then original turns.
  The donor's channel/service authorization is implicit in its service wiring;
  Agent Runtime must make authorization an explicit host-owned view.

### Escalating summarization

`EscalatingSummarizer` runs up to three stages and accepts a result only when it
is non-empty and strictly smaller than the input under the supplied estimator:

1. A detail-preserving model request targets the requested token budget.
2. A bullet-oriented request targets half that budget.
3. A deterministic fallback serializes role/text, caps output at
   `min(512, input_tokens - 1)`, and keeps a bounded head and tail with an
   explicit `\n...\n` elision marker. A binary-search fit guarantees strict shrink.

Provider failure, empty output, over-budget output, or non-shrinking output
advances the level. Empty input and zero targets return the bounded empty
outcome. This algorithm has no secret/sensitivity/trust handling; those joins
are supplied by the Agent Runtime security and context contracts.

### Deliberately untransferred donor behavior

Nyx prompts, model defaults, provider catalog, scheduler/daemon policy,
channel/actor/dispatch types, SQLite migration policy, vector memory and
settlement, and product-level memory visibility are not neutral LCM behavior.
They remain consumer concerns and must not leak into `agent-runtime-lcm`.

## Donor LCM test inventory

The following is the focused inventory at the exact donor revision. Existing
non-LCM dispatch and tool-history tests remain relevant where the neutral
projection reuses those invariants.

| Donor test location | Tests that establish the baseline |
| --- | --- |
| `crates/nyx-agent/src/compression/escalating.rs` | `level_one_suffices`; `level_one_non_shrinking_escalates`; `double_provider_failure_falls_back_to_l3`; `convergence_contract`; `small_input_both_llm_levels_fail` |
| `crates/nyx-runtime/src/dispatch/compaction.rs` | `fanout_condensation`; `tool_pairing_boundary_respected` |
| `crates/nyx-runtime/src/dispatch/steps/trigger_compaction.rs` | `soft_threshold_triggers_async_compaction`; `non_persistent_dispatch_never_spawns_compaction`; `zero_cost_below_soft`; `single_in_flight_per_channel`; `async_compaction_failure_nonfatal` |
| `crates/nyx-runtime/src/dispatch/steps/load_history.rs` | `assembly_ordering_annotations_no_duplication`; `frontier_older_than_history_limit_gap_test`; `fallback_without_dag_service`; `zero_history_limit_skips_history_load`; `boot_dispatch_skips_history_load`; `heartbeat_dispatch_loads_recent_turns_without_dag_summaries`; `non_persistent_dispatch_skips_blocking_compaction`; `hard_threshold_blocks`; `message_from_turn_reconstructs_multimodal_user_content`; `message_from_turn_reconstructs_tool_and_tool_calls_json`; `message_from_turn_uses_structured_attachments_when_text_marker_is_embedded`; `repair_tool_history_drops_orphan_tool_results`; `repair_tool_history_synthesizes_missing_tool_results`; `repair_tool_history_keeps_clean_pairs`; `repair_tool_history_allows_unknown_tool_names`; `repair_tool_history_reorders_interleaved_final_assistant`; `repair_tool_history_reorders_multi_step_tool_chain` |
| `crates/nyx-core/src/services/testing.rs` | `settlement_roundtrip_via_mock_trait` (the `InMemorySummaryDag` fixture also enforces leaf/condensation validation in its service methods) |
| `crates/nyx-store/src/sqlite_tests.rs` | `leaf_summary_write_is_atomic`; `leaf_summary_validates_turn_span`; `overlapping_active_leaf_is_rejected`; `condensation_supersedes_children_atomically`; `condensation_rejects_invalid_children`; `lossless_reachability_after_multiple_rounds`; `frontier_reflects_covered_turns`; `cursor_loads_turns_past_frontier`; `channel_isolation_for_summary_nodes`; `load_unsettled_cutoff_filtering`; `settled_exclusion_and_retry_safe_remark`; `superseded_nodes_still_settle`; `clearing_a_session_purges_its_summary_dag`; `removing_a_session_purges_its_summary_dag`; `migration_adds_summary_dag_and_drops_deprecated_table_idempotently`; `conversation_summaries_table_is_dropped`; `idempotent_migration_rerun` |
| `crates/nyx/src/runtime/integration_tests.rs` | `lossless_context_long_conversation_crosses_soft_threshold` (soft compaction, restart, persisted DAG, and post-frontier context) |

The neutral conformance suite must retain the behavioral cases above while
dropping donor-specific SQL, `channel_id` strings, integer IDs, and settlement
policy.

## Historical Agent Runtime pre-cutover baseline

The following records the flat semantic-summary surface that existed before
this approved cutover. It is retained only so migration and recovery can be
checked against a known schema; it is not an active Agent Runtime surface.

### Public API and eligibility

Before cutover, `crates/agent-runtime/src/harness/mod.rs` publicly re-exported
the semantic-summary constants and types from `harness/semantic_summary.rs`:

- `SemanticSummaryCoordinator`, `SemanticSummaryPolicy`, `SummaryModel`,
  `SummaryModelRequest`, and `SummaryModelResponse`;
- `ProtectedSemanticSummary`, `ProtectedSummaryBody`, and
  `protected_semantic_summary_from_state`;
- component/namespace constants, normal and idle purposes, and default policy
  constants.

The pre-cutover public model request was an `Arc<[Message]>`, a purpose, an
idempotency key, and `max_output_chars`; the response was text plus
`UsageDelta`. Debug output reported counts and character/usage metadata, not
message or summary bodies. Its defaults were minimum four completed user
turns, an 85% pressure trigger, two retained recent turns, and an
8,000-character output cap. Policy validation required a positive input budget
and usage limit, a trigger in `1..=100`, `retain_turns < min_turns`, at least
256 summary characters, and rejected `Secret` sensitivity.

Before cutover, eligibility was evaluated after a completed turn. Pressure was
measured from provider-attempt input tokens and excluded the coordinator's own
`UsageSource::SemanticSummary` spend. The coordinator retained the recent
suffix, required complete tool-call/result exchanges, stored the exact source
prefix as a protected artifact before asking the model, and emitted a bounded
fallback category rather than mutating state when the source, artifact, model,
output, or usage check failed. The canonical history was never deleted.

### Protected state and schema

`SEMANTIC_SUMMARY_STATE_SCHEMA_VERSION` is `1`; the state namespace/component
ID is `harness.semantic_summary`. The private JSON value inside a sensitive
`VersionedSessionState` has this exact field set:

```text
schema_version
policy_revision
omit_prefix
source_fingerprint
source_artifact
summary
summary_revision
model_id
model_revision
purpose
sensitivity
```

`ProtectedSemanticSummary` exposes only validated metadata and a
`ProtectedSummaryBody`: `omit_prefix`, source fingerprint and artifact,
summary/model revisions, model ID, purpose, sensitivity, usage, and the
protected body. Its debug representation redacts the body. Validation checks
schema, purpose, non-secret sensitivity, artifact provenance/session, a
non-zero source artifact, source fingerprint, and a content-derived summary
revision. The old coordinator has one prefix boundary and one summary; it has
no independent timeline ID, hierarchical nodes, bounded recursive expansion,
or compare-and-swap DAG revision.

### Projection, context, events, and manifests

The read-only `HistoryProjector` returns `HistoryProjection` with
`omit_prefix`, summary `ContextFragment`s, and `SummaryProvenance`. Semantic
provenance carries the exact source artifact, model purpose/revision, policy
revision, coverage IDs, and sensitivity. The context planner remains the sole
authority for ordering, token accounting, structural compaction, provider
serialization, and cache identity.

There was no dedicated semantic-summary lifecycle event at the pre-cutover
baseline; its public event vocabulary was `SCHEMA_VERSION = 14`:

- successful summary usage is a `RuntimeEvent::Usage` carrying a
  `UsageRecord` whose source is `SemanticSummary`;
- `HarnessEvent::SemanticSummaryFallback { reason }` maps to
  `RuntimeEvent::Downgrade { capability: "semantic_summary", detail: reason }`;
- structural/context compaction is represented separately as
  `RuntimeEvent::ContextCompacted { context, reason, evicted, summaries,
  reclaimed_tokens }`, with summary identity/coverage only;
- events and run manifests contain bounded identifiers, hashes, revisions,
  classifications, counts, and reasons. They do not carry summary text or raw
  source content.

The pre-cutover `MANIFEST_SCHEMA_VERSION` was `1`. Its `RunManifest` recorded
revisioned planning inputs, context segment IDs/kinds/sensitivity/content
hashes/token counts, and `SummaryCoverage` (summary segment ID plus covered
IDs); it never stored fragment text. The current LCM cutover uses manifest
schema v2 and adds redaction-safe lossless records, including producer/purpose,
binding/store/store-view revisions, source fingerprints, and node metadata;
classifier revision remains in protected LCM state.

### Idle and cold-resume recovery

Before cutover, `SessionHandle::try_idle_semantic_compaction` returned
`IdleCompactionAdmission`:

- `Accepted { summary: Option<ProtectedSemanticSummary>, fallback_reason,
  usage }` when the idle boundary was claimed;
- `Busy` when a user/admission/active operation owns the interval or the one
  attempt was already consumed;
- `Shutdown` when shutdown/cancellation wins.

The operation claims the admission and turn gates, enters a cache-activity
guard, runs the hook with a synthetic completed boundary, persists extension
state and usage under the normal persistence gate, and keeps canonical history
unchanged. A persistence failure rolls back in-memory state and usage; a model
failure is an accepted fallback and is not automatically retried. A successful
or failed attempt consumes the idle interval.

The pre-cutover cold-resume seam installed state only for an idle live session
when the active component revision, protected state, artifact session,
omit-prefix boundary, and source fingerprint matched canonical history. That
seam is not a current public API. The implemented cutover instead performs its
schema-v1 import automatically on resume when `.lcm` is configured with a
durable `SessionStore`, requires the coordinator's protected legacy
`ArtifactStore`, validates canonical history/artifact/binding, commits the
replacement LCM leaf and checkpoint before accepting turns, and fails closed
on any mismatch. There is no public/manual restore alias.

### Pre-cutover Agent Runtime semantic-summary tests

The pre-cutover focused tests in
`crates/agent-runtime/src/harness/semantic_summary.rs` are:

`a_long_session_of_small_turns_is_not_summarized`,
`one_large_tool_result_triggers_summarization`,
`a_larger_prefix_does_not_advance_the_trigger`,
`the_floor_protects_a_young_session`,
`summary_spend_does_not_feed_the_trigger`,
`cache_written_tokens_count_toward_context_size`,
`an_unmeasurable_ledger_falls_back_to_the_turn_floor`,
`idle_compaction_forces_the_idle_purpose_and_projects_canonical_history`,
`idle_compaction_reduces_follow_up_input_to_prior_summary_and_delta`,
`session_idle_compaction_returns_protected_summary_and_preserves_history`,
`protected_summary_state_restores_only_at_a_matching_idle_boundary`,
`session_idle_compaction_is_busy_when_a_user_turn_owns_admission`,
`idle_gate_remains_owned_when_a_user_turn_arrives_after_admission`,
`session_idle_compaction_failure_has_no_state_mutation_or_retry`,
`session_idle_compaction_persistence_failure_rolls_back_state_and_usage`,
`session_idle_compaction_returns_shutdown_after_shutdown_begins`,
`a_policy_without_an_input_budget_is_rejected`,
`originals_are_stored_before_a_summary_is_projected`,
`live_pipeline_projects_only_a_validated_summary_and_exact_recent_suffix`, and
`a_failed_summary_falls_back_without_mutating_state`.

The implemented extraction preserves the applicable accounting, redaction,
boundary, rollback, and recovery guarantees through the LCM timeline/DAG and
automatic schema-v1 cutover. These historical test names are not a claim that
the deleted flat module remains in the current source tree.

## Implemented Agent Runtime LCM surface

`agent-runtime-lcm` is directly consumable by a host that implements
`LcmReader` and `LcmWriter` over its own transactional store. The host creates
one `LcmViewAuthority`, shares it with the adapter, and uses its issued
`LcmView` for every operation; the adapter must validate the view before
resolving any opaque identity. `agent-runtime-testkit` exposes
`assert_lcm_store_conformance` for a host adapter, including append
idempotency/gaps, atomic leaf/condensation CAS, bounded expansion, and
unauthorized-view isolation.

The facade composition is `LcmTimelineBinding` plus a
`StaticLcmTimelineResolver` (or host resolver), an `Arc<dyn LcmStore>`, an
`Arc<dyn LcmSummaryModel>`, and `LcmCoordinatorPolicy`, attached once with
`RuntimeBuilder::lcm`. The binding's `SessionId` must match the explicit
`StartSession` id used to resume that host timeline. Soft work is admitted only
by the protected `SessionHandle::try_idle_compaction` boundary; hard work runs
in the pre-provider hook, while the context planner remains the sole final
budget/order/serialization authority.

Each node retains its own model purpose, model revision, escalation level, or
deterministic algorithm provenance. Binding/authorization, store schema,
store-view authorization, and classifier revisions are distinct. Lifecycle
events and manifests remain redaction-safe: bounded identities, fingerprints,
revisions, classifications, counts, usage, and reasons only. Manifest schema
v2 records producer/purpose and binding/store/store-view node metadata; the
protected LCM checkpoint additionally binds the classifier revision.

When `.lcm` is configured on resume with a durable `SessionStore`, schema-v1
state is imported automatically only if the coordinator has its legacy
protected `ArtifactStore`. The cutover checks canonical history, exact artifact
bytes/provenance, and the authorized binding, persists the replacement leaf and
protected checkpoint before any turn is accepted, removes the old namespace
after that durable boundary, and fails closed on mismatch. The importer is
internal; there is no public/manual restore alias.

## Consumer binding and adoption rows

These rows are intentionally status-only. No consumer repository was changed
by this baseline, and no consumer may claim support until its separate
approved change passes its gate at a recorded candidate commit.

| Consumer | Exact consumer baseline | Current binding at baseline | Planned `LcmTimelineId` binding | Conformance/adoption gate | Agent Runtime candidate | Status |
| --- | --- | --- | --- | --- | --- | --- |
| Nyx | `9614842d8f614d7d41e00d8e73ed3d042764d451` | Donor LCM binds `SummaryDagService` to a `channel_id` and SQLite-backed conversation turns; the runtime identity is not opaque or host-authorized. | Bind one opaque timeline to the host-authorized channel/conversation identity. A runtime/backend session replacement must not silently create a new timeline; expansion goes through the same Nyx-authorized view. | `cargo test -p agent-runtime-testkit --test consumer_nyx` plus a separate Nyx proposal that deletes the superseded copy | Working-tree candidate; exact commit is recorded when committed | Neutral gate passed 2026-08-12; not adopted. |
| Smith (`../tui`, remote `ForgeAILab/smith`) | `041f01fb5ca871e7d52447f4877793e432d22f32` | Smith persists Agent Runtime `SessionSnapshot`/manifest/journal state and currently resumes with a host-owned `SessionId`; its baseline protected-summary tests exercise disjoint usage. | Persist a stable root agent-session timeline binding independent of replaceable runtime `SessionId`; child/delegated sessions get separate timelines unless explicitly bound by the host. | `cargo test -p agent-runtime-testkit --test consumer_smith` plus separate host-session conformance | Working-tree candidate; exact commit is recorded when committed | Neutral gate passed 2026-08-12; not adopted. |
| Open Forge | `12f5338fcfb1060ff24fb94c9b367e56b75961ff` | Open Forge currently has task/project/agent/execution identities and adapter-native `agent_session_id`; it has no Agent Runtime LCM binding. | Bind a timeline to the host-authorized Room + AgentIdentity context (with task/execution metadata as policy), never to an adapter-native session ID alone. | `cargo test -p agent-runtime-testkit --test consumer_open_forge` plus a separate Forge recovery/conformance proposal | Working-tree candidate; exact commit is recorded when committed | Neutral gate passed 2026-08-12; not adopted. |

The package's opaque timeline ID grants no read authority by itself. Each row's
host must authorize a view at construction and on expansion. The exact
consumer baselines above are immutable inputs to this extraction; each later
consumer adoption must additionally record the exact Agent Runtime commit that
passed its gate.
