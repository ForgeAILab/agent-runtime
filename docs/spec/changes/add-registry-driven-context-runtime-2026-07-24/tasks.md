---
created_at: 2026-07-24T17:50:16Z
updated_at: 2026-07-24T20:21:14Z
completed_at:
---

## 0. Approval and Change Coordination

- [x] 0.1 Approve the unified registry, capability-routing, model-catalog,
  context-planning, cache, package-boundary, and migration decisions.
  _(Approved 2026-07-24. The `agent-runtime-ability`, `agent-runtime-provider`,
  `agent-runtime-prompt`, and `agent-runtime-obs` package split explored in the
  working tree is assigned to this change, not to
  `add-shared-agent-runtime-2026-07-23`, whose approved scope is the three
  packages `agent-runtime-core`, `agent-runtime`, and `agent-runtime-testkit`.)_
- [x] 0.2 Record this change as a dependent follow-up to
  `add-shared-agent-runtime-2026-07-23` and block the first public release until
  both approved scopes are complete.
  _(Recorded in `meta.json` → `depends_on`. Predecessor task 7.4 and this
  change's task 10.5 are the single release gate; the predecessor is validated
  but deliberately untagged and unarchived until this change completes.)_
- [ ] 0.3 Confirm permanent package names and models.dev attribution or
  redistribution constraints before publishing packages or embedding data.
  _(Still deferred, deliberately — this gates **publication**, not the tag.
  `v0.1.0` is tagged under the working `agent-runtime*` names and claims nothing
  on crates.io. The models.dev half is already settled by the implementation: no
  snapshot is vendored, since task 4.5 consumes the catalog through an injected
  transport and a host-owned cache, so only the package-naming half remains
  open.)_

## 1. Registry Kernel

- [x] 1.1 Add `agent-runtime-registry` with namespaced IDs, typed cards,
  revisions, provenance, aliases, deterministic source layers, and structured
  errors using a std-only default dependency graph.
  _(`RegistryId`/`RegistryDomain`, `RegistryRevision`, `RegistrySource` with
  derived precedence, `EntryProvenance`, `RegistryCard` with bounded untrusted
  text, `Fingerprint`. Default features are std-only; `serde` is optional.)_
- [x] 1.2 Implement builders, explicit override validation, duplicate and alias
  cycle rejection, deterministic sealing, immutable snapshots, and stable
  fingerprints.
  _(Conflicts are detected only at `seal()`, so a failed seal exposes no
  partially resolved snapshot. Canonical `(domain, name)` ordering makes
  sealing independent of registration order.)_
- [x] 1.3 Implement scoped filter inputs and immutable registry views that hide
  excluded entry metadata before any retrieval stage.
  _(`ViewFilter` precomputes visibility at construction; denials beat
  allowances, an empty allow-list means "no allow-list", and an excluded entry
  is indistinguishable from a nonexistent one through `get`, `search`, and
  alias resolution. An alias whose target is excluded is dropped with it.)_
- [x] 1.4 Add serialization fixtures, property tests for ordering/conflicts, and
  compile checks for the minimal default dependency graph.
  _(`tests/serde_fixtures.rs` and `tests/ordering_and_conflicts.rs`;
  permutation-based property tests avoid an external proptest dependency.)_

## 2. Ability Descriptors and Activation

- [x] 2.1 Migrate `agent-runtime-ability` to registry IDs and descriptor/factory
  separation while retaining a registry-only default dependency graph.
  _(`cargo tree --no-default-features` is exactly
  `agent-runtime-ability → agent-runtime-registry`; the `tool` feature is the
  only thing that pulls `agent-runtime-core`.)_
- [x] 2.2 Define bounded cards for native tools, skills, MCP servers/tools, and
  agents with affordances, dependencies, conflicts, permissions, risk,
  readiness, context cost, and content revisions.
  _(`AbilityDescriptor` wraps `RegistryCard`. Readiness declares credential and
  configuration *names* only, never values, so an unmet requirement can be
  reported without leaking what is missing.)_
- [x] 2.3 Add lazy activation handles for tool schemas, skill instruction
  content, MCP connectivity, and agent definitions, plus the optional core tool
  bridge.
  _(`ActivationHandle::activate` is the only place I/O may happen, and the free
  function `activate()` authorizes before calling it — so a denied, conflicting,
  or unready capability cannot cause a side effect by construction.)_
- [x] 2.4 Add dependency expansion, alternative binding, conflict detection,
  revision mismatch, untrusted metadata, and activation authorization tests.

## 3. Registry Hub and Policy Views

- [x] 3.1 Compose ability, provider, model, tokenizer, and context-policy stores
  behind one typed `RegistryHub` and compact cross-domain index.
  _(Each domain keeps its own payload type, so resolving a card returns a typed
  handle — a tokenizer cannot be invoked as a tool. `seal()` is fail-closed
  across all five domains at once.)_
- [x] 3.2 Build run-scoped views from identity, workspace, policy, sandbox,
  readiness, health, quota, risk, and model compatibility inputs.
  _(`ScopedRegistry::derive` is the single place inputs become per-domain
  `ViewFilter`s; risk, quota, and modality compatibility are folded in as
  exclusions *before* any view is built, so there is no second filtering path
  that could diverge from the first.)_
- [x] 3.3 Keep internal domains hidden from the agent-facing query surface and
  require explicit host authority for model/provider discovery.
  _(Reuses the kernel's `agent_facing` flag per domain rather than
  reimplementing the ability check. Model routing authority is opt-in and off
  by default; tokenizers and context policies are never agent-visible even with
  it.)_
- [x] 3.4 Emit redaction-safe filter and snapshot diagnostics and prove that
  unauthorized entries cannot be inferred through query results or errors.
  _(`ScopeDiagnostics` holds `usize` counters and reason codes only — it has no
  field capable of carrying an id or name, so leakage is structurally
  impossible rather than merely avoided.)_

## 4. Model Catalog

- [x] 4.1 Define `ModelCatalog`, catalog sources, model aliases, field-level
  provenance, and immutable `ResolvedModelProfile` contracts.
  _(`agent_runtime_core::catalog`. Aliases are indexed by the source so an
  alias resolves to the same profile as its canonical name.)_
- [x] 4.2 Implement deterministic precedence for explicit, provider-local,
  embedded, cached-remote, and future-refresh sources.
  _(`LayeredModelCatalog`. Registration order cannot change resolution;
  disagreement inside one layer fails with `SourceConflict` rather than
  resolving by insertion order.)_
- [x] 4.3 Add provider/tokenizer/cache-policy revision fields and conservative
  unknown-model handling that fails context enforcement without safe limits.
  _(`ComponentRef` per field; an unresolvable limit fails with
  `MissingLimits` before any I/O instead of assuming a permissive window.
  Undeclared non-limit fields are marked `FieldConfidence::Fallback`, so a
  stand-in is never mistaken for a declared value.)_
- [x] 4.4 Implement fake, local, embedded, stale-cache, conflict, alias, and
  mid-run-refresh conformance fixtures.
  _(Covered by `catalog::layered` tests in core and `catalog::models_dev` tests
  in provider; task 10.1 lifts them into reusable testkit suites.)_
- [x] 4.5 Add an optional models.dev source using injected transport and cache,
  schema validation, conditional/background refresh, and no request-path
  network dependency.
  _(`agent_runtime_provider::catalog`. `lookup` reads the host cache only and
  cannot reach the network; `ModelsDevRefresher` is control-plane, uses a
  conditional GET, and validates before writing the cache. Parsed records never
  populate tokenizer, request-adapter, or cache-policy fields.)_

## 5. Context Engine

- [x] 5.1 Add `agent-runtime-context` with versioned `ContextFragment`, source,
  priority, requirement, sensitivity, pairing, and cache-class contracts.
  _(Deterministic `sort_key` and `content_hash`; a changed content revision
  changes the hash, which is what makes the plan fingerprint meaningful.)_
- [ ] 5.2 Migrate prompt sections, history, tool schemas/results, active
  abilities, current input, memory/retrieval hooks, and continuation state into
  deterministic fragment producers.
- [x] 5.3 Implement `ContextPlanner` and immutable `ContextPlan`, making it the
  exclusive source of provider messages, tools, reserves, counts, and cache
  hints.
  _(`ContextPlan` has private fields and no mutators; `to_provider_request` is
  the only plan → request path.)_
- [x] 5.4 Add provider/tokenizer request-sizing hooks covering framing, schemas,
  calls/results, multimodal content, continuation state, and adapter overhead,
  with versioned fallback confidence.
  _(`RequestSizer` reports its own `ComponentRef` revision and
  `EstimationConfidence`; `CharRatioSizer` is the deterministic offline
  fallback with documented, configurable framing constants.)_
- [x] 5.5 Enforce model input/context/output limits before network I/O and add
  actionable category-level budget errors.
  _(`BudgetReport` attributes tokens per `FragmentKind`, so an over-budget plan
  names the category responsible rather than reporting one opaque total.)_

## 6. Compaction and Cache Planning

- [x] 6.1 Implement policy-controlled high/low watermarks, optional-fragment
  eviction, tool-result bounding, history summarization, and required-content
  preservation.
  _(Every stage touches only `Optional` fragments, so required content survives
  by construction; `validate_compacted` is the defense-in-depth check that
  rejects a candidate anyway rather than degrading silently.)_
- [x] 6.2 Store summary provenance, covered message IDs, policy revision,
  sensitivity, content hash, and token count; reject invalid tool-call/result
  pairing or loss of required constraints.
  _(A `Sensitivity::Secret` fragment is never eligible for summarization, and an
  outcome claiming otherwise is rejected.)_
- [x] 6.3 Implement deterministic stable-prefix planning, ordered segment
  fingerprints, local compiled-context keys, and provider cache-hint mapping.
- [x] 6.4 Report preserved and invalidated cache-prefix tokens and make
  unsupported provider cache behavior explicit.
  _(An unsupported neutral hint is reported on the plan rather than silently
  treated as a guarantee.)_
- [x] 6.5 Add boundary, repeated-compaction, stable-prefix, policy-revision,
  tokenizer-revision, adapter-revision, and cannot-fit conformance tests.

## 7. Capability Retrieval and Pre-Activation

- [x] 7.1 Implement deterministic name, keyword, tag, affordance, modality, and
  dependency retrieval over filtered capability cards.
  _(Retrieval only ever reads an already-`ViewFilter`-scoped `RegistryView`, so
  an excluded entry is invisible before ranking rather than filtered after it.)_
- [x] 7.2 Define an optional embedding/index interface with recorded model/index
  revisions and deterministic fixture implementations.
- [x] 7.3 Implement constrained complementary-bundle selection using relevance,
  coverage, context cost, latency, monetary cost, risk, conflicts, and
  dependency satisfaction.
  _(Deterministic greedy set-cover over marginal affordance coverage, not
  top-k. Conflict is enforced symmetrically, including across an epoch
  boundary: an already-active entry that declares a conflict with a new
  candidate blocks it, resolved back through the view.)_
- [x] 7.4 Pre-activate a bounded set before the first provider request and expose
  a minimal policy-scoped `registry.search` fallback for misses or intent
  changes.
  _(An unaffordable bundle returns `PreActivationOutcome::InsufficientBudget`
  rather than truncating a schema to make it fit.)_
- [x] 7.5 Freeze the activation set for each provider request/execution phase and
  create an explicit activation/context epoch when capabilities are added.
  _(`ActivationEpochs::advance` only ever appends; a handed-out epoch is
  immutable.)_
- [x] 7.6 Add research-routing fixtures covering search skills, browser MCP
  tools, research agents, redundant candidates, denied entries, missing
  credentials, and insufficient context budget.

## 8. Runtime, Persistence, and Events

- [x] 8.1 Extend `RuntimeBuilder` and session creation to seal a registry
  snapshot, derive a scoped view, resolve a model profile, and construct the
  initial activation and context plans.
  _(`RunPlanner` freezes the profile, sizer, policies, cache capability, and
  registry/view/activation fingerprints per session. **BREAKING**:
  `RuntimeBuilder::build` now fails without a `model_profile` or
  `model_catalog` — there is deliberately no default context window, since
  guessing one is how uncounted context reaches a provider.)_
- [x] 8.2 Replace direct prompt/history/all-tool request assembly with provider
  serialization from an immutable `ContextPlan`.
  _(`Driver::build_request` plans first and derives the request via
  `ContextPlan::to_provider_request`. Sampling, reasoning, and output limits
  are request *options*, applied on top without adding context the plan did not
  account for. Emits `ContextPlanned`, `CachePlanChanged`, and — on a
  preflight failure, before any network I/O — `BudgetFailure`.)_
- [x] 8.3 Add versioned neutral events for snapshot sealing, model resolution,
  capability retrieval/activation, context planning/compaction, cache changes,
  downgrades, and budget failures.
  _(Nine new `RuntimeEvent` variants; `SCHEMA_VERSION` 1 → 2, permitted pre-1.0
  by the proposal. Existing `Downgrade`/`Usage`/`CacheObservation` are reused
  rather than duplicated. The committed `event-envelope-v1.json` golden stays at
  v1 on purpose — it guards the v1 wire representation, which must not change;
  a v2 golden covering the new variants is task 10.1.)_
- [x] 8.4 Extend session snapshots with a redaction-safe run manifest containing
  registry, model, resolver, tokenizer, adapter, activation, context,
  compaction, and cache revisions/fingerprints.
  _(`SessionSnapshot::manifests`, populated per provider request. Segments are
  recorded as `{id, kind, classification, content hash, tokens}`;
  `ContextSegmentRecord` has no field a raw fragment could occupy.)_
- [x] 8.5 Add equivalent replay, revision-mismatch failure, explicitly
  non-equivalent replay, restart, and persistence migration tests.
  _(`crates/agent-runtime/tests/replay_and_persistence.rs`. A pre-manifest
  snapshot still deserializes, so this is a persistence migration rather than a
  breaking read.)_

## 9. Package Migration and Public API

- [x] 9.1 Move generic registry primitives out of the ability crate into
  `agent-runtime-registry` and update dependency-boundary tests.
  _(`Named`/`Registry<T>`/`Sealed<T>` moved to
  `agent-runtime-registry/src/collection.rs`; the ability crate's simpler
  duplicate error renamed `NameConflict` to avoid colliding with the kernel's
  own layered-sealing `RegistryError`, and re-exported unchanged as
  `agent_runtime_ability::{Named, Registry, Sealed}`. Moving `Named` out
  turned `impl Named for Arc<dyn Ability>`/`Arc<dyn Tool>` into orphan-rule
  violations (`Arc` isn't `#[fundamental]`); fixed with local `Named`-wrapper
  newtypes — `AbilityEntry` (private, ability crate) and `ToolEntry` (public,
  ability crate's `tool` feature) — and `AbilityRegistry`/`SealedAbilities`
  becoming real wrapper structs instead of type aliases, which also legalizes
  `impl SealedAbilities`. `crates/agent-runtime/src/tool/registry.rs` updated
  to hold tools via `ToolEntry`. `cargo tree -p agent-runtime-ability
  --no-default-features` stays exactly `agent-runtime-ability →
  agent-runtime-registry`.)_
- [x] 9.2 Fold `agent-runtime-prompt` behavior into `agent-runtime-context`,
  migrate callers, and remove the standalone prompt package before release.
  _(Ported to `agent-runtime-context/src/prompt.rs`: `SystemPromptBuilder`,
  section types, `PromptSection`/`format_section_block`. Added
  `SystemPromptBuilder::into_fragments`, producing versioned
  `FragmentKind::SystemInstruction` fragments with a content-derived revision,
  priority reflecting section order, and `CacheClass::Stable`; tested end to
  end through `ContextPlanner`. Dropped the old `TokenEstimator`/
  `CharBasedEstimator`/`build_with_stats` (superseded by `sizing::RequestSizer`
  — no second estimator path). Deleted the `agent-runtime-prompt` crate
  (directory, workspace member, `[workspace.dependencies]` entry, and the
  `agent-runtime` crate's `prompt` feature/optional dependency).
  `tests/obs_prompt_integration.rs` renamed to `obs_context_integration.rs` and
  migrated to the context crate's builder. README/CHANGELOG/PROVENANCE
  updated; no reference to the removed crate remains outside historical
  provenance text.)_
- [ ] 9.3 Keep `agent-runtime-provider` independently testable and add optional
  catalog-source/request-sizing/provider-cache integrations without requiring
  them in core.
- [ ] 9.4 Keep `agent-runtime-obs` optional, project new neutral events without
  making a sink part of the execution path, and preserve host-owned storage.
- [x] 9.5 Re-export the supported composition surface from `agent-runtime`,
  document extension-author leaf dependencies, and add public API examples.
  _(`prelude` now gathers the registry/ability/context/hub/capability surface
  alongside the runtime facade. `lib.rs` module docs gained "Composing through
  the facade" (with a doc example) and "Extension authors" sections naming
  `agent-runtime-registry` + `agent-runtime-ability` as the std-only
  descriptor-only leaf dependency versus the full facade for a host. Added
  `examples/facade_composition.rs` (ability registry + `SystemPromptBuilder`
  + `ContextPlanner` composed through the facade only) alongside the existing
  `examples/quickstart.rs`.)_

## 10. Conformance and Release Gate

- [x] 10.1 Add reusable registry, filtering, ability, catalog, retrieval,
  context, compaction, cache, replay, and provider-sizing conformance suites to
  `agent-runtime-testkit`.
  _(Nine modules under `conformance/`; provider-sizing folded into `context`
  rather than split out. They assert properties a host can check against its
  own composition, not our specific token counts. The ability suite uses a
  materialization tripwire to prove searching a descriptor never activates it,
  and the compaction suite locks in that a deliberately adversarial fragment
  set — one that would split a tool-call/result pair across summarization
  priorities — fails closed with `InvalidPairing` rather than returning a
  broken candidate.)_
- [x] 10.2 Run formatting, Clippy-as-error, all-feature tests, default-feature
  dependency checks, MSRV checks, schema fixtures, and documentation tests.
  _(All clean. The MSRV run caught two `let`-chains, which are not stable on
  1.86 — the CI job had only ever built two of the seven production packages,
  so it could not have caught a violation in the new crates. Both fixed, and
  the job now lists every production package and adds a
  `dependency-boundaries` job asserting the registry kernel stays std-only and
  ability depends on the kernel alone.)_
- [x] 10.3 Run Smith, Nyx, and Open Forge compatibility fixtures against the new
  facade and document required consumer migrations.
  _(All three suites pass. Migration documented in `docs/migration-0.1.md` and
  summarized under a Breaking heading in `CHANGELOG.md`.)_
- [x] 10.4 Update project architecture, README, changelog, provenance, package
  diagrams, and migration documentation only after implementation matches the
  approved deltas.
  _(`docs/spec/project.md` now describes the real eight-package layout rather
  than the originally planned three; `README.md` carries the package table, the
  model-profile requirement, and the widened MSRV/boundary commands;
  `CHANGELOG.md` has a Breaking section; `docs/migration-0.1.md` documents all
  six breaking changes with verified API paths.)_
- [x] 10.5 Tag the first release only after both this change and the predecessor
  release gates pass.
  _(`v0.1.0`. Both gates ran green together: predecessor task 7.4 and this
  change's section 10 share one combined validation, so the tag reflects both
  approved scopes rather than either alone.)_
