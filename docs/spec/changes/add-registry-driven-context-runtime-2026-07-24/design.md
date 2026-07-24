## Context

The runtime currently has a deterministic direct provider/tool loop and an
emerging generic ability registry. Provider requests are still constructed from
a static system prompt, complete history, and the complete sealed tool set. The
standalone prompt package estimates rendered strings, but it is not an
authoritative accounting or compaction boundary for provider wire requests.
Provider model descriptors likewise do not form a layered, revisioned catalog.

This is workable for a small fixed tool set. It does not scale to a host that
may install native tools, skills, MCP servers, specialized agents, model
adapters, tokenizers, and context policies. Placing every descriptor and schema
in every request consumes model context and exposes unnecessary capabilities.
Requiring the model to discover obvious capabilities after the first inference
adds latency and provider cost.

The architecture therefore separates the control plane, where entries are
registered, indexed, refreshed, and governed, from the run plane, where an
immutable filtered view is resolved into a small activation set and a complete
context plan.

This change depends on the active `add-shared-agent-runtime-2026-07-23` change.
It extends that runtime rather than editing its completed proposal artifacts.

## Goals / Non-Goals

### Goals

- Give hosts one place to register, filter, inspect, query, and audit every
  runtime capability.
- Give agents one bounded discovery surface for actionable capabilities
  without placing the complete catalog in context.
- Preserve type-specific contracts and minimal dependency graphs behind the
  unified surface.
- Pre-activate likely capabilities before the first model request while
  retaining an on-demand recovery path.
- Make model limits, token accounting, compaction, and cache decisions
  explicit and consistent across every provider request.
- Keep runs stable and replayable even when plugins or remote catalogs change.
- Keep network I/O, embeddings, storage, and provider-specific behavior
  injectable and optional.

### Non-Goals

- Defining consumer-specific intent taxonomies or default routing policy.
- Automatically granting permission because a capability was discovered.
- Requiring a particular embedding model, vector index, tokenizer library, or
  provider cache implementation.
- Persisting secret values or raw sensitive context in planning telemetry.
- Making all registry domains visible or invocable by the model.
- Selecting a different model automatically unless the host grants model
  routing authority.

## Architecture

### Control Plane and Run Plane

```text
registration sources                     optional refresh sources
(built-in, host, plugin, MCP)             (provider, cached models.dev)
             \                                      /
              +---------- RegistryHubBuilder ------+
                                  |
                               seal()
                                  v
                         RegistrySnapshot
                                  |
                 identity + policy + environment
                                  v
                           RegistryView
                                  |
                    resolve model profile
                                  |
             intent + limits + context/tool budget
                                  v
                       CapabilityResolver
                                  |
                         ActivationPlan
                                  |
                   materialize context fragments
                                  v
                          ContextPlanner
                                  |
                   immutable ContextPlan
                                  |
                    provider serialization
```

The control plane may refresh indexes and remote metadata. A run never observes
those mutations directly. It references a sealed snapshot revision and derives
a filtered `RegistryView`. A later refresh is available only to a later turn or
an explicitly declared execution phase.

### Decision 1: Dependency-Light Registry Kernel

Add `agent-runtime-registry` as the lowest-level reusable package. Its default
feature set is synchronous and network-free and does not depend on Tokio, an
HTTP client, a provider SDK, the agent loop, or any consumer package. Optional
serialization must not change registry semantics.

The kernel owns:

- `RegistryId`: a namespaced identity containing domain/kind and name;
- `RegistryRevision`: an immutable descriptor revision;
- `RegistrySource`: built-in, host, plugin, provider, or remote provenance;
- typed builders and sealed registries;
- explicit aliases and alias-cycle detection;
- ordered source layers and deterministic conflict rules;
- compact searchable cards;
- scoped filter inputs and immutable view metadata; and
- snapshot fingerprints.

The registry kernel does not instantiate providers, execute tools, read skill
files, perform network refreshes, or decide host policy.

Duplicates in one layer fail. Cross-layer replacement requires an explicit
override relationship and follows declared precedence. Iteration, query ties,
dependency expansion, and serialization use deterministic ordering.

### Decision 2: One Logical Hub, Typed Stores Underneath

`agent-runtime` composes domain registries into one `RegistryHub`. The hub is
the administrative and discovery facade; it is not an untyped map of live
objects. Each domain retains its own descriptor and factory contracts.

```text
RegistryHub
|- AbilityRegistry: tools, skills, MCP capabilities, agents
|- ProviderRegistry: provider factories and readiness
|- ModelCatalog: model profiles and aliases
|- TokenizerRegistry: tokenizer/counting implementations
`- ContextPolicyRegistry: compactors, summarizers, cache policies
```

The cross-domain index contains compact cards. Resolving a card returns a typed
handle, so callers cannot accidentally invoke a tokenizer as a tool or treat a
model profile as an ability.

The ordinary agent-facing view exposes only actionable ability cards. Internal
domains such as tokenizers and context policies are queryable only through host
APIs. Models and providers are agent-visible only when the host explicitly
grants model-routing authority.

### Decision 3: Descriptor-First Ability Lifecycle

`agent-runtime-ability` remains a separate lightweight package and depends only
on the registry kernel by default. Its optional runtime bridge may depend on
`agent-runtime-core` to adapt executable native tools.

An ability has two layers:

1. A serializable/searchable descriptor containing identity, kind, summary,
   provided affordances, dependencies, conflicts, permissions, risk, readiness
   requirements, estimated activation/context cost, and content revisions.
2. A factory or activation handle that materializes executable behavior, tool
   schemas, skill instructions, MCP connections, or an agent definition only
   after policy approval and selection.

Skill bodies and supporting files are referenced by stable handles and hashes;
they are not loaded into the global search index. MCP discovery occurs in the
control plane and records server/tool revisions. Activating an MCP tool still
performs readiness and authorization checks. An agent descriptor states its
required affordances, model constraints, limits, and context contribution
without starting a sub-agent.

### Decision 4: Policy-Scoped Global Filter

A `RegistryView` is derived from a snapshot using:

- tenant, user, workspace, and agent identity;
- allow/deny and approval policy;
- sandbox and platform compatibility;
- credential/configuration readiness without revealing secrets;
- health and availability state;
- risk, cost, and quota limits;
- model compatibility and modality requirements; and
- explicitly enabled or disabled sources.

Hard filtering happens before keyword search, embedding lookup, scoring, or
dependency expansion. An excluded card's name and metadata are not returned to
the agent. Discovery never grants execution permission; activation and each
invocation retain their existing fail-closed policy checks.

Plugin manifests, skill text, MCP metadata, and remote model records are
untrusted input. Search cards use validated bounded fields and never treat
third-party descriptive text as privileged instructions.

### Decision 5: Progressive Capability Discovery

Capability selection has two paths that share one resolver:

1. Before the first model request, the runtime derives an intent query from the
   current user input and host-provided routing hints. It pre-activates a small
   set under policy and budget.
2. A minimal `registry.search` capability remains available so the agent can
   recover when the initial selection misses or the task changes.

The deterministic baseline uses names, tags, keywords, declared affordances,
input/output media, dependencies, and host routing hints. An optional injected
embedding index may add candidates or rerank them. The system records the
retriever kind, index revision, query fingerprint, candidate revisions, scores,
and final activation set without persisting sensitive query text by default.

Selection is a constrained bundle problem rather than independent top-k
ranking. The resolver favors complementary affordance coverage and satisfies
required dependencies while penalizing redundant capabilities, context cost,
latency, monetary cost, and risk. A research request may therefore select a
search skill plus a browsing MCP tool, or a research agent that already covers
both, rather than injecting all available entries.

The context planner supplies an activation/schema token budget. If no valid
bundle fits, the resolver returns a structured result and leaves the fallback
discovery path available. It never silently exceed limits.

### Decision 6: Layered Model Catalog

The registry exposes a `ModelCatalog` contract that resolves a canonical
`ResolvedModelProfile`. A profile includes:

- provider and model identity plus aliases;
- context, maximum input, and maximum output limits;
- supported modalities and capabilities;
- tokenizer identifier and revision when known;
- provider request-adapter and wire-format revision;
- provider cache-policy identifier and revision when known;
- metadata source, source revision, retrieval time, and confidence; and
- authoritative, inferred, or fallback status for each material field.

Resolution precedence is:

1. explicit session or host override;
2. provider adapter introspection or provider-owned local configuration;
3. embedded known-good metadata;
4. validated host-cached remote metadata; and
5. optional models.dev metadata refreshed for future snapshots.

The optional models.dev source consumes provider-aware catalog data, validates
it into the neutral profile schema, uses conditional/background refresh through
an injected transport, and stores it through a host-injected cache. It is never
called synchronously to unblock a turn. A stale validated record may be used
according to host policy and remains labeled with its source revision and age.

Models.dev may supply limits, modalities, capabilities, and cost metadata. It
does not define exact tokenizer behavior, message framing, prompt-cache marker
placement, cache lifetime, or provider request serialization. Provider and
tokenizer adapters own those facts, and higher-precedence local configuration
wins.

The selected profile is frozen for the execution phase. An unknown model with
no safe input/context limits fails context enforcement and requests explicit
configuration rather than guessing a large window.

### Decision 7: One Authoritative Context Engine

Replace `agent-runtime-prompt` with `agent-runtime-context`. The new package is
deterministic and network-free. It accepts host-supplied policy and content but
owns the reusable mechanism for compiling a complete provider request.

Every contributor produces a `ContextFragment` with:

- stable identity, kind, source, and content revision;
- required/optional status and priority;
- canonical content or a resolved content handle;
- dependency and tool-call/result pairing metadata;
- sensitivity and persistence classification;
- stable, ephemeral, or no-cache classification; and
- token-counting and serialization hints.

Contributors include system/developer instructions, active ability schemas and
instructions, history, tool results, current user input, host memory, retrieved
workspace material, and provider continuation state. Contributors cannot append
directly to the provider request.

`ContextPlanner` consumes a frozen model profile, tokenizer/request-sizer,
activated fragments, history, output/reasoning reserve, and context policy. It
returns an immutable `ContextPlan` containing canonical ordered messages and
tools, per-segment counts, complete input count, reserves, estimation
confidence, compaction decisions, cache plan, and fingerprints. The driver may
only send a provider request produced from this plan. Provider adapters may
serialize the plan but may not add uncounted context.

### Decision 8: Complete Token Accounting

Accounting covers the actual provider representation, including:

- message framing and role overhead;
- tool names, descriptions, and input schemas;
- tool calls and results;
- multimodal content and provider-declared media accounting;
- continuation/reasoning input when exposed by the provider;
- provider adapter framing; and
- requested output and reasoning reserve.

Exact provider/tokenizer sizing is preferred. A fallback estimator reports its
confidence and revision. Policy may reject an estimated plan near the limit.
Provider-reported usage calibrates and audits estimation after a request; it
does not replace preflight accounting.

### Decision 9: Semantic Compaction

Compaction runs before a request exceeds its configured high watermark and
targets a lower watermark to avoid repeated rewriting on every turn. Policy
controls the exact thresholds.

The compactor preserves system/developer constraints, the current user request,
unresolved decisions, required ability instructions, and valid tool-call/result
pairs. It first removes expired or optional fragments, bounds oversized tool
results, and elides reproducible detail. It may then replace older history with
a versioned summary that records provenance and covered message identifiers.

A summary is a new fragment with its own content hash, policy revision, source
references, token count, and sensitivity classification. Compaction never
claims exact preservation silently. If required content plus reserves still
does not fit, planning fails before provider network I/O with an actionable
budget report.

### Decision 10: Cache-Aware Planning

Local compiled-context caching and provider prompt caching are separate. The
planner uses deterministic ordering and places stable system instructions and
stable activated schemas before ephemeral retrieval or turn-specific state
when the provider contract permits it.

The context fingerprint includes at least:

```text
provider/model identity
model-profile revision
tokenizer revision
provider adapter and wire-format revision
registry snapshot and activation-set revisions
prompt/ability/tool-schema revisions
compaction and cache-policy revisions
ordered segment content hashes
```

The cache plan reports the longest preserved stable prefix and which downstream
blocks changed. Provider adapters map neutral stable/ephemeral/no-cache hints to
supported provider behavior. Unsupported hints are observable; they are not
silently treated as guarantees.

Capability activation is resolved before the initial context plan. The active
set remains fixed for a provider request and normally for an execution phase.
Adding a capability creates a new activation/context epoch with a new
fingerprint instead of mutating an in-flight request.

### Decision 11: Reproducibility and Observability

Session and turn persistence records a versioned run manifest containing:

- registry snapshot and scoped-view fingerprints;
- resolved model profile and source revision;
- capability resolver/index revision and activation plan;
- tokenizer, adapter, context, compaction, and cache-policy revisions;
- context segment identifiers, content hashes, counts, and summary coverage;
- context and cache-plan fingerprints; and
- structured reasons for filtering, downgrade, compaction, or budget failure.

Neutral runtime events expose planning milestones and bounded metrics, including
tokens by category, budget/reserve, activated capability IDs, compaction reason,
estimation confidence, and preserved cache-prefix tokens. Events and default
snapshots do not include secrets, raw credentials, or raw sensitive context.

Replay uses the recorded immutable manifest and resolves referenced content by
revision. A mismatch or unavailable required revision fails explicitly unless
the host opts into a labeled non-equivalent replay.

### Decision 12: Package Boundaries

The target workspace is:

| Package | Responsibility | Default dependency constraint |
| --- | --- | --- |
| `agent-runtime-registry` | Generic registry identities, cards, layers, sealing, views, and fingerprints | std-only |
| `agent-runtime-core` | Neutral messages, provider/tool contracts, events, usage, persistence contracts | No provider/network implementation |
| `agent-runtime-ability` | Ability descriptors, skills, dependency graph, activation contracts | Registry only; core bridge optional |
| `agent-runtime-provider` | Provider adapters, request sizing, provider cache semantics, optional catalog sources | Core + registry; injected transport |
| `agent-runtime-context` | Fragments, token accounting, planning, compaction, cache plans | Core + registry; no network |
| `agent-runtime` | Registry hub, routing, activation, sessions, and direct loop | Public facade over mechanisms |
| `agent-runtime-obs` | Optional event sinks and projections | Never required by the execution path |
| `agent-runtime-testkit` | Deterministic fakes and conformance suites | Test/support only |

`agent-runtime-prompt` is folded into `agent-runtime-context` before the first
public release. Existing prompt-section assembly may survive as a compatibility
module inside context if it satisfies the fragment and accounting contracts.

Most hosts depend only on `agent-runtime`. Extension authors may depend on the
smallest relevant package. Heavy remote catalog, database, tokenizer, or
embedding implementations are optional features or host adapters and do not
enter default dependency graphs.

## Risks / Trade-offs

- **Registry becomes a service locator.** Keep descriptors and factories typed;
  the global index returns typed handles rather than arbitrary live objects.
- **Too many packages burden consumers.** Re-export the supported surface from
  `agent-runtime`; leaf packages exist for dependency isolation and extension
  authors, not as mandatory direct dependencies.
- **Intent routing hides a needed capability.** Retain bounded on-demand
  discovery and make filtering/ranking reasons observable.
- **Embeddings reduce determinism.** Make them optional, freeze the index and
  model revision, and record the selected candidate set and scores.
- **Dynamic ability selection damages provider cache reuse.** Resolve before
  request construction, sort deterministically, freeze per phase, and report
  activation epochs explicitly.
- **Remote model metadata becomes stale or unavailable.** Prefer explicit and
  provider-local sources, cache validated revisions, operate offline, and fail
  safely for unknown limits.
- **Token counts differ from provider billing.** Version tokenizers and wire
  sizers, carry confidence, reserve safety margin by policy, and compare with
  provider usage observations.
- **Summaries lose important constraints.** Preserve required classes, retain
  provenance, test invariants, and fail rather than silently dropping required
  content.
- **Registry descriptions contain prompt injection.** Validate bounded cards,
  treat plugin text as untrusted, and materialize instructions only after
  policy-approved activation.
- **Snapshots expose sensitive context.** Store identifiers, hashes, counts,
  and classifications by default; raw content persistence remains host policy.

## Migration Plan

1. Approve this proposal and hold the first public release on the new contracts.
2. Introduce the registry kernel and migrate the existing generic registry
   implementation without changing tool execution behavior.
3. Convert abilities to namespaced, revisioned descriptor/factory pairs and
   adapt the existing native tool registry through compatibility shims.
4. Introduce layered provider/model/tokenizer/context-policy registries and
   seal a run-scoped `RegistrySnapshot` and `RegistryView`.
5. Implement `ResolvedModelProfile`, local/embedded catalog sources, and fake
   catalog fixtures before adding any remote source.
6. Introduce `agent-runtime-context`, migrate prompt sections to fragments, and
   route every provider request through `ContextPlanner`.
7. Add exact/fallback sizing, output/reasoning reserves, semantic compaction,
   and cache planning with deterministic fake adapters.
8. Add capability retrieval, dependency-aware selection, automatic
   pre-activation, and the bounded `registry.search` fallback.
9. Add optional models.dev refresh through injected transport/cache and prove
   offline, stale-cache, invalid-schema, and mid-run-refresh behavior.
10. Extend events and session snapshots with run manifests and replay checks.
11. Remove the standalone prompt crate, finalize facade re-exports, run MSRV and
   default-feature dependency checks, and update migration documentation.
12. Run registry, ability, context, provider, cache, replay, security, and all
   existing consumer conformance suites before tagging the first release.

## Open Questions

- Confirm the permanent public package names before publication; the proposal
  uses the current `agent-runtime-*` working names.
- Select the first exact tokenizer/request-sizer implementations after provider
  conformance fixtures establish their required behavior.
- Confirm models.dev redistribution and attribution requirements before
  embedding any snapshot; remote consumption does not require vendoring one.
