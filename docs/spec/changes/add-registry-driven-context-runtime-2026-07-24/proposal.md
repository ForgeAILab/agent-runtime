---
created_at: 2026-07-24T17:50:16Z
updated_at: 2026-07-24T17:50:16Z
---

## Why

The shared runtime can execute registered tools, but it does not yet have one
policy-controlled discovery surface for tools, skills, MCP capabilities,
agents, providers, models, tokenizers, and context policies. It also assembles
provider requests without a single authoritative context budget, compaction
plan, or cache-stability contract. As the capability set grows, advertising
everything to every model turn would waste context, expose entries outside the
active policy scope, and force agents to spend additional turns discovering
predictable capabilities.

The active `add-shared-agent-runtime` change established the direct runtime
baseline. This follow-up formalizes the registry, ability, model-catalog, and
context-management architecture before the first public release, including the
package splits currently being explored in the working tree.

## What Changes

- Add a dependency-light registry kernel with typed, namespaced, revisioned
  entries; deterministic layered resolution; immutable snapshots; and
  policy-scoped views.
- Expose one runtime `RegistryHub` and one agent-facing discovery surface while
  retaining typed registries and typed activation contracts underneath.
- Keep abilities separate from the runtime so tools, skills, MCP endpoints, and
  agents can publish searchable descriptors without constructing executable
  instances or pulling the full runtime dependency graph.
- Add progressive capability disclosure: searchable cards remain outside model
  context, while only a budgeted, policy-approved activation set contributes
  full schemas and instructions.
- Add intent-based capability routing with deterministic keyword and
  affordance matching, optional embeddings, dependency-aware bundle selection,
  automatic pre-activation, and an on-demand discovery fallback.
- Add a layered model catalog that resolves an immutable model profile from
  explicit host configuration, provider/local metadata, embedded metadata,
  cached remote metadata, and an optional models.dev refresh source.
- Replace the isolated prompt builder with a deterministic context engine that
  owns fragment ordering, complete request token accounting, output/reasoning
  reserves, compaction, and cache planning.
- Make registry, model, activation, context, compaction, tokenizer, and adapter
  revisions observable and persistable for deterministic replay.
- Formalize package boundaries: add `agent-runtime-registry` and
  `agent-runtime-context`; keep `agent-runtime-ability` and
  `agent-runtime-provider`; fold `agent-runtime-prompt` into context; and keep
  `agent-runtime-obs` optional and outside the default execution path.
- **BREAKING**: replace direct prompt/history/all-tools request assembly with a
  prepared context-plan contract, and replace process-global mutable
  registration with sealed run-scoped registry views before the first public
  release.

## Non-Goals

- Building a hosted registry service, plugin marketplace, vector database, or
  mandatory embedding service.
- Shipping product-specific prompts, routing rules, approval decisions, memory
  policy, or workspace policy in the shared runtime.
- Sending the complete registry, full skill bodies, or every tool schema to the
  model.
- Allowing discovery, ranking, or activation to bypass approval, credential,
  sandbox, tenant, workspace, or agent policy.
- Treating models.dev as an authoritative source for provider-specific
  tokenization or prompt-cache semantics.
- Mutating the registry snapshot, model profile, or activation set silently in
  the middle of a provider request.
- Implementing a general workflow graph or autonomous multi-agent scheduler.

## Impact

- Affected specs: `registry-foundation`, `capability-routing`, `model-catalog`,
  `context-management`, `runtime-reproducibility`, `package-architecture`
- Related active specs: `runtime-api`, `provider-runtime`, `agent-execution`,
  and `tool-execution` from `add-shared-agent-runtime-2026-07-23`
- Affected code: workspace manifests; the ability, prompt, provider, core,
  runtime, observability, and testkit crates; provider request construction;
  session snapshots; runtime events; and public re-exports
- External data: optional models.dev catalog metadata through an injected HTTP
  transport and host-owned cache; network access is never required to execute a
  turn
- Security impact: registry visibility and activation become explicit
  fail-closed policy boundaries; plugin metadata and remote catalog data remain
  untrusted until validated
- Compatibility impact: pre-1.0 public types for registries, provider request
  preparation, prompt assembly, and session snapshots will change
- Release dependency: the first public release SHALL wait until this change is
  either completed or explicitly removed from the release scope by a new
  approved proposal

## Resolved Decisions

| Topic | Decision |
| --- | --- |
| Discovery surface | One logical `RegistryHub`; typed registries underneath |
| Registry scope | Immutable snapshot plus identity-, policy-, and environment-scoped views |
| Security order | Hard visibility and readiness filters run before retrieval and ranking |
| Ability loading | Descriptor-first and lazy; factories and full instructions materialize only after activation |
| Initial routing | Deterministic keyword, tag, and affordance matching |
| Embeddings | Optional ranking enhancement behind an injected interface |
| Miss recovery | One small agent-facing registry search capability remains available |
| Selection | Dependency-aware complementary bundles under context, risk, cost, and latency budgets |
| Model metadata | Layered local-first catalog with optional cached models.dev fallback |
| Remote refresh | Asynchronous control-plane work; never a request-path dependency |
| Run stability | Freeze registry snapshot, model profile, and activation set for a turn or declared execution phase |
| Context authority | One context planner produces the complete provider-ready plan |
| Compaction | Preserve required semantics and stable prefixes; fail closed if the request still cannot fit |
| Cache handling | Separate local compiled-context caching from provider prompt-cache hints |
| Prompt package | Fold into `agent-runtime-context` before the first public release |
| Observability | Optional sinks remain separate; planning events are neutral core contracts |

## Approval Boundary

Approval authorizes Stage 2 implementation of the registry kernel, ability and
model catalogs, capability routing, context engine, cache-aware compaction,
runtime integration, neutral events, persistence metadata, package migration,
and deterministic conformance coverage in this repository. It does not
authorize consumer-specific routing policy, a hosted registry, production
credentials, consumer-repository migrations, or automatic network access.
