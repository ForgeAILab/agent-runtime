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
silently reduced. Attach `SemanticCompactor` to opt into eviction, tool-result
bounding, and history summarization under configured watermarks.

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

## 6. Event schema version 2

`SCHEMA_VERSION` is now `2`. Nine planning-lifecycle events were added:
registry sealing, scoped-view derivation, model resolution, capability
retrieval and activation, context planning and compaction, cache-plan changes,
and budget failures.

Existing variants are unchanged, and the committed v1 golden fixture still
guards the v1 wire representation. A consumer that matches exhaustively on
`RuntimeEvent` must handle the new variants; one that matches specific variants
needs no change.

## Checklist

- [ ] Declare a `model_profile` or `model_catalog` on every `RuntimeBuilder`.
- [ ] Replace `agent_runtime_prompt` imports with `agent_runtime::context`.
- [ ] Replace `TokenEstimator`/`CharBasedEstimator` with a `RequestSizer`.
- [ ] Wrap any foreign-type `Named` impl in a local newtype.
- [ ] Handle the new `RuntimeEvent` variants if you match exhaustively.
- [ ] Decide a `ContextPolicy` reserve, and whether to attach a compactor.
