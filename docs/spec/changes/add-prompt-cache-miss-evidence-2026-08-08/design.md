## Context

The current flow has three independent facts:

1. `CachePlanChanged` publishes a cache-plan fingerprint and
   `preserved_prefix_tokens` before a provider request.
2. `ProviderStreamEvent::CacheObservation` carries read/write counts only when
   an adapter chooses to emit it.
3. `UsageRecord` carries request and attempt provenance, but its sparse
   `UsageDelta` intentionally omits zero-valued counters.

The driver forwards the cache observation without request, attempt, or plan
identity. OpenAI, Responses, Gemini, and Smith's separate ChatGPT adapter emit
only for positive reads; Anthropic emits only when either read or write is
positive. Consequently, a zero cannot establish a miss and a consumer cannot
safely join a plan to a retry attempt.

There is one additional trap: `CachePlan::build` treats every declared stable
segment as preserved when no previous plan exists. That is useful for planning
but is not evidence that a first provider request should have read an existing
entry. Cache-miss evidence therefore needs an explicit predecessor/baseline
signal rather than treating `preserved_prefix_tokens` alone as sufficient.

## Goals / Non-Goals

- Goals:
  - preserve provider field presence, including explicit zero;
  - attribute raw and derived evidence to request, attempt, and cache plan;
  - produce one provider-neutral cache-state projection consumers can replay;
  - keep token-count confidence and legacy-journal behavior honest;
  - cover successful, failed, and retried attempts without hiding spend.
- Non-Goals:
  - infer provider TTLs, eviction, or causal expiry;
  - add a cache probe or any additional network request;
  - put presentation policy into the shared runtime;
  - integrate a provider-specific diagnostic beta.

## Decisions

### 1. Field presence lives in `CacheObservation`, not `UsageDelta`

`UsageDelta` remains a set of non-zero disjoint billing counters. Cache
evidence changes to optional values:

- `read_tokens: Some(0)` means the provider explicitly reported no cache read;
- `read_tokens: None` means no cache-read field was reported;
- the same rule applies to `write_tokens`;
- an adapter emits no observation when both values are `None`.

This keeps accounting sparse while preserving the evidence needed for state.
An adapter MUST normalize at most one final cache observation per attempt.

### 2. The driver attaches causal identity

The provider stream stays adapter-local and attempt-scoped. When the driver
publishes a canonical `RuntimeEvent::CacheObservation`, it attaches:

- `request`;
- `attempt`;
- `cache_plan`;
- presence-aware read and write values.

The plan projection is carried beside the `ProviderRequest` into
`run_provider`. It is not reconstructed by scanning prior events.

### 3. A comparable predecessor gates expected reuse

The plan projection retains whether it was built against a prior provider
request's cache plan:

- `expected_read_tokens = None`: no prior request baseline exists;
- `expected_read_tokens = Some(0)`: a prior plan exists, but identity or prefix
  changes leave no reusable prefix;
- `expected_read_tokens = Some(n)`: `n` preserved-prefix tokens are candidates
  for reuse.

The first request therefore remains eligible even if it explicitly reports
zero. A provider/model or cache-policy identity change produces no miss against
the old identity. A retry of the same logical request remains a distinct
attempt associated with the same plan.

### 4. State is derived once per evidence-bearing attempt

The driver emits at most one `CacheStateChanged` after the final usage/cache
observation and before `ProviderAttemptFinished`. A transport failure that
never produced a provider response emits no cache state.

| Condition | Canonical state |
| --- | --- |
| Stable provider caching is unsupported | `unsupported` |
| Caching is supported but the response supplies no cache evidence | `unknown` |
| Evidence is present but no positive reusable expectation exists | `eligible` |
| Expected reuse exists and observed read is smaller | `miss_observed` |
| A positive read or write is observed without a shortfall | `warm_observed` |

When both expectation and read observation exist:

`missed_tokens = expected_read_tokens.saturating_sub(observed_read_tokens)`

The event carries the planner's token-count confidence. `missed_tokens` is
derived evidence, not a provider-reported counter, and MUST NOT be merged into
`UsageDelta`. No shared noise floor is applied; consumers may choose
presentation thresholds without changing the canonical result.

### 5. Missing evidence never becomes zero

A model whose adapter declares cache reporting unsupported remains `unknown`
even when prompt caching itself is supported. A response that unexpectedly
omits its cache fields also remains `unknown`. Only `Some(0)` can participate
in an observed miss.

### 6. Legacy observations are readable but cannot prove a miss

New optional attribution fields deserialize as absent for old journal entries,
and old numeric read/write values deserialize as present values. A legacy
observation may still contribute its positive cache tokens to a compatibility
projection, but without request/attempt/plan correlation it MUST NOT synthesize
a `CacheStateChanged` event or a missed-token count.

Adding the new enum variant and changing Rust field types is still a pre-1.0
breaking API change. Release notes and all supported consumer gates are
required.

### 7. Anthropic Cache Diagnostics is a follow-up

Anthropic's beta diagnostics compares consecutive request fingerprints and can
report structural divergence such as model, system, tools, or messages
changing. It also distinguishes a matching request with a low/zero cache read,
which supports an unavailable-entry interpretation. The feature is
Claude-API-only, best-effort, requires a beta header plus prior response ID, and
briefly stores hashed fingerprints. Those request and retention semantics need
their own approved opt-in change.

This proposal leaves `CacheStateChanged` causal wording neutral. A later
provider-diagnostic event can be correlated by the same request, attempt, and
cache-plan identities without changing the miss calculation.

## Risks / Trade-offs

- Planner token estimates and provider token counts can differ. Confidence is
  carried so consumers do not present a derived difference as provider exact.
- A new public event variant breaks exhaustive Rust matches. Coordinated
  consumer updates are part of the release gate.
- An adapter that emits duplicate observations could double-report evidence.
  Conformance requires one final observation per attempt.
- A provider may read more tokens than the stable-prefix expectation. The
  saturating difference correctly reports no miss and retains the provider's
  larger raw read count.

## Migration Plan

1. Land the core/provider/context event types with legacy deserialization
   fixtures.
2. Update the driver to carry the plan projection and emit attributed evidence
   in deterministic order.
3. Migrate first-party adapters and fake-provider fixtures to presence-aware
   observations.
4. Update observability rendering and all in-repo exhaustive matches.
5. Land the coordinated Smith consumer update and run every consumer contract
   gate before tagging the runtime revision.

## References

- [OpenAI prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching)
  documents explicit `cached_tokens`/`cache_write_tokens` fields, model-specific
  minimum prefix sizes, and retention that differs across model families.
- [Anthropic Cache Diagnostics](https://platform.claude.com/docs/en/build-with-claude/cache-diagnostics)
  documents first-turn zero behavior, structural fingerprint comparison,
  provider-unavailable interpretations, beta status, and platform limits.
- [Pi cache-miss tracking PR](https://github.com/earendil-works/pi/pull/6427)
  is a useful consumer precedent, but its consecutive-usage approximation and
  universal idle heuristic are not the shared-runtime contract.

## Open Questions

None for this approval. Provider-specific diagnostic causes and retention-aware
presentation require separate proposals.
