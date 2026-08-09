---
created_at: 2026-08-08T22:32:53Z
updated_at: 2026-08-09T01:28:05Z
---

## Why

Agent Runtime plans provider-cache reuse and normalizes cache token counts, but
its event contract cannot currently prove a miss. First-party adapters emit a
`CacheObservation` only when a cache read or write is positive, so an explicit
provider-reported zero is indistinguishable from a response that carried no
cache fields. The resulting runtime event also lacks request, attempt, and
cache-plan identity, leaving consumers to correlate turn-level plan events and
positive-only observations heuristically.

That gap is now user-visible in Smith. Its shared planner knows which stable
prefix survived, while provider usage knows how many tokens were actually read,
but neither repository owns the canonical comparison. Copying Pi's
previous-prompt approximation into each consumer would discard Agent Runtime's
stronger plan evidence and would let TUI, headless, retry, and replay behavior
drift.

## What Changes

- Make provider cache observations presence-aware. A reported zero becomes
  `Some(0)`; an omitted field remains `None`; an observation is emitted only
  when at least one cache field was present on the wire.
- Correlate every canonical cache observation with its logical request,
  provider attempt, and exact cache-plan fingerprint.
- Retain whether a cache plan had a comparable predecessor, so the first
  eligible request and a changed provider/model identity are not classified as
  misses.
- Add a canonical, attempt-scoped `CacheStateChanged` runtime event carrying
  `unsupported`, `unknown`, `eligible`, `warm_observed`, or
  `miss_observed` together with expected, observed, missed, and provenance
  fields.
- Compute missed tokens as the saturating difference between the comparable
  preserved-prefix expectation and the provider-reported cache read. No
  provider-independent TTL or token-noise threshold enters the mechanism.
- Update first-party adapters, the fake provider, observability rendering,
  serialization fixtures, and conformance suites for explicit zero, omission,
  retries, first requests, identity changes, and partial misses.
- **BREAKING**: the Rust shape of `CacheObservation` changes and
  `RuntimeEvent` gains a variant. Legacy serialized observations remain
  readable, but exhaustive consumers require a coordinated dependency update.

## Dependencies and Coordination

- Depends on `wire-provider-prompt-cache-2026-08-03`, which establishes
  `PromptCacheControl` and the serving adapter's cache capability.
- Coordinates with
  `tui:add-prompt-cache-miss-visibility-2026-08-08`, which consumes the new
  event without reimplementing the comparison.
- `add-provider-rate-limit-snapshots-2026-08-04` touches the same event and
  provider-loop files but has no semantic overlap; implementation must rebase
  rather than replace its attempt attribution.
- If `add-native-xai-responses-provider-2026-08-03` lands first, its cache
  fields join the same presence-aware provider conformance suite.

## Impact

- Affected specs: `provider-runtime`, `context-management`,
  `agent-execution`, `compatibility-contract`
- Affected code: `agent-runtime-core/src/{provider,event}.rs`,
  `agent-runtime-context/src/cache.rs`,
  `agent-runtime/src/agent/driver/{provider,turn}.rs`,
  `agent-runtime-provider/src/{openai,responses,anthropic,gemini}.rs`,
  `agent-runtime-obs/src/render.rs`, and `agent-runtime-testkit`
- Affected consumers: Smith immediately; Nyx and Open Forge through the normal
  consumer-compatibility gate
- Network behavior: no additional request and no pre-request cache probe

## Out of Scope

- Cache keepalives, prewarming, retention selection, or cache eviction policy
- A universal five-minute expiry heuristic or any claim that elapsed time
  verifies expiry
- User-interface wording, notice thresholds, settings, or cost presentation
- Anthropic's beta Cache Diagnostics request/response integration; that feature
  requires separate opt-in, response-id threading, data-retention review, and
  Claude-API-only adapter policy
- Changing cache breakpoint placement or GPT-5.6 prompt-cache controls

## Approval Boundary

Approval authorizes presence-aware cache observations, plan/attempt
correlation, the canonical state projection and its compatibility migration,
and the conformance work listed in `tasks.md`. It does not authorize provider
diagnostic APIs, cache-retention policy, synthetic cache traffic, consumer UI,
or changes to which prompt bytes a provider caches.
