---
created_at: 2026-08-08T22:32:53Z
updated_at: 2026-08-09T01:28:05Z
completed_at: 2026-08-09T01:28:05Z
---

## 1. Core contracts

- [x] 1.1 In `crates/agent-runtime-core/src/provider.rs`, make provider cache
  observations presence-aware with optional read/write counts and require at
  least one present field.
- [x] 1.2 In `crates/agent-runtime-core/src/event.rs`, add the canonical cache
  state vocabulary and `CacheStateChanged` payload with request, attempt,
  cache-plan, expectation, observation, missed-token, and confidence fields.
- [x] 1.3 Add optional request, attempt, and cache-plan attribution to the
  canonical `CacheObservation` event while preserving deserialization of the
  legacy numeric event shape.
- [x] 1.4 Keep `UsageDelta` sparse and non-zero; add tests proving explicit
  cache zero is evidence metadata rather than a billing counter.

## 2. Comparable cache expectation

- [x] 2.1 In `crates/agent-runtime-context/src/cache.rs`, retain whether a plan
  had a prior comparable provider-request baseline without changing local
  compiled-context identity.
- [x] 2.2 Project `None` for a first request, `Some(0)` for a prior but
  non-reusable identity/prefix, and `Some(n)` for a preserved reusable prefix.
- [x] 2.3 Add planner tests for first request, unchanged prefix, changed model
  identity, changed tool/schema segment, compaction, and provider-cache
  unsupported cases.

## 3. Driver correlation and ordering

- [x] 3.1 Carry the exact cache-plan projection beside the provider request
  from `agent/driver/turn.rs` into `agent/driver/provider.rs`.
- [x] 3.2 Attach logical request, provider attempt, and cache-plan fingerprint
  when forwarding each raw cache observation.
- [x] 3.3 Accumulate at most one final presence-aware observation per attempt
  and emit `CacheStateChanged` after usage/cache evidence and before
  `ProviderAttemptFinished`.
- [x] 3.4 Implement the state table and saturating missed-token calculation
  without a TTL, noise floor, or additional provider request.
- [x] 3.5 Cover successful attempts, failed attempts with reported usage,
  retry attempts, cancellation before response evidence, and provider reads
  greater than the planned expectation.

## 4. First-party adapters

- [x] 4.1 Update OpenAI Chat Completions and Responses parsing to preserve
  present zero `cached_tokens` and `cache_write_tokens` independently.
- [x] 4.2 Update Anthropic parsing to preserve independent presence of
  `cache_read_input_tokens` and `cache_creation_input_tokens`.
- [x] 4.3 Update Gemini parsing to distinguish an absent cached-token field
  from an explicit zero.
- [x] 4.4 Update the deterministic fake provider and provider conformance
  fixtures with explicit-zero, omitted, read-hit, write, and partial-hit
  cases.
- [x] 4.5 If the native xAI Responses adapter has landed, enroll its cached
  token fields in the same conformance cases.

  _Not applicable to this candidate: no native xAI Responses adapter is
  present in the workspace._

## 5. Observability and compatibility

- [x] 5.1 Update `agent-runtime-obs` rendering for attributed observations and
  the canonical state event without logging prompt content or vendor bodies.
- [x] 5.2 Add JSON/schema fixtures proving old observations deserialize and
  new observations round-trip with zero/absence intact.
- [x] 5.3 Add runtime conformance tests for event order, exact attribution, one
  state event per evidence-bearing attempt, and no state for a pre-response
  transport failure.
- [x] 5.4 Update every in-repo exhaustive `RuntimeEvent` match and document the
  pre-1.0 breaking contract in the changelog/release notes.
- [x] 5.5 Run the Smith, Nyx, and Open Forge compatibility suites against the
  candidate revision; land
  `tui:add-prompt-cache-miss-visibility-2026-08-08` before release.

  _The in-repo Smith, Nyx, and Open Forge consumer suites pass. Smith change
  `fd033dbf705281ee473a101b623b8f02bf2dde08` is landed and pins approved
  Runtime revision `0a07231649d81ccb40f2395a9924f8bd6027baf9`._

## 6. Verification

- [x] 6.1 `cargo fmt --all --check`.
- [x] 6.2 `cargo clippy --workspace --all-targets -- -D warnings`.
- [x] 6.3 `cargo test --workspace`.
- [x] 6.4 Re-run strict spec validation for
  `add-prompt-cache-miss-evidence-2026-08-08`.
