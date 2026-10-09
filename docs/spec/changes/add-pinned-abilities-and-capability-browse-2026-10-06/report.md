# Runtime implementation report

Implemented on `feat/smith-capability-discovery`. Changes are unstaged and uncommitted.

## Public API additions

All paths below are relative to the `agent_runtime` facade. Existing public signatures and public struct layouts remain source-compatible.

`runtime::RuntimeBuilder`:

```rust
pub fn pinned_abilities(mut self, ids: impl IntoIterator<Item = RegistryId>) -> Self
```

Empty input leaves the existing routing mode unchanged. Nonempty input enables live routing; repeated calls accumulate ids, deduplicated when sealed. Unknown and scope-excluded pins are skipped. Visible pins and their dependency closures are authorized and materialized before retrieval. Pins spend schema/instruction tokens, but no candidate slots. `registry.activate` also spends no candidate slot, preserving the original cardinality accounting for `registry.search`.

`hub::ScopeInputs`:

```rust
pub fn deny_pattern(mut self, pattern: impl Into<String>) -> Result<Self, String>
pub fn allow_pattern(mut self, pattern: impl Into<String>) -> Result<Self, String>
```

These setters are fallible so invalid restrictions cannot silently disappear. Allow patterns union with allowed ids; deny wins. Existing domain/source/readiness/risk/quota restrictions still apply, with both protected bootstraps always visible.

`hub::CapabilityPattern` (also re-exported from `prelude`):

```rust
pub struct CapabilityPattern { /* private validated fields */ }
impl CapabilityPattern {
    pub fn parse(pattern: &str) -> Result<Self, String>
}
```

Accepts the built-in registry domain names or `*`, and a nonempty local name containing zero or more `*` wildcards. Missing separators, unknown domains, empty names, and additional separators are rejected.

`runtime::session::SessionHandle` (also exported as `runtime::SessionHandle`):

```rust
pub fn capability_catalog(&self) -> Vec<crate::capability::CapabilityCatalogEntry>
```

Returns a read-only projection of the sealed ability registry and current session epoch. Pending or staged entries remain `Available` until applied. With live routing disabled there is no session ability registry and the catalog is empty. Denied metadata is exposed only through this host accessor, never through agent discovery.

`capability` (both types also re-exported from `prelude`):

```rust
pub enum CapabilityState {
    Active,
    Available,
    Denied,
}

pub struct CapabilityCatalogEntry {
    pub id: RegistryId,
    pub summary: String,
    pub kind: agent_runtime_ability::AbilityKind,
    pub state: CapabilityState,
}
```

`capability::CapabilityResolver`:

```rust
pub fn retrieve_descriptive(
    &self,
    view: &RegistryView<AbilityDescriptor>,
    query: &RoutingQuery,
) -> RetrievalResult
```

`capability::retrieval`:

```rust
pub fn retrieve_descriptive(
    view: &RegistryView<AbilityDescriptor>,
    query: &RoutingQuery,
    embedding: Option<&dyn EmbeddingIndex>,
) -> RetrievalResult
```

`harness`:

```rust
pub const CAPABILITY_ACTIVATE_TOOL_NAME: &str = "registry.activate";
pub struct CapabilityActivateTool;
```

The marker implements `Tool`, with the existing trait signatures:

```rust
fn spec(&self) -> ToolSpec
async fn invoke(
    &self,
    prepared: PreparedToolCall,
    ctx: &InvocationContext,
) -> Result<ToolOutcome, RuntimeError>
```

Its effects and permissions are empty. Actual activation is intercepted at the post-provider boundary. Direct marker invocation fails closed, just like `CapabilitySearchTool`.

## Existing public contracts with changed behavior

`harness::CapabilitySearchTool::spec(&self) -> ToolSpec` now advertises optional `query`, `domain`, and nonnegative `offset`; `max_results` remains bounded to 1..=8. Empty or absent queries list authorized non-bootstrap entries ordered by domain name and id, without a transaction or epoch change. Nonempty queries retain `cards`, `staged`, `already_available`, and `available_on`, add `by_domain`, and add `matched: 0` plus a listing hint on a retrieval miss. Domain filtering restricts candidates, while domain counts describe the full authorized non-bootstrap catalog.

The new activation tool takes 1..=8 qualified `ids` and returns `staged`, `already_available`, per-id `rejected` reasons, and `available_on: "next_provider_request"`. Unknown and unauthorized ids are indistinguishable. Token, policy, dependency, and materialization failures reject that id independently. It uses the existing `SearchStageGuard` and emits the existing `CapabilitiesActivated` event when pending entries are applied.

The following existing Rust signatures are unchanged:

```rust
// hub::RegistryHub
pub fn scoped(&self, inputs: &ScopeInputs) -> ScopedRegistry

// capability::retrieval
pub fn retrieve(
    view: &RegistryView<AbilityDescriptor>,
    query: &RoutingQuery,
    embedding: Option<&dyn EmbeddingIndex>,
) -> RetrievalResult
pub const DETERMINISTIC_RETRIEVER_REVISION: &str = "capability-retrieval.deterministic.v2";

// capability::preactivation
pub fn registry_search(
    view: &RegistryView<AbilityDescriptor>,
    query: &RoutingQuery,
    embedding: Option<&dyn EmbeddingIndex>,
    max_results: usize,
) -> DiscoveryResult

// capability::selection
pub fn select(
    view: &RegistryView<AbilityDescriptor>,
    candidates: &[RetrievedCandidate],
    budgets: &SelectionBudgets,
    costs: &BTreeMap<RegistryId, CapabilityCostHint>,
    already_active: &[RegistryId],
) -> ActivationPlan

// capability::CapabilityResolver
pub fn retrieve(&self, view: &RegistryView<AbilityDescriptor>, query: &RoutingQuery) -> RetrievalResult
pub fn registry_search(&self, view: &RegistryView<AbilityDescriptor>, query: &RoutingQuery, max_results: usize) -> DiscoveryResult
pub fn select(&self, view: &RegistryView<AbilityDescriptor>, candidates: &[RetrievedCandidate], budgets: &SelectionBudgets, already_active: &[RegistryId]) -> ActivationPlan
```

`scoped` enforces patterns and bootstrap visibility. Retrieval ties put tools first, then registry ids. `registry_search` uses descriptive retrieval. Selection checks instruction tokens when considering dependency alternatives, so an oversized alternative does not prevent choosing a later fitting alternative. Resolver methods inherit those changes.

`capability::MatchReasons` retains its existing public fields. Descriptive matches are recorded in `keywords` as `text:<whole-word>` with weight 1, below keyword weight 4, avoiding a new field that would break callers' struct literals. Descriptive words come from card title/display name and summary; this repository stores an ability's description in its bounded card summary, without a separate descriptor description field. Queries and text are lowercased and split on non-alphanumeric characters; words under three characters and a small stop-word list are ignored.

## Decisions and deviations

Description matching is **explicit-search-only**. `ensure_initial_activation`, `pre_activate`, and ordinary `retrieve` retain metadata-only scoring. A conformance test gives pre-activation ample instruction budget and proves it does not bind description-only reference skills. This avoids relying on the instruction budget to prevent noisy activation.

The brief's byte-for-byte default requirement conflicts with requiring `registry.activate` in every live session's first epoch and changing the search schema/retriever revision. The ordinary non-live default, including `.pinned_abilities([])`, remains unchanged. Live routing necessarily has a second bootstrap and new search metadata. One existing unit epoch expectation and two existing provider-schema expectations were updated for that required bootstrap. Serialized event fixtures were not edited.

No other intended behavior was omitted. Pattern setters return `Result` to enforce the specified rejection of invalid patterns. Descriptive reasons reuse the existing public struct layout for source compatibility.

## Validation

Each required command used `CARGO_BUILD_JOBS=6`, redirected output to a file, and printed the original exit code without piping the command through `head` or `tail`.

| Command | Exit code | Result |
| --- | --- | --- |
| `cargo fmt --all -- --check` | 0 | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | 0 | Passed |
| `cargo test --workspace --locked --no-fail-fast` | 0 | 1,228 passed, 0 failed, 1 ignored across 46 test/doctest groups |

`session::one_shutdown_deadline_bounds_all_active_turns` passed on the workspace run; no flake retry was needed. Smith consumer conformance also passed. Strict spec validation reports `Valid`; all six tasks are checked and the change is stamped complete. `git diff --check` passed.

Logs: `/private/tmp/cap-fmt-check.log`, `/private/tmp/cap-clippy.log`, and `/private/tmp/cap-workspace-tests.log`.

## Smith adoption

- Register executable tools as before, then call `.pinned_abilities(core_ids)` for the host's core set. Nonempty pins enable live routing automatically. Supply activation authorization/readiness facts as before.
- Keep the desired candidate limit (for example 8); reserve schema/instruction tokens for pins, dependencies, both bootstraps, and later discoveries. Pins do not consume candidate slots. Pinned over-budget errors surface during session derivation/startup.
- Build explicit session restrictions with `ScopeInputs::deny_pattern(...) ?` and/or `allow_pattern(...) ?`, then pass the resulting `ScopeInputs` to the builder. The host can call `CapabilityPattern::parse` to validate policy input independently.
- Render `session.capability_catalog()` for host UI. Let the model use `registry.search` with `{}` to browse and `registry.activate` with qualified ids to stage entries; no separate host activation API is required.
- No event fields or event-schema revision were added. Search/listing/activation JSON results have the contracts above. `CapabilitiesActivated` still carries the full current epoch. Retrieval events now report deterministic revision v2; snapshot/view/plan fingerprints naturally change with bootstrap/schema changes.
- Activation serialization remains `live-ability-state-2`, with unchanged wire layout. Older completed sessions load through the existing completed-boundary rebase, refresh the search bootstrap, and gain `registry.activate` and any missing authorized pins. Exact-snapshot restoration also reconciles newly configured pins. In-flight snapshots retain existing exact-match safety: for an incompatible interrupted checkpoint, Smith can use its existing `StartSession::with_checkpoint_recovery(CheckpointRecoveryPolicy::ResumeOrInterrupt)` upgrade path rather than replay the old turn under changed capabilities.
