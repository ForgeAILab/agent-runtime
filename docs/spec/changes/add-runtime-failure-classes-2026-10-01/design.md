---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-01T00:00:00Z
---

# Design: Preserve failure evidence without changing retry admission

## Evidence on Main

All references were read locally at `main`/HEAD
`b4b421fe8e049709063bac02e602bdeadf296c26`; no implementation or live calls
were run.

| Verified fact | Evidence |
| --- | --- |
| Runtime errors expose kind, message, retryability, and metadata, but no class; construction defaults to nonretryable. | `crates/agent-runtime-core/src/error.rs:46`, `:60` |
| Provider timing and credential recovery already exist, but conversion copies only four coarse runtime fields. | `crates/agent-runtime-core/src/provider.rs:1507`, `:1515`, `:1518`, `:1587` |
| Planning errors are stringified into Config after optional budget reporting. | `crates/agent-runtime/src/agent/driver/turn.rs:1205`, `:1218` |
| The harness boundary discards everything except the runtime error message. | `crates/agent-runtime/src/agent/driver/mod.rs:265` |
| LCM mapping initially distinguishes CannotFit, Unauthorized, revision/conflict errors, and backend failure; it is not intrinsically all Config. | `crates/agent-runtime/src/harness/lcm.rs:3919` |
| Context errors already have typed kinds and an optional budget report, including capability-budget overflow. | `crates/agent-runtime-context/src/budget.rs:251`, `:288`, `:304` |
| Attempt index, total budget, provider error, and admitted delay already exist; the existing Error event contains RuntimeError. | `crates/agent-runtime-core/src/event.rs:1377`, `:1412` |
| Protected checkpoints reject incompatible transition revisions; LCM Debug excludes store reasons. | `crates/agent-runtime-core/src/checkpoint/mod.rs:39`; `crates/agent-runtime-lcm/src/store.rs:79` |

G9's lossy conversion and private flattening claims are confirmed. Its broader
suggestion is reduced: retry decisions are already implemented, LCM initially
has coarse typed mappings, and a new failed-turn event would disturb Smith's
exhaustive event matches. No unverified claim about adapter context-length
parsing or consumer source literals is needed for this slice.

## Decisions

### Classification and stage

Use a new non-exhaustive `FailureClass` enum, serialized as an internally
tagged object (`reason`, snake-case names). Its `Unclassified` unit variant is
the serde default and fallback for unknown future reason tags. Known malformed
payloads still fail deserialization. Reason-bearing variants carry a fixed
`FailureStage`: `PreProvider`, `Provider`, `Tool`, or `Unknown`. Stage names a
failure's origin, not proof of cost incurred or of history commitment.

| Class | Evidence and intended host interpretation |
| --- | --- |
| `RequestRejected { stage }` | Schema/shape, configuration, unsupported feature, pairing, or cache-identity rejection; changing the request is necessary. |
| `PolicyDenied { stage }` | Approval/workspace/LCM-view denial; retry cannot grant authority. |
| `ContextOverflow { stage, required_tokens, available_tokens }` | A typed model-input-budget overflow; token numbers are optional and never invented. |
| `Transient { stage }` | Network, server, timeout, or malformed-stream evidence; retryability and replay safety still govern. |
| `RateLimited { stage }` | A momentary provider throttle. |
| `QuotaExhausted { stage }` | Provider `LimitExhausted`; waiting a normal backoff is not sufficient evidence of recovery. |
| `Auth { stage }` | Authentication failure; only the existing credential replay fence admits recovery. |
| `StateConflict { stage, component }` | LCM revision/frontier/idempotency or checkpoint conflict, with a bounded neutral component ID. |
| `HostComponent { stage, component }` | Unclassified component/backend failure; no automatic transient inference. |
| `TurnLimit { stage, limit }`, `Cancelled { stage }`, `Internal { stage }` | Known runtime limits, cancellation, or invariant failures. |
| `Unclassified` | Legacy data or insufficient evidence; no implied retry permission. |

Keep timing separate from category so conversion preserves even unusual
provider field combinations exactly: optional `retry_after_ms` is a duration,
`limit_resets_at_ms` is an absolute Unix-millisecond timestamp, and
`credential_recovery` is the existing fixed enum. Unknown does not mean zero.
`From<ProviderError>` preserves kind/message/retryable/metadata and all these
fields; the driver's local capability-validation path labels its failures
`PreProvider`, while an error from an actual provider invocation is `Provider`.
Direct conversion alone cannot prove that network I/O occurred.

Existing generic constructors cannot infer stages from prose; they default to
unknown stage or `Unclassified`. Add explicit classification builders and set
classes at typed origins. Known schema rejection and policy denial receive
deterministic classes when exposed as RuntimeError. Tool-error results remain
tool-error results; they do not become terminal turn failures.

### Preserve evidence across private boundaries

Replace the private request-building error carrier with a sum of planner
`ContextError` and harness `RuntimeError`; do not alter public `ContextError`
fields or add a dependency from core to context/LCM. Request-building failures
retain their current host-visible Config/nonretryable projection while gaining
a class and safe source metadata/timing. Keep the original harness error intact
inside the private carrier so its typed origin determines the class; do not
replace that origin with a Compaction string. Other direct runtime/provider
error paths retain their current kind and retryable values. Where no class
exists, attach a bounded component identity and unknown retry semantics. Set
LCM classes at the typed match before the harness boundary, without carrying
backend reason text into diagnostics. The new class can say StateConflict even
when the historical coarse projection is Config; it is the additive evidence
hosts should use, rather than a change to their existing kind match.

Input-budget overflow is distinguished from capability sub-budget overflow:
the latter uses `RequestRejected` unless a typed input-budget failure also
exists. Do not manufacture required/available counts for arbitrary Compaction
strings or provider BadRequest bodies. Provider context-overflow detection is
deferred until adapters have independently reviewed structured evidence.

### Wire and release behavior

All new RuntimeError fields are serde-defaulted; `Unclassified` and absent
options are omitted. Legacy JSON remains readable, including nested Error
events and checkpoints. Older current readers ignore unknown struct fields;
test this using frozen main-shaped fixtures, not an assumption about all
third-party parsers. No schema-version bump or RuntimeEvent variant is needed.
Adding a field can break Rust literals despite compatible JSON. Actual consumer
compilation and projections, not the audit's search result, decide release
eligibility. Preserve retry admission and existing coarse projections at the
final host boundary; no automatic runtime retry is added for preflight errors.

## Risks and Migration

Unknown or caller-supplied retry flags can disagree with the category. Hosts
must use category as evidence and retain their own retry/recovery policy; this
proposal does not silently rewrite flags. Secret-bearing free-form component
text is prohibited. Failure stage must be set at the actual origin, especially
for local errors constructed using ProviderError.

Fixtures must check legacy absence, future unknown reason tags, exact timing
preservation including zero values, planner budget categories, classified
contributor failures, LCM policy/conflict errors, and unchanged provider retry
counts/delays. Adoption of class-based presentation is optional in all hosts.

## Open Questions

- Owner approval of the reason/stage vocabulary and the top-level timing-field
  spelling; proposed names deliberately match existing ProviderError fields.
- Which consumer-owned error construction sites need a coordinated Rust update?
  Resolve with candidate consumer builds before release, not substring searches.
