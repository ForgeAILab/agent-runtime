## Context

Status: **PROPOSED — awaiting repository-owner review. Not implemented.**

The Responses encoder validates canonical requests before translation
(`crates/agent-runtime-provider/src/responses.rs:315`). System content is
currently emitted as system-role input (`responses.rs:649`), tuning is encoded
unconditionally when present (`responses.rs:391`), and reasoning only forwards
an effort (`responses.rs:403`). Budgets and nonempty vendor extensions are
rejected (`responses.rs:581`, `responses.rs:592`), so vendor JSON cannot solve
the consumer's requirements. Abbreviated provider paths are under
`crates/agent-runtime-provider/src/`.

The preset already selects endpoint/headers and `UsageOnly`
(`responses.rs:198`), and `with_chatgpt_account` sets a static extra header
(`responses.rs:209`). The POST builder acquires one lease and uses its bearer,
then appends static headers (`responses.rs:1892`); it never reads the existing
lease account (`crates/agent-runtime-core/src/provider_credential.rs:142`).
Account changes across refresh therefore cannot be projected coherently today.

`ReasoningConfig` already carries named effort and token budget
(`crates/agent-runtime-core/src/provider.rs:1263`). Its optional absence does
not define a separate disable instruction. Profile defaults must not silently
override explicit named choices, including a model-supported disabling label.
The context planner is the authority for counts and immutable provider context
(`docs/spec/specs/context-management/spec.md:20`,
`crates/agent-runtime-context/src/planner.rs:699`); adding fallback prompt text
only in the adapter would violate that existing contract.

## Goals / Non-Goals

- Goals: express backend request variants without a product-specific core
  schema, keep default behavior stable, ensure counted context and coherent
  lease headers, and test each transformation offline.
- Non-goals: login/storage/accounts UX, hardcoded Nyx prompts or effort
  thresholds, unrestricted JSON mutation, a new Responses stream parser,
  hosted tools, provider-side conversation state, or hidden retries.

## Options

| Option | Wire/schema impact | Testability and neutrality | Trade-off |
| --- | --- | --- | --- |
| Host-supplied declarative profile on `ResponsesConfig` | Opt-in HTTP shape changes; adding a public field breaks exact Rust literals, but core event/lease serialization stays unchanged | Bounded pure transforms and table fixtures; values and activation are host policy | Adds a finite policy surface and needs planning integration for extra context |
| Host `RequestShaper` hook | May alter arbitrary payload/headers; trait itself need not change persisted schemas | Easy to mock, but every arbitrary mutation requires post-validation and invariant tests | Can bypass counted input, `store=false`, cache identity, tool aliases, or credential boundaries; exposing the lease makes it especially risky |
| Lease-attached arbitrary header projection | Expands the core credential contract for all adapters; nonserializable leases avoid persisted wire changes | Source can bind token/header values atomically | Pushes endpoint policy into credential sources and requires restricted names/value bounds, auth collision rules, and redaction; existing account field already solves the reported identity case |
| Host wrapper `Provider` | No shared API/schema changes | Maximum host policy control; tests live in each host | A wrapper around today's `ResponsesProvider` cannot access its acquired lease or arbitrary encoder policy; a full replacement duplicates translation, renewal/recovery, stream parsing, and conformance |

## Recommendation

Choose a finite declarative profile, selected explicitly by the host, with the
standard encoder as its default. Reuse the existing lease account through one
configured header projection rather than attach arbitrary headers to leases.
Do not add a general `RequestShaper` until another concrete backend requires
transformations outside this bounded surface. A provider-builder profile is
the compatibility fallback if the owner declines a public config-field break.

The shared library owns validation and deterministic transformations. The host
owns endpoint selection, instruction/default text, synthetic user text, which
tuning fields are omitted, supported effort labels/thresholds/defaults, summary
choice, account-header name and requiredness. No hostname inspection activates
policy, and the existing ChatGPT preset must not silently install new request
defaults. Hosts may explicitly compose a profile with that preset.

## Proposed bounded profile

The following names describe design roles, not an implemented API:

- Instruction placement: preserve system-role input (default), or fold system
  text in original order into top-level `instructions` using a deterministic
  separator. In the latter mode, remove only the projected system items from
  `input`. Reject unsupported system content as today; retain all non-system
  images, tools/results, and signed reasoning in canonical order.
- Optional host fallback instructions apply only if no nonempty system text
  remains. A configured synthetic user turn applies when no non-system input
  survives projection, including a system-only conversation. Both strings must
  be bounded, redacted in Debug, and prepared as counted context before planning.
  An empty conversation follows these explicit settings or fails the profile's
  input requirement; there is no built-in product prompt or user text.
- Tuning mask: a finite set such as temperature, top-p, max-output-tokens, stop.
  Omit selected fields entirely, rather than nulling them. `max_output_tokens`
  still contributes to canonical validation and output reserve; omission only
  means this backend cannot accept the wire knob. It cannot guarantee backend
  generation is capped at that number; host/runtime limits and cancellation
  remain necessary. Never silently raise budgets or select a different model.
- Reasoning: explicit bounded effort wins; otherwise map a supplied token
  budget through a host-supplied bounded ordered threshold table; otherwise
  use a configured default effort. Emit configured summary (e.g. `auto`) when
  the profile enables reasoning and the model supports it. Accept an empty
  reasoning config as eligible for defaults. A budget without a mapping is
  rejected as today; zero/unmapped budgets fail rather than guessing. An
  explicit effort plus budget uses the named effort, while budget accounting
  remains intact. Labels come from host/model policy, including extended
  labels; no global `low`/`medium`/`high` allowlist or Nyx threshold constants.
- Account projection: validated host-selected header name, optional required
  account switch, and the value exclusively from the acquired lease. For Nyx's
  reported endpoint the host chooses `chatgpt-account-id`. Disallow auth and
  protocol-control header names. Reject any case-insensitive static header
  collision when projection is enabled, including `with_chatgpt_account`.
  No static fallback is permitted for a missing required lease account.

## Processing and authority boundaries

1. Validate bounded profile parameters and static header conflicts at provider
   construction. Validate request, unsupported behavior, and profile-specific
   context before credential acquisition.
2. Before immutable plan admission, host composition prepares fallback/synthetic
   content through a profile-aware context preparation mechanism using the
   same profile. All added text, its separator/role framing, and representation
   overhead must be counted by the configured sizer. Adapter projection must
   verify that the required additions are accounted for, and reject mismatches
   before source I/O. Direct `Provider` callers without a runtime plan still
   obey adapter byte/content bounds and supply their own budget authority.
3. Bind the effective profile revision and context projection to sizing and
   cache identity. Reordering instructions or changing fallback/synthetic
   bytes must not retain an old exact-prefix identity or maintenance claim.
   Non-content omission/default settings are recorded as adapter policy inputs
   without persisting raw prompt text. Never infer conformance from the endpoint
   alone (`responses.rs:511` preserves the existing official-endpoint fence).
4. Build the stateless payload, apply only declared transformations, and
   revalidate final byte/item/text bounds and immutable protocol controls.
   `stream=true`, `store=false`, complete local continuation, tool-name
   correlation, encrypted reasoning inclusion, and no hosted/stateful overrides
   remain enforced. A mask or reasoning policy cannot circumvent capabilities.
5. Acquire once under the existing attempt scope. Inject bearer and optional
   account header from that same lease immediately before the one transport
   call. Reject control characters/invalid HTTP values with fixed errors before
   POST, without printing the account. Do not persist the projected header or
   copy it into canonical context.
6. Keep the existing auth-recovery fence. A new visible attempt acquires its
   own token/account pair; no cached account survives a lease replacement.
   Standard/UsageOnly terminal policy and stream event normalization remain
   independent of request shaping.

## Risks / Trade-offs

Planning integration is a real part of this change, not a follow-up: hidden
fallback instructions invalidate token/cache claims. A profile-aware context
preparation helper is preferable to duplicating defaults across host and
adapter; its exact API is an owner-review gate. Suppressing a wire output cap
changes enforcement evidence and must be documented honestly. Invalid profiles
must fail closed, while the absence of a profile preserves current rejection
of vendor overrides and reasoning budgets.

Debug shows masks, placement modes, requiredness, and safe labels, but never
fallback/synthetic text, account values, secrets, or raw payloads. Reuse
`Secret`/`error_redaction` and existing lease Debug. No general lease-header map,
core credential serialization, or event schema expansion is needed.

## Migration Plan

After approval, add the opt-in API and conformance fixtures; leave OpenAI/xAI
standard defaults unchanged. Hosts explicitly configure the profile and
planning integration, migrate static account headers to lease-owned accounts
when projection is enabled, and keep their legacy provider until all reported
wire cases pass. Document any config-literal break and require a separate
consumer migration review.

## Open Questions

- Approve the config field or require the provider-builder alternative?
- Which profile-aware preparation/sizing API proves added context was admitted without expanding persisted schemas?
- Which exact omission mask, default effort, mapping thresholds, and bounded fallback strings will Nyx supply as fixtures?
- Does the selected backend require an account on every request or only for multi-account leases? Requiredness stays explicit host policy.
