## Context

Status: **PROPOSED — awaiting repository-owner review. Not implemented.**

The Anthropic provider contains only transport and config
(`crates/agent-runtime-provider/src/anthropic.rs:285`). `stream` builds a
validated payload, injects a static key, and races transport against attempt
cancellation/deadline (`anthropic.rs:1161`). `authentication_error` already maps
to `ProviderErrorKind::Auth` (`anthropic.rs:909`), but no lease revision exists
to invalidate. In contrast, OpenAI acquisition checks lease expiry after a
cancellation/deadline race (`openai.rs:469`) and its auth classifier returns
recovery only for `ReplacementPossible` (`openai.rs:573`). All abbreviated
provider paths in this document are under `crates/agent-runtime-provider/src/`.

The canonical loop already fences recovery to pre-semantic output and one
replay (`crates/agent-runtime/src/agent/driver/provider.rs:852`,
`crates/agent-runtime/src/agent/driver/provider.rs:1346`). Extend Anthropic's
participation; do not change the shared retry model. README's credential
contract (`README.md:53`) and `provider-credentials` truth remain the baseline.

## Goals / Non-Goals

- Goals: one source acquisition per visible attempt, explicit wire scheme,
  cancellation/deadline parity, exact-revision invalidation, non-disclosure,
  static-key compatibility, and deterministic conformance.
- Non-goals: browser login, OAuth endpoints/client IDs/scopes, token persistence,
  account selection, subscription eligibility policy, Claude Code prompts, or
  automatic Nyx migration.

## Options and recommendation

| Option | Benefit | Cost |
| --- | --- | --- |
| Anthropic-specific config enum `ApiKey` / `OAuthBearer` | Explicit and inspectable wire policy; config and capabilities can agree before describe/planning | Adding a public field breaks exact config literals |
| Provider builder selects scheme, defaulting to API key | Keeps existing config literals and `new` signature | Scheme lives outside config; must normalize capabilities before exposure and validate before any I/O |
| Infer scheme from token prefix or extra headers | No new field | Ambiguous, couples policy to secret contents, permits duplicate auth, and cannot establish a reliable source contract |
| Add scheme to every core credential lease | Allows a source to switch credential kinds dynamically | Cross-adapter contract expansion with no demonstrated need; unrelated lease users inherit complexity |

Recommend an explicit Anthropic-specific enum and fluent selection, defaulting
to API key, with a config field if the owner accepts the pre-1.0 struct-literal
break. If source compatibility is required, retain the same enum on the provider
builder instead; do not infer it. The lease remains neutral secret/expiry/revision
mechanism. A source target supplies one configured scheme; changing scheme
requires a new provider configuration, not a surprise lease transition.

## Proposed attempt and authentication behavior

1. Preserve `new`'s static-key path through `StaticProviderCredentialSource`.
   Add `with_credential_source(transport, config, target, source)` with static
   key/source conflict rejection, plus clock and minimum-validity builders.
   Keep `config.api_key` as API-key compatibility material; reject it when
   OAuth Bearer mode is selected. A host with a static OAuth setup token wraps
   it in the existing non-expiring `StaticProviderCredentialSource` and passes
   that source with OAuth Bearer mode, instead of relabeling it an API key.
   Preserve unauthenticated/custom-gateway construction where no managed
   credential is configured; that path has no renewal guarantee.
2. Validate payload, endpoint, and auth-channel conflicts before source or
   transport I/O. When using managed credentials, reject extra `authorization`
   and `x-api-key` headers case-insensitively. Legacy unmanaged extra-header
   authentication can remain available but cannot advertise credential recovery.
3. Acquire under `ctx.cancel` and `ctx.deadline`, race the future even if a host
   source misbehaves, and reject expiry before `clock.now() + minimum_validity`.
   Expose the lease secret only while constructing the outbound request.
4. API-key mode emits only `x-api-key`. OAuth Bearer mode emits only
   `Authorization: Bearer` and adds `oauth-2025-04-20` to `anthropic-beta`.
   Fold repeated beta headers into one comma-separated flag list, preserving
   host order and deduplicating whole flags; retain `anthropic-version` and
   thinking/tool beta flags. Normalize advertised `AuthKind` to the selection.
5. Classify an HTTP/transport Auth rejection or an Anthropic SSE
   `authentication_error` before semantic events. Invalidate exactly the used
   revision within the remaining cancellation/deadline. Only
   `ReplacementPossible` yields `RetryWithRenewedCredential`; stale/static
   outcomes terminate. Non-auth failures never invalidate. The adapter performs
   no second POST; the canonical loop owns the one visible replacement attempt.
6. Once text, reasoning, tools, usage, cache, downgrade, or finish semantics
   were accepted, report Auth terminally without invalidation or recovery.
   Header-only rate-limit observations follow the existing runtime distinction
   from semantic progress (`agent/driver/provider.rs:1069` under the runtime
   crate). Auth response bodies and header values remain unprintable.

## Risks / Trade-offs

Scheme/config normalization must not promise that a model endpoint authorizes a
particular consumer subscription. Header conflicts are a compatibility change
for hosts currently combining `api_key` with manual authorization; those hosts
must select one path. A shared generic acquisition refactor is not required;
matching existing behavior with fixtures keeps this proposal bounded.

No schema or networking dependency is needed. Config/provider Debug must show
only the scheme, safe settings, and header names; continue using `Secret` and
`error_redaction`. Sources, targets, revisions, expiry, tokens, account data,
and raw responses must not be formatted or persisted by the adapter.

## Migration Plan

After approval, retain static API-key callers, add a renewable-source example,
document struct-literal migration if the config field is selected, and publish
conformance evidence. Nyx's `claude-code` kind remains on its legacy adapter
until this contract passes and a separate consumer migration is approved.

## Open Questions

- Does the owner accept a config-field break or require the provider-builder variant?
- Which supported Anthropic credential/endpoint fixtures should gate the Nyx migration?
