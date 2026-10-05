---
created_at: 2026-10-04T00:00:00Z
updated_at: 2026-10-04T00:00:00Z
status: proposed
---

Status: **PROPOSED — awaiting repository-owner review. Not approved or implemented.**

## Why

Anthropic has only a static `api_key` plus manually supplied headers
(`crates/agent-runtime-provider/src/anthropic.rs:60`). Its attempt injects
`x-api-key` directly and then appends extra headers
(`crates/agent-runtime-provider/src/anthropic.rs:1181`); it neither acquires a
lease nor invalidates a rejected revision. Extra headers can manually express
Bearer authentication today, but provide no first-class scheme selection or
renewal contract. Nyx reports that its `claude-code` provider kind must stay on
its legacy adapter until these capabilities exist. This consumer context comes
from the request; no other repository was inspected.

OpenAI and Responses already expose `with_credential_source`
(`crates/agent-runtime-provider/src/openai.rs:295`,
`crates/agent-runtime-provider/src/responses.rs:258`), and the core contract
already supplies leases and exact-revision invalidation
(`crates/agent-runtime-core/src/provider_credential.rs:80`,
`crates/agent-runtime-core/src/provider_credential.rs:235`). Anthropic should
conform to those mechanisms rather than introduce a second refresh loop.

## What Changes

- Add an Anthropic credential-source construction path, attempt-scoped lease
  acquisition, injected clock, and configurable minimum validity (30 seconds
  by default, matching OpenAI/Responses).
- Add explicit Anthropic authentication selection: API key by default, or
  OAuth Bearer with the required OAuth API beta flag. Preserve static-key
  behavior through `StaticProviderCredentialSource`; never infer a scheme from
  token contents or from `Capabilities::auth` alone.
- Classify pre-output authentication rejections, invalidate the exact lease
  revision within the attempt scope, and return the existing recovery signal.
  Let the canonical loop expose at most one replacement attempt.
- Reject conflicting authentication channels, merge OAuth beta flags without
  duplication, and retain the existing `Secret`/diagnostic redaction rules.
- Add adapter and runtime conformance fixtures for API-key and Bearer paths,
  cancellation, validity, concurrent revision races, and bounded recovery.

## Impact

- Affected specs: `provider-credentials`, `provider-runtime` (ADDED deltas;
  existing shared recovery/non-disclosure requirements remain authoritative).
- Affected future code: Anthropic config/provider and tests; shared testkit or
  runtime conformance tests; README credential examples and CHANGELOG. No new
  dependencies or OAuth/login/storage implementation are proposed.
- Public Rust API: adding an auth selector to the public config would break
  exact struct literals. Review the builder alternative in `design.md` before
  approval; preserve `AnthropicProvider::new` for static-key users.
- No event, lease serialization, checkpoint, or manifest schema change is
  needed. Authorization affects only the adapter transport boundary.
- Nyx migration is a separate consumer change after conformance and owner
  approval; this proposal alone does not move `claude-code` off its legacy path.

Protocol reference checked on 2026-10-04: the
[official Anthropic SDK auth implementation](https://github.com/anthropics/anthropic-sdk-python/blob/main/src/anthropic/lib/credentials/_auth.py)
injects Bearer authorization and merges the OAuth API beta flag. This proposal
does not claim that every consumer subscription credential permits arbitrary
direct API access; hosts remain responsible for supported credential use.
