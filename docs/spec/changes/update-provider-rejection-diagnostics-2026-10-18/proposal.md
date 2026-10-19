---
created_at: 2026-10-19T02:01:00Z
updated_at: 2026-10-19T02:01:00Z
---

## Why

Smith session `f72edceb` stopped with `provider rejected the request` and
nothing else. Replaying it with debug logging showed the gateway's answer:
HTTP 404, `Model is not deployed or ready`. Smith's transport had read that
reason, checked it against every value it sent, and attached it to the error
together with the status. The adapter boundary threw both away.

Since `normalize-provider-tool-names` (2026-10-04), adapters rebuild every
transport error from its class alone, so that server text echoed into an
error cannot leak a credential. That is right for raw provider text. It also
drops two things that need no such caution, or that the transport has already
cleaned: the HTTP status, which is a number from the status line, and a
reason the transport redacted itself. Without them an unknown model, a
malformed request and an oversized body all read the same.

## What Changes

- `agent-runtime-core` names two provider-error metadata keys: `http.status`
  for the HTTP status of a rejected response, and `provider.redacted_reason`
  for the provider's own reason, set only by a transport that has already
  redacted it.
- Adapter redaction keeps an integer `http.status` from 100 to 599 and a
  textual `provider.redacted_reason`, which it bounds again (control
  characters become spaces, at most 300 characters). Both are appended to the
  fixed class message, for example
  `provider rejected the request (HTTP 404): Model is not deployed or ready`.
  Every other metadata entry, `provider.detail` included, and the transport's
  own message are still dropped.
- The bundled reqwest transport records `http.status` on the HTTP failures it
  classifies, so its status survives redaction too.

## Impact

- Affected specs: `provider-runtime` (new requirement "Transport-vouched
  rejection diagnostics").
- Affected code: `crates/agent-runtime-core/src/provider.rs`,
  `crates/agent-runtime-provider/src/error_redaction.rs`,
  `crates/agent-runtime-provider/src/gemini.rs`,
  `crates/agent-runtime-provider/src/responses.rs`,
  `crates/agent-runtime-provider/src/reqwest_transport.rs`.
- Additive public API: two `&str` constants in `agent_runtime_core::provider`.
  No type, checkpoint schema or journal record changes.
- Security: adapters cannot verify a reason; the transport that sets
  `provider.redacted_reason` is responsible for removing credentials from it.
  A provider that echoes request text into its error message can still have
  that text recorded once a transport vouches for it. Smith's transport keeps
  only `error.message` and `error.status` and drops the whole reason if it
  contains any value it sent.
