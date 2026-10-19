---
created_at: 2026-10-18T23:40:00Z
updated_at: 2026-10-18T23:40:00Z
---

## Why

When Gemini rejected a Smith session's request, the user saw only
`Config: Gemini provider rejected the request`. Two things hid the cause:

- Every adapter rebuilt a transport or stream error from a fixed message and
  dropped the provider's own reason. Commit 4a0af25 did this on purpose,
  because an error body can echo the request's credential and an adapter
  must not trust an arbitrary host transport to have scrubbed it. Finding
  Gemini's real answer ("Request contains an invalid argument.") then took
  wire dumps.
- `From<ProviderError> for RuntimeError` mapped `BadRequest` to
  `ErrorKind::Config`, and `RuntimeError` displays as `"{kind:?}: {message}"`,
  so a provider rejection read as a configuration problem.

Separately, restoring a parent computed every recorded child's policy
fingerprint with `?`. A host that could no longer route one child (Smith
after a child profile changed) failed the whole parent restore, and the
session could not be resumed.

## What Changes

- Adapters keep a provider's stated reason (`status`/`code` and `message`
  from the documented error object, or from an in-band stream error) when it
  repeats no credential value the adapter itself put on that request. The
  reason is normalized, bounded to 300 characters, appended to the fixed
  class message, and kept as `provider.detail`; `http.status` survives as an
  unsigned integer; everything else is still dropped. Authentication
  failures keep their fixed message.
- The reqwest transport reads the same documented fields, including from an
  SSE-framed error body such as Gemini Interactions sends.
- A converted provider `BadRequest` is `ErrorKind::Provider`. `Unsupported`
  stays `Config`, and failure classes are unchanged.
- A child whose policy fingerprint cannot be computed during catalog restore
  is marked incompatible (an interrupted one becomes non-resumable) and the
  parent restore continues.

## Impact

- Affected specs: `provider-runtime` ("Gemini credential non-disclosure",
  "Bounded redaction-safe streaming transport", new "Provider rejections
  carry a checked reason"), `agent-delegation` ("Durable child ownership and
  policy compatibility").
- Affected code: `agent-runtime-core` (error conversion), every adapter and
  `reqwest_transport.rs` in `agent-runtime-provider` (which now depends on
  `url` unconditionally), `agent-runtime/src/delegation/coordinator.rs`.
- Behavior change visible to hosts: `RuntimeError.kind` for a provider
  rejection is `provider`, and messages may carry the provider's reason.
  Released as a minor version.
