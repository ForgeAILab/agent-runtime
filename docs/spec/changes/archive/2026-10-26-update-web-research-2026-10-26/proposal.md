---
created_at: 2026-10-26T04:00:00Z
updated_at: 2026-10-26T05:08:53Z
---

## Why

Smith's research runs (Smith change `update-web-research-2026-10-25`,
approved by the owner on 2026-10-25) showed three runtime gaps. A read-only
child kept only filesystem, clock and random reads, so it lost `web_fetch`,
backend search and even its own `artifact.read`, although the host had already
applied its read-only posture. Gemini citations were opaque Google redirects,
a distinct one per grounding chunk, so 34–48% of a report's bytes were links
that named no page and repeated a page per text block. `artifact.read` returned
a byte array whenever a page edge split a UTF-8 character.

## What Changes

- `RuntimeBuilder::read_only_tool_names`: a child factory names the tools it
  reviewed for a read-only view. Both read-only filters keep a named tool unless
  its permission bound includes `fs.write`, `fs.create`, `fs.delete`,
  `host.fs.write`, `external.write`, or `process.spawn`. Unnamed tools keep the
  current filter.
- `provider::citation::CitationResolver` and
  `GeminiInteractionsProvider::with_citation_resolver`: grounding redirects are
  resolved to the pages they name before rendering, bounded in count,
  concurrency and time; an unresolved redirect is kept.
  `GroundingRedirectResolver` and `ReqwestTransport::redirect_location`
  provide a non-following lookup on the runtime's transport.
- Hosted adapters link each source once per assistant message.
- `artifact.read` cuts text pages at character boundaries, deciding text by
  media type.

## Impact

- Affected specs: `agent-delegation`, `provider-runtime`,
  `artifact-management`.
- Affected code: `runtime/builder.rs`, `delegation/{lifecycle,persistence}.rs`,
  `agent-runtime-provider/src/{citation,gemini,hosted,anthropic,responses,
  reqwest_transport}.rs`, `harness/artifacts.rs`, delegation conformance.
- Additive API. Rendering changes: a repeated source is no longer linked again
  in a later text block. No event, checkpoint or persisted-state change.
