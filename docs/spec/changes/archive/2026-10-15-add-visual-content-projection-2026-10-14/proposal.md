---
created_at: 2026-10-15T00:05:00Z
updated_at: 2026-10-15T00:05:00Z
---

## Why

Smith (`add-visual-tool-results-2026-10-14` in the Smith repo) needs
computer-use MCP servers to return screenshots to the model. Three runtime
gaps stand in the way:

- The MCP adapter replaces every image with a text line.
- The OpenAI Chat Completions adapter rejects a tool result that holds an
  image.
- Nothing stops an image from reaching a text-only model, or keeps a long
  visual session under provider image limits.

## What Changes

- **MCP image passthrough (R1).** `McpTool::translate` maps an `image` block
  to `ContentPart::Image` (a data URL with a host-supplied default `detail`).
  New host-set bounds are `max_image_bytes` (decoded, per image) and
  `max_images` (per result), with an allowed media-type set: png, jpeg, gif,
  webp. An image over a bound becomes a text line with its media type, size,
  and reason. Images do not count toward the text output limit.
- **Chat Completions tool-result images (R3).** For each run of consecutive
  tool messages, each result's text goes in its `tool` message. The run's
  images go, in order, in one following `user` message, each labelled with
  its `tool_call_id`. Text-only results serialize byte-for-byte as before.
- **Request image projection (R2).** The context planner projects image
  parts before sizing and cache identity, driven by an optional
  `ImageWindow { inline: K }` on `ContextPolicy` and the profile's
  `input_modalities`.
  - A text-only profile gets no image parts.
  - Otherwise images outside a stepped window become placeholders, while
    every image in the newest user message stays inline.
  - The placeholder is
    `[image <id>: <mime>[ <w>×<h>] not included in this request]`, where
    `<id>` is the first 8 hex digits of SHA-256 over the data URL.
  - The plan reports counts of replaced images by reason.
  - With no `ImageWindow`, a profile that accepts images is unchanged. A
    text-only profile is always projected.

## Impact

- Affected specs: `capability-routing` (MCP), `provider-runtime`,
  `context-management`
- Affected code: `agent-runtime-mcp/src/tool.rs` (+ client/config plumbing
  for bounds), `agent-runtime-provider/src/openai.rs`,
  `agent-runtime-context` (new projection module, `budget.rs`
  `ContextPolicy`, `planner.rs`, `plan.rs`), and the
  `agent-runtime` builder wiring.
- Compatibility: additive. New serde fields default. Checkpoint revision
  constants are unchanged. For text-only profiles, plan fingerprints change
  only when history holds images.
