---
created_at: 2026-10-08T00:00:00Z
updated_at: 2026-10-08T00:00:00Z
---

# Proposal: Render the deterministic LCM fallback so it cannot be mistaken for a reply

## Why

When both model summary attempts are rejected, LCM stores a head/tail cut of
the rendered source as the summary. That text is later sent to the model as
its own earlier assistant message. It rendered tool calls as
`assistant: call name(args)`. In a Nyx production session where half of the
summary nodes were fallbacks, the model started answering with the literal
text `call shell({...})` instead of calling the tool, and the host delivered
that text to the user. The fallback also carried reasoning text, which is not
part of what was said or done.

## What Changes

- The fallback renders a tool call as a bracketed note on its own line,
  `[earlier tool call: name args]`, and a tool result as
  `[earlier tool result: name]` followed by the capped result. Names, capped
  arguments and capped results are all kept.
- The fallback leaves reasoning parts out.
- The fallback begins with `[excerpt of earlier conversation, not a summary]`
  when the label fits inside the fallback target. A source too small for the
  label gets the unlabelled cut, so no source becomes unsummarizable.
- `LCM_ALGORITHM_REVISION` becomes `agent-runtime-lcm-3` and the default
  deterministic algorithm revision becomes
  `lcm-deterministic-head-tail-tools-3`.

## Impact

Affected spec: context-management. Affected code: `agent-runtime-lcm`
summarizer. `render_summary_source`, the prompt rendering used by provider
summaries, is unchanged. No public API, event, state schema or store change.
Persisted LCM state rebuilds its derived metadata once on resume because the
algorithm revision changed. Fallback nodes already stored keep their old text.
The summary ratio targets, a minimum target and persisted attempt outcomes are
not part of this change.
