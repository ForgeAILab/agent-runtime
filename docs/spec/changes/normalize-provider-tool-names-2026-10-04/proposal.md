---
created_at: 2026-10-04T00:00:00Z
updated_at: 2026-10-05T00:58:57Z
---

## Why

Canonical runtime tool names may contain dots or exceed provider wire limits.
The repository owner approved this adapter-boundary behavior on 2026-10-04.

## What Changes

- Add one private shared bidirectional tool-name helper for Anthropic Messages,
  OpenAI-compatible Chat Completions, and OpenAI Responses.
- Preserve valid names; normalize invalid names with a deterministic fingerprint
  suffix and resolve even suffix collisions without merging canonical identities.
- Translate definitions, named choices, historical calls/results, and incoming
  model calls while keeping runtime requests, events, history and manifests canonical.
- Prioritize the registered tool set so growing history cannot rename its tools.

## Impact

- Affected specs: provider-runtime.
- Affected code: agent-runtime-provider; no dependencies or public API changes.
- Wire policy: ASCII letters/digits/underscore/hyphen, 1–64 characters. Chat Completions
  documents 64; Responses uses the same OpenAI-compatible wire profile. Current Anthropic documentation lists 128; the requested 64
  compatibility ceiling accommodates Anthropic-compatible gateways. Thus ASCII
  Anthropic names of 65–128 characters are normalized under this approved policy.
- Gemini Interactions documents names as strings without a regex or length limit;
  its existing bounded-name validation remains. Do not import GenerateContent's
  distinct function-declaration constraints into the native Interactions adapter.

Sources checked on 2026-10-04:
- [Anthropic tool definitions](https://platform.claude.com/docs/en/agents-and-tools/tool-use/define-tools)
- [OpenAI Chat Completions](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)
- [OpenAI Responses function tools](https://github.com/openai/openai-python/blob/main/src/openai/types/responses/function_tool_param.py)
- [Gemini Interactions Function](https://ai.google.dev/api/interactions-api#Function)
