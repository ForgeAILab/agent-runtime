## Context

Three wire protocols share the same requested tool-name policy. Canonical
identities must stay outside provider serialization and stream normalization.
Gemini Interactions has no documented matching constraint and is unchanged.

## Decisions

Use a sorted set, reserve valid registered names first, and give invalid names
an ASCII readable prefix plus the existing stable 128-bit fingerprint. On a
candidate collision, add a deterministic numeric suffix and check uniqueness;
a fingerprint is an optimization, never the uniqueness guarantee. Historical
names not registered are allocated only after the registered map. Build a
private request projection, preserving cache identity and canonical request
ownership. Incoming complete names are inverted; fragmented OpenAI names are
buffered through the tool finish and emitted as one canonical name.

## Risks / Trade-offs

A changed registered tool set can change collision resolution; identical sets
remain stable regardless of definition order and changing history. Historical
unregistered names receive wire aliases but cannot displace registered aliases.
OpenAI name fragments may be delayed until finish; argument streaming remains.

## Migration Plan

No public types or stored canonical data change. Existing valid 1–64 character
names retain their wire spelling. Anthropic ASCII names above 64 are normalized
for the requested compatible-gateway ceiling despite current docs allowing 128.
