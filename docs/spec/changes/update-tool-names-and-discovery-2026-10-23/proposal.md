---
created_at: 2026-10-23T23:30:00Z
updated_at: 2026-10-23T23:30:00Z
---

## Why

A live Smith session (glm-class model on an OpenAI-compatible provider) could
not reach web search or delegation although both were registered:

- `registry.activate` goes on the wire as `registry_activate_<32 hex>`. The
  model copied the hash wrong, and the runtime answered only "was not active in
  the frozen provider request". A fingerprint suffix is unreadable for a model
  and protects only against name collisions that registration can refuse.
- Explicit `registry.search` for "web search fetch internet" ranked 18 skills
  above `web_search`: a tool's name matches only as one whole term, so
  `web_search` never matched "web" or "search". The top-8 cut then dropped it.
- When the model searched again, `web_search` was retrieved but selection
  rejected it as redundant with `web_fetch`, and the result said nothing.
- The model searched `domain: "agent"` for delegation. Smith files its `agent`
  tool under `tool`, so the search returned nothing and the model concluded
  it could not delegate.
- One shared word in a prompt pre-activated an unrelated skill for the rest of
  the session.

## What Changes

- **Provider tool names without hashes.** The wire name is the canonical name
  with every byte outside `[A-Za-z0-9_-]` replaced by `_`, truncated to 64
  bytes. No suffix is ever added. Model output naming either the wire or the
  canonical name maps back to the canonical name.
- **Collisions are refused, not disambiguated.** `ToolRegistry` rejects a tool
  whose provider name equals one already registered, or is empty. The live
  ability router extends its protected-name check to provider names, so a host
  tool named `registry_search` is refused. Adapters reject a request whose
  tools share a provider name (requests not built from a registry).
- **Unknown tool calls name the available tools.** The runtime's error for a
  call to a tool absent from the frozen request lists the tools it offered.
- **Explicit search matches name parts.** In descriptive retrieval, each
  `_`/`.`/`-`/`/`-separated part of a capability's local name scores like a
  curated keyword, once per term. The retriever revision becomes
  `capability-retrieval.deterministic.v3`.
- **Explicit search mixes domains.** With no domain filter, results take
  candidates round-robin across domains in rank order, so tools are not buried
  under a large skill set.
- **An empty domain filter falls back.** When a domain filter removes every
  match, search runs over all domains and says so in a `note`.
- **Unstaged matches are reported.** The result lists `not_staged` entries
  (id plus reason: redundant, budget, cardinality, conflict, dependency, or
  not materialized) for returned matches it did not stage, and says
  `registry.activate` can request one explicitly.
- **Skill pre-activation needs two terms.** Prompt pre-activation binds a skill
  only when its name matched or at least two distinct terms matched. Tools are
  unchanged.

## Impact

- Affected specs: `provider-runtime`, `tool-execution`, `capability-routing`.
- Affected code: `agent-runtime-core` (shared provider-name function),
  `agent-runtime-provider/src/tool_names.rs` and the three adapters,
  `agent-runtime/src/tool/registry.rs`, `agent/driver/tools.rs`,
  `capability/{retrieval,preactivation}.rs`,
  `harness/live_abilities/{activation,search}.rs`.
- Wire names change once for every non-conforming tool name (cache prefixes
  that contained them miss once). Persisted history keeps canonical names, so
  checkpoint revisions do not change.
- A host whose tools differ only by `.` versus `_` now fails at build with a
  conflict naming both tools.
