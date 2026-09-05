---
created_at: 2026-08-16T10:00:00Z
updated_at: 2026-09-05T04:18:28Z
---

## Why

Agents frequently need to access external web pages, API responses, or raw text documentation during problem solving (e.g. looking up library documentation, downloading raw text files, or retrieving issue/PR details). Today, agents without a dedicated web fetching capability must either rely on broad subprocess execution (`curl`, `wget`) through `shell`—which incurs unnecessary process-spawn overhead, requires excessive security permissions, and lacks structured HTML content parsing—or require host applications to implement ad-hoc custom fetch tools.

Providing a neutral, built-in `FetchTool` in the runtime solves this cleanly:
1. It establishes an explicit, narrowly scoped network capability requiring `Permission::NetHttp` and `Effect::Network` with fine-grained URL authorization and SSRF protection.
2. It standardizes web content extraction, offering automatic conversion of HTML pages into clean Markdown or returning raw text/binary responses with deterministic truncation bounds.
3. It keeps the production core transport-agnostic through an injectable `FetchTransport` trait, allowing test suites and conformance harnesses to run completely offline while production hosts can inject production HTTP clients (e.g. `reqwest`).

## What Changes

- Introduce a neutral `FetchTool` conforming to the `Tool` contract in `agent-runtime`.
- Define the `FetchTransport` trait for decoupled, async HTTP GET execution with status, headers, and bounded body streaming.
- Standardize tool input parameters:
  - `url` (required string): Valid absolute `http://` or `https://` URL.
  - `format` (optional enum: `"markdown"`, `"text"`, `"raw"`, `"html"`, defaulting to `"markdown"`).
  - `max_bytes` / `limit` (optional integer): Maximum characters or bytes returned to prevent model context flooding.
  - `offset` (optional integer): Line or character offset for paginated inspection of large documents.
- Implement HTML-to-Markdown parsing and content cleanup:
  - Strips metadata, scripts, styles, navigation, and advertisement elements.
  - Preserves headings, code blocks, lists, links, tables, and paragraphs as standard Markdown.
- Implement security validation and SSRF defenses:
  - Strict URI scheme enforcement (rejecting non-HTTP schemes like `file:`, `ftp:`, `gopher:`).
  - Configurable restriction of loopback, link-local, and private RFC 1918/RFC 4193 IP addresses to prevent local network probing.
  - Authority mapping to `Permission::NetHttp` and `SecurityResource::network(url)`.
  - Strict enforcement of turn deadlines and cancellation tokens.
- Add comprehensive unit and conformance tests in `agent-runtime-testkit` and `agent-runtime` using mock transport fixtures.

## Impact

- Affected specs: `tool-execution`
- Affected packages: `agent-runtime`, `agent-runtime-core`, `agent-runtime-testkit`
- Public compatibility: Additive only. No breaking changes to existing tool execution or session contracts.
- Dependencies: Pure Rust parsing for HTML-to-markdown (or optional minimal parsing dependencies adhering to MIT/Apache-2.0 and workspace deny policies). Production packages remain independent of specific HTTP client implementations via `FetchTransport`.
- Security: Establishes fine-grained network egress control (`Permission::NetHttp`) for web retrieval without requiring broad process-spawn or shell execution authority.

## Shared-Code Admission

Web retrieval with Markdown conversion and SSRF-safe bounded ingestion is a universal capability required across coding and research agent hosts (Smith, Nyx, Open Forge). Supplying a neutral implementation prevents divergent, insecure ad-hoc implementations across consumer applications while maintaining clean trait decoupling.
