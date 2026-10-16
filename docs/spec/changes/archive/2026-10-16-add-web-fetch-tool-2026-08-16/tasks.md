---
created_at: 2026-08-16T10:00:00Z
updated_at: 2026-09-05T04:18:28Z
completed_at: 2026-09-05T04:18:28Z
---

## 0. Coordination and Baseline

- [x] 0.1 Approve this proposal and delta spec before implementation.
- [x] 0.2 Verify existing tool execution and network security contracts in `agent-runtime-core`.

## 1. Transport Trait and Security Validation

- [x] 1.1 Define `FetchTransport`, `FetchRequest`, and `FetchResponse` in `agent-runtime`.
- [x] 1.2 Implement URL validation and SSRF checking (scheme validation, loopback/private network detection).
- [x] 1.3 Implement in-memory mock `FetchTransport` for offline testing and fixtures in `agent-runtime-testkit`.

## 2. Content Normalization (HTML to Markdown & Text)

- [x] 2.1 Implement HTML-to-Markdown parser/converter with tag stripping and element translation (`<h1>-<h6>`, `<p>`, `<a>`, `<code>/<pre>`, `<ul>/<ol>/<li>`, `<table>`, `<blockquote>`).
- [x] 2.2 Implement plaintext extraction and pagination/offset/limit windowing.
- [x] 2.3 Add untrusted content prompt injection warning prefix to formatted output.
- [x] 2.4 Add unit tests for content normalization across various HTML, text, and code payloads.

## 3. Tool Implementation and Execution Pipeline

- [x] 3.1 Implement `FetchTool` conforming to `agent_runtime_core::tool::Tool`.
- [x] 3.2 Implement `prepare` mapping target URL to `SecurityResource::network(url)` and `Permission::NetHttp`.
- [x] 3.3 Implement `invoke` delegating to `FetchTransport` with deadline and cancellation enforcement.
- [x] 3.4 Handle HTTP error responses, oversized payloads, and network timeouts returning model-visible tool errors or bounded outcomes.

## 4. Conformance and Integration Testing

- [x] 4.1 Test fetch execution with valid URLs and HTML conversion.
- [x] 4.2 Test fetch execution with raw/text formats and pagination bounds.
- [x] 4.3 Test untrusted content safety notice prefix on output.
- [x] 4.4 Test SSRF protections and invalid URL rejection.
- [x] 4.5 Test cancellation and deadline timeout handling during network fetch.
- [x] 4.6 Verify `cargo clippy`, `cargo fmt`, and full workspace tests pass.

## 5. Release review regressions

- [x] 5.1 Reproduce and reject alternate IPv4, encoded loopback, mapped IPv6,
  credentials, and malformed-authority bypasses using canonical URL parsing.
- [x] 5.2 Bound HTTP errors on UTF-8 boundaries and successful output by Unicode
  scalar values; cover invalid UTF-8 responses and saturating pagination.
- [x] 5.3 Verify the isolated published dependency candidate with formatting,
  all-feature tests, warning-denied Clippy, and Smith consumer conformance.

Release candidate verification: all-feature workspace tests (including Smith
consumer conformance), all-target all-feature Clippy with warnings denied,
formatting, and the Rust 1.86 facade/provider check pass on macOS.
