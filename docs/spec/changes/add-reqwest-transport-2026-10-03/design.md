## Context

The existing CLI implementation is the canonical source for status mapping,
restricted IP ranges, DNS rebinding protection, and response bounds. No source
is imported from the read-only Smith or Open Forge repositories.

## Decisions

- `DestinationPolicy::{PublicHttps, Loopback}` is mandatory in
  `ReqwestTransport::new(policy)`. There is no default destination policy.
  `with_origin(&str)` validates and pins scheme, normalized host, and effective
  port; paths/queries are not part of an origin. CLI always pins its base URL.
- PublicHttps keeps the CLI's exact HTTPS, hostname, and restricted IPv4/IPv6
  checks. Loopback permits HTTP/HTTPS, only literal 127/8 or ::1 and exact
  localhost (case/trailing-dot normalized), never other DNS names or mapped
  IPv6 loopback. Both refuse userinfo and fragments before network I/O.
- Resolve each domain for each request and refuse empty/mixed forbidden answers.
  Build a fresh reqwest client using only those checked addresses; no proxy,
  no redirects, no injectable client that could weaken checks. DNS injection
  stays private for hermetic tests.
- Keep the CLI defaults: ten-second connect timeout, 8 KiB error body, at most
  128 returned response headers and 8 KiB per header value. Connect timeout is
  configurable; no hidden total/stall timeout or retry is added.
- Dropping the pending transport future cancels DNS/send/error-body collection;
  dropping ByteStream releases response I/O. No detached task or buffering of
  successful response bodies. Existing adapters own cancellation/deadlines.
- Diagnostics never format URLs, headers, bodies, DNS or reqwest errors.
  Transport Debug omits the pinned origin; HttpRequest Debug redacts the URL
  too, since paths and queries may carry keys.
- Use optional reqwest 0.12 with rustls-tls/stream and optional url, matching
  the existing CLI dependency. Enable Tokio net only with the feature.
  The locked graph will be checked on Rust 1.86; default provider stays 1.86
  even if a feature-only dependency floor needs separate documentation.

## Migration and verification

Keep a thin CLI compatibility wrapper for its existing public constructor;
all HTTP mechanism/tests move into provider, preserving assertions. The CLI
selects PublicHttps and pins the origin. Tests use a private scripted DNS
resolver and local TCP servers, without external network dependence. Full
workspace, docs, lint, dependency-policy and both compiler lanes are gates.
Do not archive: this task commits locally, without shipping or pushing.
