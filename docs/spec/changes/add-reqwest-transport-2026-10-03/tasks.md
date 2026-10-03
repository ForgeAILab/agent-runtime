---
created_at: 2026-10-03T00:00:00Z
updated_at: 2026-10-03T00:00:00Z
completed_at:
---

## 1. Proposal

- [x] 1.1 Read repository rules, canonical CLI transport and consumer references.
- [ ] 1.2 Validate proposal and record authorized approval before implementation.

## 2. Implementation

- [ ] 2.1 Add optional dependencies, required policy, origin pin and timeout API.
- [ ] 2.2 Move canonical transport checks, bounds, status mapping and streaming.
- [ ] 2.3 Cover loopback/all-answer DNS checks, redaction, redirects, streaming,
  cancellation and response bounds; retain existing CLI assertions.
- [ ] 2.4 Migrate CLI to a shared PublicHttps transport without policy changes.

## 3. Documentation and gates

- [ ] 3.1 Add compile-checked usage, README package/usage, CHANGELOG and provenance.
- [ ] 3.2 Verify optional-feature Rust 1.86 and default dependency isolation.
- [ ] 3.3 Run all requested formatting, Clippy, workspace/doc, MSRV and deny gates.
- [ ] 3.4 Validate final spec, tick tasks, record outcomes and commit; do not push.
