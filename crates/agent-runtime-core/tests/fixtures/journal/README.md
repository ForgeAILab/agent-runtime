# Frozen journal-json-1 fixtures

Object `.json` files contain exact UTF-8 bytes **without a trailing newline**.
Each companion `.digests.json` freezes object-schema 4, alternate-schema 3 and
alternate-type SHA-256 results over the same bytes. Digests were calculated
independently with Python hashlib and explicit big-endian length framing,
not by calling the Rust implementation under test. Tests compare bytes and
digests; changing encoding must introduce a new encoding revision.

Fixtures cover opaque signed reasoning (NUL, newline, Unicode), signed negative
zero, positive zero, full-width signed/unsigned integers, lexical nested keys,
array order, bounded sequence/map leaves and redaction changing sizes/IDs.
The ordered-extension fixture binds opaque JSON map insertion order explicitly
as metadata without changing lexical payload keys. Hosts with preserve_order
restore it; hosts unable to represent that execution order reject it.
`history.digests.json` freezes empty and one-message history-chain domains.
`commit-input.json` and its SHA-256 freeze the distinct private retry-input
domain, binding drafts, semantic bootstrap/delta, fence and operation identity.
Ordinary policy/adapter tests omit Sensitive state, preserve the exact protected
view, reject unavailable projection and assert stable projection on retries.

The separate `tests/support/legacy_revision4.rs` oracle freezes the v3 enum,
transition implementation and helpers from base `6ec58fa`. Its enum name and
visibility are adapted for a test module; execution predicates are unchanged.
`legacy_stores.rs` is copied unchanged from that base's testkit stores and still
implements only the old trait methods. Neither fixture implements a native writer.
