# Verification Evidence

Verified on 2026-08-02 against the coordinated Smith working tree.

## Runtime gates

- `cargo fmt --all -- --check` — passed after final formatting.
- `cargo test --workspace --all-targets` — passed.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo +1.86.0 build --all-features -p agent-runtime-registry -p agent-runtime-core -p agent-runtime-ability -p agent-runtime-provider -p agent-runtime-context -p agent-runtime-obs -p agent-runtime` — passed.
- `cargo test -p agent-runtime --test active_turn_steering` — 6 passed.
- `cargo test -p agent-runtime-testkit` — passed, including the Smith steering consumer and reusable steering-barrier scenario.
- Event-schema conformance passed with the v11 golden fixture.

The MSRV gate initially exposed let-chain syntax in the existing Anthropic
adapter and reasoning accumulator plus the new goal-admission gate. These were
rewritten without behavior changes; provider tests, the full workspace, Clippy,
and Rust 1.86 all pass afterward.

## Revision handoff

An immutable runtime revision is not yet available because this implementation
is still an uncommitted working tree and no commit was authorized. The exact
revision must be recorded here and in Smith's workspace dependency after the
runtime changes are committed. Until then Smith's ignored `.cargo/config.toml`
patch exercises this sibling checkout without weakening the committed pin.
