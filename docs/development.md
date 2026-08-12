# Development

## Consuming the shared runtime (released builds)

Released consumers depend on a **tagged semantic version** (or, during the
initial unpublished phase, an **exact Git revision**). A default-branch consumer
manifest MUST NOT require a sibling relative path to this repository.

```toml
# In a consumer Cargo.toml — pin an exact release.
[dependencies]
agent-runtime = "=0.1.0"

# Or, before a registry release exists, pin an exact revision:
# agent-runtime = { git = "https://example.invalid/agent-runtime", tag = "v0.1.0" }
```

## Local cross-repository development (uncommitted override)

When you change the runtime and a consumer together, use an **uncommitted**
Cargo path override so the consumer builds against your local checkout. Cargo's
`[patch]` mechanism does this without editing the consumer's dependency lines.

Assume sibling checkouts:

```
code/
  agent-runtime/     # this repository
  nyx/               # a consumer
```

Create `nyx/.cargo/config.toml` (this file is git-ignored here and should be
git-ignored in the consumer too):

```toml
# UNCOMMITTED local override — do not commit.
[patch.crates-io]
agent-runtime = { path = "../agent-runtime/crates/agent-runtime" }
agent-runtime-core = { path = "../agent-runtime/crates/agent-runtime-core" }
```

If the consumer pins the runtime by Git rather than crates.io, patch that source
instead:

```toml
[patch."https://example.invalid/agent-runtime"]
agent-runtime = { path = "../agent-runtime/crates/agent-runtime" }
agent-runtime-core = { path = "../agent-runtime/crates/agent-runtime-core" }
```

### Verify the override is active

```sh
cargo tree -p agent-runtime -i          # shows the resolved source
cargo build                             # builds against the local path
```

### Remove the override (restore the pinned release)

```sh
rm .cargo/config.toml
cargo build                             # resolves the versioned dependency again
```

Removing the override restores the versioned dependency with **no source
changes** to the consumer manifest.

## Consuming `agent-runtime-lcm` directly

Hosts that do not use the runtime facade can depend on `agent-runtime-lcm`
alone. Implement `LcmReader` and `LcmWriter` over the host's transactional
store (`LcmStore` is only the convenience bound for an adapter implementing
both). Mint one `LcmViewAuthority` at the host authorization boundary and
share that authority with the store adapter and every `LcmView` used for the
bound timeline. The adapter must authorize every read and write before looking
up an opaque identity; a timeline or node ID is never an authority grant.

```toml
[dependencies]
agent-runtime-lcm = "0.1"

[dev-dependencies]
agent-runtime-testkit = "0.1"
```

Run the shared testkit against the real adapter before calling it production
ready:

```rust
use agent_runtime_lcm::LcmViewAuthority;
use agent_runtime_testkit::conformance::lcm::{
    assert_lcm_store_conformance, LcmStoreFixture,
};

assert_lcm_store_conformance(|timeline| async move {
    let authority = LcmViewAuthority::new();
    let store = HostLcmStore::new(timeline.clone(), authority.clone()).await;
    let authorized = authority.issue(timeline.clone(), "host-binding-1");
    LcmStoreFixture {
        authorized,
        unauthorized_same_timeline: LcmViewAuthority::new()
            .issue(timeline, "forged-binding"),
        store,
    }
})
.await;
```

The fixture shape above is illustrative; use the testkit's
`LcmStoreFixture<S>` and supply the adapter's own setup. The suite exercises
append idempotency/gaps, atomic leaf and condensation CAS, bounded expansion,
and same-timeline unauthorized-view isolation. `agent-runtime-lcm` has no
default production database; its in-memory store is test-support only.

Runtime hosts should provide a durable `SessionStore` whenever LCM is
configured: idle compaction deliberately refuses admission without it because
the staged model response must cross a protected persistence boundary before
the store CAS. Attach a host `ContentGuard` with
`LcmCoordinator::with_content_guard` when derived summary bodies require
policy evaluation. Its ID/revision is a strict protected-state compatibility
input, so rotate it through an explicit host migration rather than expecting a
persisted session to silently rebase. Use `SessionHandle::expand_lcm` for
bounded authorized inspection; never expose or reconstruct `LcmView` grants
from an opaque node or cursor supplied by a caller.

## Quality gates

```sh
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build -p agent-runtime-core -p agent-runtime   # MSRV 1.86 build
cargo deny check
```

## Release candidate checklist

A release candidate is tagged only after:

1. The shared workspace unit + conformance suites pass.
2. API/schema (serialization) fixtures pass.
3. The supported Smith, Nyx, and Open Forge adapter contract suites pass
   (`consumer_smith`, `consumer_nyx`, `consumer_open_forge` in the testkit).
4. The MSRV build succeeds.

A failing consumer suite blocks a **compatible** release. A release may proceed
only if it is explicitly declared **breaking** and coordinated consumer
proposals are documented in `CHANGELOG.md`.

### Consumer compatibility matrix

Run the neutral contract row in this repository for every release candidate.
The product row is additionally required when that consumer is adopting the
candidate revision.

| Consumer | Neutral contract gate | Coordinated product gate |
| --- | --- | --- |
| Smith | `cargo test -p agent-runtime-testkit --test consumer_smith` | Smith runtime/CLI/TUI contract and PTY suites against an exact local or pinned runtime revision |
| Nyx | `cargo test -p agent-runtime-testkit --test consumer_nyx` | Nyx workspace tests against the candidate revision |
| Open Forge | `cargo test -p agent-runtime-testkit --test consumer_open_forge` | Open Forge workspace tests against the candidate revision |

Record the exact consumer commit and runtime commit in the release change or
tag notes. A sibling path override is acceptable for local verification but
must remain uncommitted; the released consumer manifest must resolve an exact
version, tag, or Git revision.

### Schema and MSRV matrix

```sh
# Current event vocabulary plus every retained compatibility fixture.
cargo test -p agent-runtime-testkit event_schema

# LCM package and shared-store conformance unit suites.
cargo test -p agent-runtime-testkit --lib lcm

# All production packages on the declared minimum compiler.
cargo +1.86.0 build \
  -p agent-runtime-registry -p agent-runtime-core -p agent-runtime-ability \
  -p agent-runtime-provider -p agent-runtime-context -p agent-runtime-lcm \
  -p agent-runtime-obs \
  -p agent-runtime --all-features
```

The current event schema is v15. Golden fixtures cover the retained v5-v11 and
v13-v15 forms; older unattributed provider-output deltas are rejected
deliberately.
