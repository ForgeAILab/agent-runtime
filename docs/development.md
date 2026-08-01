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

# All production packages on the declared minimum compiler.
cargo +1.86.0 build \
  -p agent-runtime-registry -p agent-runtime-core -p agent-runtime-ability \
  -p agent-runtime-provider -p agent-runtime-context -p agent-runtime-obs \
  -p agent-runtime --all-features
```

The current event schema is v9. Golden fixtures cover the compatible v5, v6,
v7, v8, and v9 forms; older unattributed provider-output deltas are rejected
deliberately.
