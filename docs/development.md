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
