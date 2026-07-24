# Contributing to Agent Runtime

Agent Runtime is a neutral, embeddable Rust runtime consumed by three
independently released products (Smith, Nyx, Open Forge). These rules keep it
neutral and safe to depend on.

## The boundary rule (non-negotiable)

The shared repository owns **reusable mechanism**. Consumers own **product
policy**: prompts, configuration UX, presentation, persistence backends,
approval policy, workspace behavior, and business-domain types.

- Production packages (`agent-runtime-core`, `agent-runtime`) **MUST NOT**
  depend on Smith, Nyx, Open Forge, or any of their domain types. `deny.toml`
  enforces this and CI fails if a consumer crate enters the graph.
- No product-name conditionals anywhere in production code. If you find yourself
  writing `if consumer == "nyx"`, the behavior belongs in the consumer.
- Hosts inject provider, tool, approval, workspace, session, secret, event, and
  clock behavior through the neutral traits in `agent-runtime-core`.

## Shared-code admission

New production behavior enters the shared repository only when **at least two
consumers require it** or it is **foundational to the approved runtime
contract**. Consumer-specific policy stays in the consumer until a second real
consumer and a neutral contract exist.

Feature flags may remove heavy optional implementations, but MUST NOT change the
meaning of public events or silently disable a security check.

## Quality gates

Run before opening a PR:

```sh
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --workspace --doc
cargo build -p agent-runtime-core -p agent-runtime  # verify on the 1.86 toolchain
cargo deny check                                    # license + dependency ban
```

Clippy warnings are errors. Public items should carry doc comments; doc examples
are compiled as tests.

## Toolchain and license

- Minimum supported Rust version: **1.86**, edition **2024**. Do not use APIs
  that require a newer compiler.
- The project is distributed under the **MIT** license. Transferred source keeps
  its upstream copyright notices; record every transfer in `PROVENANCE.md`.

## Source transfer

When importing behavior from a donor repository, use a temporary filtered clone
or subtree history — never rewrite the donor's working repository — and record
the donor repository, exact revision, original path, destination path, retained
notices, and any material refactor in `PROVENANCE.md`. Once transfer of a
component starts, the shared repository is its canonical owner; the old copy
must not evolve independently.
