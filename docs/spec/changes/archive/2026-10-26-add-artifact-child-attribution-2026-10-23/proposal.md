---
created_at: 2026-10-23T22:20:00Z
updated_at: 2026-10-26T05:08:30Z
---

## Why

A parent-owned `delegation.child-reply` artifact records only its owning
session and purpose, so a host listing a session's artifacts cannot say which
child a stored reply came from. Copied child artifacts already name the child
session through their lineage; the reply is new parent content and has none.
Smith's `artifact.list` needs the child to let a model find a specific child's
reply without guessing an id.

## What Changes

- `ArtifactProvenance` gains an optional `child: Option<ChildId>` with a
  `with_child` builder. It is omitted from serialization when absent, so
  existing records load unchanged and unattributed artifacts serialize as
  before.
- A stored child reply is attributed to the child that produced it.
- The reply's idempotency key gains a `v2` tag, so a reply stored before this
  change can never collide with an attributed one under the same key.
- The semantic-summary wire mirror accepts the optional field.
- Artifact transfers are unchanged: their lineage already names the child
  session, and `ArtifactTransfer` keeps its fields.

## Impact

- Affected specs: `artifact-management`
- Affected code: `crates/agent-runtime-core/src/artifact.rs`,
  `crates/agent-runtime/src/delegation/persistence.rs`,
  `crates/agent-runtime/src/harness/semantic_summary_state.rs`,
  delegation conformance tests
- Additive public field on a struct without `#[non_exhaustive]`: consumers that
  build `ArtifactProvenance` with a struct literal must add `child`. Smith,
  nyx, and open-forge build it only through `ArtifactProvenance::new`.
