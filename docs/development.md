# Development

## Consuming the shared runtime (released builds)

Released consumers depend on a **tagged semantic version** (or, during the
initial unpublished phase, an **exact Git revision**). A default-branch consumer
manifest MUST NOT require a sibling relative path to this repository.

```toml
# In a consumer Cargo.toml — pin an exact release.
[dependencies]
agent-runtime = "=0.5.0"

# Or, before a registry release exists, pin an exact revision:
# agent-runtime = { git = "https://example.invalid/agent-runtime", tag = "v0.5.0" }
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
agent-runtime-lcm = "0.2"

[dev-dependencies]
agent-runtime-testkit = "0.2"
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
default production database; its volatile in-memory store backs ephemeral
sessions and the conformance suite, and is not a persistence backend.

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

## Session journal reference backend and conformance

`agent-runtime-core::journal::InMemorySessionJournal` is the volatile neutral
reference implementation of `SessionJournal`. Construct protected stores with
`InMemorySessionJournal::protected`; ordinary stores require an explicit
`JournalProjectionPolicy` through `InMemorySessionJournal::ordinary`, and the
projected values are encoded and hashed only after that policy succeeds. The U9
design names no durable neutral file/database adapter: production hosts remain
responsible for crash-safe framing, flushes and cross-process fencing.

The reference backend has no wall-clock lease expiry. It implements atomic
commit, lifetime idempotency, pinned reachability and monotonic writer fences.
Before a head becomes visible it is materialized through `JournalReader`, so a
boundary with a wrong history chain or count, or a checkpoint whose operation
fingerprint does not match its state, is refused instead of published.
Checkpoint progress is compared with the latest published checkpoint of the
session, even across heads that carry none. Lease expiry and revocation stay
host policy: `InMemorySessionJournal::revoke_writer` is that host control, and a
writer lease dropped without `close` keeps the session's writer slot until the
host revokes it. The single transaction lock is held while the ordinary
projection policy runs, so a policy must not call back into the same journal.
`retire` and `collect` deliberately return the typed
`JournalRetirement`/`JournalCollection` unsupported capabilities and retain all
content until U9 group C. The runtime selects native mode for a fresh session
when every configured store exposes `journal()` (or for a session with a native
head), keeps mixed capability pairs and existing v3 records on the materialized
legacy path, and rejects a detectable native head whose configured pair lost a
capability. An ordinary head published before the session's first exact
checkpoint resumes alone, as a legacy snapshot does; one published after an
exact checkpoint requires the protected head. If every configured store stops
exposing `journal()` there is nothing left to ask, so a native-only session
then reports not found unless the host also kept a materialized record.
Writer fences are released at shutdown and when a session handle is dropped.
Ephemeral sessions bypass discovery entirely. V3 import remains a later
group-B task.

Adapters run the reusable task-2.1 contract with
`agent_runtime_testkit::conformance::journal::assert_session_journal_conformance`
(protected role) and `assert_ordinary_session_journal_conformance` (ordinary
role, built from a factory that receives the scenario's host policy). A
`SessionJournalConformanceFixture` pairs a protected adapter with a
deterministic failure injector for every commit visibility cut and with the
host's writer-lease revocation, which the suite uses to hand the fence to a
successor. `assert_session_journal_reference_conformance` runs both against the
reference. The testkit's unit tests also run the suite against deliberately
broken backends (non-atomic commit, idempotency outside the transaction or
keyed by operation only, reads that skip the lease, foreign-object resolution,
unverified object reuse, a revocation that keeps the writer, an ordinary
adapter ignoring policy or dropping exact roots) and require each to be
rejected.

### Native turn byte-cost evidence (U9 task 2.7)

The native writer structurally encodes retained manifests and the Runtime-owned
redaction-safe `runtime.core.previous_cache` value. Arrays use bounded
single-reference leaves plus a fixed-height binary spine, so unchanged prefixes
are referenced rather than submitted. Host-owned opaque extensions remain
whole-value replacements. Ordinary commits carry a changed record either as a
structured draft plus a delta reference, or directly in the semantic delta,
never both.

The deterministic measurement below uses an identity ordinary policy, one
bootstrap turn, then either a one-message turn or a tool-call/result/final
turn. `S/N` means logical serialized submission bytes / newly retained
immutable-object bytes. Submission bytes use the same component accounting as
the 2.2–2.4 review: candidate object envelopes + serialized semantic delta +
batch/checkpoint envelopes. New-object bytes are the before/after encoded-byte
delta in `InMemorySessionJournal`, including projected nodes, batch and head.

| Plain-turn commit kind | N=100 S/N | N=1000 S/N | N=4000 S/N |
| --- | ---: | ---: | ---: |
| Protected Accepted | 11,968 / 12,814 | 12,562 / 13,410 | 12,124 / 12,972 |
| Protected Planning | 2,118 / 3,033 | 2,125 / 3,042 | 2,125 / 3,042 |
| Protected CallingModel | 3,959 / 4,735 | 3,968 / 4,746 | 3,968 / 4,746 |
| Protected ModelResponseReady | 18,343 / 19,137 | 18,669 / 19,465 | 18,672 / 19,468 |
| Protected Completing | 11,695 / 12,542 | 12,289 / 13,138 | 11,851 / 12,700 |
| Protected PublishingTerminal | 2,122 / 3,038 | 2,129 / 3,047 | 2,129 / 3,047 |
| Protected Terminal | 2,122 / 3,038 | 2,129 / 3,047 | 2,129 / 3,047 |
| Ordinary terminal | 29,620 / 34,177 | 30,259 / 36,430 | 30,265 / 40,412 |
| Ordinary unchanged (each) | 1,183 / 2,011 | 1,187 / 2,017 | 1,187 / 2,017 |

| Tool-turn commit kind | N=100 S/N | N=1000 S/N | N=4000 S/N |
| --- | ---: | ---: | ---: |
| Protected Accepted | 11,952 / 12,801 | 12,546 / 13,397 | 12,108 / 12,959 |
| Protected Planning (first request) | 2,114 / 3,028 | 2,121 / 3,037 | 2,121 / 3,037 |
| Protected CallingModel (tool request) | 4,018 / 4,793 | 4,027 / 4,804 | 4,027 / 4,804 |
| Protected ModelResponseReady (tool call) | 18,524 / 19,317 | 18,850 / 19,645 | 18,853 / 19,648 |
| Protected AwaitingApproval | 26,147 / 26,944 | 26,741 / 27,540 | 26,303 / 27,102 |
| Protected ExecutingTools (admission) | 2,818 / 3,733 | 2,825 / 3,742 | 2,825 / 3,742 |
| Protected ToolOutcomeReady | 3,204 / 4,119 | 3,211 / 4,128 | 3,211 / 4,128 |
| Protected ExecutingTools (committed result) | 19,454 / 20,218 | 20,048 / 20,814 | 19,610 / 20,376 |
| Protected Planning (final request) | 2,410 / 3,325 | 2,417 / 3,334 | 2,417 / 3,334 |
| Protected CallingModel (final request) | 3,773 / 4,548 | 3,781 / 4,558 | 3,781 / 4,558 |
| Protected ModelResponseReady (final response) | 17,492 / 18,285 | 17,911 / 18,706 | 17,914 / 18,709 |
| Protected Completing | 11,830 / 12,674 | 12,424 / 13,270 | 11,986 / 12,832 |
| Protected PublishingTerminal | 2,119 / 3,034 | 2,126 / 3,043 | 2,126 / 3,043 |
| Protected Terminal | 2,119 / 3,034 | 2,126 / 3,043 | 2,126 / 3,043 |
| Ordinary terminal | 46,492 / 51,328 | 47,341 / 53,791 | 47,350 / 57,776 |
| Ordinary unchanged (each) | 1,180 / 2,007 | 1,184 / 2,013 | 1,184 / 2,013 |

The complete second-turn totals are constant in N within bounded reference
metadata: plain protected 52,327/58,337, 53,871/59,895, 52,998/59,022;
plain ordinary 29,620/34,177, 30,259/36,430, 30,265/40,412; tool protected
127,974/139,853, 131,154/143,061, 129,408/141,315; and tool ordinary
46,492/51,328, 47,341/53,791, 47,350/57,776. Every second-turn commit submits
at most 512 bytes of new Message/Usage bodies. An unchanged ordinary boundary
submits zero drafts and zero body bytes; the ~1.18 KB submission and ~2.01 KB
new-object cost are only revisioned delta/batch/head metadata.

The changed previous-cache record is one structured reference at
ModelResponseReady. Its submission/new-object bytes are 18,343/19,137,
18,669/19,465 and 18,672/19,468 instead of the review's O(N) draft bytes
13,362, 114,171 and 453,171. The ordinary terminal carries one cache reference
and one plain (or two tool) manifest references. The changed opaque
`runtime.manifest_boundary` replacement contributes 140 serialized delta bytes
at every N. Any other opaque extension still costs its full replacement once
in an ordinary submission; native encoding does not reinterpret its value.

Bootstrap is deliberately O(N), and the generic persistent JSON representation
trades extra first-write objects for bounded later writes:

| Bootstrap/control | N=100 | N=1000 | N=4000 |
| --- | ---: | ---: | ---: |
| Native plain bootstrap S/N | 285,872 / 607,811 | 2,318,762 / 5,107,642 | 9,114,257 / 20,124,203 |
| Native tool bootstrap S/N | 294,745 / 619,525 | 2,327,637 / 5,119,362 | 9,123,130 / 20,135,917 |
| Legacy plain protected turn / ordinary terminal | 154,348 / 48,207 | 1,414,421 / 443,321 | 5,659,421 / 1,772,321 |
| Legacy tool protected turn / ordinary terminal | 316,555 / 63,352 | 2,836,721 / 570,973 | 11,326,721 / 2,277,973 |

Legacy materialized stores and bytes are unchanged; their turn, terminal and
unchanged-save costs remain O(N). N=10,000 is not comparable because the
existing cache-identity contract caps stable-history entries at 4,096, causing
both paths to fail planning before provider I/O. The largest successful case is
N=4,000. Reproduce the table with:

```sh
cargo test -p agent-runtime --all-features --test integration \
  session_journal_equivalence::task_2_7_native_turn_bytes_are_delta_bounded \
  -- --exact --nocapture
```

## Quality gates

```sh
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test -p agent-runtime-provider --features command-provider --test command_provider
cargo build -p agent-runtime-core -p agent-runtime   # MSRV 1.86 build
cargo +1.88.0 build -p agent-runtime-mcp -p agent-runtime-cli
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

# Bounded diagnostics, frozen legacy readers, mixed migration restarts,
# protected recovery, and manifest replay reproducibility.
cargo test -p agent-runtime-testkit --lib manifest
cargo test -p agent-runtime-testkit --lib conformance::replay
cargo test -p agent-runtime manifest_tests

# LCM package and shared-store conformance unit suites.
cargo test -p agent-runtime-testkit --lib lcm

# All production packages on the declared minimum compiler.
cargo +1.86.0 build \
  -p agent-runtime-registry -p agent-runtime-core -p agent-runtime-ability \
  -p agent-runtime-provider -p agent-runtime-context -p agent-runtime-lcm \
  -p agent-runtime-obs -p agent-runtime --all-features

# The command-provider feature remains on the embeddable Rust 1.86 lane.
cargo +1.86.0 test -p agent-runtime-provider --features command-provider

# The official MCP SDK sets the package-specific floor for these leaf hosts.
cargo +1.88.0 build -p agent-runtime-mcp -p agent-runtime-cli --all-features
```

The current event schema is v15. Golden fixtures cover the retained v5-v11 and
v13-v15 forms; older unattributed provider-output deltas are rejected
deliberately.
