---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-01T00:00:00Z
---

# Design: Private sharing and incremental accounting under existing contracts

## Evidence on Main

Verified at `main`/HEAD `b4b421fe8e049709063bac02e602bdeadf296c26`.

| Verified fact | Evidence |
| --- | --- |
| The turn driver clones history before building a request and again for snapshots. | `crates/agent-runtime/src/agent/driver/turn.rs:86`, `:1154` |
| A new Arc history view is built by first deep-copying the supplied slice. | `crates/agent-runtime/src/agent/driver/provider.rs:429` |
| Arc-backed immutable HistoryView already exists. | `crates/agent-runtime/src/harness/pipeline.rs:91` |
| History-to-fragment rendering and fragment-to-message rendering clone messages. | `crates/agent-runtime/src/agent/planning.rs:553`; `crates/agent-runtime-context/src/planner.rs:648` |
| Provider requests and checkpoint snapshots have public Vec fields; plan-to-request and CallingModel construction clone them. | `crates/agent-runtime-core/src/provider.rs:1366`; `crates/agent-runtime-core/src/store.rs:213`; `crates/agent-runtime-context/src/plan.rs:271`; `crates/agent-runtime/src/agent/driver/turn.rs:1257` |
| SessionHandle already has both an owned history getter and a borrowed with_history helper. | `crates/agent-runtime/src/runtime/session/lifecycle.rs:71`, `:80` |
| Pressure loads the entire entry history and resizes the raw suffix. | `crates/agent-runtime/src/harness/lcm.rs:1442`, `:1471` |
| Prefix checking JSON-encodes complete history; strict projection also verifies all stored entries and the current DAG revision. | `crates/agent-runtime/src/harness/lcm.rs:918`, `:1030`, `:3056`, `:3081` |
| Existing bounded range reads, revision checks, and authorization are available. | `crates/agent-runtime-lcm/src/store.rs:349`, `:357`; `crates/agent-runtime/src/harness/lcm.rs:1219` |
| LcmState stores history fingerprint, frontier and active nodes; any component descriptor revision mismatch fails. | `crates/agent-runtime/src/harness/lcm.rs:399`, `:680`, `:719` |
| Fingerprint is FNV-1a-128 and already has a cloneable incremental byte hasher. | `crates/agent-runtime-registry/src/fingerprint.rs:1`, `:34`, `:79` |
| The three consumer targets are invoked individually by CI; fixture builders lack stores/contributors/LCM. | `.github/workflows/ci.yml:139`; `crates/agent-runtime-testkit/src/consumers/smith.rs:22`, `nyx.rs:44`, `open_forge.rs:60` |

File inventory on this main revision confirms 20 direct Rust integration-test
roots in `crates/agent-runtime/tests/` and seven in
`crates/agent-runtime-testkit/tests/`, with automatic discovery and no explicit
test-target overrides in their Cargo manifests. The workspace has 12 members
(`Cargo.toml:3`). The audit's broad hot-copy/read finding is confirmed; its
"about seven copies" is path-dependent, not a measured invariant. Arc history
and a borrowed public history projection already exist, so no new public
history wrapper is warranted.

## Decisions

### Private History Generations

Introduce immutable session-owned generations behind existing boundaries.
Capture a generation under the session lock, then share an Arc while awaiting
planning/projector/checkpoint work. Use copy-on-write for append/replacement
while a generation is pinned. Existing HistoryView can reuse one contiguous
Arc<[Message]> materialization per generation rather than rebuilding it for
each phase. Keep the representation private; do not require nested
Arc<[Arc<Message>]> in the public provider or checkpoint types.

Preserve independent owned results from `history()` and `snapshot()`.
ProviderRequest.messages, FragmentContent::Message, and serialized checkpoints
require owned Message values today. Materialize those at their current public
boundaries and measure remaining clones; do not claim zero-copy operation
across every plan/request/checkpoint. COW may still copy a pinned generation
on append. This slice removes redundant private reads without promising O(delta)
history mutation or adding a public type migration. Sharing cannot mutate a
frozen plan, signed reasoning, normalized prepared action, or retained snapshot.

### Process-Local LCM Accounting

Use a private session-scoped sidecar, not new fields in persisted LcmState.
Cache per-entry token counts, checked prefix totals, active-node totals and
coverage, and incremental canonical-history fingerprint state. Bind it to the
actual authorized session/timeline grant, binding/store/sizer/classifier/guard
revisions, DAG revision, and trusted canonical-history generation/frontier.
Keep grants opaque and nonserializable. Never key authority by an ID alone.

Cold construction/resume uses existing full authorization, history/store
validation and sizing once. Warm pressure accounting explicitly re-authorizes
the view and checks current_revision; unchanged evidence reuses totals. For a
trusted append-only successor, validate and size only new entries, extending
checked totals. Summaries adjust active-node totals and subtract source-range
totals with checked arithmetic after their existing protected CAS commit.
Pending summaries, partial commits, and restart recovery retain current rules.
Use existing load_range paging for required deltas; no new store trait method
is needed. Never infer that a partial range is a complete canonical prefix.

Revocation or a binding/store/sizer/classifier/guard/DAG revision change
invalidates reuse. A truncated/replaced/untrusted history generation does too.
Take the existing full-validation path and retain its existing conflict/reject
behavior, rather than adopting external DAG mutations or a new policy
automatically. Every lookup still requires authorization. Separate accounting
reuse from strict projection: projecting raw messages and validating source
content may still read O(history); this proposal does not remove those checks
merely to advertise an O(delta) provider call.

On warm append-only accounting, cost is O(new entries plus locally changed
node records), aside from required authority/revision checks. With no delta,
repeat pressure evaluation does not reload or resize old entries solely to
count tokens. Memory for counts is O(history), managed for the session lifetime;
it adds no O(calls × history) serialized state. Cache loss affects performance
only, not budget decisions, grants, revisions, or canonical accounting.

### Stable Fingerprints and Cold Compatibility

Do not persist the audit's proposed rolling SHA-256 replacement: existing
LcmState.history_fingerprint is the fingerprint of exact serde JSON array
bytes, and descriptor revision changes currently wedge resume. Keep its
algorithm, bytes, schema, and descriptor revision unchanged.

For trusted append-only generations, retain a cloneable incremental
FingerprintHasher over the opening bracket and serialized entries/comma
separators; clone it and append the closing bracket to obtain exactly the
existing Fingerprint::of(serde_json::to_vec(history)). Serialize each new entry
once. Cache only runtime-owned generations; externally supplied slices take
the full path. Golden fixtures must cover empty arrays, escaping, every content
part, reordered/replaced history, and byte equality with main. This is a change
detector, not a cryptographic integrity or authorization boundary.

Existing persisted LCM state decodes through the same strict revision checks;
the private accounting sidecar is rebuilt. Do not add cache payloads to
extension_state, relax descriptor validation, alter LCM algorithm revisions,
or use persisted caches to bypass startup/source validation. SHA/journal
integrity and revision-tolerant migrations remain separate later work.

### Fewer Binaries, Same Coverage

Consolidate 20 runtime test roots into one integration root with private
scenario modules. Consolidate testkit's provider/runtime/goal/delegation
conformance roots into one, leaving the three named consumer roots runnable
by the existing CI commands: seven targets become four. Record the complete
test inventory before moving and compare qualified cases after moving; keep
feature gates, fixtures, ignored cases, async runtime settings, and privacy.
Do not drop tests, dependencies, scenarios, or consumer gate labels to obtain
a lower count. Test isolation must not rely on process-global mutable state;
fix revealed test collisions locally rather than running everything serially.

No normative delta requires a binary count, physical Arc layout, hash sidecar,
or validator cache. Delta requirements cover only public value isolation and
equivalent pressure/authorization behavior. Provider adapter duplication,
schema serialization improvements, and a wire-module extraction are deferred
to avoid unrelated refactoring in this independently reviewable change.

## Validation and Migration

Instrument fake sizer/store calls to separate pressure-accounting work from
projection and summarization reads. Compare warm totals/decisions/events
against a cold full-recompute oracle for no delta, append, compaction, overflow,
changed revisions, revocation, replaced history, and cold resume. Assert no
old-entry sizing for a validated warm append and no accounting-only full-range
read for unchanged history; wall-clock speed assertions are unnecessary.

Verify old snapshots remain independent after append, exact new checkpoints
round-trip through legacy-shaped serde fixtures, signed provider continuation
survives, and all named consumer commands still run. U3 determines manifest
retention if landed; U11 adds no different policy. Capture optional allocation
and CI-link measurements before/after for review without treating estimates as
completed evidence. No consumer migration or persisted-state backfill is needed.

## Open Questions

- Approve the deliberately narrower private-sharing boundary, accepting
  unavoidable copies at current public Vec boundaries.
- Approve retaining three consumer test binaries plus one testkit conformance
  binary rather than the audit's one-binary-per-crate rule.
