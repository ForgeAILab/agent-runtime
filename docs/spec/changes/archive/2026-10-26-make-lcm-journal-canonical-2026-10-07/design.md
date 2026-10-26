---
created_at: 2026-10-07T00:00:00Z
updated_at: 2026-10-07T00:00:00Z
---

# Design: One canonical LCM content record with protected checkpoint publication

## Evidence on Main

All current-code statements refer to main/HEAD
`c2e464699a455af251c29bd228fd16f47a4ecba9`. No Rust build, host benchmark,
network or consumer modification was performed. The audit's G4 is a historical
proposal; U9 is a prerequisite document, not an already shipped implementation.

| Verified fact | File:line |
| --- | --- |
| SessionSnapshot owns a Vec<Message>; TurnCheckpoint embeds it and CallingModel stores its request. | `crates/agent-runtime-core/src/store.rs:209`, `:213`; `crates/agent-runtime-core/src/checkpoint/mod.rs:382`, `:558` |
| LcmEntry stores the exact structured Message, source metadata, sequence and content Fingerprint; validation recomputes that fingerprint. | `crates/agent-runtime-lcm/src/entry.rs:18`, `:26`, `:56`, `:92` |
| Main already appends only history[previous.history_len..], computes an operation ID and calls authorized append. Identical full prefix produces no append. | `crates/agent-runtime/src/harness/lcm.rs:1978`, `:1998`, `:2024`, `:2033`, `:2036` |
| LcmWriter append is idempotent, leaf/condensation commits atomic; existing tail truncation refuses committed node ranges. It has no combined journal-head/checkpoint transaction. | `crates/agent-runtime-lcm/src/store.rs:430`, `:467`, `:473`, `:479`, `:486` |
| U7 authorizes claims and ownership epochs; reconcile checks the active frontier before truncation. | `crates/agent-runtime-lcm/src/store.rs:434`, `:447`; `crates/agent-runtime/src/harness/lcm.rs:2124` |
| U8 explicit session intents and durable fork seed/intent/supersession already exist; ephemeral is explicit. | `crates/agent-runtime/src/runtime/command.rs:57`, `:149`, `:158`, `:167`; `crates/agent-runtime/src/runtime/fork.rs:9`, `:48`; `crates/agent-runtime/src/runtime/engine.rs:765`, `:1217`, `:1289` |
| U11 accounting is process-local evidence; it sizes/reads only the missing suffix on a warm generation and remains authorized. It is not a persisted canonical-content store. | `crates/agent-runtime/src/runtime/history.rs:25`; `crates/agent-runtime/src/harness/lcm/accounting.rs:1`, `:92`, `:175`, `:200` |
| LCM state records history_len/frontier/Fingerprint, active node metadata, tunable/identity revisions, watermarks and a Sensitive pending summary. | `crates/agent-runtime/src/harness/lcm.rs:434`, `:502`, `:514`, `:528`, `:532`; `:1517` |
| Restore is strict for binding/store/classifier/guard identity, then can rebuild changed tunables from validated store evidence. | `crates/agent-runtime/src/harness/lcm.rs:1529`, `:1562`, `:1572`, `:1712` |
| Existing hard-admission and exact-successor repair protect summary CAS; ordinary-only intent/successor saves precede provider I/O. Terminal source save precedes LCM synchronization. | `crates/agent-runtime/src/harness/lcm.rs:1750`, `:1762`, `:1795`; `crates/agent-runtime/src/agent/driver/turn.rs:441`, `:675` |
| Active projection contains summaries plus uncovered raw entries; it is not a lossless replacement for full canonical source history. | `crates/agent-runtime-lcm/src/projection.rs:62`, `:157`, `:174`, `:185` |
| Opaque authority is revoked/validated by grant identity, and every reader/writer takes a view. IDs and owner fences do not grant access. | `crates/agent-runtime-lcm/src/store.rs:113`, `:170`, `:189`, `:389`, `:395` |
| Ordinary redacted terminal history remains canonical at that view, while protected extensions can overlay at compatible boundaries. | `crates/agent-runtime/src/runtime/engine.rs:50`, `:70`, `:81`, `:112`, `:127`, `:174` |
| U3 manifests are bounded and absent from new protected snapshots; U1/U2 typed failures and normalization are implemented, not U10 work. | `crates/agent-runtime/src/runtime/manifests.rs:18`; `crates/agent-runtime/src/agent/driver/turn.rs:107`; `crates/agent-runtime-core/src/provider.rs:1616`; `crates/agent-runtime/src/agent/driver/provider.rs:680`; `crates/agent-runtime/src/tool/executor.rs:853` |
| U6 WorkingSetPolicy/ratios, tolerant tunables and host-instructed provider summary model exist. | `crates/agent-runtime/src/runtime/builder.rs:57`; `crates/agent-runtime/src/harness/lcm.rs:1154`, `:1163`, `:1231`, `:1562`; `crates/agent-runtime-lcm/src/summarize.rs:360`; `crates/agent-runtime/src/harness/mod.rs:15` |
| Three named consumer targets cover protected recovery, file views and storeless history, but not the proposed canonical transaction. | `.github/workflows/ci.yml:129`; `crates/agent-runtime-testkit/tests/consumer_open_forge.rs:216`, `:221`; `crates/agent-runtime-testkit/tests/consumer_smith.rs:180`, `:185`; `crates/agent-runtime-testkit/tests/consumer_nyx.rs:153`, `:158` |

## Bytes Today, After U9, and U10 Target

Use the same logical uncompressed-byte boundary and symbols as U9: H(N) history
bytes, U cumulative usage, E extensions, M bounded diagnostics, P frozen request,
F exact state payload and indicators I_checkpoint/I_ordinary. Main today writes

`B_main = I_checkpoint*(H + U + E + F + c_s)
        + I_ordinary*(H + U + E + M + c_o)
        + L(delta_history) + B_summary`,

where L(delta_history) is the new LCM Message bodies plus per-entry/source/index
metadata. The LCM suffix append already is O(delta_history); U10 must not claim
to invent it. Summaries create distinct derived bodies/edges, not identical
copies of all history. For 1000 user messages of 1024 ASCII characters,
H=1,078,001 bytes (serde shapes at `crates/agent-runtime-core/src/content.rs:23`,
`:37`, `:114`); a changed protected checkpoint still rewrites that array;
CallingModel may add another history-sized P or a smaller projected request.
U3 bounds M's retained records but not usage records or arbitrary changed
metadata; U11 changes private capture/accounting costs, not this serialization.
See U9 design's derivation for the 100/1000/10000 byte table and metadata terms.

After native U9 bootstrap:
`B_U9 = delta(journal exact content + ordinary projections + metadata)
      + reference paths + envelopes + L(delta_history) + B_summary`.

It removes rewrites but canonical history bodies can still reside in both
protected journal objects and LCM entries. U10 target:
`B_U10 = L(delta_history) + delta(non-content metadata + request-only bodies)
       + delta(ordinary policy projections) + reference paths + envelopes
       + B_summary`.

There is **no additional exact canonical-history body write** in the journal or
checkpoint. For a 1077-byte example Message, one 1077-byte canonical body is
written once in the protected domain (plus its LCM metadata), shared by timeline,
history and any byte-identical frozen request reference. An ordinary redacted
projection may require its own policy-domain body; that is an intentional view,
not a second exact authority. Request-only summaries/instructions/raw outcomes
and changing metadata remain separately accounted. A host using one exact blob
for an LCM row and its content-address index need not literally store the JSON
inside the row: row-to-blob references satisfy one-copy storage.

As in U9, target cost is delta of all changed persisted leaves plus O(d log R)
reference paths and fixed envelopes, independent of unchanged H. A newly
created history-sized manifest/cache representation or replaced opaque extension
can still add history-sized metadata; U10 does not change that public contract.
Require counted unique body writes and logical submitted bytes for large fixed
prefixes and one-message deltas, with non-LCM/U9/legacy controls. Do not infer
physical I/O, reclaimed percentage, or Forge's incident size without host data.
Cold load/import can read O(history); this is a write/source-of-truth proposal,
not another warm-accounting or lazy-history memory proposal.

## Exact Store Trait Diff

U10 changes no existing SessionStore, CheckpointStore, SessionJournal,
LcmReader, LcmWriter, LcmStore or LcmTimelineResolver method from U9/main.
Add a companion trait in agent-runtime-lcm (core has no dependency on LCM):

```rust
#[async_trait]
pub trait LcmCanonicalJournal: LcmWriter {
    fn journal(&self, view: &LcmView)
        -> Result<Arc<dyn SessionJournal>, LcmError>;
    async fn commit_history(
        &self,
        view: &LcmView,
        lease: &JournalLease,
        expected_dag_revision: LcmRevision,
        request: LcmJournalCommit,
    ) -> Result<LcmJournalCommitResult, LcmError>;
}
```

Both methods are required; no blanket/default implementation. Existing stores
keep compiling; they continue U9 journal+LCM duplication or legacy persistence.
Add the additive method
`LcmCoordinator::with_canonical_journal(self, Arc<dyn LcmCanonicalJournal>) -> Self`
to select configuration. At a durable non-ephemeral binding, before work,
configuration validates that this capability serves the same
LCM store/revision/authorized binding as the coordinator, and the same exact
JournalStorageId/protection domain as CheckpointStore.journal(). IDs alone are
not sufficient: backend conformance must demonstrate one transactional domain.
Canonical mode requires an exact checkpoint journal, even for otherwise
ordinary-only durable sessions; without it, fail explicitly. Ordinary-only
hard-admission behavior remains available unchanged outside canonical mode.
Ephemeral overrides the durable capability with its existing volatile path.
No mandatory LCM adoption or new methods on host contributors/events/commands.

journal(view) first calls unchanged authorize_view and returns a scoped wrapper;
every subsequent SessionJournal operation revalidates that unrevoked view and
host binding before any object lookup, lease/pin operation or deletion.
Collection through this wrapper may delete only objects/timelines authorized
by that view and host retention scope; internal domain-wide reachability checks
may retain other roots but cannot expose or delete other tenants' content. A
persisted timeline ID, digest, storage ID or claim generation never manufactures
a grant. The wrapper's lease stays scoped to that view, timeline, session and
protection domain. Owner-generation fences are checked again inside every
mutation transaction. Unauthorized errors remain metadata-only LcmError.

LcmJournalCommit contains U9's expected journal revision, writer fence,
operation ID/batch digest and exact metadata/checkpoint delta, plus one
LcmAppendRequest containing only new entries, resulting history source/count/
chain, completed_len, execution-view root and immutable projection/node roots. It has no second
inline history Vec. commit_history atomically validates expected DAG/journal
revisions and owner generation, deduplicates body objects, appends entries and
metadata, publishes exact head/checkpoint and records the idempotent result.
An empty entry suffix is valid for state-only checkpoints. Its result includes
JournalHead, resulting LcmRevision and already_committed. Exact duplicate
operation/digest returns the original result; a changed operation payload,
stale claim, gap, or divergent immutable content returns typed conflict.

All canonical checkpoint writes go through commit_history, including empty
suffixes; generic SessionJournal.commit cannot advance a timeline-backed
history root outside this transaction. Existing append/leaf/condensation APIs
remain for legacy mode and U6 compaction. Canonical sessions must not call
append_history after already publishing the same suffix. External timeline
writers cannot mutate a canonical claimed history without the current owner
fence; direct append outside this capability on a canonical-bound timeline is
rejected by the native backend. DAG compaction retains its existing independent
CAS and protected intent/exact-successor validation; a DAG commit is not made
atomic with a model call or invented as a checkpoint success.

## Canonical Content and Read Boundaries

Version-5 head/checkpoint references use
`TimelineHistory { timeline_id, owner_generation, durable_len, completed_len,
sha256_chain, store_revision, source_index_root, execution_view_root, projection_root }`.
Lengths are exclusive counts (source sequences 0..len), not an inclusive range
endpoint or a summary frontier. `durable_len` includes the exact active suffix
at the protected checkpoint. `completed_len <= durable_len` identifies the last
committed terminal conversation boundary, for any TurnFinish, including failed
or cancelled turns. The proposed last-terminal root is staged at PublishingTerminal and becomes
completed_len only in the protected Terminal commit, after the existing ordinary
save/publication barrier; until then the prior completed root stays selected.
It never advances merely because a model responded or an entry appeared. Cache/local actions that append no conversation
leave the count unchanged. Pending U8 fork/import retains prior roots.

Each source-index leaf binds timeline/entry ID/sequence, classification/revisions
and U9 SHA-256 of the exact Message encoding; the current LCM Fingerprint stays
for existing planning/DAG semantics. Do not treat Fingerprint as a secure content
address or change source/operation hash revisions globally. Heads pin one
contiguous immutable source prefix and checkpoint-specific projection node roots.
Request refs resolve the canonical LCM body when it matches exactly, retaining
arbitrary request-only content objects when it does not. Every active raw result
must be durable in the same transaction as the checkpoint referencing it.
Transient provider streaming events remain observability, not conversation
commit records or an alternate source of exact state.

Protected recovery resolves canonical Message bodies from the timeline and U9
metadata from its exact journal. Ordinary SessionStore readers resolve their
redacted projection using their declared boundary, without a hidden exact-store
fetch. The runtime still materializes public Vec<Message> history/snapshots;
it does not replace raw history with `ActiveProjection.candidates`. Planning
then uses existing LCM summary projection and the sole authoritative context
planner. Presentation and policy remain in hosts.

On protected nonterminal recovery use durable_len and the pinned projection at
that exact checkpoint. On ordinary terminal reads use completed_len and the
last-terminal projection root. A summary whose range crosses the selected
cutoff cannot replace a partially visible source span: use the captured prior
complete projection, not current active_nodes sliced or summary text truncated
at that count. Superseded nodes remain immutable/pinned while that view needs
them. Unreferenced or incomplete physical staging beyond durable_len is ignored;
raw append length never extends a reader's root. A tail referenced by an active
checkpoint is not ignored on exact resume simply because the turn is unfinished.
This narrows G4's proposal to ignore provisional entries to ordinary/terminal
reads, preserving mid-turn recovery guarantees.

### Exact Source and Existing Execution Views

Main's terminal ordinary-history rule is an execution invariant, not just a UI
rule. If terminal resume loads redacted history, the next provider request and
next active checkpoint must keep that history view; canonical mode must not
secretly restore the removed literals from the protected timeline. Keep an
internal exact source stream for integrity/DAG validation separate from the
public/planner execution view, linked by validated source positions and the
ordinary policy boundary. Prefix verification uses the exact source stream,
not equality between its digest and the redacted view's digest.

The execution_view_root references the exact bytes actually used at the
checkpoint. Unmodified positions reference the canonical Message object;
redacted/transformed positions reference distinct policy-view bodies in the
protected checkpoint domain where exact execution needs them. They are distinct
content, not another copy of the same canonical body. Only changed views are
stored. Ordinary views remain independently redacted under SessionStore policy,
and protected restoration never substitutes a raw source for a recorded view.
Nonterminal recovery restores its saved execution view; a terminal resume with
ordinary state selects that validated ordinary view exactly as on main.
Without ordinary state, retain main's authorized checkpoint-only behavior.

For planner projection on such a resume, uncovered source messages resolve
through the selected execution view. Summary candidates must satisfy existing
content-guard/classification and the configured ordinary projection policy
before entering that view; keep the resulting policy-view objects by reference.
Do not route exact expansion output directly into a request. A host unable to
supply a compatible policy view must fail canonical configuration/recovery and
keep U9; it must not guess redaction from a digest. Test a registered credential
literal absent in ordinary terminal history: compare the next request and
active checkpoint against main's materialized legacy path, in addition to
checking UI reads. This source/view separation does not modify planner authority
or allow unplanned provider messages.

## Protection Invariants

An LCM store is authorized on main but is not automatically a protected exact
checkpoint store. The companion capability must explicitly provide exact
confidential storage, verified signed continuation, immutable-body integrity,
checkpoint fencing and retention. Hosts with a redacting/insufficient LCM store
cannot enable canonical mode; they keep U9 instead. Secret sources retain their
existing storage/classification and cannot enter summary output. No credential
lease, LcmViewAuthority grant or provider secret resolver object is serialized.
Digests are integrity data, never authentication or default public telemetry.

Keep CheckpointStore as the owner of exact execution recovery, even though its
referenced Message bytes reside in the protected timeline domain. SessionStore
remains the owner of ordinary redaction policy. Exact raw timeline reads do not
replace redacted completed ordinary history in the existing terminal overlay.
Journal/projection boundary proof preserves identity floors, role/count,
usage and extension revisions, U3 planned-step counters, and explicit current
idle/hard-admission predecessor proofs. Redacted roots can differ from exact
roots and are never compared for equality as an authority shortcut.

Retain U6 strict binding/store/classifier/guard identity, tolerant tunable
rebuild, working-set sizing and summary acceptance; U7 authorized claims,
frontier errors and atomic RangeOverlap checks; U8 constructors, seed rules,
durable pending-fork repair and child-before-parent supersession. No product
prompt, Forge concept, consumer-domain dependency or permissive fallback enters
the library. Approval, workspace, cache identity and unknown model limits stay
fail-closed. Events, manifests and LcmError Debug remain redaction-safe.

## Crash Safety and Recovery Matrix

| Cut | Observable reader boundary / repair |
| --- | --- |
| Before body staging, or a torn staging frame | Old complete exact head and its source prefix. Detect length/hash failure; quarantine incomplete unreferenced tail under owner/writer fence. No entry is visible as accepted work. |
| Bodies staged, before commit_history transaction publishes entries + checkpoint | Old head/source prefix; stage pin keeps retry bodies alive. Orphans are ignored, later reclaimed under U9 epoch checks. Do not adopt the longest physical timeline. |
| Atomic source/checkpoint commit lands, acknowledgement lost | Entire new prefix and checkpoint are visible or neither. Same batch retry returns original head/revision. No state with a committed checkpoint but missing canonical entries is legal. |
| Exact commit before ordinary redacted projection | Exact active recovery resumes new durable_len; ordinary readers retain prior completed projection. Repair only from a compatible exact boundary through host redaction; never expose the new raw active suffix to ordinary readers. |
| PublishingTerminal committed, ordinary/event publication or Terminal pending | Resume preserves hook/result progress and existing scoped terminal republication protocol. It never repeats committed provider, tool or summary work. Advance ordinary completed view only at its verified terminal publication boundary. |
| Protected summary response saved, before independent leaf/condensation CAS | Reuse exact pending response and validate current revisions/authority before the CAS; do not rerun the summarizer. |
| DAG CAS lands, successor checkpoint not yet published | Existing protected pending-intent exact-successor proof adopts only that committed mutation; unknown/different DAG changes fail closed. No source truncation or empty-node guess. |
| Continue fork claims timeline, before child seed publication | U8 pending source intent and durable transfer pins keep parent/child roots alive; source stays fenced. Same fork repairs child under new authority/generation, or aborts only under U8's existing no-durable-successor conditions. |
| GC sweep races with resume/fork/canonical commit | U9 domain-wide root/pin fence includes source index, all live DAG sources, frozen requests, checkpoint/retention roots and migration/transfer roots. Atomic source publication registers roots before pins can be dropped. Recheck epoch before delete. |

The backend may implement the abstract source/checkpoint atomic commit via one
DB transaction or durable file staging plus a single head publication under a
cross-process fence. Reader LCM range/node methods in canonical mode obey that
same publication root; they must not reveal rows/files written before it.
Checksummed framed physical writes, SHA-256 source-index validation, sequence
continuity and root closure detect tears. Missing/corrupt content below a
published root is StateConflict requiring verified backup/replica repair;
never truncate a committed prefix and replay side effects. If an exact protected
request/outcome can verify a repair copy, the host may restore identical bytes
under the existing digest; no current planner or summary text invents a source.

Canonical sessions need no speculative tail truncation: entries become visible
only with their referring protected root. Legacy reconcile/truncate_from remains
unchanged for legacy mode and migration classification, always frontier-safe.
Do not remove the trait method or reinterpret committed summarized sources as
provisional. If migration sees a legacy ahead tail, validate U7/hard-admission
successor evidence or require explicit U7 Fork/Retire; do not silently destroy it.

## Collection with Concurrent Fork and Resume

U9 collect in the protected shared domain must enumerate timeline/DAG roots as
well as session roots. Every live leaf/condensation and retained projection pins
its source entries and child closure, even when no active session references
those entries. Summary coverage alone is not permission to delete raw source:
LCM expansion remains lossless. Individual source entries are never reclaimed
inside a live timeline. Full retired timeline content may be collected only
when host retention explicitly releases its DAG/archival roots and no session,
checkpoint, request, expansion-reader, child, import or transfer pin needs it.
No default timeline-retention deadline is introduced.

Continue forks transfer owner generation but retain source reachability through
both protected pending intent and child root; old owner cannot append. NewTimeline
Summary/Empty forks retain only their required seed/request objects, but parent
supersession does not drop host timeline retention. FromIndex forks either retain
source refs in the same protection domain or import selected content into the
new timeline before releasing source pins; a copy required by distinct ownership
or protection domains is recorded as fork seed/import cost, not a steady-state
second canonical copy. No permanent implicit cross-timeline authority is created.
Expansion-read pins are acquired before loading nodes and released after the
bounded read. GC consults root/pin epochs transactionally, so a concurrent fork
or resume either protects the closure or fails before dereference. Backends
without this proof leave collection unsupported.

## v3/v4 Upgrade, Including an Unfinished Turn

The first released U10 version reads supported v3-era unversioned snapshots and
schema-3 checkpoints, and U9 version-4 heads/checkpoint envelopes for its entire
release. New canonical records are version 5 and CHECKPOINT_SCHEMA_VERSION advances
from 4 to 5. Existing materialized checkpoint fields remain; legacy backends
write materialized schema-5 checkpoints and must update any restrictive version
guards, even without LCM adoption. The candidate accepts supported schemas
3/4/5 with strict transition validation. An older U9 reader must reject them
explicitly, not read missing history as an empty session. Versioned execution
transition checks remain strict; retain transition revision 4 only with equivalence proof. No command/event
schema version or event variant changes in U9 or U10.

1. Acquire U9 import writer/reader/transfer pins and a current authorized LCM view;
   recheck claim ownership before any read or mutation. Select the legacy exact
   checkpoint (nonterminal) or existing validated terminal/ordinary boundary,
   respecting existing redaction/overlay semantics. v3 protected state is required
   for active exact import; an ordinary redacted snapshot is not sufficient.
2. Validate all available immutable source entries against selected exact Message
   bytes, not merely their roles or current summaries. Match timeline, sequence,
   classification, store/binding/classifier/guard identities and signed reasoning.
   Compute U9 SHA-256 side metadata without altering existing Fingerprints.
   A redacted existing timeline mismatching exact checkpoint content cannot become
   canonical; keep U9 or explicitly Fork to a new authorized exact timeline.
3. Import missing active suffix Message bodies and exact U9 metadata in a stable,
   idempotent commit_history import transaction. Existing matching timeline bodies
   are referenced without a second write. v3 CallingModel keeps its exact final
   request recipe, pending approval/interaction keeps actions/answers, and
   ModelResponseReady/ToolOutcomeReady preserve committed outcomes. Executing and
   cache-started states retain no-replay handling; migration performs no external
   work. Import identity floors, deadline, watermarks, pending summary and child
   state unchanged.
4. Derive completed_len only from a validated prior terminal/source boundary.
   If a v3 mid-turn pair lacks enough terminal evidence, use a conservative
   zero completed cutoff until exact continuation commits, with an explicit
   migration status; preserve all exact durable history and do not infer that
   a physical tail was terminal. Old ordinary display may remain available under
   its own old policy until a verified projection is published. This affects
   visibility, not execution history or accounting.
5. Before canonical publication, old v3/v4 remains authoritative. After atomic
   version-5 publication, it is authoritative even if the ordinary projection or
   migration marker trails. Recovery discovers the head and repairs the projection
   under its policy. Pin backups/U9 object roots until import completes and
   retention permits release. Then collect the redundant U9 exact history bodies
   only after all checkpoint/request refs have been converted and no old reader,
   retention or source/DAG root needs them. Never delete a v3/v4 backup merely
   because a migration marker exists.
6. Store implementations not adopting the capability keep U9/non-LCM/legacy mode;
   do not auto-enable canonical mode upon seeing a timeline ID. No supported old
   binary may execute version-5 state. Downgrade requires a verified idle export
   or stopped-writer backup restore, not silent fallback to a pre-import checkpoint.

## Alternatives, Scope and Owner Decisions

A blanket LcmStore-as-SessionJournal misses protected atomicity and redaction;
a two-store 'append then checkpoint' protocol leaves ambiguous ownership of an
active suffix. Recommend the explicit same-domain transaction capability. This
is narrower than moving all metadata into the LCM log or forcing every host to
configure LCM. A cross-backend distributed commit protocol is outside this
change; those hosts keep U9 until they can implement the required transaction.

- Approve separate U10 after U9 conformance. Recommendation: yes; the source
  switch should be audited independently from delta/checkpoint encoding.
- Approve canonical opt-in requiring exact same-domain transactional storage.
  Recommendation: yes; otherwise maintain U9 duplication with truthful cost.
- Approve version 5 and v3/v4 readers for the first U10 release. Recommendation:
  yes; reject unknown source representations, never silently treat them as empty.
- Approve conservative completed cutoff for v3 mid-turn data lacking terminal
  evidence. Recommendation: preserve exact execution and retain old ordinary
  display until its projection can be proven; do not fabricate terminal history.
- Choose retired timeline/DAG retention policy. Recommendation: host-explicit,
  no automatic GC default, and retain source expansion closure until released.
