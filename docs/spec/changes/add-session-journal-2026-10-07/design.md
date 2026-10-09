---
created_at: 2026-10-07T00:00:00Z
updated_at: 2026-10-09T00:00:00Z
---

# Design: Delta persistence without changing storage authority

## Evidence on Main

All source references below are from HEAD/main
`c2e464699a455af251c29bd228fd16f47a4ecba9`. Read-only inspection; no build or
runtime measurement. The audit was read in full from Forge commit `ef060ba2`;
roadmap §3A and row 3.7 were read from `/Volumes/Data/tmp/refactor/plan.md`.
CodeGraph tools are unavailable in this session and this worktree has no
`.codegraph/`; source reads are the evidence, without initializing the index.

| Current fact | File:line |
| --- | --- |
| Ordinary store has only load/save of a materialized snapshot; snapshot contains Vec history, usage, diagnostics and extensions. | `crates/agent-runtime-core/src/store.rs:209`, `:243`, `:245`, `:248` |
| Exact checkpoint store has load_latest/save, atomically saved and idempotent by session/turn/revision/fingerprint; backwards revisions are rejected. | `crates/agent-runtime-core/src/checkpoint/store.rs:3`, `:10`, `:12`, `:19` |
| Checkpoint schema is 3; execution transition revision is 4. TurnCheckpoint embeds a SessionSnapshot and CallingModel holds the full request. | `crates/agent-runtime-core/src/checkpoint/mod.rs:35`, `:43`, `:382`, `:558` |
| A changed transition captures a full history/usage/extension snapshot and saves a checkpoint; an unchanged revision returns without save. | `crates/agent-runtime/src/agent/driver/turn.rs:79`, `:90`, `:107`, `:362`, `:407`, `:410` |
| CallingModel freezes request.clone(); terminal ordinary save is separate from the checkpoint. | `crates/agent-runtime/src/agent/driver/turn.rs:1318`, `:492`, `:504` |
| U3 is implemented: K defaults to 32, trimming after successful plan; new protected snapshots contain no manifests, and planned-step boundary evidence is retained. | `crates/agent-runtime/src/runtime/manifests.rs:16`, `:18`, `:26`, `:71`; `crates/agent-runtime/src/agent/driver/provider.rs:698`, `:710`; `crates/agent-runtime/src/agent/driver/turn.rs:92`, `:107` |
| U11 shares private Arc<[Message]> captures; public Vec snapshots still clone. Accounting validates authorization and extends cached totals from the missing suffix, not the whole range on a warm call. Its chain is a FingerprintHasher, not a durable SHA-256 journal. | `crates/agent-runtime/src/runtime/history.rs:25`, `:43`, `:59`; `crates/agent-runtime/src/runtime/state.rs:121`; `crates/agent-runtime/src/harness/lcm/accounting.rs:92`, `:175`, `:200`; `crates/agent-runtime/src/runtime/session/lifecycle.rs:176` |
| U1 preserves failure class and retry/reset evidence; U2 diagnostic and normalization/cached-validator seams exist. | `crates/agent-runtime-core/src/provider.rs:1587`, `:1616`; `crates/agent-runtime/src/agent/driver/provider.rs:680`; `crates/agent-runtime/src/tool/executor.rs:853`; `crates/agent-runtime/src/tool/registry.rs:29`, `:46`, `:157` |
| U6 working-set and summary ratios exist; strict identities and tolerant tunable rebuild are separate. Provider summary model is exported. | `crates/agent-runtime/src/runtime/builder.rs:57`; `crates/agent-runtime/src/harness/lcm.rs:1154`, `:1163`, `:1231`, `:1517`, `:1562`, `:1572`; `crates/agent-runtime-lcm/src/summarize.rs:360`, `:366`; `crates/agent-runtime/src/harness/mod.rs:15` |
| U7 claim and frontier-safe reconcile exist; U8 constructors/schema 2, fork seeds, pending intent and supersession already exist. | `crates/agent-runtime-lcm/src/store.rs:447`; `crates/agent-runtime/src/harness/lcm.rs:2124`; `crates/agent-runtime/src/runtime/command.rs:15`, `:149`, `:158`, `:167`; `crates/agent-runtime/src/runtime/fork.rs:9`, `:48`; `crates/agent-runtime/src/runtime/engine.rs:765`, `:1217`, `:1289` |
| Hard admission saves Sensitive intent/successor before provider admission without checkpoints; terminal source save precedes LCM commit. These barriers must survive conversion. | `crates/agent-runtime/src/agent/driver/turn.rs:675`, `:685`, `:441`; `crates/agent-runtime/src/harness/lcm.rs:1750`, `:1795` |
| Redacted ordinary terminal history remains canonical; protected extension overlay requires identity/history-role/usage/manifest/revision checks. | `crates/agent-runtime/src/runtime/engine.rs:50`, `:70`, `:81`, `:112`, `:127`, `:174` |
| Usage is an unbounded append-only Vec; each usage record may contain its full cache identity. | `crates/agent-runtime-core/src/usage.rs:151`, `:178`, `:189`; `crates/agent-runtime/src/agent/driver/provider.rs:1163`, `:1171` |
| Cache identity has bounded prefix/history lists (4096 each) and tools (512); manifest segment lists are still variable-size. | `crates/agent-runtime-core/src/provider/identity.rs:84`, `:98`, `:579`; `crates/agent-runtime-core/src/manifest.rs:1043`, `:1058` |
| Secrets redact Debug/Display; extension default sensitivity is Sensitive; LCM errors omit backend/source text and authority is nonserializable and revocable. | `crates/agent-runtime-core/src/store.rs:36`, `:43`, `:140`, `:161`; `crates/agent-runtime-lcm/src/store.rs:93`, `:113`, `:170`, `:395` |
| Existing fixtures cover U3 and U11; CI names the three targets. They do not yet provide journal crash/GC conformance. Consumer crates are banned. | `crates/agent-runtime-testkit/tests/consumer_smith.rs:180`, `:185`; `crates/agent-runtime-testkit/tests/consumer_open_forge.rs:216`, `:221`; `crates/agent-runtime-testkit/tests/consumer_nyx.rs:153`, `:158`; `.github/workflows/ci.yml:129`; `deny.toml:32` |

## Bytes Written Today and Target

Count logical uncompressed serialization submitted to stores, before host
compression, encryption, database WAL/index/page amplification, deduplication or
chunking. No concrete host byte multiplier can be inferred from library code.
Let H(N) be serialized history array bytes for N messages; U(T) cumulative usage
ledger bytes after T accounted units; E extension bytes; M(K,N) retained manifest
bytes, K=32 by default; F(s,P) checkpoint state's exact payload, including a
frozen request of P bytes in CallingModel. Fixed envelopes are c_s/c_o.
For one boundary with actual writes:

`B = I_checkpoint * (H(N) + U(T) + E + F(s,P) + c_s)
   + I_ordinary * (H(N) + U(T) + E + M(K,N) + c_o)
   + B_lcm_append_or_summary`.

Normally a protected machine transition has I_checkpoint=1, I_ordinary=0;
an unchanged checkpoint revision has both zero. Terminal publication and
ordinary-only hard admission have additional ordinary writes at the cited
barriers. Do not multiply every transition by two snapshots. CallingModel's
P contains the final planner projection: without LCM it can include another
history-sized message array; with compaction it can be much smaller than H.
With both saves and P≈H, the history contribution can approach 3H. With a
normal non-CallingModel checkpoint it is H, and at terminal with both saves 2H.
A five-state tool-round estimate is a sum over those boundaries, not five
identical writes; result-ready/tool states have different exact payloads.

For a concrete derived lower bound, a compact JSON user message containing
1024 ASCII `x` characters is 1077 bytes, from the serde tags/fields at
`crates/agent-runtime-core/src/content.rs:23`, `:37`, `:114`.
Thus H(N)=1078N+1: 107,801 bytes for N=100; 1,078,001 for N=1000;
10,780,001 for N=10,000. A checkpoint alone rewrites at least this history
array; CallingModel with the same array adds another 1,078,001 at N=1000.
These are mathematical JSON payload sizes, not an executed Rust/Forge benchmark.

After U3, manifests contribute O(K × planned-fragment count) and bounded cache
identity lists, not an ever-growing calls-times-history manifest list. After
U11 the durable formula is unchanged. Usage still contributes O(T) records and,
where cache identities are present, O(sum over attempts of min(stable-history,
4096)) metadata plus other bounded fields; changing extensions may also grow.
Consequently 'U3 makes the entire snapshot O(history)' is too strong. U9 must
reference usage and metadata as well as conversation content.

Native target after one O(H+U+E+M+P) bootstrap:
`B_native = bytes(new unique bodies and changed state/diagnostic leaves)
          + O(d log R) reference-index bytes + O(1) head/checkpoint envelope`,
where d is changed leaves and R is retained indexed records. Unchanged bodies,
usage records and manifest leaves are never resubmitted, even at CallingModel.
A one-message append writes that message once per policy domain (1077 payload
bytes in the example), plus reference paths and fixed envelopes, independently
of H. Changes to an opaque extension value cost its replacement bytes; newly
planned O(N) diagnostics may still introduce O(N) *new metadata*. This proposal
does not promise constant bytes for arbitrarily changing policy/diagnostics or
eliminate those public shapes. 'O(delta)' means delta of all persisted content
and metadata with logarithmic index overhead, not only new user text. Require
counted writes to prove zero resubmission of an unchanged history prefix,
ledger prefix and request bodies for N=100/1000/10000. Legacy adapter retains B.
U9 still allows B_lcm to write a second content copy; U10 removes that copy.

## Exact Trait Diff and Mode Selection

Illustrative Rust signatures below fix names, arguments, return types and
required/default status for implementation review. New neutral DTOs live in
core; JournalLease is an opaque process handle and not serializable authority.
Bytes are a neutral owned byte vector, not a backend SDK type.

```rust
// Add to each existing trait; load/save and load_latest/save are unchanged.
fn journal(&self) -> Option<Arc<dyn SessionJournal>> { None }
// Additive convenience for an explicit capability request; default discovery
// remains exactly Option/None as specified, rather than changing its signature.
fn require_journal(&self) -> Result<Arc<dyn SessionJournal>, RuntimeError> {
    self.journal().ok_or_else(|| RuntimeError::unsupported_capability(
        UnsupportedCapability::SessionJournal))
}

#[async_trait]
pub trait SessionJournal: Send + Sync + Debug {
    fn storage_id(&self) -> JournalStorageId;
    async fn open(&self, session: &SessionId, request: JournalOpen)
        -> Result<JournalLease, RuntimeError>;
    async fn read(&self, lease: &JournalLease, object: &JournalObjectId)
        -> Result<Vec<u8>, RuntimeError>;
    async fn commit(&self, lease: &JournalLease, batch: JournalCommit)
        -> Result<JournalHead, RuntimeError>;
    async fn renew(&self, lease: &JournalLease)
        -> Result<JournalLease, RuntimeError>;
    async fn close(&self, lease: JournalLease) -> Result<(), RuntimeError>;
    async fn retire(&self, session: &SessionId, expected: JournalRevision)
        -> Result<JournalRetirement, RuntimeError>;
    async fn collect(&self, request: JournalGcRequest)
        -> Result<JournalGcReport, RuntimeError>;
}
```

All SessionJournal methods are required, with no unsafe default or blanket
implementation. An adapter may explicitly return typed unsupported for collect
and retain everything; native write/recovery conformance is still required.
Existing SessionStore/CheckpointStore implementations compile unchanged because
only default methods are added. No method is removed. Materialized
SessionSnapshot.history/manifests/usage/extensions, TurnCheckpoint.snapshot and
TurnState remain source-compatible. Storage_id identifies a protection domain,
not a credential, grant, tenant authorization or permission to read objects.

JournalOpen selects latest or a specific retained root and carries an expected
revision, operation/pin identity, and read-only or exclusive-writer intent.
Opening atomically pins the selected root before returning it. Writer open
also acquires a monotonically fenced execution lease; absent-head writer open
reserves create/import so two processes cannot bootstrap the same session.
Read-only open never starts a turn. read permits only objects reachable from the
lease's pinned roots or its own validated staging set; knowing another session's
digest cannot retrieve it. Renew preserves the pin and renews the
writer fence; stale/expired/revoked leases fail before reads or commits. Hosts
own lease durations/renewal policy; no wall-clock default silently permits work.

JournalCommit carries expected head revision, lease fence, stable operation ID,
SHA-256 batch digest, new immutable objects, transition delta, resulting state
roots and optional referenced checkpoint. It atomically installs objects and
batch, pins all resulting references, and publishes head/checkpoint. Same
operation and same digest returns the original result even after head advance;
same operation with different digest conflicts. Retain the idempotency index
for the session lifetime and every retained checkpoint/operation that needs it;
GC must not erase replay fences while their execution roots are live. Backwards checkpoint revisions
and mismatched execution fingerprints remain rejected. An exact journal backend
cannot publish a head with a missing object. Ordinary commit transforms raw
values under host policy *before* computing its own object digests and roots.

A JournalCommit is a typed semantic delta plus candidate object drafts, not an
instruction to install client-chosen raw digests into ordinary storage. Its
protected role verifies candidate IDs exactly; its ordinary role applies the
host policy to changed record drafts, remaps object/reference IDs, recomputes
stored roots and returns that projected JournalHead. The runtime tracks this
returned root independently of the exact root. Ordinary commits cannot contain
an exact checkpoint root or dereference another policy domain. The idempotency
input digest stays private to the configured store policy; it is never exposed
as projected content evidence. JournalOpen/read likewise return only the role's
stored view. Golden fixtures must cover redaction changing object sizes/IDs,
omitted Sensitive state and identical retry after policy projection; a failed
or unavailable projection must reject the ordinary write, not store raw drafts.

Native ordinary-only sessions can use SessionStore.journal without a checkpoint
journal and retain today's ordinary-only guarantees. Native protected-only
recovery can use CheckpointStore.journal without ordinary state. With both
stores configured, native mode requires both journal capabilities; mixed
pairs stay wholly on the legacy path before any native head exists. Once a
native head exists, losing a capability is an explicit incompatible-store
error, never a fallback to a stale v3 snapshot. Ephemeral bypasses all discovery,
open, read and write calls. SnapshotJournal is a materializing compatibility
adapter, not an implementation of the native trait; it keeps the two stores
separate, requires the host's existing single-writer discipline, and offers
neither cross-process CAS nor GC. No new event, start command, resolver method
or LcmReader/LcmWriter method is introduced by U9.

## Objects, Heads and Reference Checkpoints

### Group A encoding and reader refinements

Approved group A defines the DTOs in `agent-runtime-core::journal`. Object
envelopes contain encoding, kind, schema_version and value. Keys sort by UTF-8
bytes; arrays preserve order; finite JSON numbers preserve their serde type
and representation including `-0.0`. `serde_json/float_roundtrip` preserves
binary float values on reads. Signature strings are never normalized.
Frozen JSON/digest fixtures live in `agent-runtime-core/tests/fixtures/journal`.

Existing operation/preparation fingerprints also depend on the insertion order
of opaque JSON Value maps when a host unifies `serde_json/preserve_order`.
Canonical envelopes therefore optionally bind `ordered_objects` path/key-list
metadata for execution-relevant Value fields (state values, message arguments,
extension values, schema values and vendor settings). Payload keys still sort
lexically; readers restore that explicit ordering before validating existing
fingerprints. A host unable to represent a recorded order fails closed rather
than changing operation identity. No existing fingerprint algorithm or
dependency feature is changed to force preserve_order on legacy consumers.
Default-feature and preserve_order reader equivalence are both tested; the
ordered-extension bytes/domain/type/version fixture freezes this refinement.

The object SHA-256 preimage is `agent-runtime/session-journal/object\0`, then
u64-big-endian encoding-length/encoding, u64-big-endian kind-length/kind,
u32-big-endian schema, u64-big-endian envelope-length/envelope bytes. History
seed/step and private commit-input digests have distinct domains, documented
in rustdoc. Object IDs and ordering digests are separate redacted-debug types.
Storage-domain isolation remains backend authorization, not part of a public
global deduplication namespace or permission conferred by a digest.

Sequence/map descriptors bind root, total count and height. Leaves and branches
have at most 64 entries; child height decreases by one. Map child separators
bind exact first keys and exclusive upper bounds. Readers reject bad counts,
ordering, heights, object types, versions and noncanonical bytes. They read
only the lease's selected head, with explicit host object/byte/entry limits.
Backend open/read still own durable frame-length/checksum and pin/fence checks;
readers validate current batch/sequence/predecessor metadata, not dereference
collected predecessor batches or scan orphan frames.

Referenced states preserve the existing TurnState tag and reference every field;
top-level field lists use bounded sequences. CallingModel's request field must
use a Request object: persistent final message/tool sequences plus an exact
ProviderRequest settings object with empty message/tool lists. Large opaque
extension/outcome/settings leaves retain their replacement-byte cost. Typed
schema-4 object reconstruction rejects dropped fields/default substitutions.
Materialization validates the existing operation/transition/boundary checks.

Changed materialized transitions from schema 3 emit schema 4, while exact
same-state reapplication preserves the original revision/tag for idempotency.
The standalone JSON reader normalizes a validated materialized v3 checkpoint
to schema 4 without saving it. SnapshotJournal loads preserve the source tag
and never write back a legacy revision in place. Revision 4 equivalence is
checked against a frozen v3 enum, transition table and helper implementation
from base `6ec58fa`, including every persisted state tag and splice cases.
This addition of `require_journal` and these concrete encoding/DTO choices
refine the illustrative contract; no existing public signature is replaced.

Versioned, domain-separated SHA-256 hashes bind canonical encoded object bytes
and type/schema. Define encoding `journal-json-1`: compact UTF-8 JSON with
lexically ordered object keys, array order preserved, explicit tagged types,
round-trip-preserving numeric representation including negative zero, and
opaque signed reasoning retained byte-for-byte. Published golden fixtures
freeze encoding before hosts implement it. Registry Fingerprint and existing
operation fingerprints remain unchanged and are not content addresses.
SHA-256 gives integrity, not authorization or confidentiality. Object IDs and
root hashes do not enter default events/errors/logs; cross-tenant equality and
secret guessing must not be exposed by a global public dedup index.

JournalHead version 4 contains session, head revision, writer epoch, history
count/chain and persistent history root, usage count/root, extension map root,
identity counters, retained diagnostic-window root/planned-step count,
checkpoint root, retirement/supersession state, and timestamps. Large lists are
persistent sequence/map trees with bounded nodes (recommend fanout 64), not
flat ref arrays rewritten per transition. Hash chains are durable SHA-256;
U11's private Fingerprint chain remains an independent acceleration sidecar.

ReferencedTurnCheckpoint version 4 retains all current metadata, transition
revision, active_history_start, internal_input reference, visible-output flag,
deadline and scoped watermark. It holds protected state/snapshot roots instead
of inline history, ledger, extension values and requests. Each body is stored
once in the exact protection domain; raw tool outcomes/prepared invocations are
protected objects. Requests retain the *final* planner-produced message order,
tool schemas, sampling/reasoning/output settings, cache identity/boundary and
all adapter continuation data. Use persistent ordered refs for arbitrary
projected messages; a history-prefix length alone cannot represent summaries,
contributed instructions or tool-result transforms. Missing/invalid refs fail
closed before provider/tool I/O. Materialization reconstructs current public
owned types and validates the existing operation fingerprint/transition table.
It never calls a current planner to guess the prior request.

Append-only describes immutable batches/objects; mutable head pointers publish
roots. Batch predecessor digests attest ordering, but are not automatic
reachability edges to every superseded checkpoint. State roots retain all live
history/usage; old transition roots live only while head, retention or pins
require them. Host audit retention may keep batches longer. This distinction
allows collection without claiming arbitrary historical replay after GC.

## Protection Domains and Overlay

Ordinary SessionStore journal policy may redact credentials/content and omit
Sensitive extensions as today. CheckpointStore journal policy is exact and
protected for every execution-required object, including extensions, requests,
raw outcomes and signatures; manifests remain absent there after U3. Identical
exact/ordinary values may be physically shared only inside a domain whose
confidentiality and access rules satisfy both; an ordinary handle must never
retrieve an exact sensitive object by knowing its digest.

Pair matching uses a versioned non-content boundary key (session, turn,
checkpoint sequence, planned-step counter), explicit predecessor/successor
proofs for idle accounting/hard admission, and materialized existing identity,
history count/role, usage and extension-revision checks. Ordinary and protected
SHA-256 roots may differ and are never compared as proof of redaction
correctness. Preserve the existing terminal rule: ordinary completed history
remains host-redacted canonical history, exact protected state overlays only
compatible namespaces; nonterminal execution uses protected exact state.
No redacted read is upgraded to exact merely because a backend also has it.

The protected-state split, Secret handling, Sensitive default, credential-lease
exclusion, hash-only manifests, metadata-only error Debug, host-supplied policy,
planner-only provider construction, LcmViewAuthority checks, and fail-closed
approval/workspace/cache/model limits remain. No Forge Room/task/blob schema,
Smith capsule policy, Nyx channel, prompts or consumer crates enter production.

## Crash Cuts, Repair and Collection

| Crash/failure cut | Reader result and repair |
| --- | --- |
| Before immutable objects finish | Prior published head/checkpoint only; incomplete unreferenced frames are ignored. Retry same batch; no work admitted from them. |
| Objects/batch durable, head not published | Prior head; objects are orphans protected by transaction/staging pin until abort/retry. Never scan for a 'newest' orphan checkpoint. |
| Atomic protected head/checkpoint commit lands, response lost | New exact boundary; duplicate commit returns original result. A raw model/tool result-ready state is reused without I/O. Indeterminate executing/cache-started states retain existing no-replay rules. |
| Protected commit before ordinary projection save | Nonterminal reader uses exact checkpoint; terminal reader applies current boundary rules. Repair a lagging projection through its host redaction policy only with a proven boundary. No stale ordinary overlay. |
| Ordinary terminal save before protected Terminal | Protected PublishingTerminal drives scoped terminal republication; hooks/results are not rerun. Preserve external event journal watermarks and identity floors. |
| Ordinary-only hard-admission intent, DAG CAS, then successor save | The prior durable intent validates only its exact successor using existing U6/hard-admission proof. Repair or conflict before provider admission; never repeat a committed summary call. |
| Fork intent, ownership transfer, child seed, parent supersession | Retain U8's pending-fork fence, child-before-parent ordering, same-request repair and abort conditions. Pin source and child before binding transfer; only release transfer pins after roots protect them. |
| GC marks, then fork/resume starts | Pin/root acquisition and GC epoch/deletion are serialized. New pin invalidates the mark or blocks deletion; collector rechecks reachability transactionally, so either pin wins or open reports missing before dereference. |

Head publication and pin/index state must be atomic, durable and linearizable.
For file backends this means a checksummed length/type/version-framed append,
flush/fsync objects, an atomic durable head replacement with directory fsync,
and a cross-process lock/fence, not merely a Tokio mutex. Databases may use one
transaction plus durable content staging and a publish barrier. The abstract
commit has one visibility point despite multiple physical writes. Journal reads
verify frame lengths, version, type hashes, sequence, predecessor digest,
counts/root and object closure. EOF inside an unpublished trailing frame is a
torn tail; under the writer lock truncate/quarantine only that uncommitted tail
and retry the intended operation. A checksum failure or missing object below a
published root is committed corruption: return StateConflict and require a
verified host backup/replica repair; never roll back to an earlier state that
could repeat side effects. An ambiguous durable write is resolved by operation
identity lookup, never by a fresh request ID.

Retire removes a session root only with expected revision, explicit host
retention authorization, no live execution lease, and no pending fork/resume
transfer that relies on it. Pins protect the entire object closure, including
historical checkpoints selected by retained root. Collection traces all live
session/checkpoint/retention roots, active reader/writer pins, migration roots,
pending forks, child references and staged transactions across the protection
domain. Marks carry a root/pin epoch; deletion transaction rechecks it. Lease
expiry never removes committed roots. A crashed transfer's durable intent pins
remain until deterministic repair/abort. Summary/Empty forks may release unused
parent content after retirement; FromIndex shares objects and retains them via
the child root. Logical supersession alone is not deletion permission. No default
GC schedule or retention TTL is introduced. A backend unable to prove the fence
returns unsupported and leaks safely rather than deleting live content.

## v3 Upgrade, Including Mid-turn

'v3 snapshot' means the v3-era storage pair: SessionSnapshot itself has no
schema_version field on main; protected TurnCheckpoint has schema_version=3.
Do not invent a legacy snapshot tag as a precondition for reading it.

1. Ship dual readers first; supported v3 transition revision 4 and strict identity,
   binding, namespace and operation checks remain. Invalid/missing evidence is
   not a migration opportunity. No raw v3 record is pruned before U3 boundary
   bootstrapping. Keep a recovery backup/pin until native publication completes.
2. Without native capabilities, load the old pair and write materialized current
   schema-4 checkpoints through existing methods. Source implementations keep
   compiling, but custom schema guards need coordinated updates. No delta
   performance claim applies. Existing legacy records stay readable for the
   entire first U9 release; no automatic v3 write-back/downgrade promise.
3. With native capabilities, acquire import writer fence, load/validate the
   legacy pair once, and import exact protected and separately redacted ordinary
   objects under stable import IDs. Commit the exact head first. Crash before
   that commit leaves v3 authoritative; afterwards the native head is authoritative,
   even if ordinary import or an import-complete marker is missing. Native
   discovery detects the committed head, not just a legacy migration marker.
4. A mid-turn v3 CallingModel import freezes its full final request once and
   references it; ModelResponseReady and ToolOutcomeReady import committed
   results; AwaitingApproval/Interaction preserve exact prepared actions/answers;
   ExecutingTools/local-executing/cache-started retain the current indeterminate
   no-replay handling. Preserve deadlines, identity floors, visible-output flags,
   child cursors, scoped event watermarks and protected pending LCM summaries.
   Convert representation only; perform no provider/model/tool call during import.
5. Close import pins only after exact and ordinary publication/repair, and retain
   original v3 objects according to explicit host backup retention. No old binary
   may execute a migrated head. Downgrade requires an explicit idle export with
   independently verified semantics, or restoring a backup under a stopped
   writer; never silently resurrect a stale v3 mid-turn checkpoint.

Keep public snapshot/history/recent_manifests projections and event variants.
Version-4 readers run for the entire first release; future legacy-reader removal
is a separate proposal with all consumer gates. U10 has its own format change
and continues to read v3 during its first adoption release.

## Existing Proposal Delta Ordering

Main has implemented U3, U6, U7/U8, U11 and durable hard admission while their
change folders remain unarchived. Consume their implemented contracts as
prerequisites, not unfinished audit tasks. Archive their approved deltas into
truth before archiving U9; in particular U9's MODIFIED `Durable ordinary-store
hard admission` block is based on the implemented requirement in
`docs/spec/changes/add-lcm-durable-hard-admission-2026-10-05/specs/context-management/spec.md:3`.
It changes physical representation only: journal roots must materialize the
same full canonical state, intent/successor evidence and charged usage. Existing
legacy paths keep full-snapshot saves. The U3 bounded-manifest requirements stay
in force; U9 does not restore lifetime manifest retention from stale truth prose.
No prerequisite or truth file is edited/archived by this document task.

## Merge Groups and Decisions Before Implementation

Each tasks.md group can merge separately on main: Group 1 is reader/contracts
only; Group 2 activates native persistence and includes migration/crash proof;
Group 3 adds collector conformance without an enabled-by-default collector;
Group 4 supplies actual consumer adoption/release evidence. No U10 dependency
is required and no group may weaken the protected barriers.

- Approve two releases/change reviews: U9 before U10. Recommendation: yes; first
  prove journal recovery with today's LCM duplication, then remove that duplication.
- Approve native opt-in with legacy materialization retained for this release.
  Recommendation: yes; a blanket save-based adapter cannot truthfully offer CAS/GC.
- Approve canonical `journal-json-1`, SHA-256 and fanout 64. Recommendation: freeze
  golden encoding fixtures before backend coding; retain current runtime hashes.
- Choose retention of retired sessions/checkpoints. Recommendation: explicit
  host policy, no library default expiry or background collector.
- Decide release granularity for the v3 reader sunset. Recommendation: at least
  one full released U9 version plus the first U10 adoption release; removal only
  after Forge/Smith owners report stored mid-turn migration coverage.
