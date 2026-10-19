---
created_at: 2026-10-07T00:00:00Z
updated_at: 2026-10-19T00:00:00Z
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

### Task 2.7 implementation refinement (2026-10-18)

The native writer now treats two Runtime-owned, redaction-safe records as
structured journal data rather than opaque replacement leaves: retained
`TurnManifest` values and `runtime.core.previous_cache`. Their JSON arrays use
the existing bounded sequence objects with single-reference leaves and a
fixed-height binary spine. JSON object entries preserve their order explicitly.
An append therefore submits only changed scalar/entry objects and one bounded
root path; materialization still reconstructs the identical public
`TurnManifest` and `VersionedSessionState` values. Readers still accept the
schema-4 inline `Manifest`/`Extension` object kinds that group A released, but
the native writer does not migrate them: no released runtime wrote a native
head, so a protected head whose roots the writer cannot re-derive exactly fails
closed as an incompatible store, and an ordinary head in another
representation is followed by one complete structured submission.

This does **not** change the opaque-extension contract: every other namespace
remains a whole-value replacement and keeps its full replacement-byte cost.
The cache namespace is Runtime-owned, declares `RedactionSafe`, and retains the
same revision, sensitivity, value and cache-identity semantics. Ordinary
projection must preserve this Runtime-owned value and manifests exactly; it may
still redact/omit host-owned extension values under the existing policy.

`JournalTransitionDelta` carries references for these structured records.
Ordinary commits submit their referenced drafts once and carry other changed
records only in the semantic delta, eliminating the previous draft-plus-delta
duplication. Protected commits retain exact candidate verification. The
reference backend resolves draft references only from the current pinned
closure or the current commit, never from another session's stored objects.

The executed task-2.7 table is in `tasks.md` and `docs/development.md`; raw rows
are in `/Volumes/Data/tmp/refactor/u9b-3/task-2.7-measurement-1.log`. N=10,000
cannot exercise a turn: the cache identity's 4,096-entry stable-history limit
rejects both legacy and native planning before provider I/O. N=4,000 is the
largest comparable successful case.

### Task 2.7 representation refinement (2026-10-18)

The unreleased native-writer shape is replaced directly; there is no reader or
migration path for the PR #39 representation. Schema-4 readers retain the
released v0.5.0 acceptance of inline `Manifest` and `Extension` object kinds.

Persistent JSON uses a 1,024-byte canonical payload threshold. Compact values
are stored as typed `JsonInline` objects (or as explicitly tagged inline object
fields), never inferred from user JSON shape. Arrays of at most 64 items keep
their typed root inline in the parent `JsonValue`; wider arrays use two compact
values per bottom node, minimum-height binary paths, an inline array root, and
dedicated compact `JsonBranch` objects. Branch child IDs use canonical Z85
internally but expand to the same 32-byte/64-hex `JournalObjectId`; hashes and
authorization remain unchanged. Readers still reject logical heights above 32.

Each live writer domain caches the preceding history, usage, manifest and
extension object IDs plus the persistent-JSON path encoding. Equal records
reuse their roots; changed structured records rebuild only changed paths.
`InMemorySessionJournal` mirrors that cache after ordinary projection, stores
immutable objects and pin reachability in append-only copy-on-write layers, and
short-circuits already validated pinned subtrees during commit reachability.
All cached IDs are derived by the same canonical encoders: caching changes work,
not committed roots. A cache hit requires exact identity
(`journal_json_identical`: number bits, so `0.0` and `-0.0` differ, and key
order), not `serde_json` equality, and a cached array node is reused only when
it was built from exactly the same children; any other miss re-encodes cold.
The equivalence harness checks every published head against a cold encoding of
its materialized state. Ordinary referenced drafts are admitted only when every
supplied object is reachable from the current structured delta and host policy
preserves the Runtime-owned values exactly; the closure that selects them stops
at already pinned objects. For a protected commit the cached snapshot is only a
fast path: a delta it cannot replay or roots it does not reproduce fall back to
full reader validation (the protected delta stays advisory), and a fully
validated head whose canonical encoding reproduces its roots re-seeds the fast
path, so a writer change costs one full validation. Copy-on-write
layers are merged while the newest is at least as large as the one below, so a
lookup scans at most log2(total)+1 layers.

The detailed byte/time evidence is in `tasks.md` and `docs/development.md`.
Bootstrap clears the ≤4× legacy-snapshot target and later serialized turns are
smaller than the prior shape. Complete debug phase time is much lower but not
flat in N, so that CPU target is explicitly NOT MET. The remaining per-commit
O(N) is mostly the reference backends resolving structured references to full
values (the ordinary host policy and the protected checkpoint validation both
take a complete `SessionSnapshot`), validation of a `calling_model` checkpoint's
full provider request, and the runtime's public `SessionSnapshot` capture
(legacy also captures and serializes the full snapshot per save). Removing it
needs a delta-aware policy/validation contract or a decoded-value memo, not an
encoding change.

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

Group A leaves materialized persistence byte for byte as v0.2.1 wrote it:
`CHECKPOINT_SCHEMA_VERSION` stays 3, `TurnCheckpoint::validate` accepts exactly
3, transitions keep their predecessor's tag and SessionSnapshot stays
unversioned. Schema 4 (`JOURNAL_SCHEMA_VERSION`) tags journal heads, objects
and ReferencedTurnCheckpoint envelopes only, and nothing in the runtime writes
those yet. A reference checkpoint materializes to a TurnCheckpoint tagged 3,
equal to the materialized checkpoint it was built from, so it chains with a
predecessor from a legacy store. The reason is rollback: a 3-to-4 tag on an
otherwise identical inline checkpoint would make a host's in-flight turns
unreadable by its previous binary and buy nothing before the native writer
exists. A host that adopts nothing can move between v0.2.1 and this release
in both directions. Whether materialized writes ever need a new tag is decided
with the native writer in group B (task 2.8); if they do, this release's
readers must be extended first, one release ahead of the writer.
The standalone JSON reader returns a validated v3 checkpoint as stored.
SnapshotJournal loads and saves the same records as the stores it wraps and
never writes back on load. Revision 4 equivalence is
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
transfer that relies on it. Retirement evidence outlives collection: latest-root
opens with either intent return StateConflict rather than an absent native head
that could authorize legacy fallback. Existing pinned readers and available
explicitly retained historical roots remain readable. Pins protect the entire object closure, including
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

### Group C reference implementation and evidence (2026-10-18)

Collection is explicit host opt-in on the volatile reference backend:
`enable_collection` permits requests, and `authorize_retirement` binds host
retention authorization and host time to one current revision. Default
constructors retain their typed unsupported behavior. No required trait method,
serialized object/head/checkpoint field, schema version or default expiry changes.
The reference's retirement tombstone, replay fences, retained-head catalog,
root/pin epoch and durable pin/dependency indexes are backend transaction metadata.
File/database adapters must persist equivalent metadata and cross-process fences;
the volatile implementation makes no disk-durability claim.

`pin_transfer` persists a lease's closure, including an absent-head import/child
reservation that publication extends atomically. `retain_root` preserves a
selected historical checkpoint. Renew and process close/revocation cannot erase
either durable pin; only explicit epoch-fenced `release_pin` after deterministic
repair/abort or retention release can. Committed fork intents and child catalogs
also index dependency roots at the same publication point. A matching
supersession proof resolves a fork only with a published child seed. Ordinary
projection may omit Sensitive intent/catalog bodies, but their non-content pin
index survives until a semantic namespace update explicitly clears it. Historical
roots retain their own dependency index. Unknown transfer-index revisions return
typed unsupported and retain content rather than guessing their closure.

When ordinary/protected domains are separate, hosts register transfer pins in
each affected domain before publication; one store cannot infer another store's
external roots. The runtime pending-child fixture reserves protected source/child
pins at writer open, including absent-head reservations, because the real parent
catalog is an ordinary publication and the parent may have no exact checkpoint.

Every open/commit/renew/close, host revocation, durable pin change, staging change
and retirement advances the root/pin epoch. `JournalGcRequest` captures that
epoch; the reference checks it and recomputes all reachability while holding the
same transaction lock through bounded deletion. A stale mark conflicts even if
no object was deleted. Retained-head catalogs and batch predecessor digests are
not automatic retention roots. Replay-fence metadata is never collected while
its execution root is live (the reference keeps it after retirement as well).

Named protected/ordinary collection conformance assertions and adapter-specific
`JournalCollectionHost` fixture controls are exported from the testkit's journal
module. The eight-operation matrix has three explicit steps each (24 rows):

| Operation | Before mark | Between mark and deletion | After deletion |
| --- | --- | --- | --- |
| Read-only retained resume | Pin keeps full retired root readable | Pin invalidates mark; fresh sweep retains it | Open fails before dereference |
| Writer resume | Canonical append and pinned history readable | Open/commit invalidate mark | Live root opens and appends |
| Summary fork | Child seed survives parent retirement | Source/child intent pins invalidate mark | Source remains live until seed; unused parent objects reclaimed |
| Empty fork | Empty child survives parent retirement | Source/child intent pins invalidate mark | Empty child retained; parent-only objects reclaimed |
| FromIndex fork | Child suffix shares canonical message objects | Pins/child root invalidate mark | Shared suffix readable after parent retirement |
| Continue ownership transfer | All canonical summary sources move with child | Both endpoint pins invalidate mark | Child retains every committed source |
| Pending child | Source and child intent pins prevent retirement | Publication/expiry invalidate mark | Both closures readable after process loss |
| Expired process lease | Revocation removes no committed root | Revocation invalidates mark | Stale handle fails; fresh reader sees committed root |

`journal_gc_runtime` additionally runs real runtime Summary/Empty/FromIndex forks,
fresh writer resume and read-only fork views, Continue with actual committed LCM
leaf nodes, and a delegated child paused by an explicit notification at its
CallingModel publication before provider I/O. Each runs at all three GC steps on
normal two-worker Tokio runtimes. Every adapter matrix row includes aborted
staging and asserts real reclamation at its first successful sweep. It expands
the original summary sources after
parent retirement, and rebinds the completed child after collection/process lease
expiry without replaying its prior provider call. These tests have no sleeps or
timing-selected interleavings. Every successful pinned closure is read back;
the adapter matrix also checks every candidate object plus its head/batch/state.

The N=1000 long-session conformance case publishes eight exact CallingModel
checkpoints, unique projected requests/settings and 16 KB opaque extension
replacements, plus aborted orphan staging. It retains **570,121 bytes before**
collection, **404,529 bytes after** live collection (165,592 reclaimed), and
**0 bytes after** authorized retirement and pin release. Canonical history and
the latest exact request still materialize after the live sweep; obsolete
checkpoint roots no longer open. Bytes exclude physical amplification and
backend replay/tombstone indexes. Backend `compact` preserves even unreachable
objects; only logical `collect` deletes. LCM compaction preserves its canonical
sources and is a separate semantic operation.

Three broken adapters fail named cases: making a pinned batch unreadable,
ignoring the expected root/pin epoch, and clearing durable intent pins after a
failed repair. A transparent wrapper passes the complete supported suite; an
adapter returning typed unsupported retains its complete readable closure.
Failed repair is exercised at every pre-visibility commit cut, and separately
against a committed fork intent and retained dependency root. Existing test
expectations and all released encoding fixtures remain unchanged.

Review refinement (2026-10-18): scratch mutations of the reference showed the
suite could not fail a collector that ignored transfer pins on a superseded
root, stopped tracing a renewed lease, skipped any of ten object kinds (JSON
spine/inline nodes, legacy and structured manifests, usage records, tool
schemas, internal input; five were absent from every GC test and five were
caught only by the in-repo runtime harness), honored another session's
retirement grant, retired a fork parent on its supersession proof before the
child seed published, deleted an ordinary FromIndex child's shared suffix, or
opened a lease over a root that bounded deletion had only partly removed.
`assert_journal_gc_object_kind_closure_conformance` now publishes all 21 object
kinds under a live root and an explicit retention pin; the durable-pin,
retirement, committed-transfer and ordinary cases and
`assert_journal_gc_partial_collection_conformance` cover the rest. The 24 rows'
steps are host-side sequencing at the trait boundary: `collect` re-traces under
the same lock as deletion, so the reference has no internal mark/delete window,
and a two-phase collector stays safe only because every root/pin change advances
the epoch, which the epoch case checks operation by operation. The interleaving
matrix stages protected leaves, so ordinary-only adapters cannot run it.

### Retired latest-root follow-up (2026-10-19)

The reference originally checked its retirement tombstone only for writer
opens. Retirement removes `current`, so a read-only `Latest` open returned a
successful absent-head lease. The runtime's view helpers then used the v3
records left behind by import. They also loaded those records eagerly before
opening the journal, even if the open would fail.

The released `Result<JournalLease, RuntimeError>` already expresses refusal:
`InMemorySessionJournal::open` now returns `StateConflict` at `PreProvider` /
`Checkpoint`, with the message `session journal is retired`, for every retired
latest-root open. Writer refusal remains; existing pinned readers and available
explicitly retained historical roots remain readable. The backend tombstone
survives collection. No trait signature, error variant, object/head/checkpoint
format or schema changes. `SessionJournal::open` documents this requirement.
`load_snapshot_view` and `load_checkpoint_view` open first and load legacy only
after a successful absent-head result, propagating retirement from their own
domain or from the remaining counterpart during lost-capability discovery.

Nine `delegation::retirement_tests::retired_v3_*` tests create a real v3 pair,
import it through `SessionPersistence::open`, verify both native
materializations, authorize retirement and collect every immutable object,
while keeping the v3 pair unchanged. Resume, fork, snapshot/checkpoint views,
session-exists, child recovery, returned-interaction recovery, watermark refresh
and exact child rebind each require the retirement error and zero legacy loads.
Each runs with both capabilities and with either capability lost (27 rows),
without provider work or child runtime construction. Before the fix all nine
tests fail: eight read stale v3 records, while writer resume already refuses
with generic boundary evidence.

The exported `assert_journal_retired_latest_conformance` runs in both protected
and ordinary collection suites. It distinguishes a genuinely absent head from
retirement before collection, after a pinned sweep, and after all content is
deleted; both read-only and writer opens must conflict. Independent retained
reads and the pre-retirement reader still materialize their snapshots. A broken
adapter that reports retirement as an empty lease fails the named case.

The nine existing `journal_gc_runtime` schedules now serialize each
mark/mutation/delete sequence and inspection pin. Fresh-mark collection must
succeed and report the expected epoch; a mark taken before a successful
mutation must be stale and conflict. No call silently accepts an incidental
`StateConflict`, and all existing scenario assertions remain in force.

Verification through the requested timeout-bounded `c.sh` commands: formatting
check and all-target/all-feature Clippy with warnings denied pass; core **276**,
runtime **669**, testkit **410**, LCM **62** — **1,417 passed, zero failed or
ignored**. Rust **1.86.0** builds core and runtime, and strict change validation
passes. The targeted collection suite passes **29** tests and the tightened
runtime GC harness passes **9** (included in the full totals). Red/green evidence,
individual gate logs and the report are in `/Volumes/Data/tmp/refactor/u9c-2/`.

## v3 Upgrade, Including Mid-turn

'v3 snapshot' means the v3-era storage pair: SessionSnapshot itself has no
schema_version field on main; protected TurnCheckpoint has schema_version=3.
Do not invent a legacy snapshot tag as a precondition for reading it.

1. Ship dual readers first; supported v3 transition revision 4 and strict identity,
   binding, namespace and operation checks remain. Invalid/missing evidence is
   not a migration opportunity. No raw v3 record is pruned before U3 boundary
   bootstrapping. Keep a recovery backup/pin until native publication completes.
2. Without native capabilities, load the old pair and keep writing materialized
   schema-3 checkpoints through existing methods, exactly as before. Source
   implementations keep compiling and custom schema guards need no change. No
   delta performance claim applies. Such a host can roll back to the previous
   release. The v3 reader window is counted from the first release that
   activates a native writer (group B), not from the group A contracts.
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

### Task 2.5 implementation evidence (2026-10-18)

`SessionPersistence::open` now reserves both native writer domains before its
single v3 pair load. `validate_legacy_import_records` applies the same
checkpoint fingerprint, session/snapshot identity, manifest-boundary,
ordinary/protected overlay, delegation and LCM namespace checks used during
resume before any import commit. Import uses stable `runtime-import-v3-*`
operation identities and the canonical object encoder. It publishes protected
first, repairs a missing ordinary head after a crash, closes both import pins
only after all available publications, then reopens and discovers the native
heads without consulting a marker. V3 records are read-only throughout.

`v3_import_crash_cut_matrix_repairs_exactly_without_provider_io` captures real
legacy checkpoints at idle, CallingModel, ModelResponseReady,
AwaitingApproval, AwaitingInteraction, ExecutingTools, ToolOutcomeReady,
LocalActionExecuting and all four cache phases. For each state it cuts every
protected and ordinary reference-backend commit stage (`AfterObjects`,
`AfterCheckpoint`, `AfterBatch`, `AfterPin`, `AfterHead`, `AfterIdempotency`,
and ambiguous `AfterCommit`) plus import-pin close before and after release.
Every fresh open materializes the original v3 ordinary/checkpoint values and
checks their roots against a cold encoding; configured providers receive zero
requests. `v3_pair_import_preserves_pending_lcm_summary_without_repeating_model_work`
covers a protected pending LCM response whose DAG CAS landed before import.
The authorized split of
`legacy_v3_records_and_mixed_pairs_keep_the_materialized_path` is
`legacy_v3_records_with_both_capabilities_import_once` plus
`mixed_pairs_keep_the_materialized_path`; the latter expectations are
unchanged. Additional tests cover redacted ordinary versus exact protected
roots, v3 mixed-pair fallback, invalid evidence before head publication,
one-time discovery, lost capability and ephemeral isolation.

Review (2026-10-18). The crash-cut matrix resumes with
`CheckpointRecoveryPolicy::Defer`, so it proves representation and repair,
not continuation. `v3_import_continuation_matches_legacy_resume_for_every_state`
adds the continuation for all 12 states: import, resume and continue on a
1.75 MiB worker make exactly the provider requests, tool invocations and
protected/ordinary barriers of legacy resume of the same pair (no replay of
CallingModel, ExecutingTools, local-executing or cache-started work;
ModelResponseReady and ToolOutcomeReady results reused). That test first
overflowed: in debug builds `TurnMachine::resume` kept one stack slot per
awaited state future (~787 KiB), so ModelResponseReady recovery needed
~1.8 MiB legacy and ~2 MiB native; it now boxes each awaited future out of
line (~1.25 MiB). The read-only views used by fork, child recovery and child
rebind now apply open's lost-capability rule, since imported sessions keep
their v3 records. Import refusal matches legacy resume for tampered
schema/transition, foreign snapshot identity, skewed ordinary halves and
namespace revisions; raw v3 ordinary records are stored only as the host
projection; racing imports publish once; and ordinary repair refuses (writes
nothing) once a protected-only runtime has advanced the exact head past the
import, which is reachable only by changing the configured store set between
the crash and the repair. Repair now also requires a v3 checkpoint equal to
the exact head: a v3 pair without a checkpoint never writes the exact domain,
so an exact head there is native progress and a missing ordinary head must
not be rebuilt from the pre-import v3 record.

### Task 2.6 review refinement (2026-10-18)

A failed terminal publication can leave the ordinary head ahead of the exact
boundary: the protected checkpoint stays at `Completing`/`PublishingTerminal`
while a later input is saved to the ordinary store. Terminal republication on
resume then writes the boundary's shorter canonical history, as a legacy
`SessionStore::save` overwrites it. The native writer submits such a shorter
history as one complete replacement (`JournalTransitionDelta::bootstrap` over
an existing head, with no appends), which the ordinary role projects like a
bootstrap; a rewritten prefix of equal or greater length still fails closed.
Refusing the replacement wedged the session at a non-terminal checkpoint on
every resume while the legacy path continued.

Keep public snapshot/history/recent_manifests projections and event variants.
Version-4 readers run for the entire first release; future legacy-reader removal
is a separate proposal with all consumer gates. U10 has its own format change
and continues to read v3 during its first adoption release.

### Task 2.8 materialized checkpoint decision (2026-10-18)

Keep `CHECKPOINT_SCHEMA_VERSION` at 3. A native head changes the protected
storage representation, not the materialized `TurnCheckpoint` contract.
`JournalReader::checkpoint` reconstructs the same owned checkpoint fields and
sets schema 3 before running the existing validation and transition checks; the
schema-4 reference envelope, object IDs and roots are not serialized into the
materialized value. The task-2.6 matrix reads every published outcome through a
fresh `JournalReader`, compares its roots with a cold encoding, and resumes the
result through the same public checkpoint type as the legacy path; the
materialized checkpoint and snapshot bytes equal the legacy store's bytes at
every barrier of the task-2.8 byte test.

The released-reader evidence is direct. There is no persisted checkpoint-type
or validation diff from tag `v0.5.0` through `v0.6.0`, `v0.6.1` and this
decision; those releases require schema 3 and deserialize the same state,
snapshot, watermark, identity and operation fields. The native writer released
in 0.6.x therefore produces no new materialized byte interpretation for an
older reader. The original task-2.8 consumer fixtures applied hypothetical strict JSON guards;
group D replaces them with evidenced boundaries. In read-only Forge checkout
`78702e7c4821d788ba05b7b484e80abc5c5c068e` (`next/v0.14`),
`crates/agent-host/src/protected_store.rs:1942` uses typed
`serde_json::from_slice::<TurnCheckpoint>` after opening the payload. Its exact
runtime pin is now `82f5aac57a3b1012b4e96c1903f0ab4a0f00a625`
(`Cargo.toml:37`, `Cargo.lock:24`); the checkpoint DTO/validation has no diff
between that revision and this work's base. The neutral model also mirrors
nullable payload/nonce handling, ordinary snapshot fallback to the checkpoint,
and same-turn revision/fingerprint checks. It does not reproduce encryption,
compression, tenant mapping, SQL transactions or competing-process CAS.

Nyx checkout `585939d0d518e8a243c458016ed11f274d283f6d` has no
checkpoint-store schema guard. `shared/agent.rs:339-358` selects ephemeral
rebuilt history (with a stable owner) outside its optional LCM path and
subscribes before send. `shared/session_store.rs:42-74` stores complete typed
snapshot JSON, including Sensitive state; the neutral gate mirrors its
round-trip and corrupt-input refusal. Smith has no local checkout: only the
existing fixtures' typed checkpoint contract can be asserted, not an invented
product schema guard or the source compatibility of its three actual stores.

Consequently no reader-first schema-4 materialized-checkpoint plan and no new
CHANGELOG rollback limit are needed. The existing native-adoption downgrade
limit remains: an older binary must not resume a migrated native *store head*
that it cannot discover or fence. That operational store limitation does not
turn a separately materialized schema-3 checkpoint into a new wire schema.

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

## Group D consumer evidence and release eligibility (2026-10-19)

The named consumer targets include the additive journal assertions automatically;
`.github/workflows/ci.yml` already runs each target without a test-name filter.
The neutral chunk adapter delegates atomicity/fences to `InMemorySessionJournal`,
stores bounded immutable chunks on reads, checks authorization before cached
access, and counts each stored object's bytes once per protection domain. It
materializes every acknowledged native publication; it is not a durable Forge
chunk/SQL adapter. Its recovery suite imports real v3 CallingModel, pending
approval and model-result checkpoints across seven commit cuts in each domain
(42 import cases), checks exact signatures/requests/preparations, projected
ordinary content, one-time chunk writes, and fresh-runtime continuation. The
Forge target additionally includes the unchanged real LCM hard-admission suite
and native 18-row write matrix. Five fork/resume operations each run at all
three reference GC steps (15 schedules).

Smith's new file/capsule migration fixture uses two existing v3 golden pairs
through neutral file, capsule and in-memory CheckpointStore implementations,
with unchanged default `journal()` discovery and unsupported capability errors.
It checks direct manifest vectors, source-file preservation, signed pending
approval/request imports, unchanged-body deduplication and staged-versus-published
file JSON. It does not establish power-loss safety or implement a native file
journal. Existing instruction/failing contributors, steering, event matching,
held history, normalization and manifest-window checks remain in the named
suite. Paired native idle compaction starts from a valid protected idle seed
and preserves canonical history, usage, LCM state and manifest reads on resume.
No Smith source checkout or product PTY/CLI tests are available.

Nyx's ephemeral assertion configures both native stores, seeds history with a
stable owner, subscribes before send, and requires zero store discovery,
legacy operations and journal operations through persist/shutdown/drop. Optional
durability uses neutral paired journals plus Nyx's actual complete-snapshot JSON
boundary. No Nyx channel or product crate enters the runtime/testkit graph.
Captured Cargo dependency-table fixtures also exercise immutable shared-tag/rev
adoption and reject skew, local paths and moving branches. Forge's current pin
is immutable but older than the candidate. Nyx `Cargo.toml:76-80` still uses
`branch = "main"` for facade/core/testkit; its lockfile commit
`22e41fa9a8e78f6ff0101af7743806a1d7867c5c` does not satisfy immutable manifest
adoption. That is a release blocker, not a claim that this checkout adopted the
candidate. The test explicitly detects the ineligible reference; green neutral
tests alone do not grant release eligibility.

Normal `SessionPersistence::open` still reads the legacy pair once even when
both native heads exist, then selects native materializations. The new tests
record that behavior and require no second import/chunk writes. Retired-root
refusal remains the stricter zero-legacy-load contract proven in group C.

Group D authorizes documentation only for 4.5. No version bump, tag, publication
or release PR is made; task 4.5 stays open. Actual coordinated product adoption
and its immutable landed runtime revision remain release prerequisites.

Final group-D local verification (2026-10-19): core **277**, runtime **676**,
testkit **434**, LCM **62** pass (per-command totals including doc tests).
The default named targets pass Forge **25**, Smith **11**, Nyx **10**;
workspace all-features passes **2,116** with **4** pre-existing ignored cases,
and the separate default-feature doc run passes **8** with **1** ignored
example. No failures remain. Final formatting and warnings-denied Clippy pass,
as do Rust 1.86 core/runtime and eight-package all-feature builds, Rust 1.88
MCP/CLI, the 14-test command-provider target, dependency boundaries, and strict
change validation. Offline cargo deny passes all four checks using a writable
copy of existing advisory DB `999ec89d` (2026-10-17), after its original
read-only cache lock failed; only the temporary db-path is changed. Full logs
and qualifications are in `/Volumes/Data/tmp/refactor/u9d-1/report.md`.
Tasks 4.2/4.3 remain open for unavailable Smith implementation evidence and
immutable Nyx adoption, and 4.5 stays open for release. No product repository,
version, tag or release PR was changed.
