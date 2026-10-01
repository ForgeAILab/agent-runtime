---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-01T00:00:00Z
---

# Proposal: Share history and reduce repeated LCM/test work (U11)

## Why

The runtime repeatedly materializes owned history while building a request,
and LCM pressure accounting reloads and sizes old entries. Runtime and testkit
also link 20 and seven integration test targets respectively. Reducing private
copies, repeated accounting, and redundant test linking benefits all hosts
without replacing their public history/store contracts.

## What Changes

- Share immutable history generations through private Arc ownership and reuse
  the existing Arc-backed HistoryView, materializing owned values where current
  public Vec APIs require them.
- Keep per-entry accounting and append-aware fingerprint work in a private,
  process-local LCM cache. Warm unchanged/append-only accounting processes only
  the delta; cold resume or changed evidence takes the existing validation path.
- Consolidate runtime integration tests into one target and the four testkit
  conformance targets into one; retain its three named consumer targets.
- Strengthen neutral consumer fixtures at stores, contributors, LCM authority,
  and ephemeral-history seams. Do not extract a provider wire module here.

## Impact

- Affected specs: `context-management`, `runtime-reproducibility`, limited to
  externally observable equivalence and isolation guarantees.
- Pure internals (Arc representation, cache layout, incremental fingerprint
  implementation, test binary organization) are only in design/tasks.
- No public field, signature, module root, trait method, enum variant,
  dependency, or serde format changes are required. ProviderRequest.messages,
  SessionSnapshot.history, and SessionHandle.history remain owned Vec surfaces;
  HistoryView.history stays Arc<[Message]>, and with_history remains available.
- Persisted LcmState schema/revisions/fingerprints and checkpoint versions stay
  unchanged. This avoids the current hard failure on component-revision changes.
- The scope is warm LCM **accounting**, not O(delta) total provider planning,
  projection, summarization, serialization, or persistent writes. No measured
  seven-copy elimination, CPU percentage, or CI-speed claim is made.

## Compatibility and Consumer Gates

| Consumer | Required work before release | Testkit proof to add |
| --- | --- | --- |
| Forge | No API/store migration. Run candidate LCM/protected-store gates; keep the reader/writer and StaticLcmTimelineResolver behavior. | `consumer_open_forge`: counting authorized store/sizer proves unchanged warm pressure totals, append-only sizing, revoked-grant rejection, and exact cold checkpoint resume. |
| Smith | No API migration. Keep file/session/checkpoint stores, direct manifest reads, contributors, event matches, and idle compaction. | `consumer_smith`: legal contributor plus neutral file-backed/exact stores; retain an old snapshot/view across a new append and idle compaction, proving isolation and unchanged serde. |
| Nyx | No store/LCM adoption. Preserve StartSession::new().with_history and subscribe-before-send. | `consumer_nyx`: seeded storeless session, ordered streaming/terminal events, and held history snapshot remains immutable across later turns. |

These fixtures are proposed additions. Reuse/extend U1/U2/U3's seam fixtures if
landed; the proposal remains independently testable. Keep `consumer_smith`,
`consumer_nyx`, and `consumer_open_forge` as named runnable test targets because
the existing CI gate calls them. All supported consumer suites and actual
consumer compile/contract gates must pass; any failure blocks a compatible
release. Release dependencies remain tagged or exact landed revisions.

Every audit §5 public surface remains, including Smith's snapshot.manifests
type and all event variants, Forge's provider/turn events and protected/LCM
contracts, and Nyx's lifecycle flow. U3's bounded retention applies only if
separately approved/landed; U11 alone preserves current retention. Protected
execution state stays exact and idempotent with unchanged revision fences;
ordinary stores may still redact. `Secret`, LCM Debug, Sensitive extension
defaults, manifest redaction and credential exclusion stay intact. Caches
cannot substitute for opaque LCM grants or authorize_view before lookups.
Approval/workspace/cache validation/model limits remain fail-closed; no new
provider context can bypass the planner. This is foundational runtime efficiency
work, with no product prompts, Forge domain concepts, or consumer dependencies.

## Dependencies

No contract dependency on U1/U2: their finalized error/tool seams must be kept
when touching adjacent files. Prefer U3 before U11 to simplify checkpoint
staging; if implemented independently, U11 preserves then-current manifest
retention and rebases the internal work when U3 lands. The existing cache/session
responsibility refactor is already implemented and its public roots stay.
U6/U7 revision-tolerant decoding, LCM ownership/reconcile policy, and U9 journaling
are not prerequisites or authorized scope. The four W1 proposals each carry
ordering notes; no unestablished changes-root index is added.

## Approval Boundary

This is a document-only draft. Later approval covers internal sharing and
accounting/test efficiency under existing contracts. Replacing public Vec
fields with Arc slices, changing fingerprint algorithms, persisting a new cache
schema, changing LCM policy, or dropping consumer/test coverage requires a
separate proposal.
