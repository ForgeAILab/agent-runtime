## Context and current ordering (baseline 5ee41e4)

- `crates/agent-runtime/src/harness/lcm.rs:3970` synchronizes canonical history,
  handles a prior pending response, then evaluates hard pressure unconditionally.
- `crates/agent-runtime/src/harness/lcm.rs:3058` and `:3169` construct leaf and
  condensation pending responses when compact_once is called without commit.
  `:4200` returns checkpoint_and_retry with that response in harness.lcm.
- `crates/agent-runtime/src/agent/driver/turn.rs:645` applies state and charges
  usage; `:664` transitions to Planning on retry. `:409` writes only to
  CheckpointStore. Without it, the transition is only in memory.
- `crates/agent-runtime/src/harness/lcm.rs:3988` consumes the prior pending
  response on the next admission; `:3841` checks identity/body/plan and performs
  the idempotent leaf or condensation CAS.
- `crates/agent-runtime/src/agent/driver/turn.rs:469` publishes terminal and
  `:475` saves the ordinary snapshot. This is too late to protect the hard CAS.
- `crates/agent-runtime/src/harness/lcm.rs:1750` validates resume pending metadata;
  `:3550` requires an exact successor revision, matching canonical source,
  classifications, active node and predecessor DAG/plan, before adoption.
- `crates/agent-runtime/src/runtime/engine.rs:318` invokes resume validation and
  `:333` saves repaired state before returning a handle.

U7/U8 (8b4219c, 1bc2218 and their merges through 5ee41e4) add timeline claims,
frontier-safe append reconciliation and explicit create/resume/fork. They retain
this admission ordering. Their append-successor path (`lcm.rs:3733`) explicitly
rejects pending summaries; it cannot replace exact hard-successor proof.

## Goals / Non-Goals

Make hard DAG mutations recoverable for SessionStore-only hosts without weakening
validation or changing checkpointed/storeless hosts. Canonical full history and
summary usage travel in the staged snapshot. This does not introduce generic
turn replay guarantees for ordinary stores. Bounded condensation, escalation
validation hooks and guard provenance interoperability remain out of scope.

## Decision and chosen ordering

1. LCM stages the existing exact pending response in Sensitive harness.lcm.
2. Driver applies the state/usage patch and, under the session persist gate,
   saves the complete ordinary snapshot before returning RetryAdmission, only
   when CheckpointStore is absent and SessionStore is present.
3. The next admission validates and commits the exact DAG CAS; existing operation
   IDs and fingerprints make retries idempotent.
4. Subsequent staged rounds save their own predecessor/intent. The driver also
   saves finalized successor state before provider admission. For a turn that
   crossed this barrier, it also saves full canonical response history before
   terminal LCM synchronization appends it; terminal persistence then records
   the hook's final state as before.
5. Ordinary-only resume first uses full existing validation/adoption, then
   validates body and source plan and discards any still-uncommitted hard pending
   response before constructing a live handle. Discard/adoption ends the old
   admission round budget. The repaired snapshot is saved.

Discard rather than replay on ordinary resume avoids appending a new turn over
an intent with an obsolete CAS revision. Existing checkpoint recovery retains
its pending response and exact turn replay. Summary usage already charged in the
intent snapshot is retained even when its uncommitted response is discarded.

## Exact crash windows

- Before intent save succeeds: no new summary DAG CAS may happen. Save errors
  abort admission, surface RuntimeEvent::Error and refuse provider I/O.
- After intent save, before CAS: resume validates the unchanged predecessor and
  discards pending body; full canonical history remains available.
- After CAS, before any next save (including terminal): persisted predecessor
  plus intent proves the exact successor. Resume adopts it, clears intent and
  persists repair; duplicate nodes/model replay are avoided.
- Between multiple rounds: previous saved intent proves the preceding CAS until
  the next intent save succeeds; each new CAS has its own saved proof.
- After finalized successor save, before terminal: the hard mutation is already
  durable in ordinary state. Before terminal hooks, save the full canonical
  response history with that predecessor LCM state; a crash after the terminal
  immutable append uses existing exact append-successor validation. This closes
  the later window where provider response entries otherwise exist only in LCM.
- After terminal save: strict normal resume, with no pending intent.
- Failure saving a resume repair: construction fails; the old proof remains
  usable for retry, with no new DAG mutation.

## Compatibility and migration

Preferred staging is feasible and implemented; no hard-disable policy or
CannotFit fallback is needed. No public production API changes. Testkit adds reusable public fault fixtures
FaultInjectingLcmStore and LcmCommitFault (with leaf/condensation-specific arming).
Event schema
16 -> 16, command schema 2 -> 2, checkpoint schema 3 -> 3, LCM state schema
1 -> 1. Existing optional pending_summary is reused; no migration/backfill.
A changelog Fixed entry describes the additional ordinary persistence boundary.

## Risks / Trade-offs

Only ordinary-only turns that stage hard responses add intent, finalization
and pre-terminal-hook snapshots. Idle
uncommitted intent handling retains its existing behavior. SessionStore must durably
save Sensitive extension state and canonical history, as its existing contract
requires. Persist gate serializes admission snapshots with host persistence.
Storeless hosts remain non-durable. Exact adoption continues to fail closed on
unexplained DAG progress, changed source/authority/guard/classifier identity,
malformed commits, or noncanonical history. Old already-divergent snapshots
without pending proof cannot be repaired by this change.


## Implementation evidence (working tree)

`crates/agent-runtime/src/agent/driver/turn.rs:675` uses the private
ordinary_lcm_intent_saved marker to save both the retry intent and its final
successor under persist_gate, before returning from admission. No ordinary
writes are added to admissions that never stage a hard response. The marker is
process-local; ordinary resume resolves the persisted proof before any new turn.
`crates/agent-runtime/src/agent/driver/turn.rs:445` uses the
ordinary_lcm_hard_admitted marker to additionally save canonical provider
response history before turn-commit synchronization, so the later immutable
append retains exact append-successor proof until terminal save.
`crates/agent-runtime/src/harness/lcm.rs:1795` validates ordinary resume through the
existing exact-successor path, then validates the body/source plan before
clearing an uncommitted hard response. `crates/agent-runtime/src/runtime/engine.rs:319` selects ordinary validation and
`:339` persists repair before
constructing a handle. Only the ordinary-only composition selects intent discard;
checkpointed and storeless validation routes remain unchanged.

## Deterministic failure matrix

Testkit's FaultInjectingLcmStore delegates authorization, owner fences, CAS and
idempotency to InMemoryLcmStore and can fail before or after a leaf/condensation
CAS. FaultSessions rejects writes after the saved intent to retain exactly the
crash boundary; terminal failure handling cannot overwrite the fixture. New
runtimes use the retained ordinary snapshot and reference DAG, with no network.
Tests cover leaf discard/adoption, full terminal resume, staging-save errors,
finalization-save errors, repair-save errors, the late terminal-append crash,
checkpointed/storeless parity,
malformed/missing-history refusal, and condensation discard/adoption after eight
separately staged leaf rounds. Adoption tests compare exact active nodes, IDs,
node counts, model call counts, retained usage and complete canonical history;
a second restart proves the repair is durable.
