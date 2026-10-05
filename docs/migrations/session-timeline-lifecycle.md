# U7/U8 session and timeline migration

Commands use schema version 2. Wire payloads require `mode` (`create`, `resume`,
`ephemeral`) and replace `initial_history` with `seed`; schema 1 and unknown
legacy fields are rejected. Decode wire payloads with `StartSession::from_json`,
which returns a typed `Config` error for another schema version. Rust hosts
should use:

```rust,ignore
let fresh = runtime.start_session(StartSession::create(id, history)).await?;
let restored = runtime.start_session(StartSession::resume(id)).await?;
let one_call = runtime.start_session(StartSession::ephemeral(history)).await?;
assert!(restored.resumed());
```

`StartSession::with_id` is removed, so a former `new().with_id(id)` resume no
longer compiles: choose `create(id, seed)` or `resume(id)`. `new()` is an unnamed
create and `with_history` only edits the seed. Create checks both ordinary
snapshots and protected checkpoints for conflicting state. It is retained in
memory until a normal persistence boundary or explicit `persist` (a generated
Fork summary is protected immediately when a checkpoint store exists); a live
identity is leased within the runtime. Resume requires saved state and rejects
any seed with typed Conflict. Resume identity floors and checkpoint recovery
policies remain available. Ephemeral clones the composition with ordinary and
protected stores disabled, even if the builder configured them, and runs LCM on
a volatile in-memory timeline: it never claims or writes the host timeline
bound to its id.

Nyx's reconstructed per-call `new().with_history(history)` becomes
`ephemeral(history)`. Smith's durable `/new` action calls `fork_session` with
`ForkSeed::Empty` and `ForkLcm::NewTimeline`. Forge topic genesis and handoff use:

```rust,ignore
let topic = runtime.fork_session(ForkSession {
    from: previous_session_id,
    new_id: next_topic_session_id,
    seed: ForkSeed::Summary(host_summary),
    lcm: ForkLcm::NewTimeline,
}).await?;
```

A fork source must be idle and have both durable stores. Active turns, pending
checkpoints, cache work, goal controllers and delegation coordinators reject the
fork. `FromIndex(index)` carries a canonical suffix; `Empty` carries none. A
summary is stored as Sensitive extension state and enters the authoritative
planner as a required, Stable Summary fragment, without parent raw history.
`Continue` deliberately adopts the old timeline's complete canonical sources
and projects its active summaries, so with LCM it accepts only `Empty` or
`FromIndex(0)`; a Summary (which would double the projection) or a later suffix
returns Conflict. Use `NewTimeline` for bounded topic rotation. The completed
parent reports `superseded_by`, rejects new work and cannot resume.

Before writing any fork intent the runtime checks the seed, the resolver's
replacement or continuation hook, the replacement's emptiness and, for
`Continue`, that the store can fence the transfer. A fork that can never
complete therefore fails typed (`ForkRequired` for a missing hook) and leaves
the parent untouched. Then a protected pending fork fences the parent. If a
later step fails while the successor owns nothing durable (no saved successor
and, for `Continue`, no claim on the parent's timeline), the intent is rolled
back and the parent keeps working. Otherwise the parent stays fenced: retry the
**same** ForkSession to repair, including after restart. After a crash that left
an intent behind, `Runtime::abort_fork(parent)` clears it under the same rule;
a parent without an intent is left unchanged. A different seed or destination
conflicts with a pending intent. A completed identical fork returns the
successor without repeating provider work. Resolver hooks must retain their
binding decisions durably and be idempotent; a replacement bound by a failed
attempt is empty and is reused by the retry. Cross-session bindings and
populated replacement stores are rejected. Stores remain independently durable;
a failure is visible and no multi-backend transaction is implied.

Implement `LcmWriter::claim(view, owner, generation)` in reusable stores. Epoch 0
claims an empty unowned timeline; same owner/epoch is idempotent; explicit
adoption uses exactly the observed epoch plus one. A populated unclaimed legacy
store reports `TimelineOwned { owner: None, generation: 0 }`. A claim-aware
store must also bump its revision on every ownership change and reject any
mutation whose view fence (`LcmView::owner()`) is not the recorded owner and
epoch, including an unfenced write to an owned timeline, with `TimelineOwned`.
The runtime attaches its claim to every view it writes through, so an old owner
that already passed its admission re-check still cannot write after a new
claim. Never treat timeline identity as authority.

Stores that keep the default claim stay usable without fencing. The default
records nothing: it returns `Claimed` for an empty timeline and
`LcmClaimResult::Unsupported` for a populated one. A session created on its
timeline runs normally, and the same session continues on it after a resume.
A fresh binding of a populated timeline is replaced by a real Fork: the runtime
renders the old timeline's projected summary, asks the resolver's `new_timeline`
hook for an empty replacement and claims it, so implement that hook (without it
the fresh binding fails with typed `ForkRequired`). Explicit Retire is honoured;
explicit Adopt of a fresh binding returns `ForkRequired`, because the store
cannot fence it.

For a fresh binding on a claim-aware store, resolve ownership conflicts with
`with_lcm_policy(Adopt|Fork|Retire)`. Adopt claims the next generation first and
then seeds canonical history from the claimed timeline's projection, so it sees
every turn the old owner committed before the claim; it requires an empty seed
and returns Conflict otherwise. Fork preserves the projected summary on a fresh
timeline: active summaries first, then the newest unsummarized messages up to
`LcmCoordinatorPolicy::fork_summary_max_chars` (default 16 384), with
Secret-classified sources omitted and the content guard applied. Retire starts
an empty replacement while retaining old immutable data. Fork and Retire call
the resolver's `new_timeline` hook (a durable rebind) before the runtime
validates the replacement. Implement `continue_timeline` to explicitly authorize
the old timeline for a new session. Default hooks fail closed. Store
authorization, guard checks and strict U6 identity remain active, and an
unauthorized view now fails at session construction instead of first use.

A session created before U7/U8 has validated U6 state on an unclaimed timeline
once its host adopts a claim-aware store. Its plain resume fails with
`TimelineOwned { owner: None, .. }`; resume it once with
`StartSession::resume(id).with_lcm_policy(LcmRecoveryPolicy::Adopt)`. The runtime
claims generation 1, rebuilds the state against the claimed timeline, validates
it against canonical history and persists it, so later resumes need no policy.

Reconcile checks the active-node frontier immediately before truncating a
provisional tail. Below-frontier conflicts return `RuntimeError.lcm` with
`LcmFailure::LcmDivergence { frontier, at }`; `RangeOverlap` and `EntryConflict`
retain typed variants and Conflict kind through the driver. Match the borrowed
`RuntimeError::lcm_failure()` evidence; the optional public `lcm` field is boxed
without changing its serialized shape. In-turn failure leaves committed data intact; at
an idle recovery boundary choose a fresh authorized session with Adopt, or
fork with NewTimeline and Summary/Empty for Fork/Retire. Known persisted resume
state remains strict; explicit fork source loading validates identity and
authority without admitting its divergent projection to provider work. Strict invalid or
non-terminal state remains fail closed.

Protected stores must admit a validated initial idle boundary generated by
`TurnCheckpoint::session_boundary`; adapters enforcing an initial admission
state can additionally check `is_session_boundary()`. This existing Terminal
wire shape protects a seed before the first turn; checkpoint schema/transition
versions do not change. The ordinary store may redact Sensitive seed/pending
state, while the protected checkpoint must retain it exactly. A Fork seed
generated on resume is protected the same way as one generated on create.
Claims are rechecked at provider admission and commit boundaries, so stale
generations receive TimelineOwned. Persisted LCM state adds a defaulted claim
generation. Rust literal/exhaustive consumers must add the optional
`RuntimeError.lcm` field, the new LcmError variants and the new
`LcmCoordinatorPolicy::fork_summary_max_chars` field.

The in-memory reference store moved from `agent_runtime_lcm::testing` to the
always-compiled `agent_runtime_lcm::memory` module (also re-exported as
`agent_runtime_lcm::InMemoryLcmStore`); the `test-support` feature is gone. It is
the volatile store behind ephemeral sessions and the conformance reference, not
a persistence backend.

Forge 3.6 can keep U6 WorkingSetPolicy, its host instructions and its protected
store split, rotate once at topic genesis/handoff, then explicitly resume the
resulting session on subsequent turns. Remove the V149 timeline-retirement
workaround and stale-LCM-state deletion for tunable changes after deploying
ownership-aware stores and durable resolver hooks; resume each existing session
once with Adopt after the store upgrade. Strict identity mismatches continue to
require explicit migration. No task/room/prompt policy belongs in runtime code.

U9 remains SessionJournal with referenced checkpoints and a v3 compatibility
adapter. U10 will make the claimed LCM timeline canonical, removing the second
history copy; this change retains today's protected/ordinary split and history
copies.
