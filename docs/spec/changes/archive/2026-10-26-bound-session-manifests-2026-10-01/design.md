---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-01T21:32:43Z
---

# Design: A bounded diagnostic window with exact execution checkpoints

## Evidence on Main

Read at `main`/HEAD `b4b421fe8e049709063bac02e602bdeadf296c26`.

| Verified fact | Evidence |
| --- | --- |
| A manifest is pushed by request building, before provider execution; no bound is applied at this site. | `crates/agent-runtime/src/agent/driver/provider.rs:651` |
| Each run manifest stores ordered segment records and an optional identity whose history/prefix/tools are lists. | `crates/agent-runtime-core/src/manifest.rs:1041`, `:1055`; `crates/agent-runtime-core/src/provider/identity.rs:84`, `:98` |
| Session manifests are a public serde-defaulted Vec; the ordinary store saves a whole snapshot. | `crates/agent-runtime-core/src/store.rs:209`, `:220`, `:240` |
| Driver snapshots clone all manifests; CallingModel checkpoints clone the request. | `crates/agent-runtime/src/agent/driver/turn.rs:75`, `:1255` |
| TurnCheckpoint embeds SessionSnapshot; exact protected save semantics and revision gates already exist. | `crates/agent-runtime-core/src/checkpoint/mod.rs:528`, `:554`, `:35`, `:43`; `crates/agent-runtime-core/src/checkpoint/store.rs:3` |
| Terminal overlay currently compares manifests for equality in addition to identity/history/usage. | `crates/agent-runtime/src/runtime/engine.rs:41`, `:77` |
| Resume loads the manifest list, and SessionHandle.snapshot clones it. | `crates/agent-runtime/src/runtime/engine.rs:777`; `crates/agent-runtime/src/runtime/session/lifecycle.rs:133` |
| External operation fingerprints bind TurnState, not the diagnostic manifest list; same-state refresh also compares complete snapshots. | `crates/agent-runtime-core/src/checkpoint/transition.rs:519`; `crates/agent-runtime-core/src/checkpoint/validation.rs:379` |

G3's multiplicative manifest accumulation is confirmed. More precisely, records
are created per successful planning step, not per network retry; a planned step
can fail before I/O. The audit's byte estimates and claimed share of churn were
not measured. Journaling and duplicate history/request removal are outside U3.

## Decisions

### Retention Unit and Read Surface

Use `RuntimeBuilder::manifest_window(NonZeroUsize)` with default 32 records per
session. Children inherit the runtime setting. A record is one existing
TurnManifest append, including internal-turn records; repeated attempts for
one frozen request do not create extra records. There is no new grouping,
renumbering, synthetic manifest, or unlimited mode in this slice.

Retain the newest K records in original append order. Apply the bound at the
single append owner, ordinary snapshot capture, and live-state adoption after
recovery. `snapshot().manifests` remains a Vec and
`recent_manifests() -> Vec<TurnManifest>` returns the same suffix. An empty
window result means no retained diagnostic record, not no provider activity.
No new event or archival service is introduced. Hosts requiring longer audit
retention own their archive and migration plan; increasing a future window
cannot recover already evicted records.

Add a runtime-owned `runtime.manifest_boundary` extension namespace, exported
as `agent_runtime::runtime::MANIFEST_BOUNDARY_NAMESPACE`, with a versioned,
RedactionSafe value containing only a checked monotonic
`planned_steps: u64`. It counts successful manifest-producing planning steps,
not network attempts. Copy it into ordinary and protected snapshots. Increment
at the manifest append owner; overflow fails explicitly. No manifest body,
source content, credential, or authority grant enters this constant-size record.
Its purpose is boundary validation, not archive reconstruction or usage billing.
Stores must preserve this safe record; no store trait or public field changes.
If a pre-U3 binary preserves the record opaquely while appending manifests, a
later U3 resume treats a full stored list longer than the record as legacy
evidence and re-bootstraps from that length before pruning. This can repair only
an under-count still exposed by the list; the frontier remains a diagnostic
ordering aid rather than reconstructed lifetime history.

Keep checkpoint-independent cache planner state and extension revisions;
manifests must not become the cache baseline or execution authority. Verify
their existing separate state during implementation instead of using the most
recent manifest as an optimization shortcut.

### Checkpoint Representation and Recovery

New runtime-generated TurnCheckpoint.snapshot.manifests is empty in memory and
omitted by the existing serde rule, for ordinary, child, internal, and cache
operation checkpoints. All existing snapshot execution fields remain exact.
Keep `CHECKPOINT_SCHEMA_VERSION = 3` and `TURN_TRANSITION_REVISION = 4`:
the existing optional field is omitted, not structurally replaced, and no
external-operation transition changes. Equivalent-recovery tests must justify
this choice; failure to prove it blocks this proposal rather than silently
expanding it into U9.

Terminal overlay must compare identity boundaries, history structure, usage,
and compatible protected extension revisions as before, and require matching
planned-step boundary counters before overlaying protected extensions. Replace
the existing manifest-list equality check with that counter comparison; do not
simply delete a boundary check. Diagnostic absence, pruning, or differing
archive retention must not establish permission to overlay an incompatible
checkpoint. Nonterminal recovery retains the checkpoint's exact execution
snapshot; a valid ordinary snapshot whose frontier is behind or equal may
contribute its trimmed recent suffix as older diagnostics. An equal legacy pair
with different lists contributes nothing instead of failing protected
recovery. The manifest for the in-flight request is not recoverable from a
manifest-free checkpoint and must not be invented.
When the ordinary snapshot is unavailable, new checkpoint-only recovery has
an empty diagnostic window and still resumes exact execution.

For a legacy snapshot/checkpoint without the new namespace, derive its counter
from the full manifest-list length before applying any window. Validate two
legacy records using the original complete-list equality plus the other
boundary checks, then initialize the new live record. For a mixed pair, compare
the new counter with the legacy full-list length; never compare a trimmed list
length with a lifetime counter. If required boundary evidence was removed by a
host, reject the unsafe overlay explicitly rather than assuming zero or using
the protected counter to attest the ordinary snapshot. Checkpoint-only recovery
needs no ordinary-boundary attestation and still uses exact protected state.
This permits a newly bounded ordinary save to coexist with the original legacy
terminal checkpoint on a later restart without rewriting that checkpoint.

Keep generic serde deserialization lossless: do not discard manifests in the
SessionSnapshot or TurnCheckpoint deserializer. Strip only newly constructed
runtime checkpoint snapshots. Do not rewrite a loaded checkpoint at the same
`(session, turn, state_revision, operation_fingerprint)` merely to remove its
manifests; replaying an existing revision keeps its original record. Normalize
diagnostics consistently before any new same-state refresh comparison, so
trimming cannot cause a phantom execution-state transition. Never redact the
checkpoint's request, arguments, history, sensitive extensions, or outcome.

### Older Records and Replay

| Stored form | Current reader and live runtime behavior |
| --- | --- |
| Older snapshot without manifests | Decode to empty and resume all existing history, usage, identities, and extension state. |
| Older unbounded snapshot | Raw deserialization exposes the full stored list; validated runtime adoption keeps its newest K, and the next ordinary save writes that suffix. |
| Older checkpoint with manifests | Decode/validate the original record and expose its list to direct readers. Use it as a diagnostic fallback if ordinary diagnostics are unavailable; resume execution using the protected state, then bound live diagnostics. |
| New checkpoint without manifests | Decode to empty; recover exactly even without an ordinary diagnostic snapshot. |

Retained manifests still support their existing revision checks. Historical
equivalent manifest replay beyond K requires an explicitly supplied host archive;
missing manifest/revision evidence returns a structured unavailable/conflict
before I/O, never an invented manifest or silent non-equivalent replay.
Canonical conversation history and signed provider continuation are unaffected.
All replay/archival policy stays with hosts.

Main's older serde reader can parse omitted manifests, but its terminal overlay
assumes list equality and may reject a new checkpoint paired with a nonempty
snapshot. Therefore backward-readable JSON is not a promise that an old binary
can execute every new recovery record. Pin supported consumers to the candidate
runtime and document downgrade behavior; never claim untested execution
compatibility merely from unchanged schema numbers.

## Risks and Validation

The truth specs currently promise all ordered manifests and restored child
manifests. Modify those full requirement blocks explicitly. A retained-window
change can affect Smith diagnostics even though its source still compiles;
its acceptance of retention is a required consumer gate. Tests must distinguish
diagnostic equality from exact execution-state equality and retain negative
tests for stale boundaries, divergent usage, incompatible extensions, and
unsupported checkpoint revisions. Include same-history/usage but different
planned-step frontiers, mixed legacy/new pairs, two restarts after a migration
save, and missing/malformed boundary metadata: these must never permit a stale
protected overlay.

Use K=2 with at least five planned steps, repeated provider attempts, child and
internal turns, terminal restart, missing ordinary state, pending approval,
committed model/tool outcomes, cache checkpoint recovery, and same-state refresh.
Assert unchanged history/usage/identities/signatures, newest-record ordering,
no duplicated external work, and no manifest array in new checkpoint JSON.
Count records/serialized bytes deterministically; do not assert an unmeasured
percentage reduction.

## Open Questions

- Approve default K=32 and finite-only configuration.
- Do Smith or Forge require a lifetime manifest archive? Their owners must
  decide archival/retention policy before compatible release eligibility.
- Approve the stated old-binary downgrade limitation after reviewing the
  recovery-equivalence fixtures; this is not a schema-v4 migration.
