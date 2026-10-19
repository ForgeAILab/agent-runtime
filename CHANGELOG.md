# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project uses
pre-1.0 semantic versioning: while the major version is `0`, minor releases may
contain breaking changes and are coordinated with consumer proposals.

## [Unreleased]

### Added

- `JournalApplyReceipt` reports the body count and encoded bytes physically
  deleted by `JournalStorage::apply`, including shared-body last-membership
  accounting. Collection now sweeps dirty sessions incrementally.
  `JournalEngine::retained_bytes` provides a logical byte diagnostic for observed
  sessions and storage's pending dirty sessions.
- Testkit `conformance::journal_storage::assert_journal_storage_conformance`
  checks storage transactions, restart recovery and the protected, ordinary and
  collection engine suites. `JournalCollectionHost` is implemented for
  `JournalEngine<S>`; five broken storages verify named failure cases.

- Explicit host-triggered retirement and logical collection in the volatile
  `InMemorySessionJournal`, with revision-bound retention authorization, durable
  migration/fork/child and historical pins, transactional root/pin epochs,
  bounded deletion, protected orphan-staging reclamation and retirement
  tombstones. Collection preserves live canonical history, LCM summary sources,
  retained checkpoint closures and execution replay fences. Default constructors
  still return typed unsupported until the host enables collection; no schedule,
  TTL or automatic lease expiry is introduced. Existing object/head/checkpoint
  formats and required `SessionJournal` methods are unchanged.
- Named third-party protected/ordinary collection conformance suites, 24 explicit
  operation/GC-step interleavings, real runtime fork/Continue/pending-child
  schedules, retained-byte evidence, and broken adapters that lose pinned
  objects, ignore epochs or clear a pending intent on failed repair. Backend
  storage compaction is tested separately from logical GC and LCM compaction.
  The suite requires every journal object kind to survive collection, transfer
  pins and renewed leases to protect superseded roots, retirement grants to be
  session-scoped, ordinary FromIndex suffixes to outlive their parent, and an
  open of a partly collected root to fail before returning a lease.

### Fixed

- Retired native sessions now reject read-only latest-root opens as an explicit
  conflict, including after collection. Runtime views check the journal before
  loading legacy records, so imported sessions cannot fall back to their stale
  v3 snapshots during fork, session-exists, child recovery/rebind or watermark
  refresh. Writer refusal and pinned historical reads are preserved. Collection
  conformance distinguishes retirement from absence, and runtime GC schedules
  assert every expected success or stale-mark conflict.
- Waiting for a user, internal, or resumed turn to complete now also waits for
  its execution gate and active-turn admission to be released. Previously a
  host waking from `TurnHandle::completed` or `SessionHandle::run` could have
  an immediate local tool action refused as non-idle, or an explicit idle
  compaction return `Busy`, even though that turn had completed.
- Automatic child-completion delivery is no longer refused when its batch
  passes the 4,096-character internal-turn limit. Such a batch stayed ready,
  so one long child reply blocked every later delivery to that parent.
  Replies are now inlined while they fit `DelegationConfig::delivery_inline_bytes`
  (default 4,096); a longer reply is delivered as its opening 1,024 bytes plus
  a reference to a parent-owned copy (purpose `delegation.child-reply`), and
  a `delegation.child-completion` turn is bounded by
  `MAX_CHILD_COMPLETION_TURN_CHARS` instead of the general limit.
- A delivered child outcome now lists the artifacts transferred to the parent
  (parent id, child source id, media type, size); before, the model was never
  told they existed.

### Changed

- **Breaking (pre-1.0):** `ChildTaskResult` gains `reply: Option<Box<ArtifactRef>>`
  and `DelegationConfig` gains `delivery_inline_bytes`. Struct literals must
  set them; `ChildTaskResult::reply` deserializes as `None` from older state.

### Session journal migration and retention limits

- Native adoption is opt-in through `journal()` on every configured store.
  Existing store implementations, methods and v0.6.x/v0.7.0 materialized
  formats remain unchanged: `SessionSnapshot` is unversioned and
  `TurnCheckpoint` stays schema 3 / transition revision 4. Schema 4 identifies
  journal objects, heads and referenced checkpoints only. Valid v3 checkpoints
  and v3-era snapshots, including unfinished turns, remain readable throughout
  the entire first native-writer release; removing readers needs a separate
  coordinated change.
- A capable ordinary/protected pair imports v3 once under writer fences,
  publishing the exact protected head before its independently projected
  ordinary head. Import freezes requests, signatures, prepared actions and
  committed outcomes without provider/tool I/O. A crash before exact publication
  leaves v3 authoritative; after it, native discovery repairs only the supported
  partial import. Original v3 records remain unchanged. Mixed pairs stay legacy
  before any native head; detectable native data with a lost capability fails
  closed. Ephemeral starts bypass all durable discovery and operations.
- **Downgrade:** stop all old legacy writers before native adoption; they hold
  no journal fence. An old binary must never execute a migrated session from
  the stale v3 records it can still see. Downgrade requires an explicit idle
  export with verified semantics, or restoring a host backup with writers
  stopped. A separately materialized schema-3 checkpoint remains schema 3;
  readable JSON does not establish old-binary execution compatibility.
- Retention is explicit host policy: no automatic GC, TTL or lease expiry.
  Collection keeps live canonical history, LCM summary sources, retained roots,
  staged work and migration/fork/child pins. Logical supersession alone permits
  no deletion. Unsupported collection retains content; physical backend
  compaction and LCM context compaction do not delete canonical history.
  Published corruption fails closed and needs verified host repair; only an
  unpublished torn tail may be quarantined under the writer fence. The neutral
  reference is volatile; production adapters must supply their own durable
  framing, fsync/transaction and cross-process fences.
- **Pre-1.0 coordination:** native persisted-store adoption is a coordinated
  compatibility boundary. A failing Smith, Nyx or Open Forge gate blocks a
  compatible release. Record product and runtime commits, run product store
  checks, and adopt one shared immutable landed tag or exact revision across
  runtime packages; local overrides and moving `main` pins do not qualify.
  The read-only Nyx reference currently tracks `main`, so its release adoption
  remains blocked. No release/version/tag is created by this documentation.

Measured task-2.7 totals after the representation refinement are below. Each
cell is **serialized submission bytes / newly retained immutable-object bytes**,
not physical disk, encryption, compression, WAL or index amplification. A turn
is the second turn after bootstrap under an identity ordinary projection.

| Path | N=100 | N=1000 | N=4000 |
| --- | ---: | ---: | ---: |
| Plain bootstrap | 146,814 / 239,857 | 1,062,815 / 1,742,270 | 4,133,607 / 6,767,016 |
| Plain protected turn | 42,041 / 48,051 | 43,740 / 49,764 | 43,088 / 49,112 |
| Plain ordinary terminal | 10,867 / 28,134 | 11,815 / 29,671 | 12,263 / 29,681 |
| Tool bootstrap | 154,239 / 247,610 | 1,070,240 / 1,750,023 | 4,141,032 / 6,774,769 |
| Tool protected turn | 108,028 / 119,907 | 111,468 / 123,375 | 110,164 / 122,071 |
| Tool ordinary terminal | 16,496 / 34,042 | 17,848 / 35,983 | 18,520 / 36,217 |
| Plain unchanged boundary | 1,183 / 2,015 | 1,187 / 2,021 | 1,187 / 2,021 |
| Tool unchanged boundary | 1,180 / 2,011 | 1,184 / 2,017 | 1,184 / 2,017 |

Unchanged boundaries submit zero drafts and zero body bytes; their nonzero
totals are revisioned metadata. At N=4000, plain bootstrap fell from
9,114,257 / 20,124,203 bytes to 4,133,607 / 6,767,016; the latter retained
size is 3.82× one 1,772,321-byte legacy snapshot. Legacy complete-turn
protected / ordinary-terminal bytes remain 5,659,421 / 1,772,321 (plain) and
11,326,721 / 2,277,973 (tool). Opaque host extensions still cost whole-value
replacement bytes. Bootstrap, legacy writes, full materialization and policy
validation remain history-dependent; the roughly-flat end-to-end CPU target
was **not met**. N=10,000 cannot exercise a successful turn because the existing
4,096-entry stable-history identity limit rejects planning before provider I/O.

The separate N=1000 retention measurement keeps 570,121 encoded immutable bytes
before collection, 404,529 with the canonical session still live (165,592
reclaimed), and 0 after authorized retirement and pin release. It excludes the
small replay/tombstone indexes and physical amplification. Detailed task-2.7
and follow-up evidence, including per-commit rows and reproduction commands,
is in [development.md](docs/development.md#native-turn-byte-cost-evidence-u9-task-27).

## [0.7.0] - 2026-10-18

### Changed

- A provider error now carries the reason the provider gave, such as Gemini's
  `invalid_request: Request contains an invalid argument.`, instead of only
  "provider rejected the request". Every adapter checks the reason against the
  credentials it sent on that request and drops the whole reason if it repeats
  any of them; a kept reason is bounded to 300 characters and kept as
  `provider.detail`, with the HTTP status as `http.status`. The reqwest
  transport reads it too, including from Gemini's SSE-framed error bodies.
  Authentication rejections carry the reason as well, so a gateway's 403
  "Insufficient quota" no longer reads as a rejected key.
  `agent-runtime-provider` now depends on `url` unconditionally.
- A provider `BadRequest` converts to `ErrorKind::Provider` rather than
  `ErrorKind::Config`, so a rejected request no longer displays as
  `Config: ...`. `Unsupported` stays `Config`; failure classes are unchanged.
- `CharRatioSizer` counts a reasoning part's signature at its own ratio,
  `signature_chars_per_token` (default 6, settable with
  `with_signature_chars_per_token`). Gemini bills its replayed search steps,
  which live in those signatures: one search call and result cost about
  9,000 input tokens that the planner used to count as zero. The new public
  field breaks struct-literal construction, and the sizer revision is now
  `char-ratio-2`.

### Fixed

- A native session no longer wedges after its terminal publication fails. When
  a later input was saved to the ordinary store after that failure, resuming
  republished the terminal boundary with a shorter history than the stored
  projection; the native writer refused it as a non-append, failed the turn and
  left the session at a non-terminal checkpoint on every resume, while the
  legacy path overwrote the projection and continued. The writer now submits
  such a history as one complete replacement (`JournalTransitionDelta::bootstrap`
  over an existing head), which `InMemorySessionJournal` accepts and projects
  like a bootstrap. `assert_ordinary_session_journal_conformance` now requires
  ordinary journal adapters to accept the same replacement; one that still
  rejects it fails closed as before.
- Restoring a parent no longer fails when the host cannot compute one child's
  policy fingerprint, for example after that child's profile changed. That
  child is marked incompatible (non-resumable if it was interrupted) and the
  parent and its other children restore.

## [0.6.2] - 2026-10-18

### Fixed

- Gemini Interactions no longer rejects history that holds an unsigned
  function-call step it still checks: tool calls continued from another
  provider, or a turn saved before it answered. Such steps now lead with
  Google's `skip_thought_signature_validator` placeholder thought. Steps in
  earlier turns that ended with an answer are sent unchanged.

## [0.6.1] - 2026-10-18

### Fixed

- Starting a session or a child session no longer overflows a 2 MiB stack in
  debug builds. The startup body introduced with the session journal (~30 KB as
  a future) is boxed, so hosts that spawn children from inside a tool call stay
  within default test and worker thread stacks.

## [0.6.0] - 2026-10-18

### Changed

- Existing schema-3 sessions now move once to native journaling when both the
  ordinary and protected stores expose `SessionJournal`. Import is fenced and
  idempotent, publishes the exact checkpoint head before the separately
  projected ordinary head, repairs an interrupted ordinary publication on the
  next open (only while the exact head is still the imported v3 checkpoint;
  otherwise the open is an incompatible-store error), and never rewrites or
  prunes the v3 records. Mixed-capability
  pairs remain wholly materialized until a native head exists; after that, a
  detectable lost capability is an incompatible-store error rather than a
  fallback to stale v3 state. Mid-turn requests, prepared interactions,
  committed model/tool/cache results, deadlines, watermarks and pending LCM
  summaries retain their exact protected representation, without provider,
  model or tool calls during import. Because the v3 records stay in place, a
  release older than this one must not resume a migrated session: it would
  read the stale v3 pair. Upgrades must also stop older writers of a session
  before a new runtime resumes it; legacy-mode writers hold no journal fence.

- **Breaking:** the schema-4 journal vocabulary released in 0.5.0 grows so a
  native turn costs O(delta). `JournalObjectKind` gains `ExtensionReference`,
  `ManifestReference`, `JsonValue`, `JsonInline`, `JsonBranch` and
  `JsonObjectEntry`, `JournalSequenceNode` gains `JsonLeaf`,
  `JsonValueBranch` and `JsonBranch` (valid only below a persistent JSON
  array; generic sequences reject them), and
  `JournalTransitionDelta` gains `extension_references` and
  `manifest_references`, so exhaustive matches and struct literals must add
  them. The native writer now sends the Runtime-owned
  `runtime.core.previous_cache` record and new manifests only by reference to
  persistent JSON drafts: an ordinary `SessionJournal` adapter must resolve
  them from the commit's drafts or its own pinned closure and store that form
  under the same object IDs (`assert_ordinary_session_journal_conformance`
  now checks this; `assert_session_journal_conformance` checks that protected
  adapters admit and keep shared structured prefixes), and a
  `JournalProjectionPolicy` that changes or omits those records on a commit
  that references them rejects the commit. Persistent JSON now uses
  minimum-height arrays, typed compact values with a 1,024-byte threshold and
  cached path/root encodings; this directly replaces the unreleased native
  writer shape and adds no compatibility reader for it.

### Added

- `RUNTIME_PREVIOUS_CACHE_NAMESPACE`, `JOURNAL_INLINE_JSON_MAX_BYTES`,
  `journal_json_can_inline`, `JournalCompactObjectId`, `JournalJsonValue`,
  `JournalJsonArray`, `JournalJsonBranch`, `JournalJsonInline`, `JournalJsonItem`,
  `JournalJsonObjectEntry`, `ReferencedSessionState`, `ReferencedTurnManifest`,
  `InMemorySessionJournal::object_bytes` and `journal_json_identical` (exact
  JSON identity that distinguishes `0.0` from `-0.0`, for encoding caches).

### Fixed

- Gemini Interactions keeps Google Search call and result steps, which can carry
  a response's only signatures, and replays them in the order Gemini streamed
  them. A response that searched and called tools now continues instead of
  failing with "signed continuation is incomplete or out of order".
- Command providers now report stderr overflow deterministically even when stdout and the child process finish first.
- Resuming an unfinished turn no longer needs close to 2 MiB of stack in debug
  builds. `ModelResponseReady` recovery plus its continuation used ~1.8 MiB on
  the materialized path and ~2 MiB on the native path (so for every imported
  session), within 64 KiB of a default Tokio worker; it now needs ~1.25 MiB.
  Release builds were unaffected.
- The read-only session/checkpoint views used by fork, child recovery and
  child rebind now report an incompatible store when their store lost its
  journal capability but the paired store has a native head, instead of
  returning that store's stale materialized (for example pre-import v3)
  record.

## [0.5.0] - 2026-10-16

### Added

- `agent::one_shot` calls on a provider or external agent, with bounded text,
  typed finishes and errors, caller-owned usage, deadlines, and cancellation
  of child work when the call's future is dropped.
- Per-turn `ExternalTurnRequest::instructions`, appended after configured
  backend instructions. Claude Code uses `--append-system-prompt`; Codex
  prefixes instructions to its input. Both CLI configs now accept optional
  standing `instructions`.
- Opt-in `agent-runtime-provider/reqwest-transport`: a shared streaming
  `ReqwestTransport` with a required, non-exhaustive destination policy
  (`DestinationPolicy::{PublicHttps, Loopback, ConfiguredOrigin}`), optional
  exact-origin pinning (required for `ConfiguredOrigin`), a configurable
  connect timeout, checked and pinned DNS answers, no proxies or redirects,
  bounded error bodies and response headers, and redaction-safe diagnostics.
  The CLI now builds on the shared `PublicHttps` mechanism with its existing
  exact-origin policy.

### Changed

- **Breaking:** `ExternalTurnRequest` is non-exhaustive. Consumers should use
  `ExternalTurnRequest::new(input, turn, cancel)` and assign optional fields
  instead of constructing it with a struct literal.

## [0.4.0] - 2026-10-15

### Added

- The flat semantic summary is back as an opt-in alternative to LCM, for
  hosts that do not use lossless context memory.
  `RuntimeBuilder::semantic_summary(Arc<SemanticSummaryCoordinator>)`
  installs it, and the `agent_runtime::harness` names it had before LCM
  (`SemanticSummaryCoordinator`, `SemanticSummaryPolicy`, `SummaryModel`,
  `ProtectedSemanticSummary`, `protected_semantic_summary_from_state`, the
  `SEMANTIC_SUMMARY_*` constants) are public again.
  - A runtime has at most one strategy: `build` rejects the flat summary
    together with LCM, including a generic registration under the
    `harness.semantic_summary` component id, and together with
    `soft_on_turn_boundary(true)`.
  - `SessionHandle::try_idle_semantic_compaction` runs idle compaction and
    returns the protected summary as `SemanticIdleCompactionAdmission`.
    LCM's `try_idle_compaction` is unchanged.
  - `SessionHandle::restore_semantic_summary_if_absent` restores state a
    host keeps outside its ordinary session store, fail-closed.
  - With the flat summary configured, persisted `harness.semantic_summary`
    state resumes as live state and is never imported into LCM. With LCM,
    or with neither, resume behaves as before.
  - The state keeps the schema-v1 wire format, which the LCM importer reads,
    so a session can still move to LCM later.

### Changed

- **Breaking:** `HarnessEvent` gains `SemanticSummaryFallback`, reported as
  `RuntimeEvent::Downgrade { capability: "semantic_summary" }`. The event
  schema is unchanged. `CHECKPOINT_SCHEMA_VERSION` stays 3.

## [0.3.0] - 2026-10-15

### Added

- Hosted web search. A `ProviderRequest` can declare the provider's own web
  search, with an optional per-response cap, and a capability flag tells a
  host beforehand whether an adapter supports it. Responses (including xAI)
  maps it to `web_search`, Anthropic to `web_search_20250305` and Gemini to
  `google_search`; Chat Completions reports it unsupported.
  `RuntimeBuilder::hosted_tools` declares it on main-agent requests.
  - `RuntimeEvent::HostedSearch` carries the query, the sources found and,
    for a page step, the action (open page or find in page) with its URL
    and pattern.
  - Citations are rendered into assistant text as Markdown links, without
    repeating a link the cited text already contains. Usage records the
    search count.
  - Provider-opaque blocks never enter canonical history, so a session can
    continue on another provider.
  - An Anthropic organization without web search gets a classified error a
    host can fall back on.

- Image content in tool results and requests.
  - MCP `image` blocks become `ContentPart::Image`, bounded by host-set
    `max_image_bytes` and `max_images` and limited to png, jpeg, gif and
    webp. An image over a bound becomes a text line naming its type, size
    and the reason.
  - Chat Completions sends each run of tool-result images in one following
    `user` message, labelled with its `tool_call_id`. Text-only results
    serialize byte for byte as before.
  - The context planner projects images before sizing and cache identity.
    A text-only profile gets no image parts. `ContextPolicy`'s optional
    `ImageWindow { inline: K }` keeps the newest images inline and replaces
    older ones with a placeholder. `ContextPlanned` reports how many images
    were replaced and why.

- Pinned abilities and capability browse. `RuntimeBuilder::pinned_abilities`
  places named abilities in every authorized session's first activation
  epoch, independent of retrieval. `registry.search` without a query lists
  the authorized catalog with per-domain counts, and explicit searches also
  score a card's title and summary. A protected `registry.activate` tool
  stages named ids through the same transaction as search. Scope inputs
  accept allow and deny patterns (`CapabilityPattern`), and sessions expose a
  read-only capability catalog to hosts.

- SessionJournal contracts and readers (U9 merge group A), in
  `agent_runtime_core::journal`. This is the contract a host store implements
  to persist a session as immutable content-addressed objects plus a head, so
  that a transition writes only what changed. It contains the `SessionJournal`
  trait and its request, lease, head, batch and commit types; the frozen
  `journal-json-1` encoding with domain, type and version separated SHA-256
  object IDs; `JournalReader`, which materializes a `SessionSnapshot` or
  `TurnCheckpoint` from a journal head under host-supplied limits; and
  `SnapshotJournal`, an adapter over an existing `SessionStore` and
  `CheckpointStore` pair that keeps writing full snapshots.
- The runtime does not write a journal yet. Nothing here activates a native
  writer, an import or a collector; those are later merge groups.
- The journal changes no persisted data. `CHECKPOINT_SCHEMA_VERSION` stays 3,
  session snapshots stay unversioned, and the journal adds nothing to the
  checkpoints and snapshots a host's stores write. A host can upgrade without
  changing a store or a schema guard. (External tool dispatch, below, adds
  additive checkpoint state of its own.)
  `JOURNAL_SCHEMA_VERSION` (4) tags journal heads, objects and reference
  checkpoints only. A host that publishes those through its own
  `SessionJournal` cannot read them with an older release.
- `SessionStore` and `CheckpointStore` gain two defaulted methods, `journal()`
  returning `None` and `require_journal()` returning the new typed error.
  Existing implementations compile unchanged.
- `FailureClass::UnsupportedCapability { stage, capability }` and
  `UnsupportedCapability`, built by `RuntimeError::unsupported_capability`. The
  error kind is `Config` and it is not retryable. `FailureClass` was already
  `#[non_exhaustive]`; `UnsupportedCapability` is too.
- External tool dispatch, behind `external-agent`. `ExternalTurnRequest::tools`
  carries an `ExternalToolDispatch` handle, which lists the session's authorized
  tools, less the names given to `RuntimeBuilder::external_excluded_tools`, and
  runs one call at a time through the runtime's tool path. Dispatched calls and
  their results commit to canonical history in order around the backend's text.
  It sits beside the MCP bridge, which is unchanged: `ExternalToolBridge`,
  `runtime_tools` and `external-agent-bridge` keep their meaning, and MCP-bridged
  calls still stay out of history. Also:
  - Internal turns on an external session run on the backend.
  - Tools can read a bounded, volatile log of the backend's own tool
    observations through the session view.
  - `registry.activate` through the handle returns skill instructions.
  - Backends can report non-terminal `Diagnostic` events.
  - An interrupted external turn is finalized without replay.
  - Checkpoints gain the additive `ExternalToolCall` state;
    `CHECKPOINT_SCHEMA_VERSION` and `TURN_TRANSITION_REVISION` are unchanged.

### Changed

- `agent-runtime-core` now enables `serde_json/float_roundtrip`. Cargo unifies
  features, so a host's own `serde_json` parsing becomes exact for every float
  and somewhat slower on float-heavy input. The journal encoding needs it to
  read back the float it wrote.
- `ExternalTurnRequest` gains a `tools` field. A backend test that builds the
  request by hand sets `tools: ExternalToolDispatch::disabled()`.

### Fixed

- A tool call cancelled, or past its deadline, while a tool-output processor
  ran no longer loses its result. The real result, or the cancellation result,
  commits once and `ToolCallCompleted` precedes the terminal event. A
  cancellation result skips the remaining processors.
- A child outcome the model already read through a host delegation tool is no
  longer delivered again by the automatic child-completion turn. The tool
  acknowledges the outcome against its tool call; once that call's non-error
  result commits to the parent's history the outcome leaves the
  automatic-delivery projection and the parent snapshot is saved. An error
  result, or a turn that ends before the result commits, leaves delivery in
  place. The ledger keeps the outcome for inspection.
- Reasoning is now omitted from a request by one rule, applied in one place
  as the request is planned. Reasoning committed to canonical history records
  the provider and model that produced it, and a request omits a reasoning
  part when that producer is another provider or model, or when the part is
  unsigned and belongs to an earlier turn. Reasoning with no recorded
  producer is never treated as foreign: signed it is sent as before, unsigned
  it is dropped once its turn has ended. This replaces the separate step
  that dropped earlier unsigned reasoning before planning. Canonical history
  keeps every part, so LCM entries and checkpoint fingerprints are untouched,
  and an assistant message left empty is never sent as an empty message.
  Gemini accepts unsigned tool calls from earlier turns and still requires a
  signed thought in the active continuation.
- Image parts in a tool result are bounded by their estimated token cost
  rather than the length of their URL, so a large inline image that fits the
  output limit is no longer dropped.
- The release lock resolves `rustls` 0.23.46, clearing RUSTSEC-2026-0346,
  -0347, -0348 and -0349.
## [0.2.1] - 2026-10-08

### Fixed

- An LCM session whose turn was killed between provider steps can be resumed
  again. Each provider step appends to the LCM timeline, but a host with a
  SessionStore and no CheckpointStore saves the session when the turn ends, so
  a process that died mid-turn left the store several appends ahead of the
  saved session. Resume accepted only the exact checkpoint, a pending summary
  successor or one canonical append, and refused such a session on every later
  turn with `LCM DAG revision no longer matches its protected checkpoint`.
  Resume now drops that tail when the checkpoint has no pending summary, the
  summary DAG is exactly the one it recorded and every entry below its
  frontier still matches canonical history. Summary nodes are not changed, and
  any other disagreement is still a conflict. A store without `truncate_from`
  keeps the old behavior.

## [0.2.0] - 2026-10-08

The first tagged release since `v0.1.0`. That tag was cut without a changelog
section of its own, so this section also lists what `v0.1.0` contained.

### Fixed

- The deterministic LCM fallback no longer stores text a model can copy as a
  reply. When both summary attempts were rejected, the fallback kept a cut of
  the source that rendered tool calls as `assistant: call name(args)`. That
  text is later shown to the model as its own earlier message, and a session
  holding many fallback nodes started answering with the literal text
  `call shell({...})` instead of calling the tool. The fallback now renders a
  tool call as `[earlier tool call: name args]` and a result as
  `[earlier tool result: name]`, each on its own line, leaves reasoning out,
  and begins with `[excerpt of earlier conversation, not a summary]` when that
  fits its target. Tool names, capped arguments and capped results are kept.
  `LCM_ALGORITHM_REVISION` is now `agent-runtime-lcm-3` and the default
  deterministic algorithm revision is `lcm-deterministic-head-tail-tools-3`,
  so persisted LCM state rebuilds its derived metadata once on resume.
  Fallback nodes already stored keep their old text. `render_summary_source`
  is unchanged.
- A turn that fails in the middle of a tool step no longer leaves its tool
  calls unanswered in canonical history. The model response requesting them is
  already canonical at that point, so the session was rejected as
  `invalid_pairing` on every later turn, and a host saving the session carried
  that across restarts. Each unanswered call is now closed with an explicit
  error result and is never replayed: at the failure for a host with no
  CheckpointStore, and when the interrupted checkpoint is finalized on the next
  admission for a checkpointed host. Sessions already saved in that state are
  not repaired.
- LCM hard admission with a SessionStore and no CheckpointStore now saves its
  Sensitive pending response and full canonical snapshot before the summary DAG
  CAS. Resume validates and discards an uncommitted intent or adopts its exact
  committed successor without duplicate nodes. Finalized successor state is
  saved before provider admission, and canonical response history is saved
  before terminal LCM append. A staging save failure refuses provider admission. Checkpointed and storeless hosts retain existing behavior;
  no public production API, schema version or store migration changes.

### Breaking

- U.1: `RuntimeBuilder::build()` now rejects static workspace write scopes
  outside an explicitly configured workspace, returning `ErrorKind::Config`
  with `FailureClass::InvalidToolWriteScope(Box<ToolWriteScopeViolation>)`
  carrying tool name, rejected scope, and workspace root.
  Replace declarations such as `with_write("/")` with a scope the workspace
  contains, for example `with_write(workspace.root())` for workspace-wide
  access when the root is contained. Absolute contained paths remain valid;
  standalone registration and builds without a workspace are unaffected.
  Invocation checks remain in place for prepared, argument-dependent scopes.
- U7/U8 (explicit session and timeline lifecycle). Full consumer-break list:
  - `COMMAND_SCHEMA_VERSION` is 2. `StartSession` now requires explicit `mode`
    on the wire; `initial_history` becomes `seed`. Schema 1/unknown legacy
    fields are rejected. Use `create(id, seed)`, `resume(id)` or
    `ephemeral(history)`. `StartSession::with_id` is removed, so a former
    `new().with_id(id)` resume fails to compile; `new()` is an unnamed create.
    `StartSession::from_json(value)` decodes wire payloads and rejects another
    schema version with a typed `Config` error (plain serde decoding still
    fails with a serde error). Resume with any seed returns Conflict; missing
    resume state returns NotFound; create over an ordinary snapshot or protected
    checkpoint returns Conflict. Resume identity floors require resume mode.
    Checkpoint recovery policies remain.
  - Ephemeral disables session/checkpoint loading and writes even when stores
    were configured. Its LCM runs on a volatile in-memory timeline and never
    claims or writes the host timeline bound to its id. `SessionHandle::resumed()`
    reports actual persisted loading. Nyx's per-call history reconstruction maps
    to `ephemeral`.
  - `LcmWriter::claim` is defaulted. The default records nothing: it returns
    `Claimed` for an empty timeline and `LcmClaimResult::Unsupported` for a
    populated one. With it, LCM stays usable: sessions run normally, a session
    resuming its own snapshot continues unfenced, and a fresh binding of a
    populated timeline forks to the resolver's `new_timeline` with a summary
    seed (typed `ForkRequired` without that hook). Explicit Adopt of a fresh
    binding on such a store returns `ForkRequired`. Claim-aware stores must update ownership
    atomically, bump their revision on every ownership change, and reject a
    mutation whose `LcmView::owner()` fence is not the recorded owner (an
    unfenced write to an owned timeline included) with `TimelineOwned`.
    `LcmView` gains `with_owner`/`owner`; the runtime fences every write.
  - Fresh owners cannot silently bind populated timelines; select explicit
    Adopt, Fork or Retire. Adopt claims the next generation before it reads, and
    Adopt with a non-empty seed returns Conflict. A pre-U7 session on an
    unclaimed timeline fails resume with `TimelineOwned { owner: None, .. }`;
    `resume(id).with_lcm_policy(Adopt)` claims generation 1 and continues.
    Default resolver replacement and continuation hooks fail closed; hosts
    opting into rotations must implement durable, idempotent, authorized
    bindings. The Fork and Retire policies durably rebind through `new_timeline`
    before validating the replacement. NewTimeline requires an empty store. An
    unauthorized LCM view now fails at session construction, not first use.
  - The U7 Fork summary is capped by the new public
    `LcmCoordinatorPolicy::fork_summary_max_chars` (default
    `DEFAULT_FORK_SUMMARY_MAX_CHARS`, 16 384), keeps the newest tail, omits
    Secret-classified sources and passes the content guard. Exhaustive
    `LcmCoordinatorPolicy` literals must add the field.
  - `agent-runtime-lcm`: the `testing` module is now `memory` and always
    compiled, and the `test-support` feature is removed; import
    `agent_runtime_lcm::InMemoryLcmStore` or `memory::InMemoryLcmStore`. It
    remains a volatile reference store, not a persistence backend.
  - `LcmError` adds TimelineOwned, LcmDivergence and ForkRequired; update exhaustive
    matches. `RuntimeError` adds optional typed `lcm: Option<Box<LcmFailure>>`; add it to
    Rust literals/destructures; `lcm_failure()` borrows the evidence. RangeOverlap/EntryConflict now retain Conflict kind
    through provider admission, with typed evidence rather than Config strings.
    Below-frontier reconcile fails without truncation. Serialized errors remain
    additive and legacy/future optional LCM evidence is readable.
  - `Runtime::fork_session(ForkSession)` accepts Summary/FromIndex/Empty and
    NewTimeline/Continue. It requires idle durable sources and supersedes parents.
    The resolver hook, replacement emptiness, Continue claim support and the seed
    are validated before any pending intent is written. A later failure rolls the
    intent back while the successor owns nothing durable; otherwise retry the same
    request. `Runtime::abort_fork(parent)` clears an intent a crash left behind
    under the same rule. Default resume refuses pending or superseded parents.
    With LCM, Continue accepts only `Empty` or `FromIndex(0)` (its adopted history);
    a Summary or a later suffix returns Conflict. Use NewTimeline plus Summary for
    bounded topic rotation. Smith `/new` maps to an Empty fork; Forge
    genesis/handoff maps to a Summary/NewTimeline fork.
  - Protected adapters that restrict their first save to admission states must
    additionally accept `TurnCheckpoint::is_session_boundary()` for exact idle
    seed protection. The constructor uses existing Terminal wire data; checkpoint
    schema/transition versions and store trait signatures are unchanged. Ordinary
    Sensitive-state redaction and exact protected retention remain mandatory.
    Compatible exact immutable seeds/pending intents override even newer ordinary
    redacted values; generated U7 Fork seeds are protected before the first turn,
    on the create and the resume path.
    Persisted LCM state adds a defaulted claim generation; U6 tunable rebuilds remain.
  - Forge can remove V149 timeline retirement and stale-LCM-state deletion for
    tunable changes after adopting claims and authorized session/topic forks.
    See `docs/migrations/session-timeline-lifecycle.md` for migration and U9/U10.

- U6 (LCM working sets, one sizer, provider summaries). Consumer-break list:
  - `LCM_ALGORITHM_REVISION` is now `agent-runtime-lcm-2`; persisted LCM state
    from `-1` rebuilds its derived metadata once on resume (see below).
  - `LcmEscalationPolicy::target_tokens` is replaced by
    `leaf_source_target_tokens`, `summary_max_ratio` (default 0.25) and
    `min_reclaim_ratio` (default 0.5). Update struct literals or use
    `..Default::default()`. The struct lost `Eq` (ratio fields are `f64`);
    remove downstream `Eq` bounds. Serialized escalation settings have no
    serde default or alias: old JSON without the new fields fails to
    deserialize and must be migrated. `policy_revision` defaults to
    `lcm-summary-policy-2`.
  - Ratio enforcement: model summaries must meet `summary_max_ratio` and
    `min_reclaim_ratio` at every escalation level, so fewer model outputs are
    accepted than before. The deterministic fallback keeps the base target,
    `min(deterministic_token_cap, source_tokens - 1)`, and is not ratio-capped.
    At the defaults the 0.25 ratio is stricter than the 0.5 reclaim floor, so
    `min_reclaim_ratio` only binds when a host raises `summary_max_ratio` above
    `1 - min_reclaim_ratio`.
  - A leaf the deterministic fallback cannot strictly shrink (one short turn,
    smaller than the fallback's own framing) is widened through the next turn
    instead of wedging compaction (on the hard path up to the active turn;
    idle compaction never widens into the retained recent entries). A summary
    that still cannot fit under hard
    pressure now ends as the structured `cannot_fit` limit error (with
    `summary_attempts` and summary usage metadata) instead of an unstructured
    "cannot fit after escalation" limit.
  - `LcmSummaryAttemptOutcome::NonShrinking` is now reported before
    `OverBudget`: under ratio caps every non-shrinking output is also over
    budget, so the old order made `NonShrinking` unreachable.
  - `LcmPressurePolicy::max_rounds` defaults to 0 (was 3) and `validate`
    accepts 0, which derives the bound. The one shared formula is
    `derive_hard_rounds` (overage above the soft threshold divided by
    `expected_leaf_reclaim_tokens`, plus two), fixed once per admission epoch
    and never above `MAX_DERIVED_HARD_ROUNDS` (64). Positive values remain
    explicit limits. The structured `cannot_fit` error's `max_rounds` now
    reports the effective bound, not the configured 0. Added
    `decide_pressure_with_reclaim`, `derive_hard_rounds`,
    `expected_leaf_reclaim_tokens`, `MAX_DERIVED_HARD_ROUNDS` and
    `LcmEscalationPolicy::expected_reclaim_tokens`.
  - Default sizer: `LcmCoordinatorPolicy::default().sizer` is now the shared
    `RequestSizerAdapter` over the default request sizer, and
    `RuntimeBuilder::lcm` replaces exactly that default with the planner's
    `RequestSizer`. A host-supplied `LcmSizer` is kept as given; it is never
    discarded. With a host sizer and a `WorkingSetPolicy`, measured overhead is
    the planner total minus the host-sized history, so the host sizer should
    approximate the request sizer (or omit it to share). The LCM
    `CharRatioSizer` now counts all content parts (revision
    `char-ratio-content-2`), and non-default context `CharRatioSizer` settings
    get distinct revisions; the default revision is unchanged.
  - `RuntimeBuilder::lcm` registers the coordinator's history projector and
    turn-commit hook at `build()` instead of at call time, because the
    coordinator is rebuilt with the final request sizer and working set.
    Execution order is unchanged: the sealed pipeline orders components by
    declared constraints and then component id, never by registration order.
  - `LcmLifecycleReason` adds `OverheadExceedsTarget`. Exhaustive matches need
    the new arm, and readers of stored events must accept the new
    `overhead_exceeds_target` value. `LcmLifecycleMetadata` adds optional
    `overhead_tokens` and `target_tokens` (serde default, omitted when `None`,
    so stored events still read); struct literals without `..Default::default()`
    and exhaustive destructuring must include them.
  - Revision bump and tuning: LCM tunable policy, sizer, model and algorithm
    changes rebuild derived state from authorized active nodes instead of
    failing with a conflict. Timeline, binding, store schema, classifier and
    guard mismatches remain fail-closed. A protected pending response from
    older tunables is accepted only after source-identity validation against
    canonical history; one from current tunables is validated by recomputing
    its exact plan. No store migration or stale-state deletion is needed.
  - Persistence: private LCM state adds `fixed_overhead_tokens` and
    `hard_round_limit` (both `serde(default)`). Idle LCM batches write the
    redaction-safe extension namespace `runtime.lcm.idle_boundary` (revision
    `lcm-idle-boundary-1`, a fingerprint of the predecessor usage ledger) and
    refresh the exact terminal checkpoint; the namespace is kept, not removed.
    `merge_terminal_checkpoint_snapshot` now accepts a usage ledger skew when
    that marker proves the protected ledger extends the ordinary one with
    semantic-summary records only, and takes the protected LCM state when the
    ordinary copy has a different revision but the same identity fields.
    A failed exact idle checkpoint rolls back the batch's extension state,
    usage records and marker; once the exact checkpoint is saved, an ordinary
    save failure keeps the progress (the checkpoint is its saved counterpart).
- U6 additive API: `LcmCoordinator::with_summary_policy`,
  `ContextPlanner::with_input_cap` and `RunPlanner::with_input_cap`,
  `lcm::RequestSizerAdapter`, `lcm::summarize::render_summary_source`,
  `lcm::LCM_SUMMARY_PURPOSE` (now defined in the LCM crate and re-exported by
  `harness`), `LcmEscalatingSummarizer::can_always_summarize`, and the
  derived-rounds helpers listed above.
- Add `WorkingSetPolicy { target_tokens, hard_tokens }` through
  `RuntimeBuilder::working_set_policy`, measured `fixed_overhead_tokens`, and
  opt-in `soft_on_turn_boundary(bool)`. The planner enforces
  `min(resolved input, hard)`; pressure uses `target - measured overhead`, but
  never less than 25% of the target. When that floor applies the admission
  pass emits `LcmLifecycle { reason: OverheadExceedsTarget }` with the clamped
  thresholds and the new optional `LcmLifecycleMetadata::overhead_tokens`
  (measured overhead) and `target_tokens` (working-set target) fields. A persisted overhead is trusted only until the next plan
  re-measures it. Without a working set, existing budgets remain.
- Add feature `provider-summary` and `ProviderLcmSummaryModel<P: Provider>`
  (re-exported through `harness` and `lcm`) with host-supplied instructions,
  planner-admitted tool-aware map-reduce and aggregate usage.
  `ProviderSummaryLimits` sets a per-call deadline (default 60 s) and an
  operation ceiling (default 10 min); the operation budget is the per-call
  deadline times the calls planned so far, up to the ceiling.
  `with_operation_scope` supplies the host's cancellation and deadline per
  summary operation (`ProviderSummaryScope`), so cancelling one operation never
  poisons the next; `with_clock` replaces the clock. Calls are attributed
  `IdleCompaction` only for idle-boundary summaries, `Ordinary` otherwise.
  Deterministic fallback preserves bounded tool arguments and results. Forge
  can replace its private summarizer and tuning-state deletion; timeline
  ownership/V149 retirement still depends on U7.

See [`docs/migration-0.1.md`](docs/migration-0.1.md) for the full migration.

- `RuntimeError` adds `class`, `retry_after_ms`, `limit_resets_at_ms`, and
  `credential_recovery`. Rust struct literals must supply the new fields;
  exhaustive destructuring must include them or use `..`. Existing JSON stays
  readable in both directions, and context-overflow counts are `Option<u32>`.
  `FailureStage` and `FailureComponent` are non-exhaustive, and
  `FailureComponent` adds `Unknown`, so enum matches need a wildcard arm.
  Actual consumer builds remain a release gate.
- `CachePlan` and `RuntimeEvent::CachePlanChanged` add an optional
  `first_changed_fragment`. Rust struct literals must supply it (`None` is
  fine) and field-exact patterns must bind it or use `..`. JSON without the
  field stays readable, and it is omitted when unknown. The pre-hook tool
  error text now says "before tool normalization" instead of "before tool
  preparation".
- Session manifest retention is now a finite recent window (default 32
  planned steps). `snapshot.manifests` remains an ordered `Vec<TurnManifest>`;
  `RuntimeBuilder::manifest_window(NonZeroUsize)` and
  `SessionHandle::recent_manifests()` expose configuration and the same recent
  suffix. Hosts requiring lifetime audit/replay must archive records. New
  protected checkpoints omit diagnostic manifests and preserve exact execution
  state using a RedactionSafe planned-step boundary record. Legacy JSON remains
  readable; older binaries can reject a new terminal checkpoint paired with
  nonempty ordinary diagnostics. Roll-forward repairs an opaque pre-U3
  under-count when the longer legacy list still proves it, and nonterminal
  crash recovery retains diagnostics from a lagging ordinary snapshot. The
  namespace is exported as `runtime::MANIFEST_BOUNDARY_NAMESPACE`; additive
  fields in its value remain readable. See the retention and downgrade
  migration.
- The removed session-scoped rolling-summary contract is replaced by Lossless
  Context Memory (LCM). Hosts bind an authorized logical timeline and compose
  `LcmCoordinator`; when `.lcm` is configured, resume automatically imports
  valid schema-v1 state only when the coordinator has the legacy protected
  `ArtifactStore` and the runtime has a durable `SessionStore`, validates
  canonical history/artifact/binding identity, and persists the replacement
  before accepting turns. There is no public/manual restore alias or second
  semantic-compaction path.
- `RunManifest` is now manifest schema v2 with redaction-safe lossless LCM
  records and fingerprint semantics. `RunManifest::check_replay_as` returns a
  typed `ReplayMismatch` report covering revision, lossless-record, and
  assembled-context differences, even for a labeled non-equivalent replay;
  strict equivalent replay rejects every such difference. LCM equivalent
  replay must use `check_replay_with_lossless_context` or
  `check_replay_as_with_lossless_context` with the restored lossless records
  and assembled context fingerprint; the revision-only entry points do not
  establish LCM equivalence.
- The old idle semantic-summary API is replaced by
  `SessionHandle::try_idle_compaction()`, which returns metadata only through
  `IdleCompactionAdmission::Accepted { changed, fallback_reason, usage }`
  (or `Busy`/`Shutdown`). No summary body crosses the runtime facade.
- LCM integrations may attach a summary-body `ContentGuard`; its ID/revision
  is a strict checkpoint compatibility boundary and guarded historical state
  fails closed if the guard is removed. `SessionHandle::expand_lcm` provides
  bounded, read-only inspection through the coordinator's host-authorized
  timeline binding and emits metadata-only lifecycle events.

- `RuntimeBuilder::build()` now **requires** a resolvable model profile, via
  `model_profile(..)` or `model_catalog(..)`, and fails otherwise. There is no
  default context window: a runtime that cannot state its model's limits cannot
  enforce a budget, and guessing one is how uncounted context reaches a
  provider.
- Provider requests are derived from an immutable `ContextPlan` instead of being
  assembled from the system prompt, full history, and every registered tool.
  Every context-bearing field is counted before the request is sent, and a turn
  that cannot fit fails before any network I/O rather than at the provider.
- `agent-runtime-prompt` was folded into `agent-runtime-context` and removed;
  its `TokenEstimator`/`CharBasedEstimator` are superseded by `RequestSizer`.
- `Named`/`Registry<T>`/`Sealed<T>` moved to `agent-runtime-registry`. A `Named`
  impl on a foreign type (e.g. `Arc<dyn YourTrait>`) is now an orphan impl and
  needs a local newtype.
- Event `SCHEMA_VERSION` is now `15`. Since the registry-driven v2 baseline,
  tool-call argument projection, delegation, attempt-scoped streaming,
  metadata-only host interaction, lossless child `needs_input`, and
  durability-aligned `PlanUpdated`, durable-child recovery/resume, and
  attempt-attributed prompt-cache evidence each advanced the vocabulary.
  Provider `CacheObservation` read/write fields are now independently
  presence-aware, canonical observations carry request/attempt/cache-plan
  attribution, and `CacheStateChanged` reports the comparable expectation,
  provider observation, saturating shortfall, and confidence. Exhaustive
  provider/event matches must handle the new shapes. Legacy numeric cache
  observations remain readable without fabricating attribution or a miss.
  Version 14 adds the canonical cache-operation lifecycle; version 15 adds the
  metadata-only `LcmLifecycle` event for pressure, admission, escalation,
  commits, fallback, import, expansion, and failure. Golden fixtures retain
  the compatible v5-v11 and v13-v15 wire forms; pre-v5 unattributed output
  deltas are intentionally rejected.
- `SessionHandle::send` and `run` return `Result<TurnHandle, RuntimeError>`.
  `TurnHandle` owns turn-local interruption and completion; use
  `cancel_session` only for terminal session teardown. The compatibility
  `cancel` alias retains terminal semantics.
- `Tool` now separates `spec`, argument/resource `prepare`, and exact
  `invoke(PreparedToolCall, ..)`. `LegacyTool` remains as a conservative
  migration adapter, but cannot claim invocation-specific authority.

### Security

- Provider configuration Debug output now hides all Anthropic extra-header
  values while retaining header names and non-secret settings. Anthropic,
  OpenAI, Responses, HTTP requests, and catalog refreshers also redact URLs
  containing query strings, userinfo, or fragments, using the existing secret
  redaction. API keys and transport header values remain non-disclosing in
  both compact and pretty Debug output; Gemini was already redacted.

- Redact Anthropic in-stream error messages and Anthropic/OpenAI transport
  diagnostics; retain typed error classes and numeric retry hints. Gemini
  transport details and unknown terminal statuses no longer echo provider text
  into printable errors, events, metadata or Debug output. Responses already
  redacts these diagnostic paths.

### Changed

- Anthropic, OpenAI-compatible Chat Completions and Responses now map canonical
  tool names to deterministic, collision-safe 1–64 character wire aliases for
  definitions, named choices and history replay, and restore canonical names in
  model calls. Valid names and unknown model names remain unchanged. OpenAI
  name fragments are assembled before canonical emission when aliases are in
  use. Gemini Interactions retains its native name handling.

- History and LCM accounting do less repeated work per provider call.
  Private history captures share immutable generations instead of copying
  every message (about 4x fewer message copies per call in the benchmark),
  and LCM keeps authorized, revision-bound, process-local token totals so an
  unchanged or appended history only accounts for the new entries. Public
  owned history, request and checkpoint types, serde formats, persisted LCM
  revisions and fingerprints are unchanged. A DAG revision change between
  checkpoint and pressure accounting now fails the turn with the existing
  revision-conflict error instead of accounting against the newer DAG.
  Truncating the stored tail now drops the cached totals, so a later append
  cannot reuse counts for removed entries.
- Integration tests build as fewer binaries: the 20 `agent-runtime` test
  targets are one `integration` target and the four testkit conformance
  targets are one `conformance` target. Every case is kept; the consumer
  targets `consumer_open_forge`, `consumer_smith` and `consumer_nyx` keep their
  names. Run a moved case with `--test integration <module>::` or
  `--test conformance <module>::`.
- `ProviderAttemptFinished` now carries optional zero-based attempt position,
  configured attempt total, and the effective delay before an actually
  admitted retry. Legacy journals deserialize the fields as absent, while
  consumers can distinguish retryable classification from a scheduled retry
  without duplicating runtime backoff or deadline policy.
- The direct loop is a versioned, checkpointable turn machine. Mutable
  planning/cache/activation/extension state is session-owned, and completed
  turns are saved before `TurnCompleted` becomes the durable terminal
  boundary.
- Conversation classification no longer determines provider-wire placement.
  Chronology is preserved within one conversation lane, complete parallel
  tool exchanges are atomic, and the active-turn continuation is required
  during compaction.
- Deterministic structural compaction remains network-free. Persisted semantic
  history compaction now uses LCM's immutable timeline and transactional
  hierarchical summary DAG; committed active nodes are projected back through
  the authoritative context planner with lossless source pointers.
- Live ability routing derives a scoped view and activation epochs per
  session. `registry.search` stages an authorized, dependency-complete bundle
  transactionally and exposes it only after the canonical search result
  commits. Hosts attaching after session startup can inspect the current
  immutable epoch through `SessionHandle::activation_epoch`; live event
  subscriptions cover events emitted after subscription, while persisted
  journals remain authoritative for earlier events and delivery gaps.
- A valid persisted provider-cache baseline is now discarded and rebuilt when
  a resumed session changes model profile or provider cache contract; malformed
  or unknown cache-state schemas still fail closed. Ready terminal hooks can
  record an explicit cancellation without converting it into a failed turn,
  while pending hooks remain cancellation- and deadline-bounded.
- Split the monolithic `agent-runtime` crate into focused, single-responsibility
  crates so consumers (Nyx, Open Forge, Smith) can depend on just the mechanism
  they need. Provider adapters moved from `agent-runtime::provider` into the new
  `agent-runtime-provider` crate; `agent_runtime::provider::*` paths still
  resolve via a re-export, so this is source-compatible.
- The tool registry is now a thin, schema-validating specialization of the
  shared `agent-runtime-registry` collection mechanism, held via a local
  `Named` wrapper; its public API is unchanged.
- Generic registry primitives (`Named`, `Registry<T>`, `Sealed<T>`) moved from
  `agent-runtime-ability` into `agent-runtime-registry`, which owns every
  registry mechanism now; `agent-runtime-ability` re-exports them for
  compatibility. `agent-runtime-ability` is now descriptor-first: bounded
  `AbilityDescriptor`s with affordances/dependencies/conflicts/readiness/risk,
  and lazy policy-checked activation, built on the registry kernel's
  namespaced `RegistryId` identity.
- Folded the standalone `agent-runtime-prompt` crate into `agent-runtime-context`
  before its first release, so the workspace has exactly one token-budget and
  provider-context assembly path. `SystemPromptBuilder::into_fragments` turns
  named prompt sections into versioned `ContextFragment`s (revision, priority,
  and cache class carried through to the authoritative `ContextPlan`); the
  standalone crate's separate `TokenEstimator`/`CharBasedEstimator` was
  dropped in favor of `agent-runtime-context`'s `RequestSizer`/`CharRatioSizer`.

### Added

- Cache diagnostics: `first_changed_fragment` names the first plan segment
  (in plan-segment order) that differs from the committed predecessor,
  including a removed segment, without changing fingerprints, cache
  identities or provider requests.
- `Tool::normalize_arguments`: an identity-default hook that runs before full
  schema validation in both executor paths and on approval edits, so a host
  can accept a model's wrapped arguments while advertising the canonical
  schema. Edited arguments are re-normalized and re-authorized; checkpointed
  prepared actions resume without normalization. For opt-in tools the
  execution-facing call carries the normalized arguments; history keeps the
  raw ones.
- The sealed tool registry reuses each tool's compiled validator instead of
  recompiling it on every call.

- Neutral `FailureClass`, `FailureStage`, and fixed `FailureComponent`
  evidence on runtime errors. Legacy, future, and malformed classification
  evidence cannot make a record unreadable; stages/components have `Unknown`
  fallbacks, invalid optional timing/recovery evidence becomes absent, and zero
  remains explicit. Delegated child failures retain all four evidence fields.
  Provider conversion preserves timing and credential recovery, while private
  request construction retains planner/harness evidence and LCM typed or
  genuine concurrent-revision classifications with the existing coarse kind,
  retryability, error channel, and terminal outcome.
  Classification and timing hints never admit a retry. Frozen error/event/
  child-failure fixtures and all three neutral consumer suites cover the
  contract.
- External agent capability injection: `ExternalCapabilities` (skills, MCP
  servers, tool allowlist, runtime tools) attached with
  `RuntimeBuilder::external_capabilities` and delivered on every
  `ExternalTurnRequest`. Feature `external-agent-bridge` serves the session's
  runtime tools to the CLI as a turn-scoped loopback MCP server whose calls go
  through ordinary authorization and approval.
- `agent-runtime-agent-cli`: `ClaudeCodeBackend` (`claude` feature) and
  `CodexBackend` (`codex` feature) realize those capabilities with each CLI's
  launch-scoped flags, never touching global CLI config. See
  `docs/external-agents.md`.
- Responses adapters now forward bounded model-advertised reasoning efforts,
  including Codex `xhigh`, `max`, and `ultra`, instead of imposing the legacy
  `low`/`medium`/`high` allowlist.
- `agent-runtime-provider/command-provider`, an opt-in process-bounded
  implementation of the canonical provider contract for trusted,
  consumer-owned model CLI codecs. It provides exact capability validation,
  shell-free direct argv, canonical executable/cwd resolution, a cleared and
  explicit child environment, redacted config/attempt diagnostics, bounded
  stdin/stdout/stderr, typed machine-output decoding, explicit compatibility
  probing, and cancellation/deadline/drop-safe process-group cleanup. Runtime
  retains canonical history, tools/MCP, approvals, retries, and events; named
  Codex/Claude/other adapters and Smith configuration remain consumer work.
- `agent-runtime-cli`, an isolated reference host that installs the
  `agent-runtime run` command for one in-process turn. It supports the
  first-party provider adapters, explicit model limits, positional or piped
  prompts, credential-by-environment configuration, assistant-text or
  canonical JSONL streaming, exact-origin HTTPS transport with restricted
  address and redirect denial, Ctrl-C interruption, and stable process exits.
  It now accepts one explicit strict version-1 TOML file for existing run
  defaults and trusted local stdio MCP definitions. Static inspection,
  per-run server consent, exact per-run tool approval, minimal child
  environment mapping, conservative tool authority, optional/required failure
  handling, and bounded connection shutdown keep a file from authorizing its
  own process or tools. The CLI package declares Rust 1.88 for the official MCP
  SDK while embeddable packages retain Rust 1.86. Interactive chat,
  persistence, remote MCP/OAuth, ambient config discovery, daemons, and
  consumer policy remain outside this command.
- `agent-runtime-lcm`, a store- and provider-neutral package for immutable
  logical timelines, transactional leaf/condensed summary DAGs, deterministic
  tool-exchange-safe planning, bounded expansion, soft/hard pressure decisions,
  and three-stage convergence-guaranteed summarization. The runtime facade
  re-exports it as `agent_runtime::lcm`.
- Native stateless OpenAI Responses provider, first fixture-verified against
  xAI Grok: bounded input-item encoding, session-keyed implicit prompt caching,
  encrypted reasoning replay, function-call streaming, structured output,
  usage/cache normalization, terminal fencing, and renewable bearer
  credentials. Provider-side storage, background responses, and hosted tools
  remain rejected before I/O.
- Native Google Gemini Interactions adapter over injected `HttpTransport`, with
  stateless `store=false` history replay, renewable `x-goog-api-key`
  credentials, bounded native request/stream types, function and multimodal
  result translation, structured output, usage/cache normalization, and exact
  signed-thought continuation. Vertex AI, hosted tools, provider storage, and
  live-network tests remain outside the shared runtime.
- Host-injected renewable provider credentials through
  `ProviderCredentialSource`, with optional lease expiry, opaque exact-revision
  invalidation, static API-key compatibility, cancellation/deadline bounds,
  and one attempt-visible pre-output authentication recovery replay. OAuth
  ceremony and credential persistence remain host policy, and credential
  material is excluded from runtime observability and persistence.
- Typed active-turn steering: `SessionHandle::steer_current_turn` admits
  bounded FIFO `UserInput` against an optional expected `TurnId`, returns a
  stable `SteerReceipt`, and retains caller input in structured rejection.
  Inputs commit only at protected provider/tool boundaries and continue under
  the same logical turn; metadata-only `TurnSteerCommitted` and
  `TurnSteerDiscarded` events make disposition explicit without exposing raw
  content. Atomic drain-or-close prevents acceptance after a terminal fence,
  while cancellation discards before `TurnCompleted`.
- `GoalAdmissionGate` lets an interactive host defer idle-only automatic goal
  continuation while process-local real-user work is pending. It does not
  interrupt or pause an already-serving goal turn.
- Protected `CheckpointStore` records for accepted input, assembled model
  responses, pending approvals/interactions, raw tool outcomes, every
  canonical tool result, and terminal publication. Recovery never implicitly
  replays an indeterminate provider call or tool side effect.
- Invocation-specific prepared authority: canonical arguments, exact
  `SecurityResource`, typed permission bounds, scheduler effects, approval
  display, and a preparation fingerprint all describe the same immutable
  action. Edited approval input restarts preparation and authorization.
- Phase-specific ordered harness contracts for tool views, context, history
  projection, model options, tool output, and turn commits. Components receive
  immutable views, return explicit patches, and are bounded by turn
  cancellation/deadlines.
- Standard harness components: typed checkpointed todos with `PlanUpdated`,
  descriptor-first lazily verified skills, bounded memory contribution,
  session-private artifact offloading plus authorized paginated
  `artifact.read`, structured questionnaire interaction, and Lossless Context
  Memory coordination. Persistent goals add
  descriptor-first `get_goal`/`create_goal`/`update_goal`, optimistic typed
  host controls, provider-evidence accounting, and a process-scoped conditional
  continuation controller with no synthetic user history.
- Lossless delegated task outcomes, including typed child `needs_input`
  handoff without a root broker, deterministic multi-child delivery, and
  follow-up reuse of the same child session.
- Agent delegation (`add-agent-delegation-runtime`): a neutral
  `DelegationCoordinator` spawns children as full runtime sessions built by a
  host `ChildRuntimeFactory`, with spawn/list/follow-up/wait/result/stop
  addressed by stable `ChildId`. Depth-one is enforced fail-closed (child
  views lose delegation tools; a child session cannot construct a
  coordinator), spawn/follow-up/stop pass the composed authorization path
  under the host-covered `agent.delegate` permission, per-parent and shared
  capacity are reject-by-default with an explicit queue policy. Hosts that do
  not provide both child stores retain process-ephemeral behavior. With both
  stores, bounded parent-owned records retain stable child/session identity,
  cumulative limits, policy fingerprints, and safe checkpoint watermarks;
  restored children remain dormant until an explicit new-turn `follow_up` or
  exact-checkpoint `resume`. Unsafe in-flight provider checkpoints fail closed,
  competing in-process coordinators are rejected, and host lifecycle leases
  remain the cross-process boundary. The provider-free `recover()` pass
  reconciles a protected child checkpoint newer than its parent catalog after
  abrupt process loss before child commands are accepted. Returned child
  questionnaires live in protected extension state and can be re-queued after
  restart without provider work. Attributed child
  lifecycle events (`ChildSpawned` … `ChildFailed`) join the event vocabulary
  (`SCHEMA_VERSION` is now `10`); the completed event carries the child's
  final result so coalescing can never drop it.
- Event schema v10 adds metadata-only `InternalTurnStarted` and
  durability-aligned `GoalUpdated` projections. Checkpoint schema/revision v2
  records attributed internal accepted input while retaining ordinary user
  turn compatibility.
- Event schema v11 adds metadata-only active-turn steering dispositions and a
  persisted steer identity floor. Existing snapshot reads default the new
  counter safely; consumers matching `RuntimeEvent` exhaustively must handle
  both disposition variants.
- Safe-boundary content injection: `SessionHandle::inject` queues bounded
  host content (`RuntimeBuilder::injection_queue_limit`, default 64) that the
  driver introduces only at provider/tool boundaries — never mid-stream —
  with structured overflow for coalescable items and guaranteed delivery for
  must-deliver items (e.g. final child results).
- Testkit: a delegation conformance suite (lifecycle ordering, depth
  rejection, fail-closed coverage, capacity, scoped views, stop/teardown
  cancellation propagation) and safe-boundary injection integration tests.
- Reasoning preservation: the driver retains streamed reasoning as
  `ContentPart::Reasoning` history parts for the turn that produced it
  (merging consecutive same-`redacted` deltas, placed ahead of visible text
  and tool calls), sheds prior-turn unsigned reasoning when the next user turn
  starts, and retains signed provider continuation—including signature-only
  blocks—across serialization and replay. The OpenAI-compatible adapter serializes non-redacted reasoning as
  `reasoning_content` on assistant wire messages — required by
  OpenAI-compatible thinking models (e.g. Z.AI GLM) during tool-call
  continuations — and never serializes redacted reasoning. Compaction strips
  prior-turn unsigned reasoning as its cheapest first stage but never
  truncates signed continuation content.
- `ContextPlanned` gains `input_tokens` (the counted consumption,
  `serde(default)` for journals written before the field existed) and
  `ContextPlan::input_budget()` exposes the enforced budget.
- `TurnCompleted` gains `visible_output`: `false` flags a reasoning-only
  completion so hosts can react instead of showing nothing. Serialized only
  when `false`; ordinary turns and old journals keep the previous wire shape.
- `ContentPart::Reasoning` gains an optional `signature` for providers that
  sign thinking blocks; absent from the wire when unset, and dropped by
  tool-output truncation whenever the signed text is altered.
- Provider conformance now covers reasoning: adapters must normalize
  streamed reasoning identically and accept continuation requests carrying
  reasoning history back (`assert_normalized_reasoning_stream`), and the
  OpenAI adapter's wire echo of `reasoning_content` is asserted end to end.
- `agent-runtime-registry`: the dependency-light registry kernel — namespaced
  `RegistryId`/`RegistryDomain` identity, `RegistryRevision`/`RegistrySource`/
  `EntryProvenance`, layered sealing with deterministic conflict/override
  rules, bounded searchable `RegistryCard`s, scoped `RegistryView`s, stable
  `Fingerprint`s, and the generic `Named`/`Registry<T>`/`Sealed<T>` collection.
  Std-only by default; `serde` adds (de)serialization.
- `agent-runtime-provider`: the provider mechanism (injectable HTTP transport,
  SSE normalization, OpenAI-compatible adapter, deterministic fake, and the
  retry/backoff classifier) as its own crate depending only on
  `agent-runtime-core`.
- `agent-runtime-context`: the authoritative context engine — versioned
  `ContextFragment`s, complete provider-wire token accounting via
  `RequestSizer`/`CharRatioSizer`, structural compaction, cache-aware planning,
  and the immutable `ContextPlan` that is the exclusive source of provider
  messages/tools/reserves/counts. Includes the folded-in composable
  system-prompt mechanism (`SystemPromptBuilder` and its section types).
  Deterministic and network-free; semantic summarization is coordinated above
  it by the runtime harness.
- `agent-runtime-obs`: an observability facade over the neutral event envelope —
  an async `EventSink` trait, `FanoutSink`, a `SinkObserver` bridge onto the
  runtime's observer hook, a `drive` pump for the async event stream, an
  `ObsRow` SQL projection, and feature-gated `CliSink` (default), `FileSink`
  (JSONL), and `SqliteSink` (opt-in) sinks.
- `agent-runtime` re-exports `registry`, `ability`, `provider`, and `context`
  directly, and `obs` behind an opt-in feature, for one-stop consumption.
- Rust 2024 workspace with `agent-runtime-core`, `agent-runtime`, and
  `agent-runtime-testkit` (minimum supported Rust version 1.86).
- Host-neutral core contracts: neutral IDs, messages/content, structured
  errors, cancellation, deadlines, redaction-safe metadata, versioned events,
  and disjoint usage counters with per-counter provenance.
- Host adapter traits: `Provider`, `Tool`, `ApprovalPolicy`, `Workspace`,
  `SessionStore`, `SecretStore`, `EventObserver`, and `Clock`.
- Provider runtime: capability/model descriptors, normalized requests, typed
  streaming events, a deterministic fake adapter, a configurable
  OpenAI-compatible adapter over an injectable HTTP transport, and an
  attempt-recording retry wrapper.
- Tool + agent execution: deterministic tool registry with name-conflict
  validation, fail-closed approval and workspace enforcement, side-effect-aware
  scheduling, and one canonical direct provider/tool loop with configured
  limits.
- Embeddable runtime facade: `RuntimeBuilder`, `Runtime`, and `SessionHandle`
  with injected host services, versioned commands/events, concurrent
  subscribers, cancellation propagation, and bounded shutdown.
- `agent-runtime-testkit`: fake clock, event recorder, temporary workspace, and
  reusable conformance suites (provider, tool, runtime, cancellation,
  event-schema, shutdown) plus neutral consumer adapter fixtures.

### Fixed

- Anthropic preserves non-empty initial tool input when no JSON deltas arrive;
  any JSON delta supersedes initial input. Compatible-gateway `[DONE]` stream
  endings are tolerated, and image data URIs with an empty media type or
  payload fail as BadRequest before transport I/O, including tool-result images.
- OpenAI-compatible JSON error envelopes now preserve authentication,
  rate-limit/quota, and invalid-request classifications instead of always
  becoming retryable server errors. Pre-output authentication failures
  invalidate the exact credential revision once; errors after semantic output
  do not trigger credential recovery. Rate-limit retry hints are retained,
  unknown errors remain server failures, and provider bodies stay redacted.
- LCM hard compaction no longer wedges a session behind one oversized agentic
  turn. Leaf planning only cuts at user boundaries, so a single turn whose
  tool loop outgrew `leaf_target_tokens` (many tool rounds, no user message
  inside) could never be planned: every admission backed up to the frontier
  and failed with "LCM context cannot fit after bounded hard compaction",
  forever. The same wedge hit a turn that reached into the
  `retain_recent_entries` tail. When the oldest raw turn cannot be cut, it is
  now taken whole as one oversized leaf.
- A tool call that breaks a registered tool's schema is now answered instead
  of fatal. The stream boundary rejected it as a malformed stream, which
  failed the whole turn, while the executor -- which already validates the
  same arguments -- would have returned a canonical tool error naming the
  offending property. A model that makes the mistake deterministically (an
  extra discriminator property on every attempt, say) could never get past
  its first tool call. Arguments that are not an object at all, where the
  schema requires one, remain a malformed stream.
- Shedding a prior turn's unsigned reasoning is now a projection onto the
  model-facing request instead of a rewrite of canonical history. Rewriting
  history between turns diverged it from the immutable LCM entries and the
  protected checkpoint that fingerprinted them, so the second turn of any
  session whose provider streams unsigned reasoning (every OpenAI-compatible
  thinking endpoint, z.ai GLM among them) failed closed with "LCM canonical
  history no longer matches its protected checkpoint" and stayed wedged.
  Messages are projected one for one, so an assistant message that carried
  nothing but shed reasoning arrives empty; the OpenAI-compatible wire drops
  it rather than sending a blank assistant line.
- The release lock now resolves `rustls` 0.23.45 and `rustls-webpki` 0.103.15,
  clearing RUSTSEC-2026-0285 before publication.
- `ContextPlanned::input_budget_tokens` now reports the enforced input budget
  it was always documented as, instead of the counted consumption (which
  moved to the new `input_tokens` field).

### Provenance
- Reusable provider, agent-loop, and tool mechanisms were adapted from the Nyx
  project. See `PROVENANCE.md` for the donor revision and path mappings.
