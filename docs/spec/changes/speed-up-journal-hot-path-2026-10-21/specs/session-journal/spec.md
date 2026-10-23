## ADDED Requirements

### Requirement: Incremental exact journal preparation

For supported warmed transitions, the runtime writer and engine SHALL prepare
candidate state, manifests, canonical encodings, exact memo proofs and protected
checkpoint evidence from changed immutable nodes and affected paths, sharing
unchanged state as specified in design P1. They MUST preserve schema-4 roots,
protected exactness, execution/recovery checks and atomic fenced publication;
unprovable or unsupported protected deltas SHALL use full validation.

#### Scenario: A fixed append into different history sizes

- **GIVEN** validated A/B bases of N=1000 and 4000 with the same bounded append and checkpoint update
- **WHEN** the writer prepares and commits each update
- **THEN** unchanged history, usage and manifest prefixes are not traversed or copied by targeted preparation
- **AND** work counts and paired release times satisfy bench.md P1 acceptance
- **AND** the resulting canonical roots, bytes and materialized protected state equal the complete-validation oracle

#### Scenario: A retained manifest or cache path is removed

- **GIVEN** a graph with a rolling manifest window and multiply referenced immutable subgraphs
- **WHEN** a transition removes a reference and adds its replacement
- **THEN** its exact proof equals the full tracer's set, including removal of nodes with no remaining path
- **AND** shared nodes remain reachable without retaining every superseded proof or semantic version

#### Scenario: A typed immutable edge is reused under a different expectation

- **WHEN** an update references cached bytes under a new expected kind, shape or depth
- **THEN** the runtime runs the required typed validation and rejects invalid references
- **AND** an active node or cache hit cannot suppress that validation

#### Scenario: A superseded cache body lost membership

- **GIVEN** a body retained only in cache was released from the session
- **WHEN** a later commit references it outside its validated base's exact closure
- **THEN** the engine rechecks membership or explicitly re-admits newly supplied bytes
- **AND** absence conflicts rather than publishing a head over a missing body

#### Scenario: Protected delta cannot prove submitted roots

- **WHEN** an advisory protected delta cannot reconstruct the submitted canonical roots or checkpoint evidence
- **THEN** the existing complete protected validator decides acceptance
- **AND** invalid execution state, missing objects and recovery barriers still fail without external work being replayed

#### Scenario: Preparation fails or acknowledgement is lost

- **WHEN** a policy, validation, generation fence or pre-commit failure cut rejects a transition
- **THEN** published state and its validated cache version remain unchanged
- **AND** a lost post-commit acknowledgement follows retryable replay semantics and never publishes an unacknowledged speculative proof

#### Scenario: Incremental charge on a normal worker stack

- **WHEN** JSON floats including negative zero, key ordering, optional values and deep bounded trees update a memo entry
- **THEN** incremental canonical length and conservative charge equal their complete counting oracles
- **AND** debug and release execution on a normal 2 MiB Tokio worker completes without a recursive sizing or drop stack overflow

### Requirement: Incremental ordinary projection is explicit and equivalent

Ordinary journals SHALL retain full policy projection as the default. An
optional revisioned incremental policy contract MUST produce the same semantic
policy result as complete projection over the previous projected base plus
the delta; the engine SHALL validate exact fields, remap references and encode
only projected admitted objects before publication.

#### Scenario: A policy only implements complete projection

- **WHEN** an existing policy or closure has no incremental capability
- **THEN** it continues to receive the complete ordinary candidate
- **AND** no append-only assumption is inferred from a cache hit or unchanged prefix

#### Scenario: A policy can change old content

- **GIVEN** a policy whose new input can redact or rewrite an older message
- **WHEN** incremental equivalence cannot be established under its declared contract
- **THEN** it requests complete projection
- **AND** the resulting state equals the existing complete-policy path

#### Scenario: A conservative policy opts in

- **WHEN** the benchmark conservative policy projects a warmed delta
- **THEN** differential tests prove equality with complete projection
- **AND** identity, ordered counts, execution counters, structured manifests and runtime previous-cache exactness retain their current checks
- **AND** raw sensitive drafts do not enter ordinary storage on either success or policy failure

#### Scenario: Policy configuration changes

- **WHEN** policy identity or revision changes
- **THEN** old incremental projection evidence is invalidated
- **AND** the next commit establishes an equivalent fresh policy base before reusing it

### Requirement: Collection authenticates and reuses exact validated proofs

Collection SHALL reuse current/pinned exact closure proofs only after
authenticating their current-head or head-bound durable-pin authority and
checking the mark epoch and full session fence. Roots without valid evidence
MUST use full typed membership-scoped tracing; exact membership inventory,
retirement dependencies, live leases, staging and transfer retention remain
part of the complete live union.

#### Scenario: Current and certified historical roots are warm

- **WHEN** collection authenticates the same current head and certified durable pins as its validated proofs
- **THEN** those roots require zero old-body closure fetches and zero old-edge revalidation
- **AND** collection results and receipts equal full tracing while satisfying bench.md P2 acceptance

#### Scenario: Historical pin is released and recreated

- **GIVEN** a warm proof for a root that was current when its old pin was created
- **WHEN** that pin is released, membership is collected and a historical lease recreates the pin
- **THEN** pin presence alone cannot reuse the old proof or restore its head-bound certificate
- **AND** membership-scoped fallback observes missing data with warm/cold parity

#### Scenario: Head or epoch changes during collection reads

- **WHEN** the selected head, retirement state, pin certificate, root/pin epoch or full session fence changes during authentication or before release
- **THEN** collection discards or retries the stale mark before a destructive transaction
- **AND** live membership and newly protected staged/transfer roots remain readable

#### Scenario: Proof is unavailable

- **GIVEN** a cold engine, zero/tiny memo budget, evicted proof or uncertified pin/staged root
- **WHEN** collection computes liveness
- **THEN** it fully traces and validates that portion with existing limits and error behavior
- **AND** proof absence affects speed rather than retention correctness

#### Scenario: Authority metadata is unreadable

- **WHEN** collection cannot authenticate a session or dependency with a non-retryable non-conflict failure
- **THEN** existing session isolation, sorted unreadable ids, conservative dependency blocking and retry hints remain in force
- **AND** cached graph evidence cannot turn unreadable authority into permission to release

#### Scenario: Many certified roots share a bounded collector

- **WHEN** more than sixteen certified roots are reused in one mark
- **THEN** every proof layer retains its original immutable evidence without a copied member-set layer
- **AND** cursor charge covers the layer references and proof handles while memo reservations cover proof retention

#### Scenario: Reader and writer-renew churn during a mark

- **WHEN** a reader opens or closes during Pins, Trace or Inventory, or a writer renews with an evicted memo for the same stored head
- **THEN** unchanged working proof and already shared layers retain mark progress
- **AND** added lease protection is checked at pre-apply and filtered before every destructive batch

#### Scenario: Proof retention cannot fit

- **WHEN** outstanding reservations or protected current-head entries prevent a proof reservation
- **THEN** reuse declines before evicting any memo entry
- **AND** collector lookups do not change LRU priority and unresolved cached summaries take the stored-metadata cold path

#### Scenario: Live membership is incomplete

- **WHEN** any independently counted shared live layer is not a subset of session membership
- **THEN** collection releases no member from that session and reports its id in unreadable_sessions
- **AND** the session remains dirty for a later host pass without requesting an endless current-pass retry

### Requirement: Writer initialization consumes an authority-checked validated view

If the deferred optional P3 phase is built, the runtime SHALL use an available
optional canonical validated writer view
to initialize semantic state,
checkpoint and encoding without materialize-then-re-derive work. The operation
MUST preserve final D-1 authority and lease checks, domain/session/root identity,
ordinary projected-id separation, native-first open and the complete reader
fallback for absent or unsupported views.

#### Scenario: A new writer selects a warm native root

- **WHEN** a replacement Runtime opens the latest writer on the same shared engine
- **THEN** its valid view initializes writer state with zero full root-derivation passes and zero unchanged body reads
- **AND** full warm Runtime resume satisfies bench.md P3 acceptance, including any owned restore materialization
- **AND** the final authority check and lease-liveness check still execute

#### Scenario: Cached view becomes stale before return

- **WHEN** stored head or certified-pin authority changes while a writer/reader view operation is assembling its result
- **THEN** that speculative result is discarded and the membership-scoped retry/conflict path runs
- **AND** writer-open fencing is not substituted for the final read authority check

#### Scenario: Ordinary representation differs from exact writer candidates

- **WHEN** a writer initializes from an ordinary view and later receives exact unprojected input
- **THEN** only ordinary projected ids seed ordinary known-state reuse
- **AND** a representation or prefix mismatch takes the existing replacement/fallback behavior without treating ordinary ids as protected draft ids

#### Scenario: Backend declines the view

- **WHEN** a custom journal, noncanonical representation or unavailable validated view returns no fast view
- **THEN** the existing reader/materialization path remains correct
- **AND** failed initialization closes its local lease without leaking writer authority

#### Scenario: Cold restart or import

- **WHEN** a native root or a legacy source is opened without reusable validated evidence
- **THEN** cold graph/checkpoint validation and exact import visibility/recovery ordering still execute
- **AND** published native state never falls back to stale legacy payloads

### Requirement: Collection yields and resumes within a cooperative work budget

Collection SHALL bound discovery, marking, inventory, dependency indexing,
release and maintenance units and MAY yield with no releases. It MUST preserve
a private charged cursor across calls, discard stale speculative marks, retain
dirty state until completion and satisfy the call/gate and full-drain acceptance
in bench.md P4 under the named fixture/storage conditions.

#### Scenario: A large live graph has no garbage

- **WHEN** a bounded call exhausts its marking or inventory budget before any release
- **THEN** it yields with explicit progress and has_more rather than performing an unbounded graph walk
- **AND** no membership is released before the complete authenticated live union is known
- **AND** every call and actual gate hold is below 1 s in the release acceptance runs, targeting below 250 ms

#### Scenario: Mark spans several calls

- **GIVEN** positive work/release budgets and sufficient configured cursor capacity
- **WHEN** quiescent collection resumes with an unchanged epoch and session fence
- **THEN** it advances its frontier/page position without restarting at the first dirty session
- **AND** a completed drain equals the reference retained state and cumulative acknowledged physical receipts

#### Scenario: A request carries a stale expected epoch

- **WHEN** expected_root_epoch does not match the engine epoch
- **THEN** the existing state-conflict result is retained without destructive work
- **AND** Restarted progress applies only to a stale private cursor under an otherwise valid fresh request

#### Scenario: Root ownership changes across a yield

- **WHEN** commit, open, retirement, staging, pin change, close or revocation invalidates a stored mark
- **THEN** the affected speculative mark is restarted before release
- **AND** old live sets cannot be rebased by merely replacing their fence
- **AND** close and revocation remain I/O-free

#### Scenario: A release batch succeeds and the process restarts

- **WHEN** the process loses its cursor after any acknowledged collection batch
- **THEN** durable dirty state and current fences allow safe rediscovery
- **AND** no unacknowledged release is assumed and no physical deletion receipt is counted twice

#### Scenario: Unreadable sessions consume part of a pass

- **WHEN** a session or dependency fails while a budgeted pass is running
- **THEN** it is attempted at most once per call and classified as in the collection-isolation contract
- **AND** its hint survives while independent sessions receive work in later units/calls
- **AND** retryable or domain-wide failures still abort without rolling back earlier acknowledged batches

#### Scenario: Gate wait consumes the soft budget

- **WHEN** collection cannot acquire the engine gate within its remaining cooperative budget
- **THEN** it yields without starting a new unit
- **AND** reports distinguish gate wait, actual gate ownership and whole-call elapsed time

#### Scenario: Atomic I/O exceeds a deadline

- **WHEN** an already-started atomic storage apply is delayed
- **THEN** the runtime preserves its acknowledgement/ambiguity semantics instead of cancelling it to manufacture a time-bound pass
- **AND** the measured call is recorded as an outlier/failure when it exceeds acceptance

#### Scenario: Cursor memory or request release allowance is exhausted

- **WHEN** the configured cursor workspace or max_objects release allowance prevents further work
- **THEN** collection releases no incompletely marked object and retains dirty state
- **AND** cursor eviction/restart and zero release allowance preserve existing release-counter semantics
- **AND** unchanged work that cannot fit the configured capacity returns typed unsupported with all content retained rather than succeeding in an endless restart loop

### Requirement: Storage collection units are paged and atomically bounded

A storage offering bounded collection SHALL provide fence-bound, byte/id-limited
metadata, pin/catalog, member and maintenance pages plus read-only bounded apply
preflight and atomic collection apply. It MUST NOT emulate paging by fully
materializing the unbounded result, silently apply a subset of a declared write,
or defer required purge work outside accounting to meet a timer.

#### Scenario: A session has many pins or a large opaque engine payload

- **WHEN** collection scans its authority and membership
- **THEN** storage returns authenticated bounded units with scope/fence-bound continuation
- **AND** the engine decodes/validates incrementally without an unbounded whole-session pins, members or metadata operation

#### Scenario: Storage must split a proposed release

- **WHEN** a proposed release would rewrite too many packs/segments for one declared unit
- **THEN** preflight requests a smaller batch without durable effect before beginning a transaction
- **AND** the engine retains exact selection of released ids and publishes each accepted batch atomically under the full fence

#### Scenario: Deferred bodies and replay packs require maintenance

- **WHEN** a release reaches a purge threshold or S5/base-repack work is pending
- **THEN** that work is included in bounded prepared units and exact physical receipts
- **AND** every acknowledged batch preserves the existing deferred-byte threshold, inclusive replay floor and current-writer last-operation exception
- **AND** dirty state is not cleared before required maintenance completes

#### Scenario: Stale page or plan is submitted

- **WHEN** storage/domain/session/fence/page kind differs from a continuation or apply plan
- **THEN** the operation rejects the stale or cross-scope token without effect
- **AND** the engine restarts authentication before selecting another release

#### Scenario: Sharing or a write failure affects a bounded batch

- **WHEN** a bounded batch releases shared membership or encounters an injected write failure
- **THEN** other holders stay readable under the declared sharing scope
- **AND** pre-commit failure has no effect while ambiguous post-commit acknowledgement is retryable
- **AND** receipts describe complete physically removed canonical bodies rather than membership-bit counts

#### Scenario: Adapter cannot support bounded collection

- **WHEN** a storage declares the required collection capability unsupported
- **THEN** bounded collection returns typed unsupported and retains all content
- **AND** it cannot advertise a bound while calling the previous unbounded methods internally
