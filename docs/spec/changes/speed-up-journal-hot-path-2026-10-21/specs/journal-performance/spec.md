## ADDED Requirements

### Requirement: Runtime journal profiling is opt-in and reproducible

The runtime SHALL provide the named in-repo release benchmark and opt-in
instrumentation defined in bench.md, including timers equivalent to the
supplied runtime profiling patch, writer preparation and gate/continuation
spans, work counters and structured raw/summary output. Production default
builds MUST compile out the observer surface and MUST NOT acquire a benchmark
or production telemetry dependency.

#### Scenario: Benchmark feature is off

- **WHEN** core/runtime are built with their production defaults
- **THEN** journal timer/counter hooks and observer exports are absent
- **AND** canonical behavior, logging and dependency boundaries remain unchanged

#### Scenario: Feature is on but observation is disabled

- **WHEN** the same fixture runs without an active scoped observer
- **THEN** it produces the same canonical state and authority behavior
- **AND** disabled/enabled timer and callback overhead are calibrated and recorded without correcting gate times by subtraction

#### Scenario: A builder's cost was previously only a residual

- **WHEN** the benchmark reports writer preparation and first import
- **THEN** direct non-overlapping writer spans identify their cost outside awaited engine commits
- **AND** parent/child timer identity prevents adding manifest or memo-closure time again to inclusive parents

### Requirement: Journal phase measurements preserve the fixture and full cadence

Every phase SHALL measure A/B N=1000/4000 plain/tool, original C tool at both
sizes and the explicitly supplemental C plain variant defined in bench.md,
using frozen generators, seeds, scripts, cache assertions and fixed clock.
All comparisons MUST use identical storage/policy/budgets and charge complete
per-terminal S2 plus both-domain collection drains, including P4 continuations;
N=100 and isolated/full-matrix C diagnostics remain required.

#### Scenario: A/B comparison is made after a phase

- **WHEN** the named release target compares a frozen baseline and optimized revision
- **THEN** generated fixture bytes/fingerprints match and at least five fresh A/B samples per shape are retained
- **AND** cold/warm open, full Runtime resume, first import and last-turn-plus-maintenance are reported separately

#### Scenario: Supplemental C plain is measured

- **WHEN** the plain C variant grows to N messages
- **THEN** it uses the unchanged realistic seeds/content branches with N/2 two-message turns and a new writer per turn
- **AND** every artifact labels it C-plain-supplement with its own pre-phase baseline, never a historical Forge C tool result

#### Scenario: Collection yields cheaply

- **WHEN** P4 completes one turn's collection in several calls
- **THEN** the turn metric includes their full drain time and all maintenance/write costs
- **AND** output retains individual call/gate maxima, yields/restarts and cumulative receipts

#### Scenario: Original C stall is investigated

- **WHEN** phase acceptance is evaluated for collection bounds
- **THEN** at least three complete C replicates in isolated and full-matrix contexts plus the 100-dirty-session case are retained
- **AND** each call's wall time is paired with its own actual gate-held intervals
- **AND** an unpaired historical maximum is not assigned an invented cause

### Requirement: Each journal phase has independent numeric and structural acceptance

P1–P4 SHALL satisfy the phase-specific numeric, work, correctness and regression
criteria in bench.md at the required sizes/shapes. Estimates in design.md are
conditional planning evidence; the report MUST distinguish measurements,
acceptance thresholds, unsupported/fallback cases and unmet criteria.

#### Scenario: Incremental commit is evaluated

- **WHEN** a supported warmed phase-P1 transition runs
- **THEN** each targeted engine stage and their disjoint sum is at most 50% of its paired baseline, aiming at 25%, and attributed writer preparation is at most 50%
- **AND** the fixed-delta N=4000 work count is at most 1.5 times N=1000 with zero targeted unchanged-prefix walks
- **AND** exact roots, checkpoint execution and policy outputs equal the complete oracles

#### Scenario: Proof reuse is evaluated

- **WHEN** P2 collects certified warm roots
- **THEN** old-body closure reads and old-edge revalidation are zero
- **AND** B's combined collection time is at most 30% of the paired predecessor at both N values and shapes
- **AND** cold/uncertified roots still trace correctly and all inventory/storage work is reported

#### Scenario: Writer-open view is evaluated

- **WHEN** P3 initializes a valid warm native writer
- **THEN** unchanged-body reads and writer root-derivation passes are zero and D-1 final authority checks remain
- **AND** full warm Runtime resume is at most 25% of pre-P3, with B/4000 at most 150 ms plain and 200 ms tool
- **AND** full cold resume/import do not regress more than 10%

#### Scenario: Bounded collection is evaluated

- **WHEN** the P4 release matrix runs over supported bounded adapters
- **THEN** every call and gate hold targets below 250 ms and is strictly below 1,000 ms
- **AND** quiescent supported-capacity drains with positive budgets terminate with reference-equivalent retention/receipts and at most 15% extra default-budget full-drain time and pack-write bytes
- **AND** delayed non-preemptible storage calls are recorded as failures rather than cancelled or omitted

### Requirement: Performance reporting exposes memory, limits and consumer reachability

Reports SHALL include the configured 128 MiB per-engine comparison memo budget,
memo/work/cursor/cache metrics, per-operation timings, maxima and calibration
specified in bench.md. They MUST distinguish estimated charge, allocator heap
and process memory, report unavailable metrics as unavailable, and preserve the
unchanged Forge D10 denominators and verdicts.

#### Scenario: A shared proof or collection cursor is retained

- **WHEN** a benchmark reports reduced validation time
- **THEN** it also reports shared proof retention, persistent version depth and cursor charge
- **AND** leaked superseded versions or uncharged unbounded cursors fail acceptance
- **AND** RSS or canonical byte charge is not labeled measured memo heap

#### Scenario: Runtime acceptance passes while consumer import is slow

- **WHEN** evaluating the final combined result
- **THEN** the report includes the design's arithmetic against Forge turn/import/collection/write/disk gates
- **AND** it states remaining failures and further work rather than changing workload, denominator, projection or threshold
- **AND** a modeled runtime adapter is not described as Forge's real encrypted storage
