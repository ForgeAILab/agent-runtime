---
created_at: 2026-10-07T00:00:00Z
updated_at: 2026-10-07T00:00:00Z
---

# Tasks: Canonical LCM journal

Documents only are authorized now. Begin after U9 native reference persistence,
v3 migration, protected recovery and pin/GC conformance have passed. Do not
combine implementation approval or release eligibility with U9.

## 1. Contracts and storage proof

- [ ] 1.1 Approve canonical opt-in, exact same-domain atomic capability, version-5 formats, legacy-reader window and conservative mid-turn visibility rule.
- [ ] 1.2 Add precisely LcmCanonicalJournal::journal and commit_history plus the neutral DTOs/configuration in design.md; keep all existing store/reader/writer/resolver methods and U9 non-LCM paths compiling.
- [ ] 1.3 Implement reference-backend atomic entry/index/exact-head/checkpoint publication, operation-id digest binding and expected DAG/journal revision plus owner-generation fencing; generic commit/append cannot bypass it on canonical timelines.
- [ ] 1.4 Add scoped wrappers that authorize before every journal lookup/read/pin/commit/retire/collect, including grant revocation, wrong binding, cross-timeline object and stale-owner failures.

## 2. Canonical source routing and migration

- [ ] 2.1 Introduce version-5 TimelineHistory roots, durable_len/completed_len, SHA-256 source indices and pinned checkpoint/terminal projection roots without changing current LCM fingerprints.
- [ ] 2.2 Route every canonical transition through commit_history, reuse byte-identical LCM bodies in frozen request refs and preserve exact non-content state; stop secondary append_history calls in canonical mode only.
- [ ] 2.3 Retain planner-only request construction, active tool-exchange order, signed continuation, U3 diagnostics, U6 tunable rebuild and hard-admission pending-summary exact-successor proofs.
- [ ] 2.4 Preserve exact/nonterminal versus ordinary/redacted terminal views and distinct cutoffs and exact-source versus execution-view roots; compare next provider request/active checkpoint after credential redaction against main and test nodes crossing a cutoff and retained superseded projection roots.
- [ ] 2.5 Import v3 and U9 v4 sessions under stable import IDs/pins; cover missing active suffix, mismatching/redacted legacy timeline, pending action, frozen request/results/cache states, incomplete terminal evidence and two restarts after import.
- [ ] 2.6 Reject old-reader/downgrade/unsupported-capability fallback and unproven ahead tails; keep U7 frontier-safe legacy repair and U8 pending-fork semantics.

## 3. Crash, reachability and bytes

- [ ] 3.1 Cut crashes before/after each body/index/entry/head/checkpoint/projection write and lose commit acknowledgements; prove all-or-neither source/checkpoint visibility and no repeated committed external work.
- [ ] 3.2 Detect partial frames, sequence/hash/root closure failures, scoped-watermark publication gaps and corruption below published roots; repair only uncommitted tails or verified identical copies.
- [ ] 3.3 Include every source, DAG node, retained projection, request, expansion reader, child, import and Continue transfer in U9 GC fences; race fork/resume/claim/expansion with mark/delete.
- [ ] 3.4 Reclaim redundant U9 history objects only after refs convert and pins/retention release them; keep every live timeline source required for expansion and no default retirement expiry.
- [ ] 3.5 Count one canonical body across LCM/history/checkpoint/request refs and fixed-delta bytes at N=100/1000/10000; report ordinary projection, metadata, summary, import and legitimate cross-domain fork costs separately.

## 4. Coordinated release gate

- [ ] 4.1 Extend consumer_open_forge with assert_canonical_timeline_journal; validate actual Forge row-3.7 chunk/authorized-LCM transaction interface and all crash/GC/security cuts.
- [ ] 4.2 Extend consumer_smith with assert_optional_canonical_file_journal; verify file/capsule version guards, exact prepared actions/signatures, ordinary redaction and U9 non-LCM regression controls.
- [ ] 4.3 Extend consumer_nyx with assert_canonical_ephemeral_isolation and optional durable authorized timeline fixture; preserve history/subscription and zero durable activity.
- [ ] 4.4 Run existing consumer_smith, consumer_nyx and consumer_open_forge targets plus workspace/schema/conformance/docs, formatting, Clippy, deny/neutrality/license and MSRV checks during implementation; current document validation is not implementation verification.
- [ ] 4.5 Coordinate all consumer adapters/readers and immutable landed pins; document version-5 break, v3/v4 upgrade including mid-turn and downgrade/retention limitations. A failing supported consumer blocks a compatible release.
