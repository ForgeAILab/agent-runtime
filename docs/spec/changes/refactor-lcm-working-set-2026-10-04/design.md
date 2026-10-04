## Context

This implements audit G6/G7/G8.3 and plan W2 U6. Revision tolerance must land
first so algorithm/sizer/policy changes cannot wedge valid persisted sessions.

## Decisions

Identity includes state schema, authorized timeline and binding, store revision,
classifier and configured content guard. Tunables include algorithm, pressure,
summary output/reclaim settings, request sizer and model adapter. Rebuild using
the existing authorized store validation path; retain committed node provenance,
source classifications, canonical-history validation and protected pending
operation recovery. Never bypass LcmViewAuthority or silently adopt a different
timeline. Uncommitted old-policy responses must be handled without replaying
model work or weakening identity checks.

A shared RequestSizer adapter counts source messages and the same system
message shape used by LCM projection. Working-set caps affect the authoritative
planner budget, while LCM targets exclude the measured non-history categories
of the most recent plan. Before any plan exists overhead is zero. The LCM budget
never drops below 25% of the target: a fixed share is independent of turn shape
and tokenizer, while the planner hard cap still bounds the real request, so a
large or stale overhead cannot refuse every turn. The clamp emits a typed
OverheadExceedsTarget lifecycle diagnostic, and the next plan re-measures the
overhead. Non-LCM sessions may use the planner cap too. Builder composition
replaces only the default LCM sizer with the planner RequestSizer; a host
LcmSizer is kept.

The provider summary adapter lives in the LCM crate behind provider-summary;
it uses core Provider and context planner contracts, without consumer/provider
adapter dependencies. Its host configuration supplies instructions, resolved
profile, sizer, a per-operation cancellation/deadline scope and per-call /
per-operation time limits. Compact rendering is split into
sizer-measured chunks even within an oversized message, then reduced through
bounded planner-approved calls. Every call has the trigger's purpose (idle
compaction only for idle-boundary work) and aggregate usage;
nonconvergence is a bounded failure handled by ordinary escalation/fallback.
Debug output omits instructions, source bodies and summary output.

## Risks / Trade-offs

A tuned sizer can change derived token totals while committed node metadata
retains provenance. Re-measure active summaries during accounting/rebuild and
keep store validation separate from current sizing. Ratio caps include message
framing, so they bind model output only; the deterministic fallback keeps the
base min(cap, source - 1) target. A source smaller than the fallback's own
framing is widened through the next turn by the deterministic leaf planner (so
pending-plan recomputation reproduces it); if nothing wider exists, hard
pressure ends with a structured cannot_fit. Derived hard rounds use one shared
formula, are fixed per admission epoch and capped at 64. Summary
map-reduce adds provider cost; usage includes every subcall. Boundary compaction
uses existing durable idle admission and never races a queued user turn.

## Migration Plan

Update escalation literals; enable provider-summary only when required. Keep
host prompts and model/profile selection in host configuration. Working-set
and boundary-compaction controls are additive. Forge can remove stale-state
removal for tunable changes after adoption; ownership/V149 retirement still
requires U7. No U8 session contract changes are included.

## Idle Durability Detail

Opt-in boundary compaction exposed the existing mismatch between ordinary idle
summary usage and the last exact terminal ledger. Refresh the exact terminal
checkpoint using the existing validated successor contract, with unchanged
session/turn/operation identity and monotonically advanced watermarks. A private
redaction-safe predecessor-usage fingerprint fences roll-forward if ordinary
persistence lags. Only protected semantic-summary records may extend that exact
ledger. No checkpoint schema, transition revision, public events or store traits
change. A tune-only revision mismatch between ordinary/protected LCM copies
uses the exact copy after matching identity fields, then rebuilds normally.
