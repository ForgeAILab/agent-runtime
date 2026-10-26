## Context

See Smith's approved proposal at `../tui/docs/spec/changes/add-profile-model-fallback-2026-10-23/`.

## Goals / Non-Goals
- Goals: ordered request-level fallback, bounded startup latency, unchanged authority and canonical history.
- Non-Goals: product configuration grammar, mid-answer switching, racing providers, or persistent health routing.

## Decisions
- Capture model services from a validated runtime into `ModelRoute`; activate only those services on a turn-local driver clone.
- Retain session execution state, tool authority, canonical history, and every original host interceptor. The active route supplies its session-partitioned planner and normalized request controls.
- Retries share the logical request's original startup budget.
- Unknown adapters cannot replay an accepted stream; provider-hosted effects require proof of replay safety.
- Save the active binding and profile fingerprint in nonterminal checkpoints; new user turns start at the primary.
- Opt fallback-enabled admissions into transition table 5; keep table 4 and its operation fingerprints frozen. Revision-4 in-flight turns finish under their original table. New readers accept both versions and reject unknown versions or in-place table changes.

## Risks / Trade-offs

Local cancellation cannot guarantee remote inference stops; unreported usage remains unknown.

## Migration Plan

New options are opt-in; existing compositions retain their event sequences and retry behavior.

## Open Questions

None beyond implementation verification under the approved Smith contract.
