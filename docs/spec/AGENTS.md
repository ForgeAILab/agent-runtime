<!-- SPEC:START -->
# Spec Instructions

## TL;DR Quick Checklist

- Discover current state: `docs/spec/specs/` (truth) + `docs/spec/changes/` (proposals)
- Decide: proposal needed vs direct fix
- Pick a unique `change-id` (kebab-case, verb-led: add-/update-/remove-/refactor-)
- Scaffold: `proposal.md`, `tasks.md`, optional `design.md`, and delta specs per capability
- Write deltas using `## ADDED|MODIFIED|REMOVED|RENAMED Requirements`
- Every `### Requirement:` MUST have descriptive text + ≥1 `#### Scenario:`
- Validate, then request approval; do not implement until approved

## Three-stage workflow

### Stage 1: Proposal (no coding)
Create `docs/spec/changes/<change-id>/` with:
- `proposal.md` (Why/What/Impact)
- `tasks.md` (implementation checklist)
- `design.md` (only when cross-cutting / ambiguous / perf/security/migration heavy)
- delta specs under `docs/spec/changes/<id>/specs/<capability>/spec.md`

### Stage 2: Implement (approved only)
Implement tasks sequentially. Keep `tasks.md` checkboxes accurate. Avoid scope creep.

### Stage 3: Archive (after shipping)
Merge approved deltas into `docs/spec/specs/` (truth) and move the change into `docs/spec/changes/archive/YYYY-MM-DD-<id>/`.

## Delta rules (strict)

- Delta files live under `docs/spec/changes/<id>/specs/<capability>/spec.md`
- Use one of:
  - `## ADDED Requirements`
  - `## MODIFIED Requirements`
  - `## REMOVED Requirements`
  - `## RENAMED Requirements`
- ADDED/MODIFIED blocks must contain SHALL/MUST and at least one `#### Scenario:`
- REMOVED is names-only (header lines or bullet list)
- RENAMED uses FROM/TO pairs
<!-- SPEC:END -->
