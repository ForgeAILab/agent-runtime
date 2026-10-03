---
created_at: 2026-10-01T00:00:00Z
updated_at: 2026-10-01T00:00:00Z
---

# Proposal: Add cache diagnostics and argument normalization (U2)

## Why

Hosts cannot identify the first fragment that changed a comparable cache
prefix from runtime events. Tool authors also cannot normalize a model's
argument representation before advertised-schema validation. Both gaps force
host workarounds around neutral runtime seams. Smith can diagnose contributor
changes, Forge can remove redundant tool schemas, and Nyx can use the same
opt-in tool contract without provider-specific policy in the library.

## What Changes

- Add optional `first_changed_fragment` evidence to cache plans and the existing
  `CachePlanChanged` event, computed from the actual predecessor comparison.
- Add a default identity `Tool::normalize_arguments` hook before full schema
  validation and preparation, including newly edited approval arguments.
- Retain compiled validators in the runtime's sealed registry, keyed to its
  frozen tool specification, without changing advertisements or the ability
  package's dependency boundary.
- Add focused schema, redaction, normalization, and three-consumer fixtures.

## Impact

- Affected specs: `context-management`, `tool-execution`,
  `compatibility-contract`.
- Affected implementation: context cache comparison, the core cache event and
  tool trait, runtime registry/executor paths, and testkit.
- Optional diagnostic fields are serde-defaulted and omitted when unknown.
  The event variant set, schema versions, classes, lane rules, cache identities,
  fingerprints, and provider wire requests remain unchanged by diagnostics.
- The default trait method needs no implementation from existing tools.
  Opt-in normalization changes argument interpretation only for the tool whose
  host enables it; the same canonical schema remains the authority.
- New event/cache-plan Rust fields may require coordinated constructors or
  field-exhaustive destructuring updates. Additive JSON does not waive the
  compatibility gate.

## Compatibility and Consumer Gates

| Consumer | Required work before release | Testkit proof to add |
| --- | --- | --- |
| Forge | Compile event projections/constructors; optionally implement normalization in its own tools and remove its widened schema after its gate passes. No automatic envelope repair is enabled. | `consumer_open_forge`: strict canonical schema, explicit envelope normalizer, then workspace/policy denial prevents execution; unchanged schema bytes are advertised. |
| Smith | Compile exhaustive event matches and any field-exact projections; diagnostic presentation is optional. Preserve its existing contributor placement rules. | `consumer_smith`: two legal instruction contributors identify the first changed ID on the next request, and approval edits are re-normalized/re-authorized. |
| Nyx | No tool change required; retain identity normalization and its start/subscription flow. Update field-exact event construction if present. | `consumer_nyx`: a legacy tool rejects wrapped arguments under identity normalization while seeded ephemeral history and subscribe-before-send still work. |

Extend the existing named testkit suites with neutral fake adapters, plus
cache-plan, event-schema, invalid-argument, and prepared-action/recovery
conformance. These fixtures are proposed implementation work. Actual consumer
builds and all three gates must pass; failure blocks a compatible release.
Published consumer dependencies must use a tag or exact landed revision.

All audit §5 surfaces remain: Nyx's `StartSession::new().with_history` and
subscription ordering; Smith's file/session/checkpoint stores, explicit
contributor lanes/classes, `snapshot.manifests`, idle compaction, and event
variant matches; Forge's protected stores, authorized LCM reader/writer and
resolver, `ProviderAttemptFinished`, and `TurnFinish`. Store traits and command
shapes are untouched. Protected checkpoint exactness/idempotency/revision
fences and the redacting ordinary store remain separate. `Secret`, LCM Debug,
Sensitive extension defaults, and authorization before every LCM lookup stay
intact; no credential lease or raw argument/prompt content enters diagnostics.
The planner remains the only request-construction authority. Normalization
grants no permissions; schema, approval, workspace, cache-identity validation,
and model-limit defaults stay fail-closed. Product prompts, envelopes, and
model-family choices stay with hosts.

## Dependencies

Implement after U1 so hook failures can use its classes. U3 and U11 are
independent; U11 should use the final registry/executor boundaries and preserve
the three named consumer targets. Use the module roots already established by
`refactor-cache-session-responsibilities-2026-09-22`. No changes-root index is
introduced because that is not an established repository convention.

## Approval Boundary

This draft authorizes no code. Later implementation approval covers these two
additive seams and private validator reuse. Stability-class redesign,
breakpoint changes, tail rendering, model-specific adapter repair, and replacing
`system_prompt(String)` belong to later proposals.
