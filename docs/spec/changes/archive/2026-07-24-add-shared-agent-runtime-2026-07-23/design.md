## Context

Smith, Nyx, and Open Forge are separate products with different users,
release schedules, and business rules. They nevertheless need the same hard
runtime mechanisms: provider normalization, streaming, direct tool loops,
cancellation, usage accounting, and deterministic testing.

Nyx currently contains the broadest implementation base. Open Forge contains
useful executor and worktree behavior but most of its public types express
Forge's task domain. Smith's current proposal started creating parallel
`smith-*` runtime crates, which would make a third implementation.

The shared repository prevents that divergence without coupling the products
to one another. Consumer repositories depend only on tagged releases and map
their policy and domain types onto neutral host contracts.

## Goals / Non-Goals

### Goals

- Provide one canonical provider/agent/tool runtime for all three consumers.
- Keep the public package surface small and host-neutral.
- Preserve useful source history and license provenance during migration.
- Support deterministic in-process embedding without a required daemon.
- Let each consumer choose presentation, prompts, configuration precedence,
  persistence backends, approval policy, and workspace behavior.
- Make breaking changes visible and testable across consumers.

### Non-Goals

- A monorepo or shared product release cadence.
- A lowest-common-denominator product framework.
- Product-specific state machines, chat channels, screens, commands, or
  configuration files.
- Moving all Nyx or Forge crates into this repository.
- Stable `1.0` compatibility in the first release.

## Decisions

### Decision 1: Separate Neutral Repository

The shared runtime is an independent repository. Smith, Nyx, and Open Forge
consume it through ordinary Cargo dependency resolution and keep independent
repositories, issue trackers, release versions, and product roadmaps.

The shared repository MUST NOT depend on consumer repositories. Integration
types and conversions live in each consumer.

Alternatives considered:

- A monorepo was rejected because the products are intentionally independent.
- Three local implementations were rejected because fixes and security
  behavior would drift.
- A source-copy synchronization script was rejected because it preserves
  duplicate ownership rather than eliminating it.

### Decision 2: Small Public Package Surface

The initial workspace contains:

| Package | Responsibility | Must not own |
| --- | --- | --- |
| `agent-runtime-core` | Neutral messages/content, typed events, errors, cancellation, usage primitives, IDs, and host adapter traits | HTTP clients, product configuration, UI, consumer types |
| `agent-runtime` | Runtime composition, provider adapters, direct agent loop, tool registry/execution, and host-facing session API | Smith, Nyx, or Forge policy |
| `agent-runtime-testkit` | Fake provider, controllable clock, event recorder, temporary workspace, and conformance harness | Production behavior unavailable outside tests |

Internal modules may be split into unpublished packages later when compile-time
or dependency evidence justifies it. Consumers SHOULD depend on the public
facade rather than unpublished implementation packages.

### Decision 3: Embeddable Host Contract

The intended public shape is:

```rust
pub struct RuntimeBuilder { /* injected services and policy */ }
pub struct Runtime { /* shared immutable composition */ }
pub struct SessionHandle { /* one active/resumable session */ }

impl Runtime {
    pub async fn start_session(
        &self,
        request: StartSession,
    ) -> Result<SessionHandle>;
}

impl SessionHandle {
    pub async fn send(&self, input: UserInput) -> Result<TurnId>;
    pub fn subscribe(&self) -> RuntimeEventStream;
    pub async fn cancel(&self, reason: CancelReason) -> Result<()>;
}
```

Exact signatures may change during implementation, but the contract MUST
support concurrent event consumption, explicit cancellation, resumable session
identity, and host injection without consumer-domain types.

Host adapters cover:

- provider lookup and credentials;
- tools and tool metadata;
- approval decisions;
- workspace boundaries;
- session persistence;
- secret resolution;
- event observation; and
- time for deterministic policies.

### Decision 4: Capability-Driven Provider Events

The provider boundary uses a capability descriptor and a normalized event
stream. The initial event vocabulary covers text, reasoning, tool-call deltas,
finish state, errors, usage, and cache observations. Unsupported options fail
before network I/O or emit an explicit configured downgrade.

The first production vertical slice uses a configurable OpenAI-compatible
adapter plus a deterministic fake. Additional OpenAI Responses and Anthropic
adapters require conformance against the same public contract and may land in
follow-up changes.

### Decision 5: One Direct Agent and Tool Loop

The runtime owns the mechanism for:

1. assembling host-supplied prompt content and canonical history;
2. streaming a provider attempt;
3. validating complete tool calls;
4. applying the host approval policy;
5. invoking allowed tools with workspace, deadline, output, and cancellation
   context;
6. appending canonical tool results; and
7. repeating until completion, cancellation, or a configured limit.

Prompt wording, product instructions, UI-only notifications, and business
workflow decisions remain consumer policy.

### Decision 6: Source Transfer, Not Copy Synchronization

Nyx is the primary source donor. Implementation uses temporary filtered clones
or subtree history to preserve relevant commits without rewriting the working
Nyx repository. A `PROVENANCE.md` file records source repositories, revisions,
path mappings, retained notices, and later refactors.

When a consumer migrates, it deletes its superseded implementation in the same
consumer change that adopts the shared release. A temporary duplicate may
exist only during the documented transfer window; new behavior lands in the
shared owner once transfer starts.

Open Forge's `git` and `workspace` packages do not move in this change. If
Smith later consumes identical worktree behavior, a separate proposal may
transfer those whole packages or establish a neutral adapter package.

### Decision 7: Versioned Releases and Local Overrides

Normal consumer manifests use a tagged semantic version or an exact Git
revision during the initial unpublished phase. Checked-in releases MUST NOT
depend on sibling relative paths.

Developers may use an uncommitted Cargo path override to a sibling checkout.
Documentation supplies the override, verification, and cleanup steps without
requiring a permanent super-workspace.

Before a release candidate is tagged, CI runs:

- the shared workspace unit and conformance suites;
- API/schema compatibility fixtures;
- the supported Smith adapter suite;
- the supported Nyx adapter suite; and
- the supported Open Forge adapter suite.

Consumer failures block the shared release unless the release is explicitly
declared breaking and coordinated consumer proposals exist.

### Decision 8: Shared-Code Admission Rule

A capability belongs here when it is required by at least two consumers or is
foundational to the approved runtime contract. Product-specific features stay
in their product until a second real consumer and a neutral contract exist.

Feature flags MAY remove heavy optional implementations, but MUST NOT change
the meaning of public events or silently disable security checks.

## Risks / Trade-offs

- **Shared API becomes a lowest common denominator.** Use capability
  descriptors, host traits, and explicit extension data rather than consumer
  conditionals.
- **One consumer dominates the design.** Require neutral naming and
  compatibility fixtures from all active consumers.
- **Cross-repository changes are slower.** Use pre-release tags, exact pins,
  local path overrides, and coordinated proposals.
- **Migration creates a temporary duplicate.** Declare the shared repository
  the owner at transfer start and prohibit independent edits to the old copy.
- **History or licensing is lost.** Import from filtered clones and maintain a
  machine-reviewable provenance map.
- **The facade grows into a framework.** Apply the two-consumer/foundational
  admission rule and keep product policy outside.

## Migration Plan

1. Approve this proposal and confirm the working repository/package names.
2. Establish the Rust workspace, quality baseline, and provenance records.
3. Implement neutral core contracts and the deterministic testkit.
4. Transfer and adapt the Nyx provider, agent-loop, and tool mechanisms into
   the shared packages.
5. Complete the fake and OpenAI-compatible vertical-slice conformance suite.
6. Tag an unpublished or pre-release `0.1.0` candidate.
7. Create and approve the Nyx consumer migration; delete superseded Nyx code
   when adopting the shared release.
8. Rewrite the Smith proposal as a thin terminal host and adopt the same
   release.
9. Create the Open Forge executor-adapter proposal and test the third consumer.
10. Enable the three-consumer compatibility gate for subsequent releases.

Each migration must keep its consumer buildable. No consumer is switched to an
unreleased relative path in its default branch.

## Open Questions

- What permanent repository and public package names are available?
- Will initial releases use a public registry, a private registry, or exact Git
  tags?
- Which remote CI identity is allowed to fetch all three consumer repositories?
