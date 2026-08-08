---
created_at: 2026-08-07T20:26:54Z
updated_at: 2026-08-07T20:33:14Z
---

## Why

The runtime already treats Model Context Protocol capabilities as first-class
in its approved contracts, but nothing can ever satisfy them. `AbilityKind::Mcp`
and `RegistryDomain::Mcp` exist, descriptors may declare MCP dependencies, and
`capability-routing` requires that activation materialize "MCP connections"
under policy. Yet `Activated::McpConnection` documents that "establishing the
connection is the caller's responsibility", and no package in the workspace can
establish one. The contract has a hole shaped exactly like a client.

That hole is not a missing product feature; it is an unimplementable branch of
an approved runtime contract. Every consumer that reaches `Activated::McpConnection`
today must invent its own transport, its own schema translation, and — most
importantly — its own authority model for tools whose effects the runtime cannot
see. Authority is the reason this belongs in the shared repository rather than in
each consumer: an MCP tool arrives as an untrusted name and a JSON schema, with
no declared effects. Deriving a conservative `ToolEffects` and
`PermissionSet` from that is security mechanism, and the prepared-invocation
pipeline that must enforce it lives here.

`add-agent-runtime-harness`'s successor change gated this work explicitly
(`stabilize-session-harness-pipeline-2026-07-31`, task 0.4: "Block MCP … until
Sections 1 through 4 pass") and named it as a sanctioned follow-on: "separate
proposals may … add MCP and subprocess sources through the ability registry."
That change is archived and its gate is satisfied.

## What Changes

- Add `agent-runtime-mcp`, a new workspace package that turns a configured MCP
  server into registry abilities and `Tool` implementations. It is additive: no
  existing package gains a dependency on it, and hosts that never enable it see
  an unchanged dependency graph.
- Implement the client over the official `rmcp` SDK (crates.io, MIT, maintained
  by `modelcontextprotocol/rust-sdk`) rather than hand-rolling JSON-RPC framing,
  handshake, and transport negotiation.
- Map `tools/list` to bounded `AbilityDescriptor`s so an MCP server's catalog is
  searchable through the existing retrieval path with zero connection cost after
  the initial listing.
- Map `tools/call` to the existing `Tool` contract so remote tools flow through
  the same prepare → authorize → approve → invoke pipeline as built-ins, with no
  parallel execution path.
- Establish the **untrusted-effects rule**: a remote tool's authority is a
  host-supplied conservative floor. Server-provided annotations
  (`readOnlyHint`, `destructiveHint`) MAY raise the declared effects and MUST
  NOT lower them. A server cannot describe itself into fewer permissions.
- Fill `Activated::McpConnection` end to end: activation policy gates the dial,
  and readiness/credential checks run before any process is spawned or socket
  opened.
- Isolate failure: an MCP server that fails to start, hangs, dies mid-session,
  or returns malformed frames removes its own tools and emits a diagnostic
  without failing the session or corrupting the epoch.

## Impact

- Affected specs: `capability-routing`, `package-architecture`, `tool-execution`
- Affected code: new `agent-runtime-mcp`; `agent-runtime-testkit` gains a fake
  server fixture. No production changes to `agent-runtime-core`,
  `agent-runtime-ability`, or `agent-runtime` are required — the contracts they
  already publish are sufficient, which is the evidence that this design fits.
- Public compatibility: additive only. No existing type, event, or checkpoint
  schema changes.
- Dependency graph: `rmcp`'s mandatory set is `chrono`, `futures`,
  `pin-project-lite`, `serde`, `serde_json`, `thiserror`, `tokio`, `tokio-util`,
  `tracing`. `chrono` and `tracing` are new to the workspace and both are
  MIT/Apache-2.0. Transports are feature-gated: `stdio` adds `process-wrap` and
  `which`; `http` adds `reqwest`/`hyper`. `tokio`'s `process` and `io-util`
  features are enabled **only** in this package, never promoted to the workspace
  default, so no existing crate's build widens.
- Security: remote tools are the first tools in the workspace whose effects are
  self-declared by an untrusted party. The untrusted-effects rule and its
  conformance tests are the load-bearing part of this change.
- Consumers: coordinated Smith work is specified by
  `../tui/docs/spec/changes/add-mcp-servers-2026-08-07/`.

## Shared-Code Admission

`CONTRIBUTING.md` admits shared production behavior when it is required by two
consumers **or** foundational to the approved runtime contract. This change
qualifies under the second clause: `Activated::McpConnection`, `AbilityKind::Mcp`,
and the `capability-routing` activation requirement are already approved and
already reference MCP connections that no package can produce.

The boundary is held by what this change deliberately excludes. Server
definitions, on-disk configuration format, trust prompts, approval UX, status
presentation, and which servers a product ships are **product policy** and stay
in the consumer. This package accepts an already-resolved `McpServerConfig` and
never reads a config file, prompts a user, or names a product.

## Non-Goals

- No MCP **server** implementation. This is a client only.
- No configuration file format, discovery, or auto-installation of servers.
- No OAuth flow. Servers needing bearer credentials receive them through the
  existing `SecretStore`/readiness path; interactive authorization is a
  follow-on if a real consumer needs it.
- No MCP resources, prompts, or sampling. Tools first; the other primitives get
  their own proposal once the tool path is proven.
- No transport beyond stdio and streamable HTTP. Legacy HTTP+SSE is out.

## Delivery Slices

1. Package skeleton, `McpServerConfig`, error taxonomy, and the fake-server
   testkit fixture — no network, no child processes.
2. Connection lifecycle over `rmcp`: dial, initialize, version negotiation,
   `tools/list`, bounded shutdown, and the timeout/failure-isolation tests.
3. Descriptor mapping and the untrusted-effects rule, with conformance tests
   asserting a hostile server cannot lower its own authority.
4. `Tool` adapter: prepare/invoke, deadline and cancellation propagation,
   content-block translation, and oversized-output bounding.
5. Streamable HTTP transport behind the `http` feature, plus `cargo deny`
   verification of the full feature graph.

Slices 1 through 4 must pass before slice 5 adds the heavier dependency set.

## Approval Boundary

Approval authorizes Stage 2 implementation in this repository only. It does not
authorize consumer changes, package publication, MCP resources/prompts/sampling,
an OAuth flow, or a server implementation. `../tui` requires separate approval of
its coordinated proposal.
