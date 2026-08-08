---
created_at: 2026-08-07T20:26:54Z
updated_at: 2026-08-07T20:26:54Z
---

# Design: MCP as a capability source

## Why a design document

Three decisions here are cross-cutting and not obvious from the proposal: how an
untrusted server's tools acquire authority, where the package boundary falls so
the dependency-light guarantees survive, and how a dying server fails without
taking the session with it. Each is recorded below with the alternative that was
rejected.

## Package boundary

```
agent-runtime-registry   (std-only)      ← RegistryId, Permission
agent-runtime-ability    (+ registry)    ← AbilityDescriptor, McpConnectionInfo
agent-runtime-core       (+ tokio)       ← Tool, ToolSpec, ToolEffects, RuntimeError
        ↑
agent-runtime-mcp        (+ rmcp)        ← NEW: transport, listing, adapter
```

`agent-runtime-mcp` sits beside the facade, not beneath it. Nothing in the
existing graph depends on it; a host opts in by adding the dependency and passing
the produced tools into composition. This keeps the `package-architecture`
guarantee that a descriptor-only extension never pulls the runtime graph, and it
keeps `rmcp` out of every build that does not ask for MCP.

`tokio`'s `process` and `io-util` features are required by the stdio transport.
They are declared in this package's own `[dependencies]`, not in
`[workspace.dependencies]`. Cargo feature unification means an MCP-enabled build
widens tokio for the whole graph, but a build without this package does not — the
workspace default stays `sync, rt, macros, time`.

### Feature layout

| Feature  | Adds                                  | Default |
|----------|---------------------------------------|---------|
| `stdio`  | `rmcp/transport-child-process`, `process-wrap`, `which` | yes |
| `http`   | `rmcp/transport-streamable-http-client`, `reqwest`, `hyper` | no |

`stdio` is the default because it is the transport nearly every local server
uses and it carries the lighter dependency set.

## Decision 1: authority for tools whose effects are unknown

An MCP tool arrives as a name, a description, and a JSON input schema. Nothing in
the protocol tells the runtime whether calling it reads a file, deletes a
repository, or spends money. The optional `annotations` object carries
`readOnlyHint` and `destructiveHint`, but those are **written by the server**,
which is precisely the party whose behavior is in question.

`capability-routing` already forbids the naive reading: "Empty or `None` risk
defaults MUST NOT make an effectful tool appear harmless."

**Rule adopted.** Authority is a floor supplied by the host, which annotations
may raise and may never lower:

```
declared = host_floor ∪ annotation_derived_additions
```

- The host supplies `McpServerConfig::effect_floor`, defaulting to
  `ToolEffects::read_only().with_network()` — every remote call is at minimum a
  network egress to the server.
- `destructiveHint: true` **adds** a write effect. `readOnlyHint: true` is
  recorded as metadata for display and retrieval ranking but subtracts nothing.
- `permission_upper_bound` is then derived from `declared` by the existing
  `ToolEffects::permission_upper_bound()`, so remote tools and built-ins share
  one derivation.

A server that omits annotations entirely and a server that claims to be read-only
receive identical authority. Lying is therefore useless, which is the property
worth having.

**Rejected: trusting annotations.** It reads naturally and matches how several
MCP hosts behave, but it lets the audited party write its own audit. A server
that is compromised after the user approves it would silently drop to
`read_only`.

**Rejected: one blanket "remote" permission.** Simple, but it collapses a
filesystem server and a search server into the same approval prompt, which makes
the consumer's approval UX useless and pushes discrimination back into every
consumer.

### Argument-specific authority

`Tool::prepare` exists so a tool can narrow authority from its arguments — an
edit of `./src/a.rs` claims that path, not the whole workspace. A remote tool
**cannot** do this: the argument schema is server-defined and the runtime has no
mapping from an arbitrary field to a host resource. `McpTool::prepare` therefore
uses the default static derivation and never claims narrowed authority. This is
the conservative direction, and it is what the trait's default already does.

## Decision 2: identity and naming

`RegistryId` is flat — a domain plus a name — so server and tool are encoded in
the name:

| Surface        | Form                        | Example                        |
|----------------|-----------------------------|--------------------------------|
| Registry id    | `mcp:<server>/<tool>`       | `mcp:github/create_issue`      |
| Server id      | `mcp:<server>`              | `mcp:github`                   |
| Model-facing   | `mcp__<server>__<tool>`     | `mcp__github__create_issue`    |

The model-facing separator is a double underscore, not a dot. Anthropic and
OpenAI both restrict tool names to `[a-zA-Z0-9_-]`, so a dot is rejected at the
provider boundary; `__` is the widest separator that survives every provider
grammar and is distinctive enough not to collide with an ordinary tool name. The
full name is bounded at 128 characters, the tightest provider limit, well inside
the runtime's own 256-character check.

Server and tool names are validated on ingest rather than sanitized: a server
returning a name with disallowed characters, a name colliding with another tool
on the same server, or a name that would exceed the bound is rejected with a
diagnostic. Silently rewriting a name would mean the model calls something whose
identity the server never agreed to.

Namespacing by server is what makes two servers exposing `search` coexist. The
existing consumer-side fixture (`smith-tui` renders `mcp.some_third_party_tool`)
predates multi-server support and the consumer proposal updates it.

## Decision 3: failure isolation

An MCP server is a separate process or a remote host. It can fail to start, hang
during initialize, die mid-session, or return frames that do not parse. None of
those may fail the session.

Lifecycle states and their handling:

| Event                       | Handling                                            |
|-----------------------------|-----------------------------------------------------|
| Dial/initialize timeout     | Server contributes zero abilities; diagnostic; session proceeds |
| Protocol version mismatch   | Rejected with an actionable error naming both versions |
| Death after listing         | In-flight calls fail as `RuntimeError::tool`; tools become unavailable at the next safe boundary |
| Malformed frame             | Bounded and rejected; repeated violations disconnect the server |
| Oversized result            | Truncated to the configured bound with an explicit marker |
| Call exceeds request timeout| Cancelled; the deadline comes from `InvocationContext`, not a local clock |

Removal of a dead server's tools advances an activation epoch at a **safe
boundary**, per the existing `capability-routing` requirement that activation
changes never mutate an in-flight provider request. A server dying mid-turn does
not retract a schema the model is currently looking at; the current request
completes and the next one is replanned.

Child processes are terminated as a group on shutdown. `process-wrap` provides
this, matching how `smith-tools` already kills a process tree rather than only
its root.

## Decision 4: result translation

MCP returns a content-block array (`text`, `image`, `audio`, `resource`) plus an
`isError` flag. `ToolOutcome` is the runtime's canonical shape.

- `isError: true` becomes a tool-level error outcome, not a transport error — the
  model should see and recover from it, as it would from a failing built-in.
- `text` blocks concatenate in order.
- `image`/`audio`/`resource` blocks are summarized as a bounded placeholder
  recording type and size. Sending binary payloads into the transcript is a
  context-budget hazard and belongs with the artifact store, which is a follow-on.
- Total output is bounded before it reaches the transcript; the runtime's
  existing artifact-offload path handles oversized exact outcomes.

## Testing

The testkit gains an in-process fake server implementing the MCP server side over
an in-memory duplex, so the conformance suite runs with no child process and no
network — matching the `package-architecture` requirement that mechanism tests
stay offline. Hostile-server cases are first-class fixtures: a server that claims
`readOnlyHint` on a destructive tool, one that returns duplicate tool names, one
that never answers `initialize`, one that dies after listing, and one that
returns a 10 MB text block.

Real-transport tests are a separate opt-in target that spawns a trivial stdio
server, excluded from the default `cargo test` run.
