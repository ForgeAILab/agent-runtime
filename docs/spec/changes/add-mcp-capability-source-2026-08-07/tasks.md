---
created_at: 2026-08-07T20:26:54Z
updated_at: 2026-08-07T20:33:14Z
completed_at:
---

## 0. Coordination and Baseline

- [x] 0.1 Approve this proposal and the coordinated Smith proposal
  (`../tui/docs/spec/changes/add-mcp-servers-2026-08-07/`) before implementation.
- [x] 0.2 Confirm the `stabilize-session-harness-pipeline-2026-07-31` MCP gate is
  satisfied; that change is archived and its Sections 1-4 pass.
- [x] 0.3 Record the `rmcp` version pinned at implementation time and verify its
  license and transitive graph against `deny.toml` before adding it.

## 1. Package Skeleton

- [x] 1.1 Add `crates/agent-runtime-mcp` to the workspace members and
  `[workspace.dependencies]` with a path + version entry.
- [x] 1.2 Declare `rmcp` with `client` plus the `stdio` and `http` transport
  features gated; keep `tokio`'s `process`/`io-util` local to this package.
- [x] 1.3 Define `McpServerConfig`, `McpTransport`, `ToolFilter`, and the
  `effect_floor` field defaulting to read-only plus network.
- [x] 1.4 Define the `McpError` taxonomy: startup, version, protocol, transport,
  timeout, and server-reported tool error.
- [x] 1.5 Add the in-process fake server fixture to `agent-runtime-testkit` over
  an in-memory duplex.
- [x] 1.6 Assert `cargo tree` shows no HTTP client with default features.

## 2. Connection Lifecycle

- [x] 2.1 Implement `McpClient::connect` with startup deadline and protocol
  version negotiation.
- [x] 2.2 Implement `list_tools` returning raw remote tool records.
- [x] 2.3 Implement bounded `shutdown`, terminating a stdio child as a process
  group.
- [x] 2.4 Add `server_that_never_initializes_contributes_no_abilities`.
- [x] 2.5 Add `incompatible_protocol_version_reports_both_versions`.
- [x] 2.6 Add `server_death_after_listing_fails_only_its_own_calls`.
- [ ] 2.7 Add `malformed_frame_is_bounded_and_disconnects_after_repeats`.

## 3. Descriptor Mapping and the Untrusted-Effects Rule

- [x] 3.1 Map a remote tool record to `AbilityDescriptor` with
  `mcp:<server>/<tool>` identity and a dependency on `mcp:<server>`.
- [x] 3.2 Implement the union rule: floor ∪ annotation additions, never
  subtraction.
- [x] 3.3 Derive `permission_upper_bound` through
  `ToolEffects::permission_upper_bound()`, shared with native tools.
- [x] 3.4 Validate and sanitize names; reject disallowed characters, per-server
  duplicates, and over-long names with a diagnostic.
- [x] 3.5 Add `read_only_hint_does_not_lower_authority` asserting an identical
  bound to an unannotated tool.
- [x] 3.6 Add `destructive_hint_raises_effects_above_the_floor`.
- [x] 3.7 Add `duplicate_tool_names_on_one_server_are_rejected` and
  `same_tool_name_on_two_servers_coexists`.
- [x] 3.8 Add `tool_is_not_selectable_without_its_server`.

## 4. Tool Adapter

- [x] 4.1 Implement `Tool for McpTool`: `spec()` from the mapped descriptor.
- [x] 4.2 Use the default static `prepare`; add
  `remote_tool_never_claims_argument_narrowed_authority`.
- [x] 4.3 Implement `invoke` over `tools/call`, deriving the timeout from
  `InvocationContext`'s deadline and observing cancellation.
- [x] 4.4 Translate content blocks: concatenate text, summarize binary as bounded
  metadata, map `isError` to a model-visible tool error.
- [x] 4.5 Bound total output before it reaches the transcript.
- [x] 4.6 Add `unanswered_call_resolves_as_tool_error_and_turn_completes`.
- [x] 4.7 Add `interrupted_turn_cancels_the_in_flight_request`.
- [x] 4.8 Add `server_error_result_is_visible_to_the_model`.
- [x] 4.9 Add `oversized_result_is_truncated_with_a_marker` and
  `image_content_records_metadata_not_bytes`.

## 5. Activation Path

- [ ] 5.1 Produce `Activated::McpConnection` only after policy and readiness
  pass; dial from that value.
- [ ] 5.2 Add `unready_credential_prevents_any_spawn_or_connection`.
- [ ] 5.3 Add `denied_server_issues_no_protocol_request`.
- [ ] 5.4 Remove a dead server's tools at a safe boundary with a new epoch; add
  `server_death_does_not_mutate_an_in_flight_request`.

## 6. HTTP Transport

- [x] 6.1 Implement streamable HTTP behind the `http` feature.
- [x] 6.2 Resolve bearer credentials through the existing readiness/secret path;
  never log credential values.
- [ ] 6.3 Add HTTP-transport conformance against the fake server.
- [x] 6.4 Run `cargo deny check` with `all-features = true` and record the result.

## 7. Release Gate

- [x] 7.1 `cargo test --workspace` and `cargo test -p agent-runtime-mcp
  --all-features` pass. Workspace: 38 suites, 888 tests, 0 failures. Crate:
  50 tests (33 unit, 16 conformance, 1 doctest).
- [x] 7.2 `cargo clippy --all-targets --all-features -- -D warnings` passes.
- [x] 7.6 `cargo fmt --all --check` passes for this package. *(Added: the gate
  checked Clippy but not formatting, and this package had drifted. Note that
  `crates/agent-runtime-provider/{gemini,openai,responses}.rs` are still
  unformatted from `fe99588`, which is unrelated to this change and was left
  alone.)*
- [x] 7.3 Public items carry doc comments; doc examples compile.
- [x] 7.4 Confirm no production package outside this one depends on it.
- [x] 7.5 Hand off to the coordinated Smith change; do not archive until that
  change has consumed the package. Consumed: `../tui` depends on
  `agent-runtime-mcp` with default features only, registers a real server's
  tools through its composition path, and exercised a live stdio server end to
  end (`crates/smith-runtime/tests/mcp_live.rs`).

## Deviations and Remaining Work

Recorded at the end of slices 1-4 so the gaps are visible rather than implied.

**1.5 — fixture location changed.** The in-process fake server lives in
`crates/agent-runtime-mcp/tests/conformance.rs`, not in `agent-runtime-testkit`.
Putting it in the testkit would give that crate an `rmcp` dependency with the
`server` feature, which every consumer of the testkit would then acquire —
directly against this change's own dependency-isolation requirement. If a second
consumer ever needs the fixture, it moves then.

**Behavior added beyond the task text.** `rmcp` completes initialization
against a server reporting an unknown protocol version rather than refusing, so
the `capability-routing` requirement that an incompatible version be rejected
was not actually enforced by the SDK. `finish_connection` now checks the
negotiated version against `ProtocolVersion::KNOWN_VERSIONS` and closes the
connection before listing anything. Found by writing the test for 2.5.

**Deferred, with reasons:**

- 2.7 — no malformed-frame test. The duplex fixture speaks through `rmcp`'s own
  codec, so injecting a bad frame needs a raw transport rather than a server
  handler.
- 6.3 — HTTP-transport conformance. The fake server is duplex-based; exercising
  the streamable HTTP path needs a real local HTTP server, which belongs in the
  opt-in non-hermetic target rather than the default run. **This is now on the
  critical path rather than a nicety:** the coordinated Smith change ships with
  `http` enabled, so a remote server is a supported configuration whose success
  path is covered by no test in either repository. A loopback streamable-HTTP
  fixture here would close it for both.
- Section 5 in full — the activation path. `McpClient::connect` documents that a
  caller must satisfy policy and readiness first, but nothing here *produces*
  `Activated::McpConnection`. The coordinated Smith change has now landed and
  does **not** produce it either: Smith gates the dial on its own execution-trust
  decision before `connect` is reached, and registers each remote tool through
  its existing tool-ability wrapper rather than through the binding's
  `AbilityKind::Mcp` descriptor — which would require a server-level ability
  inside a sealed registry that Smith's live routing has invariants about. The
  authority property the section exists for holds (an unadmitted server is never
  dialed and contributes nothing), but the typed seam is still unexercised and
  remains open.

**Config change outside this crate:** `deny.toml` gained `CDLA-Permissive-2.0`
to the license allow list. It covers `webpki-root-certs`, the Mozilla CA root
bundle reached through `reqwest` only when the non-default `http` feature is
enabled. It is a permissive data license with no copyleft.
