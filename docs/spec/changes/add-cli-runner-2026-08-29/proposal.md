---
created_at: 2026-08-29T18:20:37Z
updated_at: 2026-08-29T18:44:11Z
---

## Why

`agent-runtime` is usable only as an embedded Rust library or a deterministic
example. A developer cannot install the framework, submit one real prompt, and
consume its canonical event stream from a shell or CI job without first
writing a host application. A small reference CLI would close that adoption
gap while exercising the same public runtime facade every consumer uses.

The CLI must remain a host of the runtime, not a new execution path. Provider
selection, terminal rendering, environment lookup, and process exit codes are
host concerns; session admission, provider/tool execution, events, accounting,
and cancellation remain owned by the existing runtime contracts.

## What Changes

- Add an isolated `agent-runtime-cli` package with an `agent-runtime` binary.
  No existing workspace package depends on it, so embedders do not acquire
  Clap, Reqwest, terminal, or process-level dependencies.
- Add `agent-runtime run` as a non-interactive, one-turn command. It accepts a
  prompt either as a positional value or from piped standard input, composes an
  in-process `Runtime`, waits for the terminal turn boundary, and shuts the
  session down cleanly.
- Support the first-party provider adapters through a neutral provider factory:
  OpenAI, OpenAI-compatible, Anthropic, xAI Responses, and Gemini
  Interactions. Provider/model identity and all three model limits are
  explicit; the CLI does not guess an unknown model's context window.
- Resolve provider secrets only from named environment variables. The command
  has no API-key value flag, never persists credentials, and never emits a
  credential in output, errors, or debug formatting.
- Add a CLI-owned Reqwest `HttpTransport` modeled on Open Forge's production
  host transport: HTTPS by default, no automatic redirects, URL userinfo and
  fragments rejected, and loopback/private/link-local/reserved destinations
  denied after DNS resolution.
- Add `text` and `jsonl` output modes. Text mode writes only assistant text to
  stdout and diagnostics to stderr. JSONL mode writes one serialized canonical
  runtime event envelope per line so scripts can consume stable schema-versioned
  data.
- Define stable process outcomes: success for a completed turn, usage/config
  errors through Clap's exit contract, runtime/provider failures as failure,
  and Ctrl-C as turn interruption followed by bounded session shutdown.
- Document install, provider configuration, model-limit flags, stdin behavior,
  output contracts, and examples.

## Impact

- Affected specs: `cli-runtime`, `package-architecture`
- Affected code: new `crates/agent-runtime-cli/`, workspace manifest and lockfile,
  README/CLI documentation, and test-only composition seams
- Public compatibility: additive; existing library crates and their default
  dependency graphs remain unchanged
- Security: introduces a credentialed network host and therefore requires
  exact endpoint validation, redirect denial, secret non-disclosure, bounded
  response/error handling, and adversarial transport tests
- Consumers: no Smith, Nyx, or Open Forge source changes are authorized by this
  proposal

## Shared-Code Admission

The executable is a reference host for this framework and belongs beside the
facade it demonstrates. Its separate package materially improves dependency
isolation: library consumers continue to receive no CLI parser, concrete HTTP
client, terminal behavior, or process policy. The package depends only on
shared runtime contracts and never imports consumer-domain types.

Open Forge is a design reference, not a dependency. This change adopts its
useful structural choices—a thin Clap entry point, reusable command logic,
explicit machine output, actionable configuration errors, secret-safe input,
and terminal-state exit semantics—without moving Forge server, authentication,
task, daemon, or product policy into the shared repository.

## Related Active Changes

- `add-provider-presets-and-fluent-config-builders` already adds the provider
  constructors the CLI factory can consume. Stage 2 must either confirm that
  change is approved/landed or use the equivalent stable constructors without
  broadening this proposal.
- `add-runtime-security-boundary-2026-07-24` defines host-transport egress
  obligations. The CLI transport must satisfy that boundary and must not add a
  bypass around runtime authorization.
- Other active `package-architecture` deltas add independent packages. This
  proposal adds an orthogonal requirement and does not modify their package
  contracts.
- The uncommitted web-fetch work already present in the worktree is unrelated
  and must be preserved untouched.

## Non-Goals

- No interactive REPL or terminal UI.
- No persisted sessions, checkpoint store, conversation history, or credential
  store.
- No config-file format, provider discovery, OAuth ceremony, or import of
  credentials from another product.
- No built-in shell/filesystem tools, MCP server discovery, plugin loading, or
  product-specific system prompt.
- No daemon, REST API, web UI, task lifecycle, or Open Forge command surface.
- No library-facade dependency on the CLI package and no CLI dependencies added
  to existing packages.

## Approval Boundary

Approval authorizes Stage 2 implementation of the isolated CLI package,
documentation, and tests in this repository only. It does not authorize edits
to `../open-forge`, consumer migrations, package publication, credential
persistence, interactive chat, or tool/plugin configuration.
