---
created_at: 2026-10-16T23:45:00Z
updated_at: 2026-10-20T01:02:12Z
---

## Why

A host often needs one bounded, tool-free answer from a model outside the
turn loop. Examples are a reviewer (advisor), a semantic summary, a vision
question, or a WASM module's `complete` call. The runtime has no API for
that, so each host hand-writes the same stream fold:
- collect text up to a bound;
- merge usage, keeping it on failure;
- map finish reasons;
- refuse tool calls;
- treat an empty answer as an error;
- race cancellation and a deadline.

Smith has three copies (advisor, semantic summary, vision), and a fourth is
planned.

An installed CLI agent is worse. Turns reach it through
`ExternalAgentBackend`, and the turn engine owns that path. A host that wants
one answer from a CLI must call `run_turn` itself and fold
`ExternalAgentEvent`s by hand. It also has no way to hand the backend
instructions for that one call: `ExternalTurnRequest` carries input but no
instructions, so Smith builds its advisor's backend with the instructions
baked in. The result is that a provider and a CLI agent are driven
differently, by every host, for the same job.

The Smith owner asked on 2026-10-16 for one library abstraction, so a host
asks for an answer the same way whether a provider or an installed agent
gives it.

## What Changes

- **New:** `agent_runtime::agent::one_shot`, with:
  - `run_one_shot(&executor, request, &mut usage)`;
  - `OneShotExecutor::{Provider { provider, model }, External(backend)}`;
  - `OneShotRequest`, `OneShotAnswer { text, finish }`;
  - `OneShotFinish::{Stopped, Truncated}`;
  - a typed `OneShotError`: cancelled, timed out, tool call, filtered,
    empty, unfinished, unsupported input, or failed with a runtime error.

  One fold serves both executors. The caller owns the usage accumulator, so
  partial usage survives a dropped future. The call owns a child
  cancellation and cancels it on drop, which stops a running CLI process.
  The `External` variant sits behind the `external-agent` feature.
- **New:** `ExternalTurnRequest::instructions: Option<String>`, which is
  added to the backend's own system instructions for that turn. Turns driven
  by the runtime pass `None`, so their behavior is unchanged.
- **BREAKING (pre-1.0 minor):** `ExternalTurnRequest` becomes
  `#[non_exhaustive]` with `ExternalTurnRequest::new(input, turn, cancel)`.
  Later fields can then be added without breaking consumers. Struct literals
  outside the crate must move to `new` and field assignment.
- The bundled CLI backends in `agent-runtime-agent-cli` honor
  `instructions`: Claude Code through `--append-system-prompt`, Codex by
  prefixing it to the input.
- This ships as runtime v0.5.0 together with the unreleased reqwest
  transport.

## Impact

- Affected specs: `runtime-api`
- Affected code: `crates/agent-runtime/src/agent/{one_shot.rs (new),
  external.rs, mod.rs}`, the external turn driver (`driver/external_turn.rs`,
  which constructs the request), `crates/agent-runtime-agent-cli/src/{claude,
  codex}.rs`, integration tests under `crates/agent-runtime/tests/`,
  `CHANGELOG.md`
- Consumers: Smith will adopt this after the release. Its advisor, semantic
  summary, and vision tool will move onto `run_one_shot`; its CLI backend
  will read `request.instructions`; and its six `ExternalTurnRequest`
  literals will move to `new`. The planned WASM `complete` host call
  (`add-wasm-model-and-wake-2026-10-11` in Smith) will build on it.
