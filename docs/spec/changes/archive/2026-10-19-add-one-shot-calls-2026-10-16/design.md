## Context

Three Smith callers fold a provider stream by hand today:
`smith-runtime/src/advisor.rs`, `smith-runtime/src/summary.rs`, and
`modules/vision/src/tool.rs`. The advisor also folds `ExternalAgentEvent`s
by hand for an installed-agent reviewer. Their request fields differ only
in these places:

| Field | Advisor | Summary | Vision |
|---|---|---|---|
| messages | system + one user transcript | system + history messages | system + one user (text + image) |
| `tool_choice` | `None` | `None` | `None` |
| `max_output_tokens` | set | set | set |
| `reasoning` | set | — | — |
| attempt purpose | `Ordinary` | idle-compaction or ordinary | `Ordinary` |
| deadline | tool context | own timeout | tool context |
| accepted finish | Stop, Length | Stop | Stop, Length |
| output bound | chars | chars | chars |

## Goals / Non-Goals

- Goals: one fold for provider and external one-shot calls; every field
  above representable; drop-safe cancellation; per-call instructions for
  external backends.
- Non-Goals:
  - Streaming deltas to the caller. Add an observer later if needed.
  - Tool use, retries, caching, and context planning. The caller sizes its
    input.
  - Multi-message input to an external backend. A CLI takes one input; the
    caller renders a transcript itself, as the advisor already does.

## Decisions

### API

```rust
pub enum OneShotExecutor {
    Provider { provider: Arc<dyn Provider>, model: ModelId },
    #[cfg(feature = "external-agent")]
    External(SharedExternalAgentBackend),
}

pub struct OneShotRequest {
    pub instructions: String,            // system message / external instructions
    pub messages: Vec<Message>,          // non-system input
    pub max_output_tokens: Option<u32>,  // provider only
    pub reasoning: Option<ReasoningConfig>, // provider only
    pub purpose: ProviderAttemptPurpose, // provider only
    pub output_char_limit: Option<usize>,
    pub session: SessionId,
    pub request: RequestId,
    pub attempt: AttemptId,
    pub cancel: Cancellation,            // parent; the call cancels a child
    pub deadline: Deadline,
    pub clock: Arc<dyn Clock>,
}

pub struct OneShotAnswer { pub text: String, pub finish: OneShotFinish }
pub enum OneShotFinish { Stopped, Truncated }
pub enum OneShotError {
    Cancelled, TimedOut, ToolCall, Filtered, Empty, Unfinished,
    UnsupportedInput(&'static str), Failed(RuntimeError),
}

pub async fn run_one_shot(
    executor: &OneShotExecutor,
    request: OneShotRequest,
    usage: &mut UsageDelta,
) -> Result<OneShotAnswer, OneShotError>;
```

The request gets a constructor with the common defaults. Callers keep their
own user-visible messages by matching on `OneShotError`.

### Provider path

The provider receives `[system(instructions)] + messages`, with
`tool_choice = None`, no tools, and `cache_identity = None`.
- `TextDelta` accumulates up to `output_char_limit` and keeps reading, so
  usage and the finish reason still arrive.
- `Usage` merges into the caller's accumulator.
- `ToolCallDelta` or `Finish(ToolCalls)` → `ToolCall`.
- `Stop` → `Stopped`; `Length` → `Truncated`.
- `ContentFilter` → `Filtered`; `Cancelled` → `Cancelled`.
- `Error`, or no finish at all → `Unfinished`.
- `ProviderStreamEvent::Error` → `Failed`.

### External path

The input must be exactly one user message, otherwise `UnsupportedInput`.
The request is `ExternalTurnRequest::new(input, turn, child_cancel)` with
`instructions = Some(instructions)`, no resume, default capabilities, no
bridge, and a disabled tool dispatch. `turn` is derived from the request id.
- `Text` accumulates.
- `Usage` merges.
- `Completed` → `Stopped`.
- `Failed { message }` → `Failed`.
- A stream that ends without a terminal event → `Unfinished`.
- `Reasoning`, `ToolInvoked`, `ToolCompleted`, `Diagnostic`, and
  `SessionStarted` are ignored. Whether the backend runs its own tools is
  decided by how the host constructed it; observations are not answers.

### Shared

- Non-whitespace text is required; otherwise `Empty`.
- The call races parent cancellation (`Cancelled`) against the deadline,
  which cancels the child with `Timeout` and returns `TimedOut`.
- A drop guard cancels the child. Dropping the future therefore stops the
  provider stream or the CLI process, and leaves the caller's usage
  accumulator holding whatever was reported.

### `ExternalTurnRequest`

The type becomes `#[non_exhaustive]` with `new(input, turn, cancel)`, which
defaults `resume: None`, `capabilities: Arc::default()`, `bridge: None`,
`tools: ExternalToolDispatch::disabled()`, and `instructions: None`. Fields
stay `pub` for assignment. The turn driver keeps building its request
in-crate and sets `instructions: None`.

### Instruction precedence in CLI backends

Instructions configured on the backend come first, then
`request.instructions`, joined by a blank line. Claude Code receives them
with `--append-system-prompt`. Codex has no such flag, so they are prefixed
to the input text.

## Risks / Trade-offs

- `#[non_exhaustive]` breaks external struct literals once (Smith has six).
  It is pre-1.0 and coordinated in the same release, and it prevents the
  next field from breaking anyone.
- Ignoring backend tool observations means a CLI in read-only mode may read
  files while answering. The host chooses that when it builds the backend.

## Migration Plan

Release v0.5.0 and move Smith's pin. Smith then:
- replaces its literals with `new`;
- routes the advisor, summary, and vision through `run_one_shot`;
- reads `request.instructions` in its CLI backend, keeping its settings
  instructions first;
- maps `OneShotError` to its existing messages.

## Open Questions

None.
