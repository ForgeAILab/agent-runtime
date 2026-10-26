# Design: external agent backend

## Boundary

```rust
/// Executes one whole turn outside the runtime's provider/tool loop.
#[async_trait]
pub trait ExternalAgentBackend: Send + Sync + std::fmt::Debug {
    /// Runs one turn, streaming normalized agent events until a terminal one.
    async fn run_turn(
        &self,
        request: ExternalTurnRequest,
    ) -> Result<ExternalTurnStream, RuntimeError>;
}

pub struct ExternalTurnRequest {
    /// The user input for this turn, already resolved by the host.
    pub input: UserInput,
    /// Identity the backend reported previously, offered for continuation.
    pub resume: Option<ExternalSessionId>,
    /// Turn identity, for correlation in host logs.
    pub turn: TurnId,
    /// Cancelled when the turn is cancelled or the session shuts down.
    pub cancel: Cancellation,
}

pub type ExternalTurnStream =
    Pin<Box<dyn Stream<Item = ExternalAgentEvent> + Send>>;
```

## Normalized events

```rust
pub enum ExternalAgentEvent {
    /// The backend's own conversation identity for this turn.
    SessionStarted { session: ExternalSessionId },
    /// Assistant prose, incremental or whole.
    Text { text: String },
    /// Backend-visible reasoning, where it reports any.
    Reasoning { text: String },
    /// A tool the backend ran itself. Observation only.
    ToolInvoked { id: String, name: String, detail: serde_json::Value },
    /// That tool's outcome, as the backend reported it.
    ToolCompleted { id: String, ok: bool, detail: serde_json::Value },
    /// Token counts for this turn, with cache breakdown where known.
    Usage { usage: UsageDelta },
    /// Terminal: the turn produced this final assistant text.
    Completed,
    /// Terminal: the turn failed for a reason safe to show a host.
    Failed { message: String },
}
```

`Text` accumulates into exactly one canonical assistant message, appended when
`Completed` arrives. `Failed` appends nothing, so a partial answer never enters
history as if it were complete. Anything after a terminal event is dropped.

## Dispatch

`Driver` gains `external: Option<Arc<dyn ExternalAgentBackend>>`. `run_turn`
branches once, at the top:

- `None` -- build `TurnMachine` exactly as today. No behavior change.
- `Some(backend)` -- build `ExternalTurnMachine`, which owns the same
  `TurnMachineContext` (state, execution, emitter, minter, cancel, inbox, turn
  id) so turn identity, admission, and the event stream are shared rather than
  reimplemented.

The two machines never run within one turn, which is what keeps history
single-owner: either the provider loop appends messages or the external machine
does.

## Continuation identity

Stored in extension state under `EXTERNAL_AGENT_STATE_NAMESPACE` as a
`VersionedSessionState` holding `{ schema_version, session: ExternalSessionId }`.
Read before the turn to populate `ExternalTurnRequest::resume`; rewritten when
the backend reports a `SessionStarted` that differs from the stored one.

Advisory by construction: the runtime never validates the identity, and a
backend that starts fresh simply reports a different one.

## What an external turn does not do

No context plan, no provider attempt, no prompt-cache maintenance, no tool
dispatch, no retry. These belong to the loop the backend replaced. Emitting
provider-attempt events for a turn that made no provider call would make usage,
cache, and attempt accounting lie.

## Sensitivity

`ToolInvoked`/`ToolCompleted` detail is backend-reported and may contain
workspace content: it is recorded at the same sensitivity as tool output on the
direct path, and hosts bound it before display.
