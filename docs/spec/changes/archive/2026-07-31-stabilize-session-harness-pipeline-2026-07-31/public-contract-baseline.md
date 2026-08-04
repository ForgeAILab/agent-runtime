# Public Contract Baseline

Recorded before Stage 2 implementation on 2026-07-31.

## Runtime session facade

```rust
pub fn SessionHandle::send(&self, input: UserInput) -> TurnId;
pub async fn SessionHandle::run(&self, input: UserInput) -> TurnId;
pub fn SessionHandle::cancel(&self, reason: CancelReason);
pub async fn SessionHandle::shutdown(&self) -> Result<(), RuntimeError>;
```

`send` mints a `TurnId` even when shutdown has begun, `run` cannot expose a
completion result, and `cancel` permanently cancels the root session token.
There is no turn-local handle in the baseline.

## Provider-output events

`agent_runtime_core::event::SCHEMA_VERSION` is `4`.

```rust
RuntimeEvent::TextDelta { text: String }
RuntimeEvent::ReasoningDelta { text: String, redacted: bool }
RuntimeEvent::ProviderAttemptFinished {
    attempt: AttemptId,
    finish: FinishReason,
    retryable: bool,
}
```

The deltas have no request or attempt identity and there is no explicit
speculative-output commit/discard event. The exact JSON form is frozen in
`agent-runtime-testkit/src/conformance/fixtures/event-envelope-v4.json`.

## Tool and approval facade

```rust
pub trait Tool {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn input_schema(&self) -> Value;
    fn effects(&self) -> ToolEffects;
    fn spec(&self) -> ToolSpec;
    async fn invoke(
        &self,
        arguments: Value,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError>;
}

pub trait ApprovalPolicy {
    async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision;
}
```

Authority is derived from static `ToolEffects`; approval receives validated
arguments but no prepared fingerprint, concrete resource, permission set,
deadline, or cancellation handle.

### Approved migration contract

The prepared-invocation migration replaces the facade above with:

- `Tool::spec`, `Tool::prepare`, and `Tool::invoke(PreparedToolCall, ...)`;
- one immutable prepared object binding canonical arguments, exact resource,
  required typed permissions, scheduler effects, approval display, and a
  fingerprint;
- the invariant `prepared.required_permissions ⊆
  spec.permission_upper_bound`;
- authorization, approval display, scheduling, and invocation consuming that
  same verified object.

`LegacyTool` is a bounded source-compatibility adapter only. Its unit
`Effect::Read` is conservatively interpreted as broad workspace-root
`fs.read`, because an unspecified read cannot safely be treated as
authority-free. Static write scopes map to `fs.write`; process and network
effects map to their corresponding typed permissions. The legacy adapter
never claims argument-specific narrowing. The compatibility authority allows
a workspace-scoped `fs.read`-only request without HITL and requires approval
for legacy write/process/network requests; mixed requests still require
approval.

An approval edit is not a mutation of an authorized action and never reuses
the prior eligible grant. Edited arguments restart schema validation,
preparation, authorization, and (when required) approval with a newly
fingerprinted `PreparedToolCall`.

## Persistence and checkpointing

`StartSession` uses command schema version `1`. `SessionSnapshot` persists
history, usage, identity, manifests, and `updated`. There is no protected
checkpoint schema or `CheckpointStore` in the baseline. Observability events
are the only versioned streaming schema and are explicitly insufficient for
exact mid-turn recovery.
