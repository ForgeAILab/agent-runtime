# Public Contract Baseline

Captured at Agent Runtime commit `50f230e4ca8ba318fd4c5c84df0876051c54d93e`
before durable-child implementation.

- `ChildRuntimeFactory::child_builder(&ChildSpec) -> RuntimeBuilder` supplies
  the child composition; the coordinator unconditionally clears its session
  and checkpoint stores.
- `DelegationCoordinator::new` is synchronous and creates an empty in-memory
  child map. Its operations are `spawn`, `list`, `status`, `result`,
  `task_outcome`, `wait`, `follow_up`, and `stop`.
- `ChildState` is `Running | Idle | Stopped | Failed`; `ChildStatus` carries
  child/parent IDs, workspace, task usage, latest result, and artifacts but no
  child-session identity or durability.
- `Runtime::start_child_session` excludes every parented session from snapshot
  and checkpoint loading.
- Event schema version 8 carries `ChildSpawned`, `ChildProgress`,
  `ChildNeedsInput`, `ChildCompleted`, `ChildStopped`, and `ChildFailed`.
- `agent-runtime-testkit::conformance::delegation` proves in-process
  follow-ups reuse the same handle/history and enforce the cumulative turn
  cap; no cross-process child recovery fixture exists.

The approved change explicitly rebases the completed-but-unarchived
`add-agent-delegation-runtime-2026-07-26` behavior: stable IDs, depth-one
authorization, safe parent delivery, and in-process follow-up remain the
compatibility floor while process-ephemeral persistence is superseded.
