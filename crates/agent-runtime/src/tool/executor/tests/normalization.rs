use super::*;
use agent_runtime_core::clock::Timestamp;
use std::sync::atomic::AtomicU64;

#[derive(Debug, Default)]
struct HookClock(AtomicU64);
impl Clock for HookClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Envelope,
    Fail,
    Invalid,
    Cancel,
    Expire,
}

#[derive(Debug)]
struct NormalizingTool {
    mode: Mode,
    normalized: AtomicUsize,
    prepared: AtomicUsize,
    invoked: AtomicUsize,
    cancel: Cancellation,
    clock: Arc<HookClock>,
}

#[async_trait]
impl Tool for NormalizingTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            "canonical",
            "canonical fixture",
            json!({"type":"object", "properties":{"path":{"type":"string"}}, "required":["path"], "additionalProperties":false}),
            ToolEffects::new(vec![]),
        )
    }

    fn normalize_arguments(&self, mut arguments: Value) -> Result<Value, RuntimeError> {
        self.normalized.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            Mode::Fail => return Err(RuntimeError::tool("normalization failed")),
            Mode::Invalid => return Ok(json!({"path": 42})),
            Mode::Cancel => self
                .cancel
                .cancel(agent_runtime_core::cancel::CancelReason::UserRequested),
            Mode::Expire => {
                self.clock.0.store(100, Ordering::SeqCst);
            }
            Mode::Envelope => {}
        }
        Ok(arguments
            .as_object_mut()
            .and_then(|o| o.remove("parameters"))
            .unwrap_or(arguments))
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        Ok(PreparedToolCall::from_static_effects(
            ctx.call_id.clone(),
            &self.spec(),
            arguments,
            ctx.workspace.root(),
        ))
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.invoked.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::json(prepared.into_arguments()))
    }
}

fn fixture(mode: Mode) -> (ToolExecutor, Arc<NormalizingTool>) {
    let tool = Arc::new(NormalizingTool {
        mode,
        normalized: AtomicUsize::new(0),
        prepared: AtomicUsize::new(0),
        invoked: AtomicUsize::new(0),
        cancel: Cancellation::new(),
        clock: Arc::new(HookClock::default()),
    });
    let mut registry = ToolRegistry::new();
    registry.register(tool.clone()).unwrap();
    let executor = ToolExecutor::new(
        registry.seal(),
        Arc::new(AllowAll),
        Arc::new(WsRoot),
        tool.clock.clone(),
        10_000,
        ConflictPolicy::ScopeOverlap,
        empty_security_config(),
    );
    (executor, tool)
}

async fn run_path(
    executor: &ToolExecutor,
    once: bool,
    call: &ToolCall,
    cancel: &Cancellation,
    deadline: Deadline,
) -> ToolResultBlock {
    let request = RequestId::new("request");
    let session = SessionId::new("session");
    if once {
        match executor
            .prepare_and_authorize_once(
                call,
                call.arguments.clone(),
                PreparationAuthorizationContext::new(&request, &session, None, cancel, deadline),
            )
            .await
        {
            PreparedAuthorization::Ready(ready) => {
                executor.invoke_one(ready, &request, cancel, deadline).await
            }
            PreparedAuthorization::Rejected(result) => result,
            PreparedAuthorization::AwaitingApproval(_) => panic!("pure fixture needs no approval"),
        }
    } else {
        executor
            .execute(
                std::slice::from_ref(call),
                &request,
                &session,
                cancel,
                deadline,
            )
            .await
            .remove(0)
    }
}

#[tokio::test]
async fn both_paths_normalize_once_before_validation_and_never_fall_back() {
    for once in [false, true] {
        for mode in [Mode::Envelope, Mode::Fail, Mode::Invalid] {
            let (executor, tool) = fixture(mode);
            let input = call(
                "canonical",
                "call",
                json!({"parameters":{"path":"out.txt"}}),
            );
            let result = run_path(&executor, once, &input, &tool.cancel, Deadline::never()).await;
            let valid = matches!(mode, Mode::Envelope);
            assert_eq!(result.is_error, !valid);
            assert_eq!(tool.normalized.load(Ordering::SeqCst), 1);
            assert_eq!(tool.prepared.load(Ordering::SeqCst), usize::from(valid));
            assert_eq!(tool.invoked.load(Ordering::SeqCst), usize::from(valid));
            if valid {
                assert_eq!(result.content[0].as_text(), Some(r#"{"path":"out.txt"}"#));
            } else if matches!(mode, Mode::Fail) {
                assert!(
                    result.content[0]
                        .as_text()
                        .unwrap()
                        .contains("normalization failed")
                );
            }
        }
    }
}

#[tokio::test]
async fn both_paths_check_cancellation_and_deadlines_around_the_hook() {
    for once in [false, true] {
        for before in [false, true] {
            for mode in [Mode::Cancel, Mode::Expire] {
                let (executor, tool) = fixture(mode);
                if before {
                    match mode {
                        Mode::Cancel => tool
                            .cancel
                            .cancel(agent_runtime_core::cancel::CancelReason::UserRequested),
                        Mode::Expire => {
                            tool.clock.0.store(100, Ordering::SeqCst);
                        }
                        _ => unreachable!(),
                    }
                }
                let input = call(
                    "canonical",
                    "call",
                    json!({"parameters":{"path":"out.txt"}}),
                );
                let result = run_path(
                    &executor,
                    once,
                    &input,
                    &tool.cancel,
                    Deadline::at(Timestamp(100)),
                )
                .await;
                assert!(result.is_error);
                let text = result.content[0].as_text().unwrap();
                assert!(text.contains(if matches!(mode, Mode::Cancel) {
                    "cancelled"
                } else {
                    "deadline elapsed"
                }));
                assert_eq!(tool.normalized.load(Ordering::SeqCst), usize::from(!before));
                assert_eq!(tool.prepared.load(Ordering::SeqCst), 0);
                assert_eq!(tool.invoked.load(Ordering::SeqCst), 0);
            }
        }
    }
}

#[derive(Debug)]
struct ParametersTool;
#[async_trait]
impl Tool for ParametersTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            "parameters",
            "legitimate canonical parameters property",
            json!({"type":"object", "properties":{"parameters":{"type":"object"}}, "required":["parameters"], "additionalProperties":false}),
            ToolEffects::new(vec![]),
        )
    }
    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        _ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        Ok(ToolOutcome::json(prepared.into_arguments()))
    }
}

#[tokio::test]
async fn identity_hook_preserves_a_legitimate_parameters_property() {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(ParametersTool)).unwrap();
    let executor = ToolExecutor::new(
        registry.seal(),
        Arc::new(AllowAll),
        Arc::new(WsRoot),
        Arc::new(SystemClock),
        10_000,
        ConflictPolicy::ScopeOverlap,
        empty_security_config(),
    );
    for once in [false, true] {
        let input = call(
            "parameters",
            "call",
            json!({"parameters":{"path":"out.txt"}}),
        );
        let result = run_path(
            &executor,
            once,
            &input,
            &Cancellation::new(),
            Deadline::never(),
        )
        .await;
        assert!(!result.is_error);
        assert_eq!(
            result.content[0].as_text(),
            Some(r#"{"parameters":{"path":"out.txt"}}"#)
        );
    }
}

#[tokio::test]
async fn exact_prepared_reauthorization_does_not_call_the_hook_or_prepare_again() {
    let (executor, tool) = fixture(Mode::Envelope);
    let request = RequestId::new("request");
    let session = SessionId::new("session");
    let input = call(
        "canonical",
        "call",
        json!({"parameters":{"path":"out.txt"}}),
    );
    let PreparedAuthorization::Ready(ready) = executor
        .prepare_and_authorize_once(
            &input,
            input.arguments.clone(),
            PreparationAuthorizationContext::new(
                &request,
                &session,
                None,
                &tool.cancel,
                Deadline::never(),
            ),
        )
        .await
    else {
        panic!("ready action");
    };
    let prepared: PreparedToolCall =
        serde_json::from_value(serde_json::to_value(&ready.prepared).unwrap()).unwrap();
    assert_eq!(prepared.arguments(), &json!({"path":"out.txt"}));
    let fingerprint = prepared.fingerprint().clone();
    // Recovery installs a hook that now fails. Exact prepared authority survives.
    let (recovery, changed_tool) = fixture(Mode::Fail);
    let PreparedAuthorization::Ready(ready) = recovery
        .reauthorize_prepared(
            prepared,
            &session,
            None,
            &changed_tool.cancel,
            Deadline::never(),
        )
        .await
    else {
        panic!("ready recovery");
    };
    assert_eq!(ready.prepared.fingerprint(), &fingerprint);
    let result = recovery
        .invoke_one(ready, &request, &changed_tool.cancel, Deadline::never())
        .await;
    assert!(!result.is_error);
    assert_eq!(changed_tool.normalized.load(Ordering::SeqCst), 0);
    assert_eq!(changed_tool.prepared.load(Ordering::SeqCst), 0);
    assert_eq!(changed_tool.invoked.load(Ordering::SeqCst), 1);
}

#[derive(Debug)]
struct NormalizedEditTool {
    inner: ExactEditTool,
    normalizations: AtomicUsize,
}
#[async_trait]
impl Tool for NormalizedEditTool {
    fn spec(&self) -> ToolSpec {
        self.inner.spec()
    }
    fn normalize_arguments(&self, mut arguments: Value) -> Result<Value, RuntimeError> {
        self.normalizations.fetch_add(1, Ordering::SeqCst);
        Ok(arguments
            .as_object_mut()
            .and_then(|object| object.remove("parameters"))
            .unwrap_or(arguments))
    }
    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        self.inner.prepare(arguments, ctx).await
    }
    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.inner.invoke(prepared, ctx).await
    }
}

#[tokio::test]
async fn execute_approval_edits_normalize_again_and_replace_authority() {
    let invoked = Arc::new(Mutex::new(Vec::new()));
    let resources = Arc::new(Mutex::new(Vec::new()));
    let approvals = Arc::new(Mutex::new(Vec::new()));
    let tool = Arc::new(NormalizedEditTool {
        inner: ExactEditTool {
            invoked_paths: invoked.clone(),
        },
        normalizations: AtomicUsize::new(0),
    });
    let mut registry = ToolRegistry::new();
    registry.register(tool.clone()).unwrap();
    let executor = ToolExecutor::new(
        registry.seal(),
        Arc::new(EditThenAllow {
            calls: AtomicUsize::new(0),
            seen: approvals.clone(),
            edited_arguments: json!({"parameters":{"path":"edited.txt"}}),
        }),
        Arc::new(WsRoot),
        Arc::new(SystemClock),
        10_000,
        ConflictPolicy::ScopeOverlap,
        recording_approval_security(resources.clone()),
    );
    let result = executor
        .execute(
            &[call(
                "exact_edit",
                "call",
                json!({"parameters":{"path":"original.txt"}}),
            )],
            &RequestId::new("r"),
            &SessionId::new("s"),
            &Cancellation::new(),
            Deadline::never(),
        )
        .await;
    assert!(!result[0].is_error);
    assert_eq!(tool.normalizations.load(Ordering::SeqCst), 2);
    let approvals = approvals.lock().unwrap();
    assert_eq!(approvals.len(), 2);
    assert_ne!(approvals[0].fingerprint(), approvals[1].fingerprint());
    assert_eq!(
        resources.lock().unwrap().as_slice(),
        [
            SecurityResource::filesystem("/ws", vec!["original.txt".into()]),
            SecurityResource::filesystem("/ws", vec!["edited.txt".into()])
        ]
    );
    assert_eq!(invoked.lock().unwrap().as_slice(), ["/ws/edited.txt"]);
}

#[test]
fn stream_representability_remains_before_normalization() {
    let (executor, tool) = fixture(Mode::Envelope);
    let error = executor
        .registry
        .validate_call(&call(
            "canonical",
            "call",
            json!([{"parameters":{"path":"out.txt"}}]),
        ))
        .unwrap_err();
    assert_eq!(
        error.kind,
        agent_runtime_core::provider::ProviderErrorKind::MalformedStream
    );
    assert_eq!(tool.normalized.load(Ordering::SeqCst), 0);
}
