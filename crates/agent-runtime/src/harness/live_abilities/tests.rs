use super::*;
use agent_runtime_ability::activation::FailClosedPolicy;
use agent_runtime_core::store::SessionStateSensitivity;

use crate::harness::{HarnessPipelineBuilder, QuestionnaireTool};

fn live_runtime_with_context(context: ActivationContext) -> LiveAbilityRuntime {
    let sealed = LiveAbilityRuntime::seal(
        vec![Arc::new(QuestionnaireTool::new())],
        Vec::new(),
        Vec::new(),
        Arc::new(CapabilityResolver::new()),
        Arc::new(FailClosedPolicy),
        context,
        ScopeInputs::new(),
        ActivationBudget::new(16_384, 8),
    )
    .expect("test ability registry seals");
    Arc::into_inner(sealed.runtime).expect("the test owns the only runtime reference")
}

#[tokio::test]
async fn completed_session_rebases_activation_when_interaction_readiness_changes() {
    let runtime = live_runtime_with_context(ActivationContext::new());
    let pipeline = HarnessPipelineBuilder::new()
        .seal()
        .expect("empty pipeline seals");
    let session = SessionId::new("session-rebase");
    let headless = runtime
        .derive_session(
            session.clone(),
            None,
            false,
            &pipeline,
            &BTreeMap::new(),
            false,
        )
        .await
        .expect("fresh headless scope derives");
    assert!(
        headless
            .descriptor_view
            .get(&RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME))
            .is_none()
    );

    let persisted = headless.persisted_state();
    assert_eq!(
        persisted.sensitivity,
        SessionStateSensitivity::RedactionSafe
    );
    let persisted_value: PersistedActivationState =
        serde_json::from_value(persisted.value.clone()).expect("activation state parses");
    assert_eq!(
        persisted_value.epochs[0],
        vec![(
            RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME),
            runtime
                .descriptors
                .get(&RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME))
                .unwrap()
                .payload()
                .content_revision()
                .clone(),
        )]
    );

    let extension = BTreeMap::from([(ACTIVATION_STATE_NAMESPACE.to_owned(), persisted)]);
    let strict = runtime
        .derive_session(session.clone(), None, true, &pipeline, &extension, false)
        .await;
    assert!(
        strict
            .expect_err("an in-flight restore must require the exact scoped view")
            .message
            .contains("different registry snapshot or scoped view")
    );

    let rebased = runtime
        .derive_session(session, None, true, &pipeline, &extension, true)
        .await
        .expect("a completed boundary may rebase onto current readiness");
    assert!(
        rebased
            .descriptor_view
            .get(&RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME))
            .is_some()
    );
    let state = rebased.state.lock().expect("activation state poisoned");
    assert_eq!(state.epochs.current().unwrap().index(), 1);
    assert!(
        state
            .epochs
            .current()
            .unwrap()
            .contains(&RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME))
    );
    assert!(
        !state
            .epochs
            .current()
            .unwrap()
            .contains(&RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME))
    );
    assert!(
        !state.initialized,
        "the next turn must rerun capability routing"
    );
}

#[tokio::test]
async fn capability_search_stages_only_authorized_materialized_cards_transactionally() {
    let runtime = live_runtime_with_context(ActivationContext::new());
    let pipeline = HarnessPipelineBuilder::new()
        .seal()
        .expect("empty pipeline seals");
    let session = runtime
        .derive_session(
            SessionId::new("session-search"),
            None,
            true,
            &pipeline,
            &BTreeMap::new(),
            false,
        )
        .await
        .expect("interactive scope derives");
    let emitter = EventEmitter::new(
        SessionId::new("session-search"),
        Arc::new(crate::ids::IdMinter::new()),
        Arc::new(agent_runtime_core::clock::SystemClock),
        Arc::from(Vec::<Arc<dyn agent_runtime_core::observer::EventObserver>>::new()),
        1,
        0,
    );
    let ask_id = RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME);
    let (retrieval, plan) = runtime.select(
        &session.descriptor_view,
        &RoutingQuery::derive("ask_user", Vec::<String>::new()),
        &[RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME)],
        8,
    );
    assert!(
        !plan.bindings.is_empty(),
        "test query must select questionnaire: retrieval={retrieval:?} plan={plan:?}"
    );

    let first_call = ToolCallId::new("search-1");
    let outcome = runtime
        .search_and_stage(
            &session,
            &first_call,
            &serde_json::json!({"query": "ask_user"}),
            &emitter,
            &None,
        )
        .expect("authorized search succeeds");
    assert!(
        outcome
            .value
            .get("cards")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|cards| !cards.is_empty())
    );
    {
        let state = session.state.lock().expect("activation state poisoned");
        assert!(state.staged[&first_call].contains_key(&ask_id));
        assert!(state.pending.is_empty());
        assert!(!state.epochs.current().unwrap().contains(&ask_id));
    }
    drop(
        session
            .search_stage_guard(&first_call)
            .expect("staging guard exists"),
    );
    assert!(
        session
            .state
            .lock()
            .expect("activation state poisoned")
            .staged
            .is_empty(),
        "dropping before canonical commit rolls the stage back"
    );

    let second_call = ToolCallId::new("search-2");
    runtime
        .search_and_stage(
            &session,
            &second_call,
            &serde_json::json!({"query": "ask_user"}),
            &emitter,
            &None,
        )
        .expect("a rolled-back capability can be searched again");
    let mut guard = session
        .search_stage_guard(&second_call)
        .expect("second staging guard exists");
    guard
        .commit()
        .expect("canonical result commit can promote stage");
    {
        let state = session.state.lock().expect("activation state poisoned");
        assert!(state.staged.is_empty());
        assert!(state.pending.contains_key(&ask_id));
        assert!(!state.epochs.current().unwrap().contains(&ask_id));
    }
    guard.finish();
    assert!(
        session
            .state
            .lock()
            .expect("activation state poisoned")
            .pending
            .contains_key(&ask_id),
        "a finished canonical commit leaves activation pending for the next boundary"
    );
}

#[tokio::test]
async fn capability_search_returns_no_cards_or_stage_when_policy_denies_activation() {
    let ask_id = RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME);
    let runtime = live_runtime_with_context(ActivationContext::new().with_denied([ask_id.clone()]));
    let pipeline = HarnessPipelineBuilder::new()
        .seal()
        .expect("empty pipeline seals");
    let session = runtime
        .derive_session(
            SessionId::new("session-denied-search"),
            None,
            true,
            &pipeline,
            &BTreeMap::new(),
            false,
        )
        .await
        .expect("interactive scope derives");
    let emitter = EventEmitter::new(
        SessionId::new("session-denied-search"),
        Arc::new(crate::ids::IdMinter::new()),
        Arc::new(agent_runtime_core::clock::SystemClock),
        Arc::from(Vec::<Arc<dyn agent_runtime_core::observer::EventObserver>>::new()),
        1,
        0,
    );
    let error = runtime
        .search_and_stage(
            &session,
            &ToolCallId::new("search-denied"),
            &serde_json::json!({"query": "ask_user"}),
            &emitter,
            &None,
        )
        .expect_err("policy-denied activation must not return a discovery card");
    assert!(error.message.contains("denied"));
    let state = session.state.lock().expect("activation state poisoned");
    assert!(state.staged.is_empty());
    assert!(state.pending.is_empty());
    assert!(!state.epochs.current().unwrap().contains(&ask_id));
}
