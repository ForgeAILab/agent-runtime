use super::*;
use agent_runtime_ability::activation::FailClosedPolicy;
use agent_runtime_core::store::SessionStateSensitivity;

use crate::harness::{HarnessPipelineBuilder, QuestionnaireTool};

fn live_runtime_with_context(context: ActivationContext) -> LiveAbilityRuntime {
    live_runtime_with_budget(context, ActivationBudget::new(16_384, 8))
}

fn live_runtime_with_budget(
    context: ActivationContext,
    budget: ActivationBudget,
) -> LiveAbilityRuntime {
    let sealed = LiveAbilityRuntime::seal(
        vec![Arc::new(QuestionnaireTool::new())],
        Vec::new(),
        Vec::new(),
        Arc::new(CapabilityResolver::new()),
        Arc::new(FailClosedPolicy),
        context,
        ScopeInputs::new(),
        budget,
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
        [CAPABILITY_ACTIVATE_TOOL_NAME, CAPABILITY_SEARCH_TOOL_NAME]
            .into_iter()
            .map(|name| {
                let id = RegistryId::tool(name);
                let revision = runtime
                    .descriptors
                    .get(&id)
                    .unwrap()
                    .payload()
                    .content_revision()
                    .clone();
                (id, revision)
            })
            .collect::<Vec<_>>()
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
async fn concurrent_capability_searches_never_stage_the_same_ability_twice() {
    let runtime = live_runtime_with_context(ActivationContext::new());
    let pipeline = HarnessPipelineBuilder::new()
        .seal()
        .expect("empty pipeline seals");
    let session = runtime
        .derive_session(
            SessionId::new("session-search-batch"),
            None,
            true,
            &pipeline,
            &BTreeMap::new(),
            false,
        )
        .await
        .expect("interactive scope derives");
    let emitter = EventEmitter::new(
        SessionId::new("session-search-batch"),
        Arc::new(crate::ids::IdMinter::new()),
        Arc::new(agent_runtime_core::clock::SystemClock),
        Arc::from(Vec::<Arc<dyn agent_runtime_core::observer::EventObserver>>::new()),
        1,
        0,
    );
    let ask_id = RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME);

    // One assistant message can emit several searches. They stage inline,
    // before any of them commits, so the second must observe the first.
    let first_call = ToolCallId::new("search-batch-1");
    let second_call = ToolCallId::new("search-batch-2");
    runtime
        .search_and_stage(
            &session,
            &first_call,
            &serde_json::json!({"query": "ask_user"}),
            &emitter,
            &None,
        )
        .expect("the first search of the batch stages");
    let outcome = runtime
        .search_and_stage(
            &session,
            &second_call,
            &serde_json::json!({"query": "ask_user"}),
            &emitter,
            &None,
        )
        .expect("the second search of the batch succeeds");
    assert_eq!(
        outcome.value["staged"],
        serde_json::json!([]),
        "an ability another open transaction holds must not be staged again"
    );
    assert_eq!(
        outcome.value["already_available"],
        serde_json::json!([ask_id.qualified()])
    );

    // Both results commit in canonical order; neither conflicts.
    let mut first_stage = session
        .search_stage_guard(&first_call)
        .expect("first staging guard exists");
    first_stage
        .commit()
        .expect("first commit promotes the stage");
    first_stage.finish();
    let mut second_stage = session
        .search_stage_guard(&second_call)
        .expect("second staging guard exists");
    second_stage
        .commit()
        .expect("a search that staged nothing new must not conflict with pending");
    second_stage.finish();
    {
        let state = session.state.lock().expect("activation state poisoned");
        assert!(state.staged.is_empty());
        assert!(state.pending.contains_key(&ask_id));
    }

    // Once pending becomes active, a later search reports it as available
    // rather than re-activating it into a duplicate epoch entry.
    runtime.apply_pending(&session, &emitter, &None);
    let third_call = ToolCallId::new("search-batch-3");
    let outcome = runtime
        .search_and_stage(
            &session,
            &third_call,
            &serde_json::json!({"query": "ask_user"}),
            &emitter,
            &None,
        )
        .expect("searching for an active ability succeeds");
    assert_eq!(outcome.value["staged"], serde_json::json!([]));
    assert_eq!(
        outcome.value["already_available"],
        serde_json::json!([ask_id.qualified()])
    );
    let mut third_stage = session
        .search_stage_guard(&third_call)
        .expect("third staging guard exists");
    third_stage
        .commit()
        .expect("an already-active result commits cleanly");
    third_stage.finish();
    let state = session.state.lock().expect("activation state poisoned");
    assert!(state.pending.is_empty());
    assert!(state.epochs.current().unwrap().contains(&ask_id));
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

/// Mid-turn staging spends what the activated set has already spent.
///
/// The context planner enforces one capability budget over *every* activated
/// schema, and rejects the whole turn when the total overflows. So a
/// `registry.search` that stages a capability must charge it against the
/// budget left, not the budget as if nothing were active: a schema that fits
/// on its own but not alongside what is already bound has to be refused here,
/// where the model still gets a usable "nothing fit" answer, rather than
/// downstream where it is an unrecoverable turn failure.
#[tokio::test]
async fn staging_charges_a_candidate_against_the_budget_the_active_set_leaves() {
    let search_id = RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME);
    let ask_id = RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME);
    let query = RoutingQuery::derive("ask_user", Vec::<String>::new());

    let probe = live_runtime_with_context(ActivationContext::new());
    let pipeline = HarnessPipelineBuilder::new()
        .seal()
        .expect("empty pipeline seals");
    let session = probe
        .derive_session(
            SessionId::new("session-budget-probe"),
            None,
            true,
            &pipeline,
            &BTreeMap::new(),
            false,
        )
        .await
        .expect("interactive scope derives");
    let cost = |id: &RegistryId| {
        session
            .descriptor_view
            .get(id)
            .expect("the descriptor is in view")
            .payload()
            .context_cost()
            .total_tokens()
    };
    let (active_cost, candidate_cost) = (cost(&search_id), cost(&ask_id));
    assert!(
        active_cost > 0 && candidate_cost > 0,
        "the fixture must have real costs to budget against"
    );

    // Exactly enough for both: staging still admits the candidate, so the
    // subtraction cannot be an off-by-one that starves a legitimate search.
    let roomy = live_runtime_with_budget(
        ActivationContext::new(),
        ActivationBudget::new(active_cost + candidate_cost, 8),
    );
    let (_, plan) = roomy.select(
        &session.descriptor_view,
        &query,
        std::slice::from_ref(&search_id),
        8,
    );
    assert!(
        plan.bindings.iter().any(|b| b.descriptor.id() == &ask_id),
        "a candidate that fits beside the active set must still bind: {plan:?}"
    );

    // One token short of both, but still roomier than the candidate alone —
    // the exact shape that used to bind and then fail the turn downstream.
    let tight = live_runtime_with_budget(
        ActivationContext::new(),
        ActivationBudget::new(active_cost + candidate_cost - 1, 8),
    );
    let (_, plan) = tight.select(&session.descriptor_view, &query, &[search_id], 8);
    assert!(
        plan.bindings.is_empty(),
        "a candidate that overflows the budget once the active set is charged \
         must be refused at staging, not left for the planner: {plan:?}"
    );
}

fn test_emitter() -> EventEmitter {
    EventEmitter::new(
        SessionId::new("capabilities"),
        Arc::new(crate::ids::IdMinter::new()),
        Arc::new(agent_runtime_core::clock::SystemClock),
        Arc::from(Vec::<Arc<dyn agent_runtime_core::observer::EventObserver>>::new()),
        1,
        0,
    )
}

async fn derive(
    runtime: &LiveAbilityRuntime,
    persisted: Option<VersionedSessionState>,
    rebase: bool,
) -> Result<SessionAbilities, RuntimeError> {
    let pipeline = HarnessPipelineBuilder::new().seal().unwrap();
    let extension = persisted
        .map(|state| BTreeMap::from([(ACTIVATION_STATE_NAMESPACE.to_owned(), state)]))
        .unwrap_or_default();
    runtime
        .derive_session(
            SessionId::new("capabilities"),
            None,
            true,
            &pipeline,
            &extension,
            rebase,
        )
        .await
}

#[tokio::test]
async fn pins_are_present_in_first_epoch_and_do_not_consume_candidate_slots() {
    let sealed = LiveAbilityRuntime::seal(
        vec![Arc::new(QuestionnaireTool::new())],
        Vec::new(),
        vec![Arc::new(CostedSkill)],
        Arc::new(CapabilityResolver::new()),
        Arc::new(FailClosedPolicy),
        ActivationContext::new(),
        ScopeInputs::new(),
        ActivationBudget::new(16_384, 1),
    )
    .unwrap();
    let mut runtime = Arc::into_inner(sealed.runtime).unwrap();
    let pin = RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME);
    runtime.set_pinned([pin.clone(), RegistryId::tool("unknown")]);
    let session = derive(&runtime, None, false).await.unwrap();
    assert!(session.active_ids().contains(&pin));
    assert_eq!(session.current_epoch().index(), 0);
    assert_eq!(session.materialized().unwrap().0.len(), 3);
    let active = session.active_ids();
    let query = RoutingQuery::derive("manual", Vec::<String>::new());
    let (_, plan) = runtime.select(&session.descriptor_view, &query, &active, 8);
    assert!(plan.bindings.is_empty());
    runtime.budget.max_candidates = 2;
    let (_, plan) = runtime.select(&session.descriptor_view, &query, &active, 8);
    assert_eq!(plan.bindings.len(), 1);
    assert_eq!(
        plan.bindings[0].descriptor.id(),
        &RegistryId::skill("manual")
    );
    runtime
        .ensure_initial_activation(&session, "manual", &test_emitter(), &None)
        .unwrap();
    assert!(session.active_ids().contains(&RegistryId::skill("manual")));
}

#[tokio::test]
async fn denied_pins_are_skipped_and_oversized_pins_fail_configuration() {
    let pin = RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME);
    let mut runtime =
        live_runtime_with_budget(ActivationContext::new(), ActivationBudget::new(1, 8));
    let tokens = runtime
        .descriptors
        .get(&pin)
        .unwrap()
        .payload()
        .context_cost()
        .total_tokens();
    runtime.set_pinned([pin.clone()]);
    let error = derive(&runtime, None, false).await.unwrap_err();
    assert!(error.message.contains("max_schema_tokens"));
    assert!(error.message.contains(&tokens.to_string()));
    runtime.scope_inputs = ScopeInputs::new().deny_pattern("tool:ask*").unwrap();
    let session = derive(&runtime, None, false).await.unwrap();
    assert!(!session.active_ids().contains(&pin));
    assert_eq!(
        session
            .capability_catalog()
            .iter()
            .find(|row| row.id == pin)
            .unwrap()
            .state,
        crate::capability::CapabilityState::Denied
    );
}

#[tokio::test]
async fn restore_and_rebase_add_missing_pins_and_rebase_drops_denied_pins() {
    let mut runtime = live_runtime_with_context(ActivationContext::new());
    let old = derive(&runtime, None, false)
        .await
        .unwrap()
        .persisted_state();
    let pin = RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME);
    runtime.set_pinned([pin.clone()]);
    let restored = derive(&runtime, Some(old.clone()), false).await.unwrap();
    assert!(restored.active_ids().contains(&pin));
    assert_eq!(restored.current_epoch().index(), 1);
    // Exercise the rebase path independently of exact-scope restoration.
    let rebased = derive(&runtime, Some(old), true).await.unwrap();
    assert!(rebased.active_ids().contains(&pin));
    runtime.scope_inputs = ScopeInputs::new().deny_id(pin.clone());
    let denied = derive(&runtime, Some(restored.persisted_state()), true)
        .await
        .unwrap();
    assert!(!denied.active_ids().contains(&pin));
}

#[tokio::test]
async fn browse_is_paged_authorized_and_never_stages_or_advances_epoch() {
    let mut runtime = live_runtime_with_context(ActivationContext::new());
    let session = derive(&runtime, None, false).await.unwrap();
    let outcome = runtime
        .search_and_stage(
            &session,
            &ToolCallId::new("list"),
            &serde_json::json!({}),
            &test_emitter(),
            &None,
        )
        .unwrap();
    assert_eq!(outcome.value["total"], 1);
    assert_eq!(outcome.value["listing"][0]["id"], "tool:ask_user");
    assert_eq!(outcome.value["listing"][0]["active"], false);
    assert_eq!(outcome.value["by_domain"]["tool"], 1);
    assert!(outcome.value["next_offset"].is_null());
    assert!(!session.has_staged_call(&ToolCallId::new("list")));
    assert_eq!(session.current_epoch().index(), 0);
    let page = runtime
        .search_and_stage(
            &session,
            &ToolCallId::new("page"),
            &serde_json::json!({"query":" ","offset":1,"domain":"tool"}),
            &test_emitter(),
            &None,
        )
        .unwrap();
    assert_eq!(page.value["listing"], serde_json::json!([]));
    let miss = runtime
        .search_and_stage(
            &session,
            &ToolCallId::new("miss"),
            &serde_json::json!({"query":"zzzyyyxxx"}),
            &test_emitter(),
            &None,
        )
        .unwrap();
    assert_eq!(miss.value["matched"], 0);
    assert!(miss.value["note"].as_str().unwrap().contains("Omit query"));
    assert_eq!(miss.value["by_domain"]["tool"], 1);
    runtime.scope_inputs = ScopeInputs::new().deny_pattern("tool:ask*").unwrap();
    let session = derive(&runtime, None, false).await.unwrap();
    let list = runtime
        .search_and_stage(
            &session,
            &ToolCallId::new("denied-list"),
            &serde_json::json!({}),
            &test_emitter(),
            &None,
        )
        .unwrap();
    assert_eq!(list.value["total"], 0);
    assert_eq!(list.value["by_domain"], serde_json::json!({}));
}

#[tokio::test]
async fn explicit_activation_is_partial_unbounded_by_cardinality_and_transactional() {
    let runtime =
        live_runtime_with_budget(ActivationContext::new(), ActivationBudget::new(16_384, 0));
    let session = derive(&runtime, None, false).await.unwrap();
    let call = ToolCallId::new("activate");
    let outcome = runtime.activate_and_stage(&session, &call, &serde_json::json!({"ids":["tool:ask_user","tool:unknown","tool:registry.search","tool:ask_user"]})).unwrap();
    assert_eq!(
        outcome.value["staged"],
        serde_json::json!(["tool:ask_user"])
    );
    assert_eq!(
        outcome.value["rejected"][0]["reason"],
        "unknown or not authorized"
    );
    assert_eq!(
        outcome.value["already_available"],
        serde_json::json!(["tool:registry.search", "tool:ask_user"])
    );
    assert!(!session.active_ids().contains(&RegistryId::tool("ask_user")));
    drop(session.search_stage_guard(&call).unwrap());
    assert!(!session.has_staged_call(&call));
    let outcome = runtime
        .activate_and_stage(
            &session,
            &call,
            &serde_json::json!({"ids":["tool:ask_user"]}),
        )
        .unwrap();
    assert_eq!(outcome.value["staged"].as_array().unwrap().len(), 1);
    let mut guard = session.search_stage_guard(&call).unwrap();
    guard.commit().unwrap();
    guard.finish();
    runtime.apply_pending(&session, &test_emitter(), &None);
    assert!(session.active_ids().contains(&RegistryId::tool("ask_user")));
    assert_eq!(session.current_epoch().index(), 1);
    let again = runtime
        .activate_and_stage(
            &session,
            &ToolCallId::new("again"),
            &serde_json::json!({"ids":["tool:ask_user"]}),
        )
        .unwrap();
    assert_eq!(
        again.value["already_available"],
        serde_json::json!(["tool:ask_user"])
    );
}

#[tokio::test]
async fn explicit_activation_rejects_schema_and_instruction_budget_without_failing_call() {
    let runtime = live_runtime_with_budget(ActivationContext::new(), ActivationBudget::new(1, 8));
    let session = derive(&runtime, None, false).await.unwrap();
    let outcome = runtime
        .activate_and_stage(
            &session,
            &ToolCallId::new("over"),
            &serde_json::json!({"ids":["tool:ask_user"]}),
        )
        .unwrap();
    assert!(outcome.value["staged"].as_array().unwrap().is_empty());
    assert!(
        outcome.value["rejected"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("max_schema_tokens")
    );
}

#[tokio::test]
async fn pinned_dependencies_materialize_and_unsatisfied_dependencies_name_both_ids() {
    use agent_runtime_ability::descriptor::DependencyRequirement;
    let tool: Arc<dyn Tool> = Arc::new(QuestionnaireTool::new());
    let descriptor =
        tool_ability(tool.clone())
            .descriptor()
            .with_dependency(DependencyRequirement::single(RegistryId::skill(
                "prerequisite",
            )));
    let sealed = LiveAbilityRuntime::seal(
        vec![tool],
        vec![descriptor],
        vec![Arc::new(agent_runtime_ability::Skill::inline(
            "prerequisite",
            "A reference",
            "required instructions",
        ))],
        Arc::new(CapabilityResolver::new()),
        Arc::new(FailClosedPolicy),
        ActivationContext::new(),
        ScopeInputs::new(),
        ActivationBudget::new(16_384, 0),
    )
    .unwrap();
    let mut runtime = Arc::into_inner(sealed.runtime).unwrap();
    runtime.set_pinned([RegistryId::tool("ask_user")]);
    let session = derive(&runtime, None, false).await.unwrap();
    assert!(
        session
            .active_ids()
            .contains(&RegistryId::skill("prerequisite"))
    );
    assert_eq!(session.materialized().unwrap().1.len(), 1);
    runtime.scope_inputs = ScopeInputs::new().deny_id(RegistryId::skill("prerequisite"));
    let error = derive(&runtime, None, false).await.unwrap_err();
    assert!(error.message.contains("ask_user"));
    assert!(error.message.contains("prerequisite"));
}

#[tokio::test]
async fn explicit_skill_instruction_budget_and_description_search_domain_filter() {
    let sealed = LiveAbilityRuntime::seal(
        Vec::new(),
        Vec::new(),
        vec![Arc::new(CostedSkill)],
        Arc::new(CapabilityResolver::new()),
        Arc::new(FailClosedPolicy),
        ActivationContext::new(),
        ScopeInputs::new(),
        ActivationBudget::new(16_384, 8).with_instruction_budget(1),
    )
    .unwrap();
    let runtime = Arc::into_inner(sealed.runtime).unwrap();
    let session = derive(&runtime, None, false).await.unwrap();
    runtime
        .ensure_initial_activation(&session, "eigenvectors", &test_emitter(), &None)
        .unwrap();
    assert!(!session.active_ids().contains(&RegistryId::skill("manual")));
    let explicit = runtime
        .activate_and_stage(
            &session,
            &ToolCallId::new("skill"),
            &serde_json::json!({"ids":["skill:manual"]}),
        )
        .unwrap();
    assert!(
        explicit.value["rejected"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("max_instruction_tokens")
    );
    let found = runtime.resolver.retrieve_descriptive(
        &session.descriptor_view,
        &RoutingQuery::derive("eigenvectors", Vec::<String>::new()),
    );
    assert_eq!(found.candidates.len(), 1);
    let excluded = runtime
        .search_and_stage(
            &session,
            &ToolCallId::new("domain"),
            &serde_json::json!({"query":"eigenvectors","domain":"tool"}),
            &test_emitter(),
            &None,
        )
        .unwrap();
    assert_eq!(excluded.value["matched"], 0);
    assert_eq!(excluded.value["by_domain"]["skill"], 1);
}

#[derive(Debug)]
struct CostedSkill;
impl agent_runtime_ability::Named for CostedSkill {
    fn name(&self) -> &str {
        "manual"
    }
}
impl Ability for CostedSkill {
    fn kind(&self) -> AbilityKind {
        AbilityKind::Skill
    }
    fn description(&self) -> &str {
        "A reference about eigenvectors"
    }
    fn descriptor(&self) -> AbilityDescriptor {
        AbilityDescriptor::new(
            AbilityKind::Skill,
            "manual",
            EntryProvenance::new(RegistrySource::Host, RegistryRevision::new("1")),
            "Manual",
            self.description(),
            RegistryRevision::new("1"),
        )
        .with_context_cost(ContextCost::new(0, 512))
        .with_affordances(["manual"])
    }
    fn materialize(&self) -> Result<Activated, agent_runtime_ability::activation::ActivationError> {
        Ok(Activated::SkillInstructions("Instructions".into()))
    }
}

#[tokio::test]
async fn historical_bootstrap_state_rebases_with_new_bootstrap_and_pins_without_revision_bump() {
    let mut runtime = live_runtime_with_context(ActivationContext::new());
    let mut persisted = derive(&runtime, None, false)
        .await
        .unwrap()
        .persisted_state();
    let mut value: PersistedActivationState =
        serde_json::from_value(persisted.value.clone()).unwrap();
    value.snapshot = "historical-registry".into();
    value.view = "historical-view".into();
    for epoch in &mut value.epochs {
        epoch.retain(|(id, _)| id != &RegistryId::tool(CAPABILITY_ACTIVATE_TOOL_NAME));
        for (_, revision) in epoch {
            *revision = RegistryRevision::new("historical-search-schema");
        }
    }
    persisted.value = serde_json::to_value(value).unwrap();
    runtime.set_pinned([RegistryId::tool("ask_user")]);
    let resumed = derive(&runtime, Some(persisted), true).await.unwrap();
    assert!(resumed.active_ids().contains(&RegistryId::tool("ask_user")));
    assert!(
        resumed
            .active_ids()
            .contains(&RegistryId::tool(CAPABILITY_ACTIVATE_TOOL_NAME))
    );
    assert_eq!(
        resumed.persisted_state().revision,
        RegistryRevision::new("live-ability-state-2")
    );
    assert!(resumed.rebased);
}

#[test]
fn both_bootstrap_names_are_protected_and_marker_tools_are_permission_free() {
    for tool in [
        Arc::new(CapabilitySearchTool) as Arc<dyn Tool>,
        Arc::new(CapabilityActivateTool) as Arc<dyn Tool>,
    ] {
        assert!(tool.spec().effects.is_empty());
        let error = LiveAbilityRuntime::seal(
            vec![tool],
            Vec::new(),
            Vec::new(),
            Arc::new(CapabilityResolver::new()),
            Arc::new(FailClosedPolicy),
            ActivationContext::new(),
            ScopeInputs::new(),
            ActivationBudget::new(16_384, 8),
        )
        .err()
        .unwrap();
        assert!(error.message.contains("protected"));
    }
}

#[tokio::test]
async fn pinned_and_explicit_dependencies_choose_an_alternative_that_fits_remaining_tokens() {
    use agent_runtime_ability::descriptor::DependencyRequirement;
    let tool: Arc<dyn Tool> = Arc::new(QuestionnaireTool::new());
    let descriptor =
        tool_ability(tool.clone())
            .descriptor()
            .with_dependency(DependencyRequirement::any_of([
                RegistryId::skill("manual"),
                RegistryId::skill("small"),
            ]));
    let sealed = LiveAbilityRuntime::seal(
        vec![tool],
        vec![descriptor],
        vec![
            Arc::new(CostedSkill),
            Arc::new(agent_runtime_ability::Skill::inline(
                "small",
                "Small prerequisite",
                "body",
            )),
        ],
        Arc::new(CapabilityResolver::new()),
        Arc::new(FailClosedPolicy),
        ActivationContext::new(),
        ScopeInputs::new(),
        ActivationBudget::new(16_384, 0).with_instruction_budget(1),
    )
    .unwrap();
    let mut runtime = Arc::into_inner(sealed.runtime).unwrap();
    let explicit = derive(&runtime, None, false).await.unwrap();
    let result = runtime
        .activate_and_stage(
            &explicit,
            &ToolCallId::new("alternative"),
            &serde_json::json!({"ids":["tool:ask_user"]}),
        )
        .unwrap();
    assert_eq!(
        result.value["staged"],
        serde_json::json!(["tool:ask_user", "skill:small"])
    );
    assert!(result.value["rejected"].as_array().unwrap().is_empty());
    runtime.set_pinned([RegistryId::tool("ask_user")]);
    let pinned = derive(&runtime, None, false).await.unwrap();
    assert!(pinned.active_ids().contains(&RegistryId::skill("small")));
    assert!(!pinned.active_ids().contains(&RegistryId::skill("manual")));
}
