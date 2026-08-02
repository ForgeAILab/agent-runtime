use super::*;

impl LiveAbilityRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn seal(
        mut tools: Vec<Arc<dyn Tool>>,
        descriptor_overrides: Vec<AbilityDescriptor>,
        abilities: Vec<Arc<dyn Ability>>,
        resolver: Arc<CapabilityResolver>,
        policy: Arc<dyn ActivationPolicy>,
        activation_context: ActivationContext,
        scope_inputs: ScopeInputs,
        budget: ActivationBudget,
    ) -> Result<SealedLiveAbilities, RuntimeError> {
        if tools
            .iter()
            .any(|tool| tool.spec().name == CAPABILITY_SEARCH_TOOL_NAME)
        {
            return Err(RuntimeError::conflict(format!(
                "`{CAPABILITY_SEARCH_TOOL_NAME}` is a protected runtime ability name"
            )));
        }
        tools.push(Arc::new(CapabilitySearchTool));

        let mut overrides = BTreeMap::new();
        for descriptor in descriptor_overrides {
            if descriptor.kind() != &AbilityKind::Tool {
                return Err(RuntimeError::config(format!(
                    "tool descriptor override `{}` must have kind `tool`",
                    descriptor.id()
                )));
            }
            if overrides
                .insert(descriptor.id().clone(), descriptor)
                .is_some()
            {
                return Err(RuntimeError::conflict(
                    "a tool descriptor override was registered more than once",
                ));
            }
        }

        let search_descriptor = search_descriptor(&tools)?;
        overrides.insert(search_descriptor.id().clone(), search_descriptor);

        let mut hub_builder = RegistryHubBuilder::new();
        for tool in &tools {
            let id = RegistryId::tool(tool.spec().name);
            let ability = match overrides.remove(&id) {
                Some(descriptor) => tool_ability_with_descriptor(tool.clone(), descriptor)
                    .map_err(RuntimeError::config)?,
                None => tool_ability(tool.clone()),
            };
            hub_builder.ability(ability);
        }
        if let Some((id, _)) = overrides.into_iter().next() {
            return Err(RuntimeError::config(format!(
                "tool descriptor override `{id}` has no registered executable tool"
            )));
        }
        for ability in abilities {
            hub_builder.ability(ability);
        }
        let hub = hub_builder
            .seal()
            .map_err(|error| RuntimeError::config(error.to_string()))?;

        let mut descriptor_builder = RegistryBuilder::new();
        for entry in hub.abilities().iter() {
            let descriptor = entry.payload().descriptor();
            descriptor_builder.declare(RegistryEntry::new(descriptor.card().clone(), descriptor));
        }
        let descriptors = descriptor_builder
            .seal()
            .map_err(|error| RuntimeError::config(error.to_string()))?;

        Ok(SealedLiveAbilities {
            runtime: Arc::new(Self {
                hub,
                descriptors,
                resolver,
                policy,
                activation_context,
                scope_inputs,
                budget,
            }),
            tools,
        })
    }

    pub(crate) fn snapshot_fingerprint(&self) -> Fingerprint {
        self.hub.fingerprint()
    }

    pub(crate) fn entry_count(&self) -> u32 {
        self.descriptors.len() as u32
    }

    pub(crate) async fn derive_session(
        &self,
        session: SessionId,
        parent: Option<SessionId>,
        interaction_ready: bool,
        pipeline: &HarnessPipeline,
        extension_state: &BTreeMap<String, VersionedSessionState>,
        rebase_completed: bool,
    ) -> Result<SessionAbilities, RuntimeError> {
        let mut inputs = self.scope_inputs.clone();
        if !interaction_ready {
            inputs = inputs.deny_id(RegistryId::tool(crate::harness::QUESTIONNAIRE_TOOL_NAME));
        }
        let mut scoped = self.hub.scoped(&inputs);
        let mut routing_hints = Vec::new();
        for resolver in pipeline.tool_view() {
            let descriptor = resolver.descriptor();
            let visible = scoped
                .agent_view()
                .abilities()
                .iter()
                .map(|entry| entry.id().clone())
                .collect();
            let patch = resolver
                .resolve(&ToolViewContext {
                    session: session.clone(),
                    parent: parent.clone(),
                    interaction_ready,
                    visible,
                    state: extension_state.get(descriptor.id().as_str()).cloned(),
                })
                .await?;
            for id in patch.deny {
                inputs = inputs.deny_id(id);
            }
            routing_hints.extend(patch.routing_hints);
            scoped = self.hub.scoped(&inputs);
        }
        routing_hints.sort();
        routing_hints.dedup();

        let agent_view = scoped.agent_view();
        let visible = agent_view.abilities();
        let mut filter = ViewFilter::new();
        for entry in self.descriptors.iter() {
            if visible.get(entry.id()).is_none()
                && entry.id() != &RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME)
            {
                filter = filter.deny_id(entry.id().clone());
            }
        }
        let descriptor_view = self.descriptors.view(&filter);

        let search_id = RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME);
        let search_descriptor = self
            .descriptors
            .get(&search_id)
            .ok_or_else(|| RuntimeError::internal("protected registry.search descriptor missing"))?
            .payload()
            .clone();
        let search_ability = self
            .hub
            .abilities()
            .get(&search_id)
            .ok_or_else(|| RuntimeError::internal("protected registry.search ability missing"))?
            .payload()
            .clone();
        let mut context = self.activation_context.clone();
        context.expected_revision = Some(search_descriptor.content_revision().clone());
        self.policy
            .authorize(&search_descriptor, &context)
            .map_err(|error| RuntimeError::config(error.to_string()))?;
        let search_payload = search_ability
            .materialize()
            .map_err(|error| RuntimeError::config(error.to_string()))?;

        let mut epochs = ActivationEpochs::new();
        epochs.advance([(
            search_id.clone(),
            search_descriptor.content_revision().clone(),
        )]);
        let mut materialized = BTreeMap::new();
        materialized.insert(search_id, search_payload);
        let session = SessionAbilities {
            snapshot: self.snapshot_fingerprint(),
            scoped,
            descriptor_view,
            routing_hints,
            state: Arc::new(Mutex::new(SessionActivationState {
                epochs,
                materialized,
                initialized: false,
                pending: BTreeMap::new(),
                staged: BTreeMap::new(),
            })),
        };
        if let Some(persisted) = extension_state.get(ACTIVATION_STATE_NAMESPACE) {
            if rebase_completed && !self.persisted_scope_matches(&session, persisted)? {
                self.rebase_session_state(&session, persisted)?;
            } else {
                self.restore_session_state(&session, persisted)?;
            }
        }
        Ok(session)
    }
}

impl LiveAbilityRuntime {
    pub(super) fn select(
        &self,
        view: &RegistryView<AbilityDescriptor>,
        query: &RoutingQuery,
        already_active: &[RegistryId],
        max_candidates: usize,
    ) -> (
        crate::capability::RetrievalResult,
        crate::capability::ActivationPlan,
    ) {
        let retrieval = self.resolver.retrieve(view, query);
        let candidates = retrieval
            .candidates
            .iter()
            .filter(|candidate| {
                candidate.descriptor.id() != &RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME)
            })
            .take(max_candidates)
            .cloned()
            .collect::<Vec<_>>();
        let plan = self.resolver.select(
            view,
            &candidates,
            &SelectionBudgets::new(
                self.budget.max_schema_tokens,
                u32::MAX,
                u32::MAX,
                RiskLevel::High,
                self.budget.max_candidates.min(max_candidates),
            ),
            already_active,
        );
        (retrieval, plan)
    }

    pub(super) fn authorize_and_materialize(
        &self,
        session: &SessionAbilities,
        plan: &crate::capability::ActivationPlan,
        active: &[RegistryId],
    ) -> Result<BTreeMap<RegistryId, Activated>, RuntimeError> {
        let plan_ids = plan
            .bindings
            .iter()
            .map(|binding| binding.descriptor.id().clone())
            .collect::<Vec<_>>();
        let satisfied = active
            .iter()
            .cloned()
            .chain(plan_ids.iter().cloned())
            .collect::<Vec<_>>();
        let mut materialized = BTreeMap::new();
        for binding in &plan.bindings {
            let descriptor = &binding.descriptor;
            let mut context = self
                .activation_context
                .clone()
                .with_active(active.iter().cloned())
                .with_satisfied(satisfied.iter().cloned());
            context.expected_revision = Some(descriptor.content_revision().clone());
            self.policy
                .authorize(descriptor, &context)
                .map_err(|error| RuntimeError::config(error.to_string()))?;
            let ability = session
                .scoped
                .resolve_ability(descriptor.id())
                .ok_or_else(|| {
                    RuntimeError::conflict(format!(
                        "selected ability `{}` is not available in the session scope",
                        descriptor.id()
                    ))
                })?;
            let payload = ability
                .materialize()
                .map_err(|error| RuntimeError::config(error.to_string()))?;
            match &payload {
                Activated::SkillInstructions(_) => {}
                Activated::ToolSchema(_) => {}
                _ => {
                    return Err(RuntimeError::config(format!(
                        "ability `{}` materialized a payload the direct harness cannot advertise",
                        descriptor.id()
                    )));
                }
            }
            materialized.insert(descriptor.id().clone(), payload);
        }
        Ok(materialized)
    }

    pub(crate) fn ensure_initial_activation(
        &self,
        session: &SessionAbilities,
        user_text: &str,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
    ) -> Result<(), RuntimeError> {
        let (already_active, initialized) = {
            let state = session.state.lock().expect("activation state poisoned");
            (
                state
                    .epochs
                    .current()
                    .map(|epoch| {
                        epoch
                            .activated()
                            .iter()
                            .map(|(id, _)| id.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
                state.initialized,
            )
        };
        if initialized {
            return Ok(());
        }

        let query = RoutingQuery::derive(user_text, session.routing_hints.clone());
        let (retrieval, plan) = self.select(
            &session.descriptor_view,
            &query,
            &already_active,
            self.budget.max_candidates,
        );
        emit_retrieval(emitter, turn, &retrieval);
        let materialized = self.authorize_and_materialize(session, &plan, &already_active)?;
        let mut state = session.state.lock().expect("activation state poisoned");
        if state.initialized {
            return Ok(());
        }
        state.materialized.extend(materialized);
        if !plan.bindings.is_empty() {
            let epoch = state.epochs.advance(plan.activated_ids()).clone();
            emit_activation_epoch(emitter, turn, &epoch);
        }
        state.initialized = true;
        Ok(())
    }
}
