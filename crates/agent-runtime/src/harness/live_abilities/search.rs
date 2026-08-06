use super::*;

impl LiveAbilityRuntime {
    pub(crate) fn apply_pending(
        &self,
        session: &SessionAbilities,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
    ) {
        let mut state = session.state.lock().expect("activation state poisoned");
        if state.pending.is_empty() {
            return;
        }
        let pending = std::mem::take(&mut state.pending);
        let additions = pending
            .iter()
            .map(|(id, (revision, _))| (id.clone(), revision.clone()))
            .collect::<Vec<_>>();
        state
            .materialized
            .extend(pending.into_iter().map(|(id, (_, payload))| (id, payload)));
        let epoch = state.epochs.advance(additions).clone();
        emit_activation_epoch(emitter, turn, &epoch);
    }

    pub(crate) fn search_and_stage(
        &self,
        session: &SessionAbilities,
        call: &ToolCallId,
        arguments: &serde_json::Value,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
    ) -> Result<ToolOutcome, RuntimeError> {
        let (query, max_results) = search_arguments(arguments)?;
        let already_active = {
            let state = session.state.lock().expect("activation state poisoned");
            let mut ids = state
                .epochs
                .current()
                .map(|epoch| {
                    epoch
                        .activated()
                        .iter()
                        .map(|(id, _)| id.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            ids.extend(state.pending.keys().cloned());
            ids.extend(
                state
                    .staged
                    .values()
                    .flat_map(|entries| entries.keys().cloned()),
            );
            ids
        };
        let query = RoutingQuery::derive(query, session.routing_hints.clone());
        let (retrieval, plan) = self.select(
            &session.descriptor_view,
            &query,
            &already_active,
            max_results,
        );
        emit_retrieval(emitter, turn, &retrieval);
        let materialized = self.authorize_and_materialize(session, &plan, &already_active)?;

        // Selection scores an ability on its own merits: `already_active` only
        // guards conflicts and dependencies there, so a retrieval can name an
        // ability this session already holds — including one another search in
        // the same batch just staged. Staging it a second time would collide
        // with the first transaction at commit and fail the whole turn, so it
        // is reported as already available instead.
        let held = already_active.iter().collect::<BTreeSet<_>>();
        let mut staged = BTreeMap::new();
        let mut cards = Vec::new();
        let mut staged_ids = Vec::new();
        let mut already_available = Vec::new();
        for binding in &plan.bindings {
            let id = binding.descriptor.id();
            let Some(payload) = materialized.get(id).cloned() else {
                continue;
            };
            cards.push(binding.descriptor.card().clone());
            if held.contains(id) {
                already_available.push(id.qualified());
                continue;
            }
            staged_ids.push(id.qualified());
            staged.insert(
                id.clone(),
                (binding.descriptor.content_revision().clone(), payload),
            );
        }
        {
            let mut state = session.state.lock().expect("activation state poisoned");
            if state.staged.contains_key(call) {
                return Err(RuntimeError::conflict(format!(
                    "search staging transaction `{call}` already exists"
                )));
            }
            state.staged.insert(call.clone(), staged);
        }

        Ok(ToolOutcome::json(serde_json::json!({
            "cards": cards,
            "staged": staged_ids,
            "already_available": already_available,
            "available_on": "next_provider_request"
        })))
    }
}
pub(crate) struct SearchStageGuard {
    pub(super) state: Arc<Mutex<SessionActivationState>>,
    pub(super) call: ToolCallId,
    pub(super) ids: Vec<RegistryId>,
    pub(super) committed: bool,
    pub(super) finished: bool,
}

impl SearchStageGuard {
    pub(crate) fn commit(&mut self) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().expect("activation state poisoned");
        let staged = state.staged.remove(&self.call).ok_or_else(|| {
            RuntimeError::conflict(format!(
                "search staging transaction `{}` disappeared before commit",
                self.call
            ))
        })?;
        if let Some(id) = staged
            .keys()
            .find(|id| state.pending.contains_key(*id))
            .cloned()
        {
            state.staged.insert(self.call.clone(), staged);
            return Err(RuntimeError::conflict(format!(
                "search staging transaction `{}` conflicts with pending ability `{id}`",
                self.call
            )));
        }
        state.pending.extend(staged);
        self.committed = true;
        Ok(())
    }

    pub(crate) fn finish(mut self) {
        self.finished = true;
    }
}

impl Drop for SearchStageGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut state = self.state.lock().expect("activation state poisoned");
        if self.committed {
            let mut staged = BTreeMap::new();
            for id in &self.ids {
                if let Some(entry) = state.pending.remove(id) {
                    staged.insert(id.clone(), entry);
                }
            }
            state.staged.insert(self.call.clone(), staged);
        } else {
            state.staged.remove(&self.call);
        }
    }
}

pub(super) fn search_descriptor(
    tools: &[Arc<dyn Tool>],
) -> Result<AbilityDescriptor, RuntimeError> {
    let tool = tools
        .iter()
        .find(|tool| tool.spec().name == CAPABILITY_SEARCH_TOOL_NAME)
        .ok_or_else(|| RuntimeError::internal("protected registry.search tool missing"))?;
    let spec = tool.spec();
    let revision = RegistryRevision::from_content(
        serde_json::to_vec(&spec).unwrap_or_else(|_| spec.description.as_bytes().to_vec()),
    );
    Ok(AbilityDescriptor::new(
        AbilityKind::Tool,
        CAPABILITY_SEARCH_TOOL_NAME,
        EntryProvenance::new(RegistrySource::BuiltIn, revision.clone()),
        "Capability search",
        spec.description.clone(),
        revision,
    )
    .with_tags(["bootstrap", "capability"])
    .with_keywords(["registry", "search", "capability", "discover"])
    .with_affordances(["capability-search"])
    .with_context_cost(ContextCost::estimate(
        &spec.input_schema.to_string(),
        &spec.description,
    )))
}

pub(super) fn emit_retrieval(
    emitter: &EventEmitter,
    turn: &Option<TurnId>,
    retrieval: &crate::capability::RetrievalResult,
) {
    let index_revision = retrieval.embedding_revision.as_ref().map(|revision| {
        RegistryRevision::from_content(format!("{}:{}", revision.model, revision.index))
    });
    emitter.emit(
        turn.clone(),
        RuntimeEvent::CapabilityRetrievalPerformed {
            resolver_revision: RegistryRevision::new(
                crate::capability::DETERMINISTIC_RETRIEVER_REVISION,
            ),
            index_revision,
            candidates: retrieval
                .candidates
                .iter()
                .map(|candidate| candidate.descriptor.id().clone())
                .collect(),
        },
    );
}

pub(crate) fn emit_activation_epoch(
    emitter: &EventEmitter,
    turn: &Option<TurnId>,
    epoch: &ActivationEpoch,
) {
    emitter.emit(
        turn.clone(),
        RuntimeEvent::CapabilitiesActivated {
            epoch: epoch.index() as u32,
            activation: epoch
                .activated()
                .iter()
                .map(|(id, revision)| ActivatedCapability::new(id.clone(), revision.clone()))
                .collect(),
        },
    );
}
