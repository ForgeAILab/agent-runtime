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
        let args = search_arguments(arguments)?;
        let by_domain = session.by_domain();
        if args.query.is_empty() {
            let active = session.active_ids();
            let mut entries = session
                .descriptor_view
                .iter()
                .filter(|entry| !crate::hub::is_bootstrap(entry.id()))
                .filter(|entry| {
                    args.domain
                        .is_none_or(|domain| entry.id().domain.as_str() == domain)
                })
                .collect::<Vec<_>>();
            entries.sort_by(|a, b| {
                a.id()
                    .domain
                    .as_str()
                    .cmp(b.id().domain.as_str())
                    .then_with(|| a.id().cmp(b.id()))
            });
            let total = entries.len();
            let listing = entries
                .into_iter()
                .skip(args.offset)
                .take(args.max_results)
                .map(|entry| {
                    serde_json::json!({"id":entry.id().qualified(), "summary":entry.card().summary,
                    "active":active.contains(entry.id())})
                })
                .collect::<Vec<_>>();
            let next = args.offset.saturating_add(listing.len());
            return Ok(ToolOutcome::json(
                serde_json::json!({"listing":listing, "total":total,
                "next_offset":if next < total {Some(next)} else {None}, "by_domain":by_domain}),
            ));
        }
        let query_text = args.query;
        let max_results = args.max_results;
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
        let query = RoutingQuery::derive(query_text, session.routing_hints.clone());
        let mut retrieval = self
            .resolver
            .retrieve_descriptive(&session.descriptor_view, &query);
        retrieval.candidates.retain(|candidate| {
            !crate::hub::is_bootstrap(candidate.descriptor.id())
                && args
                    .domain
                    .is_none_or(|domain| candidate.descriptor.id().domain.as_str() == domain)
        });
        let plan = self.select_retrieved(
            &session.descriptor_view,
            &retrieval,
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
        for candidate in retrieval
            .candidates
            .iter()
            .take(max_results)
            .filter(|candidate| {
                self.pinned.contains(candidate.descriptor.id())
                    && held.contains(candidate.descriptor.id())
            })
        {
            cards.push(candidate.descriptor.card().clone());
            already_available.push(candidate.descriptor.id().qualified());
        }
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

        let mut result = serde_json::json!({
            "by_domain": by_domain,
            "cards": cards,
            "staged": staged_ids,
            "already_available": already_available,
            "available_on": "next_provider_request"
        });
        if retrieval.candidates.is_empty() {
            result["matched"] = serde_json::json!(0);
            result["note"] = serde_json::json!(
                "Nothing matched the query. Omit query to list everything available."
            );
        }
        Ok(ToolOutcome::json(result))
    }
}
impl SessionAbilities {
    fn by_domain(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for entry in self
            .descriptor_view
            .iter()
            .filter(|entry| !crate::hub::is_bootstrap(entry.id()))
        {
            *counts
                .entry(entry.id().domain.as_str().to_owned())
                .or_default() += 1;
        }
        counts
    }

    fn available_ids(&self) -> Vec<RegistryId> {
        let state = self.state.lock().expect("activation state poisoned");
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
        ids.sort();
        ids.dedup();
        ids
    }
}

impl LiveAbilityRuntime {
    pub(crate) fn discover_and_stage(
        &self,
        name: &str,
        session: &SessionAbilities,
        call: &ToolCallId,
        arguments: &serde_json::Value,
        emitter: &EventEmitter,
        turn: &Option<TurnId>,
    ) -> Result<ToolOutcome, RuntimeError> {
        if name == CAPABILITY_ACTIVATE_TOOL_NAME {
            self.activate_and_stage(session, call, arguments)
        } else {
            self.search_and_stage(session, call, arguments, emitter, turn)
        }
    }

    pub(crate) fn activate_and_stage(
        &self,
        session: &SessionAbilities,
        call: &ToolCallId,
        arguments: &serde_json::Value,
    ) -> Result<ToolOutcome, RuntimeError> {
        let ids = arguments
            .get("ids")
            .and_then(serde_json::Value::as_array)
            .filter(|ids| !ids.is_empty() && ids.len() <= 8)
            .ok_or_else(|| RuntimeError::tool("registry.activate requires 1..=8 qualified ids"))?;
        let mut held = session.available_ids();
        let mut staged = BTreeMap::new();
        let mut staged_ids = Vec::new();
        let mut already_available = Vec::new();
        let mut rejected = Vec::new();
        for raw in ids {
            let text = raw.as_str().unwrap_or("");
            // Match against authorized qualified identities; malformed, absent,
            // and denied ids are deliberately indistinguishable.
            let entry = session
                .descriptor_view
                .iter()
                .find(|entry| entry.id().qualified() == text);
            let Some(entry) = entry else {
                rejected.push(serde_json::json!({"id":raw,"reason":"unknown or not authorized"}));
                continue;
            };
            let id = entry.id();
            if held.contains(id) {
                already_available.push(text.to_owned());
                continue;
            }
            let attempt: Result<_, RuntimeError> = (|| {
                let plan = self.explicit_plan(session, entry.payload(), &held)?;
                let payloads = self.authorize_and_materialize(session, &plan, &held)?;
                Ok((plan, payloads))
            })();
            match attempt {
                Ok((plan, payloads)) => {
                    for binding in plan.bindings {
                        let id = binding.descriptor.id();
                        if held.contains(id) {
                            continue;
                        }
                        let payload = payloads
                            .get(id)
                            .expect("authorized plan materialized every binding")
                            .clone();
                        held.push(id.clone());
                        staged_ids.push(id.qualified());
                        staged.insert(
                            id.clone(),
                            (binding.descriptor.content_revision().clone(), payload),
                        );
                    }
                }
                Err(error) => rejected.push(serde_json::json!({"id":text, "reason":error.message})),
            }
        }
        let mut state = session.state.lock().expect("activation state poisoned");
        if state.staged.contains_key(call) {
            return Err(RuntimeError::conflict(format!(
                "activation staging transaction `{call}` already exists"
            )));
        }
        state.staged.insert(call.clone(), staged);
        Ok(ToolOutcome::json(
            serde_json::json!({"staged":staged_ids,"already_available":already_available,
            "rejected":rejected,"available_on":"next_provider_request"}),
        ))
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
    name: &str,
) -> Result<AbilityDescriptor, RuntimeError> {
    let tool = tools
        .iter()
        .find(|tool| tool.spec().name == name)
        .ok_or_else(|| RuntimeError::internal(format!("protected {name} tool missing")))?;
    let spec = tool.spec();
    let revision = RegistryRevision::from_content(
        serde_json::to_vec(&spec).unwrap_or_else(|_| spec.description.as_bytes().to_vec()),
    );
    Ok(AbilityDescriptor::new(
        AbilityKind::Tool,
        name,
        EntryProvenance::new(RegistrySource::BuiltIn, revision.clone()),
        if name == CAPABILITY_SEARCH_TOOL_NAME {
            "Capability search"
        } else {
            "Capability activation"
        },
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
