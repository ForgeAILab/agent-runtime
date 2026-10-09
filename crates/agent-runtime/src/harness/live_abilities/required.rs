use super::*;

impl LiveAbilityRuntime {
    fn required_materialization(
        &self,
        session: &SessionAbilities,
        active: &[RegistryId],
    ) -> Result<BTreeMap<RegistryId, Activated>, RuntimeError> {
        let authorized_pins = self
            .pinned
            .iter()
            .filter_map(|id| session.descriptor_view.get(id))
            .collect::<Vec<_>>();
        let pin_tokens = authorized_pins
            .iter()
            .map(|entry| entry.payload().context_cost().total_tokens())
            .fold(0u32, u32::saturating_add);
        if pin_tokens > self.budget.max_schema_tokens {
            return Err(RuntimeError::config(format!(
                "max_schema_tokens budget {} requires {pin_tokens} tokens for pinned abilities",
                self.budget.max_schema_tokens
            )));
        }
        let mut held = active.to_vec();
        let mut payloads = BTreeMap::new();
        for id in [
            RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME),
            RegistryId::tool(CAPABILITY_ACTIVATE_TOOL_NAME),
        ]
        .into_iter()
        .chain(self.pinned.iter().cloned())
        {
            if held.contains(&id) {
                continue;
            }
            let Some(entry) = session.descriptor_view.get(&id) else {
                continue;
            };
            let plan = if crate::hub::is_bootstrap(&id) {
                crate::capability::selection::select_explicit(
                    &session.descriptor_view,
                    entry.payload(),
                    &held,
                )
                .map_err(|reason| {
                    RuntimeError::config(format!(
                        "required ability `{id}` cannot resolve dependencies: {reason:?}"
                    ))
                })?
            } else {
                self.explicit_plan(session, entry.payload(), &held)
                    .map_err(|error| {
                        RuntimeError::config(format!("required ability `{id}`: {}", error.message))
                    })?
            };
            let materialized = self
                .authorize_and_materialize(session, &plan, &held)
                .map_err(|error| {
                    RuntimeError::config(format!(
                        "required ability `{id}` (closure {}): {}",
                        plan.bindings
                            .iter()
                            .map(|binding| binding.descriptor.id().qualified())
                            .collect::<Vec<_>>()
                            .join(", "),
                        error.message
                    ))
                })?;
            held.extend(materialized.keys().cloned());
            payloads.extend(materialized);
        }
        Ok(payloads)
    }

    pub(super) fn initialize_required(
        &self,
        session: &SessionAbilities,
    ) -> Result<(), RuntimeError> {
        let payloads = self.required_materialization(session, &[])?;
        let revisions = payloads
            .keys()
            .map(|id| {
                (
                    id.clone(),
                    session
                        .descriptor_view
                        .get(id)
                        .expect("required descriptor exists")
                        .payload()
                        .content_revision()
                        .clone(),
                )
            })
            .collect::<Vec<_>>();
        let mut state = session.state.lock().expect("activation state poisoned");
        state.epochs.advance(revisions);
        state.materialized.extend(payloads);
        Ok(())
    }

    pub(super) fn reconcile_required(
        &self,
        session: &SessionAbilities,
    ) -> Result<(), RuntimeError> {
        let active = session.active_ids();
        let payloads = self.required_materialization(session, &active)?;
        if payloads.is_empty() {
            return Ok(());
        }
        let revisions = payloads
            .keys()
            .map(|id| {
                (
                    id.clone(),
                    session
                        .descriptor_view
                        .get(id)
                        .expect("required descriptor exists")
                        .payload()
                        .content_revision()
                        .clone(),
                )
            })
            .collect::<Vec<_>>();
        let mut state = session.state.lock().expect("activation state poisoned");
        // A required id may have been staged by an interrupted old session.
        // It is promoted once and removed from every unfinished transaction.
        for id in payloads.keys() {
            state.pending.remove(id);
            for entries in state.staged.values_mut() {
                entries.remove(id);
            }
        }
        state.materialized.extend(payloads);
        state.epochs.advance(revisions);
        Ok(())
    }
}
