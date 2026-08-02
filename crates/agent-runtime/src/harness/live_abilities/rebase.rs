use super::*;

impl LiveAbilityRuntime {
    pub(super) fn persisted_scope_matches(
        &self,
        session: &SessionAbilities,
        persisted: &VersionedSessionState,
    ) -> Result<bool, RuntimeError> {
        let expected_revision = RegistryRevision::new(ACTIVATION_STATE_REVISION);
        if persisted.revision != expected_revision {
            return Err(RuntimeError::conflict(format!(
                "activation state revision `{}` is incompatible with `{expected_revision}`",
                persisted.revision
            )));
        }
        let persisted: PersistedActivationState = serde_json::from_value(persisted.value.clone())
            .map_err(|error| {
            RuntimeError::conflict(format!("activation state is malformed: {error}"))
        })?;
        Ok(persisted.snapshot == self.snapshot_fingerprint().as_str()
            && persisted.view == session.view_fingerprint().as_str())
    }

    pub(super) fn restore_session_state(
        &self,
        session: &SessionAbilities,
        persisted: &VersionedSessionState,
    ) -> Result<(), RuntimeError> {
        let expected_revision = RegistryRevision::new(ACTIVATION_STATE_REVISION);
        if persisted.revision != expected_revision {
            return Err(RuntimeError::conflict(format!(
                "activation state revision `{}` is incompatible with `{expected_revision}`",
                persisted.revision
            )));
        }
        let persisted: PersistedActivationState = serde_json::from_value(persisted.value.clone())
            .map_err(|error| {
            RuntimeError::conflict(format!("activation state is malformed: {error}"))
        })?;
        if persisted.snapshot != self.snapshot_fingerprint().as_str()
            || persisted.view != session.view_fingerprint().as_str()
        {
            return Err(RuntimeError::conflict(
                "activation state belongs to a different registry snapshot or scoped view",
            ));
        }
        let epochs = ActivationEpochs::restore(persisted.epochs).map_err(RuntimeError::conflict)?;
        let current = epochs
            .current()
            .ok_or_else(|| RuntimeError::conflict("activation state contains no current epoch"))?;
        let search_id = RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME);
        let search_revision = self
            .descriptors
            .get(&search_id)
            .ok_or_else(|| RuntimeError::internal("protected registry.search descriptor missing"))?
            .payload()
            .content_revision();
        if !current
            .activated()
            .iter()
            .any(|(id, revision)| id == &search_id && revision == search_revision)
        {
            return Err(RuntimeError::conflict(
                "restored activation state omits or changes protected registry.search",
            ));
        }

        let active = current
            .activated()
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let pending_ids = persisted
            .pending
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let staged_ids = persisted
            .staged
            .iter()
            .flat_map(|(_, entries)| entries.iter().map(|(id, _)| id.clone()))
            .collect::<Vec<_>>();
        let satisfied = active
            .iter()
            .chain(&pending_ids)
            .chain(&staged_ids)
            .cloned()
            .collect::<Vec<_>>();
        let mut materialized = BTreeMap::new();
        for (id, revision) in current.activated() {
            materialized.insert(
                id.clone(),
                self.restore_payload(session, id, revision, &active, &satisfied)?,
            );
        }
        let mut pending = BTreeMap::new();
        for (id, revision) in persisted.pending {
            if current.contains(&id) || pending.contains_key(&id) {
                return Err(RuntimeError::conflict(format!(
                    "restored pending activation duplicates `{id}`"
                )));
            }
            let payload = self.restore_payload(session, &id, &revision, &active, &satisfied)?;
            pending.insert(id, (revision, payload));
        }
        let mut staged = BTreeMap::new();
        let mut staged_ids_seen = BTreeMap::<RegistryId, ToolCallId>::new();
        for (call, entries) in persisted.staged {
            if staged.contains_key(&call) {
                return Err(RuntimeError::conflict(format!(
                    "restored search staging duplicates transaction `{call}`"
                )));
            }
            let mut transaction = BTreeMap::new();
            for (id, revision) in entries {
                if current.contains(&id)
                    || pending.contains_key(&id)
                    || transaction.contains_key(&id)
                {
                    return Err(RuntimeError::conflict(format!(
                        "restored uncommitted search staging duplicates `{id}`"
                    )));
                }
                if let Some(prior) = staged_ids_seen.insert(id.clone(), call.clone()) {
                    return Err(RuntimeError::conflict(format!(
                        "restored uncommitted ability `{id}` appears in both `{prior}` and `{call}`"
                    )));
                }
                let payload = self.restore_payload(session, &id, &revision, &active, &satisfied)?;
                transaction.insert(id, (revision, payload));
            }
            staged.insert(call, transaction);
        }
        *session.state.lock().expect("activation state poisoned") = SessionActivationState {
            epochs,
            materialized,
            initialized: persisted.initialized,
            pending,
            staged,
        };
        Ok(())
    }

    /// Re-derives a completed session's operational activation state against
    /// the host's current scoped view.
    ///
    /// A completed boundary has no in-flight provider request or tool batch,
    /// so optional capabilities that disappeared from the scope can be
    /// pruned safely. The protected bootstrap remains mandatory. Capabilities
    /// that survive are re-authorized and re-materialized; newly visible
    /// capabilities are left to normal initial routing on the next turn.
    pub(super) fn rebase_session_state(
        &self,
        session: &SessionAbilities,
        persisted: &VersionedSessionState,
    ) -> Result<(), RuntimeError> {
        let expected_revision = RegistryRevision::new(ACTIVATION_STATE_REVISION);
        if persisted.revision != expected_revision {
            return Err(RuntimeError::conflict(format!(
                "activation state revision `{}` is incompatible with `{expected_revision}`",
                persisted.revision
            )));
        }
        let persisted: PersistedActivationState = serde_json::from_value(persisted.value.clone())
            .map_err(|error| {
            RuntimeError::conflict(format!("activation state is malformed: {error}"))
        })?;
        let restored =
            ActivationEpochs::restore(persisted.epochs).map_err(RuntimeError::conflict)?;
        let current = restored
            .current()
            .ok_or_else(|| RuntimeError::conflict("activation state contains no current epoch"))?;

        let search_id = RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME);
        if !current.contains(&search_id) {
            return Err(RuntimeError::conflict(
                "restored activation state omits protected registry.search",
            ));
        }
        let search_revision = self
            .descriptors
            .get(&search_id)
            .ok_or_else(|| RuntimeError::internal("protected registry.search descriptor missing"))?
            .payload()
            .content_revision()
            .clone();

        // Validate uniqueness independently of current visibility. A malformed
        // protected record must not be made acceptable merely because the
        // conflicting ability disappeared from the new scope.
        let mut locations = BTreeMap::<RegistryId, String>::new();
        for (id, _) in current.activated() {
            if locations.insert(id.clone(), "active".into()).is_some() {
                return Err(RuntimeError::conflict(format!(
                    "restored active state duplicates `{id}`"
                )));
            }
        }
        for (id, _) in &persisted.pending {
            if let Some(prior) = locations.insert(id.clone(), "pending".into()) {
                return Err(RuntimeError::conflict(format!(
                    "restored pending activation duplicates `{id}` from `{prior}`"
                )));
            }
        }
        let mut staged_calls = BTreeMap::<ToolCallId, ()>::new();
        for (call, entries) in &persisted.staged {
            if staged_calls.insert(call.clone(), ()).is_some() {
                return Err(RuntimeError::conflict(format!(
                    "restored search staging duplicates transaction `{call}`"
                )));
            }
            for (id, _) in entries {
                if let Some(prior) =
                    locations.insert(id.clone(), format!("staged transaction `{call}`"))
                {
                    return Err(RuntimeError::conflict(format!(
                        "restored uncommitted ability `{id}` duplicates `{prior}`"
                    )));
                }
            }
        }

        let mut candidates = BTreeMap::<RegistryId, RebaseCandidate>::new();
        candidates.insert(
            search_id.clone(),
            RebaseCandidate {
                revision: search_revision,
                placement: RebasePlacement::Active,
            },
        );
        for (id, revision) in current.activated() {
            if id == &search_id {
                continue;
            }
            if session
                .descriptor_view
                .get(id)
                .is_some_and(|entry| entry.payload().content_revision() == revision)
            {
                candidates.insert(
                    id.clone(),
                    RebaseCandidate {
                        revision: revision.clone(),
                        placement: RebasePlacement::Active,
                    },
                );
            }
        }
        for (id, revision) in persisted.pending {
            if session
                .descriptor_view
                .get(&id)
                .is_some_and(|entry| entry.payload().content_revision() == &revision)
            {
                candidates.insert(
                    id,
                    RebaseCandidate {
                        revision,
                        placement: RebasePlacement::Pending,
                    },
                );
            }
        }
        for (call, entries) in persisted.staged {
            for (id, revision) in entries {
                if session
                    .descriptor_view
                    .get(&id)
                    .is_some_and(|entry| entry.payload().content_revision() == &revision)
                {
                    candidates.insert(
                        id,
                        RebaseCandidate {
                            revision,
                            placement: RebasePlacement::Staged(call.clone()),
                        },
                    );
                }
            }
        }

        // Re-run authorization to a fixed point. If an optional dependency is
        // pruned, a dependent candidate gets another pass without that
        // dependency in `satisfied` and is pruned as well.
        let materialized = loop {
            let active = candidates
                .iter()
                .filter(|(_, candidate)| matches!(candidate.placement, RebasePlacement::Active))
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            let satisfied = candidates.keys().cloned().collect::<Vec<_>>();
            let mut payloads = BTreeMap::new();
            let mut rejected = Vec::new();
            for (id, candidate) in &candidates {
                match self.restore_payload(session, id, &candidate.revision, &active, &satisfied) {
                    Ok(payload) => {
                        payloads.insert(id.clone(), payload);
                    }
                    Err(error) if id == &search_id => return Err(error),
                    Err(_) => rejected.push(id.clone()),
                }
            }
            if rejected.is_empty() {
                break payloads;
            }
            for id in rejected {
                candidates.remove(&id);
            }
        };

        let mut active_materialized = BTreeMap::new();
        let mut active_revisions = Vec::new();
        let mut pending = BTreeMap::new();
        let mut staged =
            BTreeMap::<ToolCallId, BTreeMap<RegistryId, (RegistryRevision, Activated)>>::new();
        for (id, candidate) in candidates {
            let payload = materialized
                .get(&id)
                .cloned()
                .ok_or_else(|| RuntimeError::internal("rebased ability payload disappeared"))?;
            match candidate.placement {
                RebasePlacement::Active => {
                    active_revisions.push((id.clone(), candidate.revision));
                    active_materialized.insert(id, payload);
                }
                RebasePlacement::Pending => {
                    pending.insert(id, (candidate.revision, payload));
                }
                RebasePlacement::Staged(call) => {
                    staged
                        .entry(call)
                        .or_default()
                        .insert(id, (candidate.revision, payload));
                }
            }
        }

        let mut epochs = ActivationEpochs::new();
        epochs.advance(active_revisions);
        // Record a distinct safe-boundary epoch even when the retained set is
        // identical. Its fingerprint therefore cannot be confused with the
        // persisted scope's final provider epoch.
        epochs.advance(std::iter::empty());
        *session.state.lock().expect("activation state poisoned") = SessionActivationState {
            epochs,
            materialized: active_materialized,
            initialized: false,
            pending,
            staged,
        };
        Ok(())
    }

    pub(super) fn restore_payload(
        &self,
        session: &SessionAbilities,
        id: &RegistryId,
        revision: &RegistryRevision,
        active: &[RegistryId],
        satisfied: &[RegistryId],
    ) -> Result<Activated, RuntimeError> {
        let descriptor = session
            .descriptor_view
            .get(id)
            .ok_or_else(|| {
                RuntimeError::conflict(format!(
                    "restored ability `{id}` is no longer visible in the session scope"
                ))
            })?
            .payload();
        if descriptor.content_revision() != revision {
            return Err(RuntimeError::conflict(format!(
                "restored ability `{id}` expected revision `{revision}`, found `{}`",
                descriptor.content_revision()
            )));
        }
        let mut context = self
            .activation_context
            .clone()
            .with_active(active.iter().cloned())
            .with_satisfied(satisfied.iter().cloned());
        context.expected_revision = Some(revision.clone());
        self.policy
            .authorize(descriptor, &context)
            .map_err(|error| RuntimeError::conflict(error.to_string()))?;
        let ability = if id == &RegistryId::tool(CAPABILITY_SEARCH_TOOL_NAME) {
            self.hub.abilities().get(id).map(|entry| entry.payload())
        } else {
            session.scoped.resolve_ability(id)
        }
        .ok_or_else(|| {
            RuntimeError::conflict(format!(
                "restored ability `{id}` has no executable scoped implementation"
            ))
        })?;
        let payload = ability
            .materialize()
            .map_err(|error| RuntimeError::conflict(error.to_string()))?;
        match payload {
            Activated::SkillInstructions(_) | Activated::ToolSchema(_) => Ok(payload),
            _ => Err(RuntimeError::conflict(format!(
                "restored ability `{id}` cannot materialize into the direct harness"
            ))),
        }
    }
}
