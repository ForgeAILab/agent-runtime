use super::*;

#[derive(Serialize, Deserialize)]
pub(super) struct PersistedActivationState {
    pub(super) snapshot: String,
    pub(super) view: String,
    pub(super) initialized: bool,
    pub(super) epochs: Vec<Vec<(RegistryId, RegistryRevision)>>,
    pub(super) pending: Vec<(RegistryId, RegistryRevision)>,
    pub(super) staged: Vec<(ToolCallId, Vec<(RegistryId, RegistryRevision)>)>,
}

#[derive(Clone)]
pub(super) enum RebasePlacement {
    Active,
    Pending,
    Staged(ToolCallId),
}

#[derive(Clone)]
pub(super) struct RebaseCandidate {
    pub(super) revision: RegistryRevision,
    pub(super) placement: RebasePlacement,
}

/// Session-owned scoped view and activation history.
pub(crate) struct SessionAbilities {
    pub(super) snapshot: Fingerprint,
    pub(super) scoped: ScopedRegistry,
    pub(super) descriptor_view: RegistryView<AbilityDescriptor>,
    pub(super) routing_hints: Vec<String>,
    pub(super) state: Arc<Mutex<SessionActivationState>>,
}

impl fmt::Debug for SessionAbilities {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock().expect("activation state poisoned");
        formatter
            .debug_struct("SessionAbilities")
            .field("view", &self.scoped.fingerprint())
            .field("visible", &self.descriptor_view.len())
            .field("epochs", &state.epochs.history().len())
            .field("pending", &state.pending.len())
            .finish_non_exhaustive()
    }
}

pub(super) struct SessionActivationState {
    pub(super) epochs: ActivationEpochs,
    pub(super) materialized: BTreeMap<RegistryId, Activated>,
    pub(super) initialized: bool,
    pub(super) pending: BTreeMap<RegistryId, (RegistryRevision, Activated)>,
    pub(super) staged: BTreeMap<ToolCallId, BTreeMap<RegistryId, (RegistryRevision, Activated)>>,
}

impl SessionAbilities {
    pub(crate) fn search_stage_guard(
        &self,
        call: &ToolCallId,
    ) -> Result<SearchStageGuard, RuntimeError> {
        let ids = self
            .state
            .lock()
            .expect("activation state poisoned")
            .staged
            .get(call)
            .ok_or_else(|| {
                RuntimeError::conflict(format!(
                    "search staging transaction `{call}` is unavailable"
                ))
            })?
            .keys()
            .cloned()
            .collect();
        Ok(SearchStageGuard {
            state: self.state.clone(),
            call: call.clone(),
            ids,
            committed: false,
            finished: false,
        })
    }

    pub(crate) fn view_fingerprint(&self) -> Fingerprint {
        self.scoped.fingerprint()
    }

    pub(crate) fn visible_count(&self) -> u32 {
        self.descriptor_view.len() as u32
    }

    pub(crate) fn current_epoch(&self) -> ActivationEpoch {
        self.state
            .lock()
            .expect("activation state poisoned")
            .epochs
            .current()
            .cloned()
            .expect("protected bootstrap creates epoch zero")
    }

    pub(crate) fn persisted_state(&self) -> VersionedSessionState {
        let state = self.state.lock().expect("activation state poisoned");
        let value = serde_json::to_value(PersistedActivationState {
            snapshot: self.snapshot.as_str().to_owned(),
            view: self.view_fingerprint().as_str().to_owned(),
            initialized: state.initialized,
            epochs: state
                .epochs
                .history()
                .iter()
                .map(|epoch| epoch.activated().to_vec())
                .collect(),
            pending: state
                .pending
                .iter()
                .map(|(id, (revision, _))| (id.clone(), revision.clone()))
                .collect(),
            staged: state
                .staged
                .iter()
                .map(|(call, entries)| {
                    (
                        call.clone(),
                        entries
                            .iter()
                            .map(|(id, (revision, _))| (id.clone(), revision.clone()))
                            .collect(),
                    )
                })
                .collect(),
        })
        .expect("runtime-owned activation state is JSON serializable");
        VersionedSessionState::new(RegistryRevision::new(ACTIVATION_STATE_REVISION), value)
            .redaction_safe()
    }

    pub(crate) fn materialized(
        &self,
    ) -> Result<(Vec<ToolSchema>, Vec<ContextFragment>), RuntimeError> {
        let state = self.state.lock().expect("activation state poisoned");
        let epoch = state
            .epochs
            .current()
            .ok_or_else(|| RuntimeError::internal("session has no activation epoch"))?;
        let mut schemas = Vec::new();
        let mut instructions = Vec::new();
        for (sequence, (id, revision)) in epoch.activated().iter().enumerate() {
            let payload = state.materialized.get(id).ok_or_else(|| {
                RuntimeError::conflict(format!(
                    "activation epoch references unavailable payload `{id}`"
                ))
            })?;
            match payload {
                Activated::ToolSchema(schema) => schemas.push(schema.clone()),
                Activated::SkillInstructions(text) => instructions.push(
                    ContextFragment::new(
                        format!("ability:{}:instructions", id.qualified()),
                        FragmentKind::AbilityInstruction,
                        FragmentSource::Ability { id: id.clone() },
                        revision.clone(),
                        FragmentContent::Text(text.clone()),
                    )
                    .with_position(ContextPosition::new(
                        agent_runtime_context::ContextLane::Capabilities,
                        sequence as u64,
                    ))
                    .with_cache_class(CacheClass::Stable),
                ),
                _ => {
                    return Err(RuntimeError::config(format!(
                        "active ability `{id}` has no direct-loop materialization"
                    )));
                }
            }
        }
        Ok((schemas, instructions))
    }
}
