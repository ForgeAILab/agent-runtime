use super::*;

/// Logical session references in the existing runtime-owned intent/catalog
/// extensions. These are index metadata, not new serialized object fields and
/// never read authority for another session. Unknown intent shapes leak safely.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(in crate::journal) struct DependencyRoots {
    pub(in crate::journal) sessions: BTreeSet<SessionId>,
    pub(in crate::journal) pending: bool,
    pub(in crate::journal) completed_fork: Option<SessionId>,
    pub(in crate::journal) unresolved: bool,
    pub(in crate::journal) fork_present: bool,
    pub(in crate::journal) fork_child: Option<SessionId>,
    pub(in crate::journal) fork_unknown: bool,
    pub(in crate::journal) proof_child: Option<SessionId>,
    pub(in crate::journal) catalog_sessions: BTreeSet<SessionId>,
    pub(in crate::journal) catalog_pending: bool,
    pub(in crate::journal) catalog_unknown: bool,
}

impl DependencyRoots {
    pub(in crate::journal) fn view(snapshot: &JournalSnapshotView) -> Self {
        Self::snapshot(&SessionSnapshot {
            id: snapshot.id.clone(),
            history: vec![],
            usage: Default::default(),
            identity: snapshot.identity.clone(),
            manifests: vec![],
            extension_state: [
                "runtime.session.fork_pending",
                "runtime.session.superseded",
                "agent-runtime.delegation.children",
            ]
            .into_iter()
            .filter_map(|key| {
                snapshot
                    .extension_state
                    .get(key)
                    .map(|v| (key.to_owned(), v.clone()))
            })
            .collect(),
            updated: snapshot.updated,
        })
    }
    pub(in crate::journal) fn transition_view(
        current: &Self,
        delta: &JournalTransitionDelta,
        snapshot: &JournalSnapshotView,
    ) -> Self {
        let minimal = SessionSnapshot {
            id: snapshot.id.clone(),
            history: vec![],
            usage: Default::default(),
            identity: snapshot.identity.clone(),
            manifests: vec![],
            extension_state: [
                "runtime.session.fork_pending",
                "runtime.session.superseded",
                "agent-runtime.delegation.children",
            ]
            .into_iter()
            .filter_map(|key| {
                snapshot
                    .extension_state
                    .get(key)
                    .map(|v| (key.to_owned(), v.clone()))
            })
            .collect(),
            updated: snapshot.updated,
        };
        Self::transition(current, delta, &minimal)
    }
    pub(in crate::journal) fn snapshot(snapshot: &SessionSnapshot) -> Self {
        let mut roots = Self::default();
        if let Some(intent) = snapshot.extension_state.get("runtime.session.fork_pending") {
            roots.fork_present = true;
            roots.fork_unknown = intent.revision.as_str() != "session-fork-pending-1";
            if let Some(child) = intent
                .value
                .get("new_id")
                .and_then(serde_json::Value::as_str)
            {
                roots.fork_child = Some(SessionId::new(child));
            } else {
                roots.fork_unknown = true;
            }
        }
        roots.proof_child = snapshot
            .extension_state
            .get("runtime.session.superseded")
            .filter(|proof| proof.revision.as_str() == "session-superseded-1")
            .and_then(|proof| proof.value.as_str())
            .map(SessionId::new);
        if let Some(catalog) = snapshot
            .extension_state
            .get("agent-runtime.delegation.children")
        {
            roots.catalog_unknown = catalog.revision.as_str() != "resumable-child-catalog-1"
                || catalog
                    .value
                    .get("schema_version")
                    .and_then(serde_json::Value::as_u64)
                    != Some(1);
            if let Some(children) = catalog
                .value
                .get("children")
                .and_then(serde_json::Value::as_array)
            {
                for child in children {
                    if let Some(session) = child
                        .get("child_session")
                        .and_then(serde_json::Value::as_str)
                    {
                        roots.catalog_sessions.insert(SessionId::new(session));
                    } else {
                        roots.catalog_unknown = true;
                    }
                    let terminal = child
                        .get("status")
                        .and_then(|status| status.get("state"))
                        .and_then(|state| state.get("state"))
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|state| matches!(state, "stopped" | "failed" | "expired"));
                    roots.catalog_pending |= !terminal;
                }
            } else {
                roots.catalog_unknown = true;
            }
        }
        roots.refresh();
        roots
    }

    pub(in crate::journal) fn transition(
        current: &Self,
        delta: &JournalTransitionDelta,
        snapshot: &SessionSnapshot,
    ) -> Self {
        let mut next = Self::snapshot(snapshot);
        if delta.bootstrap.is_none() {
            // Ordinary policy may omit Sensitive intent/catalog bodies. Preserve
            // their non-content dependency index unless the semantic delta
            // explicitly changes that namespace; never infer clearing from an
            // omission in the previously projected snapshot.
            if !delta
                .extension_updates
                .contains_key("runtime.session.fork_pending")
            {
                next.fork_present = current.fork_present;
                next.fork_child = current.fork_child.clone();
                next.fork_unknown = current.fork_unknown;
            }
            if !delta
                .extension_updates
                .contains_key("runtime.session.superseded")
            {
                next.proof_child = current.proof_child.clone();
            }
            if !delta
                .extension_updates
                .contains_key("agent-runtime.delegation.children")
            {
                next.catalog_sessions = current.catalog_sessions.clone();
                next.catalog_pending = current.catalog_pending;
                next.catalog_unknown = current.catalog_unknown;
            }
            next.refresh();
        }
        next
    }

    pub(in crate::journal) fn refresh(&mut self) {
        self.sessions = self.catalog_sessions.clone();
        self.sessions.extend(self.fork_child.iter().cloned());
        self.completed_fork = self
            .fork_child
            .as_ref()
            .filter(|child| self.proof_child.as_ref() == Some(child))
            .cloned();
        self.unresolved = self.fork_unknown || self.catalog_unknown;
        self.pending = self.unresolved
            || self.catalog_pending
            || (self.fork_present && self.completed_fork.is_none());
    }

    pub(in crate::journal) fn extend(&mut self, roots: &Self) {
        self.sessions.extend(roots.sessions.iter().cloned());
        self.pending |= roots.pending;
        self.unresolved |= roots.unresolved;
    }

    pub(in crate::journal) fn validate(&self, state: &State) -> Result<(), RuntimeError> {
        if self.sessions.iter().any(|id| {
            state
                .sessions
                .get(id)
                .is_some_and(|session| session.retirement.is_some())
        }) {
            return Err(conflict());
        }
        Ok(())
    }
}
