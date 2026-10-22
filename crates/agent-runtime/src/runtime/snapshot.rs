//! Runtime-owned mutation provenance for journal preparation.
use std::collections::BTreeMap;
use std::sync::Arc;

use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::content::Message;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::journal::{JournalSnapshotView, journal_json_identical};
use agent_runtime_core::store::{
    SessionIdentityState, SessionSnapshot, TurnManifest, VersionedSessionState,
};

use super::state::SessionState;

#[derive(Debug, Default)]
pub(crate) struct LiveSnapshot {
    enabled: bool,
    view: Option<JournalSnapshotView>,
    history_from: Option<usize>,
    usage_from: Option<usize>,
    manifests: Vec<TurnManifest>,
    manifest_keep: Option<usize>,
}

impl LiveSnapshot {
    pub(crate) fn enable(&mut self) {
        self.enabled = true;
    }
    pub(crate) fn disable(&mut self) {
        *self = Self::default();
    }
    pub(crate) fn history_changed(&mut self, from: usize) {
        self.history_from = Some(self.history_from.map_or(from, |old| old.min(from)));
    }
    pub(crate) fn usage_changed(&mut self, from: usize) {
        self.usage_from = Some(self.usage_from.map_or(from, |old| old.min(from)));
    }
    pub(crate) fn manifest_appended(&mut self, value: TurnManifest, keep: usize) {
        if self.view.is_some() {
            self.manifests.push(value);
            self.manifest_keep = Some(keep);
        }
    }
    pub(crate) fn capture(
        &mut self,
        session: &SessionId,
        state: &SessionState,
        extensions: BTreeMap<String, Arc<VersionedSessionState>>,
        identity: SessionIdentityState,
        updated: Timestamp,
        include_manifests: bool,
    ) -> Result<JournalSnapshotView, RuntimeError> {
        let compatible = self.enabled
            && self.view.as_ref().is_some_and(|view| {
                (view.history_len() == state.history.len() || self.history_from.is_some())
                    && (view.usage_len() == state.usage.records().len()
                        || self.usage_from.is_some())
                    && (view.manifest_len() == state.manifests.len()
                        || self.manifest_keep.is_some())
            });
        let view = if compatible {
            let base = self.view.as_ref().expect("compatible live version");
            let mut mutation = base.mutation();
            if let Some(from) = self.history_from {
                let from = from.min(base.history_len()).min(state.history.len());
                mutation.truncate_history(from);
                mutation.append_history(state.history[from..].iter().cloned());
            }
            if let Some(from) = self.usage_from {
                let from = from.min(base.usage_len()).min(state.usage.records().len());
                mutation.truncate_usage(from);
                mutation.append_usage(state.usage.records()[from..].iter().cloned());
            }
            mutation.append_manifests(self.manifests.iter().cloned());
            if let Some(keep) = self.manifest_keep {
                mutation.retain_manifests(keep);
            }
            for (key, old) in base.extensions() {
                if !extensions.contains_key(key) {
                    mutation.set_extension(key.clone(), None);
                } else if !base
                    .extension_shared(key)
                    .zip(extensions.get(key))
                    .is_some_and(|(old, next)| Arc::ptr_eq(&old, next))
                    && !state_identical(Some(old), extensions.get(key).map(Arc::as_ref))
                {
                    mutation.set_extension_shared(
                        key.clone(),
                        extensions.get(key).expect("present namespace").clone(),
                    );
                }
            }
            for (key, value) in &extensions {
                if base.extension(key).is_none() {
                    mutation.set_extension_shared(key.clone(), value.clone());
                }
            }
            mutation.finish(identity, updated)?
        } else {
            JournalSnapshotView::from_snapshot(SessionSnapshot {
                id: session.clone(),
                history: state.history.clone(),
                usage: state.usage.clone(),
                identity,
                manifests: state.manifests.clone(),
                extension_state: extensions
                    .into_iter()
                    .map(|(key, value)| (key, value.as_ref().clone()))
                    .collect(),
                updated,
            })?
        };
        if self.enabled {
            self.view = Some(view.clone());
        }
        self.history_from = None;
        self.usage_from = None;
        self.manifests.clear();
        self.manifest_keep = None;
        if include_manifests {
            Ok(view)
        } else {
            let mut mutation = view.mutation();
            let count = super::manifests::planned_steps(
                view.extension(super::MANIFEST_BOUNDARY_NAMESPACE),
                &[],
            )?;
            mutation.set_extension(
                super::MANIFEST_BOUNDARY_NAMESPACE.to_owned(),
                Some(super::manifests::boundary_record(count)),
            );
            mutation.retain_manifests(0);
            mutation.finish(view.identity().clone(), view.updated())
        }
    }
}

fn state_identical(
    left: Option<&VersionedSessionState>,
    right: Option<&VersionedSessionState>,
) -> bool {
    match (left, right) {
        (Some(a), Some(b)) => {
            a.revision == b.revision
                && a.sensitivity == b.sensitivity
                && journal_json_identical(&a.value, &b.value)
        }
        (None, None) => true,
        _ => false,
    }
}

impl super::state::SessionExecutionContext {
    pub(crate) fn append_history(
        &self,
        state: &mut SessionState,
        values: impl IntoIterator<Item = Message>,
    ) {
        self.live_snapshot
            .lock()
            .expect("live snapshot poisoned")
            .history_changed(state.history.len());
        state.history.extend(values);
    }
    pub(crate) fn truncate_history(&self, state: &mut SessionState, keep: usize) {
        self.live_snapshot
            .lock()
            .expect("live snapshot poisoned")
            .history_changed(keep);
        state.history.truncate(keep);
    }
    pub(crate) fn pop_history(&self, state: &mut SessionState) -> Option<Message> {
        self.live_snapshot
            .lock()
            .expect("live snapshot poisoned")
            .history_changed(state.history.len().saturating_sub(1));
        state.history.pop()
    }
    pub(crate) fn truncate_usage(&self, state: &mut SessionState, keep: usize) {
        self.live_snapshot
            .lock()
            .expect("live snapshot poisoned")
            .usage_changed(keep);
        state.usage.truncate(keep);
    }
    pub(crate) fn replace_usage(
        &self,
        state: &mut SessionState,
        usage: agent_runtime_core::usage::UsageLedger,
    ) {
        self.live_snapshot
            .lock()
            .expect("live snapshot poisoned")
            .usage_changed(0);
        state.usage = usage;
    }
    pub(crate) fn record_usage(
        &self,
        state: &mut SessionState,
        value: agent_runtime_core::usage::UsageRecord,
    ) {
        self.live_snapshot
            .lock()
            .expect("live snapshot poisoned")
            .usage_changed(state.usage.records().len());
        state.usage.record(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::journal::JournalWriterAssembler;

    #[test]
    fn recorded_appends_rollbacks_and_rewrites_match_complete_encoding() {
        let id = SessionId::new("live-differential");
        let mut live = LiveSnapshot::default();
        live.enable();
        let mut state = SessionState::with_history(
            (0..100)
                .map(|i| Message::user(format!("seed {i}")))
                .collect(),
        );
        let mut previous = None;
        let mut encoding = None;
        for step in 0..130 {
            let from = if step % 11 == 0 {
                state.history.len().saturating_sub(2)
            } else {
                state.history.len()
            };
            live.history_changed(from);
            state.history.truncate(from);
            state.history.push(Message::user(format!("edit {step}")));
            let extensions = BTreeMap::from([(
                super::super::MANIFEST_BOUNDARY_NAMESPACE.to_owned(),
                super::super::manifests::boundary_record(0),
            )]);
            let view = live
                .capture(
                    &id,
                    &state,
                    extensions
                        .clone()
                        .into_iter()
                        .map(|(key, value)| (key, Arc::new(value)))
                        .collect(),
                    Default::default(),
                    Timestamp(step),
                    true,
                )
                .unwrap();
            let expected = SessionSnapshot {
                id: id.clone(),
                history: state.history.clone(),
                usage: state.usage.clone(),
                identity: Default::default(),
                manifests: vec![],
                extension_state: extensions,
                updated: Timestamp(step),
            };
            assert_eq!(
                serde_json::to_vec(&view.materialize()).unwrap(),
                serde_json::to_vec(&expected).unwrap()
            );
            let mut shared = JournalWriterAssembler::default();
            let (roots, cache) = shared
                .snapshot_shared_cached(&view, 0, previous.as_ref(), encoding.as_ref())
                .unwrap();
            let mut complete = JournalWriterAssembler::default();
            assert_eq!(roots, complete.snapshot(&expected, 0).unwrap());
            for (id, object) in &shared.objects {
                assert_eq!(complete.objects.get(id), Some(object));
            }
            previous = Some(view);
            encoding = Some(cache);
        }
    }
}
