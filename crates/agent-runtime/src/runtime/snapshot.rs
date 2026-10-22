//! Runtime-owned mutation provenance for journal preparation.
use std::collections::BTreeMap;
use std::sync::Arc;

use agent_runtime_core::clock::Timestamp;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::journal::{JournalSnapshotView, journal_json_identical};
use agent_runtime_core::store::{SessionIdentityState, SessionSnapshot, VersionedSessionState};

use super::state::SessionState;

#[derive(Debug, Default)]
pub(crate) struct LiveSnapshot {
    enabled: bool,
    view: Option<JournalSnapshotView>,
    /// The `SessionState` instance the view was captured from.
    instance: Option<Arc<()>>,
    #[cfg(test)]
    incremental_captures: usize,
}

impl LiveSnapshot {
    pub(crate) fn enable(&mut self) {
        self.enabled = true;
    }
    pub(crate) fn capture(
        &mut self,
        session: &SessionId,
        state: &mut SessionState,
        extensions: BTreeMap<String, Arc<VersionedSessionState>>,
        identity: SessionIdentityState,
        updated: Timestamp,
        include_manifests: bool,
    ) -> Result<JournalSnapshotView, RuntimeError> {
        let changes = state.take_changes();
        let compatible = self.enabled
            && !changes.manifests_replaced
            && self
                .instance
                .as_ref()
                .is_some_and(|instance| Arc::ptr_eq(instance, state.instance()))
            && self.view.as_ref().is_some_and(|view| {
                (view.history_len() == state.history().len() || changes.history_from.is_some())
                    && (view.usage_len() == state.usage().records().len()
                        || changes.usage_from.is_some())
                    && (view.manifest_len() == state.manifests().len()
                        || changes.manifests_appended != 0)
            });
        let view = if compatible {
            #[cfg(test)]
            {
                self.incremental_captures += 1;
            }
            let base = self.view.as_ref().expect("compatible live version");
            let mut mutation = base.mutation();
            if let Some(from) = changes.history_from {
                let from = from.min(base.history_len()).min(state.history().len());
                mutation.truncate_history(from);
                mutation.append_history(state.history()[from..].iter().cloned());
            }
            if let Some(from) = changes.usage_from {
                let from = from
                    .min(base.usage_len())
                    .min(state.usage().records().len());
                mutation.truncate_usage(from);
                mutation.append_usage(state.usage().records()[from..].iter().cloned());
            }
            if changes.manifests_appended != 0 {
                let manifests = state.manifests();
                let appended = changes.manifests_appended.min(manifests.len());
                mutation.append_manifests(manifests[manifests.len() - appended..].iter().cloned());
                mutation.retain_manifests(manifests.len());
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
            let view = mutation.finish(identity, updated)?;
            #[cfg(any(test, debug_assertions))]
            {
                let complete =
                    complete_snapshot(session, state, &extensions, view.identity(), updated);
                assert_eq!(
                    serde_json::to_value(view.materialize()).expect("live view serializes"),
                    serde_json::to_value(&complete).expect("complete snapshot serializes"),
                    "live snapshot view diverged from the complete session state"
                );
            }
            view
        } else {
            JournalSnapshotView::from_snapshot(complete_snapshot(
                session,
                state,
                &extensions,
                &identity,
                updated,
            ))?
        };
        if self.enabled {
            self.view = Some(view.clone());
            self.instance = Some(state.instance().clone());
        }
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

fn complete_snapshot(
    session: &SessionId,
    state: &SessionState,
    extensions: &BTreeMap<String, Arc<VersionedSessionState>>,
    identity: &SessionIdentityState,
    updated: Timestamp,
) -> SessionSnapshot {
    SessionSnapshot {
        id: session.clone(),
        history: state.history().to_vec(),
        usage: state.usage().clone(),
        identity: identity.clone(),
        manifests: state.manifests().to_vec(),
        extension_state: extensions
            .iter()
            .map(|(key, value)| (key.clone(), value.as_ref().clone()))
            .collect(),
        updated,
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use agent_runtime_core::content::Message;
    use agent_runtime_core::ids::TurnId;
    use agent_runtime_core::journal::JournalWriterAssembler;
    use agent_runtime_core::manifest::{CapabilityResolution, ModelResolution, RunManifest};
    use agent_runtime_core::provider::ModelId;
    use agent_runtime_core::store::TurnManifest;
    use agent_runtime_core::usage::{
        Provenance, UsageDelta, UsageLedger, UsageRecord, UsageSource,
    };
    use agent_runtime_registry::{Fingerprint, RegistryRevision};

    fn usage(i: u64) -> UsageRecord {
        UsageRecord {
            source: UsageSource::ToolLoop,
            provenance: Provenance {
                purpose: Some(format!("u{i}")),
                ..Provenance::default()
            },
            delta: UsageDelta::new(),
        }
    }

    fn manifest(index: u64) -> TurnManifest {
        let fp = Fingerprint::of(index.to_string());
        TurnManifest::new(
            TurnId::new(format!("t{index}")),
            RunManifest::new(
                fp.clone(),
                fp.clone(),
                ModelResolution::new("p", ModelId::new("m"), fp.clone(), BTreeMap::new()),
                CapabilityResolution::new(RegistryRevision::new("r")),
                fp.clone(),
                fp,
            ),
        )
    }

    fn namespace(value: serde_json::Value) -> Arc<VersionedSessionState> {
        Arc::new(VersionedSessionState::new(
            RegistryRevision::new("r"),
            value,
        ))
    }

    /// Captures through `live` and checks the view against a complete
    /// capture of the same state, both materialized and as writer roots.
    struct Oracle {
        id: SessionId,
        previous: Option<JournalSnapshotView>,
        encoding: Option<agent_runtime_core::journal::JournalWriterEncoding>,
    }
    impl Oracle {
        fn new() -> Self {
            Self {
                id: SessionId::new("live-differential"),
                previous: None,
                encoding: None,
            }
        }
        fn check(
            &mut self,
            live: &mut LiveSnapshot,
            state: &mut SessionState,
            extensions: &BTreeMap<String, Arc<VersionedSessionState>>,
            step: u64,
            include_manifests: bool,
        ) -> JournalSnapshotView {
            let view = live
                .capture(
                    &self.id,
                    state,
                    extensions.clone(),
                    Default::default(),
                    Timestamp(step),
                    include_manifests,
                )
                .unwrap();
            let expected = LiveSnapshot::default()
                .capture(
                    &self.id,
                    state,
                    extensions.clone(),
                    Default::default(),
                    Timestamp(step),
                    include_manifests,
                )
                .unwrap()
                .materialize();
            assert_eq!(
                serde_json::to_value(view.materialize()).unwrap(),
                serde_json::to_value(&expected).unwrap(),
                "step {step}"
            );
            let planned = super::super::manifests::planned_steps(
                view.extension(super::super::MANIFEST_BOUNDARY_NAMESPACE),
                &[],
            )
            .unwrap();
            let mut shared = JournalWriterAssembler::default();
            let (roots, cache) = shared
                .snapshot_shared_cached(
                    &view,
                    planned,
                    self.previous.as_ref(),
                    self.encoding.as_ref(),
                )
                .unwrap();
            let mut complete = JournalWriterAssembler::default();
            assert_eq!(
                roots,
                complete.snapshot(&expected, planned).unwrap(),
                "step {step}"
            );
            for (id, object) in &shared.objects {
                assert_eq!(complete.objects.get(id), Some(object));
            }
            self.previous = Some(view.clone());
            self.encoding = Some(cache);
            view
        }
    }

    fn boundary() -> BTreeMap<String, Arc<VersionedSessionState>> {
        BTreeMap::from([(
            super::super::MANIFEST_BOUNDARY_NAMESPACE.to_owned(),
            Arc::new(super::super::manifests::boundary_record(0)),
        )])
    }

    #[test]
    fn recorded_appends_rollbacks_and_rewrites_match_complete_encoding() {
        let mut live = LiveSnapshot::default();
        live.enable();
        let mut oracle = Oracle::new();
        let mut state = SessionState::with_history(
            (0..100)
                .map(|i| Message::user(format!("seed {i}")))
                .collect(),
        );
        let extensions = boundary();
        for step in 0..130 {
            if step % 11 == 0 {
                state.truncate_history(state.history().len().saturating_sub(2));
            }
            state.append_history([Message::user(format!("edit {step}"))]);
            oracle.check(&mut live, &mut state, &extensions, step, true);
        }
        assert_eq!(live.incremental_captures, 129);
    }

    #[test]
    fn usage_manifests_and_namespaces_match_complete_capture() {
        let window = NonZeroUsize::new(4).unwrap();
        let mut live = LiveSnapshot::default();
        live.enable();
        let mut oracle = Oracle::new();
        let mut state = SessionState::with_history(vec![Message::user("seed")]);
        let mut extensions = boundary();
        let mut next_usage = 0;
        let mut next_manifest = 0;
        for step in 0..90u64 {
            match step % 9 {
                // Usage: record, roll back to an append boundary, replace.
                0 => {
                    for _ in 0..3 {
                        state.record_usage(usage(next_usage));
                        next_usage += 1;
                    }
                }
                1 => {
                    let keep = state.usage().records().len().saturating_sub(2);
                    state.truncate_usage(keep);
                    state.record_usage(usage(next_usage));
                    next_usage += 1;
                }
                2 => {
                    let mut ledger = UsageLedger::new();
                    for record in state.usage().records().iter().take(1) {
                        ledger.record(record.clone());
                    }
                    ledger.record(usage(next_usage));
                    next_usage += 1;
                    state.replace_usage(ledger);
                }
                // Manifests: several appends per capture past a full window.
                3 | 4 => {
                    for _ in 0..(step % 9 + 2) {
                        state.push_manifest(manifest(next_manifest), window);
                        next_manifest += 1;
                    }
                    extensions.insert(
                        super::super::MANIFEST_BOUNDARY_NAMESPACE.to_owned(),
                        Arc::new(super::super::manifests::boundary_record(next_manifest)),
                    );
                }
                // Namespaces: add, replace in place, copy-on-write, remove.
                5 => {
                    extensions.insert(
                        format!("ns.{step}"),
                        namespace(serde_json::json!({"n": step})),
                    );
                    extensions.insert("ns.stable".to_owned(), namespace(serde_json::json!([step])));
                }
                6 => {
                    if let Some(value) = extensions.get_mut("ns.stable") {
                        Arc::make_mut(value).value = serde_json::json!({"cow": step});
                    }
                }
                7 => {
                    let key = extensions
                        .keys()
                        .find(|key| key.starts_with("ns.") && key.as_str() != "ns.stable")
                        .cloned();
                    if let Some(key) = key {
                        extensions.remove(&key);
                    }
                }
                _ => {
                    // An identical value under a fresh Arc must not rewrite.
                    if let Some(value) = extensions.get("ns.stable").cloned() {
                        extensions.insert("ns.stable".to_owned(), Arc::new(value.as_ref().clone()));
                    }
                    state.append_history([Message::user(format!("m{step}"))]);
                }
            }
            oracle.check(&mut live, &mut state, &extensions, step, step % 2 == 0);
        }
        assert!(next_manifest > window.get() as u64 * 2);
        assert_eq!(live.incremental_captures, 89);
    }

    #[test]
    fn truncate_and_append_in_one_interval_and_clamped_positions_match() {
        let mut live = LiveSnapshot::default();
        live.enable();
        let mut oracle = Oracle::new();
        let extensions = boundary();
        let mut state = SessionState::with_history(
            (0..20)
                .map(|i| Message::user(format!("seed {i}")))
                .collect(),
        );
        oracle.check(&mut live, &mut state, &extensions, 0, true);
        // A recorded truncate plus several appends, then a rewrite of the tail.
        state.truncate_history(12);
        state.append_history([Message::user("a"), Message::user("b")]);
        state.pop_history();
        state.append_history([Message::user("c")]);
        oracle.check(&mut live, &mut state, &extensions, 1, true);
        // Recorded positions beyond the captured base clamp to it.
        let len = state.history().len();
        state.truncate_history(len + 7);
        state.truncate_usage(0);
        state.append_history([Message::user("d")]);
        oracle.check(&mut live, &mut state, &extensions, 2, true);
        assert_eq!(live.incremental_captures, 2);
        // An unrecorded append (length differs, nothing recorded) falls back
        // to a complete capture.
        state
            .history_mut_unrecorded()
            .push(Message::user("unrecorded"));
        oracle.check(&mut live, &mut state, &extensions, 3, true);
        assert_eq!(live.incremental_captures, 2);
        // A replaced state instance with an equal length is never patched
        // from the old view.
        let mut replaced = SessionState::with_history(
            state
                .history()
                .iter()
                .enumerate()
                .map(|(i, _)| Message::user(format!("replaced {i}")))
                .collect(),
        );
        oracle.check(&mut live, &mut replaced, &extensions, 4, true);
        assert_eq!(live.incremental_captures, 2);
    }

    #[test]
    #[should_panic(expected = "live snapshot view diverged")]
    fn same_length_unrecorded_edit_is_caught_by_the_capture_check() {
        let mut live = LiveSnapshot::default();
        live.enable();
        let extensions = boundary();
        let id = SessionId::new("stale");
        let mut state = SessionState::with_history(vec![Message::user("a"), Message::user("b")]);
        live.capture(
            &id,
            &mut state,
            extensions.clone(),
            Default::default(),
            Timestamp(0),
            true,
        )
        .unwrap();
        state.history_mut_unrecorded()[1] = Message::user("edited in place");
        let _ = live.capture(
            &id,
            &mut state,
            extensions,
            Default::default(),
            Timestamp(1),
            true,
        );
    }
}
