use super::tests::{TestStore, test_coordinator};
use super::*;
use crate::runtime::history::HistoryGenerations;
use agent_runtime_lcm::decide_pressure;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug, Default)]
struct CountingSizer {
    entries: Mutex<Vec<u64>>,
    revision: AtomicUsize,
    huge: bool,
}

impl LcmSizer for CountingSizer {
    fn entry_tokens(&self, entry: &LcmEntry) -> u64 {
        self.entries.lock().unwrap().push(entry.sequence.get());
        if self.huge {
            u64::MAX
        } else {
            agent_runtime_lcm::CharRatioSizer::default().entry_tokens(entry)
        }
    }
    fn summary_tokens(&self, text: &str) -> u64 {
        agent_runtime_lcm::CharRatioSizer::default().summary_tokens(text)
    }
    fn revision(&self) -> RegistryRevision {
        RegistryRevision::new(format!(
            "counting-{}-{}",
            self.huge,
            self.revision.load(Ordering::SeqCst)
        ))
    }
}

fn fixture() -> (
    Arc<TestStore>,
    Arc<CountingSizer>,
    LcmCoordinator,
    LcmTimelineBinding,
) {
    let store = Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline")));
    let sizer = Arc::new(CountingSizer::default());
    let mut coordinator = test_coordinator(store.clone());
    coordinator.policy.sizer = sizer.clone();
    let binding = coordinator
        .timeline_binding(&SessionId::new("lcm-session"))
        .unwrap();
    (store, sizer, coordinator, binding)
}

fn reset(store: &TestStore, sizer: &CountingSizer) {
    store.read_ranges.lock().unwrap().clear();
    sizer.entries.lock().unwrap().clear();
}

// Independent full-recompute oracle: current sizer counts active summaries
// plus only the uncovered raw suffix, with checked accumulation.
async fn oracle(
    coordinator: &LcmCoordinator,
    binding: &LcmTimelineBinding,
    len: usize,
) -> Result<u64, RuntimeError> {
    let view = binding.view();
    let mut nodes = coordinator
        .store
        .active_nodes(&view)
        .await
        .map_err(map_lcm_error)?;
    nodes.sort_by_key(|node| (node.range.start, node.range.end, node.id.clone()));
    let end = nodes.last().map_or(0, |node| node.range.end.get() + 1);
    let active = nodes
        .iter()
        .map(|node| coordinator.policy.sizer.summary_tokens(&node.summary))
        .try_fold(0u64, u64::checked_add)
        .ok_or_else(|| RuntimeError::conflict("active overflow"))?;
    let entries = coordinator.load_entries(&view, len).await?;
    let raw = entries
        .iter()
        .filter(|entry| entry.sequence.get() >= end)
        .map(|entry| coordinator.policy.sizer.entry_tokens(entry))
        .try_fold(0u64, u64::checked_add)
        .ok_or_else(|| RuntimeError::conflict("raw overflow"))?;
    active
        .checked_add(raw)
        .ok_or_else(|| RuntimeError::conflict("context overflow"))
}

#[tokio::test]
async fn warm_unchanged_append_and_cold_resume_match_full_oracle() {
    let (store, sizer, coordinator, binding) = fixture();
    let mut generations = HistoryGenerations::default();
    let mut history = vec![
        Message::user("first"),
        Message::assistant(vec![ContentPart::text("answer")]),
    ];
    let held = generations.capture(&history);
    coordinator.register_history(&binding.session, &held);
    let state = coordinator
        .synchronize(&binding, None, &held.history)
        .await
        .unwrap();
    let expected = oracle(&coordinator, &binding, history.len()).await.unwrap();
    reset(&store, &sizer);
    assert_eq!(
        coordinator
            .accounted_context_tokens(&binding, &held.history, &state)
            .await
            .unwrap(),
        expected
    );
    assert_eq!(*sizer.entries.lock().unwrap(), [0, 1]);
    reset(&store, &sizer);
    assert_eq!(
        coordinator
            .accounted_context_tokens(&binding, &held.history, &state)
            .await
            .unwrap(),
        expected
    );
    assert!(store.read_ranges.lock().unwrap().is_empty());
    assert!(sizer.entries.lock().unwrap().is_empty());

    history.extend([
        Message::user("next"),
        Message::assistant(vec![ContentPart::text("more")]),
    ]);
    let appended = generations.capture(&history);
    coordinator.register_history(&binding.session, &appended);
    let state = coordinator
        .synchronize(&binding, Some(&state), &appended.history)
        .await
        .unwrap();
    let expected = oracle(&coordinator, &binding, history.len()).await.unwrap();
    reset(&store, &sizer);
    assert_eq!(
        coordinator
            .accounted_context_tokens(&binding, &appended.history, &state)
            .await
            .unwrap(),
        expected
    );
    assert_eq!(*store.read_ranges.lock().unwrap(), [(2, 3)]);
    assert_eq!(*sizer.entries.lock().unwrap(), [2, 3]);
    assert_eq!(held.history.len(), 2);

    // A new coordinator has no sidecar; the exact legacy-shaped Sensitive
    // extension record resumes through all of the existing source checks.
    let persisted = VersionedSessionState::new(
        coordinator.descriptor_value().revision().clone(),
        serde_json::to_value(&state).unwrap(),
    );
    let mut cold = test_coordinator(store.clone());
    cold.policy.sizer = sizer.clone();
    assert_eq!(
        cold.descriptor_value().revision(),
        coordinator.descriptor_value().revision()
    );
    assert!(
        cold.validate_resume_state(&binding.session, &history, &persisted, None)
            .await
            .unwrap()
            .is_none()
    );
    let decoded = cold.decode_state(&binding, &persisted).unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), persisted.value);
    cold.register_history(&binding.session, &appended);
    reset(&store, &sizer);
    assert_eq!(
        cold.accounted_context_tokens(&binding, &appended.history, &decoded)
            .await
            .unwrap(),
        expected
    );
    assert_eq!(*sizer.entries.lock().unwrap(), [0, 1, 2, 3]);
    reset(&store, &sizer);
    assert_eq!(
        cold.accounted_context_tokens(&binding, &appended.history, &decoded)
            .await
            .unwrap(),
        expected
    );
    assert!(store.read_ranges.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mismatched_accounting_evidence_rebuilds_once_then_stays_warm() {
    let (store, sizer, coordinator, mut binding) = fixture();
    let mut generations = HistoryGenerations::default();
    let generation = generations.capture(&[Message::user("first"), Message::user("second")]);
    coordinator.register_history(&binding.session, &generation);
    let mut state = coordinator
        .synchronize(&binding, None, &generation.history)
        .await
        .unwrap();
    coordinator
        .accounted_context_tokens(&binding, &generation.history, &state)
        .await
        .unwrap();

    for mismatch in 0..3 {
        match mismatch {
            0 => binding.authorization_revision = RegistryRevision::new("new authorized binding"),
            1 => {
                sizer.revision.fetch_add(1, Ordering::SeqCst);
            }
            _ => {
                *store.revision.lock().unwrap() = state.dag_revision.next().unwrap();
            }
        }
        state = coordinator
            .checkpoint_state(&binding, &generation.history, &[], None, None, 0)
            .await
            .unwrap();
        let expected = oracle(&coordinator, &binding, generation.history.len())
            .await
            .unwrap();
        reset(&store, &sizer);
        assert_eq!(
            coordinator
                .accounted_context_tokens(&binding, &generation.history, &state)
                .await
                .unwrap(),
            expected
        );
        assert_eq!(*sizer.entries.lock().unwrap(), [0, 1]);
        assert_eq!(*store.read_ranges.lock().unwrap(), [(0, 1)]);
        reset(&store, &sizer);
        assert_eq!(
            coordinator
                .accounted_context_tokens(&binding, &generation.history, &state)
                .await
                .unwrap(),
            expected
        );
        assert!(sizer.entries.lock().unwrap().is_empty());
        assert!(store.read_ranges.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn committed_summary_reuses_source_totals_and_strict_projection_reads_sources() {
    let (store, sizer, mut coordinator, binding) = fixture();
    coordinator.policy.pressure.retain_recent_entries = 0;
    let mut generations = HistoryGenerations::default();
    let history = vec![
        Message::user("source ".repeat(200)),
        Message::assistant(vec![ContentPart::text("answer ".repeat(200))]),
        Message::user("tail"),
    ];
    let generation = generations.capture(&history);
    coordinator.register_history(&binding.session, &generation);
    let state = coordinator
        .synchronize(&binding, None, &generation.history)
        .await
        .unwrap();
    coordinator
        .accounted_context_tokens(&binding, &generation.history, &state)
        .await
        .unwrap();
    let report = coordinator
        .compact_once(&binding, history.len(), false, Some(2))
        .await
        .unwrap();
    let pending = report.pending_summary.expect("leaf operation");
    coordinator
        .commit_pending_summary(&binding, &history, &pending, LCM_SUMMARY_PURPOSE)
        .await
        .unwrap();
    let state = coordinator
        .checkpoint_state(
            &binding,
            &generation.history,
            &[],
            Some(LCM_SUMMARY_PURPOSE.into()),
            None,
            0,
        )
        .await
        .unwrap();
    let expected = oracle(&coordinator, &binding, history.len()).await.unwrap();
    reset(&store, &sizer);
    assert_eq!(
        coordinator
            .accounted_context_tokens(&binding, &generation.history, &state)
            .await
            .unwrap(),
        expected
    );
    assert!(store.read_ranges.lock().unwrap().is_empty());
    assert!(sizer.entries.lock().unwrap().is_empty());
    let view = HistoryView {
        session: binding.session.clone(),
        turn: TurnId::new("next"),
        history: generation.history.clone(),
        active_history_start: 2,
        state: None,
    };
    assert!(
        coordinator
            .project_state(&binding, &state, &view)
            .await
            .unwrap()
            .omit_prefix
            > 0
    );
    assert!(
        !store.read_ranges.lock().unwrap().is_empty(),
        "strict projection retains full source validation"
    );
}

#[tokio::test]
async fn changed_revisions_grants_and_untrusted_history_cannot_reuse_accounting() {
    let (store, sizer, coordinator, mut binding) = fixture();
    let mut generations = HistoryGenerations::default();
    let history = vec![Message::user("first")];
    let generation = generations.capture(&history);
    coordinator.register_history(&binding.session, &generation);
    let state = coordinator
        .synchronize(&binding, None, &generation.history)
        .await
        .unwrap();
    coordinator
        .accounted_context_tokens(&binding, &generation.history, &state)
        .await
        .unwrap();
    // An equal externally supplied slice does not have trusted lineage.
    reset(&store, &sizer);
    coordinator
        .accounted_context_tokens(&binding, &history, &state)
        .await
        .unwrap();
    assert_eq!(*sizer.entries.lock().unwrap(), [0]);
    coordinator
        .accounted_context_tokens(&binding, &generation.history, &state)
        .await
        .unwrap();
    *store.revision.lock().unwrap() = state.dag_revision.next().unwrap();
    reset(&store, &sizer);
    assert_eq!(
        coordinator
            .accounted_context_tokens(&binding, &generation.history, &state)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    assert!(store.read_ranges.lock().unwrap().is_empty());
    let updated = coordinator
        .checkpoint_state(&binding, &generation.history, &[], None, None, 0)
        .await
        .unwrap();
    coordinator
        .accounted_context_tokens(&binding, &generation.history, &updated)
        .await
        .unwrap();
    assert_eq!(*sizer.entries.lock().unwrap(), [0]);
    let persisted = VersionedSessionState::new(
        coordinator.descriptor_value().revision().clone(),
        serde_json::to_value(&updated).unwrap(),
    );
    sizer.revision.fetch_add(1, Ordering::SeqCst);
    let decoded = coordinator.decode_state(&binding, &persisted).unwrap();
    assert!(coordinator.tunables_changed(&persisted, &decoded));
    let rebuilt = coordinator
        .validate_resume_state(&binding.session, &generation.history, &persisted, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        coordinator
            .decode_state(&binding, &rebuilt)
            .unwrap()
            .sizer_revision,
        sizer.revision()
    );
    reset(&store, &sizer);
    coordinator
        .accounted_context_tokens(&binding, &generation.history, &updated)
        .await
        .unwrap();
    assert_eq!(*sizer.entries.lock().unwrap(), [0]);
    binding.view_authority = LcmViewAuthority::new();
    reset(&store, &sizer);
    assert_eq!(
        coordinator
            .accounted_context_tokens(&binding, &generation.history, &updated)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Approval
    );
    assert!(store.read_ranges.lock().unwrap().is_empty());
    assert!(sizer.entries.lock().unwrap().is_empty());
    binding = coordinator.timeline_binding(&binding.session).unwrap();
    binding.view_authority.revoke();
    assert_eq!(
        coordinator
            .accounted_context_tokens(&binding, &generation.history, &updated)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Approval
    );
}

#[tokio::test]
async fn replaced_truncated_history_and_overflow_preserve_fail_closed_results() {
    let (store, sizer, mut coordinator, binding) = fixture();
    let mut generations = HistoryGenerations::default();
    let history = vec![Message::user("first"), Message::user("second")];
    let generation = generations.capture(&history);
    coordinator.register_history(&binding.session, &generation);
    let state = coordinator
        .synchronize(&binding, None, &generation.history)
        .await
        .unwrap();
    coordinator
        .accounted_context_tokens(&binding, &generation.history, &state)
        .await
        .unwrap();
    for changed in [
        vec![Message::user("replaced"), history[1].clone()],
        history[..1].to_vec(),
        vec![history[1].clone(), history[0].clone()],
    ] {
        let changed = generations.capture(&changed);
        coordinator.register_history(&binding.session, &changed);
        assert_eq!(
            coordinator
                .synchronize(&binding, Some(&state), &changed.history)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Conflict
        );
        assert_eq!(
            changed.fingerprint(changed.history.len()).unwrap(),
            LcmCoordinator::history_fingerprint(&changed.history).unwrap()
        );
    }
    coordinator.policy.sizer = Arc::new(CountingSizer {
        huge: true,
        ..Default::default()
    });
    let generation = generations.capture(&history);
    coordinator.register_history(&binding.session, &generation);
    assert_eq!(
        oracle(&coordinator, &binding, history.len())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    assert_eq!(
        coordinator
            .accounted_context_tokens(&binding, &generation.history, &state)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    reset(&store, &sizer);
}

#[tokio::test]
async fn paged_append_cache_loss_and_component_evidence_rebuild_or_reject() {
    let (store, sizer, coordinator, binding) = fixture();
    let mut generations = HistoryGenerations::default();
    let mut history = vec![Message::user("first")];
    let generation = generations.capture(&history);
    coordinator.register_history(&binding.session, &generation);
    let state = coordinator
        .synchronize(&binding, None, &generation.history)
        .await
        .unwrap();
    coordinator
        .accounted_context_tokens(&binding, &generation.history, &state)
        .await
        .unwrap();
    history.extend((0..2050).map(|i| Message::user(format!("entry {i}"))));
    let generation = generations.capture(&history);
    coordinator.register_history(&binding.session, &generation);
    let state = coordinator
        .synchronize(&binding, Some(&state), &generation.history)
        .await
        .unwrap();
    let expected = oracle(&coordinator, &binding, history.len()).await.unwrap();
    reset(&store, &sizer);
    assert_eq!(
        coordinator
            .accounted_context_tokens(&binding, &generation.history, &state)
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        *store.read_ranges.lock().unwrap(),
        [(1, 1024), (1025, 2048), (2049, 2050)]
    );
    assert_eq!(
        *sizer.entries.lock().unwrap(),
        (1..2051).collect::<Vec<_>>()
    );
    coordinator.accounting.lock().unwrap().clear();
    coordinator.register_history(&binding.session, &generation);
    reset(&store, &sizer);
    assert_eq!(
        coordinator
            .accounted_context_tokens(&binding, &generation.history, &state)
            .await
            .unwrap(),
        expected
    );
    assert_eq!(sizer.entries.lock().unwrap().len(), history.len());

    let persisted = VersionedSessionState::new(
        coordinator.descriptor_value().revision().clone(),
        serde_json::to_value(&state).unwrap(),
    );
    let mut changed_binding = binding.clone();
    changed_binding.authorization_revision = RegistryRevision::new("changed binding");
    assert_eq!(
        coordinator
            .decode_state(&changed_binding, &persisted)
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    for changed in
        [
            coordinator
                .clone()
                .with_source_classifier(Arc::new(DefaultLcmSourceClassifier::new(
                    Sensitivity::Public,
                ))),
            coordinator.clone().with_content_guard(Arc::new(
                super::tests::TestContentGuard::clean(Arc::new(AtomicUsize::new(0))),
            )),
        ]
    {
        assert_eq!(
            changed.decode_state(&binding, &persisted).unwrap_err().kind,
            ErrorKind::Conflict
        );
        reset(&store, &sizer);
        assert_eq!(
            changed
                .accounted_context_tokens(&binding, &generation.history, &state)
                .await
                .unwrap(),
            expected
        );
        assert_eq!(sizer.entries.lock().unwrap().len(), history.len());
    }
    *store.store_revision_override.lock().unwrap() = Some(RegistryRevision::new("changed store"));
    assert_eq!(
        coordinator
            .decode_state(&binding, &persisted)
            .unwrap_err()
            .kind,
        ErrorKind::Conflict
    );
    reset(&store, &sizer);
    assert_eq!(
        coordinator
            .accounted_context_tokens(&binding, &generation.history, &state)
            .await
            .unwrap(),
        expected
    );
    assert_eq!(sizer.entries.lock().unwrap().len(), history.len());
}

#[tokio::test]
async fn summarized_source_overflow_does_not_overflow_a_valid_raw_suffix() {
    let (store, _, mut coordinator, binding) = fixture();
    coordinator.policy.pressure.retain_recent_entries = 0;
    let history = vec![
        Message::user("source ".repeat(200)),
        Message::assistant(vec![ContentPart::text("answer ".repeat(200))]),
        Message::user("tail"),
    ];
    let mut generations = HistoryGenerations::default();
    let generation = generations.capture(&history);
    coordinator.register_history(&binding.session, &generation);
    coordinator
        .synchronize(&binding, None, &generation.history)
        .await
        .unwrap();
    let pending = coordinator
        .compact_once(&binding, history.len(), false, Some(2))
        .await
        .unwrap()
        .pending_summary
        .unwrap();
    coordinator
        .commit_pending_summary(&binding, &history, &pending, LCM_SUMMARY_PURPOSE)
        .await
        .unwrap();
    #[derive(Debug)]
    struct CoveredOverflowSizer;
    impl LcmSizer for CoveredOverflowSizer {
        fn entry_tokens(&self, entry: &LcmEntry) -> u64 {
            if entry.sequence.get() < 2 {
                u64::MAX
            } else {
                1
            }
        }
        fn summary_tokens(&self, _: &str) -> u64 {
            1
        }
        fn revision(&self) -> RegistryRevision {
            RegistryRevision::new("covered-overflow")
        }
    }
    coordinator.policy.sizer = Arc::new(CoveredOverflowSizer);
    let state = coordinator
        .checkpoint_state(&binding, &generation.history, &[], None, None, 0)
        .await
        .unwrap();
    assert_eq!(state.active_nodes.len(), 1);
    assert_eq!(
        coordinator
            .accounted_context_tokens(&binding, &generation.history, &state)
            .await
            .unwrap(),
        oracle(&coordinator, &binding, history.len()).await.unwrap()
    );
    assert!(store.read_ranges.lock().unwrap().len() >= 2);
}

#[tokio::test]
async fn session_generation_drop_releases_sidecar_but_preserves_held_arc_values() {
    let (_, _, coordinator, binding) = fixture();
    let coordinator = Arc::new(coordinator);
    let mut generations = HistoryGenerations::default();
    let generation = generations.capture(&[Message::user("held")]);
    let held = generation.history.clone();
    generations.register_cleanup(&binding.session, &coordinator);
    coordinator.register_history(&binding.session, &generation);
    let state = coordinator
        .synchronize(&binding, None, &generation.history)
        .await
        .unwrap();
    coordinator
        .accounted_context_tokens(&binding, &generation.history, &state)
        .await
        .unwrap();
    assert_eq!(coordinator.accounting.lock().unwrap().len(), 1);
    drop(generation);
    drop(generations);
    assert!(coordinator.accounting.lock().unwrap().is_empty());
    assert_eq!(&*held, &[Message::user("held")]);
}

#[tokio::test]
async fn warm_leaf_and_condensation_deltas_match_reference_store_oracle() {
    let (_, sizer, mut coordinator, _) = fixture();
    let store = Arc::new(agent_runtime_lcm::memory::InMemoryLcmStore::new(
        LcmTimelineId::new("lcm-timeline"),
    ));
    let binding = LcmTimelineBinding::new(
        SessionId::new("lcm-session"),
        LcmTimelineId::new("lcm-timeline"),
        RegistryRevision::new("lcm-auth-v1"),
        store.authority(),
    )
    .unwrap();
    coordinator.store = store;
    coordinator.resolver = Arc::new(StaticLcmTimelineResolver::new(binding.clone()));
    coordinator.policy.pressure.retain_recent_entries = 0;
    coordinator.policy.pressure.leaf_target_tokens = 50;
    coordinator = coordinator
        .with_summary_policy(LcmEscalationPolicy {
            leaf_source_target_tokens: 50,
            ..Default::default()
        })
        .unwrap();
    coordinator.policy.pressure.condensation_fanout = 2;
    let mut history = Vec::new();
    for turn in 0..3 {
        history.push(Message::user(format!(
            "question {turn} {}",
            "source ".repeat(100)
        )));
        history.push(Message::assistant(vec![ContentPart::text(
            "answer ".repeat(100),
        )]));
    }
    history.push(Message::user("protected tail"));
    let mut generations = HistoryGenerations::default();
    let generation = generations.capture(&history);
    coordinator.register_history(&binding.session, &generation);
    let mut state = coordinator
        .synchronize(&binding, None, &generation.history)
        .await
        .unwrap();
    coordinator
        .accounted_context_tokens(&binding, &generation.history, &state)
        .await
        .unwrap();
    for round in 0..4 {
        let pending = coordinator
            .compact_once(&binding, history.len(), false, Some(6))
            .await
            .unwrap()
            .pending_summary
            .unwrap();
        let (node, _) = coordinator
            .commit_pending_summary(&binding, &history, &pending, LCM_SUMMARY_PURPOSE)
            .await
            .unwrap();
        assert_eq!(
            node.kind,
            if round < 3 {
                agent_runtime_lcm::LcmNodeKind::Leaf
            } else {
                agent_runtime_lcm::LcmNodeKind::Condensed
            }
        );
        state = coordinator
            .checkpoint_state(&binding, &generation.history, &[], None, None, 0)
            .await
            .unwrap();
        let expected = oracle(&coordinator, &binding, history.len()).await.unwrap();
        sizer.entries.lock().unwrap().clear();
        let actual = coordinator
            .accounted_context_tokens(&binding, &generation.history, &state)
            .await
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(
            decide_pressure(
                actual,
                coordinator.policy.input_budget_tokens,
                0,
                &coordinator.policy.pressure
            ),
            decide_pressure(
                expected,
                coordinator.policy.input_budget_tokens,
                0,
                &coordinator.policy.pressure
            )
        );
        assert!(sizer.entries.lock().unwrap().is_empty());
    }
    assert_eq!(state.active_nodes.len(), 1);
    assert_eq!(state.active_nodes[0].range.end.get(), 5);
}
