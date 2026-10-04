//! Process-local evidence only; none of this is checkpointed or authorizes a view.

use std::sync::{Mutex, Weak};

use super::*;
use crate::runtime::history::HistoryGeneration;

#[derive(Default)]
pub(super) struct AccountingSlot {
    generation: Weak<HistoryGeneration>,
    lineage: Weak<Mutex<Vec<agent_runtime_registry::FingerprintHasher>>>,
    totals: Option<EntryTotals>,
}

#[derive(Clone)]
struct EntryTotals {
    binding: LcmTimelineBinding,
    descriptor: RegistryRevision,
    dag_revision: LcmRevision,
    // u128 avoids rejecting a valid u64 raw suffix merely because already
    // summarized source entries overflow u64. Every accumulation/subtraction
    // and the final provider-facing sum remains checked.
    prefixes: Vec<u128>,
    active: Option<(LcmRevision, usize, u64)>,
}

impl LcmCoordinator {
    pub(super) fn invalidate_accounting(&self, session: &SessionId) {
        let mut slots = self.accounting.lock().expect("LCM accounting poisoned");
        if let Some(slot) = slots.get_mut(session) {
            slot.totals = None;
        }
    }

    pub(crate) fn release_history(&self, session: &SessionId, generation: &Arc<HistoryGeneration>) {
        let mut slots = self.accounting.lock().expect("LCM accounting poisoned");
        if slots
            .get(session)
            .and_then(|slot| slot.generation.upgrade())
            .is_some_and(|current| Arc::ptr_eq(&current, generation))
        {
            slots.remove(session);
            self.overheads
                .lock()
                .expect("LCM overheads poisoned")
                .remove(session);
            self.conversation_tokens
                .lock()
                .expect("LCM conversation counts poisoned")
                .remove(session);
        }
    }

    pub(crate) fn register_history(
        &self,
        session: &SessionId,
        generation: &Arc<HistoryGeneration>,
    ) {
        let mut slots = self.accounting.lock().expect("LCM accounting poisoned");
        if let Some(slot) = slots.get_mut(session) {
            if !slot.lineage.ptr_eq(&generation.lineage()) {
                slot.totals = None;
            }
            slot.generation = Arc::downgrade(generation);
            slot.lineage = generation.lineage();
        } else {
            slots.insert(
                session.clone(),
                AccountingSlot {
                    generation: Arc::downgrade(generation),
                    lineage: generation.lineage(),
                    totals: None,
                },
            );
        }
        // A coordinator may outlive its sessions. Do not retain their counts.
        slots.retain(|_, slot| slot.generation.strong_count() > 0);
    }

    fn trusted_generation(
        &self,
        binding: &LcmTimelineBinding,
        history: &[Message],
    ) -> Option<Arc<HistoryGeneration>> {
        let slots = self.accounting.lock().expect("LCM accounting poisoned");
        let generation = slots.get(&binding.session)?.generation.upgrade()?;
        (generation.history.len() == history.len()
            && std::ptr::eq(generation.history.as_ptr(), history.as_ptr()))
        .then_some(generation)
    }

    pub(super) fn canonical_fingerprint(
        &self,
        binding: &LcmTimelineBinding,
        history: &[Message],
        len: usize,
    ) -> Result<Fingerprint, RuntimeError> {
        self.store
            .authorize_view(&binding.view())
            .map_err(map_lcm_error)?;
        match self.trusted_generation(binding, history) {
            Some(generation) => generation.fingerprint(len),
            None => Self::history_fingerprint(&history[..len]),
        }
    }

    // Called only with the exact revision returned by a successful local
    // append/CAS. External DAG changes never become warm cache evidence.
    pub(super) fn accounting_commit(
        &self,
        binding: &LcmTimelineBinding,
        before: LcmRevision,
        after: LcmRevision,
        node: Option<&agent_runtime_lcm::LcmNode>,
    ) {
        let mut slots = self.accounting.lock().expect("LCM accounting poisoned");
        if let Some(totals) = slots
            .get_mut(&binding.session)
            .and_then(|slot| slot.totals.as_mut())
        {
            if totals.binding == *binding
                && totals.dag_revision == before
                && before.next() == Some(after)
            {
                totals.dag_revision = after;
                totals.active = totals.active.and_then(|(revision, covered, active)| {
                    if revision != before {
                        return None;
                    }
                    let (covered, active) = match node {
                        // An immutable append changes no active node record.
                        None => (covered, active),
                        Some(node) => match node.kind {
                            agent_runtime_lcm::LcmNodeKind::Leaf => {
                                if node.range.start.get() != covered as u64 {
                                    return None;
                                }
                                let end = node.range.end.get().checked_add(1)?;
                                (
                                    usize::try_from(end).ok()?,
                                    active.checked_add(
                                        self.policy.sizer.summary_tokens(&node.summary),
                                    )?,
                                )
                            }
                            // Children can carry different historical sizing
                            // revisions. Recompute the active-node total after
                            // condensation rather than subtracting their old
                            // persisted count from a current measured total.
                            agent_runtime_lcm::LcmNodeKind::Condensed => return None,
                        },
                    };
                    Some((after, covered, active))
                });
            }
        }
    }

    pub(super) async fn accounted_context_tokens(
        &self,
        binding: &LcmTimelineBinding,
        history: &[Message],
        state: &LcmState,
    ) -> Result<u64, RuntimeError> {
        let view = binding.view();
        // Authorization always precedes even a no-delta cache lookup.
        self.store.authorize_view(&view).map_err(map_lcm_error)?;
        let revision = self
            .store
            .current_revision(&view)
            .await
            .map_err(map_lcm_error)?;
        if revision != state.dag_revision {
            return Err(lcm_revision_conflict(
                "LCM store changed before pressure accounting",
            ));
        }
        let descriptor = self.descriptor_value().revision().clone();
        let generation = self.trusted_generation(binding, history);
        let mut totals = {
            let mut slots = self.accounting.lock().expect("LCM accounting poisoned");
            slots
                .get_mut(&binding.session)
                .and_then(|slot| slot.totals.take())
                .filter(|totals| {
                    generation.is_some()
                        && totals.binding == *binding
                        && totals.descriptor == descriptor
                        && totals.dag_revision == revision
                        && totals.prefixes.len().saturating_sub(1) <= history.len()
                })
                .unwrap_or_else(|| EntryTotals {
                    binding: binding.clone(),
                    descriptor,
                    dag_revision: revision,
                    prefixes: vec![0],
                    active: None,
                })
        };
        let start = totals.prefixes.len() - 1;
        let entries = self.load_range_paged(&view, start, history.len()).await?;
        for (index, entry) in entries.iter().enumerate() {
            if entry.content != history[start + index] {
                return Err(RuntimeError::conflict(
                    "LCM immutable entry no longer matches canonical history",
                ));
            }
            let next = totals
                .prefixes
                .last()
                .copied()
                .expect("prefix origin")
                .checked_add(u128::from(self.policy.sizer.entry_tokens(entry)))
                .ok_or_else(|| RuntimeError::conflict("LCM entry token count overflowed"))?;
            totals.prefixes.push(next);
        }
        let (covered, active) = match totals.active.filter(|(dag, _, _)| *dag == revision) {
            Some((_, covered, active)) => (covered, active),
            None => {
                let mut covered = 0usize;
                let mut active = 0u64;
                for node in &state.active_nodes {
                    if node.range.start.get() != covered as u64 {
                        return Err(RuntimeError::conflict(
                            "LCM active nodes do not cover one canonical prefix",
                        ));
                    }
                    covered = node
                        .range
                        .end
                        .get()
                        .checked_add(1)
                        .and_then(|end| usize::try_from(end).ok())
                        .ok_or_else(|| {
                            RuntimeError::conflict(
                                "LCM active node range exceeds the sequence space",
                            )
                        })?;
                    let measured = if node.sizer_revision == self.policy.sizer.revision() {
                        node.token_count
                    } else {
                        let stored = self
                            .store
                            .node(&view, &node.id)
                            .await
                            .map_err(map_lcm_error)?;
                        self.policy.sizer.summary_tokens(&stored.summary)
                    };
                    active = active.checked_add(measured).ok_or_else(|| {
                        RuntimeError::conflict("LCM active token count overflowed")
                    })?;
                }
                totals.active = Some((revision, covered, active));
                (covered, active)
            }
        };
        let prefix = totals.prefixes.get(covered).ok_or_else(|| {
            RuntimeError::conflict("LCM active node frontier exceeds canonical history")
        })?;
        let raw = totals
            .prefixes
            .last()
            .expect("prefix origin")
            .checked_sub(*prefix)
            .and_then(|tokens| u64::try_from(tokens).ok())
            .ok_or_else(|| RuntimeError::conflict("LCM raw suffix token count overflowed"))?;
        let result = active
            .checked_add(raw)
            .ok_or_else(|| RuntimeError::conflict("LCM context token count overflowed"))?;
        self.store.authorize_view(&view).map_err(map_lcm_error)?;
        if self
            .store
            .current_revision(&view)
            .await
            .map_err(map_lcm_error)?
            != revision
        {
            return Err(lcm_revision_conflict(
                "LCM store changed during pressure accounting",
            ));
        }
        if let Some(generation) = generation {
            let mut slots = self.accounting.lock().expect("LCM accounting poisoned");
            if let Some(slot) = slots.get_mut(&binding.session) {
                if slot
                    .generation
                    .upgrade()
                    .is_some_and(|current| Arc::ptr_eq(&current, &generation))
                {
                    slot.totals = Some(totals);
                }
            }
        }
        self.conversation_tokens
            .lock()
            .expect("LCM conversation counts poisoned")
            .insert(binding.session.clone(), result);
        Ok(result)
    }
}

pub(super) type Accounting = Arc<Mutex<BTreeMap<SessionId, AccountingSlot>>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::lcm::tests::{TestStore, test_coordinator};
    use crate::runtime::history::HistoryGenerations;

    #[tokio::test]
    async fn truncated_store_tail_discards_removed_entry_totals() {
        let mut coordinator =
            test_coordinator(Arc::new(TestStore::new(LcmTimelineId::new("lcm-timeline"))));
        let store = Arc::new(agent_runtime_lcm::testing::InMemoryLcmStore::new(
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
        let mut generations = HistoryGenerations::default();
        let mut history = vec![
            Message::user("retained"),
            Message::user("orphan".repeat(100)),
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
        assert!(
            coordinator.accounting.lock().unwrap()[&binding.session]
                .totals
                .is_some()
        );

        // Recover only the terminal prefix first. No append has yet occurred
        // to force a revision-mismatch lookup of the orphan's cached totals.
        coordinator
            .reconcile_diverged_store(&binding, &history[..1], 1, None)
            .await
            .unwrap();
        assert!(
            coordinator.accounting.lock().unwrap()[&binding.session]
                .totals
                .is_none()
        );

        history[1] = Message::user("new canonical tail");
        let generation = generations.capture(&history);
        coordinator.register_history(&binding.session, &generation);
        assert!(
            coordinator
                .try_append_range(&binding, &generation.history, 1)
                .await
                .unwrap()
                .is_ok()
        );
        let state = coordinator
            .checkpoint_state(&binding, &generation.history, &[], None, None, 0)
            .await
            .unwrap();
        let expected = history
            .iter()
            .enumerate()
            .map(|(index, message)| {
                let entry = coordinator
                    .entry_for(&binding, index as u64, message)
                    .unwrap();
                coordinator.policy.sizer.entry_tokens(&entry)
            })
            .sum::<u64>();
        for _ in 0..2 {
            assert_eq!(
                coordinator
                    .accounted_context_tokens(&binding, &generation.history, &state)
                    .await
                    .unwrap(),
                expected
            );
        }
    }
}
