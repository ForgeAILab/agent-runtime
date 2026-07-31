//! Activation epochs: the frozen activation set for one provider request or
//! execution phase.
//!
//! Capability activation is resolved once and then held fixed while a
//! provider request (and normally a whole execution phase) is in flight.
//! Adding a capability — because the on-demand discovery fallback found one —
//! never mutates that frozen set: it produces a new [`ActivationEpoch`] with
//! its own index and fingerprint. Anything already holding a reference to an
//! earlier epoch keeps seeing exactly what it started with, which is what
//! protects a request already under construction from a plan that changes out
//! from under it mid-flight.

use agent_runtime_registry::{Fingerprint, FingerprintHasher, RegistryId, RegistryRevision};

/// One frozen activation set: the ordered `(id, revision)` pairs active for
/// one provider request or execution phase, plus a fingerprint over them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationEpoch {
    index: u64,
    activated: Vec<(RegistryId, RegistryRevision)>,
    fingerprint: Fingerprint,
}

impl ActivationEpoch {
    fn build(index: u64, mut activated: Vec<(RegistryId, RegistryRevision)>) -> Self {
        activated.sort_by(|a, b| a.0.cmp(&b.0));
        activated.dedup_by(|a, b| a.0 == b.0);

        let mut hasher = FingerprintHasher::new();
        hasher.pair("kind", "activation_epoch");
        hasher.pair("index", index.to_string());
        for (id, revision) in &activated {
            id.fingerprint_into(&mut hasher);
            hasher.pair("revision", revision.as_str());
        }

        Self {
            index,
            fingerprint: hasher.finish(),
            activated,
        }
    }

    /// This epoch's position in its run's activation history, starting at 0.
    pub fn index(&self) -> u64 {
        self.index
    }

    /// The ids and content revisions active in this epoch, in canonical
    /// `(domain, name)` order.
    pub fn activated(&self) -> &[(RegistryId, RegistryRevision)] {
        &self.activated
    }

    /// This epoch's fingerprint, derived from its index and its ordered
    /// activation set.
    pub fn fingerprint(&self) -> &Fingerprint {
        &self.fingerprint
    }

    /// Whether `id` is active in this epoch.
    pub fn contains(&self, id: &RegistryId) -> bool {
        self.activated.iter().any(|(active, _)| active == id)
    }
}

/// The ordered activation-epoch history for one run.
///
/// [`ActivationEpochs::advance`] is the only way to add a capability: it
/// always builds a brand new [`ActivationEpoch`] on top of the current one
/// and appends it, and never edits an epoch already handed out.
#[derive(Debug, Clone, Default)]
pub struct ActivationEpochs {
    history: Vec<ActivationEpoch>,
}

impl ActivationEpochs {
    /// An empty history: no capability has been activated yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The current (most recently created) epoch, if any.
    pub fn current(&self) -> Option<&ActivationEpoch> {
        self.history.last()
    }

    /// Every epoch created so far, oldest first.
    pub fn history(&self) -> &[ActivationEpoch] {
        &self.history
    }

    /// Creates and appends a new epoch containing everything the current
    /// epoch already held plus `additions`. The current epoch (and every
    /// earlier one) is left untouched; callers holding a reference or clone
    /// of it see no change.
    pub fn advance(
        &mut self,
        additions: impl IntoIterator<Item = (RegistryId, RegistryRevision)>,
    ) -> &ActivationEpoch {
        let mut activated: Vec<(RegistryId, RegistryRevision)> = self
            .current()
            .map(|epoch| epoch.activated().to_vec())
            .unwrap_or_default();
        activated.extend(additions);
        let index = self.history.len() as u64;
        self.history.push(ActivationEpoch::build(index, activated));
        self.history.last().expect("an epoch was just pushed")
    }

    /// Restores an exact persisted epoch history after validating that every
    /// epoch is the canonical, monotonic successor of the preceding one.
    pub(crate) fn restore(
        history: Vec<Vec<(RegistryId, RegistryRevision)>>,
    ) -> Result<Self, String> {
        if history.is_empty() {
            return Err("activation state contains no epoch".into());
        }
        let mut restored = Self::new();
        for (index, persisted) in history.into_iter().enumerate() {
            let mut canonical = persisted.clone();
            canonical.sort_by(|left, right| left.0.cmp(&right.0));
            canonical.dedup_by(|left, right| left.0 == right.0);
            if canonical != persisted {
                return Err(format!(
                    "activation epoch {index} is not canonically ordered or contains duplicate ids"
                ));
            }
            if let Some(previous) = restored.current() {
                for (id, revision) in previous.activated() {
                    if !canonical
                        .iter()
                        .any(|(next_id, next_revision)| next_id == id && next_revision == revision)
                    {
                        return Err(format!(
                            "activation epoch {index} regresses or changes `{id}`"
                        ));
                    }
                }
            }
            restored
                .history
                .push(ActivationEpoch::build(index as u64, canonical));
        }
        Ok(restored)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(name: &str) -> (RegistryId, RegistryRevision) {
        (RegistryId::tool(name), RegistryRevision::new("1"))
    }

    #[test]
    fn advancing_creates_a_new_epoch_with_an_incrementing_index() {
        let mut epochs = ActivationEpochs::new();
        epochs.advance([pair("search")]);
        epochs.advance([pair("browser")]);

        assert_eq!(epochs.history().len(), 2);
        assert_eq!(epochs.history()[0].index(), 0);
        assert_eq!(epochs.history()[1].index(), 1);
    }

    #[test]
    fn a_later_epoch_carries_forward_everything_the_current_one_held() {
        let mut epochs = ActivationEpochs::new();
        epochs.advance([pair("search")]);
        epochs.advance([pair("browser")]);

        let current = epochs.current().unwrap();
        assert!(current.contains(&RegistryId::tool("search")));
        assert!(current.contains(&RegistryId::tool("browser")));
    }

    /// Spec scenario: "Initial routing misses a needed browser" — selecting
    /// the browser produces a new recorded epoch rather than mutating the
    /// in-flight one.
    #[test]
    fn an_in_flight_epoch_is_unchanged_by_a_later_one() {
        let mut epochs = ActivationEpochs::new();
        epochs.advance([pair("search")]);
        let in_flight = epochs.current().cloned().expect("an epoch exists");

        epochs.advance([pair("browser")]);

        assert!(!in_flight.contains(&RegistryId::tool("browser")));
        assert_eq!(in_flight, epochs.history()[0]);
        assert_ne!(
            in_flight.fingerprint(),
            epochs.current().unwrap().fingerprint()
        );
    }

    #[test]
    fn duplicate_additions_do_not_produce_duplicate_entries() {
        let mut epochs = ActivationEpochs::new();
        epochs.advance([pair("search")]);
        epochs.advance([pair("search")]);

        assert_eq!(epochs.current().unwrap().activated().len(), 1);
    }

    #[test]
    fn identical_activation_sets_at_different_indices_fingerprint_differently() {
        let mut a = ActivationEpochs::new();
        a.advance([pair("search")]);
        let mut b = ActivationEpochs::new();
        b.advance([pair("browser")]);
        b.advance([pair("search")]);

        assert_ne!(
            a.current().unwrap().fingerprint(),
            b.history()[0].fingerprint()
        );
    }
}
