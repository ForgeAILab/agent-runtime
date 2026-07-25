//! Side-effect-aware scheduling.
//!
//! Greenfield (the donor had no declarative scheduling): given the effects of
//! the tool calls in one model turn, [`plan_batches`] groups them into ordered
//! batches. Calls within a batch may run concurrently; batches run in order.
//! Two calls whose declared write scopes overlap are placed in different
//! batches so they never run concurrently, and the request order is preserved,
//! keeping results deterministic. Network effects are serialized against each
//! other on their own dimension, independent of write-scope conflicts.

use agent_runtime_core::tool::ToolEffects;

/// How aggressively to serialize mutating calls.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// Serialize only calls whose declared write scopes overlap (default).
    #[default]
    ScopeOverlap,
    /// Serialize every mutating call into its own batch.
    SerializeAllWrites,
}

/// Groups call indices into ordered, internally-concurrent batches.
///
/// `effects[i]` is the declared effect set of the i-th call, in request order.
pub fn plan_batches(effects: &[ToolEffects], policy: ConflictPolicy) -> Vec<Vec<usize>> {
    let mut batches: Vec<Vec<usize>> = Vec::new();
    let mut current: Vec<usize> = Vec::new();
    let mut current_scopes: Vec<String> = Vec::new();
    let mut current_mutates = false;
    let mut current_spawns = false;
    let mut current_network = false;

    for (i, eff) in effects.iter().enumerate() {
        let conflicts = !current.is_empty()
            && conflicts_with(
                eff,
                &current_scopes,
                current_mutates,
                current_spawns,
                current_network,
                policy,
            );
        if conflicts {
            batches.push(std::mem::take(&mut current));
            current_scopes.clear();
            current_mutates = false;
            current_spawns = false;
            current_network = false;
        }
        current.push(i);
        current_mutates |= eff.mutates();
        current_spawns |= eff.spawns_process();
        current_network |= eff.has_network();
        current_scopes.extend(eff.write_scopes().map(|s| s.as_str().to_owned()));
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

fn conflicts_with(
    eff: &ToolEffects,
    batch_scopes: &[String],
    batch_mutates: bool,
    batch_spawns: bool,
    batch_network: bool,
    policy: ConflictPolicy,
) -> bool {
    match policy {
        ConflictPolicy::SerializeAllWrites => {
            (eff.mutates() && batch_mutates) || (eff.has_network() && batch_network)
        }
        ConflictPolicy::ScopeOverlap => {
            // Overlapping write scopes conflict.
            let scope_overlap = eff
                .write_scopes()
                .any(|s| batch_scopes.iter().any(|b| b == s.as_str()));
            // A process spawn is conservatively serialized against any other
            // mutating work, and vice versa, since its side effects are opaque.
            let spawn_conflict =
                (eff.spawns_process() && batch_mutates) || (batch_spawns && eff.mutates());
            // Effect::Network carries no resource/endpoint payload, so any two
            // network-declaring calls are the same contended resource by
            // construction; network conflicts stay their own dimension and do
            // not spill onto unrelated write scopes.
            let network_conflict = eff.has_network() && batch_network;
            scope_overlap || spawn_conflict || network_conflict
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_calls_share_one_batch() {
        let effects = vec![
            ToolEffects::read_only(),
            ToolEffects::read_only(),
            ToolEffects::read_only(),
        ];
        let batches = plan_batches(&effects, ConflictPolicy::ScopeOverlap);
        assert_eq!(batches, vec![vec![0, 1, 2]]);
    }

    #[test]
    fn overlapping_writes_are_serialized_in_order() {
        let effects = vec![
            ToolEffects::read_only().with_write("/w/a"),
            ToolEffects::read_only().with_write("/w/a"),
        ];
        let batches = plan_batches(&effects, ConflictPolicy::ScopeOverlap);
        // Two batches => not concurrent; order preserved.
        assert_eq!(batches, vec![vec![0], vec![1]]);
    }

    #[test]
    fn independent_writes_may_batch_together() {
        let effects = vec![
            ToolEffects::read_only().with_write("/w/a"),
            ToolEffects::read_only().with_write("/w/b"),
        ];
        let batches = plan_batches(&effects, ConflictPolicy::ScopeOverlap);
        assert_eq!(batches, vec![vec![0, 1]]);
    }

    #[test]
    fn serialize_all_writes_isolates_each_mutation() {
        let effects = vec![
            ToolEffects::read_only().with_write("/w/a"),
            ToolEffects::read_only().with_write("/w/b"),
        ];
        let batches = plan_batches(&effects, ConflictPolicy::SerializeAllWrites);
        assert_eq!(batches, vec![vec![0], vec![1]]);
    }

    #[test]
    fn two_network_calls_are_serialized_as_the_same_contended_resource() {
        // Effect::Network carries no resource/endpoint payload, so two
        // network-declaring calls are indistinguishable and must be treated
        // as the same resource by default.
        let effects = vec![
            ToolEffects::new(vec![]).with_network(),
            ToolEffects::new(vec![]).with_network(),
        ];
        let batches = plan_batches(&effects, ConflictPolicy::ScopeOverlap);
        assert_eq!(batches, vec![vec![0], vec![1]]);
    }

    #[test]
    fn network_call_and_unrelated_write_scope_may_batch() {
        let effects = vec![
            ToolEffects::new(vec![]).with_network(),
            ToolEffects::read_only().with_write("/w/a"),
        ];
        let batches = plan_batches(&effects, ConflictPolicy::ScopeOverlap);
        assert_eq!(batches, vec![vec![0, 1]]);
    }

    #[test]
    fn network_call_and_read_only_call_may_batch() {
        let effects = vec![
            ToolEffects::new(vec![]).with_network(),
            ToolEffects::read_only(),
        ];
        let batches = plan_batches(&effects, ConflictPolicy::ScopeOverlap);
        assert_eq!(batches, vec![vec![0, 1]]);
    }

    #[test]
    fn serialize_all_writes_also_isolates_each_network_call() {
        let effects = vec![
            ToolEffects::new(vec![]).with_network(),
            ToolEffects::new(vec![]).with_network(),
        ];
        let batches = plan_batches(&effects, ConflictPolicy::SerializeAllWrites);
        assert_eq!(batches, vec![vec![0], vec![1]]);
    }
}
