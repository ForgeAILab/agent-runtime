//! Run-manifest replay conformance: the invariants that make a
//! [`RunManifest`] a reproducibility contract rather than just a log.
//!
//! A manifest is only useful for replay if its fingerprint is a pure function
//! of what it records — the same inputs must always fingerprint identically,
//! or two runs that depended on exactly the same revisions could not be
//! recognized as equivalent. And [`RunManifest::check_replay`] must never
//! substitute: a required revision that is missing, or present at a
//! different value, fails equivalent replay explicitly, naming the id and
//! (when found) what was actually installed. The only way past a mismatch is
//! an explicit, labeled opt-in via [`ReplayMode::LabeledNonEquivalent`] —
//! [`RunManifest::check_replay_as`] still reports every mismatch even then,
//! so a host that chooses to proceed anyway does so with full knowledge of
//! what changed, never silently.

use std::collections::BTreeMap;

use agent_runtime::core::manifest::{
    ActivatedCapability, CapabilityResolution, ModelResolution, ReplayMode, RunManifest,
};
use agent_runtime::core::provider::ModelId;
use agent_runtime::registry::{Fingerprint, RegistryId, RegistryRevision};

/// Builds a manifest whose only required-revision dependency is `id` at
/// `revision`, for suite fixtures.
pub fn conformance_manifest_requiring(id: RegistryId, revision: RegistryRevision) -> RunManifest {
    RunManifest::new(
        Fingerprint::of("conformance-snapshot"),
        Fingerprint::of("conformance-view"),
        ModelResolution::new(
            "conformance",
            ModelId::new("conformance-model"),
            Fingerprint::of("conformance-profile"),
            BTreeMap::new(),
        ),
        CapabilityResolution::new(RegistryRevision::new("conformance-resolver-1")),
        Fingerprint::of("conformance-context"),
        Fingerprint::of("conformance-cache"),
    )
    .with_activation(vec![ActivatedCapability::new(id, revision)])
}

/// Asserts building the same manifest inputs twice (via `build`) reproduces
/// an identical fingerprint.
pub fn assert_identical_inputs_reproduce_identical_fingerprints(build: impl Fn() -> RunManifest) {
    let a = build();
    let b = build();
    assert_eq!(
        a.fingerprint(),
        b.fingerprint(),
        "identical manifest inputs must reproduce an identical fingerprint"
    );
}

/// Asserts replay succeeds when every revision the manifest requires is
/// present in `available` at exactly the recorded value — the positive
/// control the failure assertions below are contrasted against.
pub fn assert_equivalent_replay_succeeds_when_every_revision_matches(
    manifest: &RunManifest,
    available: &BTreeMap<RegistryId, RegistryRevision>,
) {
    assert!(
        manifest.check_replay(available).is_ok(),
        "replay must succeed when every required revision is present and matches"
    );
}

/// Asserts replay fails explicitly, naming at least one mismatch, when a
/// revision the manifest requires is entirely absent.
pub fn assert_missing_required_revision_fails_equivalent_replay(manifest: &RunManifest) {
    let empty = BTreeMap::new();
    let err = manifest.check_replay(&empty).expect_err(
        "a manifest with any required revision must fail replay against an empty environment",
    );
    assert!(
        !err.mismatches.is_empty(),
        "the failure must name at least one missing revision"
    );
    assert!(
        err.mismatches.iter().all(|m| m.found.is_none()),
        "an absent revision must be reported as absent, never as some substituted value"
    );
}

/// Asserts replay fails explicitly, naming the id and the revision actually
/// found, when a required revision changed rather than went missing.
pub fn assert_changed_required_revision_fails_equivalent_replay(
    manifest: &RunManifest,
    id: RegistryId,
    changed_revision: RegistryRevision,
) {
    let available = BTreeMap::from([(id.clone(), changed_revision.clone())]);
    let err = manifest
        .check_replay(&available)
        .expect_err("a changed required revision must fail equivalent replay");
    assert!(
        err.mismatches
            .iter()
            .any(|m| m.id == id && m.found.as_ref() == Some(&changed_revision)),
        "the mismatch must name the id and the revision actually found, never silently substitute"
    );
}

/// Asserts that only an explicit, labeled non-equivalent replay proceeds
/// past a mismatch — and that even then, the mismatch is still reported
/// rather than hidden.
pub fn assert_only_labeled_non_equivalent_replay_proceeds_past_a_mismatch(
    manifest: &RunManifest,
    available: &BTreeMap<RegistryId, RegistryRevision>,
) {
    assert!(
        manifest
            .check_replay_as(available, ReplayMode::Equivalent)
            .is_err(),
        "this assertion requires a manifest/environment pair that actually mismatches"
    );
    let reported = manifest
        .check_replay_as(available, ReplayMode::LabeledNonEquivalent)
        .expect("a labeled non-equivalent replay must proceed despite the mismatch");
    assert!(
        !reported.is_empty(),
        "a labeled non-equivalent replay must still report what mismatched, never hide it"
    );
}

/// Runs every replay assertion over a standard fixture: a manifest whose
/// only dependency is one skill activation at a fixed revision.
pub fn assert_replay_conformance() {
    let id = RegistryId::skill("conformance-research");
    let revision = RegistryRevision::new("v1");

    assert_identical_inputs_reproduce_identical_fingerprints(|| {
        conformance_manifest_requiring(id.clone(), revision.clone())
    });

    let manifest = conformance_manifest_requiring(id.clone(), revision.clone());
    let matching = BTreeMap::from([(id.clone(), revision.clone())]);
    assert_equivalent_replay_succeeds_when_every_revision_matches(&manifest, &matching);

    assert_missing_required_revision_fails_equivalent_replay(&manifest);

    let changed = RegistryRevision::new("v2");
    assert_changed_required_revision_fails_equivalent_replay(
        &manifest,
        id.clone(),
        changed.clone(),
    );

    let mismatched = BTreeMap::from([(id, changed)]);
    assert_only_labeled_non_equivalent_replay_proceeds_past_a_mismatch(&manifest, &mismatched);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_manifest_replay_satisfies_the_conformance_suite() {
        assert_replay_conformance();
    }

    #[test]
    fn a_manifest_with_no_required_revisions_at_all_still_replays_equivalently() {
        let manifest = RunManifest::new(
            Fingerprint::of("conformance-snapshot"),
            Fingerprint::of("conformance-view"),
            ModelResolution::new(
                "conformance",
                ModelId::new("conformance-model"),
                Fingerprint::of("conformance-profile"),
                BTreeMap::new(),
            ),
            CapabilityResolution::new(RegistryRevision::new("conformance-resolver-1")),
            Fingerprint::of("conformance-context"),
            Fingerprint::of("conformance-cache"),
        );
        let empty = BTreeMap::new();
        assert_equivalent_replay_succeeds_when_every_revision_matches(&manifest, &empty);
    }

    #[test]
    #[should_panic(
        expected = "this assertion requires a manifest/environment pair that actually mismatches"
    )]
    fn the_labeled_replay_assertion_refuses_to_run_without_a_real_mismatch() {
        let id = RegistryId::skill("conformance-research");
        let revision = RegistryRevision::new("v1");
        let manifest = conformance_manifest_requiring(id.clone(), revision.clone());
        let matching = BTreeMap::from([(id, revision)]);
        assert_only_labeled_non_equivalent_replay_proceeds_past_a_mismatch(&manifest, &matching);
    }
}
