//! Replay, revision-mismatch, restart, and persistence-migration coverage
//! for task 8.5.
//!
//! The property under test throughout is that a run is reproducible *because
//! its manifest says what it depended on* — not because replaying happens to
//! produce the same bytes. So these tests assert on recorded revisions and
//! fingerprints, and on what happens when one of them no longer matches.

use std::collections::BTreeMap;
use std::sync::Arc;

use agent_runtime::core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime::core::manifest::{ReplayMode, RunManifest};
use agent_runtime::core::prelude::*;
use agent_runtime::core::store::SessionSnapshot;
use agent_runtime::provider::fake::FakeProvider;
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_registry::{RegistryId, RegistryRevision};

fn profile() -> ResolvedModelProfile {
    ResolvedModelProfile::explicit(
        "fake",
        ModelId::new("fake"),
        ModelLimits::new(128_000, 128_000, 4_096),
    )
}

async fn run_one_turn(prompt: &str, input: &str) -> SessionSnapshot {
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(Arc::new(FakeProvider::text_reply("done")))
        .model_profile(profile())
        .system_prompt(prompt)
        .build()
        .expect("runtime builds");
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("session starts");
    session.run(UserInput::text(input)).await;
    session.snapshot()
}

/// A completed turn records a manifest an operator can actually audit.
#[tokio::test]
async fn a_completed_turn_persists_an_auditable_run_manifest() {
    let snapshot = run_one_turn("be helpful", "hello").await;

    assert_eq!(
        snapshot.manifests.len(),
        1,
        "one provider request means one recorded manifest"
    );
    let manifest = &snapshot.manifests[0].manifest;
    assert_eq!(manifest.model.provider, "fake");
    assert_eq!(manifest.model.model, ModelId::new("fake"));
    assert!(!manifest.segments.is_empty());
    assert!(manifest.segments.iter().all(|s| s.tokens > 0));
}

/// Identical inputs replay to identical fingerprints. Without this, a manifest
/// records history rather than enabling reproduction.
#[tokio::test]
async fn an_equivalent_run_reproduces_the_same_context_fingerprint() {
    let first = run_one_turn("be helpful", "hello").await;
    let second = run_one_turn("be helpful", "hello").await;

    assert_eq!(
        first.manifests[0].manifest.context_fingerprint,
        second.manifests[0].manifest.context_fingerprint
    );
    assert_eq!(
        first.manifests[0].manifest.fingerprint(),
        second.manifests[0].manifest.fingerprint()
    );
}

/// A changed system prompt is a changed context, and the fingerprint must say
/// so — otherwise a cache plan or a replay could claim equivalence it does not
/// have.
#[tokio::test]
async fn a_changed_system_prompt_changes_the_recorded_context_fingerprint() {
    let first = run_one_turn("be helpful", "hello").await;
    let second = run_one_turn("be extremely terse", "hello").await;

    assert_ne!(
        first.manifests[0].manifest.context_fingerprint,
        second.manifests[0].manifest.context_fingerprint
    );
}

/// Replay against a different installed revision fails explicitly rather than
/// silently substituting what happens to be present.
#[test]
fn replay_fails_when_a_required_revision_changed() {
    let manifest = manifest_requiring(RegistryRevision::new("v1"));

    let installed = BTreeMap::from([(RegistryId::skill("research"), RegistryRevision::new("v2"))]);
    let mismatch = manifest
        .check_replay_as(&installed, ReplayMode::Equivalent)
        .expect_err("a changed revision must fail equivalent replay");
    assert!(
        format!("{mismatch:?}").contains("research"),
        "the mismatch must name which entry disagreed"
    );
}

/// A missing revision is just as fatal as a changed one.
#[test]
fn replay_fails_when_a_required_revision_is_absent() {
    let manifest = manifest_requiring(RegistryRevision::new("v1"));

    let installed = BTreeMap::new();
    assert!(
        manifest
            .check_replay_as(&installed, ReplayMode::Equivalent)
            .is_err(),
        "an absent revision must fail equivalent replay"
    );
}

/// A host may knowingly proceed, but only by asking for a labeled
/// non-equivalent replay — never by default.
#[test]
fn a_labeled_non_equivalent_replay_is_the_only_way_past_a_mismatch() {
    let manifest = manifest_requiring(RegistryRevision::new("v1"));
    let installed = BTreeMap::from([(RegistryId::skill("research"), RegistryRevision::new("v2"))]);

    assert!(
        manifest
            .check_replay_as(&installed, ReplayMode::Equivalent)
            .is_err()
    );
    assert!(
        manifest
            .check_replay_as(&installed, ReplayMode::LabeledNonEquivalent)
            .is_ok(),
        "an explicit opt-in may proceed despite the mismatch"
    );
}

/// A snapshot written before manifests existed must still load — persistence
/// migration, not a breaking read.
#[test]
fn a_snapshot_persisted_without_manifests_still_loads() {
    let legacy = serde_json::json!({
        "id": "session-legacy",
        "history": [],
        "updated": 0
    });

    let snapshot: SessionSnapshot =
        serde_json::from_value(legacy).expect("a pre-manifest snapshot must still deserialize");
    assert!(snapshot.manifests.is_empty());
}

/// A snapshot round-trips with its manifests intact, so a restarted host
/// resumes with the same audit trail it persisted.
#[tokio::test]
async fn manifests_survive_a_snapshot_round_trip() {
    let snapshot = run_one_turn("be helpful", "hello").await;

    let json = serde_json::to_string(&snapshot).expect("serialize");
    let restored: SessionSnapshot = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(restored.manifests.len(), snapshot.manifests.len());
    assert_eq!(
        restored.manifests[0].manifest.fingerprint(),
        snapshot.manifests[0].manifest.fingerprint()
    );
}

/// Builds a manifest whose activation depends on one skill at `revision`.
fn manifest_requiring(revision: RegistryRevision) -> RunManifest {
    use agent_runtime::core::manifest::{
        ActivatedCapability, CapabilityResolution, ModelResolution,
    };
    use agent_runtime_registry::Fingerprint;

    RunManifest::new(
        Fingerprint::of_fields(["snapshot"]),
        Fingerprint::of_fields(["view"]),
        ModelResolution::new(
            "fake",
            ModelId::new("fake"),
            Fingerprint::of_fields(["profile"]),
            BTreeMap::new(),
        ),
        CapabilityResolution::new(RegistryRevision::new("deterministic-1")),
        Fingerprint::of_fields(["context"]),
        Fingerprint::of_fields(["cache"]),
    )
    .with_activation(vec![ActivatedCapability::new(
        RegistryId::skill("research"),
        revision,
    )])
}
