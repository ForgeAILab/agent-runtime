//! Bounded diagnostic retention and its independent recovery frontier.

use std::num::NonZeroUsize;

use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::store::{
    SessionSnapshot, SessionStateSensitivity, TurnManifest, VersionedSessionState,
};
use agent_runtime_registry::RegistryRevision;
use serde::{Deserialize, Serialize};

/// Extension-state namespace for the runtime's manifest planned-step boundary.
///
/// Hosts that rewrite [`SessionSnapshot::extension_state`] must preserve this
/// record so bounded manifest lists can still be ordered across resume.
pub const MANIFEST_BOUNDARY_NAMESPACE: &str = "runtime.manifest_boundary";
const BOUNDARY_REVISION: &str = "manifest-boundary-1";
pub(crate) const DEFAULT_MANIFEST_WINDOW: NonZeroUsize = NonZeroUsize::new(32).unwrap();

#[derive(Serialize, Deserialize)]
struct ManifestBoundary {
    schema_version: u32,
    planned_steps: u64,
}

pub(crate) fn boundary_record(planned_steps: u64) -> VersionedSessionState {
    VersionedSessionState::new(
        RegistryRevision::new(BOUNDARY_REVISION),
        serde_json::json!({"schema_version": 1, "planned_steps": planned_steps}),
    )
    .redaction_safe()
}

/// Legacy lists must be counted in full, before any retention is applied.
pub(crate) fn planned_steps(
    record: Option<&VersionedSessionState>,
    manifests: &[TurnManifest],
) -> Result<u64, RuntimeError> {
    let legacy_count = u64::try_from(manifests.len())
        .map_err(|_| RuntimeError::conflict("legacy manifest count exceeds u64"))?;
    let Some(record) = record else {
        return Ok(legacy_count);
    };
    if record.revision.as_str() != BOUNDARY_REVISION
        || record.sensitivity != SessionStateSensitivity::RedactionSafe
    {
        return Err(RuntimeError::conflict(
            "manifest boundary has an incompatible revision or sensitivity",
        ));
    }
    let boundary: ManifestBoundary = serde_json::from_value(record.value.clone())
        .map_err(|_| RuntimeError::conflict("malformed manifest boundary evidence"))?;
    if boundary.schema_version != 1 {
        return Err(RuntimeError::conflict(
            "manifest boundary has an unsupported schema",
        ));
    }
    // A pre-U3 binary preserves this extension opaquely but can append more
    // manifests without incrementing it. Treat a longer stored list exactly
    // like legacy evidence without a record and repair the frontier on resume.
    Ok(boundary.planned_steps.max(legacy_count))
}

pub(crate) fn snapshot_planned_steps(snapshot: &SessionSnapshot) -> Result<u64, RuntimeError> {
    planned_steps(
        snapshot.extension_state.get(MANIFEST_BOUNDARY_NAMESPACE),
        &snapshot.manifests,
    )
}

pub(crate) fn validate_boundary_pair(
    ordinary: &SessionSnapshot,
    protected: &SessionSnapshot,
) -> Result<(), RuntimeError> {
    if snapshot_planned_steps(ordinary)? != snapshot_planned_steps(protected)?
        || (!ordinary
            .extension_state
            .contains_key(MANIFEST_BOUNDARY_NAMESPACE)
            && !protected
                .extension_state
                .contains_key(MANIFEST_BOUNDARY_NAMESPACE)
            && ordinary.manifests != protected.manifests)
    {
        return Err(RuntimeError::conflict(
            "canonical manifest boundary and checkpoint are from different planned-step boundaries; \
             full legacy evidence or a preserved boundary record is required",
        ));
    }
    Ok(())
}

pub(crate) fn initialize_boundary(snapshot: &mut SessionSnapshot) -> Result<(), RuntimeError> {
    let count = snapshot_planned_steps(snapshot)?;
    snapshot.extension_state.insert(
        MANIFEST_BOUNDARY_NAMESPACE.to_owned(),
        boundary_record(count),
    );
    Ok(())
}

pub(crate) fn trim(manifests: &mut Vec<TurnManifest>, window: NonZeroUsize) {
    let evicted = manifests.len().saturating_sub(window.get());
    manifests.drain(..evicted);
}

pub(crate) fn recent(manifests: &[TurnManifest], window: NonZeroUsize) -> Vec<TurnManifest> {
    manifests[manifests.len().saturating_sub(window.get())..].to_vec()
}

/// Normalize only runtime-produced successors/comparisons. Loaded records and
/// generic serde readers retain the original diagnostic payload.
pub(crate) fn normalize_checkpoint_snapshot(
    snapshot: &mut SessionSnapshot,
) -> Result<(), RuntimeError> {
    initialize_boundary(snapshot)?;
    snapshot.manifests.clear();
    Ok(())
}

/// Diagnostics never override the protected execution snapshot. A valid
/// ordinary snapshot at or behind the protected frontier may supply its older
/// retained suffix; the in-flight protected manifest itself is unavailable
/// from a manifest-free checkpoint.
pub(crate) fn carry_compatible_diagnostics(
    protected: &mut SessionSnapshot,
    ordinary: &SessionSnapshot,
    window: NonZeroUsize,
) -> Result<(), RuntimeError> {
    let protected_steps = snapshot_planned_steps(protected)?;
    let ordinary_steps = snapshot_planned_steps(ordinary)?;
    if ordinary_steps > protected_steps
        || (ordinary_steps == protected_steps
            && validate_boundary_pair(ordinary, protected).is_err())
    {
        return Ok(());
    }
    initialize_boundary(protected)?;
    protected.manifests = recent(&ordinary.manifests, window);
    Ok(())
}
