use super::*;

/// Host policy applied by an ordinary journal before it computes stored object
/// IDs and roots.
///
/// The input is the complete semantic ordinary candidate reconstructed from the
/// current projected root plus the typed transition delta. A policy may redact
/// message/extension values or omit extension namespaces, but must preserve the
/// session identity, ordered history/usage/manifest cardinalities, execution
/// counters and timestamp. Returning an error rejects the whole commit before
/// raw drafts become visible. Native structured deltas additionally require
/// manifests and the Runtime-owned redaction-safe
/// [`RUNTIME_PREVIOUS_CACHE_NAMESPACE`] value to remain exact; host-owned opaque
/// extensions retain their whole-value projection/replacement contract.
pub trait JournalProjectionPolicy: Send + Sync + 'static {
    /// Produces the host-policy view that the ordinary store may encode.
    fn project(&self, snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError>;
    /// Configuration revision for incremental equivalence. `None` always uses
    /// complete projection. Change the revision before changing policy behavior.
    fn revision(&self) -> Option<u64> {
        None
    }
    /// Opt-in promise of equivalence to `project`, including repeated projection
    /// of old positions. The base is the previously projected immutable view.
    fn project_delta(
        &self,
        _base: &JournalSnapshotView,
        _delta: &JournalTransitionDelta,
        _revision: u64,
    ) -> Result<JournalDeltaProjection, RuntimeError> {
        Ok(JournalDeltaProjection::FullProjectionRequired)
    }
}

impl<F> JournalProjectionPolicy for F
where
    F: Fn(SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> + Send + Sync + 'static,
{
    fn project(&self, snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
        self(snapshot)
    }
}

/// Explicit pass-through ordinary policy for already-sanitized values.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityJournalProjection;

impl JournalProjectionPolicy for IdentityJournalProjection {
    fn revision(&self) -> Option<u64> {
        Some(1)
    }
    fn project_delta(
        &self,
        _base: &JournalSnapshotView,
        delta: &JournalTransitionDelta,
        _revision: u64,
    ) -> Result<JournalDeltaProjection, RuntimeError> {
        Ok(JournalDeltaProjection::Projected(Box::new(delta.clone())))
    }
    fn project(&self, snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
        Ok(snapshot)
    }
}

/// A policy either projects only changed positions or requests complete replay.
#[derive(Debug, Clone)]
pub enum JournalDeltaProjection {
    /// The policy depends on the complete candidate or its configuration changed.
    FullProjectionRequired,
    /// A delta equivalent to applying `project` to the reconstructed candidate.
    Projected(Box<JournalTransitionDelta>),
}
