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
    fn project(&self, snapshot: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
        Ok(snapshot)
    }
}
