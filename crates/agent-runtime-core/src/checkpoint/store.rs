use super::*;

/// Host-provided protected storage for exact resumable turn state.
///
/// Implementations MUST make `save` idempotent by
/// `(session, turn, state_revision, operation_fingerprint)`, reject revisions
/// that move backwards, and apply confidentiality/retention policy suitable
/// for raw model and tool arguments.
#[async_trait]
pub trait CheckpointStore: Send + Sync + fmt::Debug {
    /// Discovers an exact protected native journal; legacy stores return `None`.
    fn journal(&self) -> Option<std::sync::Arc<dyn crate::journal::SessionJournal>> {
        None
    }

    /// Requests native protected persistence explicitly, failing with typed
    /// unsupported-capability evidence when discovery returns `None`.
    fn require_journal(
        &self,
    ) -> Result<std::sync::Arc<dyn crate::journal::SessionJournal>, RuntimeError> {
        self.journal().ok_or_else(|| {
            RuntimeError::unsupported_capability(
                crate::error::UnsupportedCapability::SessionJournal,
            )
        })
    }

    /// Loads the latest checkpoint for `session`, if one exists.
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError>;

    /// Atomically saves one validated checkpoint.
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError>;
}
