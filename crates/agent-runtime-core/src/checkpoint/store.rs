use super::*;

/// Host-provided protected storage for exact resumable turn state.
///
/// Implementations MUST make `save` idempotent by
/// `(session, turn, state_revision, operation_fingerprint)`, reject revisions
/// that move backwards, and apply confidentiality/retention policy suitable
/// for raw model and tool arguments.
#[async_trait]
pub trait CheckpointStore: Send + Sync + fmt::Debug {
    /// Loads the latest checkpoint for `session`, if one exists.
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError>;

    /// Atomically saves one validated checkpoint.
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError>;
}
