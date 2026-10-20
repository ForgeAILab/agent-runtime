//! Unpublished reference packed SQLite adapter and physical-disk harness.
//! Sealing models nonce/tag lengths; this is not an encrypted production store.
mod codec;
mod metadata;
mod packed;
use agent_runtime_core::error::{FailureClass, FailureComponent, FailureStage, RuntimeError};
pub use packed::{LOGICAL_PACK_CAP, PackedSqliteConfig, PackedSqliteJournalStorage};
fn conflict() -> RuntimeError {
    RuntimeError::conflict("packed journal conflict").with_class(FailureClass::StateConflict {
        stage: FailureStage::PreProvider,
        component: FailureComponent::Checkpoint,
    })
}
fn failure<T>(_: T) -> RuntimeError {
    RuntimeError::internal("packed journal storage failure").with_class(
        FailureClass::HostComponent {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint,
        },
    )
}
