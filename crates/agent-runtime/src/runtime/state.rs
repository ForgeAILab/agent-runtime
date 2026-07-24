//! Mutable per-session state guarded behind the session handle.

use agent_runtime_core::content::Message;
use agent_runtime_core::store::TurnManifest;
use agent_runtime_core::usage::UsageLedger;

/// The canonical mutable state of one session.
#[derive(Debug, Default)]
pub struct SessionState {
    /// The canonical conversation history.
    pub history: Vec<Message>,
    /// The accumulated usage ledger.
    pub usage: UsageLedger,
    /// The run manifest recorded for each completed turn, in turn order.
    pub manifests: Vec<TurnManifest>,
}

impl SessionState {
    /// A state seeded with an initial history.
    pub fn with_history(history: Vec<Message>) -> Self {
        Self {
            history,
            usage: UsageLedger::new(),
            manifests: Vec::new(),
        }
    }
}
