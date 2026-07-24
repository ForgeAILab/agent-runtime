//! Cancellation with an attached reason.
//!
//! Wraps [`tokio_util::sync::CancellationToken`] so cancellation propagates
//! from a session to its turns, provider attempts, and tool invocations through
//! child tokens, while additionally carrying a [`CancelReason`]. A child
//! resolves its reason from itself first, then walks up to its parent, so code
//! holding a derived token can still report why the session was cancelled.

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

/// Why a session or turn was cancelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelReason {
    /// The host explicitly requested cancellation.
    UserRequested,
    /// A deadline elapsed.
    Timeout,
    /// A configured limit was reached.
    LimitReached,
    /// The runtime is shutting down.
    Shutdown,
    /// A host-defined reason.
    Host(String),
}

/// A cancellation handle that carries a reason.
#[derive(Clone)]
pub struct Cancellation {
    token: CancellationToken,
    reason: Arc<Mutex<Option<CancelReason>>>,
    parent: Option<Arc<Cancellation>>,
}

impl std::fmt::Debug for Cancellation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cancellation")
            .field("cancelled", &self.is_cancelled())
            .field("reason", &self.reason())
            .finish()
    }
}

impl Default for Cancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl Cancellation {
    /// Creates a fresh, uncancelled root handle.
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            reason: Arc::new(Mutex::new(None)),
            parent: None,
        }
    }

    /// Derives a child handle. Cancelling the parent cancels every child; the
    /// child may also be cancelled independently for its own reason.
    pub fn child(&self) -> Cancellation {
        Cancellation {
            token: self.token.child_token(),
            reason: Arc::new(Mutex::new(None)),
            parent: Some(Arc::new(self.clone())),
        }
    }

    /// Cancels this handle (and its children) with the given reason.
    pub fn cancel(&self, reason: CancelReason) {
        {
            let mut slot = self.reason.lock().expect("cancellation reason poisoned");
            if slot.is_none() {
                *slot = Some(reason);
            }
        }
        self.token.cancel();
    }

    /// Whether this handle or any ancestor has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// The reason for cancellation, resolved from this handle then its ancestors.
    pub fn reason(&self) -> Option<CancelReason> {
        if let Some(reason) = self
            .reason
            .lock()
            .expect("cancellation reason poisoned")
            .clone()
        {
            return Some(reason);
        }
        self.parent.as_ref().and_then(|parent| parent.reason())
    }

    /// Resolves once this handle (or an ancestor) is cancelled.
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parent_cancellation_propagates_to_children() {
        let root = Cancellation::new();
        let child = root.child();
        assert!(!child.is_cancelled());

        root.cancel(CancelReason::UserRequested);
        // The child observes cancellation and resolves the parent's reason.
        child.cancelled().await;
        assert!(child.is_cancelled());
        assert_eq!(child.reason(), Some(CancelReason::UserRequested));
    }

    #[test]
    fn child_reason_wins_over_parent() {
        let root = Cancellation::new();
        let child = root.child();
        child.cancel(CancelReason::Timeout);
        assert_eq!(child.reason(), Some(CancelReason::Timeout));
        // The root itself is not cancelled by a child.
        assert!(!root.is_cancelled());
    }
}
