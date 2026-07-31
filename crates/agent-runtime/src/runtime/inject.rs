//! Safe-boundary content injection.
//!
//! Hosts enqueue content for an active session; the driver introduces it to
//! the model only at provider/tool boundaries, never by mutating an in-flight
//! provider stream. The queue is bounded for coalescable content; content
//! marked `must_deliver` (for example a final child result) is always
//! accepted and never dropped.

use agent_runtime_core::content::{ContentPart, Message, Role};
use agent_runtime_core::error::RuntimeError;

/// Host-enqueued content introduced at the next safe boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct InjectedContent {
    /// The content parts, introduced as one user-role message.
    pub parts: Vec<ContentPart>,
    /// Whether this content must survive queue bounds and coalescing (for
    /// example a final child result). Coalescable content is rejected with a
    /// structured overflow result once the queue bound is reached;
    /// `must_deliver` content is always accepted.
    pub must_deliver: bool,
}

impl InjectedContent {
    /// Coalescable text content.
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            parts: vec![ContentPart::text(text)],
            must_deliver: false,
        }
    }

    /// Marks this content as must-deliver.
    pub fn must_deliver(mut self) -> Self {
        self.must_deliver = true;
        self
    }
}

/// The bounded per-session injection queue. Public only because it appears in
/// [`crate::agent::driver::Driver::run_turn`]'s signature; hosts interact with
/// it exclusively through
/// [`SessionHandle::inject`](crate::runtime::SessionHandle::inject).
#[derive(Debug)]
pub struct InjectionQueue {
    items: Vec<InjectedContent>,
    limit: usize,
}

impl InjectionQueue {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            items: Vec::new(),
            limit: limit.max(1),
        }
    }

    /// Enqueues content, returning a structured overflow error when the bound
    /// is reached for coalescable content. Must-deliver content is always
    /// accepted.
    pub(crate) fn push(&mut self, content: InjectedContent) -> Result<(), RuntimeError> {
        if !content.must_deliver {
            let coalescable = self.items.iter().filter(|i| !i.must_deliver).count();
            if coalescable >= self.limit {
                return Err(RuntimeError::new(
                    agent_runtime_core::error::ErrorKind::Limit,
                    format!(
                        "injection queue is at its bound of {} coalescable items",
                        self.limit
                    ),
                ));
            }
        }
        self.items.push(content);
        Ok(())
    }

    /// Drains every queued item into user-role messages, in enqueue order.
    pub(crate) fn drain_messages(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.items)
            .into_iter()
            .map(|content| Message {
                role: Role::User,
                content: content.parts,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bound_applies_to_coalescable_content_only() {
        let mut queue = InjectionQueue::new(2);
        queue.push(InjectedContent::text("a")).unwrap();
        queue.push(InjectedContent::text("b")).unwrap();
        assert!(queue.push(InjectedContent::text("c")).is_err());
        // A final result is still delivered past the bound.
        queue
            .push(InjectedContent::text("final").must_deliver())
            .unwrap();
        let drained = queue.drain_messages();
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[2].joined_text(), "final");
    }

    #[test]
    fn drain_preserves_order_and_empties_the_queue() {
        let mut queue = InjectionQueue::new(8);
        queue.push(InjectedContent::text("first")).unwrap();
        queue.push(InjectedContent::text("second")).unwrap();
        let drained = queue.drain_messages();
        assert_eq!(drained[0].joined_text(), "first");
        assert_eq!(drained[1].joined_text(), "second");
        assert!(queue.drain_messages().is_empty());
    }
}
