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
    /// Optional canonical ordering key for concurrent peer notifications.
    ///
    /// At drain time, keyed items are ordered lexicographically among their
    /// keyed slots while unkeyed items retain their exact FIFO positions.
    order_key: Option<String>,
}

impl InjectedContent {
    /// Coalescable text content.
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            parts: vec![ContentPart::text(text)],
            must_deliver: false,
            order_key: None,
        }
    }

    /// Marks this content as must-deliver.
    pub fn must_deliver(mut self) -> Self {
        self.must_deliver = true;
        self
    }

    /// Orders this item canonically relative to other keyed items drained at
    /// the same safe boundary. Intended for stable identities such as
    /// `(child_id, interaction_request_id)`.
    pub fn ordered_by(mut self, key: impl Into<String>) -> Self {
        self.order_key = Some(key.into());
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

    /// Drains every queued item into user-role messages. Unkeyed slots remain
    /// FIFO; keyed peers are canonicalized within their existing slots.
    pub(crate) fn drain_messages(&mut self) -> Vec<Message> {
        let mut items = std::mem::take(&mut self.items);
        let mut keyed = items
            .iter()
            .filter(|content| content.order_key.is_some())
            .cloned()
            .collect::<Vec<_>>();
        keyed.sort_by(|left, right| left.order_key.cmp(&right.order_key));
        let mut keyed = keyed.into_iter();
        for item in &mut items {
            if item.order_key.is_some() {
                *item = keyed
                    .next()
                    .expect("every keyed injection slot has one sorted item");
            }
        }
        items
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

    #[test]
    fn keyed_peers_are_canonical_without_reordering_fifo_slots() {
        let mut queue = InjectionQueue::new(8);
        queue
            .push(InjectedContent::text("child-2").ordered_by("child-2/request-1"))
            .unwrap();
        queue.push(InjectedContent::text("ordinary")).unwrap();
        queue
            .push(InjectedContent::text("child-1").ordered_by("child-1/request-9"))
            .unwrap();

        let drained = queue
            .drain_messages()
            .into_iter()
            .map(|message| message.joined_text())
            .collect::<Vec<_>>();
        assert_eq!(drained, ["child-1", "ordinary", "child-2"]);
    }
}
