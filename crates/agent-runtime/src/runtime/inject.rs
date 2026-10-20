//! Safe-boundary content injection.
//!
//! Hosts enqueue content for an active session; the driver introduces it to
//! the model only at provider/tool boundaries, never by mutating an in-flight
//! provider stream. The queue is bounded for coalescable content; content
//! marked `must_deliver` (for example a final child result) is always
//! accepted and never dropped.
//!
//! The queue is process-local: content still queued when its session ends
//! (shutdown, drop or a process crash) is not persisted. A turn drains the
//! queue at its acceptance boundary; if that acceptance checkpoint's outcome
//! cannot be proven, the session is fenced and the drained batch is handed to
//! the host as [`UnresolvedInjections`] so a reopened session can restore it
//! exactly once (see `SessionHandle::restore_unresolved_injections`).

use agent_runtime_core::content::{ContentPart, Message, Role};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::{SessionId, TurnId};

/// Injected content drained for a turn whose acceptance checkpoint failed
/// with an outcome the protected store could not prove. The session that
/// owned it is fenced; restore it on the reopened session with
/// `SessionHandle::restore_unresolved_injections`. It is deliberately not
/// `Clone`: restoring consumes it, which keeps delivery exactly once.
#[derive(Debug, PartialEq)]
pub struct UnresolvedInjections {
    pub(crate) session: SessionId,
    pub(crate) turn: TurnId,
    pub(crate) items: Vec<InjectedContent>,
}

impl UnresolvedInjections {
    /// The session the batch was drained from.
    pub fn session(&self) -> &SessionId {
        &self.session
    }

    /// The turn whose acceptance outcome is unknown.
    pub fn turn(&self) -> &TurnId {
        &self.turn
    }

    /// The number of injected items in the batch.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the batch holds no items.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// What restoring an [`UnresolvedInjections`] batch did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectionRecovery {
    /// The reopened protected head is the batch's own turn: its acceptance
    /// committed, and the recovered history already holds the batch.
    AlreadyAccepted,
    /// The acceptance is absent from the reopened protected head: the batch
    /// went back to the front of the queue for the next turn.
    Requeued {
        /// The number of items requeued.
        items: usize,
    },
}

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
        Self::messages(&self.drain_items())
    }

    /// The user-role messages that drained `items` introduce to history.
    pub(crate) fn messages(items: &[InjectedContent]) -> Vec<Message> {
        items
            .iter()
            .map(|content| Message {
                role: Role::User,
                content: content.parts.clone(),
            })
            .collect()
    }

    /// Returns items drained for a turn whose acceptance checkpoint never
    /// committed to the front of the queue, ahead of anything enqueued since.
    /// They were admitted once, so the coalescable bound is not applied again
    /// and must-deliver content is never dropped.
    pub(crate) fn restore_front(&mut self, items: Vec<InjectedContent>) {
        if items.is_empty() {
            return;
        }
        let newer = std::mem::replace(&mut self.items, items);
        self.items.extend(newer);
    }

    /// Drains every queued item in its canonical drain order.
    pub(crate) fn drain_items(&mut self) -> Vec<InjectedContent> {
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
    fn restored_items_precede_newer_ones_and_bypass_the_bound() {
        let mut queue = InjectionQueue::new(1);
        queue.push(InjectedContent::text("drained")).unwrap();
        let drained = queue.drain_items();
        queue.push(InjectedContent::text("newer")).unwrap();
        queue.restore_front(drained);
        let texts = queue
            .drain_messages()
            .into_iter()
            .map(|message| message.joined_text())
            .collect::<Vec<_>>();
        assert_eq!(texts, ["drained", "newer"]);
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
