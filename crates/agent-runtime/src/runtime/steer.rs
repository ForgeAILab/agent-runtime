//! Serving-turn steering mailbox.

use std::collections::VecDeque;
use std::sync::Mutex;

use agent_runtime_core::content::{ContentPart, UserInput};
use agent_runtime_core::ids::{SteerId, TurnId};
use agent_runtime_core::steer::{SteerLimits, SteerReceipt, SteerRejection, SteerRejectionReason};

/// One accepted input retained until a protected boundary commits or discards
/// it. The raw input never enters default lifecycle events.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SteerEntry {
    pub(crate) receipt: SteerReceipt,
    pub(crate) input: UserInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MailboxStatus {
    Open,
    Closed,
}

#[derive(Debug)]
struct MailboxState {
    status: MailboxStatus,
    pending: VecDeque<SteerEntry>,
    accepted_bytes: usize,
    next_ordinal: u64,
}

/// Result of the atomic ordinary-completion fence.
#[derive(Debug, PartialEq)]
pub(crate) enum DrainOrClose {
    /// Pending input won the fence; the mailbox remains open for the next
    /// provider pass.
    Pending(Vec<SteerEntry>),
    /// The mailbox was empty and is now permanently closed.
    Closed,
}

/// Bounded FIFO owned by exactly one eligible serving turn.
#[derive(Debug)]
pub(crate) struct SteerMailbox {
    turn: TurnId,
    limits: SteerLimits,
    state: Mutex<MailboxState>,
}

impl SteerMailbox {
    pub(crate) fn new(turn: TurnId, limits: SteerLimits) -> Self {
        debug_assert!(limits.validate().is_ok());
        Self {
            turn,
            limits,
            state: Mutex::new(MailboxState {
                status: MailboxStatus::Open,
                pending: VecDeque::new(),
                accepted_bytes: 0,
                next_ordinal: 1,
            }),
        }
    }

    pub(crate) fn is_open(&self) -> bool {
        self.state.lock().expect("steer mailbox poisoned").status == MailboxStatus::Open
    }

    /// Admits one input while holding the mailbox lifecycle fence. The id is
    /// minted only after all validation succeeds, so rejected calls create no
    /// receipt identity or disposition obligation.
    pub(crate) fn admit(
        &self,
        input: UserInput,
        mint: impl FnOnce() -> SteerId,
    ) -> Result<SteerReceipt, SteerRejection> {
        if !has_meaningful_content(&input) {
            return Err(SteerRejection::new(SteerRejectionReason::EmptyInput, input));
        }
        let bytes = serde_json::to_vec(&input)
            .map(|value| value.len())
            .unwrap_or(usize::MAX);
        if bytes > self.limits.max_input_bytes {
            return Err(SteerRejection::new(
                SteerRejectionReason::InputTooLarge {
                    limit_bytes: self.limits.max_input_bytes,
                },
                input,
            ));
        }

        let mut state = self.state.lock().expect("steer mailbox poisoned");
        if state.status == MailboxStatus::Closed {
            return Err(SteerRejection::new(
                SteerRejectionReason::TurnClosing {
                    turn: self.turn.clone(),
                },
                input,
            ));
        }
        if state.pending.len() >= self.limits.max_pending {
            return Err(SteerRejection::new(
                SteerRejectionReason::PendingLimit {
                    limit: self.limits.max_pending,
                },
                input,
            ));
        }
        if state.accepted_bytes.saturating_add(bytes) > self.limits.max_turn_bytes {
            return Err(SteerRejection::new(
                SteerRejectionReason::TurnByteLimit {
                    limit_bytes: self.limits.max_turn_bytes,
                },
                input,
            ));
        }

        let receipt = SteerReceipt {
            id: mint(),
            turn: self.turn.clone(),
            ordinal: state.next_ordinal,
        };
        state.next_ordinal = state.next_ordinal.saturating_add(1);
        state.accepted_bytes = state.accepted_bytes.saturating_add(bytes);
        state.pending.push_back(SteerEntry {
            receipt: receipt.clone(),
            input,
        });
        Ok(receipt)
    }

    /// Drains pending entries at a non-terminal safe boundary while retaining
    /// open admission for the same turn.
    pub(crate) fn drain_open(&self) -> Vec<SteerEntry> {
        let mut state = self.state.lock().expect("steer mailbox poisoned");
        if state.status == MailboxStatus::Closed {
            return Vec::new();
        }
        state.pending.drain(..).collect()
    }

    /// Atomically drains pending input or closes an empty mailbox. This is the
    /// ordinary-completion race fence.
    pub(crate) fn drain_or_close(&self) -> DrainOrClose {
        let mut state = self.state.lock().expect("steer mailbox poisoned");
        if state.status == MailboxStatus::Closed {
            return DrainOrClose::Closed;
        }
        if state.pending.is_empty() {
            state.status = MailboxStatus::Closed;
            DrainOrClose::Closed
        } else {
            DrainOrClose::Pending(state.pending.drain(..).collect())
        }
    }

    /// Closes admission and takes every accepted-but-uncommitted entry.
    pub(crate) fn close_and_drain(&self) -> Vec<SteerEntry> {
        let mut state = self.state.lock().expect("steer mailbox poisoned");
        state.status = MailboxStatus::Closed;
        state.pending.drain(..).collect()
    }

    #[cfg(test)]
    fn is_closed(&self) -> bool {
        !self.is_open()
    }
}

fn has_meaningful_content(input: &UserInput) -> bool {
    input.parts.iter().any(|part| match part {
        ContentPart::Text { text } => !text.trim().is_empty(),
        ContentPart::Reasoning { text, .. } => !text.trim().is_empty(),
        ContentPart::Image { url, .. } => !url.trim().is_empty(),
        ContentPart::ToolCall(_) | ContentPart::ToolResult(_) => true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    fn mailbox(limits: SteerLimits) -> SteerMailbox {
        SteerMailbox::new(TurnId::new("turn-1"), limits)
    }

    #[test]
    fn fifo_ordinals_and_atomic_close_are_stable() {
        let mailbox = mailbox(SteerLimits::default());
        let first = mailbox
            .admit(UserInput::text("first"), || SteerId::new("steer-1"))
            .unwrap();
        let second = mailbox
            .admit(UserInput::text("second"), || SteerId::new("steer-2"))
            .unwrap();
        assert_eq!((first.ordinal, second.ordinal), (1, 2));

        let DrainOrClose::Pending(entries) = mailbox.drain_or_close() else {
            panic!("pending input must win the first terminal fence");
        };
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.input.clone().into_message().joined_text())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert_eq!(mailbox.drain_or_close(), DrainOrClose::Closed);
        assert!(mailbox.is_closed());
    }

    #[test]
    fn pending_and_cumulative_bounds_preserve_owned_input() {
        let sample_bytes = serde_json::to_vec(&UserInput::text("one")).unwrap().len();
        let mailbox = mailbox(SteerLimits {
            max_input_bytes: sample_bytes + 16,
            max_pending: 1,
            max_turn_bytes: sample_bytes * 2,
        });
        mailbox
            .admit(UserInput::text("one"), || SteerId::new("steer-1"))
            .unwrap();
        let rejected = mailbox
            .admit(UserInput::text("two"), || SteerId::new("steer-2"))
            .unwrap_err();
        assert!(matches!(
            rejected.reason,
            SteerRejectionReason::PendingLimit { limit: 1 }
        ));
        assert_eq!(rejected.input, UserInput::text("two"));

        mailbox.drain_open();
        mailbox
            .admit(UserInput::text("two"), || SteerId::new("steer-3"))
            .unwrap();
        mailbox.drain_open();
        let rejected = mailbox
            .admit(UserInput::text("x"), || SteerId::new("steer-4"))
            .unwrap_err();
        assert!(matches!(
            rejected.reason,
            SteerRejectionReason::TurnByteLimit { .. }
        ));
    }

    #[test]
    fn cancellation_close_rejects_late_admission() {
        let mailbox = mailbox(SteerLimits::default());
        mailbox
            .admit(UserInput::text("pending"), || SteerId::new("steer-1"))
            .unwrap();
        let discarded = mailbox.close_and_drain();
        assert_eq!(discarded.len(), 1);
        let rejected = mailbox
            .admit(UserInput::text("late"), || SteerId::new("steer-2"))
            .unwrap_err();
        assert!(matches!(
            rejected.reason,
            SteerRejectionReason::TurnClosing { .. }
        ));
    }

    #[test]
    fn admission_and_terminal_close_have_no_lost_outcome() {
        for iteration in 0..128 {
            let mailbox = Arc::new(mailbox(SteerLimits::default()));
            let start = Arc::new(Barrier::new(3));
            let admit_mailbox = mailbox.clone();
            let admit_start = start.clone();
            let admit = std::thread::spawn(move || {
                admit_start.wait();
                admit_mailbox.admit(UserInput::text("race"), || {
                    SteerId::new(format!("steer-{iteration}"))
                })
            });
            let close_mailbox = mailbox.clone();
            let close_start = start.clone();
            let close = std::thread::spawn(move || {
                close_start.wait();
                close_mailbox.drain_or_close()
            });
            start.wait();

            let admitted = admit.join().unwrap();
            let closed = close.join().unwrap();
            match (admitted, closed) {
                (Ok(receipt), DrainOrClose::Pending(entries)) => {
                    assert_eq!(entries.len(), 1);
                    assert_eq!(entries[0].receipt, receipt);
                }
                (Err(rejection), DrainOrClose::Closed) => assert!(matches!(
                    rejection.reason,
                    SteerRejectionReason::TurnClosing { .. }
                )),
                other => panic!("atomic fence produced an impossible outcome: {other:?}"),
            }
        }
    }

    #[test]
    fn admission_and_cancellation_close_accept_then_discard_or_reject() {
        for iteration in 0..128 {
            let mailbox = Arc::new(mailbox(SteerLimits::default()));
            let start = Arc::new(Barrier::new(3));
            let admit_mailbox = mailbox.clone();
            let admit_start = start.clone();
            let admit = std::thread::spawn(move || {
                admit_start.wait();
                admit_mailbox.admit(UserInput::text("race"), || {
                    SteerId::new(format!("steer-{iteration}"))
                })
            });
            let close_mailbox = mailbox.clone();
            let close_start = start.clone();
            let close = std::thread::spawn(move || {
                close_start.wait();
                close_mailbox.close_and_drain()
            });
            start.wait();

            let admitted = admit.join().unwrap();
            let discarded = close.join().unwrap();
            match admitted {
                Ok(receipt) => {
                    assert_eq!(discarded.len(), 1);
                    assert_eq!(discarded[0].receipt, receipt);
                }
                Err(rejection) => {
                    assert!(discarded.is_empty());
                    assert!(matches!(
                        rejection.reason,
                        SteerRejectionReason::TurnClosing { .. }
                    ));
                }
            }
        }
    }
}
