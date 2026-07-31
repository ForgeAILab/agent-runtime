//! Fail-closed approval.
//!
//! A composed authorization decision may require host approval for any
//! prepared permission, including reads, writes, process/network access, or
//! host-defined authority. Absence of host support, denial, cancellation, or
//! timeout fails closed. The donor expressed approval through a domain tool
//! and interactive `PermissionService`; here it is a neutral, injectable
//! policy the runtime enforces centrally.

use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::clock::Deadline;
use crate::ids::{RequestId, SessionId, TurnId};
use crate::tool::{PreparedToolCall, WriteScope};

/// Stable origin identity for one pending approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalOrigin {
    /// Session that owns the pending action.
    session: SessionId,
    /// Provider request that produced the action.
    request: RequestId,
    /// Owning turn once the checkpointable turn machine supplies it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    turn: Option<TurnId>,
}

impl ApprovalOrigin {
    /// Creates an origin before turn identity is available at the call site.
    pub fn new(session: SessionId, request: RequestId) -> Self {
        Self {
            session,
            request,
            turn: None,
        }
    }

    /// Adds the owning turn identity.
    pub fn with_turn(mut self, turn: TurnId) -> Self {
        self.turn = Some(turn);
        self
    }

    /// The owning session.
    pub fn session(&self) -> &SessionId {
        &self.session
    }

    /// The provider request that produced the action.
    pub fn request(&self) -> &RequestId {
        &self.request
    }

    /// The owning turn, when available.
    pub fn turn(&self) -> Option<&TurnId> {
        self.turn.as_ref()
    }
}

/// A request for approval of a tool invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    /// The exact immutable action requiring approval.
    prepared: PreparedToolCall,
    /// The turn deadline that bounds this approval wait.
    deadline: Deadline,
    /// Stable session/request/turn attribution.
    origin: ApprovalOrigin,
}

impl ApprovalRequest {
    /// Builds a request around one verified prepared action.
    pub fn new(prepared: PreparedToolCall, deadline: Deadline, origin: ApprovalOrigin) -> Self {
        Self {
            prepared,
            deadline,
            origin,
        }
    }

    /// The exact action whose eligible grant this decision may resolve.
    pub fn prepared(&self) -> &PreparedToolCall {
        &self.prepared
    }

    /// The absolute turn deadline rendered by a host approval surface.
    pub fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// Stable origin identity used to key, persist, reopen, and cancel the
    /// correct pending prompt without crossing session boundaries.
    pub fn origin(&self) -> &ApprovalOrigin {
        &self.origin
    }

    /// The write scopes the invocation would touch.
    pub fn write_scopes(&self) -> Vec<WriteScope> {
        self.prepared.effects().write_scopes().cloned().collect()
    }

    /// Whether the invocation spawns a process, distinct from a plain write.
    pub fn spawns_process(&self) -> bool {
        self.prepared.effects().spawns_process()
    }
}

/// The outcome of an approval decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum ApprovalDecision {
    /// The action is allowed.
    Allow,
    /// The action is denied, with a reason.
    Deny {
        /// Why the action was denied.
        reason: String,
    },
    /// The host proposes different arguments. This is not approval: the
    /// executor must discard the prior eligibility and start a new
    /// validate/prepare/authorize/approval cycle.
    Edit {
        /// Replacement raw arguments to validate and prepare.
        arguments: Value,
    },
    /// The host did not answer before the turn deadline.
    TimedOut,
    /// The approval wait was cancelled with the turn.
    Cancelled,
    /// The current host cannot surface approval.
    Unavailable {
        /// Why approval support is unavailable.
        reason: String,
    },
}

impl ApprovalDecision {
    /// Whether the decision allows the action.
    pub fn is_allowed(&self) -> bool {
        matches!(self, ApprovalDecision::Allow)
    }

    /// A denial with the given reason.
    pub fn deny(reason: impl Into<String>) -> Self {
        ApprovalDecision::Deny {
            reason: reason.into(),
        }
    }

    /// Reports unavailable host approval support.
    pub fn unavailable(reason: impl Into<String>) -> Self {
        ApprovalDecision::Unavailable {
            reason: reason.into(),
        }
    }
}

/// A host-injected approval policy.
#[async_trait]
pub trait ApprovalPolicy: Send + Sync + fmt::Debug {
    /// Decides whether the invocation may proceed. Implementations should
    /// default to denial when uncertain.
    async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision;
}

/// A policy that denies every request. This is the fail-closed default used
/// when a host supplies no approval policy.
#[derive(Debug, Clone, Copy, Default)]
pub struct DenyAll;

#[async_trait]
impl ApprovalPolicy for DenyAll {
    async fn decide(&self, _request: &ApprovalRequest) -> ApprovalDecision {
        ApprovalDecision::deny("no approval policy configured")
    }
}

/// Reports that the current host cannot surface approval.
///
/// This is the runtime's fail-closed default when no approval policy is
/// configured. [`DenyAll`] remains available for a host that intentionally
/// rejects every action; the two outcomes are observably distinct.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnavailableApproval;

#[async_trait]
impl ApprovalPolicy for UnavailableApproval {
    async fn decide(&self, _request: &ApprovalRequest) -> ApprovalDecision {
        ApprovalDecision::unavailable("no approval policy configured")
    }
}

/// A policy that allows every request. Intended for trusted headless hosts and
/// tests only.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

#[async_trait]
impl ApprovalPolicy for AllowAll {
    async fn decide(&self, _request: &ApprovalRequest) -> ApprovalDecision {
        ApprovalDecision::Allow
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::ToolCallId;
    use crate::security::{PermissionSet, SecurityResource};
    use crate::tool::{PreparedToolCall, ToolCallDisplay, ToolEffects};

    fn request(name: &str, effects: ToolEffects) -> ApprovalRequest {
        ApprovalRequest::new(
            PreparedToolCall::new(
                ToolCallId::new("c"),
                name,
                Value::Null,
                PermissionSet::new(),
                SecurityResource::other("tool", name),
                effects,
                ToolCallDisplay::new(format!("Run {name}")),
            ),
            Deadline::never(),
            ApprovalOrigin::new(SessionId::new("s"), RequestId::new("r")),
        )
    }

    #[test]
    fn spawns_process_is_distinguishable_from_a_plain_write() {
        let write = request("write", ToolEffects::read_only().with_write("/w"));
        let spawn = request("spawn", ToolEffects::new(vec![]).with_spawn());
        assert!(!write.spawns_process());
        assert!(spawn.spawns_process());
    }

    #[tokio::test]
    async fn deny_all_is_fail_closed() {
        let req = request("write", ToolEffects::read_only().with_write("/w"));
        assert!(!DenyAll.decide(&req).await.is_allowed());
        assert!(AllowAll.decide(&req).await.is_allowed());
        assert!(matches!(
            UnavailableApproval.decide(&req).await,
            ApprovalDecision::Unavailable { .. }
        ));
    }
}
