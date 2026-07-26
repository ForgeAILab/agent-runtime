//! Fail-closed approval.
//!
//! The runtime consults an [`ApprovalPolicy`] before any mutating or
//! process-spawning tool runs. Absence of a policy, or a `Deny`/timeout
//! decision, denies the action. The donor expressed approval through a domain
//! tool and interactive `PermissionService`; here it is a neutral, injectable
//! policy the runtime enforces centrally.

use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::ToolCallId;
use crate::tool::{ToolEffects, WriteScope};

/// A request for approval of a tool invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    /// The tool call requiring approval.
    pub call_id: ToolCallId,
    /// The tool name.
    pub tool: String,
    /// The (validated) arguments.
    pub arguments: Value,
    /// The tool's declared effects.
    pub effects: ToolEffects,
}

impl ApprovalRequest {
    /// The write scopes the invocation would touch.
    pub fn write_scopes(&self) -> Vec<WriteScope> {
        self.effects.write_scopes().cloned().collect()
    }

    /// Whether the invocation spawns a process, distinct from a plain write.
    pub fn spawns_process(&self) -> bool {
        self.effects.spawns_process()
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

    #[test]
    fn spawns_process_is_distinguishable_from_a_plain_write() {
        let write = ApprovalRequest {
            call_id: ToolCallId::new("c"),
            tool: "write".into(),
            arguments: Value::Null,
            effects: ToolEffects::read_only().with_write("/w"),
        };
        let spawn = ApprovalRequest {
            call_id: ToolCallId::new("c"),
            tool: "spawn".into(),
            arguments: Value::Null,
            effects: ToolEffects::new(vec![]).with_spawn(),
        };
        assert!(!write.spawns_process());
        assert!(spawn.spawns_process());
    }

    #[tokio::test]
    async fn deny_all_is_fail_closed() {
        let req = ApprovalRequest {
            call_id: ToolCallId::new("c"),
            tool: "write".into(),
            arguments: Value::Null,
            effects: ToolEffects::read_only().with_write("/w"),
        };
        assert!(!DenyAll.decide(&req).await.is_allowed());
        assert!(AllowAll.decide(&req).await.is_allowed());
    }
}
