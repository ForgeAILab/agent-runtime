use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use agent_runtime::core::approval::{ApprovalDecision, ApprovalPolicy, ApprovalRequest};
use agent_runtime::core::cancel::Cancellation;
use agent_runtime::core::grant::{
    DecisionCode, GrantConstraints, SecurityCheck, SecurityCheckId, SecurityCheckOutcome,
    SecurityCheckRevision,
};
use agent_runtime::core::security::{AuthorizationRequest, PermissionSet};
use agent_runtime::registry::Permission;
use agent_runtime_mcp::McpConnection;
use async_trait::async_trait;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(crate) struct CliMcpPolicy {
    tools: BTreeSet<String>,
    id: SecurityCheckId,
    revision: SecurityCheckRevision,
    policy_revision: SecurityCheckRevision,
    coverage: PermissionSet,
}

impl CliMcpPolicy {
    pub(crate) fn new(tools: BTreeSet<String>) -> Self {
        let policy_material = tools.iter().cloned().collect::<Vec<_>>().join("\n");
        Self {
            tools,
            id: SecurityCheckId::new("cli-mcp-exact-tool-policy"),
            revision: SecurityCheckRevision::new("v1"),
            policy_revision: SecurityCheckRevision::new(format!("allow:{policy_material}")),
            coverage: Self::coverage_set(),
        }
    }

    pub(crate) fn coverage(&self) -> PermissionSet {
        self.coverage.clone()
    }

    fn coverage_set() -> PermissionSet {
        PermissionSet::from_iter([
            Permission::ExternalRead,
            Permission::ExternalWrite,
            Permission::NetHttp,
            Permission::DataEgress,
        ])
    }

    fn action_tool<'a>(&self, request: &'a AuthorizationRequest) -> Option<&'a str> {
        request.action.as_str().strip_prefix("tool.")
    }

    fn allows_tool(&self, tool: &str) -> bool {
        self.tools.contains(tool)
    }
}

#[async_trait]
impl SecurityCheck for CliMcpPolicy {
    fn id(&self) -> &SecurityCheckId {
        &self.id
    }

    fn revision(&self) -> &SecurityCheckRevision {
        &self.revision
    }

    fn policy_data_revision(&self) -> Option<SecurityCheckRevision> {
        Some(self.policy_revision.clone())
    }

    fn declared_coverage(&self) -> Option<PermissionSet> {
        Some(self.coverage.clone())
    }

    async fn evaluate(
        &self,
        request: &AuthorizationRequest,
        _cancel: &Cancellation,
    ) -> SecurityCheckOutcome {
        let Some(tool) = self.action_tool(request) else {
            return SecurityCheckOutcome::Deny {
                code: DecisionCode::other("cli_mcp_non_tool_action"),
            };
        };
        if !self.allows_tool(tool) {
            return SecurityCheckOutcome::Deny {
                code: DecisionCode::other("cli_mcp_tool_not_approved"),
            };
        }
        if !request.requested.is_subset(&self.coverage) {
            return SecurityCheckOutcome::Deny {
                code: DecisionCode::other("cli_mcp_permission_outside_policy"),
            };
        }
        SecurityCheckOutcome::RequireApproval {
            constraints: GrantConstraints::unconstrained(),
        }
    }
}

#[async_trait]
impl ApprovalPolicy for CliMcpPolicy {
    async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
        if self.allows_tool(request.prepared().tool()) {
            ApprovalDecision::Allow
        } else {
            ApprovalDecision::deny("MCP tool was not explicitly approved for this CLI run")
        }
    }
}

pub(crate) async fn shutdown_connections(connections: &[Arc<McpConnection>]) -> Vec<String> {
    let cleanup = async {
        let mut diagnostics = Vec::new();
        for connection in connections.iter().rev() {
            let server = connection.server().to_owned();
            if let Err(error) = connection.shutdown().await {
                diagnostics.push(format!(
                    "MCP server `{server}` did not shut down cleanly: {error}"
                ));
            }
        }
        diagnostics
    };
    match tokio::time::timeout(SHUTDOWN_TIMEOUT, cleanup).await {
        Ok(diagnostics) => diagnostics,
        Err(_elapsed) => vec![format!(
            "MCP connection cleanup exceeded the aggregate {} ms shutdown bound",
            SHUTDOWN_TIMEOUT.as_millis()
        )],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_tool_policy_does_not_become_allow_all() {
        let policy = CliMcpPolicy::new(BTreeSet::from(["mcp__demo__search".to_owned()]));
        assert!(policy.allows_tool("mcp__demo__search"));
        assert!(!policy.allows_tool("mcp__demo__delete"));
        assert_eq!(policy.coverage().len(), 4);
    }
}
