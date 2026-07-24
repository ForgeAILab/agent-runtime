//! The executable half of an ability, materialized late.
//!
//! An [`AbilityDescriptor`] is enough to index, search, and budget a
//! capability. It is never enough to *use* one: turning it into a tool
//! schema, a skill's instruction body, an MCP connection, or an agent
//! definition is a separate, deliberately later step performed by an
//! [`ActivationHandle`], and only after [`ActivationPolicy`] approves.
//!
//! This split protects one invariant end to end: **discovery never implies
//! activation permission, and activation never bypasses invocation-time
//! approval.** A descriptor can be searched, ranked, and returned to an agent
//! with no side effect whatsoever; a handle only reads a file, dials a
//! connection, or otherwise spends context or opens something once a policy
//! has separately said yes.

use std::fmt;

use agent_runtime_registry::{RegistryId, RegistryRevision};

use crate::descriptor::{AbilityDescriptor, DependencyRequirement};

/// What activating a descriptor materializes — the one thing that actually
/// costs context or opens a connection. Typed so a caller cannot, for
/// example, mistake a tool's JSON schema for a skill's instruction body.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Activated {
    /// A skill's instruction body, read from its source only now.
    SkillInstructions(String),
    /// Enough information to open a Model Context Protocol connection.
    /// Establishing the connection is the caller's responsibility; producing
    /// this value never dials on its own.
    McpConnection(McpConnectionInfo),
    /// Enough information to construct or delegate to a sub-agent. Producing
    /// this value never starts the sub-agent.
    AgentDefinition(AgentDefinitionInfo),
    /// A native tool's JSON schema, ready to advertise to a provider. Only
    /// constructible with the `tool` feature enabled.
    #[cfg(feature = "tool")]
    ToolSchema(agent_runtime_core::provider::ToolSchema),
}

/// Enough information to open an MCP connection: which server, and which
/// tool on it if this descriptor names one specifically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpConnectionInfo {
    /// The MCP server's registry id.
    pub server: RegistryId,
    /// The specific tool name on that server, if any.
    pub tool: Option<String>,
}

/// Enough information to construct or delegate to a sub-agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDefinitionInfo {
    /// The agent's registry id.
    pub agent: RegistryId,
}

/// Why an activation attempt was rejected. Every variant is safe to log:
/// credential/configuration requirements surface as *names only*, never
/// values.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ActivationError {
    /// Required credential or configuration names are not confirmed ready.
    ReadinessUnmet {
        /// The missing names (credentials and/or configuration keys).
        missing: Vec<String>,
    },
    /// Host policy explicitly denied this activation.
    Denied {
        /// A human-facing reason, safe to surface.
        reason: String,
    },
    /// An already-active id conflicts with this one.
    Conflict {
        /// The active id this descriptor conflicts with.
        with: RegistryId,
    },
    /// The descriptor's content revision no longer matches what the caller
    /// expected (for example, a stale search result).
    RevisionMismatch {
        /// The revision the caller expected.
        expected: RegistryRevision,
        /// The descriptor's current revision.
        found: RegistryRevision,
    },
    /// A required dependency has no satisfied alternative.
    DependencyUnsatisfied {
        /// The unsatisfied requirement.
        requirement: DependencyRequirement,
    },
    /// Materializing the payload failed (for example, the instruction file
    /// could not be read).
    Unavailable {
        /// A human-facing reason, safe to surface.
        reason: String,
    },
}

impl fmt::Display for ActivationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActivationError::ReadinessUnmet { missing } => {
                write!(
                    f,
                    "activation requires unavailable readiness: {}",
                    missing.join(", ")
                )
            }
            ActivationError::Denied { reason } => write!(f, "activation denied: {reason}"),
            ActivationError::Conflict { with } => write!(f, "conflicts with active `{with}`"),
            ActivationError::RevisionMismatch { expected, found } => {
                write!(f, "expected revision `{expected}`, found `{found}`")
            }
            ActivationError::DependencyUnsatisfied { requirement } => write!(
                f,
                "no satisfied dependency among: {}",
                requirement
                    .alternatives()
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            ActivationError::Unavailable { reason } => {
                write!(f, "activation unavailable: {reason}")
            }
        }
    }
}

impl std::error::Error for ActivationError {}

/// What the caller currently knows about readiness and policy — the
/// fail-closed input to [`ActivationPolicy::authorize`]. Building one is the
/// host's job; nothing here is inferred from discovery.
#[derive(Debug, Clone, Default)]
pub struct ActivationContext {
    /// Ids already active or otherwise satisfied, for dependency checks.
    pub satisfied: Vec<RegistryId>,
    /// Ids currently active, for conflict checks.
    pub active: Vec<RegistryId>,
    /// Credential names confirmed ready.
    pub ready_credentials: Vec<String>,
    /// Configuration key names confirmed ready.
    pub ready_config: Vec<String>,
    /// Ids explicitly denied by host policy.
    pub denied: Vec<RegistryId>,
    /// The content revision the caller expects, if activating a previously
    /// discovered descriptor.
    pub expected_revision: Option<RegistryRevision>,
}

impl ActivationContext {
    /// An empty context: nothing satisfied, nothing ready, nothing denied.
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares ids already active or otherwise satisfied.
    pub fn with_satisfied(mut self, ids: impl IntoIterator<Item = RegistryId>) -> Self {
        self.satisfied = ids.into_iter().collect();
        self
    }

    /// Declares ids currently active.
    pub fn with_active(mut self, ids: impl IntoIterator<Item = RegistryId>) -> Self {
        self.active = ids.into_iter().collect();
        self
    }

    /// Declares credential names confirmed ready.
    pub fn with_ready_credentials<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.ready_credentials = names.into_iter().map(Into::into).collect();
        self
    }

    /// Declares configuration key names confirmed ready.
    pub fn with_ready_config<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.ready_config = names.into_iter().map(Into::into).collect();
        self
    }

    /// Declares ids explicitly denied by host policy.
    pub fn with_denied(mut self, ids: impl IntoIterator<Item = RegistryId>) -> Self {
        self.denied = ids.into_iter().collect();
        self
    }

    /// Declares the content revision the caller expects to activate.
    pub fn expecting_revision(mut self, revision: RegistryRevision) -> Self {
        self.expected_revision = Some(revision);
        self
    }
}

/// Authorizes (or rejects) one activation attempt.
///
/// Discovery never implies this: every activation call goes through a
/// policy, and the default [`FailClosedPolicy`] only approves when
/// dependencies are satisfied, no conflict is active, the expected revision
/// (if any) matches, and readiness requirements are met.
pub trait ActivationPolicy {
    /// Authorizes activating `descriptor` given `context`.
    fn authorize(
        &self,
        descriptor: &AbilityDescriptor,
        context: &ActivationContext,
    ) -> Result<(), ActivationError>;
}

/// The default fail-closed policy: satisfied dependencies, no active
/// conflict, a matching expected revision, and met readiness — nothing more.
/// It grants nothing a descriptor did not already declare a way to satisfy.
#[derive(Debug, Clone, Copy, Default)]
pub struct FailClosedPolicy;

impl ActivationPolicy for FailClosedPolicy {
    fn authorize(
        &self,
        descriptor: &AbilityDescriptor,
        context: &ActivationContext,
    ) -> Result<(), ActivationError> {
        if context.denied.contains(descriptor.id()) {
            return Err(ActivationError::Denied {
                reason: format!("{} is denied by host policy", descriptor.id()),
            });
        }
        if let Some(expected) = &context.expected_revision {
            if expected != descriptor.content_revision() {
                return Err(ActivationError::RevisionMismatch {
                    expected: expected.clone(),
                    found: descriptor.content_revision().clone(),
                });
            }
        }
        for conflict in descriptor.conflicts() {
            if context.active.contains(conflict) {
                return Err(ActivationError::Conflict {
                    with: conflict.clone(),
                });
            }
        }
        if let Some(requirement) = descriptor
            .unsatisfied_dependencies(&context.satisfied)
            .into_iter()
            .next()
        {
            return Err(ActivationError::DependencyUnsatisfied {
                requirement: requirement.clone(),
            });
        }
        let missing = descriptor
            .readiness()
            .missing(&context.ready_credentials, &context.ready_config);
        if !missing.is_empty() {
            return Err(ActivationError::ReadinessUnmet { missing });
        }
        Ok(())
    }
}

/// Resolves a descriptor into the one thing that actually costs context or
/// opens a connection.
///
/// A descriptor alone is enough to index, search, and budget; implementing
/// this trait is what performs the possibly I/O-bound work of turning it
/// into something invocable — and [`activate`] only ever calls it after a
/// policy has approved.
pub trait ActivationHandle {
    /// Materializes the executable payload. Implementors may perform I/O
    /// here (reading a file, opening a connection) — and only here.
    fn activate(&self) -> Result<Activated, ActivationError>;
}

/// Authorizes, then activates: the one path through which a descriptor ever
/// becomes something that costs context or opens a connection.
///
/// Authorization runs first, unconditionally. [`ActivationHandle::activate`]
/// is never called when it fails, so a denied, conflicting, or unready
/// capability never causes a side effect.
pub fn activate<H: ActivationHandle>(
    descriptor: &AbilityDescriptor,
    handle: &H,
    policy: &dyn ActivationPolicy,
    context: &ActivationContext,
) -> Result<Activated, ActivationError> {
    policy.authorize(descriptor, context)?;
    handle.activate()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ability::AbilityKind;
    use agent_runtime_registry::{EntryProvenance, RegistrySource};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn provenance() -> EntryProvenance {
        EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1"))
    }

    fn descriptor(name: &str) -> AbilityDescriptor {
        AbilityDescriptor::new(
            AbilityKind::Mcp,
            name,
            provenance(),
            name,
            "a fake capability",
            RegistryRevision::new("2"),
        )
    }

    struct FakeHandle {
        dialed: Arc<AtomicBool>,
    }

    impl ActivationHandle for FakeHandle {
        fn activate(&self) -> Result<Activated, ActivationError> {
            self.dialed.store(true, Ordering::SeqCst);
            Ok(Activated::McpConnection(McpConnectionInfo {
                server: RegistryId::mcp("paid-search"),
                tool: None,
            }))
        }
    }

    #[test]
    fn every_declared_dependency_must_be_satisfied() {
        let descriptor = descriptor("research")
            .with_dependency(DependencyRequirement::single(RegistryId::tool("search")))
            .with_dependency(DependencyRequirement::single(RegistryId::mcp("browser")));
        let context = ActivationContext::new().with_satisfied([RegistryId::tool("search")]);

        let err = FailClosedPolicy
            .authorize(&descriptor, &context)
            .unwrap_err();
        assert_eq!(
            err,
            ActivationError::DependencyUnsatisfied {
                requirement: DependencyRequirement::single(RegistryId::mcp("browser"))
            }
        );
    }

    #[test]
    fn a_dependency_is_satisfied_by_any_declared_alternative() {
        let descriptor = descriptor("research").with_dependency(DependencyRequirement::any_of([
            RegistryId::tool("search-a"),
            RegistryId::tool("search-b"),
        ]));
        let context = ActivationContext::new().with_satisfied([RegistryId::tool("search-b")]);

        assert!(FailClosedPolicy.authorize(&descriptor, &context).is_ok());
    }

    #[test]
    fn an_active_conflict_denies_activation() {
        let descriptor =
            descriptor("aggressive-edit").with_conflicts([RegistryId::tool("safe-edit")]);
        let context = ActivationContext::new().with_active([RegistryId::tool("safe-edit")]);

        assert_eq!(
            FailClosedPolicy
                .authorize(&descriptor, &context)
                .unwrap_err(),
            ActivationError::Conflict {
                with: RegistryId::tool("safe-edit")
            }
        );
    }

    #[test]
    fn a_stale_expected_revision_is_rejected() {
        let descriptor = descriptor("research");
        let context = ActivationContext::new().expecting_revision(RegistryRevision::new("1"));

        assert_eq!(
            FailClosedPolicy
                .authorize(&descriptor, &context)
                .unwrap_err(),
            ActivationError::RevisionMismatch {
                expected: RegistryRevision::new("1"),
                found: RegistryRevision::new("2"),
            }
        );
    }

    #[test]
    fn denied_ids_are_rejected_even_when_otherwise_ready() {
        let descriptor = descriptor("research");
        let context = ActivationContext::new().with_denied([RegistryId::mcp("research")]);

        assert!(matches!(
            FailClosedPolicy.authorize(&descriptor, &context),
            Err(ActivationError::Denied { .. })
        ));
    }

    /// Spec scenario: "Search result requires unavailable credentials". The
    /// descriptor is relevant and otherwise satisfiable, but its readiness
    /// requirement is unmet, so activation must fail closed *before* the
    /// handle ever runs — no connection or side effect occurs.
    #[test]
    fn activation_fails_closed_when_credentials_are_not_ready_and_causes_no_side_effect() {
        let descriptor = descriptor("paid-search").with_readiness(
            crate::descriptor::ReadinessRequirement::none().with_credentials(["SEARCH_API_KEY"]),
        );
        let dialed = Arc::new(AtomicBool::new(false));
        let handle = FakeHandle {
            dialed: dialed.clone(),
        };
        let context = ActivationContext::new();

        let err = activate(&descriptor, &handle, &FailClosedPolicy, &context).unwrap_err();

        assert_eq!(
            err,
            ActivationError::ReadinessUnmet {
                missing: vec!["SEARCH_API_KEY".to_string()]
            }
        );
        assert!(
            !dialed.load(Ordering::SeqCst),
            "activation must not dial when unready"
        );
    }

    #[test]
    fn activation_succeeds_once_authorized_and_returns_the_typed_payload() {
        let descriptor = descriptor("paid-search").with_readiness(
            crate::descriptor::ReadinessRequirement::none().with_credentials(["SEARCH_API_KEY"]),
        );
        let dialed = Arc::new(AtomicBool::new(false));
        let handle = FakeHandle {
            dialed: dialed.clone(),
        };
        let context = ActivationContext::new().with_ready_credentials(["SEARCH_API_KEY"]);

        let activated = activate(&descriptor, &handle, &FailClosedPolicy, &context).unwrap();

        assert!(dialed.load(Ordering::SeqCst));
        assert_eq!(
            activated,
            Activated::McpConnection(McpConnectionInfo {
                server: RegistryId::mcp("paid-search"),
                tool: None,
            })
        );
    }
}
