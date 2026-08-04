//! Tool execution: authorization, approval, workspace enforcement,
//! scheduling, and bounds.
//!
//! The executor is the single choke point where side effects happen. Every
//! call is validated and prepared first; any non-empty prepared permission
//! set must obtain a composed authorization decision from the injected
//! [`SecurityCheckSet`] (security-enforcement's "Central default-deny
//! authorization"); a `RequireApproval` decision must then also obtain an
//! `Allow` from the injected [`ApprovalPolicy`], and every declared write
//! scope must lie inside the [`Workspace`]. A missing approval policy denies
//! by construction as an observable unavailable-host outcome; a missing
//! authoritative check for a requested permission denies by construction too, since
//! [`SecurityCheckSet`] itself is default-deny. Unknown tools, denials,
//! workspace violations, deadlines, and tool errors all become canonical
//! error [`ToolResultBlock`]s so the model always receives a result for every
//! call it made.

use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::join_all;
use serde_json::Value;

use agent_runtime_core::approval::{
    ApprovalDecision, ApprovalOrigin, ApprovalPolicy, ApprovalRequest,
};
use agent_runtime_core::cancel::Cancellation;
use agent_runtime_core::check_set::SecurityCheckSet;
use agent_runtime_core::checkpoint::ToolSlotCheckpoint;
use agent_runtime_core::clock::{Clock, Deadline};
use agent_runtime_core::content::{ToolCall, ToolResultBlock};
use agent_runtime_core::grant::{AuthorizationDecision, CapabilityGrant};
use agent_runtime_core::ids::{RequestId, SessionId, TenantId, TurnId};
use agent_runtime_core::security::{
    AuthorizationRequest, SecurityAction, SecurityContext, SecurityEvidence, SecurityResource,
    SecuritySubject,
};
use agent_runtime_core::tool::{
    InvocationContext, PreparationContext, PreparedToolCall, Tool, ToolEffects, ToolOutcome,
    ToolSpec,
};
use agent_runtime_core::workspace::Workspace;
use agent_runtime_registry::TrustClass;

use super::registry::SealedToolRegistry;
use super::scheduler::{ConflictPolicy, plan_batches};

/// The composed check set an executor authorizes every non-pure invocation
/// against, plus the identity used to build each invocation's
/// [`SecurityContext`].
///
/// `subject` and `tenant` are shared by every session this executor serves;
/// per-session scoping comes from the [`SessionId`] passed to
/// [`ToolExecutor::execute`] itself. Finer-grained per-session identity is
/// registry-routing work (`tasks.md` 2.3), not something this executor
/// invents on its own.
#[derive(Debug, Clone)]
pub struct SecurityConfig {
    /// The sealed, runtime-owned composer.
    pub check_set: Arc<SecurityCheckSet>,
    /// The security subject every request from this executor is attributed
    /// to.
    pub subject: SecuritySubject,
    /// The tenant every request from this executor is scoped to.
    pub tenant: TenantId,
}

/// Executes tool calls for one turn.
#[derive(Debug, Clone)]
pub struct ToolExecutor {
    registry: SealedToolRegistry,
    approval: Arc<dyn ApprovalPolicy>,
    workspace: Arc<dyn Workspace>,
    clock: Arc<dyn Clock>,
    output_limit: usize,
    conflict_policy: ConflictPolicy,
    security: SecurityConfig,
}

/// One validation/preparation/authorization cycle, before any host approval.
#[allow(clippy::large_enum_variant)]
pub(crate) enum PreparedAuthorization {
    /// The composed authority allowed this exact action directly.
    Ready(ReadyToolCall),
    /// The exact action is eligible but requires host approval.
    AwaitingApproval(PendingToolApproval),
    /// Preparation or authorization failed as a canonical tool result.
    Rejected(ToolResultBlock),
}

/// Immutable request boundary used while preparing and authorizing one call.
///
/// Keeping these related values together prevents a caller from accidentally
/// mixing request, session, turn, cancellation, and deadline state.
pub(crate) struct PreparationAuthorizationContext<'a> {
    request: &'a RequestId,
    session: &'a SessionId,
    turn: Option<&'a TurnId>,
    cancel: &'a Cancellation,
    deadline: Deadline,
}

impl<'a> PreparationAuthorizationContext<'a> {
    pub(crate) fn new(
        request: &'a RequestId,
        session: &'a SessionId,
        turn: Option<&'a TurnId>,
        cancel: &'a Cancellation,
        deadline: Deadline,
    ) -> Self {
        Self {
            request,
            session,
            turn,
            cancel,
            deadline,
        }
    }
}

/// An exact prepared action plus its in-memory eligible grant.
///
/// The grant itself is deliberately not checkpointed. Recovery reauthorizes
/// and, when required, re-approves the exact remaining preparation so policy
/// revision and revocation are observed after restart.
pub(crate) struct PendingToolApproval {
    call: ToolCall,
    tool: Arc<dyn Tool>,
    prepared: PreparedToolCall,
    eligible: CapabilityGrant,
}

impl PendingToolApproval {
    pub(crate) fn prepared(&self) -> &PreparedToolCall {
        &self.prepared
    }
}

/// Resolution of one pending approval decision.
#[allow(clippy::large_enum_variant)]
pub(crate) enum PendingApprovalResolution {
    /// The same immutable prepared action was approved.
    Ready(ReadyToolCall),
    /// The host proposed replacement arguments; this is not approval.
    Edited(ToolCall),
    /// Approval failed closed as a canonical tool result.
    Rejected(ToolResultBlock),
}

/// The refusal for a workspace escape that no approval decision covered.
const UNATTENDED_ESCAPE: &str = "an out-of-workspace filesystem resource requires an explicit \
     approval decision; the composed checks allowed it unattended";

impl ToolExecutor {
    const MAX_APPROVAL_EDITS: usize = 8;

    /// Builds an executor from its injected services.
    pub fn new(
        registry: SealedToolRegistry,
        approval: Arc<dyn ApprovalPolicy>,
        workspace: Arc<dyn Workspace>,
        clock: Arc<dyn Clock>,
        output_limit: usize,
        conflict_policy: ConflictPolicy,
        security: SecurityConfig,
    ) -> Self {
        Self {
            registry,
            approval,
            workspace,
            clock,
            output_limit,
            conflict_policy,
            security,
        }
    }

    pub(crate) fn security(&self) -> &SecurityConfig {
        &self.security
    }

    pub(crate) fn approval_policy(&self) -> &Arc<dyn ApprovalPolicy> {
        &self.approval
    }

    /// Executes `calls`, returning one [`ToolResultBlock`] per call in request
    /// order. Overlapping writes are serialized; independent calls in a batch
    /// run concurrently.
    pub async fn execute(
        &self,
        calls: &[ToolCall],
        request: &RequestId,
        session: &SessionId,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Vec<ToolResultBlock> {
        self.execute_with_turn(calls, request, session, None, cancel, deadline)
            .await
    }

    pub(crate) async fn execute_with_turn(
        &self,
        calls: &[ToolCall],
        request: &RequestId,
        session: &SessionId,
        turn: Option<&TurnId>,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Vec<ToolResultBlock> {
        let prepared = self
            .prepare_batch(calls, request, session, turn, cancel, deadline)
            .await;
        self.invoke_batch(prepared, request, cancel, deadline).await
    }

    /// Side-effect-aware execution batches for a prepared set.
    pub(crate) fn execution_batches(&self, prepared: &PreparedToolBatch) -> Vec<Vec<usize>> {
        plan_batches(&prepared.effects, self.conflict_policy)
    }

    async fn prepare_batch(
        &self,
        calls: &[ToolCall],
        request: &RequestId,
        session: &SessionId,
        turn: Option<&TurnId>,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> PreparedToolBatch {
        let mut results: Vec<Option<ToolResultBlock>> = vec![None; calls.len()];
        let mut ready: Vec<Option<ReadyToolCall>> =
            std::iter::repeat_with(|| None).take(calls.len()).collect();
        let mut effects = vec![ToolEffects::default(); calls.len()];

        // Preparation and host interaction stay deterministic in request
        // order. No invocation starts until every runnable call has reached
        // an authorized/approved immutable prepared action.
        for (index, call) in calls.iter().enumerate() {
            match self
                .prepare_one(call, request, session, turn, cancel, deadline)
                .await
            {
                Ok(prepared) => {
                    effects[index] = prepared.prepared.effects().clone();
                    ready[index] = Some(prepared);
                }
                Err(message) => {
                    results[index] = Some(error_block(call, message, self.output_limit));
                }
            }
        }
        PreparedToolBatch {
            calls: calls.to_vec(),
            ready,
            results,
            effects,
        }
    }

    async fn invoke_batch(
        &self,
        mut prepared_batch: PreparedToolBatch,
        request: &RequestId,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Vec<ToolResultBlock> {
        let batches = plan_batches(&prepared_batch.effects, self.conflict_policy);
        for batch in batches {
            let futures = batch.iter().filter_map(|&i| {
                let prepared = prepared_batch.ready[i].take()?;
                Some(async move {
                    let block = self.invoke_one(prepared, request, cancel, deadline).await;
                    (i, block)
                })
            });
            for (i, block) in join_all(futures).await {
                prepared_batch.results[i] = Some(block);
            }
        }

        prepared_batch
            .results
            .into_iter()
            .enumerate()
            .map(|(i, block)| {
                block.unwrap_or_else(|| {
                    error_block(
                        &prepared_batch.calls[i],
                        "tool was not executed",
                        self.output_limit,
                    )
                })
            })
            .collect()
    }

    /// Runs exactly one validate → prepare → authorize cycle.
    ///
    /// `AwaitingApproval` returns before consulting the approval host so the
    /// turn machine can durably checkpoint the exact preparation first.
    pub(crate) async fn prepare_and_authorize_once(
        &self,
        call: &ToolCall,
        arguments: Value,
        context: PreparationAuthorizationContext<'_>,
    ) -> PreparedAuthorization {
        let PreparationAuthorizationContext {
            request,
            session,
            turn,
            cancel,
            deadline,
        } = context;
        let mut authoritative_call = call.clone();
        authoritative_call.arguments = arguments.clone();
        let Some(tool) = self.registry.get(&call.name) else {
            return PreparedAuthorization::Rejected(error_block(
                &authoritative_call,
                format!("tool `{}` is not available", call.name),
                self.output_limit,
            ));
        };
        let Some(spec) = self.registry.spec(&call.name).cloned() else {
            return PreparedAuthorization::Rejected(error_block(
                &authoritative_call,
                format!("tool `{}` is not available", call.name),
                self.output_limit,
            ));
        };

        if let Err(message) = self.ensure_active(cancel, deadline, "before tool preparation") {
            return PreparedAuthorization::Rejected(error_block(
                &authoritative_call,
                message,
                self.output_limit,
            ));
        }
        if let Err(error) = self.registry.validate_arguments(&call.name, &arguments) {
            return PreparedAuthorization::Rejected(error_block(
                &authoritative_call,
                error.message,
                self.output_limit,
            ));
        }

        let preparation_context = PreparationContext {
            session: session.clone(),
            turn: turn.cloned(),
            call_id: call.id.clone(),
            request: request.clone(),
            workspace: self.workspace.clone(),
            clock: self.clock.clone(),
            cancel: cancel.child(),
            deadline,
        };
        let preparation = tokio::select! {
            biased;
            _ = preparation_context.cancel.cancelled() => {
                Err(agent_runtime_core::error::RuntimeError::cancelled(
                    "cancelled while tool was being prepared",
                ))
            }
            _ = wait_for_deadline(deadline, self.clock.clone()) => {
                Err(agent_runtime_core::error::RuntimeError::tool(
                    "deadline elapsed while tool was being prepared",
                ))
            }
            result = tool.prepare(arguments, &preparation_context) => result,
        };
        let prepared = match preparation {
            Ok(prepared) => prepared,
            Err(error) => {
                return PreparedAuthorization::Rejected(error_block(
                    &authoritative_call,
                    error.message,
                    self.output_limit,
                ));
            }
        };
        if let Err(message) = self.verify_prepared(call, &spec, &prepared) {
            return PreparedAuthorization::Rejected(error_block(
                &authoritative_call,
                message,
                self.output_limit,
            ));
        }
        if let Err(message) = self.enforce_workspace(&prepared) {
            return PreparedAuthorization::Rejected(error_block(
                &authoritative_call,
                message,
                self.output_limit,
            ));
        }
        if prepared.required_permissions().is_empty() {
            return PreparedAuthorization::Ready(ReadyToolCall {
                call: authoritative_call,
                tool,
                prepared,
                session: session.clone(),
                turn: turn.cloned(),
            });
        }

        let context = SecurityContext::new(
            self.security.subject.clone(),
            session.clone(),
            self.security.tenant.clone(),
            self.security.check_set.revision().clone(),
        );
        let evidence =
            SecurityEvidence::new(TrustClass::ExternalContent, prepared.fingerprint().clone());
        let auth_request = AuthorizationRequest::new(
            context,
            SecurityAction::new(format!("tool.{}", call.name)),
            prepared.resource().clone(),
            prepared.required_permissions().clone(),
            deadline,
            evidence,
        );
        let outcome = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return PreparedAuthorization::Rejected(error_block(
                    &authoritative_call,
                    "cancelled while tool was being authorized",
                    self.output_limit,
                ));
            }
            _ = wait_for_deadline(deadline, self.clock.clone()) => {
                return PreparedAuthorization::Rejected(error_block(
                    &authoritative_call,
                    "deadline elapsed while tool was being authorized",
                    self.output_limit,
                ));
            }
            outcome = self.security.check_set.authorize(&auth_request, cancel) => outcome,
        };
        match outcome.decision {
            AuthorizationDecision::Deny { code } => PreparedAuthorization::Rejected(error_block(
                &authoritative_call,
                format!("authorization denied: {code}"),
                self.output_limit,
            )),
            AuthorizationDecision::RequireApproval { eligible } => {
                PreparedAuthorization::AwaitingApproval(PendingToolApproval {
                    call: authoritative_call,
                    tool,
                    prepared,
                    eligible,
                })
            }
            AuthorizationDecision::Allow { grant: _ } => {
                if self.escapes_workspace(&prepared) {
                    return PreparedAuthorization::Rejected(error_block(
                        &authoritative_call,
                        UNATTENDED_ESCAPE,
                        self.output_limit,
                    ));
                }
                PreparedAuthorization::Ready(ReadyToolCall {
                    call: authoritative_call,
                    tool,
                    prepared,
                    session: session.clone(),
                    turn: turn.cloned(),
                })
            }
        }
    }

    /// Reauthorizes one exact checkpointed preparation without calling
    /// `Tool::prepare` again.
    ///
    /// This is the recovery path for `AwaitingApproval`: policy/revocation is
    /// evaluated fresh, while canonical arguments/resource/effects remain the
    /// exact fingerprinted action that the prior process checkpointed.
    pub(crate) async fn reauthorize_prepared(
        &self,
        prepared: PreparedToolCall,
        session: &SessionId,
        turn: Option<&TurnId>,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> PreparedAuthorization {
        let call = ToolCall {
            id: prepared.call_id().clone(),
            name: prepared.tool().to_owned(),
            arguments: prepared.arguments().clone(),
        };
        let Some(tool) = self.registry.get(prepared.tool()) else {
            return PreparedAuthorization::Rejected(error_block(
                &call,
                format!("tool `{}` is no longer available", prepared.tool()),
                self.output_limit,
            ));
        };
        let Some(spec) = self.registry.spec(prepared.tool()).cloned() else {
            return PreparedAuthorization::Rejected(error_block(
                &call,
                format!("tool `{}` is no longer available", prepared.tool()),
                self.output_limit,
            ));
        };
        if let Err(message) = self.ensure_active(cancel, deadline, "before reauthorization") {
            return PreparedAuthorization::Rejected(error_block(&call, message, self.output_limit));
        }
        if let Err(message) = self.verify_prepared(&call, &spec, &prepared) {
            return PreparedAuthorization::Rejected(error_block(&call, message, self.output_limit));
        }
        if let Err(message) = self.enforce_workspace(&prepared) {
            return PreparedAuthorization::Rejected(error_block(&call, message, self.output_limit));
        }
        if prepared.required_permissions().is_empty() {
            return PreparedAuthorization::Ready(ReadyToolCall {
                call,
                tool,
                prepared,
                session: session.clone(),
                turn: turn.cloned(),
            });
        }

        let context = SecurityContext::new(
            self.security.subject.clone(),
            session.clone(),
            self.security.tenant.clone(),
            self.security.check_set.revision().clone(),
        );
        let evidence =
            SecurityEvidence::new(TrustClass::ExternalContent, prepared.fingerprint().clone());
        let auth_request = AuthorizationRequest::new(
            context,
            SecurityAction::new(format!("tool.{}", prepared.tool())),
            prepared.resource().clone(),
            prepared.required_permissions().clone(),
            deadline,
            evidence,
        );
        let outcome = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return PreparedAuthorization::Rejected(error_block(
                    &call,
                    "cancelled while tool was being reauthorized",
                    self.output_limit,
                ));
            }
            _ = wait_for_deadline(deadline, self.clock.clone()) => {
                return PreparedAuthorization::Rejected(error_block(
                    &call,
                    "deadline elapsed while tool was being reauthorized",
                    self.output_limit,
                ));
            }
            outcome = self.security.check_set.authorize(&auth_request, cancel) => outcome,
        };
        match outcome.decision {
            AuthorizationDecision::Deny { code } => PreparedAuthorization::Rejected(error_block(
                &call,
                format!("authorization denied after restart: {code}"),
                self.output_limit,
            )),
            AuthorizationDecision::RequireApproval { eligible } => {
                PreparedAuthorization::AwaitingApproval(PendingToolApproval {
                    call,
                    tool,
                    prepared,
                    eligible,
                })
            }
            AuthorizationDecision::Allow { grant: _ } => {
                if self.escapes_workspace(&prepared) {
                    return PreparedAuthorization::Rejected(error_block(
                        &call,
                        UNATTENDED_ESCAPE,
                        self.output_limit,
                    ));
                }
                PreparedAuthorization::Ready(ReadyToolCall {
                    call,
                    tool,
                    prepared,
                    session: session.clone(),
                    turn: turn.cloned(),
                })
            }
        }
    }

    /// Consults the approval host once for a checkpointed exact action.
    pub(crate) async fn decide_pending_approval(
        &self,
        pending: PendingToolApproval,
        request: &RequestId,
        session: &SessionId,
        turn: &TurnId,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> PendingApprovalResolution {
        let PendingToolApproval {
            call,
            tool,
            prepared,
            eligible,
        } = pending;
        let origin = ApprovalOrigin::new(session.clone(), request.clone()).with_turn(turn.clone());
        let approval_request = ApprovalRequest::new(prepared.clone(), deadline, origin);
        let decision = tokio::select! {
            biased;
            _ = cancel.cancelled() => ApprovalDecision::Cancelled,
            _ = wait_for_deadline(deadline, self.clock.clone()) => ApprovalDecision::TimedOut,
            decision = self.approval.decide(&approval_request) => decision,
        };
        match decision {
            ApprovalDecision::Allow => {
                match self.security.check_set.resolve_approval(eligible, true) {
                    AuthorizationDecision::Allow { grant: _ } => {
                        PendingApprovalResolution::Ready(ReadyToolCall {
                            call,
                            tool,
                            prepared,
                            session: session.clone(),
                            turn: Some(turn.clone()),
                        })
                    }
                    _ => unreachable!("resolve_approval(_, true) always returns Allow"),
                }
            }
            ApprovalDecision::Edit { arguments } => {
                let _ = self.security.check_set.resolve_approval(eligible, false);
                let mut edited = call;
                edited.arguments = arguments;
                PendingApprovalResolution::Edited(edited)
            }
            ApprovalDecision::Deny { reason } => {
                let _ = self.security.check_set.resolve_approval(eligible, false);
                PendingApprovalResolution::Rejected(error_block(
                    &call,
                    format!("approval declined: {reason}"),
                    self.output_limit,
                ))
            }
            ApprovalDecision::TimedOut => {
                let _ = self.security.check_set.resolve_approval(eligible, false);
                PendingApprovalResolution::Rejected(error_block(
                    &call,
                    "approval timed out",
                    self.output_limit,
                ))
            }
            ApprovalDecision::Cancelled => {
                let _ = self.security.check_set.resolve_approval(eligible, false);
                PendingApprovalResolution::Rejected(error_block(
                    &call,
                    "approval cancelled",
                    self.output_limit,
                ))
            }
            ApprovalDecision::Unavailable { reason } => {
                let _ = self.security.check_set.resolve_approval(eligible, false);
                PendingApprovalResolution::Rejected(error_block(
                    &call,
                    format!("approval unavailable: {reason}"),
                    self.output_limit,
                ))
            }
        }
    }

    async fn prepare_one(
        &self,
        call: &ToolCall,
        request: &RequestId,
        session: &SessionId,
        turn: Option<&TurnId>,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> Result<ReadyToolCall, String> {
        let Some(tool) = self.registry.get(&call.name) else {
            return Err(format!("tool `{}` is not available", call.name));
        };
        let Some(spec) = self.registry.spec(&call.name).cloned() else {
            return Err(format!("tool `{}` is not available", call.name));
        };
        let mut arguments = call.arguments.clone();
        let mut edit_count = 0usize;

        loop {
            self.ensure_active(cancel, deadline, "before tool preparation")?;
            self.registry
                .validate_arguments(&call.name, &arguments)
                .map_err(|error| error.message)?;

            let preparation_context = PreparationContext {
                session: session.clone(),
                turn: turn.cloned(),
                call_id: call.id.clone(),
                request: request.clone(),
                workspace: self.workspace.clone(),
                clock: self.clock.clone(),
                cancel: cancel.child(),
                deadline,
            };
            let preparation = tokio::select! {
                biased;
                _ = preparation_context.cancel.cancelled() => {
                    return Err("cancelled while tool was being prepared".into());
                }
                _ = wait_for_deadline(deadline, self.clock.clone()) => {
                    return Err("deadline elapsed while tool was being prepared".into());
                }
                result = tool.prepare(arguments, &preparation_context) => result,
            };
            let prepared = preparation.map_err(|error| error.message)?;
            self.verify_prepared(call, &spec, &prepared)?;
            self.enforce_workspace(&prepared)?;

            if prepared.required_permissions().is_empty() {
                return Ok(ReadyToolCall {
                    call: call.clone(),
                    tool,
                    prepared,
                    session: session.clone(),
                    turn: turn.cloned(),
                });
            }

            let context = SecurityContext::new(
                self.security.subject.clone(),
                session.clone(),
                self.security.tenant.clone(),
                self.security.check_set.revision().clone(),
            );
            // No content-guard system is wired into the executor yet, so the
            // least-trusted non-extension class remains conservative. The
            // evidence fingerprint now binds the exact prepared action.
            let evidence =
                SecurityEvidence::new(TrustClass::ExternalContent, prepared.fingerprint().clone());
            let auth_request = AuthorizationRequest::new(
                context,
                SecurityAction::new(format!("tool.{}", call.name)),
                prepared.resource().clone(),
                prepared.required_permissions().clone(),
                deadline,
                evidence,
            );
            let outcome = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    return Err("cancelled while tool was being authorized".into());
                }
                _ = wait_for_deadline(deadline, self.clock.clone()) => {
                    return Err("deadline elapsed while tool was being authorized".into());
                }
                outcome = self.security.check_set.authorize(&auth_request, cancel) => outcome,
            };
            match outcome.decision {
                AuthorizationDecision::Deny { code } => {
                    return Err(format!("authorization denied: {code}"));
                }
                AuthorizationDecision::RequireApproval { eligible } => {
                    let mut origin = ApprovalOrigin::new(session.clone(), request.clone());
                    if let Some(turn) = turn {
                        origin = origin.with_turn(turn.clone());
                    }
                    let approval_request = ApprovalRequest::new(prepared.clone(), deadline, origin);
                    let decision = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => ApprovalDecision::Cancelled,
                        _ = wait_for_deadline(deadline, self.clock.clone()) => {
                            ApprovalDecision::TimedOut
                        }
                        decision = self.approval.decide(&approval_request) => decision,
                    };
                    match decision {
                        ApprovalDecision::Allow => {
                            match self.security.check_set.resolve_approval(eligible, true) {
                                AuthorizationDecision::Allow { grant: _ } => {
                                    return Ok(ReadyToolCall {
                                        call: call.clone(),
                                        tool,
                                        prepared,
                                        session: session.clone(),
                                        turn: turn.cloned(),
                                    });
                                }
                                _ => {
                                    unreachable!("resolve_approval(_, true) always returns Allow")
                                }
                            }
                        }
                        ApprovalDecision::Edit { arguments: edited } => {
                            let _ = self.security.check_set.resolve_approval(eligible, false);
                            edit_count = edit_count.saturating_add(1);
                            if edit_count > Self::MAX_APPROVAL_EDITS {
                                return Err(
                                    "approval denied: too many edited action proposals".into()
                                );
                            }
                            arguments = edited;
                            continue;
                        }
                        ApprovalDecision::Deny { reason } => {
                            let _ = self.security.check_set.resolve_approval(eligible, false);
                            return Err(format!("approval declined: {reason}"));
                        }
                        ApprovalDecision::TimedOut => {
                            let _ = self.security.check_set.resolve_approval(eligible, false);
                            return Err("approval timed out".into());
                        }
                        ApprovalDecision::Cancelled => {
                            let _ = self.security.check_set.resolve_approval(eligible, false);
                            return Err("approval cancelled".into());
                        }
                        ApprovalDecision::Unavailable { reason } => {
                            let _ = self.security.check_set.resolve_approval(eligible, false);
                            return Err(format!("approval unavailable: {reason}"));
                        }
                    }
                }
                AuthorizationDecision::Allow { grant: _ } => {
                    if self.escapes_workspace(&prepared) {
                        return Err(UNATTENDED_ESCAPE.into());
                    }
                    return Ok(ReadyToolCall {
                        call: call.clone(),
                        tool,
                        prepared,
                        session: session.clone(),
                        turn: turn.cloned(),
                    });
                }
            }
        }
    }

    fn ensure_active(
        &self,
        cancel: &Cancellation,
        deadline: Deadline,
        phase: &str,
    ) -> Result<(), String> {
        if cancel.is_cancelled() {
            return Err(format!("cancelled {phase}"));
        }
        if deadline.is_expired(self.clock.as_ref()) {
            return Err(format!("deadline elapsed {phase}"));
        }
        Ok(())
    }

    fn verify_prepared(
        &self,
        call: &ToolCall,
        spec: &ToolSpec,
        prepared: &PreparedToolCall,
    ) -> Result<(), String> {
        if prepared.call_id() != &call.id {
            return Err("prepared call id does not match the provider call".into());
        }
        if prepared.tool() != call.name || prepared.tool() != spec.name {
            return Err("prepared tool name does not match the registered tool".into());
        }
        if !prepared.verify_fingerprint() {
            return Err("prepared action fingerprint mismatch".into());
        }
        if !prepared
            .required_permissions()
            .is_subset(&spec.permission_upper_bound)
        {
            return Err("prepared permissions exceed the tool descriptor upper bound".into());
        }
        let implied = prepared.effects().permission_upper_bound();
        if !implied.is_subset(prepared.required_permissions()) {
            return Err("prepared effects exercise undeclared permissions".into());
        }
        let permissions = prepared.required_permissions();
        if permissions.contains(&agent_runtime_registry::Permission::FsRead)
            && !prepared.effects().has_read()
        {
            return Err("prepared fs.read permission has no matching read effect".into());
        }
        if permissions.iter().any(|permission| {
            matches!(
                permission,
                agent_runtime_registry::Permission::FsWrite
                    | agent_runtime_registry::Permission::FsCreate
                    | agent_runtime_registry::Permission::FsDelete
            )
        }) && prepared.effects().write_scopes().next().is_none()
        {
            return Err(
                "prepared filesystem mutation permission has no matching write effect".into(),
            );
        }
        if permissions.contains(&agent_runtime_registry::Permission::ProcessSpawn)
            && !prepared.effects().spawns_process()
        {
            return Err("prepared process.spawn permission has no matching spawn effect".into());
        }
        if permissions.iter().any(|permission| {
            matches!(
                permission,
                agent_runtime_registry::Permission::NetHttp
                    | agent_runtime_registry::Permission::DataEgress
            )
        }) && !prepared.effects().has_network()
        {
            return Err("prepared network permission has no matching network effect".into());
        }
        Ok(())
    }

    fn enforce_workspace(&self, prepared: &PreparedToolCall) -> Result<(), String> {
        for scope in prepared.effects().write_scopes() {
            let SecurityResource::Filesystem { mount, .. } = prepared.resource() else {
                return Err(
                    "prepared filesystem write effect requires a filesystem resource".into(),
                );
            };
            if mount != self.workspace.root() {
                // An out-of-workspace write never runs unattended (see
                // `escapes_workspace`), but even an approved one stays bound
                // to the exact resource the approval reviewed: the scope must
                // sit under the resource mount and inside its segments.
                let trimmed = mount.trim_end_matches('/');
                let relative = match scope.as_str().strip_prefix(trimmed) {
                    Some(rest) if rest.is_empty() || rest.starts_with('/') => rest,
                    _ => {
                        return Err(format!(
                            "prepared write scope `{}` is outside its resource mount `{mount}`",
                            scope.as_str()
                        ));
                    }
                };
                let scope_resource = SecurityResource::filesystem(
                    mount.clone(),
                    relative
                        .split('/')
                        .filter(|segment| !segment.is_empty())
                        .map(str::to_owned)
                        .collect(),
                );
                if !prepared.resource().contains(&scope_resource) {
                    return Err(format!(
                        "prepared write scope `{}` is not covered by the authorized resource",
                        scope.as_str()
                    ));
                }
                continue;
            }
            if !self.workspace.contains(scope.as_str()) {
                return Err(format!(
                    "workspace violation: `{}` is outside `{}`",
                    scope.as_str(),
                    self.workspace.root()
                ));
            }
            let relative = scope
                .as_str()
                .strip_prefix(self.workspace.root())
                .unwrap_or(scope.as_str());
            let scope_resource = SecurityResource::filesystem(
                self.workspace.root(),
                relative
                    .split('/')
                    .filter(|segment| !segment.is_empty())
                    .map(str::to_owned)
                    .collect(),
            );
            if !prepared.resource().contains(&scope_resource) {
                return Err(format!(
                    "prepared write scope `{}` is not covered by the authorized resource",
                    scope.as_str()
                ));
            }
        }

        let filesystem_authority = prepared.required_permissions().iter().any(|permission| {
            matches!(
                permission,
                agent_runtime_registry::Permission::FsRead
                    | agent_runtime_registry::Permission::FsWrite
                    | agent_runtime_registry::Permission::FsCreate
                    | agent_runtime_registry::Permission::FsDelete
            )
        });
        if filesystem_authority {
            let SecurityResource::Filesystem { mount, segments } = prepared.resource() else {
                return Err("prepared filesystem permission requires a filesystem resource".into());
            };
            if segments.iter().any(|segment| {
                segment.is_empty()
                    || segment == "."
                    || segment == ".."
                    || segment.contains('/')
                    || segment.contains('\\')
            }) {
                return Err("prepared filesystem resource is not structurally canonical".into());
            }
            if mount == self.workspace.root() && !segments.is_empty() {
                let path = format!("{}/{}", mount.trim_end_matches('/'), segments.join("/"));
                if !self.workspace.contains(&path) {
                    return Err(format!(
                        "workspace violation: `{path}` is outside `{}`",
                        self.workspace.root()
                    ));
                }
            }
            // A resource on any other mount is not rejected here: it is an
            // out-of-workspace claim, and `escapes_workspace` pins it to an
            // explicit approval decision instead of an unattended allow.
        }
        Ok(())
    }

    /// Whether the prepared action claims filesystem authority on a resource
    /// mounted outside the session workspace.
    ///
    /// Such an action is never allowed unattended: even when the composed
    /// checks answer `Allow`, the executor refuses it unless the decision
    /// came through an approval. The boundary stays fail-closed while a host
    /// that wants "ask the user" semantics for escapes gets exactly that.
    fn escapes_workspace(&self, prepared: &PreparedToolCall) -> bool {
        let filesystem_authority = prepared.required_permissions().iter().any(|permission| {
            matches!(
                permission,
                agent_runtime_registry::Permission::FsRead
                    | agent_runtime_registry::Permission::FsWrite
                    | agent_runtime_registry::Permission::FsCreate
                    | agent_runtime_registry::Permission::FsDelete
            )
        });
        filesystem_authority
            && matches!(
                prepared.resource(),
                SecurityResource::Filesystem { mount, .. } if mount != self.workspace.root()
            )
    }

    pub(crate) async fn invoke_one(
        &self,
        ready: ReadyToolCall,
        request: &RequestId,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> ToolResultBlock {
        let raw = self.invoke_one_raw(ready, request, cancel, deadline).await;
        raw.outcome
            .into_result_block(raw.call.id, raw.call.name, self.output_limit)
    }

    /// Invokes one exact prepared action and returns its unbounded,
    /// serializable outcome. The turn machine checkpoints this value before
    /// calling any fallible harness processor.
    pub(crate) async fn invoke_one_raw(
        &self,
        ready: ReadyToolCall,
        request: &RequestId,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> RawToolResult {
        let ReadyToolCall {
            call,
            tool,
            prepared,
            session,
            turn,
        } = ready;
        if !prepared.verify_fingerprint() {
            return RawToolResult {
                call,
                outcome: ToolOutcome::error(
                    "prepared action fingerprint mismatch before invocation",
                ),
            };
        }
        if let Err(message) = self.ensure_active(cancel, deadline, "before tool ran") {
            return RawToolResult {
                call,
                outcome: ToolOutcome::error(message),
            };
        }

        let ctx = InvocationContext {
            session,
            turn,
            call_id: call.id.clone(),
            request: request.clone(),
            workspace: self.workspace.clone(),
            clock: self.clock.clone(),
            cancel: cancel.child(),
            deadline,
            output_limit: self.output_limit,
        };

        let outcome = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => {
                Err(agent_runtime_core::error::RuntimeError::cancelled(
                    "cancelled while tool was running",
                ))
            }
            _ = wait_for_deadline(deadline, self.clock.clone()) => {
                Err(agent_runtime_core::error::RuntimeError::tool(
                    "deadline elapsed while tool was running",
                ))
            }
            result = tool.invoke(prepared, &ctx) => result,
        };

        RawToolResult {
            call,
            outcome: outcome.unwrap_or_else(|error| ToolOutcome::error(error.message)),
        }
    }
}

/// Exact unbounded output associated with its canonical source call.
pub(crate) struct RawToolResult {
    pub(crate) call: ToolCall,
    pub(crate) outcome: ToolOutcome,
}

#[derive(Debug)]
pub(crate) struct ReadyToolCall {
    pub(crate) call: ToolCall,
    pub(crate) tool: Arc<dyn Tool>,
    pub(crate) prepared: PreparedToolCall,
    pub(crate) session: SessionId,
    pub(crate) turn: Option<TurnId>,
}

/// Prepared/authorized calls plus deterministic preparation failures.
pub(crate) struct PreparedToolBatch {
    pub(crate) calls: Vec<ToolCall>,
    pub(crate) ready: Vec<Option<ReadyToolCall>>,
    pub(crate) results: Vec<Option<ToolResultBlock>>,
    pub(crate) effects: Vec<ToolEffects>,
}

impl PreparedToolBatch {
    /// Exact prepared or deterministic-result disposition of every source
    /// call, in provider request order.
    pub(crate) fn checkpoint_slots(
        &self,
    ) -> Result<Vec<ToolSlotCheckpoint>, agent_runtime_core::error::RuntimeError> {
        self.ready
            .iter()
            .zip(&self.results)
            .map(|(ready, result)| match (ready, result) {
                (Some(ready), None) => Ok(ToolSlotCheckpoint::Prepared(ready.prepared.clone())),
                (None, Some(result)) => Ok(ToolSlotCheckpoint::CanonicalResult(result.clone())),
                (Some(_), Some(_)) => Err(agent_runtime_core::error::RuntimeError::internal(
                    "prepared tool batch contains both an action and a result",
                )),
                (None, None) => Err(agent_runtime_core::error::RuntimeError::internal(
                    "prepared tool batch contains an empty source slot",
                )),
            })
            .collect()
    }
}

async fn wait_for_deadline(deadline: Deadline, clock: Arc<dyn Clock>) {
    loop {
        match deadline.remaining_millis(clock.as_ref()) {
            Some(0) => return,
            Some(ms) => {
                tokio::time::sleep(Duration::from_millis(ms.min(25))).await;
            }
            None => pending::<()>().await,
        }
    }
}

pub(crate) fn error_block(
    call: &ToolCall,
    message: impl Into<String>,
    output_limit: usize,
) -> ToolResultBlock {
    agent_runtime_core::tool::ToolOutcome::error(message).into_result_block(
        call.id.clone(),
        call.name.clone(),
        output_limit,
    )
}

#[cfg(test)]
mod tests;
