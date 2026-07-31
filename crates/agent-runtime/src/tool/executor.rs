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

    async fn execute_with_turn(
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
        request: &RequestId,
        session: &SessionId,
        cancel: &Cancellation,
        deadline: Deadline,
    ) -> PreparedAuthorization {
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
            turn: None,
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
                turn: None,
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
                PreparedAuthorization::Ready(ReadyToolCall {
                    call: authoritative_call,
                    tool,
                    prepared,
                    session: session.clone(),
                    turn: None,
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
                turn: None,
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
                PreparedAuthorization::Ready(ReadyToolCall {
                    call,
                    tool,
                    prepared,
                    session: session.clone(),
                    turn: None,
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
            if !self.workspace.contains(scope.as_str()) {
                return Err(format!(
                    "workspace violation: `{}` is outside `{}`",
                    scope.as_str(),
                    self.workspace.root()
                ));
            }
            let SecurityResource::Filesystem { .. } = prepared.resource() else {
                return Err(
                    "prepared filesystem write effect requires a filesystem resource".into(),
                );
            };
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
            if mount != self.workspace.root()
                || segments.iter().any(|segment| {
                    segment.is_empty()
                        || segment == "."
                        || segment == ".."
                        || segment.contains('/')
                        || segment.contains('\\')
                })
            {
                return Err("prepared filesystem resource is outside the workspace".into());
            }
            if !segments.is_empty() {
                let path = format!("{}/{}", mount.trim_end_matches('/'), segments.join("/"));
                if !self.workspace.contains(&path) {
                    return Err(format!(
                        "workspace violation: `{path}` is outside `{}`",
                        self.workspace.root()
                    ));
                }
            }
        }
        Ok(())
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
mod tests {
    use super::*;
    use crate::tool::registry::ToolRegistry;
    use agent_runtime_core::approval::{AllowAll, DenyAll, UnavailableApproval};
    use agent_runtime_core::check_set::{ActionClass, EnforcementLimits, SecurityCheckSetBuilder};
    use agent_runtime_core::clock::SystemClock;
    use agent_runtime_core::compat::LegacyApprovalAuthority;
    use agent_runtime_core::error::RuntimeError;
    use agent_runtime_core::grant::{
        DecisionCode, GrantConstraints, SecurityCheck, SecurityCheckId, SecurityCheckMode,
        SecurityCheckOutcome, SecurityCheckRevision,
    };
    use agent_runtime_core::ids::ToolCallId;
    use agent_runtime_core::security::{PermissionSet, SecurityResource};
    use agent_runtime_core::tool::{LegacyTool, ToolCallDisplay, ToolOutcome};
    use agent_runtime_core::workspace::Workspace;
    use agent_runtime_registry::Permission;
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Debug)]
    struct EchoTool;
    #[async_trait]
    impl LegacyTool for EchoTool {
        fn name(&self) -> &str {
            "echo"
        }
        fn description(&self) -> &str {
            "echoes"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![])
        }
        async fn invoke_legacy(
            &self,
            arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::json(arguments))
        }
    }

    #[derive(Debug)]
    struct NetworkTool;
    #[async_trait]
    impl LegacyTool for NetworkTool {
        fn name(&self) -> &str {
            "network"
        }
        fn description(&self) -> &str {
            "performs network I/O"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![]).with_network()
        }
        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("fetched"))
        }
    }

    /// Ignores `should_stop()` entirely; only the executor's cancel/deadline
    /// preemption can end it.
    #[derive(Debug)]
    struct HangingTool;
    #[async_trait]
    impl LegacyTool for HangingTool {
        fn name(&self) -> &str {
            "hang"
        }
        fn description(&self) -> &str {
            "never returns"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![])
        }
        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            unreachable!("preempted before the sleep elapses")
        }
    }

    #[derive(Debug)]
    struct WriteTool;
    #[async_trait]
    impl LegacyTool for WriteTool {
        fn name(&self) -> &str {
            "write"
        }
        fn description(&self) -> &str {
            "writes"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![]).with_write("/ws/file")
        }
        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("wrote"))
        }
    }

    #[derive(Debug)]
    struct LargeErrorTool;

    #[async_trait]
    impl LegacyTool for LargeErrorTool {
        fn name(&self) -> &str {
            "large_error"
        }
        fn description(&self) -> &str {
            "fails with a large diagnostic"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![])
        }
        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Err(RuntimeError::tool("x".repeat(10_000)))
        }
    }

    #[derive(Debug)]
    struct WsRoot;
    impl Workspace for WsRoot {
        fn root(&self) -> &str {
            "/ws"
        }
        fn contains(&self, path: &str) -> bool {
            path.starts_with("/ws/")
        }
    }

    /// Denies whichever `permission` it is registered to cover; `NotApplicable`
    /// otherwise.
    #[derive(Debug)]
    struct DenyingCheck {
        id: SecurityCheckId,
        revision: SecurityCheckRevision,
        permission: Permission,
    }

    impl DenyingCheck {
        fn new(permission: Permission) -> Arc<Self> {
            Arc::new(Self {
                id: SecurityCheckId::new("denying"),
                revision: SecurityCheckRevision::new("v1"),
                permission,
            })
        }
    }

    #[async_trait]
    impl SecurityCheck for DenyingCheck {
        fn id(&self) -> &SecurityCheckId {
            &self.id
        }
        fn revision(&self) -> &SecurityCheckRevision {
            &self.revision
        }
        async fn evaluate(
            &self,
            request: &AuthorizationRequest,
            _cancel: &Cancellation,
        ) -> SecurityCheckOutcome {
            if request.requested.contains(&self.permission) {
                SecurityCheckOutcome::Deny {
                    code: DecisionCode::other("blocked"),
                }
            } else {
                SecurityCheckOutcome::NotApplicable
            }
        }
    }

    /// Denies an `fs.write` request only when its resource carries
    /// `forbidden_segment`; `NotApplicable` otherwise. Registered as
    /// `RequiredConstraint` so its `Deny` is enforcing without itself
    /// satisfying coverage.
    #[derive(Debug)]
    struct ScopedDenyingCheck {
        id: SecurityCheckId,
        revision: SecurityCheckRevision,
        forbidden_segment: String,
    }

    #[async_trait]
    impl SecurityCheck for ScopedDenyingCheck {
        fn id(&self) -> &SecurityCheckId {
            &self.id
        }
        fn revision(&self) -> &SecurityCheckRevision {
            &self.revision
        }
        async fn evaluate(
            &self,
            request: &AuthorizationRequest,
            _cancel: &Cancellation,
        ) -> SecurityCheckOutcome {
            let denies = match &request.resource {
                SecurityResource::Filesystem { segments, .. } => segments
                    .iter()
                    .any(|segment| segment == &self.forbidden_segment),
                _ => false,
            };
            if denies {
                SecurityCheckOutcome::Deny {
                    code: DecisionCode::other("scoped-deny"),
                }
            } else {
                SecurityCheckOutcome::NotApplicable
            }
        }
    }

    #[derive(Debug)]
    struct TrackingApproval {
        called: Arc<AtomicBool>,
    }
    #[async_trait]
    impl ApprovalPolicy for TrackingApproval {
        async fn decide(&self, _request: &ApprovalRequest) -> ApprovalDecision {
            self.called.store(true, Ordering::SeqCst);
            ApprovalDecision::Allow
        }
    }

    #[derive(Debug)]
    struct TrackingWriteTool {
        invoked: Arc<AtomicBool>,
    }
    #[async_trait]
    impl LegacyTool for TrackingWriteTool {
        fn name(&self) -> &str {
            "tracked_write"
        }
        fn description(&self) -> &str {
            "writes, tracking whether it ran"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![]).with_write("/ws/tracked")
        }
        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            self.invoked.store(true, Ordering::SeqCst);
            Ok(ToolOutcome::text("wrote"))
        }
    }

    #[derive(Debug)]
    struct TrackingNetworkTool {
        invoked: Arc<AtomicBool>,
    }
    #[async_trait]
    impl LegacyTool for TrackingNetworkTool {
        fn name(&self) -> &str {
            "tracked_network"
        }
        fn description(&self) -> &str {
            "performs network I/O, tracking whether it ran"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![]).with_network()
        }
        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            self.invoked.store(true, Ordering::SeqCst);
            Ok(ToolOutcome::text("fetched"))
        }
    }

    #[derive(Debug)]
    struct WriteOkTool;
    #[async_trait]
    impl LegacyTool for WriteOkTool {
        fn name(&self) -> &str {
            "write_ok"
        }
        fn description(&self) -> &str {
            "writes to an allowed path"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![]).with_write("/ws/ok")
        }
        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("wrote"))
        }
    }

    #[derive(Debug)]
    struct WriteForbiddenTool;
    #[async_trait]
    impl LegacyTool for WriteForbiddenTool {
        fn name(&self) -> &str {
            "write_forbidden"
        }
        fn description(&self) -> &str {
            "writes to a forbidden path"
        }
        fn input_schema(&self) -> Value {
            json!({"type":"object"})
        }
        fn effects(&self) -> ToolEffects {
            ToolEffects::new(vec![]).with_write("/ws/forbidden")
        }
        async fn invoke_legacy(
            &self,
            _arguments: Value,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            Ok(ToolOutcome::text("wrote"))
        }
    }

    #[derive(Debug)]
    struct ExactEditTool {
        invoked_paths: Arc<Mutex<Vec<String>>>,
    }

    impl ExactEditTool {
        fn new(invoked_paths: Arc<Mutex<Vec<String>>>) -> Self {
            Self { invoked_paths }
        }
    }

    #[async_trait]
    impl Tool for ExactEditTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec::new(
                "exact_edit",
                "edits one exact workspace file",
                json!({
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                    "additionalProperties": false
                }),
                ToolEffects::new(vec![]).with_write("/ws"),
            )
        }

        async fn prepare(
            &self,
            mut arguments: Value,
            ctx: &PreparationContext,
        ) -> Result<PreparedToolCall, RuntimeError> {
            let relative = arguments
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| RuntimeError::tool("path is required"))?;
            if relative.starts_with('/')
                || relative
                    .split('/')
                    .any(|segment| segment.is_empty() || segment == "." || segment == "..")
            {
                return Err(RuntimeError::tool("path must be a canonical relative path"));
            }
            let segments = relative.split('/').map(str::to_owned).collect::<Vec<_>>();
            let canonical = format!(
                "{}/{}",
                ctx.workspace.root().trim_end_matches('/'),
                segments.join("/")
            );
            arguments["path"] = Value::String(canonical.clone());
            Ok(PreparedToolCall::new(
                ctx.call_id.clone(),
                "exact_edit",
                arguments,
                PermissionSet::single(Permission::FsWrite),
                SecurityResource::filesystem(ctx.workspace.root(), segments),
                ToolEffects::new(vec![]).with_write(canonical.clone()),
                ToolCallDisplay::new("Edit workspace file").with_detail(canonical),
            ))
        }

        async fn invoke(
            &self,
            prepared: PreparedToolCall,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            let path = prepared
                .arguments()
                .get("path")
                .and_then(Value::as_str)
                .expect("prepared edit path")
                .to_owned();
            self.invoked_paths
                .lock()
                .expect("invoked paths poisoned")
                .push(path.clone());
            Ok(ToolOutcome::text(path))
        }
    }

    #[derive(Debug)]
    struct RecordingApprovalCheck {
        id: SecurityCheckId,
        revision: SecurityCheckRevision,
        resources: Arc<Mutex<Vec<SecurityResource>>>,
    }

    #[async_trait]
    impl SecurityCheck for RecordingApprovalCheck {
        fn id(&self) -> &SecurityCheckId {
            &self.id
        }

        fn revision(&self) -> &SecurityCheckRevision {
            &self.revision
        }

        async fn evaluate(
            &self,
            request: &AuthorizationRequest,
            _cancel: &Cancellation,
        ) -> SecurityCheckOutcome {
            self.resources
                .lock()
                .expect("resources poisoned")
                .push(request.resource.clone());
            SecurityCheckOutcome::RequireApproval {
                constraints: GrantConstraints::unconstrained(),
            }
        }
    }

    #[derive(Debug)]
    struct EditThenAllow {
        calls: AtomicUsize,
        seen: Arc<Mutex<Vec<PreparedToolCall>>>,
        edited_arguments: Value,
    }

    #[async_trait]
    impl ApprovalPolicy for EditThenAllow {
        async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
            self.seen
                .lock()
                .expect("approval observations poisoned")
                .push(request.prepared().clone());
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                ApprovalDecision::Edit {
                    arguments: self.edited_arguments.clone(),
                }
            } else {
                ApprovalDecision::Allow
            }
        }
    }

    #[derive(Debug)]
    struct HangingApproval;

    #[async_trait]
    impl ApprovalPolicy for HangingApproval {
        async fn decide(&self, _request: &ApprovalRequest) -> ApprovalDecision {
            std::future::pending().await
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum InvalidPreparation {
        TamperedFingerprint,
        ExceedsPermissionBound,
        MissingWriteEffect,
        MismatchedWriteResource,
    }

    #[derive(Debug)]
    struct InvalidPreparedTool {
        mode: InvalidPreparation,
        invoked: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Tool for InvalidPreparedTool {
        fn spec(&self) -> ToolSpec {
            let effects = match self.mode {
                InvalidPreparation::TamperedFingerprint => ToolEffects::new(vec![]),
                InvalidPreparation::ExceedsPermissionBound => ToolEffects::read_only(),
                InvalidPreparation::MissingWriteEffect
                | InvalidPreparation::MismatchedWriteResource => {
                    ToolEffects::new(vec![]).with_write("/ws")
                }
            };
            ToolSpec::new(
                "invalid_prepared",
                "returns an invalid prepared action",
                json!({"type":"object"}),
                effects,
            )
        }

        async fn prepare(
            &self,
            arguments: Value,
            ctx: &PreparationContext,
        ) -> Result<PreparedToolCall, RuntimeError> {
            let prepared = match self.mode {
                InvalidPreparation::TamperedFingerprint => PreparedToolCall::new(
                    ctx.call_id.clone(),
                    "invalid_prepared",
                    arguments,
                    PermissionSet::new(),
                    SecurityResource::other("tool", "invalid_prepared"),
                    ToolEffects::new(vec![]),
                    ToolCallDisplay::new("Invalid"),
                ),
                InvalidPreparation::ExceedsPermissionBound => PreparedToolCall::new(
                    ctx.call_id.clone(),
                    "invalid_prepared",
                    arguments,
                    PermissionSet::single(Permission::NetHttp),
                    SecurityResource::network("https://example.test", "GET", Vec::new()),
                    ToolEffects::new(vec![]).with_network(),
                    ToolCallDisplay::new("Invalid"),
                ),
                InvalidPreparation::MissingWriteEffect => PreparedToolCall::new(
                    ctx.call_id.clone(),
                    "invalid_prepared",
                    arguments,
                    PermissionSet::single(Permission::FsWrite),
                    SecurityResource::filesystem("/ws", vec!["target".into()]),
                    ToolEffects::new(vec![]),
                    ToolCallDisplay::new("Invalid"),
                ),
                InvalidPreparation::MismatchedWriteResource => PreparedToolCall::new(
                    ctx.call_id.clone(),
                    "invalid_prepared",
                    arguments,
                    PermissionSet::single(Permission::FsWrite),
                    SecurityResource::filesystem("/ws", vec!["authorized".into()]),
                    ToolEffects::new(vec![]).with_write("/ws/executed"),
                    ToolCallDisplay::new("Invalid"),
                ),
            };
            if matches!(self.mode, InvalidPreparation::TamperedFingerprint) {
                let mut serialized = serde_json::to_value(prepared).unwrap();
                serialized["canonical_arguments"] = json!({"tampered": true});
                Ok(serde_json::from_value(serialized).unwrap())
            } else {
                Ok(prepared)
            }
        }

        async fn invoke(
            &self,
            _prepared: PreparedToolCall,
            _ctx: &InvocationContext,
        ) -> Result<ToolOutcome, RuntimeError> {
            self.invoked.store(true, Ordering::SeqCst);
            Ok(ToolOutcome::text("must not run"))
        }
    }

    fn registry() -> SealedToolRegistry {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(EchoTool)).unwrap();
        reg.register(Arc::new(WriteTool)).unwrap();
        reg.register(Arc::new(LargeErrorTool)).unwrap();
        reg.register(Arc::new(NetworkTool)).unwrap();
        reg.register(Arc::new(HangingTool)).unwrap();
        reg.seal()
    }

    fn call(name: &str, id: &str, args: Value) -> ToolCall {
        ToolCall {
            id: ToolCallId::new(id),
            name: name.into(),
            arguments: args,
        }
    }

    /// No checks registered at all — used to prove a read-only tool's
    /// invocation never reaches authorization in the first place, not merely
    /// that some particular check happens to allow it.
    fn empty_security_config() -> SecurityConfig {
        let builder =
            SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
        SecurityConfig {
            check_set: Arc::new(builder.seal().unwrap()),
            subject: SecuritySubject::new("test-subject"),
            tenant: TenantId::new("test-tenant"),
        }
    }

    /// Registers only [`LegacyApprovalAuthority`] — reproduces the migration
    /// posture: workspace reads pass authoritative policy without HITL, while
    /// mutating/spawning/network invocations require approval.
    fn security_config() -> SecurityConfig {
        let mut builder =
            SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
        let compat = Arc::new(LegacyApprovalAuthority::new());
        builder.register(
            compat.clone(),
            SecurityCheckMode::Authoritative,
            compat.coverage().clone(),
            ActionClass::new("test"),
        );
        SecurityConfig {
            check_set: Arc::new(builder.seal().unwrap()),
            subject: SecuritySubject::new("test-subject"),
            tenant: TenantId::new("test-tenant"),
        }
    }

    /// Registers one [`DenyingCheck`] covering `permission`, authoritatively.
    fn denying_security_config(permission: Permission) -> SecurityConfig {
        let mut builder =
            SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
        builder.register(
            DenyingCheck::new(permission.clone()),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(permission),
            ActionClass::new("test"),
        );
        SecurityConfig {
            check_set: Arc::new(builder.seal().unwrap()),
            subject: SecuritySubject::new("test-subject"),
            tenant: TenantId::new("test-tenant"),
        }
    }

    fn recording_approval_security(resources: Arc<Mutex<Vec<SecurityResource>>>) -> SecurityConfig {
        let mut builder =
            SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
        builder.register(
            Arc::new(RecordingApprovalCheck {
                id: SecurityCheckId::new("recording-approval"),
                revision: SecurityCheckRevision::new("v1"),
                resources,
            }),
            SecurityCheckMode::Authoritative,
            PermissionSet::single(Permission::FsWrite),
            ActionClass::new("test"),
        );
        SecurityConfig {
            check_set: Arc::new(builder.seal().unwrap()),
            subject: SecuritySubject::new("test-subject"),
            tenant: TenantId::new("test-tenant"),
        }
    }

    fn exact_edit_registry(invoked_paths: Arc<Mutex<Vec<String>>>) -> SealedToolRegistry {
        let mut registry = ToolRegistry::new();
        registry
            .register(Arc::new(ExactEditTool::new(invoked_paths)))
            .unwrap();
        registry.seal()
    }

    #[tokio::test]
    async fn authority_free_tool_runs_without_approval() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(DenyAll), // authority-free work never reaches approval
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            empty_security_config(),
        );
        let calls = vec![call("echo", "c1", json!({"x":1}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert_eq!(out.len(), 1);
        assert!(!out[0].is_error);
    }

    #[tokio::test]
    async fn mutating_tool_is_denied_fail_closed() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(DenyAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("write", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("approval declined")
        );
    }

    #[tokio::test]
    async fn mutating_tool_runs_when_allowed_and_in_workspace() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("write", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(!out[0].is_error);
    }

    #[tokio::test]
    async fn unknown_tool_becomes_error_result() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("missing", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("not available")
        );
    }

    #[tokio::test]
    async fn tool_runtime_errors_are_output_bounded() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            20,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("large_error", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        let text = out[0].content[0].as_text().unwrap();
        assert!(out[0].is_error);
        assert_eq!(text.chars().count(), 20);
    }

    #[tokio::test]
    async fn network_only_tool_requires_approval() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(DenyAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("network", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("approval declined")
        );
    }

    #[tokio::test]
    async fn hanging_tool_that_ignores_should_stop_is_terminated_at_deadline() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("hang", "c1", json!({}))];
        let out = tokio::time::timeout(
            Duration::from_millis(2_000),
            ex.execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::after(&SystemClock, 30),
            ),
        )
        .await
        .expect("deadline must preempt a tool that ignores should_stop()");
        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("deadline elapsed")
        );
    }

    #[tokio::test]
    async fn hanging_tool_that_ignores_cancellation_is_terminated_on_cancel() {
        let ex = ToolExecutor::new(
            registry(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security_config(),
        );
        let calls = vec![call("hang", "c1", json!({}))];
        let cancel = Cancellation::new();
        let request = RequestId::new("r");
        let session = SessionId::new("s1");
        let run = ex.execute(&calls, &request, &session, &cancel, Deadline::never());
        let trigger = async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cancel.cancel(agent_runtime_core::cancel::CancelReason::UserRequested);
        };
        let (out, ()) = tokio::time::timeout(Duration::from_millis(2_000), async {
            tokio::join!(run, trigger)
        })
        .await
        .expect("cancellation must preempt a tool that ignores should_stop()");
        assert!(out[0].is_error);
        assert!(out[0].content[0].as_text().unwrap().contains("cancelled"));
    }

    #[tokio::test]
    async fn authorization_denial_short_circuits_before_approval_and_the_tool_body() {
        let approval_called = Arc::new(AtomicBool::new(false));
        let invoked = Arc::new(AtomicBool::new(false));
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(TrackingWriteTool {
            invoked: invoked.clone(),
        }))
        .unwrap();

        let ex = ToolExecutor::new(
            reg.seal(),
            Arc::new(TrackingApproval {
                called: approval_called.clone(),
            }),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            denying_security_config(Permission::FsWrite),
        );
        let calls = vec![call("tracked_write", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;

        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("authorization denied")
        );
        assert!(
            !approval_called.load(Ordering::SeqCst),
            "approval must not be consulted after an authorization denial"
        );
        assert!(
            !invoked.load(Ordering::SeqCst),
            "the tool body must not run after an authorization denial"
        );
    }

    #[tokio::test]
    async fn authorization_runs_before_the_tool_body_for_a_network_only_tool() {
        let invoked = Arc::new(AtomicBool::new(false));
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(TrackingNetworkTool {
            invoked: invoked.clone(),
        }))
        .unwrap();

        let ex = ToolExecutor::new(
            reg.seal(),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            denying_security_config(Permission::NetHttp),
        );
        let calls = vec![call("tracked_network", "c1", json!({}))];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;

        assert!(out[0].is_error);
        assert!(
            out[0].content[0]
                .as_text()
                .unwrap()
                .contains("authorization denied")
        );
        assert!(
            !invoked.load(Ordering::SeqCst),
            "the tool body must not run when authorization denies a network-only tool"
        );
    }

    #[tokio::test]
    async fn approval_cannot_widen_authorization_beyond_the_composed_grant() {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(WriteOkTool)).unwrap();
        reg.register(Arc::new(WriteForbiddenTool)).unwrap();

        let mut builder =
            SecurityCheckSetBuilder::new(EnforcementLimits::default(), Arc::new(SystemClock));
        let compat = Arc::new(LegacyApprovalAuthority::new());
        builder.register(
            compat.clone(),
            SecurityCheckMode::Authoritative,
            compat.coverage().clone(),
            ActionClass::new("test"),
        );
        builder.register(
            Arc::new(ScopedDenyingCheck {
                id: SecurityCheckId::new("scoped-deny"),
                revision: SecurityCheckRevision::new("v1"),
                forbidden_segment: "forbidden".to_owned(),
            }),
            SecurityCheckMode::RequiredConstraint,
            PermissionSet::single(Permission::FsWrite),
            ActionClass::new("test"),
        );
        let security = SecurityConfig {
            check_set: Arc::new(builder.seal().unwrap()),
            subject: SecuritySubject::new("test-subject"),
            tenant: TenantId::new("test-tenant"),
        };

        let ex = ToolExecutor::new(
            reg.seal(),
            // Would allow anything it is asked about — proving the denial
            // below is not something a permissive approval could rescue.
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            security,
        );
        let calls = vec![
            call("write_ok", "c1", json!({})),
            call("write_forbidden", "c2", json!({})),
        ];
        let out = ex
            .execute(
                &calls,
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;

        assert!(
            !out[0].is_error,
            "the in-scope write must still succeed via approval"
        );
        assert!(out[1].is_error);
        assert!(
            out[1].content[0]
                .as_text()
                .unwrap()
                .contains("authorization denied"),
            "an unlimited approval policy must not widen authorization past the composed deny"
        );
    }

    #[tokio::test]
    async fn prepared_edit_authorizes_the_exact_canonical_path() {
        let invoked = Arc::new(Mutex::new(Vec::new()));
        let resources = Arc::new(Mutex::new(Vec::new()));
        let executor = ToolExecutor::new(
            exact_edit_registry(invoked.clone()),
            Arc::new(AllowAll),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            recording_approval_security(resources.clone()),
        );

        let output = executor
            .execute(
                &[call("exact_edit", "c1", json!({"path": "src/lib.rs"}))],
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;

        assert!(!output[0].is_error);
        assert_eq!(
            resources.lock().expect("resources poisoned").as_slice(),
            [SecurityResource::filesystem(
                "/ws",
                vec!["src".into(), "lib.rs".into()]
            )]
        );
        assert_eq!(
            invoked.lock().expect("invoked paths poisoned").as_slice(),
            ["/ws/src/lib.rs"]
        );
    }

    #[tokio::test]
    async fn edited_approval_revalidates_reprepares_and_reauthorizes() {
        let invoked = Arc::new(Mutex::new(Vec::new()));
        let resources = Arc::new(Mutex::new(Vec::new()));
        let approvals = Arc::new(Mutex::new(Vec::new()));
        let executor = ToolExecutor::new(
            exact_edit_registry(invoked.clone()),
            Arc::new(EditThenAllow {
                calls: AtomicUsize::new(0),
                seen: approvals.clone(),
                edited_arguments: json!({"path": "src/edited.rs"}),
            }),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            recording_approval_security(resources.clone()),
        );

        let output = executor
            .execute(
                &[call("exact_edit", "c1", json!({"path": "src/original.rs"}))],
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;

        assert!(!output[0].is_error);
        let observed = approvals.lock().expect("approval observations poisoned");
        assert_eq!(observed.len(), 2);
        assert_ne!(observed[0].fingerprint(), observed[1].fingerprint());
        assert_eq!(
            observed[0].resource(),
            &SecurityResource::filesystem("/ws", vec!["src".into(), "original.rs".into()])
        );
        assert_eq!(
            observed[1].resource(),
            &SecurityResource::filesystem("/ws", vec!["src".into(), "edited.rs".into()])
        );
        drop(observed);
        assert_eq!(
            resources.lock().expect("resources poisoned").as_slice(),
            [
                SecurityResource::filesystem("/ws", vec!["src".into(), "original.rs".into()]),
                SecurityResource::filesystem("/ws", vec!["src".into(), "edited.rs".into()])
            ]
        );
        assert_eq!(
            invoked.lock().expect("invoked paths poisoned").as_slice(),
            ["/ws/src/edited.rs"]
        );
    }

    #[tokio::test]
    async fn approval_observes_cancellation_and_deadline() {
        let invoked = Arc::new(Mutex::new(Vec::new()));
        let deadline_executor = ToolExecutor::new(
            exact_edit_registry(invoked.clone()),
            Arc::new(HangingApproval),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            recording_approval_security(Arc::new(Mutex::new(Vec::new()))),
        );
        let deadline_output = tokio::time::timeout(
            Duration::from_secs(2),
            deadline_executor.execute(
                &[call("exact_edit", "deadline", json!({"path": "a.rs"}))],
                &RequestId::new("r-deadline"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::after(&SystemClock, 30),
            ),
        )
        .await
        .expect("approval deadline must preempt an unresponsive policy");
        assert!(deadline_output[0].is_error);
        assert!(
            deadline_output[0].content[0]
                .as_text()
                .unwrap()
                .contains("approval timed out")
        );

        let cancel_executor = ToolExecutor::new(
            exact_edit_registry(invoked.clone()),
            Arc::new(HangingApproval),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            recording_approval_security(Arc::new(Mutex::new(Vec::new()))),
        );
        let cancel = Cancellation::new();
        let cancel_call = [call("exact_edit", "cancel", json!({"path": "b.rs"}))];
        let request = RequestId::new("r-cancel");
        let session = SessionId::new("s1");
        let run =
            cancel_executor.execute(&cancel_call, &request, &session, &cancel, Deadline::never());
        let trigger = async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cancel.cancel(agent_runtime_core::cancel::CancelReason::UserRequested);
        };
        let (cancel_output, ()) =
            tokio::time::timeout(Duration::from_secs(2), async { tokio::join!(run, trigger) })
                .await
                .expect("turn cancellation must preempt an unresponsive approval policy");
        assert!(cancel_output[0].is_error);
        assert!(
            cancel_output[0].content[0]
                .as_text()
                .unwrap()
                .contains("approval cancelled")
        );
        assert!(
            invoked.lock().expect("invoked paths poisoned").is_empty(),
            "neither timed-out nor cancelled approval may invoke the tool"
        );
    }

    #[tokio::test]
    async fn unavailable_approval_is_distinct_from_explicit_decline() {
        let invoked = Arc::new(Mutex::new(Vec::new()));
        let executor = ToolExecutor::new(
            exact_edit_registry(invoked.clone()),
            Arc::new(UnavailableApproval),
            Arc::new(WsRoot),
            Arc::new(SystemClock),
            10_000,
            ConflictPolicy::ScopeOverlap,
            recording_approval_security(Arc::new(Mutex::new(Vec::new()))),
        );
        let output = executor
            .execute(
                &[call("exact_edit", "c1", json!({"path": "src/lib.rs"}))],
                &RequestId::new("r"),
                &SessionId::new("s1"),
                &Cancellation::new(),
                Deadline::never(),
            )
            .await;
        assert!(output[0].is_error);
        assert!(
            output[0].content[0]
                .as_text()
                .unwrap()
                .contains("approval unavailable")
        );
        assert!(invoked.lock().expect("invoked paths poisoned").is_empty());
    }

    #[tokio::test]
    async fn invalid_prepared_authority_fails_closed_before_invocation() {
        for (mode, expected) in [
            (
                InvalidPreparation::TamperedFingerprint,
                "fingerprint mismatch",
            ),
            (
                InvalidPreparation::ExceedsPermissionBound,
                "exceed the tool descriptor upper bound",
            ),
            (
                InvalidPreparation::MissingWriteEffect,
                "no matching write effect",
            ),
            (
                InvalidPreparation::MismatchedWriteResource,
                "not covered by the authorized resource",
            ),
        ] {
            let invoked = Arc::new(AtomicBool::new(false));
            let mut registry = ToolRegistry::new();
            registry
                .register(Arc::new(InvalidPreparedTool {
                    mode,
                    invoked: invoked.clone(),
                }))
                .unwrap();
            let executor = ToolExecutor::new(
                registry.seal(),
                Arc::new(AllowAll),
                Arc::new(WsRoot),
                Arc::new(SystemClock),
                10_000,
                ConflictPolicy::ScopeOverlap,
                empty_security_config(),
            );

            let output = executor
                .execute(
                    &[call("invalid_prepared", "c1", json!({}))],
                    &RequestId::new("r"),
                    &SessionId::new("s1"),
                    &Cancellation::new(),
                    Deadline::never(),
                )
                .await;
            assert!(output[0].is_error, "{mode:?} must fail");
            assert!(
                output[0].content[0].as_text().unwrap().contains(expected),
                "{mode:?} returned {:?}",
                output[0].content
            );
            assert!(
                !invoked.load(Ordering::SeqCst),
                "{mode:?} must fail before invocation"
            );
        }
    }
}
