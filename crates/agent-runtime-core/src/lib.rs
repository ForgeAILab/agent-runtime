//! Host-neutral contracts for the shared agent runtime.
//!
//! `agent-runtime-core` owns the vocabulary every consumer and adapter agrees
//! on: neutral identifiers, messages and content, structured errors,
//! cancellation and deadlines, redaction-safe metadata, versioned events,
//! disjoint usage counters, the provider/tool streaming contract, and the host
//! adapter traits. It contains **no** consumer-domain types and **no** network,
//! configuration, or presentation code.
//!
//! # Host adapters
//!
//! A host embeds the runtime by injecting implementations of these traits:
//!
//! - [`provider::Provider`] — an LLM backend.
//! - [`tool::Tool`] — a callable tool.
//! - [`approval::ApprovalPolicy`] — the fail-closed approval gate.
//! - [`workspace::Workspace`] — the write boundary.
//! - [`store::SessionStore`] / [`store::SecretStore`] — persistence and secrets.
//! - [`observer::EventObserver`] — a synchronous event sink.
//! - [`clock::Clock`] — injectable time.
//! - [`grant::SecurityCheck`] — a registered security check.
//! - [`isolation::IsolationBackend`] — an engine-neutral untrusted-execution
//!   backend.
//! - [`broker::CredentialBroker`], [`broker::EgressBroker`],
//!   [`broker::FilesystemBroker`] — grant-mediated host operation brokers.
//! - [`guard::LeakDetector`], [`guard::ContentGuard`] — defense-in-depth
//!   leak detection and untrusted-content risk signals.
//!
//! The runtime itself, not a host adapter, owns
//! [`check_set::SecurityCheckSet`]: the sealed composer that evaluates every
//! registered [`grant::SecurityCheck`] for one [`security::AuthorizationRequest`]
//! and produces the composed [`grant::AuthorizationDecision`].
#![forbid(unsafe_code)]

pub mod approval;
pub mod broker;
pub mod cancel;
pub mod catalog;
pub mod check_set;
pub mod clock;
pub mod content;
pub mod error;
pub mod event;
pub mod grant;
pub mod guard;
pub mod ids;
pub mod isolation;
pub mod manifest;
pub mod metadata;
pub mod observer;
pub mod provider;
pub mod security;
pub mod store;
pub mod tool;
pub mod usage;
pub mod workspace;

/// A convenient re-export of the most commonly used items.
pub mod prelude {
    pub use crate::approval::{
        AllowAll, ApprovalDecision, ApprovalPolicy, ApprovalRequest, DenyAll,
    };
    pub use crate::broker::{
        CredentialBroker, CredentialError, CredentialRef, CredentialSink, EgressAuthorization,
        EgressBroker, EgressDenied, EgressRequest, EgressTuple, FilesystemBroker, FilesystemError,
        FilesystemHandle, FilesystemRight, FilesystemRights, MountName,
    };
    pub use crate::cancel::{CancelReason, Cancellation};
    pub use crate::check_set::{
        ActionClass, AdvisoryFinding, CheckAudit, CheckSetError, CheckSetOutcome, CheckStatus,
        EnforcementLimits, RevocationTarget, SecurityCheckSet, SecurityCheckSetBuilder,
    };
    pub use crate::clock::{Clock, Deadline, SystemClock, Timestamp};
    pub use crate::content::{ContentPart, Message, Role, ToolCall, ToolResultBlock, UserInput};
    pub use crate::error::{ErrorKind, Result, RuntimeError};
    pub use crate::event::{
        BudgetCategory, CompactionReason, EstimationConfidence, EventEnvelope, LimitKind,
        RuntimeEvent, SCHEMA_VERSION, TurnFinish,
    };
    pub use crate::grant::{
        AuthorizationDecision, CapabilityGrant, ConstraintDimension, ConstraintValue, DecisionCode,
        GrantConstraints, PolicyEpoch, SecurityCheck, SecurityCheckId, SecurityCheckMode,
        SecurityCheckOutcome, SecurityCheckRevision, SecuritySignal,
    };
    pub use crate::guard::{
        ContentGuard, ContentGuardId, ContentGuardRevision, GuardFindings, GuardRiskKind,
        GuardRiskSignal, GuardedFragment, LeakBoundary, LeakCoverageRevision, LeakDetector,
        LeakDetectorId, LeakFinding, LeakScanResult,
    };
    pub use crate::ids::{AttemptId, EventId, RequestId, SessionId, TenantId, ToolCallId, TurnId};
    pub use crate::isolation::{
        BackendConformance, BoundedRendering, DeclaredInterface, IsolationBackend,
        IsolationBackendId, IsolationBackendRevision, IsolationError, IsolationInvocation,
        IsolationInvocationContext, IsolationInvocationId, IsolationOutcome, IsolationProfile,
        ResourceLimitKind, ResourceLimits, TerminationReason, VerifiedArtifact, backend_conforms,
    };
    pub use crate::manifest::{
        ActivatedCapability, CapabilityResolution, ContextSegmentRecord, MANIFEST_SCHEMA_VERSION,
        ManifestReason, ModelResolution, PolicyRevisions, ReplayMismatch, ReplayMode,
        RevisionMismatch, RunManifest, SegmentId, SegmentKind, SegmentSensitivity, SummaryCoverage,
    };
    pub use crate::metadata::{MetaValue, Metadata, VendorLimits};
    pub use crate::observer::EventObserver;
    pub use crate::provider::{
        Capabilities, FinishReason, ModelDescriptor, ModelId, Provider, ProviderAttempt,
        ProviderCallContext, ProviderError, ProviderErrorKind, ProviderRequest, ProviderStream,
        ProviderStreamEvent, ReasoningConfig, ReasoningSupport, Sampling, ToolChoice, ToolSchema,
        UnsupportedFeature,
    };
    pub use crate::security::{
        ArgumentPath, AuthorizationRequest, CheckSetRevision, PermissionSet, SecurityAction,
        SecurityContext, SecurityEvidence, SecurityResource, SecuritySubject, TaintSource,
    };
    pub use crate::store::{Secret, SecretStore, SessionSnapshot, SessionStore, TurnManifest};
    pub use crate::tool::{
        Effect, InvocationContext, Tool, ToolEffects, ToolOutcome, ToolSpec, WriteScope,
    };
    pub use crate::usage::{
        CounterKind, Provenance, UsageDelta, UsageLedger, UsageRecord, UsageSource,
    };
    pub use crate::workspace::{DenyAllWorkspace, Workspace};
}
