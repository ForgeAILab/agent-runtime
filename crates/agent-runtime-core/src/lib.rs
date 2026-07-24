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
#![forbid(unsafe_code)]

pub mod approval;
pub mod cancel;
pub mod catalog;
pub mod clock;
pub mod content;
pub mod error;
pub mod event;
pub mod ids;
pub mod manifest;
pub mod metadata;
pub mod observer;
pub mod provider;
pub mod store;
pub mod tool;
pub mod usage;
pub mod workspace;

/// A convenient re-export of the most commonly used items.
pub mod prelude {
    pub use crate::approval::{
        AllowAll, ApprovalDecision, ApprovalPolicy, ApprovalRequest, DenyAll,
    };
    pub use crate::cancel::{CancelReason, Cancellation};
    pub use crate::clock::{Clock, Deadline, SystemClock, Timestamp};
    pub use crate::content::{ContentPart, Message, Role, ToolCall, ToolResultBlock, UserInput};
    pub use crate::error::{ErrorKind, Result, RuntimeError};
    pub use crate::event::{
        BudgetCategory, CompactionReason, EstimationConfidence, EventEnvelope, LimitKind,
        RuntimeEvent, SCHEMA_VERSION, TurnFinish,
    };
    pub use crate::ids::{AttemptId, EventId, RequestId, SessionId, ToolCallId, TurnId};
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
    pub use crate::store::{Secret, SecretStore, SessionSnapshot, SessionStore, TurnManifest};
    pub use crate::tool::{
        Effect, InvocationContext, Tool, ToolEffects, ToolOutcome, ToolSpec, WriteScope,
    };
    pub use crate::usage::{
        CounterKind, Provenance, UsageDelta, UsageLedger, UsageRecord, UsageSource,
    };
    pub use crate::workspace::{DenyAllWorkspace, Workspace};
}
