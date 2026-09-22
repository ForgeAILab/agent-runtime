//! Runtime-owned cache maintenance mechanism.
//!
//! This module is the stable facade for plan-bound cache requests, provider
//! dispatch, identity-scoped state, and validation. Implementation lives in
//! private responsibility-focused modules below.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::pending;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use agent_runtime_context::plan::ContextPlan;
use agent_runtime_core::cancel::{CancelReason, Cancellation};
use agent_runtime_core::checkpoint::{
    CacheOperationCheckpoint, CacheOperationResultCheckpoint, validate_cache_operation_id,
};
use agent_runtime_core::clock::{Clock, Deadline, Timestamp};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{
    CacheOperationOutcome, CacheOperationReason, CacheState, RuntimeEvent,
};
use agent_runtime_core::ids::{AttemptId, CacheOperationId, RequestId, SessionId, TurnId};
use agent_runtime_core::provider::{
    CacheAuthority, CacheAvailabilityEvidence, CacheEvidenceKind, CacheEvidenceSource,
    CacheIdentity, CacheOperationBudget, CacheRefreshCause, CacheResourceOperationKind,
    CacheResourceOperationRequest, FinishReason, Provider, ProviderAttemptPurpose,
    ProviderCacheContract, ProviderCallContext, ProviderError, ProviderRequest,
    ProviderStreamEvent, ToolChoice,
};
use agent_runtime_core::store::{Secret, SessionStateSensitivity, VersionedSessionState};
use agent_runtime_core::usage::{
    CounterKind, Provenance, UsageDelta, UsageLedger, UsageRecord, UsageSource,
};
use agent_runtime_registry::Fingerprint;

use crate::runtime::emitter::EventEmitter;
use crate::runtime::session::CacheStartBarrier;
use crate::runtime::state::SessionState;

const MAX_PERSISTED_CACHE_METRICS: usize = 64;
const MAX_PERSISTED_CACHE_METRIC_KEY_BYTES: usize = 128;

mod dispatch;
mod fingerprint;
mod request;
mod state;
mod validation;

pub use request::{
    CACHE_MECHANISM_STATE_NAMESPACE, CacheCapturedOutput, CacheHandoffSuffix,
    CacheOperationRequest, CacheOperationResult, CacheResourceDispatchRequest,
    MAX_HANDOFF_SUFFIX_BYTES, SyntheticCacheRequest,
};
pub use state::{CacheMechanism, CacheStateRecord};

pub(crate) use fingerprint::CacheOperationFingerprint;
pub(crate) use request::cache_operation_turn;
