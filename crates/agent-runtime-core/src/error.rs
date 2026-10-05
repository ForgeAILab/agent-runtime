//! Structured, redaction-safe errors.
//!
//! The donor code had per-crate error enums with no shared `kind`, no
//! `retryable` flag, and no redaction. This unified [`RuntimeError`] carries a
//! coarse [`ErrorKind`] discriminant, an explicit retryability flag, and a
//! redaction-safe [`Metadata`] bag so errors can be emitted in events safely.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::event::LimitKind;
use crate::metadata::Metadata;
use crate::provider_credential::ProviderCredentialRecovery;

/// Where a failure originated. This is not evidence of cost or retry admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum FailureStage {
    /// Request construction or local validation.
    PreProvider,
    /// A provider invocation.
    Provider,
    /// Tool preparation or execution.
    Tool,
    /// The origin is not known.
    #[serde(other)]
    Unknown,
}

/// Bounded neutral component identities, never caller or backend prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum FailureComponent {
    /// The context planner or its compactor.
    ContextPlanner,
    /// A host harness component.
    Harness,
    /// The lossless context memory coordinator or store.
    Lcm,
    /// Protected turn checkpoint state.
    Checkpoint,
    /// The component is not known to this reader.
    #[serde(other)]
    Unknown,
}

/// Host-visible details of a static tool write scope rejected during build.
/// Declared tool/scope/root strings are configuration evidence, not telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolWriteScopeViolation {
    /// The registered tool name.
    pub tool: String,
    /// The scope rejected by the configured workspace.
    pub scope: String,
    /// The configured workspace root.
    pub workspace: String,
}

/// Typed failure evidence. Classification never grants retry or lookup authority.
/// Unknown future reason tags deserialize as [`Self::Unclassified`]. On a
/// [`RuntimeError`], any malformed classification also becomes unclassified so
/// diagnostic evidence can never make its enclosing record unreadable.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum FailureClass {
    /// A static workspace write scope was rejected during runtime build.
    /// Host-visible configuration evidence; declared paths are not telemetry.
    InvalidToolWriteScope(Box<ToolWriteScopeViolation>),
    /// Configuration, schema, pairing, capability, or cache rejection.
    RequestRejected { stage: FailureStage },
    /// Approval, workspace, or timeline authority was denied.
    PolicyDenied { stage: FailureStage },
    /// A typed model-input budget overflow; absent counts remain unknown.
    ContextOverflow {
        stage: FailureStage,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        required_tokens: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        available_tokens: Option<u32>,
    },
    /// Transport, server, timeout, or malformed-stream evidence.
    Transient { stage: FailureStage },
    /// A momentary provider throttle.
    RateLimited { stage: FailureStage },
    /// An exhausted provider usage window.
    QuotaExhausted { stage: FailureStage },
    /// Authentication failed; credential recovery remains separately fenced.
    Auth { stage: FailureStage },
    /// A revision, frontier, identity, or checkpoint conflict.
    StateConflict {
        stage: FailureStage,
        component: FailureComponent,
    },
    /// A component failed without more specific evidence.
    HostComponent {
        stage: FailureStage,
        component: FailureComponent,
    },
    /// A known runtime turn limit.
    TurnLimit {
        stage: FailureStage,
        limit: LimitKind,
    },
    /// Work was cancelled.
    Cancelled { stage: FailureStage },
    /// A runtime invariant failed.
    Internal { stage: FailureStage },
    /// Legacy data or insufficient typed evidence.
    #[default]
    #[serde(other)]
    Unclassified,
}

impl FailureClass {
    /// The originating stage, or unknown for legacy/unclassified evidence.
    pub fn stage(&self) -> FailureStage {
        match self {
            Self::InvalidToolWriteScope(_) => FailureStage::PreProvider,
            Self::Unclassified => FailureStage::Unknown,
            Self::RequestRejected { stage }
            | Self::PolicyDenied { stage }
            | Self::ContextOverflow { stage, .. }
            | Self::Transient { stage }
            | Self::RateLimited { stage }
            | Self::QuotaExhausted { stage }
            | Self::Auth { stage }
            | Self::StateConflict { stage, .. }
            | Self::HostComponent { stage, .. }
            | Self::TurnLimit { stage, .. }
            | Self::Cancelled { stage }
            | Self::Internal { stage } => *stage,
        }
    }

    /// Whether no classification evidence is available.
    pub fn is_unclassified(&self) -> bool {
        matches!(self, Self::Unclassified)
    }

    /// Sets a known origin without changing the category or retry semantics.
    /// Static write-scope violations retain their build-time `PreProvider` origin.
    pub fn with_stage(mut self, origin: FailureStage) -> Self {
        match &mut self {
            Self::Unclassified | Self::InvalidToolWriteScope(_) => {}
            Self::RequestRejected { stage }
            | Self::PolicyDenied { stage }
            | Self::ContextOverflow { stage, .. }
            | Self::Transient { stage }
            | Self::RateLimited { stage }
            | Self::QuotaExhausted { stage }
            | Self::Auth { stage }
            | Self::StateConflict { stage, .. }
            | Self::HostComponent { stage, .. }
            | Self::TurnLimit { stage, .. }
            | Self::Cancelled { stage }
            | Self::Internal { stage } => *stage = origin,
        }
        self
    }
}

/// A coarse, stable classification of a runtime error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// A provider (LLM backend) failure.
    Provider,
    /// A tool invocation failure.
    Tool,
    /// The action was denied by the approval policy.
    Approval,
    /// A workspace boundary violation.
    Workspace,
    /// Work was cancelled.
    Cancelled,
    /// A configured limit was reached.
    Limit,
    /// A deadline elapsed.
    Timeout,
    /// Invalid configuration or request.
    Config,
    /// Serialization / deserialization failure.
    Serialization,
    /// A referenced entity was not found.
    NotFound,
    /// A conflicting state (e.g. a duplicate registration).
    Conflict,
    /// An unexpected internal error.
    Internal,
}

/// The canonical error type of the runtime.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeError {
    /// The coarse classification.
    pub kind: ErrorKind,
    /// A human-readable, redaction-safe message.
    pub message: String,
    /// Whether retrying the operation might succeed.
    pub retryable: bool,
    /// Redaction-safe structured context.
    #[serde(default, skip_serializing_if = "Metadata::is_empty")]
    pub metadata: Metadata,
    /// Additive failure evidence, independent of coarse kind and retryability.
    #[serde(
        default,
        deserialize_with = "deserialize_failure_class",
        skip_serializing_if = "FailureClass::is_unclassified"
    )]
    pub class: FailureClass,
    /// Provider-suggested delay in milliseconds; never an admitted retry.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u64",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_after_ms: Option<u64>,
    /// Provider-reported reset time in absolute Unix milliseconds.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u64",
        skip_serializing_if = "Option::is_none"
    )]
    pub limit_resets_at_ms: Option<u64>,
    /// Fixed provider recovery evidence, never a credential lease or secret.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_credential_recovery",
        skip_serializing_if = "Option::is_none"
    )]
    pub credential_recovery: Option<ProviderCredentialRecovery>,
}

fn deserialize_failure_class<'de, D>(deserializer: D) -> Result<FailureClass, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(FailureClass::deserialize(value).unwrap_or_default())
}

fn deserialize_optional_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(value.as_u64())
}

fn deserialize_optional_credential_recovery<'de, D>(
    deserializer: D,
) -> Result<Option<ProviderCredentialRecovery>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(ProviderCredentialRecovery::deserialize(value).ok())
}

impl RuntimeError {
    /// Builds an error of the given kind.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable: false,
            metadata: Metadata::new(),
            class: FailureClass::Unclassified,
            retry_after_ms: None,
            limit_resets_at_ms: None,
            credential_recovery: None,
        }
    }

    /// Attaches explicit typed evidence without changing coarse projections.
    pub fn with_class(mut self, class: FailureClass) -> Self {
        self.class = class;
        self
    }

    /// Records an origin known by the caller at a typed failure boundary.
    pub fn with_failure_stage(mut self, stage: FailureStage) -> Self {
        self.class = self.class.with_stage(stage);
        self
    }

    /// Marks the error retryable.
    pub fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }

    /// Attaches metadata.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Whether retrying might succeed.
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    // Convenience constructors for the common kinds.

    /// A [`ErrorKind::Config`] error.
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Config, message).with_class(FailureClass::RequestRejected {
            stage: FailureStage::Unknown,
        })
    }
    /// A [`ErrorKind::Tool`] error.
    pub fn tool(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Tool, message)
    }
    /// A [`ErrorKind::Approval`] denial.
    pub fn approval(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Approval, message).with_class(FailureClass::PolicyDenied {
            stage: FailureStage::Unknown,
        })
    }
    /// A [`ErrorKind::Workspace`] violation.
    pub fn workspace(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Workspace, message).with_class(FailureClass::PolicyDenied {
            stage: FailureStage::Unknown,
        })
    }
    /// A [`ErrorKind::Cancelled`] error.
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Cancelled, message).with_class(FailureClass::Cancelled {
            stage: FailureStage::Unknown,
        })
    }
    /// A [`ErrorKind::Limit`] error.
    pub fn limit(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Limit, message)
    }
    /// A [`ErrorKind::NotFound`] error.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }
    /// A [`ErrorKind::Conflict`] error.
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, message)
    }
    /// A [`ErrorKind::Internal`] error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message)
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for RuntimeError {}

impl From<serde_json::Error> for RuntimeError {
    fn from(err: serde_json::Error) -> Self {
        RuntimeError::new(ErrorKind::Serialization, err.to_string())
    }
}

/// The runtime's result alias.
pub type Result<T, E = RuntimeError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{ProviderError, ProviderErrorKind};

    #[test]
    fn failure_classes_roundtrip() {
        let stage = FailureStage::Tool;
        let component = FailureComponent::Checkpoint;
        for class in [
            FailureClass::Unclassified,
            FailureClass::RequestRejected { stage },
            FailureClass::PolicyDenied { stage },
            FailureClass::ContextOverflow {
                stage,
                required_tokens: Some(0),
                available_tokens: None,
            },
            FailureClass::Transient { stage },
            FailureClass::RateLimited { stage },
            FailureClass::QuotaExhausted { stage },
            FailureClass::Auth { stage },
            FailureClass::StateConflict { stage, component },
            FailureClass::HostComponent { stage, component },
            FailureClass::TurnLimit {
                stage,
                limit: LimitKind::Time,
            },
            FailureClass::Cancelled { stage },
            FailureClass::Internal { stage },
        ] {
            let error = RuntimeError::new(ErrorKind::Internal, "safe").with_class(class);
            let value = serde_json::to_value(&error).unwrap();
            assert_eq!(
                serde_json::from_value::<RuntimeError>(value).unwrap(),
                error
            );
            assert!(!error.retryable);
        }
    }

    #[test]
    fn diagnostic_evidence_never_makes_a_runtime_error_unreadable() {
        let decode = |class: serde_json::Value| {
            serde_json::from_value::<RuntimeError>(serde_json::json!({
                "kind": "internal",
                "message": "safe",
                "retryable": false,
                "class": class,
            }))
            .unwrap()
        };

        assert_eq!(
            decode(serde_json::json!({"reason":"transient", "stage":"future"})).class,
            FailureClass::Transient {
                stage: FailureStage::Unknown
            }
        );
        assert_eq!(
            decode(serde_json::json!({
                "reason":"state_conflict",
                "stage":"pre_provider",
                "component":"future"
            }))
            .class,
            FailureClass::StateConflict {
                stage: FailureStage::PreProvider,
                component: FailureComponent::Unknown,
            }
        );
        for class in [
            serde_json::json!({"reason":"transient"}),
            serde_json::json!({"reason":"turn_limit", "stage":"provider", "limit":"future"}),
            serde_json::json!({"reason":"context_overflow", "stage":"provider", "required_tokens":-1}),
            serde_json::json!("not an object"),
        ] {
            assert_eq!(decode(class).class, FailureClass::Unclassified);
        }

        let timing: RuntimeError = serde_json::from_value(serde_json::json!({
            "kind": "provider",
            "message": "safe",
            "retryable": false,
            "retry_after_ms": -1,
            "limit_resets_at_ms": "future",
            "credential_recovery": "future"
        }))
        .unwrap();
        assert_eq!(timing.retry_after_ms, None);
        assert_eq!(timing.limit_resets_at_ms, None);
        assert_eq!(timing.credential_recovery, None);

        let envelope: crate::event::EventEnvelope = serde_json::from_value(serde_json::json!({
            "schema_version": crate::event::SCHEMA_VERSION,
            "seq": 1,
            "id": "evt-future-class",
            "session": "session-future-class",
            "turn": "turn-future-class",
            "timestamp": 0,
            "payload": {
                "event": "error",
                "error": {
                    "kind": "provider",
                    "message": "safe",
                    "retryable": false,
                    "class": {
                        "reason": "turn_limit",
                        "stage": "provider",
                        "limit": "future"
                    }
                }
            }
        }))
        .unwrap();
        let crate::event::RuntimeEvent::Error { error } = envelope.payload else {
            panic!("error event fixture")
        };
        assert_eq!(error.class, FailureClass::Unclassified);
    }

    #[test]
    fn generic_and_policy_constructors_do_not_infer_origin_or_retry_permission() {
        assert_eq!(
            RuntimeError::new(ErrorKind::Provider, "network").class,
            FailureClass::Unclassified
        );
        assert_eq!(
            RuntimeError::config("schema").class,
            FailureClass::RequestRejected {
                stage: FailureStage::Unknown
            }
        );
        for error in [
            RuntimeError::approval("deny"),
            RuntimeError::workspace("deny"),
        ] {
            assert_eq!(
                error.class,
                FailureClass::PolicyDenied {
                    stage: FailureStage::Unknown
                }
            );
            assert!(!error.retryable);
        }
    }

    #[test]
    fn provider_conversion_preserves_all_fields_and_coarse_projections() {
        let stage = FailureStage::Unknown;
        for (provider_kind, kind, class) in [
            (
                ProviderErrorKind::Network,
                ErrorKind::Provider,
                FailureClass::Transient { stage },
            ),
            (
                ProviderErrorKind::Timeout,
                ErrorKind::Timeout,
                FailureClass::Transient { stage },
            ),
            (
                ProviderErrorKind::RateLimited,
                ErrorKind::Provider,
                FailureClass::RateLimited { stage },
            ),
            (
                ProviderErrorKind::Auth,
                ErrorKind::Provider,
                FailureClass::Auth { stage },
            ),
            (
                ProviderErrorKind::BadRequest,
                ErrorKind::Config,
                FailureClass::RequestRejected { stage },
            ),
            (
                ProviderErrorKind::MalformedStream,
                ErrorKind::Provider,
                FailureClass::Transient { stage },
            ),
            (
                ProviderErrorKind::Server,
                ErrorKind::Provider,
                FailureClass::Transient { stage },
            ),
            (
                ProviderErrorKind::Cancelled,
                ErrorKind::Cancelled,
                FailureClass::Cancelled { stage },
            ),
            (
                ProviderErrorKind::Unsupported,
                ErrorKind::Config,
                FailureClass::RequestRejected { stage },
            ),
            (
                ProviderErrorKind::CacheExpired,
                ErrorKind::Provider,
                FailureClass::RequestRejected { stage },
            ),
            (
                ProviderErrorKind::LimitExhausted,
                ErrorKind::Limit,
                FailureClass::QuotaExhausted { stage },
            ),
        ] {
            for (retry_after_ms, limit_resets_at_ms) in [
                (None, None),
                (Some(0), Some(1)),
                (Some(25), Some(1_700_000_000_123)),
            ] {
                for retryable in [false, true] {
                    let mut provider = ProviderError::new(provider_kind, "safe provider evidence");
                    provider.retryable = retryable;
                    provider.retry_after_ms = retry_after_ms;
                    provider.limit_resets_at_ms = limit_resets_at_ms;
                    provider.credential_recovery =
                        Some(ProviderCredentialRecovery::RetryWithRenewedCredential);
                    provider.metadata = Metadata::new().with("source", "fixture");
                    let runtime = RuntimeError::from(provider.clone());
                    assert_eq!(runtime.kind, kind);
                    assert_eq!(runtime.class, class);
                    assert_eq!(runtime.message, provider.message);
                    assert_eq!(runtime.retryable, retryable);
                    assert_eq!(runtime.metadata, provider.metadata);
                    assert_eq!(runtime.retry_after_ms, retry_after_ms);
                    assert_eq!(runtime.limit_resets_at_ms, limit_resets_at_ms);
                    assert_eq!(runtime.credential_recovery, provider.credential_recovery);
                }
            }
        }
        let absent = RuntimeError::from(ProviderError::new(ProviderErrorKind::Auth, "safe"));
        assert_eq!(absent.credential_recovery, None);
    }

    #[test]
    fn retryable_flag_and_display() {
        let e = RuntimeError::tool("boom").retryable();
        assert!(e.is_retryable());
        assert_eq!(e.kind, ErrorKind::Tool);
        assert!(format!("{e}").contains("boom"));
    }

    #[test]
    fn serializes_without_empty_metadata() {
        let e = RuntimeError::config("bad");
        let v = serde_json::to_value(&e).unwrap();
        assert!(v.get("metadata").is_none());
    }
}
