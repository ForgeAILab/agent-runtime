//! Neutral session rotation and protected summary seeds.
use agent_runtime_core::content::Message;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::SessionId;
use agent_runtime_core::store::VersionedSessionState;
use agent_runtime_registry::RegistryRevision;
use serde::{Deserialize, Serialize};

/// Protected namespace containing the stable summary seed of a fork.
pub(crate) const SUMMARY_SEED_NAMESPACE: &str = "runtime.session.summary_seed";
/// Protected intent fencing a partially persisted fork; only the same fork may repair it.
pub(crate) const PENDING_FORK_NAMESPACE: &str = "runtime.session.fork_pending";
/// Redaction-safe successor marker; superseded sessions cannot accept work.
pub(crate) const SUPERSEDED_NAMESPACE: &str = "runtime.session.superseded";

/// Initial context of a fork. Summary text is omitted from Debug output.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForkSeed {
    /// Host-authored summary, entering context as a Stable Summary fragment.
    Summary(String),
    /// Canonical history suffix starting at the given message index.
    FromIndex(usize),
    /// Start with empty canonical history.
    Empty,
}

impl std::fmt::Debug for ForkSeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Summary(_) => f.write_str("Summary([redacted])"),
            Self::FromIndex(index) => f.debug_tuple("FromIndex").field(index).finish(),
            Self::Empty => f.write_str("Empty"),
        }
    }
}

/// Timeline disposition of a fork, authorized by the host resolver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForkLcm {
    /// Start an empty, newly authorized and claimed timeline.
    NewTimeline,
    /// Transfer the existing timeline to the new owner and adopt its projection.
    Continue,
}

/// Request to rotate an idle session; no provider work is performed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkSession {
    /// Idle parent identity in this runtime or its configured durable stores.
    pub from: SessionId,
    /// Fresh successor identity.
    pub new_id: SessionId,
    /// Context carried to the successor.
    pub seed: ForkSeed,
    /// Explicit timeline disposition (ignored when LCM is not configured).
    pub lcm: ForkLcm,
}

pub(crate) fn summary_state(summary: String) -> VersionedSessionState {
    VersionedSessionState::new(
        RegistryRevision::new("session-summary-seed-1"),
        serde_json::json!(summary),
    )
}

pub(crate) fn summary_fragment(
    state: Option<&VersionedSessionState>,
) -> Result<Option<agent_runtime_context::ContextFragment>, RuntimeError> {
    use agent_runtime_context::{
        CacheClass, ContextFragment, FragmentContent, FragmentKind, FragmentSource, Sensitivity,
    };
    let Some(state) = state else {
        return Ok(None);
    };
    if state.revision.as_str() != "session-summary-seed-1" {
        return Err(RuntimeError::conflict("unsupported summary seed revision"));
    }
    let summary = state
        .value
        .as_str()
        .ok_or_else(|| RuntimeError::conflict("invalid summary seed"))?;
    Ok(Some(
        ContextFragment::new(
            "session:summary-seed",
            FragmentKind::Summary,
            FragmentSource::Host,
            RegistryRevision::from_content(summary.as_bytes()),
            FragmentContent::Text(summary.to_owned()),
        )
        .with_cache_class(CacheClass::Stable)
        .with_sensitivity(Sensitivity::Sensitive),
    ))
}

pub(crate) fn seed_history(
    seed: &ForkSeed,
    history: &[Message],
) -> Result<Vec<Message>, RuntimeError> {
    match seed {
        ForkSeed::FromIndex(index) => history
            .get(*index..)
            .map(<[Message]>::to_vec)
            .ok_or_else(|| RuntimeError::conflict("fork history index is out of range")),
        ForkSeed::Summary(_) | ForkSeed::Empty => Ok(Vec::new()),
    }
}

/// Releases a reservation if validation failed before durable fork intent.
pub(crate) struct ForkReservation {
    pub(crate) inner: std::sync::Arc<super::session::SessionInner>,
    pub(crate) retain: bool,
}
impl Drop for ForkReservation {
    fn drop(&mut self) {
        if !self.retain {
            self.inner
                .turns
                .lock()
                .expect("session turns poisoned")
                .shutting_down = false;
        }
    }
}
