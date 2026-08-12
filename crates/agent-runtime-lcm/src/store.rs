//! Least-authority store contracts and bounded expansion types.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use agent_runtime_registry::RegistryRevision;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::entry::{LcmAppendRequest, LcmEntry};
use crate::ids::{
    LcmExpansionCursor, LcmNodeId, LcmOperationFingerprint, LcmRange, LcmRevision, LcmTimelineId,
    MAX_LCM_ID_CHARS,
};
use crate::node::{CondensationCommit, LcmNode, LeafCommit};

/// Maximum page size accepted by the reference contracts.
pub const DEFAULT_MAX_PAGE_SIZE: usize = 1_024;

/// Structured LCM failures. Variants carry metadata only; source messages and
/// summary bodies never enter errors.
#[derive(Clone, PartialEq, Eq, Error)]
pub enum LcmError {
    /// Input or persisted metadata violated an invariant.
    #[error("invalid LCM input")]
    Invalid { reason: String },
    /// Supplied view is not authorized for this timeline.
    #[error("LCM view is unauthorized")]
    Unauthorized,
    /// Compare-and-swap revision mismatch.
    #[error("LCM revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict {
        expected: LcmRevision,
        actual: LcmRevision,
    },
    /// Operation id was reused with different inputs.
    #[error("LCM operation identity was reused with different inputs")]
    IdempotencyConflict,
    /// Append would leave a sequence gap.
    #[error("LCM append expected sequence {expected}, received {actual}")]
    SequenceGap { expected: u64, actual: u64 },
    /// An id or sequence already identifies different immutable content.
    #[error("LCM immutable entry conflict")]
    EntryConflict,
    /// A leaf source range overlaps an existing committed leaf.
    #[error("LCM leaf source range overlaps an existing span")]
    RangeOverlap,
    /// Required entry or node identity does not exist.
    #[error("LCM source identity is missing")]
    MissingSource,
    /// Condensation referenced a superseded child.
    #[error("LCM condensation child is not active")]
    InactiveChild,
    /// Mutation crossed timeline boundaries.
    #[error("LCM source identity belongs to another timeline")]
    CrossTimeline,
    /// Expansion cursor was invalidated or malformed.
    #[error("LCM expansion cursor is invalid")]
    InvalidCursor,
    /// Page limit is invalid or exceeds a host bound.
    #[error("LCM expansion/read bound is invalid")]
    InvalidBound,
    /// Secret-class source cannot enter a normal summary body.
    #[error("secret-class source is not eligible for semantic summarization")]
    SecretSource,
    /// Required context cannot fit after bounded compaction.
    #[error("LCM context cannot fit within the resolved budget")]
    CannotFit {
        required_tokens: u64,
        available_tokens: u64,
    },
    /// Host adapter failed; backend diagnostics stay outside this contract.
    #[error("LCM store failure")]
    StoreFailure,
}

impl fmt::Debug for LcmError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Validation details are useful to the in-process caller through the
        // typed field, but they are deliberately excluded from diagnostics:
        // a third-party store must not be able to smuggle source content into
        // logs by returning an `Invalid` reason.
        formatter
            .debug_tuple("LcmError")
            .field(&self.to_string())
            .finish()
    }
}

static NEXT_AUTHORITY_ID: AtomicU64 = AtomicU64::new(1);

struct AuthorityGrant {
    id: u64,
    revoked: AtomicBool,
}

/// Opaque, non-serializable authority issuer for LCM request views.
///
/// A host creates one authority at its trusted store/binding boundary, passes
/// a clone to the adapter, and issues views from the same authority. The
/// opaque grant is identity-based and can be revoked; knowing a timeline ID
/// or authorization revision cannot manufacture a usable view.
#[derive(Clone)]
pub struct LcmViewAuthority {
    grant: Arc<AuthorityGrant>,
}

impl fmt::Debug for LcmViewAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LcmViewAuthority")
            .field("grant", &"[redacted]")
            .finish()
    }
}

impl LcmViewAuthority {
    /// Mints a new process-local authority identity.
    pub fn new() -> Self {
        Self {
            grant: Arc::new(AuthorityGrant {
                id: NEXT_AUTHORITY_ID.fetch_add(1, Ordering::Relaxed),
                revoked: AtomicBool::new(false),
            }),
        }
    }

    /// Issues a request view carrying a host-owned authorization revision.
    /// The revision is metadata only until the store's authorization callback
    /// validates it against the host binding.
    pub fn issue(
        &self,
        timeline_id: LcmTimelineId,
        authorization_revision: impl Into<String>,
    ) -> LcmView {
        LcmView {
            timeline_id,
            authorization_revision: Some(authorization_revision.into()),
            grant: Arc::clone(&self.grant),
        }
    }

    /// Revokes all views issued by this authority, including cloned views.
    pub fn revoke(&self) {
        self.grant.revoked.store(true, Ordering::Release);
    }

    /// Whether this authority has been revoked.
    pub fn is_revoked(&self) -> bool {
        self.grant.revoked.load(Ordering::Acquire)
    }

    /// Validates that a view carries this exact unrevoked grant.
    pub fn authorize(&self, view: &LcmView) -> Result<(), LcmError> {
        validate_request_scope(view)?;
        if self.is_revoked()
            || !Arc::ptr_eq(&self.grant, &view.grant)
            || self.grant.id != view.grant.id
        {
            return Err(LcmError::Unauthorized);
        }
        Ok(())
    }
}

impl Default for LcmViewAuthority {
    fn default() -> Self {
        Self::new()
    }
}

/// A host-authorized request scope over one logical timeline.
///
/// This value carries an opaque, non-serializable grant minted by
/// [`LcmViewAuthority`]. A host creates the authority only after authorizing a
/// timeline binding, gives the same authority to its store adapter, and then
/// passes the issued view to every read/write operation. IDs supplied in
/// model or repository text are never accepted in place of this scope.
#[derive(Clone)]
pub struct LcmView {
    timeline_id: LcmTimelineId,
    authorization_revision: Option<String>,
    grant: Arc<AuthorityGrant>,
}

impl PartialEq for LcmView {
    fn eq(&self, other: &Self) -> bool {
        self.timeline_id == other.timeline_id
            && self.authorization_revision == other.authorization_revision
            && Arc::ptr_eq(&self.grant, &other.grant)
    }
}

impl Eq for LcmView {}

impl fmt::Debug for LcmView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LcmView")
            .field("timeline_id", &self.timeline_id)
            .field("authorization_revision", &"[redacted]")
            .field("grant", &"[redacted]")
            .finish()
    }
}

impl LcmView {
    /// Timeline bound to this view.
    pub fn timeline_id(&self) -> &LcmTimelineId {
        &self.timeline_id
    }

    /// Optional host authorization/configuration revision.
    pub fn authorization_revision(&self) -> Option<&str> {
        self.authorization_revision.as_deref()
    }
}

/// Result of an immutable append operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendResult {
    /// Durable revision after append (or original append on replay).
    pub revision: LcmRevision,
    /// Number of represented entries.
    pub entries: usize,
    /// Whether the operation was already durably applied.
    pub already_committed: bool,
}

/// Result of an atomic node mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitResult {
    /// Committed node metadata and protected body.
    pub node: LcmNode,
    /// Durable revision after commit.
    pub revision: LcmRevision,
    /// Whether the operation was already durably applied.
    pub already_committed: bool,
}

/// A bounded expansion request over an authorized view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpansionRequest {
    /// Opaque node to expand.
    pub node_id: LcmNodeId,
    /// Maximum direct children/entries to return.
    pub limit: usize,
    /// Optional continuation from a previous response.
    pub cursor: Option<LcmExpansionCursor>,
}

impl ExpansionRequest {
    /// Creates a first-page request.
    pub fn new(node_id: LcmNodeId, limit: usize) -> Self {
        Self {
            node_id,
            limit,
            cursor: None,
        }
    }

    /// Continues a prior request.
    pub fn from_cursor(cursor: LcmExpansionCursor, limit: usize) -> Self {
        Self {
            node_id: cursor.node_id.clone(),
            limit,
            cursor: Some(cursor),
        }
    }
}

/// One direct expansion child with source provenance retained.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum ExpansionItem {
    /// Immutable source entry.
    Entry(LcmEntry),
    /// Child summary node, itself expandable.
    Node(LcmNode),
}

impl ExpansionItem {
    /// Source sequence range represented by this item.
    pub fn range(&self) -> LcmRange {
        match self {
            Self::Entry(entry) => LcmRange::single(entry.sequence),
            Self::Node(node) => node.range,
        }
    }

    /// Whether the item can be expanded further.
    pub fn expandable(&self) -> bool {
        matches!(self, Self::Node(_))
    }
}

/// Deterministic bounded expansion response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LcmExpansion {
    /// Requested node.
    pub node_id: LcmNodeId,
    /// Stable fingerprint checked by the cursor.
    pub source_fingerprint: agent_runtime_registry::Fingerprint,
    /// Ordered direct children or entries.
    pub items: Vec<ExpansionItem>,
    /// Whether all direct children/entries were returned.
    pub complete: bool,
    /// Stable continuation when incomplete.
    pub next_cursor: Option<LcmExpansionCursor>,
}

impl LcmExpansion {
    /// Number of returned items.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether no items were returned.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Least-authority read contract. Implementations validate `view` before
/// looking up opaque identities. Every method takes the view so one adapter
/// may safely serve multiple authorized timelines; possession of a timeline
/// or node ID alone is insufficient.
#[async_trait]
pub trait LcmReader: Send + Sync + fmt::Debug {
    /// Stable adapter/schema revision governing persisted store semantics.
    ///
    /// Runtime integrations include this value in checkpoint and replay
    /// compatibility. It is distinct from a host authorization revision: a
    /// policy grant may stay the same while the store's schema or invariant
    /// implementation changes.
    fn store_revision(&self) -> RegistryRevision;

    /// Validates the host-owned authorization attached to a request scope.
    ///
    /// The implementation MUST validate a host-owned capability or external
    /// authorization binding; every reader and writer method must call this
    /// callback before looking up an opaque identity. A matching timeline ID
    /// alone is never a sufficient authorization proof.
    fn authorize_view(&self, view: &LcmView) -> Result<(), LcmError>;

    /// Current immutable timeline/DAG revision.
    async fn current_revision(&self, view: &LcmView) -> Result<LcmRevision, LcmError>;
    /// Bounded immutable range read.
    async fn load_range(
        &self,
        view: &LcmView,
        range: LcmRange,
        limit: usize,
    ) -> Result<Vec<LcmEntry>, LcmError>;
    /// Active, non-superseded nodes in source order.
    async fn active_nodes(&self, view: &LcmView) -> Result<Vec<LcmNode>, LcmError>;
    /// Reads one node through the authorized view.
    async fn node(&self, view: &LcmView, node_id: &LcmNodeId) -> Result<LcmNode, LcmError>;
    /// Bounded direct expansion through the authorized view.
    async fn expand(
        &self,
        view: &LcmView,
        request: ExpansionRequest,
    ) -> Result<LcmExpansion, LcmError>;
}

/// Least-authority mutation contract. Mutations are expected-revision CAS and
/// operation-fingerprint idempotent.
#[async_trait]
pub trait LcmWriter: LcmReader {
    /// Idempotently appends immutable entries.
    async fn append(
        &self,
        view: &LcmView,
        request: LcmAppendRequest,
    ) -> Result<AppendResult, LcmError>;
    /// Atomically commits a leaf node and its entry edges.
    async fn commit_leaf(
        &self,
        view: &LcmView,
        request: LeafCommit,
    ) -> Result<CommitResult, LcmError>;
    /// Atomically commits a condensed node, edges, and supersession.
    async fn commit_condensation(
        &self,
        view: &LcmView,
        request: CondensationCommit,
    ) -> Result<CommitResult, LcmError>;
}

/// Combined store convenience bound; no concrete backend is supplied here.
pub trait LcmStore: LcmReader + LcmWriter {}

impl<T> LcmStore for T where T: LcmReader + LcmWriter {}

#[allow(dead_code)]
pub(crate) fn validate_limit(limit: usize) -> Result<usize, LcmError> {
    if limit == 0 || limit > DEFAULT_MAX_PAGE_SIZE {
        Err(LcmError::InvalidBound)
    } else {
        Ok(limit)
    }
}

#[allow(dead_code)]
pub(crate) fn validate_view(expected: &LcmTimelineId, view: &LcmView) -> Result<(), LcmError> {
    validate_request_scope(view)?;
    if expected.validate().is_err() {
        return Err(LcmError::Unauthorized);
    }
    if expected == view.timeline_id() {
        Ok(())
    } else {
        Err(LcmError::Unauthorized)
    }
}

/// Validates the non-authority portion of a request scope. Host adapters are
/// responsible for layering their own authorization callback over this
/// structural check.
pub(crate) fn validate_request_scope(view: &LcmView) -> Result<(), LcmError> {
    if view.timeline_id.validate().is_err()
        || view
            .authorization_revision
            .as_deref()
            .is_some_and(|revision| {
                let length = revision.chars().count();
                length == 0 || length > MAX_LCM_ID_CHARS || revision.trim().is_empty()
            })
    {
        Err(LcmError::Unauthorized)
    } else {
        Ok(())
    }
}

/// Safe operation fingerprint helper for adapters.
pub fn operation_fingerprint(
    timeline_id: &LcmTimelineId,
    operation_id: &str,
    source: &str,
) -> LcmOperationFingerprint {
    LcmOperationFingerprint::from_fields([timeline_id.as_str(), operation_id, source])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_form_store_details_do_not_enter_diagnostics() {
        let error = LcmError::Invalid {
            reason: "private source body".into(),
        };
        assert!(!error.to_string().contains("private source body"));
        assert!(!format!("{error:?}").contains("private source body"));
    }
}
