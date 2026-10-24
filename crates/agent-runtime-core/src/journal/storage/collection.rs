//! Bounded, fence-scoped collection protocol.
//!
//! Collection never asks storage for a whole session. Every read is a bounded
//! page bound to one `(domain, session, full fence, page kind)`; a changed
//! generation rejects the page instead of slicing a newer view. Releases are
//! prepared read-only (`prepare_collection_apply`) and applied as one atomic,
//! fenced transaction (`apply_collection`). An adapter that cannot offer these
//! bounded units declares [`JournalCollectionCapability::Unsupported`]; the
//! engine then refuses collection and retains everything. There is no default
//! implementation that reads an unbounded result and slices it.
use super::*;

/// Storage-declared bounds of its non-preemptible collection units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalCollectionLimits {
    /// Largest number of ids or records one page may return.
    pub max_page_items: u32,
    /// Largest physical unit (pack or operation segment, canonical bytes) one
    /// collection apply rewrites without yielding.
    pub max_unit_bytes: u64,
    /// Largest number of physical units one accepted collection apply rewrites.
    pub max_apply_units: u32,
}

/// Whether storage offers the bounded collection protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalCollectionCapability {
    /// Collection is refused; commit/open service continues and all content is retained.
    Unsupported,
    /// Bounded pages, read-only preflight and atomic collection applies.
    Bounded(JournalCollectionLimits),
}

/// Kind of a fence-bound collection page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JournalCollectionPageKind {
    /// Membership ids of the session in a storage-defined order. Every member
    /// appears exactly once across a page sequence. A physical change without
    /// a fence change (repack, compaction) may reject a continuation with a
    /// state conflict; it must never make a continuation silently skip an id.
    Members,
    /// Durable pin records ordered by pin id.
    Pins,
}

/// Opaque storage continuation bound to storage domain, session, full fence and
/// page kind. Its position bytes are storage-defined and never shown in Debug.
#[derive(Clone, PartialEq, Eq)]
pub struct JournalCollectionContinuation {
    domain: JournalDomain,
    session: SessionId,
    fence: JournalFence,
    kind: JournalCollectionPageKind,
    position: Vec<u8>,
}

impl JournalCollectionContinuation {
    /// Storage-side constructor for the next page position.
    pub fn new(
        domain: &JournalDomain,
        session: &SessionId,
        fence: JournalFence,
        kind: JournalCollectionPageKind,
        position: Vec<u8>,
    ) -> Self {
        Self {
            domain: domain.clone(),
            session: session.clone(),
            fence,
            kind,
            position,
        }
    }
    /// Storage-side accessor. Rejects cross-scope or stale-fence use with a
    /// state conflict, so a continuation can never page a newer generation.
    pub fn position(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        fence: JournalFence,
        kind: JournalCollectionPageKind,
    ) -> Result<&[u8], RuntimeError> {
        if &self.domain != domain
            || &self.session != session
            || self.fence != fence
            || self.kind != kind
        {
            return Err(conflict());
        }
        Ok(&self.position)
    }
}

impl fmt::Debug for JournalCollectionContinuation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JournalCollectionContinuation")
            .field("kind", &self.kind)
            .field("fence", &self.fence)
            .field("position", &self.position.len())
            .finish_non_exhaustive()
    }
}

/// One bounded page request under the session's full fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalCollectionPageRequest {
    pub kind: JournalCollectionPageKind,
    /// The stored record's full fence must equal this (zero for an absent record).
    pub fence: JournalFence,
    /// `None` starts at the first item; otherwise a continuation from this scope.
    pub after: Option<JournalCollectionContinuation>,
    /// The page may return fewer, never more, items.
    pub max_items: u32,
}

/// Items of one page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalCollectionItems {
    Members(Vec<JournalObjectId>),
    Pins(Vec<JournalPinRecord>),
}

impl JournalCollectionItems {
    /// Number of ids or records in this page.
    pub fn len(&self) -> usize {
        match self {
            Self::Members(ids) => ids.len(),
            Self::Pins(pins) => pins.len(),
        }
    }
    /// Whether this page carries no items.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One fence-bound page. `next == None` is the end marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalCollectionPage {
    pub fence: JournalFence,
    pub items: JournalCollectionItems,
    /// Encoded bytes the page carried (ids or pin metadata), for budget charging.
    pub bytes: u64,
    pub next: Option<JournalCollectionContinuation>,
}

/// Exact engine-selected collection write and the unit bound it must fit.
/// Storage may ask for a smaller release set; it never selects live ids,
/// applies a subset, or reinterprets retention.
#[derive(Debug, Clone, PartialEq)]
pub struct JournalCollectionPlan {
    /// A `JournalWriteKind::Collect` write with full fence, catalog removals,
    /// release ids and replay floor.
    pub write: JournalWrite,
    /// Maximum physical units (packs/segments) this apply may rewrite.
    pub max_units: u32,
}

/// Acknowledged collection apply: exact physical receipts plus the number of
/// physical units (sealed packs/segments written or deleted) it rewrote, which
/// must not exceed the units its preflight declared.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JournalCollectionReceipt {
    pub receipt: JournalApplyReceipt,
    pub rewritten_units: u32,
}

/// Result of a read-only, no-effect collection preflight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalCollectionPreflight {
    /// The complete plan fits in `units` physical units, at most the plan's
    /// `max_units`; the engine rejects a larger claim as a contract violation.
    Fits { units: u32 },
    /// The plan does not fit; retry with at most `max_release` release ids.
    /// Nothing was changed.
    Split { max_release: u64 },
}
