use super::*;

pub(in crate::journal) type ProjectedSnapshot = (
    JournalSnapshotView,
    JournalSnapshotRoots,
    SnapshotEncodingCache,
    BTreeMap<JournalObjectId, Vec<u8>>,
    DependencyRoots,
);

#[derive(Debug, Clone)]
pub(in crate::journal) struct SnapshotEncodingCache {
    pub(in crate::journal) roots: JournalSnapshotRoots,
    pub(in crate::journal) history_view: JournalSequenceView,
    pub(in crate::journal) usage_view: JournalSequenceView,
    pub(in crate::journal) policy_revision: Option<u64>,
    pub(in crate::journal) history_ids: crate::journal::semantic::Sequence<JournalObjectId>,
    pub(in crate::journal) usage_ids: crate::journal::semantic::Sequence<JournalObjectId>,
    pub(in crate::journal) manifest_ids: crate::journal::semantic::Sequence<JournalObjectId>,
    pub(in crate::journal) extension_ids: BTreeMap<String, JournalObjectId>,
}

/// Exact extension-record identity for the encoding cache; derived equality
/// would keep a `0.0` encoding for a value rewritten to `-0.0`.
pub(super) fn state_identical(
    left: &crate::store::VersionedSessionState,
    right: &crate::store::VersionedSessionState,
) -> bool {
    left.revision == right.revision
        && left.sensitivity == right.sensitivity
        && journal_json_identical(&left.value, &right.value)
}
