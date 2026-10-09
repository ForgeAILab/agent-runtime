use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use agent_runtime_registry::Fingerprint;
use serde::{Deserialize, Serialize};

use super::{JournalDigest, JournalObject, JournalObjectId};
use crate::checkpoint::CheckpointWatermark;
use crate::clock::{Deadline, Timestamp};
use crate::content::Message;
use crate::ids::{SessionId, TurnId};
use crate::store::{SessionIdentityState, SessionSnapshot, TurnManifest, VersionedSessionState};
use crate::usage::UsageRecord;

/// Schema for journal heads, objects and reference checkpoints.
pub const JOURNAL_SCHEMA_VERSION: u32 = 4;
/// Maximum children/entries per persistent sequence or map node.
pub const JOURNAL_FANOUT: usize = 64;
/// Maximum tree height accepted by readers (well above a u64-sized fanout-64 tree).
pub const JOURNAL_MAX_TREE_HEIGHT: u32 = 32;

macro_rules! opaque_identity {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);
        impl $name {
            /// Creates an identity in the host's namespace. Not an authorization grant.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }
            /// Explicitly exposes the identity for backend lookup, never default logging.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "([redacted])"))
            }
        }
    };
}
opaque_identity!(
    JournalStorageId,
    "Host-namespaced protection-domain identity; never a credential."
);
opaque_identity!(
    JournalOperationId,
    "Stable commit/import operation identity; retries must reuse it."
);
opaque_identity!(
    JournalPinId,
    "Host-namespaced durable pin/transfer identity; not read authority."
);

/// Monotonic published-head revision, independent of turn state revision.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JournalRevision(pub u64);

/// Which policy domain a journal handle exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalRole {
    /// Host-policy projection; cannot contain an exact checkpoint root.
    Ordinary,
    /// Exact execution state under the host's protected persistence policy.
    Protected,
}

/// Root selected atomically by an open operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "selection", rename_all = "snake_case")]
pub enum JournalRootSelection {
    /// The current published boundary (or absent-head writer reservation).
    Latest,
    /// A specifically retained head object; opening must pin its whole closure.
    Retained { root: JournalObjectId },
}

/// Pin-only or fenced execution intent. Durations/renewal are host policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalOpenIntent {
    /// Read without admitting new execution.
    ReadOnly,
    /// Exclusive fenced execution, including create/import when no head exists.
    ExclusiveWriter,
}

/// Atomic root/pin/fence acquisition request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalOpen {
    /// Current or retained root.
    pub root: JournalRootSelection,
    /// If present, require this published revision before opening.
    pub expected_revision: Option<JournalRevision>,
    /// Stable caller operation identity.
    pub operation: JournalOperationId,
    /// Stable reader or durable transfer pin identity.
    pub pin: JournalPinId,
    /// Whether execution is requested.
    pub intent: JournalOpenIntent,
}

/// Opaque, nonserializable process handle issued by a backend.
///
/// Possession alone grants nothing: the issuing backend verifies its private
/// handle, protection domain, pin and live fence on every read/commit. Tokens
/// cannot be reconstructed from a serialized head or copied between backends.
pub struct JournalLease {
    storage: JournalStorageId,
    session: SessionId,
    head: Option<Arc<JournalHead>>,
    writer_epoch: Option<u64>,
    handle: Arc<dyn Any + Send + Sync>,
}

impl JournalLease {
    /// Backend constructor after atomic root/pin/fence acquisition.
    pub fn new<T: Any + Send + Sync>(
        storage: JournalStorageId,
        session: SessionId,
        head: Option<JournalHead>,
        writer_epoch: Option<u64>,
        handle: Arc<T>,
    ) -> Self {
        Self {
            storage,
            session,
            head: head.map(Arc::new),
            writer_epoch,
            handle,
        }
    }
    /// Issuing protection domain.
    pub fn storage_id(&self) -> &JournalStorageId {
        &self.storage
    }
    /// Session whose closure was pinned.
    pub fn session(&self) -> &SessionId {
        &self.session
    }
    /// Immutable selected head; a writer must reopen/renew to select a newer root.
    pub fn head(&self) -> Option<&JournalHead> {
        self.head.as_deref()
    }
    /// Monotonic writer fence, absent for read-only opens.
    pub fn writer_epoch(&self) -> Option<u64> {
        self.writer_epoch
    }
    /// Retrieves an issuing backend's private typed token. This is not a
    /// substitute for checking its registration, expiry and revocation.
    pub fn handle<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.handle.downcast_ref()
    }
}

impl fmt::Debug for JournalLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JournalLease")
            .field("has_head", &self.head.is_some())
            .field("writer_epoch", &self.writer_epoch)
            .finish_non_exhaustive()
    }
}

/// Ordered persistent sequence descriptor. Empty is `(None, 0, 0)`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalSequence {
    /// Root node, absent only for an empty sequence.
    pub root: Option<JournalObjectId>,
    /// Total leaf item count.
    pub len: u64,
    /// Zero for a leaf; branches contain children exactly one level lower.
    pub height: u32,
}

/// Bounded immutable sequence node; append changes only a path to its root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub enum JournalSequenceNode {
    /// Ordered object references, at most [`JOURNAL_FANOUT`].
    Leaf { items: Vec<JournalObjectId> },
    /// Ordered subtree descriptors, at most [`JOURNAL_FANOUT`].
    Branch { children: Vec<JournalSequence> },
}

/// Persistent lexically ordered extension map descriptor.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalMap {
    /// Root node, absent only for an empty map.
    pub root: Option<JournalObjectId>,
    /// Number of key/value leaves in the whole subtree.
    pub len: u64,
    /// Zero for leaves; child heights decrease by one.
    pub height: u32,
}

/// One namespaced map leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalMapEntry {
    /// Exact namespace; entries must be strictly increasing by UTF-8 bytes.
    pub key: String,
    /// Immutable value in this same protection domain.
    pub value: JournalObjectId,
}

/// Ordered map subtree with its first-key separator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalMapChild {
    /// Exact first key in this nonempty child subtree.
    pub first_key: String,
    /// Bounded subtree descriptor.
    pub tree: JournalMap,
}

/// Bounded map node, not an ever-growing array of all namespace references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub enum JournalMapNode {
    /// At most 64 strictly ordered entries.
    Leaf { entries: Vec<JournalMapEntry> },
    /// At most 64 strictly ordered child separators.
    Branch { children: Vec<JournalMapChild> },
}

/// Complete roots/counters for a materialized session snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalSnapshotRoots {
    /// Every canonical message, in order (not just the active request prefix).
    pub history: JournalSequence,
    /// Domain-separated SHA-256 chain of the ordered history message IDs.
    pub history_chain: JournalDigest,
    /// Append-only accounted units, including failed attempts.
    pub usage: JournalSequence,
    /// Exact or projected versioned extension leaves, according to role.
    pub extensions: JournalMap,
    /// Retained planned-step diagnostic window; protected new writes omit it.
    pub manifests: JournalSequence,
    /// Lifetime planned-step counter, independent of retained window length.
    pub planned_step_count: u64,
    /// All monotonic execution/event identity counters.
    pub identity: SessionIdentityState,
    /// Snapshot timestamp, preserved independently of head/checkpoint timestamps.
    pub updated: Timestamp,
}

/// Host-authorized retirement evidence, never inferred from lease expiry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalRetirement {
    /// Session whose live root was removed.
    pub session: SessionId,
    /// Revision against which retirement was fenced.
    pub revision: JournalRevision,
    /// Root/pin epoch at the retirement visibility point.
    pub root_epoch: u64,
    /// Host-clock time of authorized retirement.
    pub retired_at: Timestamp,
}

/// Published version-4 boundary. Predecessor digests attest ordering but are
/// not reachability edges to all old batches/checkpoints (which may be collected).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalHead {
    /// Must equal [`JOURNAL_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// Protection domain of every reachable object.
    pub storage_id: JournalStorageId,
    /// Exact or host-projected view.
    pub role: JournalRole,
    /// Logical session identity.
    pub session: SessionId,
    /// Monotonic published revision.
    pub revision: JournalRevision,
    /// Execution lease fence that published this boundary.
    pub writer_epoch: u64,
    /// Append-only batch ordinal, starting at one.
    pub batch_sequence: u64,
    /// Current batch object; its metadata must match this head.
    pub batch: JournalObjectId,
    /// Complete snapshot roots, counters and diagnostic window.
    pub snapshot: JournalSnapshotRoots,
    /// Exact referenced checkpoint; forbidden in an ordinary head.
    pub checkpoint: Option<JournalObjectId>,
    /// Logical successor, not permission to delete this session.
    pub superseded_by: Option<SessionId>,
    /// Explicit host-authorized retirement evidence, if retired.
    pub retirement: Option<JournalRetirement>,
    /// Initial head creation timestamp.
    pub created: Timestamp,
    /// Publication timestamp.
    pub updated: Timestamp,
}

/// Non-content evidence for matching ordinary/exact boundaries. Roots can
/// differ after redaction; equality of hashes never proves policy correctness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalBoundary {
    /// Boundary-key schema (currently 1).
    pub version: u32,
    /// Logical session.
    pub session: SessionId,
    /// Active turn, absent for an idle accounting boundary.
    pub turn: Option<TurnId>,
    /// Protected checkpoint sequence (zero for ordinary-only boundaries).
    pub checkpoint_sequence: u64,
    /// Lifetime planned-step counter.
    pub planned_step_count: u64,
}

/// Typed changed records presented to host policy before computing stored IDs.
/// This is not permission for an ordinary store to install exact raw objects.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalTransitionDelta {
    /// Boundary evidence, never inferred from content hashes.
    pub boundary: JournalBoundary,
    /// Complete initial snapshot on bootstrap/import only.
    pub bootstrap: Option<Box<SessionSnapshot>>,
    /// Newly appended canonical messages.
    pub history_append: Vec<Message>,
    /// Newly charged usage records.
    pub usage_append: Vec<UsageRecord>,
    /// Replacements/removals; host policy may omit Sensitive values.
    pub extension_updates: BTreeMap<String, Option<VersionedSessionState>>,
    /// Newly produced diagnostics; retention decisions stay host-owned.
    pub manifests_append: Vec<TurnManifest>,
}

impl fmt::Debug for JournalTransitionDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JournalTransitionDelta")
            .field("bootstrap", &self.bootstrap.is_some())
            .field("history_append_count", &self.history_append.len())
            .field("usage_append_count", &self.usage_append.len())
            .field("extension_update_count", &self.extension_updates.len())
            .field("manifest_append_count", &self.manifests_append.len())
            .finish_non_exhaustive()
    }
}

/// Immutable batch metadata. Previous batch digest is evidence, not a root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalBatch {
    /// Session this batch advances.
    pub session: SessionId,
    /// Ordinal, starting at one.
    pub sequence: u64,
    /// Previous batch digest; absent only at sequence one.
    pub predecessor: Option<JournalDigest>,
    /// Revision expected before commit; absent only at create/import.
    pub expected_revision: Option<JournalRevision>,
    /// Monotonic exclusive-writer fence.
    pub writer_epoch: u64,
    /// Lifetime stable idempotency key.
    pub operation: JournalOperationId,
    /// Metadata boundary joining policy domains.
    pub boundary: JournalBoundary,
    /// Resulting complete roots in this stored policy domain.
    pub snapshot: JournalSnapshotRoots,
    /// Resulting exact checkpoint object, absent in the ordinary domain.
    pub checkpoint: Option<JournalObjectId>,
}

/// Candidate commit. Protected stores verify candidate objects exactly;
/// ordinary stores project the semantic delta and remap every resulting ref.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalCommit {
    /// Expected revision, fence, operation and resulting roots.
    pub batch: JournalBatch,
    /// Private idempotency digest of this entire input, not projected evidence.
    pub digest: JournalDigest,
    /// New immutable candidate drafts only, excluding unchanged bodies.
    pub objects: Vec<JournalObject>,
    /// Typed records on which ordinary host policy must act.
    pub delta: JournalTransitionDelta,
    /// Exact candidate checkpoint, forbidden for ordinary publication.
    pub checkpoint: Option<ReferencedTurnCheckpoint>,
}

/// Host-triggered collector request. No default schedule or retention TTL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalGcRequest {
    /// Explicit requested domain.
    pub storage_id: JournalStorageId,
    /// Mark/deletion root-and-pin epoch, rechecked transactionally.
    pub expected_root_epoch: u64,
    /// Host-chosen upper bound on objects to delete during this operation.
    pub max_objects: u64,
}

/// Metadata-only collection result; content IDs never enter default diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalGcReport {
    /// Transactionally verified root/pin epoch.
    pub root_epoch: u64,
    /// Number of objects examined.
    pub examined: u64,
    /// Number of unreachable objects deleted.
    pub deleted: u64,
    /// Logical encoded bytes reclaimed, not physical backend amplification.
    pub reclaimed_bytes: u64,
}

/// Final planned request, with independent persistent ordered message/tool refs.
/// Settings are a RequestSettings object containing a ProviderRequest with
/// empty messages/tools; reconstruction fills those lists without replanning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferencedProviderRequest {
    /// Every projected message, including summaries and contributed instructions.
    pub messages: JournalSequence,
    /// Every advertised schema in final planner order.
    pub tools: JournalSequence,
    /// Exact model, sampling/reasoning/output/cache/continuation settings.
    pub settings: JournalObjectId,
}

/// Representation of one state field; arbitrary lists use bounded sequences.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reference", rename_all = "snake_case")]
pub enum JournalStateValue {
    /// One exact StateValue object (prepared invocation, raw outcome or scalar).
    Object { object: JournalObjectId },
    /// Ordered StateValue objects (source calls, prepared slots, results).
    Sequence { sequence: JournalSequence },
    /// Exact Request object with persistent message/tool sequences.
    Request { object: JournalObjectId },
}

/// Reference representation of the unchanged internally tagged TurnState.
/// Readers reject unknown tags/fields by comparing the reconstructed typed
/// state's full wire value; serde's unknown-field tolerance is not authority.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferencedTurnState {
    /// Exact current TurnState `state` tag.
    pub tag: String,
    /// At most 64 named field references; large payloads are separate objects.
    pub fields: BTreeMap<String, JournalStateValue>,
}
impl fmt::Debug for ReferencedTurnState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReferencedTurnState")
            .field("field_count", &self.fields.len())
            .finish_non_exhaustive()
    }
}

/// Version-4 protected checkpoint envelope; materializes the existing owned
/// TurnCheckpoint/TurnState without planner/provider/tool calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferencedTurnCheckpoint {
    /// Must be 4.
    pub schema_version: u32,
    /// Must be the unchanged supported transition revision 4.
    pub transition_revision: u32,
    /// Logical session identity.
    pub session: SessionId,
    /// Turn identity.
    pub turn: TurnId,
    /// Monotonic state revision within the turn.
    pub state_revision: u64,
    /// Existing operation fingerprint, not a journal content address.
    pub operation_fingerprint: Fingerprint,
    /// Active canonical history boundary.
    pub active_history_start: usize,
    /// Exact InternalInput object if this is an internal turn.
    pub internal_input: Option<JournalObjectId>,
    /// Durable visible-output progress.
    pub visible_output: bool,
    /// TurnState reference object (not the materialized payload).
    pub state: JournalObjectId,
    /// Exact complete snapshot roots/counters.
    pub snapshot: JournalSnapshotRoots,
    /// Original absolute deadline.
    pub deadline: Deadline,
    /// Exact checkpoint/event progress, including cache-operation scope.
    pub watermark: CheckpointWatermark,
    /// Checkpoint timestamp.
    pub updated: Timestamp,
}
