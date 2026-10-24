use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use agent_runtime_registry::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{JournalDigest, JournalObject, JournalObjectId, conflict};
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
/// Maximum canonical JSON payload stored inline in a persistent JSON parent.
///
/// Values at or below 1,024 bytes avoid scalar/key subobjects and 64-hex
/// reference. Larger values remain independently addressed so their unchanged
/// subtrees can be shared. The explicit [`JournalJsonItem`] tag, never the
/// user's JSON shape, selects inline data versus an object reference.
pub const JOURNAL_INLINE_JSON_MAX_BYTES: usize = 1_024;
/// Runtime-owned redaction-safe cache predecessor stored structurally in native journals.
///
/// Other extension namespaces retain their opaque whole-value replacement contract.
pub const RUNTIME_PREVIOUS_CACHE_NAMESPACE: &str = "runtime.core.previous_cache";

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

/// One persistent JSON child, explicitly distinguished from user JSON data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "storage", rename_all = "snake_case")]
pub enum JournalJsonItem {
    /// A compact JSON value stored directly in its containing node.
    Inline { value: Value },
    /// A larger persistent JSON value stored as a typed `JsonValue` object.
    Reference { object: JournalObjectId },
}

/// Compact Z85 form of a 32-byte object ID inside JSON-only branch nodes.
/// Public object identities remain canonical 64-hex [`JournalObjectId`] values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JournalCompactObjectId(String);

impl JournalCompactObjectId {
    /// Encodes one canonical object ID without padding.
    pub fn from_object(id: &JournalObjectId) -> Self {
        const ALPHABET: &[u8; 85] =
            b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
        let mut encoded = Vec::with_capacity(40);
        for chunk in id.to_bytes().chunks_exact(4) {
            let mut value = u32::from_be_bytes(chunk.try_into().expect("four-byte chunk"));
            let mut digits = [0u8; 5];
            for digit in digits.iter_mut().rev() {
                *digit = ALPHABET[(value % 85) as usize];
                value /= 85;
            }
            encoded.extend_from_slice(&digits);
        }
        Self(String::from_utf8(encoded).expect("Z85 alphabet is ASCII"))
    }

    /// Restores the canonical object ID and rejects noncanonical Z85.
    pub fn to_object(&self) -> Result<JournalObjectId, crate::error::RuntimeError> {
        const ALPHABET: &[u8; 85] =
            b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
        if self.0.len() != 40 {
            return Err(conflict());
        }
        let mut bytes = [0u8; 32];
        for (chunk, output) in self
            .0
            .as_bytes()
            .chunks_exact(5)
            .zip(bytes.chunks_exact_mut(4))
        {
            let mut value = 0u64;
            for byte in chunk {
                let digit = ALPHABET
                    .iter()
                    .position(|candidate| candidate == byte)
                    .ok_or_else(conflict)? as u64;
                value = value
                    .checked_mul(85)
                    .and_then(|value| value.checked_add(digit))
                    .ok_or_else(conflict)?;
            }
            let value = u32::try_from(value).map_err(|_| conflict())?;
            output.copy_from_slice(&value.to_be_bytes());
        }
        if Self::from_object(&JournalObjectId::from_bytes(bytes)).0 != self.0 {
            return Err(conflict());
        }
        Ok(JournalObjectId::from_bytes(bytes))
    }
}

/// Bounded immutable sequence node; append changes only a path to its root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub enum JournalSequenceNode {
    /// Ordered object references, at most [`JOURNAL_FANOUT`].
    Leaf { items: Vec<JournalObjectId> },
    /// Ordered persistent JSON children, at most [`JOURNAL_FANOUT`].
    ///
    /// This variant is valid only below [`JournalJsonValue::Array`]. Generic
    /// typed sequences reject it instead of interpreting a user's value shape.
    #[serde(rename = "jl")]
    JsonLeaf {
        #[serde(rename = "i")]
        items: Vec<JournalJsonItem>,
    },
    /// Bottom binary branch for arrays wider than one fanout leaf. Its JSON
    /// children are logical height-zero leaves stored directly in this node.
    #[serde(rename = "jv")]
    JsonValueBranch {
        #[serde(rename = "i")]
        items: Vec<JournalJsonItem>,
    },
    /// Binary persistent-JSON branch with child lengths/heights inferred from
    /// its parent descriptor. Omitting repeated descriptor fields keeps each
    /// changed path compact while retaining typed, schema-driven traversal.
    #[serde(rename = "jb")]
    JsonBranch {
        #[serde(rename = "c")]
        children: Vec<JournalCompactObjectId>,
    },
    /// Ordered subtree descriptors, at most [`JOURNAL_FANOUT`].
    Branch { children: Vec<JournalSequence> },
}

/// Persistent JSON array root stored directly in its `JsonValue` parent.
/// Internal descendants remain content-addressed sequence objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalJsonArray {
    /// Total logical JSON item count.
    pub len: u64,
    /// Logical root height; zero is an inline root leaf.
    pub height: u32,
    /// Typed inline root node. Generic reference leaves/branches are invalid.
    pub node: JournalSequenceNode,
}

/// Compact internal persistent-JSON branch. Its dedicated envelope kind makes
/// references unambiguous without repeating a sequence-node discriminator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JournalJsonBranch(pub Vec<JournalCompactObjectId>);

/// Compact JSON data stored in a dedicated typed object envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JournalJsonInline(pub Value);

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

/// A persistent JSON node used only by native journal-owned structured records.
///
/// Arrays and objects use the existing bounded persistent sequence encoding so
/// an unchanged prefix remains reachable by reference. Arbitrary host-owned
/// opaque extensions do not use this representation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "value", rename_all = "snake_case")]
pub enum JournalJsonValue {
    /// Null, boolean, number or string. Containers are rejected by readers.
    Scalar { scalar: Value },
    /// Ordered array entries.
    Array { items: JournalJsonArray },
    /// Ordered object entries, preserving insertion order when available.
    Object { entries: JournalSequence },
}

/// One ordered key/value edge in a persistent JSON object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalJsonObjectEntry {
    /// Exact JSON object key; empty keys remain valid JSON keys.
    pub key: String,
    /// Compact inline value or a typed persistent JSON reference in this domain.
    pub value: JournalJsonItem,
}

/// Runtime-owned extension wrapper whose exact public value materializes unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferencedSessionState {
    /// Root persistent JSON node for one `VersionedSessionState`.
    pub value: JournalObjectId,
}

/// Redaction-safe manifest wrapper whose public value materializes unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferencedTurnManifest {
    /// Root persistent JSON node for one `TurnManifest`.
    pub value: JournalObjectId,
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
    /// Complete snapshot on bootstrap/import, or a complete replacement of an
    /// existing head whose history cannot be expressed as an append (a shorter
    /// history). With it, every append/update/reference field is empty; the
    /// ordinary role projects it under host policy like a bootstrap.
    pub bootstrap: Option<Box<SessionSnapshot>>,
    /// Newly appended canonical messages.
    pub history_append: Vec<Message>,
    /// Newly charged usage records.
    pub usage_append: Vec<UsageRecord>,
    /// Replacements/removals; host policy may omit Sensitive values.
    pub extension_updates: BTreeMap<String, Option<VersionedSessionState>>,
    /// Runtime-owned structured replacements/removals, referenced from candidate drafts.
    /// Keys MUST be disjoint from `extension_updates`. Ordinary projection MUST
    /// preserve these redaction-safe values exactly; arbitrary host-owned
    /// opaque extensions continue to use `extension_updates`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extension_references: BTreeMap<String, Option<JournalObjectId>>,
    /// Newly produced diagnostics; retention decisions stay host-owned.
    pub manifests_append: Vec<TurnManifest>,
    /// Newly produced structured diagnostics, referenced from candidate drafts.
    /// Ordinary projection MUST preserve these redaction-safe records exactly.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub manifest_references: Vec<JournalObjectId>,
}

impl fmt::Debug for JournalTransitionDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JournalTransitionDelta")
            .field("bootstrap", &self.bootstrap.is_some())
            .field("history_append_count", &self.history_append.len())
            .field("usage_append_count", &self.usage_append.len())
            .field("extension_update_count", &self.extension_updates.len())
            .field(
                "extension_reference_count",
                &self.extension_references.len(),
            )
            .field(
                "manifest_append_count",
                &(self.manifests_append.len() + self.manifest_references.len()),
            )
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
///
/// Collection is bounded per call: a successful call may yield before it has
/// examined every dirty session. Hosts loop while [`JournalGcReport::has_more`],
/// refreshing `expected_root_epoch` from the engine before each request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalGcRequest {
    /// Explicit requested domain.
    pub storage_id: JournalStorageId,
    /// Mark/deletion root-and-pin epoch, rechecked transactionally.
    pub expected_root_epoch: u64,
    /// Host-chosen upper bound on memberships released during this operation.
    /// Pack rewrites may also remove bodies released by earlier collections.
    pub max_objects: u64,
    /// Cooperative per-call work budget. A missing serialized budget selects
    /// the bounded defaults; a host may request less, never unbounded work.
    #[serde(default)]
    pub work_budget: JournalGcBudget,
}

impl JournalGcRequest {
    /// Request with the default bounded work budget.
    pub fn new(storage_id: JournalStorageId, expected_root_epoch: u64, max_objects: u64) -> Self {
        Self {
            storage_id,
            expected_root_epoch,
            max_objects,
            work_budget: JournalGcBudget::default(),
        }
    }
    /// Replaces the per-call work budget.
    pub fn with_budget(mut self, work_budget: JournalGcBudget) -> Self {
        self.work_budget = work_budget;
        self
    }
}

/// Cooperative bound on one collection call. Values above the runtime limits
/// are clamped to them; zero selects the minimum positive bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct JournalGcBudget {
    /// Soft elapsed target from call entry, including gate wait, in milliseconds.
    /// The runtime reserves the measured cost of the next unit before starting it.
    pub target_millis: u64,
    /// Bounded work units (discovery page, trace batch, page, dependency index,
    /// release batch) this call may start.
    pub max_work_units: u64,
    /// Ids or records per discovery/trace/inventory/pin unit.
    pub max_unit_items: u32,
}

impl JournalGcBudget {
    /// Runtime default and maximum elapsed target.
    pub const DEFAULT_TARGET_MILLIS: u64 = 250;
    /// Runtime maximum ids or records per bounded unit.
    pub const MAX_UNIT_ITEMS: u32 = 256;
}

impl Default for JournalGcBudget {
    fn default() -> Self {
        Self {
            target_millis: Self::DEFAULT_TARGET_MILLIS,
            max_work_units: u64::MAX,
            max_unit_items: Self::MAX_UNIT_ITEMS,
        }
    }
}

/// Progress of the engine's private collection pass after one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalGcProgress {
    /// The captured dirty pass finished. Concurrent work can require another pass.
    Complete,
    /// The call stopped at its budget; the private cursor resumes next call.
    Yielded,
    /// Stale speculative mark state was discarded during this call (a root,
    /// pin, fence or epoch changed); nothing was released from the stale mark.
    Restarted,
}

/// Metadata-only collection result; content IDs never enter default diagnostics.
/// Counts, receipts and unreadable sessions describe this call only; a host
/// drain accumulates them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalGcReport {
    /// Transactionally verified root/pin epoch.
    pub root_epoch: u64,
    /// Number of objects examined.
    pub examined: u64,
    /// Number of physical body copies deleted, including deferred dead bodies.
    pub deleted: u64,
    /// Canonical encoded body bytes physically removed; excludes compression
    /// and backend amplification. Deferred bodies count at physical removal.
    pub reclaimed_bytes: u64,
    /// Sessions whose state could not be read or validated; hosts may log each once.
    /// Sorted and unique, without content/object ids. Only non-retryable,
    /// non-conflict failures and proven invalid state (including incomplete
    /// live membership) are reported. Dirty hints survive for the next call.
    /// A source blocked by an unreadable dependency is included with that dependency.
    /// A process-local lease over bodies already collected by another engine,
    /// or a recreated historical pin with missing bodies, is reported on every
    /// pass while it persists. Reporting does not repair cross-engine retention.
    #[serde(default)]
    pub unreadable_sessions: Vec<SessionId>,
    /// Pass progress; `None` decodes historical reports (unknown progress).
    #[serde(default)]
    pub progress: Option<JournalGcProgress>,
    /// Whether the host should call again to continue the current pass.
    /// Historical reports decode as `false` (no continuation).
    #[serde(default)]
    pub has_more: bool,
    /// Bounded work units started by this call.
    #[serde(default)]
    pub work_units: u64,
    /// Consecutive calls, including this one, that started no work unit (for
    /// example because the membership-release budget was already spent). A
    /// host may back off while it grows; zero means this call progressed.
    #[serde(default)]
    pub zero_work_calls: u64,
    /// Sessions whose mark exceeded the engine's cursor workspace cap in this
    /// call. Each is skipped with its dirty state retained; sorted and unique.
    #[serde(default)]
    pub over_capacity_sessions: Vec<SessionId>,
}

impl JournalGcReport {
    /// Host-drain migration helper: adds one bounded call's per-call counts,
    /// receipts and unreadable sessions (kept sorted and unique) to a running
    /// total. Progress and continuation come from the latest call; the root
    /// epoch stays the first call's mark.
    pub fn accumulate(&mut self, next: JournalGcReport) {
        self.examined += next.examined;
        self.deleted += next.deleted;
        self.reclaimed_bytes += next.reclaimed_bytes;
        self.work_units += next.work_units;
        self.zero_work_calls = next.zero_work_calls;
        self.unreadable_sessions.extend(next.unreadable_sessions);
        self.unreadable_sessions.sort();
        self.unreadable_sessions.dedup();
        self.over_capacity_sessions
            .extend(next.over_capacity_sessions);
        self.over_capacity_sessions.sort();
        self.over_capacity_sessions.dedup();
        self.progress = next.progress;
        self.has_more = next.has_more;
    }
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
