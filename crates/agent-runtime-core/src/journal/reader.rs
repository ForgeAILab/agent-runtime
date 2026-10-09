use std::collections::BTreeMap;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::*;
use crate::checkpoint::{TURN_TRANSITION_REVISION, TurnCheckpoint, TurnState};
use crate::content::{InternalTurnInput, Message};
use crate::error::RuntimeError;
use crate::provider::{ProviderRequest, ToolSchema};
use crate::store::{SessionSnapshot, TurnManifest, VersionedSessionState};
use crate::usage::{UsageLedger, UsageRecord};

/// Explicit host resource limits for one materialization. Counts include
/// repeated reads; no background policy/expiry or execution authority is added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalReadLimits {
    /// Maximum object reads for this reader instance.
    pub objects: u64,
    /// Maximum total encoded bytes read.
    pub encoded_bytes: u64,
    /// Maximum entries in any one materialized sequence/map.
    pub entries: u64,
}

/// Reads schema-3 or materialized schema-4 JSON, validating exact operation and
/// transition evidence before normalizing its schema tag to 4. This is a
/// representation conversion only; no execution or store write occurs.
pub fn read_checkpoint_json(bytes: &[u8]) -> Result<TurnCheckpoint, RuntimeError> {
    let mut checkpoint: TurnCheckpoint = serde_json::from_slice(bytes).map_err(|_| conflict())?;
    checkpoint.validate()?;
    checkpoint.schema_version = JOURNAL_SCHEMA_VERSION;
    Ok(checkpoint)
}

/// Reads v3-era unversioned SessionSnapshot JSON; no invented legacy tag is
/// required. Tagged journal heads must use their own reader, never serde's
/// unknown-field fallback to a stale inline snapshot.
pub fn read_snapshot_json(bytes: &[u8]) -> Result<SessionSnapshot, RuntimeError> {
    let value: Value = serde_json::from_slice(bytes).map_err(|_| conflict())?;
    if value.get("schema_version").is_some() {
        return Err(conflict());
    }
    serde_json::from_value(value).map_err(|_| conflict())
}

impl JournalHead {
    /// Parses a backend's published head record and validates its shape. Backend
    /// framing/checksum and atomic pin selection remain the backend's obligation.
    pub fn from_json(bytes: &[u8]) -> Result<Self, RuntimeError> {
        let value: Value = serde_json::from_slice(bytes).map_err(|_| conflict())?;
        let head: Self = serde_json::from_value(value.clone()).map_err(|_| conflict())?;
        if canonical_journal_json(&head)? != canonical_journal_json(&value)? {
            return Err(conflict());
        }
        head.validate()?;
        Ok(head)
    }

    /// Validates supported version, role, counts and retirement metadata. Full
    /// object closure, chain and batch evidence are checked by JournalReader.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.schema_version != JOURNAL_SCHEMA_VERSION
            || self.revision.0 == 0
            || self.writer_epoch == 0
            || self.batch_sequence == 0
            || self.storage_id.as_str().is_empty()
            || self.session.as_str().is_empty()
            || (self.role == JournalRole::Ordinary && self.checkpoint.is_some())
            || self.snapshot.planned_step_count < self.snapshot.manifests.len
        {
            return Err(conflict());
        }
        validate_sequence(&self.snapshot.history)?;
        validate_sequence(&self.snapshot.usage)?;
        validate_sequence(&self.snapshot.manifests)?;
        validate_tree(
            self.snapshot.extensions.root.as_ref(),
            self.snapshot.extensions.len,
            self.snapshot.extensions.height,
        )?;
        if let Some(retirement) = &self.retirement {
            if retirement.session != self.session || retirement.revision != self.revision {
                return Err(conflict());
            }
        }
        Ok(())
    }
}

fn validate_tree(
    root: Option<&JournalObjectId>,
    len: u64,
    height: u32,
) -> Result<(), RuntimeError> {
    if (root.is_none() != (len == 0))
        || (len == 0 && height != 0)
        || height > JOURNAL_MAX_TREE_HEIGHT
    {
        return Err(conflict());
    }
    Ok(())
}
fn validate_sequence(sequence: &JournalSequence) -> Result<(), RuntimeError> {
    validate_tree(sequence.root.as_ref(), sequence.len, sequence.height)
}
fn bounded(len: usize) -> Result<(), RuntimeError> {
    if len == 0 || len > JOURNAL_FANOUT {
        return Err(conflict());
    }
    Ok(())
}

/// Materializes only the head selected by an already pinned lease. No planner,
/// provider, tool, fallback scan or write is available to this reader. Errors
/// never expose object IDs or payloads, and missing published objects conflict.
pub struct JournalReader<'a> {
    journal: &'a dyn SessionJournal,
    lease: &'a JournalLease,
    limits: JournalReadLimits,
    reads: u64,
    bytes: u64,
}

impl std::fmt::Debug for JournalReader<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalReader")
            .field("has_head", &self.lease.head().is_some())
            .field("limits", &self.limits)
            .field("object_reads", &self.reads)
            .field("encoded_bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl<'a> JournalReader<'a> {
    /// Creates a reader scoped to a live backend lease. Domain/session matching
    /// is checked here; the backend checks token/pin/fence liveness on each read.
    pub fn new(
        journal: &'a dyn SessionJournal,
        lease: &'a JournalLease,
        limits: JournalReadLimits,
    ) -> Result<Self, RuntimeError> {
        if journal.storage_id() != *lease.storage_id() {
            return Err(conflict());
        }
        if let Some(head) = lease.head() {
            head.validate()?;
            if head.storage_id != *lease.storage_id() || head.session != *lease.session() {
                return Err(conflict());
            }
        }
        Ok(Self {
            journal,
            lease,
            limits,
            reads: 0,
            bytes: 0,
        })
    }

    async fn object<T: DeserializeOwned + Serialize>(
        &mut self,
        id: &JournalObjectId,
        kind: JournalObjectKind,
    ) -> Result<T, RuntimeError> {
        self.reads = self.reads.checked_add(1).ok_or_else(conflict)?;
        if self.reads > self.limits.objects {
            return Err(conflict());
        }
        let bytes = self
            .journal
            .read(self.lease, id)
            .await
            .map_err(|_| conflict())?;
        self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(conflict)?;
        if self.bytes > self.limits.encoded_bytes {
            return Err(conflict());
        }
        JournalObject {
            id: id.clone(),
            bytes,
        }
        .decode(kind)
    }

    async fn sequence(
        &mut self,
        sequence: &JournalSequence,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        validate_sequence(sequence)?;
        if sequence.len > self.limits.entries {
            return Err(conflict());
        }
        let mut pending = vec![sequence.clone()];
        let mut items = Vec::new();
        while let Some(tree) = pending.pop() {
            validate_sequence(&tree)?;
            let Some(root) = &tree.root else {
                continue;
            };
            let node: JournalSequenceNode = self.object(root, JournalObjectKind::Sequence).await?;
            match node {
                JournalSequenceNode::Leaf { items: leaves } => {
                    bounded(leaves.len())?;
                    if tree.height != 0 || leaves.len() as u64 != tree.len {
                        return Err(conflict());
                    }
                    items.extend(leaves);
                    if items.len() as u64 > sequence.len {
                        return Err(conflict());
                    }
                }
                JournalSequenceNode::Branch { children } => {
                    bounded(children.len())?;
                    if tree.height == 0 {
                        return Err(conflict());
                    }
                    let mut len = 0u64;
                    for child in &children {
                        validate_sequence(child)?;
                        if child.len == 0 || child.height != tree.height - 1 {
                            return Err(conflict());
                        }
                        len = len.checked_add(child.len).ok_or_else(conflict)?;
                    }
                    if len != tree.len {
                        return Err(conflict());
                    }
                    pending.extend(children.into_iter().rev());
                }
            }
        }
        if items.len() as u64 != sequence.len {
            return Err(conflict());
        }
        Ok(items)
    }

    async fn map(&mut self, map: &JournalMap) -> Result<Vec<JournalMapEntry>, RuntimeError> {
        validate_tree(map.root.as_ref(), map.len, map.height)?;
        if map.len > self.limits.entries {
            return Err(conflict());
        }
        // Each child carries an exclusive upper key separator inherited from its
        // next sibling/parent. This checks separators against actual leaf keys.
        let mut pending = vec![(map.clone(), None::<String>, None::<String>)];
        let mut entries: Vec<JournalMapEntry> = Vec::new();
        while let Some((tree, first, upper)) = pending.pop() {
            validate_tree(tree.root.as_ref(), tree.len, tree.height)?;
            let Some(root) = &tree.root else {
                continue;
            };
            let node: JournalMapNode = self.object(root, JournalObjectKind::Map).await?;
            match node {
                JournalMapNode::Leaf { entries: leaves } => {
                    bounded(leaves.len())?;
                    if tree.height != 0
                        || leaves.len() as u64 != tree.len
                        || first.as_ref().is_some_and(|key| key != &leaves[0].key)
                    {
                        return Err(conflict());
                    }
                    for leaf in leaves {
                        if leaf.key.is_empty()
                            || upper.as_ref().is_some_and(|key| &leaf.key >= key)
                            || entries.last().is_some_and(|prev| prev.key >= leaf.key)
                        {
                            return Err(conflict());
                        }
                        entries.push(leaf);
                        if entries.len() as u64 > map.len {
                            return Err(conflict());
                        }
                    }
                }
                JournalMapNode::Branch { children } => {
                    bounded(children.len())?;
                    if tree.height == 0
                        || first
                            .as_ref()
                            .is_some_and(|key| key != &children[0].first_key)
                    {
                        return Err(conflict());
                    }
                    let mut len = 0u64;
                    for (i, child) in children.iter().enumerate() {
                        validate_tree(child.tree.root.as_ref(), child.tree.len, child.tree.height)?;
                        if child.tree.len == 0
                            || child.tree.height != tree.height - 1
                            || child.first_key.is_empty()
                            || upper.as_ref().is_some_and(|key| &child.first_key >= key)
                            || (i > 0 && children[i - 1].first_key >= child.first_key)
                        {
                            return Err(conflict());
                        }
                        len = len.checked_add(child.tree.len).ok_or_else(conflict)?;
                    }
                    if len != tree.len {
                        return Err(conflict());
                    }
                    for (i, child) in children.iter().enumerate().rev() {
                        let child_upper = children
                            .get(i + 1)
                            .map(|next| next.first_key.clone())
                            .or_else(|| upper.clone());
                        pending.push((
                            child.tree.clone(),
                            Some(child.first_key.clone()),
                            child_upper,
                        ));
                    }
                }
            }
        }
        if entries.len() as u64 != map.len {
            return Err(conflict());
        }
        Ok(entries)
    }

    async fn batch(&mut self, head: &JournalHead) -> Result<JournalBatch, RuntimeError> {
        head.validate()?;
        if head.retirement.is_some() {
            return Err(conflict());
        }
        let batch: JournalBatch = self.object(&head.batch, JournalObjectKind::Batch).await?;
        if batch.session != head.session
            || batch.sequence != head.batch_sequence
            || batch.writer_epoch != head.writer_epoch
            || batch.snapshot != head.snapshot
            || batch.checkpoint != head.checkpoint
            || (batch.sequence == 1) != batch.predecessor.is_none()
            || (batch.sequence == 1) != batch.expected_revision.is_none()
            || batch.boundary.version != 1
            || batch.boundary.session != head.session
            || batch.boundary.planned_step_count != head.snapshot.planned_step_count
            || batch.operation.as_str().is_empty()
            || batch
                .expected_revision
                .map_or(Some(1), |rev| rev.0.checked_add(1))
                != Some(head.revision.0)
        {
            return Err(conflict());
        }
        Ok(batch)
    }

    async fn snapshot_roots(
        &mut self,
        session: &crate::ids::SessionId,
        roots: &JournalSnapshotRoots,
    ) -> Result<SessionSnapshot, RuntimeError> {
        let history_ids = self.sequence(&roots.history).await?;
        if journal_history_chain(&history_ids) != roots.history_chain
            || roots.planned_step_count < roots.manifests.len
        {
            return Err(conflict());
        }
        let mut history = Vec::new();
        for id in history_ids {
            history.push(
                self.object::<Message>(&id, JournalObjectKind::Message)
                    .await?,
            );
        }
        let mut usage = UsageLedger::new();
        for id in self.sequence(&roots.usage).await? {
            usage.record(
                self.object::<UsageRecord>(&id, JournalObjectKind::UsageRecord)
                    .await?,
            );
        }
        let mut manifests = Vec::new();
        for id in self.sequence(&roots.manifests).await? {
            manifests.push(
                self.object::<TurnManifest>(&id, JournalObjectKind::Manifest)
                    .await?,
            );
        }
        let mut extension_state = BTreeMap::new();
        for entry in self.map(&roots.extensions).await? {
            extension_state.insert(
                entry.key,
                self.object::<VersionedSessionState>(&entry.value, JournalObjectKind::Extension)
                    .await?,
            );
        }
        Ok(SessionSnapshot {
            id: session.clone(),
            history,
            usage,
            identity: roots.identity.clone(),
            manifests,
            extension_state,
            updated: roots.updated,
        })
    }

    async fn request(&mut self, id: &JournalObjectId) -> Result<ProviderRequest, RuntimeError> {
        let refs: ReferencedProviderRequest = self.object(id, JournalObjectKind::Request).await?;
        let mut request: ProviderRequest = self
            .object(&refs.settings, JournalObjectKind::RequestSettings)
            .await?;
        if !request.messages.is_empty() || !request.tools.is_empty() {
            return Err(conflict());
        }
        for id in self.sequence(&refs.messages).await? {
            request.messages.push(
                self.object::<Message>(&id, JournalObjectKind::Message)
                    .await?,
            );
        }
        for id in self.sequence(&refs.tools).await? {
            request.tools.push(
                self.object::<ToolSchema>(&id, JournalObjectKind::ToolSchema)
                    .await?,
            );
        }
        request.validate_cache_identity().map_err(|_| conflict())?;
        Ok(request)
    }

    async fn state(&mut self, id: &JournalObjectId) -> Result<TurnState, RuntimeError> {
        let refs: ReferencedTurnState = self.object(id, JournalObjectKind::TurnState).await?;
        if refs.fields.len() > JOURNAL_FANOUT || refs.fields.contains_key("state") {
            return Err(conflict());
        }
        let mut fields = serde_json::Map::new();
        fields.insert("state".to_owned(), Value::String(refs.tag.clone()));
        for (name, reference) in refs.fields {
            let request_field = refs.tag == "calling_model" && name == "request";
            if request_field != matches!(&reference, JournalStateValue::Request { .. }) {
                return Err(conflict());
            }
            let value = match reference {
                JournalStateValue::Object { object } => {
                    let value = self
                        .object::<Value>(&object, JournalObjectKind::StateValue)
                        .await?;
                    if value.is_array() {
                        return Err(conflict());
                    }
                    value
                }
                JournalStateValue::Sequence { sequence } => {
                    let mut values = Vec::new();
                    for object in self.sequence(&sequence).await? {
                        values.push(
                            self.object::<Value>(&object, JournalObjectKind::StateValue)
                                .await?,
                        );
                    }
                    Value::Array(values)
                }
                JournalStateValue::Request { object } => {
                    if refs.tag != "calling_model" || name != "request" {
                        return Err(conflict());
                    }
                    serde_json::to_value(self.request(&object).await?).map_err(|_| conflict())?
                }
            };
            fields.insert(name, value);
        }
        let value = Value::Object(fields);
        let state: TurnState = serde_json::from_value(value.clone()).map_err(|_| conflict())?;
        if serde_json::to_value(&state).map_err(|_| conflict())? != value {
            return Err(conflict());
        }
        Ok(state)
    }

    /// Materializes the selected stored view, preserving ordinary redaction.
    /// `None` means an absent-head reservation; it never scans legacy records.
    pub async fn snapshot(&mut self) -> Result<Option<SessionSnapshot>, RuntimeError> {
        let Some(head) = self.lease.head().cloned() else {
            return Ok(None);
        };
        self.batch(&head).await?;
        let snapshot = self.snapshot_roots(&head.session, &head.snapshot).await?;
        // A snapshot read also validates any published execution root's closure.
        if head.checkpoint.is_some() {
            self.checkpoint_with_snapshot(&head, snapshot.clone())
                .await?;
        }
        Ok(Some(snapshot))
    }

    /// Materializes exact protected execution and validates the unchanged
    /// operation/transition table. Ordinary heads cannot supply execution roots.
    pub async fn checkpoint(&mut self) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        let Some(head) = self.lease.head().cloned() else {
            return Ok(None);
        };
        self.batch(&head).await?;
        let snapshot = self.snapshot_roots(&head.session, &head.snapshot).await?;
        if head.checkpoint.is_none() {
            return Ok(None);
        }
        self.checkpoint_with_snapshot(&head, snapshot)
            .await
            .map(Some)
    }

    async fn checkpoint_with_snapshot(
        &mut self,
        head: &JournalHead,
        snapshot: SessionSnapshot,
    ) -> Result<TurnCheckpoint, RuntimeError> {
        if head.role != JournalRole::Protected {
            return Err(conflict());
        }
        let id = head.checkpoint.as_ref().ok_or_else(conflict)?;
        let refs: ReferencedTurnCheckpoint = self.object(id, JournalObjectKind::Checkpoint).await?;
        if refs.schema_version != JOURNAL_SCHEMA_VERSION
            || refs.transition_revision != TURN_TRANSITION_REVISION
            || refs.session != head.session
            || refs.snapshot != head.snapshot
        {
            return Err(conflict());
        }
        let batch = self.batch(head).await?;
        if batch.boundary.turn.as_ref() != Some(&refs.turn)
            || batch.boundary.checkpoint_sequence != refs.watermark.checkpoint_sequence
        {
            return Err(conflict());
        }
        let internal_input: Option<InternalTurnInput> = match &refs.internal_input {
            Some(id) => Some(self.object(id, JournalObjectKind::InternalInput).await?),
            None => None,
        };
        let checkpoint = TurnCheckpoint {
            schema_version: refs.schema_version,
            transition_revision: refs.transition_revision,
            session: refs.session,
            turn: refs.turn,
            state_revision: refs.state_revision,
            operation_fingerprint: refs.operation_fingerprint,
            active_history_start: refs.active_history_start,
            internal_input,
            visible_output: refs.visible_output,
            state: self.state(&refs.state).await?,
            snapshot,
            deadline: refs.deadline,
            watermark: refs.watermark,
            updated: refs.updated,
        };
        checkpoint.validate().map_err(|_| conflict())?;
        Ok(checkpoint)
    }
}
