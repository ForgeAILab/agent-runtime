//! Immutable semantic positions, shared by transition preparation and memo roots.
use super::persistent::Map;
use super::*;
use crate::content::Message;
use crate::store::{SessionIdentityState, SessionSnapshot, TurnManifest, VersionedSessionState};
use crate::usage::UsageRecord;
use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};
use serde::{Serialize, Serializer};
use std::sync::Arc;

pub(crate) trait SemanticLength {
    fn semantic_len(&self) -> Result<usize, crate::error::RuntimeError>;
}
impl SemanticLength for JournalObjectId {
    fn semantic_len(&self) -> Result<usize, crate::error::RuntimeError> {
        encoding::canonical_journal_json_len(self)
    }
}
impl SemanticLength for UsageRecord {
    fn semantic_len(&self) -> Result<usize, crate::error::RuntimeError> {
        encoding::canonical_journal_json_len(self)
    }
}
impl SemanticLength for TurnManifest {
    fn semantic_len(&self) -> Result<usize, crate::error::RuntimeError> {
        encoding::canonical_journal_json_len(self)
    }
}
impl SemanticLength for crate::provider::ToolSchema {
    fn semantic_len(&self) -> Result<usize, crate::error::RuntimeError> {
        encoding::canonical_journal_json_len(self)
    }
}
impl SemanticLength for VersionedSessionState {
    fn semantic_len(&self) -> Result<usize, crate::error::RuntimeError> {
        let empty = VersionedSessionState {
            revision: self.revision.clone(),
            sensitivity: self.sensitivity,
            value: serde_json::Value::Null,
        };
        encoding::canonical_journal_json_len(&empty)?
            .checked_sub(4)
            .and_then(|n| n.checked_add(encoding::canonical_journal_value_len(&self.value).ok()?))
            .ok_or_else(conflict)
    }
}
impl SemanticLength for Message {
    fn semantic_len(&self) -> Result<usize, crate::error::RuntimeError> {
        use crate::content::{ContentPart, ToolCall, ToolResultBlock};
        let empty = Message {
            role: self.role,
            content: Vec::new(),
        };
        let mut bytes = encoding::canonical_journal_json_len(&empty)?
            .checked_add(self.content.len().saturating_sub(1))
            .ok_or_else(conflict)?;
        let mut pending = self.content.iter().collect::<Vec<_>>();
        while let Some(part) = pending.pop() {
            let size = match part {
                ContentPart::ToolCall(call) => {
                    let empty = ContentPart::ToolCall(ToolCall {
                        id: call.id.clone(),
                        name: call.name.clone(),
                        arguments: serde_json::Value::Null,
                    });
                    encoding::canonical_journal_json_len(&empty)?
                        .checked_sub(4)
                        .and_then(|n| {
                            n.checked_add(
                                encoding::canonical_journal_value_len(&call.arguments).ok()?,
                            )
                        })
                        .ok_or_else(conflict)?
                }
                ContentPart::ToolResult(result) => {
                    let empty = ContentPart::ToolResult(ToolResultBlock {
                        call_id: result.call_id.clone(),
                        name: result.name.clone(),
                        content: Vec::new(),
                        is_error: result.is_error,
                    });
                    pending.extend(&result.content);
                    encoding::canonical_journal_json_len(&empty)?
                        .checked_add(result.content.len().saturating_sub(1))
                        .ok_or_else(conflict)?
                }
                scalar => encoding::canonical_journal_json_len(scalar)?,
            };
            bytes = bytes.checked_add(size).ok_or_else(conflict)?;
        }
        Ok(bytes)
    }
}

#[derive(Debug)]
struct Measured<T> {
    value: Arc<T>,
    bytes: usize,
}
#[derive(Debug)]
pub(crate) struct Sequence<T> {
    items: Map<usize, Arc<Measured<T>>>,
    start: usize,
    bytes: usize,
}
impl<T> Clone for Sequence<T> {
    fn clone(&self) -> Self {
        Self {
            items: self.items.clone(),
            start: self.start,
            bytes: self.bytes,
        }
    }
}
impl<T> Default for Sequence<T> {
    fn default() -> Self {
        Self {
            items: Map::default(),
            start: 0,
            bytes: 0,
        }
    }
}
impl<T> Sequence<T> {
    pub(crate) fn persistent_overhead(&self) -> usize {
        self.items
            .shallow_bytes()
            .saturating_add(self.len().saturating_mul(
                std::mem::size_of::<Measured<T>>()
                    + std::mem::size_of::<T>()
                    + 4 * std::mem::size_of::<usize>(),
            ))
    }
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    pub(crate) fn get(&self, index: usize) -> Option<&T> {
        self.items
            .get(&self.start.checked_add(index)?)
            .map(|v| v.value.as_ref())
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.values().map(|v| v.value.as_ref())
    }
    pub(crate) fn push_shared(
        &mut self,
        value: Arc<T>,
        bytes: usize,
    ) -> Result<(), crate::error::RuntimeError> {
        let total = self.bytes.checked_add(bytes).ok_or_else(conflict)?;
        let index = self.start.checked_add(self.len()).ok_or_else(conflict)?;
        total
            .checked_add(self.len())
            .and_then(|n| n.checked_add(2))
            .ok_or_else(conflict)?;
        self.bytes = total;
        self.items
            .insert(index, Arc::new(Measured { value, bytes }));
        Ok(())
    }
    pub(crate) fn canonical_len(&self) -> usize {
        2 + self.bytes + self.len().saturating_sub(1)
    }
    pub(crate) fn same(&self, other: &Self) -> bool {
        self.items.ptr_eq(&other.items) && self.start == other.start
    }
    pub(crate) fn truncate_front(&mut self, keep: usize) -> Result<(), crate::error::RuntimeError> {
        if keep > self.len() {
            return Err(conflict());
        }
        while self.len() > keep {
            let value = self.items.get(&self.start).ok_or_else(conflict)?;
            self.bytes = self.bytes.checked_sub(value.bytes).ok_or_else(conflict)?;
            self.items.remove(&self.start);
            self.start = self.start.checked_add(1).ok_or_else(conflict)?;
        }
        Ok(())
    }
}
impl<T: Serialize + SemanticLength> Sequence<T> {
    pub(crate) fn push(&mut self, value: T) -> Result<(), crate::error::RuntimeError> {
        let bytes = value.semantic_len()?;
        self.push_shared(Arc::new(value), bytes)
    }
    pub(crate) fn from_values(
        values: impl IntoIterator<Item = T>,
    ) -> Result<Self, crate::error::RuntimeError> {
        let mut next = Self::default();
        for value in values {
            next.push(value)?;
        }
        Ok(next)
    }
}
impl<T: Serialize> Serialize for Sequence<T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.len()))?;
        for value in self.iter() {
            seq.serialize_element(value)?;
        }
        seq.end()
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Extensions {
    values: Map<String, Arc<Measured<VersionedSessionState>>>,
    bytes: usize,
}
impl Extensions {
    pub(crate) fn same(&self, other: &Self) -> bool {
        self.values.ptr_eq(&other.values)
    }
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    pub(crate) fn get(&self, key: &str) -> Option<&VersionedSessionState> {
        self.values.get(&key.to_owned()).map(|v| v.value.as_ref())
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &VersionedSessionState)> {
        self.values.iter().map(|(k, v)| (k, v.value.as_ref()))
    }
    pub(crate) fn set(
        &mut self,
        key: String,
        value: Option<VersionedSessionState>,
    ) -> Result<(), crate::error::RuntimeError> {
        if let Some(old) = self.values.get(&key) {
            self.bytes = self.bytes.checked_sub(old.bytes).ok_or_else(conflict)?;
        }
        match value {
            Some(value) => {
                let bytes = encoding::canonical_journal_json_len(&key)?
                    .checked_add(1)
                    .and_then(|n| n.checked_add(value.semantic_len().ok()?))
                    .ok_or_else(conflict)?;
                self.bytes = self.bytes.checked_add(bytes).ok_or_else(conflict)?;
                self.values.insert(
                    key,
                    Arc::new(Measured {
                        value: Arc::new(value),
                        bytes,
                    }),
                );
            }
            None => self.values.remove(&key),
        }
        Ok(())
    }
    pub(crate) fn set_shared(
        &mut self,
        key: String,
        value: Arc<VersionedSessionState>,
        value_bytes: usize,
    ) -> Result<(), crate::error::RuntimeError> {
        if let Some(old) = self.values.get(&key) {
            self.bytes = self.bytes.checked_sub(old.bytes).ok_or_else(conflict)?;
        }
        let bytes = encoding::canonical_journal_json_len(&key)?
            .checked_add(1)
            .and_then(|n| n.checked_add(value_bytes))
            .ok_or_else(conflict)?;
        self.bytes = self.bytes.checked_add(bytes).ok_or_else(conflict)?;
        self.values.insert(key, Arc::new(Measured { value, bytes }));
        Ok(())
    }
    fn canonical_len(&self) -> usize {
        2 + self.bytes + self.len().saturating_sub(1)
    }
}
impl Serialize for Extensions {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(self.len()))?;
        for (key, value) in self.iter() {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

/// Immutable journal semantic view. It carries no storage or read authority.
/// Owned DTOs are materialized only at explicit API and full-policy boundaries.
#[derive(Clone)]
pub struct JournalSnapshotView {
    pub(crate) id: crate::ids::SessionId,
    pub(crate) history: Sequence<Message>,
    pub(crate) usage: Sequence<UsageRecord>,
    pub(crate) identity: SessionIdentityState,
    pub(crate) manifests: Sequence<TurnManifest>,
    pub(crate) extension_state: Extensions,
    pub(crate) updated: crate::clock::Timestamp,
}
impl JournalSnapshotView {
    /// Builds an immutable view from a complete owned snapshot. This is O(N).
    pub fn from_snapshot(snapshot: SessionSnapshot) -> Result<Self, crate::error::RuntimeError> {
        let mut extension_state = Extensions::default();
        for (key, value) in snapshot.extension_state {
            extension_state.set(key, Some(value))?;
        }
        Ok(Self {
            id: snapshot.id,
            history: Sequence::from_values(snapshot.history)?,
            usage: Sequence::from_values(snapshot.usage.records().iter().cloned())?,
            identity: snapshot.identity,
            manifests: Sequence::from_values(snapshot.manifests)?,
            extension_state,
            updated: snapshot.updated,
        })
    }
    /// Explicit complete DTO materialization, including every semantic position.
    pub fn materialize(&self) -> SessionSnapshot {
        #[cfg(feature = "test-support")]
        crate::journal::profile::event("rt.work.full_candidate_materializations", 1, 0);
        let mut usage = crate::usage::UsageLedger::new();
        for record in self.usage.iter() {
            usage.record(record.clone());
        }
        SessionSnapshot {
            id: self.id.clone(),
            history: self.history.iter().cloned().collect(),
            usage,
            identity: self.identity.clone(),
            manifests: self.manifests.iter().cloned().collect(),
            extension_state: self
                .extension_state
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            updated: self.updated,
        }
    }
    /// Returns the session identity; this is not a storage credential.
    pub fn session(&self) -> &crate::ids::SessionId {
        &self.id
    }
    /// Number of canonical history messages.
    pub fn history_len(&self) -> usize {
        self.history.len()
    }
    /// Reads one semantic message without flattening its sequence.
    pub fn message(&self, index: usize) -> Option<&Message> {
        self.history.get(index)
    }
    /// Number of charged usage positions.
    pub fn usage_len(&self) -> usize {
        self.usage.len()
    }
    /// Number of retained manifests.
    pub fn manifest_len(&self) -> usize {
        self.manifests.len()
    }
    /// Reads an extension value without materializing other namespaces.
    pub fn extension(&self, namespace: &str) -> Option<&VersionedSessionState> {
        self.extension_state.get(namespace)
    }
    /// Iterates history in canonical order without allocating a vector.
    pub fn history_messages(&self) -> impl Iterator<Item = &Message> {
        self.history.iter()
    }
    /// Iterates charged usage in canonical order.
    pub fn usage_records(&self) -> impl Iterator<Item = &UsageRecord> {
        self.usage.iter()
    }
    /// Reads one retained diagnostic record.
    pub fn manifest(&self, index: usize) -> Option<&TurnManifest> {
        self.manifests.get(index)
    }
    /// Iterates extension namespaces in canonical order.
    pub fn extensions(&self) -> impl Iterator<Item = (&String, &VersionedSessionState)> {
        self.extension_state.iter()
    }
    /// Number of retained extension namespaces.
    pub fn extension_len(&self) -> usize {
        self.extension_state.len()
    }
    pub(crate) fn same_version(&self, other: &Self) -> bool {
        self.id == other.id
            && self.identity == other.identity
            && self.updated == other.updated
            && self.history.same(&other.history)
            && self.usage.same(&other.usage)
            && self.manifests.same(&other.manifests)
            && self.extension_state.same(&other.extension_state)
    }
    /// Starts a mutation bound to this immutable semantic version.
    #[doc(hidden)]
    pub fn mutation(&self) -> JournalSnapshotMutation {
        JournalSnapshotMutation {
            base: self.clone(),
            history_append: vec![],
            usage_append: vec![],
            manifests_append: vec![],
            retained_manifests: None,
            extension_updates: Default::default(),
        }
    }
    pub(crate) fn persistent_overhead(&self) -> usize {
        self.history
            .persistent_overhead()
            .saturating_add(self.usage.persistent_overhead())
            .saturating_add(self.manifests.persistent_overhead())
            .saturating_add(self.extension_state.values.shallow_bytes())
            .saturating_add(self.extension_len().saturating_mul(
                std::mem::size_of::<Measured<VersionedSessionState>>()
                    + std::mem::size_of::<VersionedSessionState>()
                    + 4 * std::mem::size_of::<usize>(),
            ))
    }
    pub(crate) fn canonical_len(&self) -> Result<usize, crate::error::RuntimeError> {
        let empty = SessionSnapshot {
            id: self.id.clone(),
            history: vec![],
            usage: Default::default(),
            identity: self.identity.clone(),
            manifests: vec![],
            extension_state: Default::default(),
            updated: self.updated,
        };
        let mut bytes = encoding::canonical_journal_json_len(&empty)?;
        bytes = bytes
            .checked_add(self.history.canonical_len() - 2)
            .and_then(|n| n.checked_add(self.usage.canonical_len() - 2))
            .ok_or_else(conflict)?;
        if !self.manifests.is_empty() {
            bytes = bytes
                .checked_add(13 + self.manifests.canonical_len())
                .ok_or_else(conflict)?;
        }
        if !self.extension_state.is_empty() {
            bytes = bytes
                .checked_add(19 + self.extension_state.canonical_len())
                .ok_or_else(conflict)?;
        }
        Ok(bytes)
    }
}
impl Serialize for JournalSnapshotView {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut state = s.serialize_struct("SessionSnapshot", 7)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("history", &self.history)?;
        state.serialize_field("usage", &self.usage)?;
        state.serialize_field("identity", &self.identity)?;
        if !self.manifests.is_empty() {
            state.serialize_field("manifests", &self.manifests)?;
        }
        if !self.extension_state.is_empty() {
            state.serialize_field("extension_state", &self.extension_state)?;
        }
        state.serialize_field("updated", &self.updated)?;
        state.end()
    }
}

/// Typed changes anchored to an immutable base. Its fields cannot substitute a
/// different base after construction; applying it shares all unchanged nodes.
#[doc(hidden)]
#[derive(Debug)]
pub struct JournalSnapshotMutation {
    base: JournalSnapshotView,
    history_append: Vec<Message>,
    usage_append: Vec<UsageRecord>,
    manifests_append: Vec<TurnManifest>,
    retained_manifests: Option<usize>,
    extension_updates: std::collections::BTreeMap<String, Option<VersionedSessionState>>,
}
impl JournalSnapshotMutation {
    /// Adds canonical history positions.
    pub fn append_history(&mut self, values: impl IntoIterator<Item = Message>) {
        self.history_append.extend(values);
    }
    /// Adds charged usage positions.
    pub fn append_usage(&mut self, values: impl IntoIterator<Item = UsageRecord>) {
        self.usage_append.extend(values);
    }
    /// Adds diagnostics before applying the resulting retention limit.
    pub fn append_manifests(&mut self, values: impl IntoIterator<Item = TurnManifest>) {
        self.manifests_append.extend(values);
    }
    /// Sets the resulting number of retained diagnostic positions.
    pub fn retain_manifests(&mut self, count: usize) {
        self.retained_manifests = Some(count);
    }
    /// Replaces or removes one opaque namespace.
    pub fn set_extension(&mut self, key: String, value: Option<VersionedSessionState>) {
        self.extension_updates.insert(key, value);
    }
    /// Applies changes to their captured base and updates scalar boundary data.
    pub fn finish(
        self,
        identity: SessionIdentityState,
        updated: crate::clock::Timestamp,
    ) -> Result<JournalSnapshotView, crate::error::RuntimeError> {
        let mut next = self.base;
        for value in self.history_append {
            next.history.push(value)?;
        }
        for value in self.usage_append {
            next.usage.push(value)?;
        }
        for value in self.manifests_append {
            next.manifests.push(value)?;
        }
        if let Some(count) = self.retained_manifests {
            next.manifests.truncate_front(count)?;
        }
        for (key, value) in self.extension_updates {
            next.extension_state.set(key, value)?;
        }
        next.identity = identity;
        next.updated = updated;
        Ok(next)
    }
}

impl std::fmt::Debug for JournalSnapshotView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalSnapshotView")
            .field("history", &self.history_len())
            .field("usage", &self.usage_len())
            .field("manifests", &self.manifest_len())
            .field("extensions", &self.extension_len())
            .finish_non_exhaustive()
    }
}
