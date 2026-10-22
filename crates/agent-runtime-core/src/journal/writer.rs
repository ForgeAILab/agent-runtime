//! Canonical writer preparation and immutable, hash-bound semantic evidence.
use super::*;
use crate::checkpoint::{TurnCheckpoint, TurnState};
use crate::content::{ContentPart, Message};
use crate::error::RuntimeError;
use crate::provider::ProviderRequest;
use crate::store::{SessionSnapshot, TurnManifest, VersionedSessionState};
use serde::Serialize;
use serde_json::Value;
use std::any::{Any, TypeId};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
mod source;
use source::{ArraySource, JsonSource};
#[derive(Clone, Default)]
pub struct JournalPreparationHints {
    pub(crate) manifests: BTreeMap<JournalObjectId, Arc<ManifestEvidence>>,
    pub(crate) extensions: BTreeMap<JournalObjectId, Arc<ExtensionEvidence>>,
    pub(crate) states: BTreeMap<JournalObjectId, Arc<StateEvidence>>,
}
impl std::fmt::Debug for JournalPreparationHints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalPreparationHints")
            .field("manifests", &self.manifests.len())
            .field("extensions", &self.extensions.len())
            .finish()
    }
}
#[derive(Clone)]
pub(crate) enum StateValueView {
    Owned(Arc<TurnState>),
    Checkpoint(Arc<crate::checkpoint::CheckpointMetadata>),
}
impl std::ops::Deref for StateValueView {
    type Target = TurnState;
    fn deref(&self) -> &TurnState {
        match self {
            Self::Owned(state) => state,
            Self::Checkpoint(metadata) => &metadata.state,
        }
    }
}
pub(crate) struct StateEvidence {
    pub(crate) value: StateValueView,
    pub(crate) fingerprint: agent_runtime_registry::Fingerprint,
    pub(crate) encoded: Vec<u8>,
}
pub(crate) struct ManifestEvidence {
    bound_value: Arc<TurnManifest>,
    witness: Arc<JsonWitness>,
    pub(crate) value: Arc<TurnManifest>,
    pub(crate) bytes: usize,
}
pub(crate) struct ExtensionEvidence {
    bound_value: Arc<VersionedSessionState>,
    witness: Arc<JsonWitness>,
    pub(crate) namespace: String,
    pub(crate) value: Arc<VersionedSessionState>,
    pub(crate) bytes: usize,
}
fn state_identical(
    left: Option<&crate::store::VersionedSessionState>,
    right: Option<&crate::store::VersionedSessionState>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => {
            left.revision == right.revision
                && left.sensitivity == right.sensitivity
                && journal_json_identical(&left.value, &right.value)
        }
        (None, None) => true,
        _ => false,
    }
}

fn retained_overlap_view(previous: &JournalSnapshotView, next: &[TurnManifest]) -> usize {
    let max = previous.manifest_len().min(next.len());
    (0..=max)
        .rev()
        .find(|overlap| {
            (0..*overlap)
                .all(|i| previous.manifest(previous.manifest_len() - overlap + i) == Some(&next[i]))
        })
        .unwrap_or(0)
}

pub(crate) fn messages_match(left: &[Message], right: &[Message]) -> bool {
    left == right
        && left.iter().zip(right).all(|(left, right)| {
            let Message {
                role: _,
                content: left,
            } = left;
            let Message {
                role: _,
                content: right,
            } = right;
            parts_match(left, right)
        })
}

fn parts_match(left: &[ContentPart], right: &[ContentPart]) -> bool {
    left.iter()
        .zip(right)
        .all(|(left, right)| match (left, right) {
            (ContentPart::ToolCall(left), ContentPart::ToolCall(right)) => {
                numbers_match(&left.arguments, &right.arguments)
            }
            (ContentPart::ToolResult(left), ContentPart::ToolResult(right)) => {
                parts_match(&left.content, &right.content)
            }
            (ContentPart::Text { text: _ }, ContentPart::Text { text: _ })
            | (
                ContentPart::Reasoning {
                    text: _,
                    redacted: _,
                    signature: _,
                    producer: _,
                },
                ContentPart::Reasoning {
                    text: _,
                    redacted: _,
                    signature: _,
                    producer: _,
                },
            )
            | (
                ContentPart::Image { url: _, detail: _ },
                ContentPart::Image { url: _, detail: _ },
            ) => true,
            (
                ContentPart::Text { .. }
                | ContentPart::Reasoning { .. }
                | ContentPart::Image { .. }
                | ContentPart::ToolCall(_)
                | ContentPart::ToolResult(_),
                _,
            ) => false,
        })
}

fn numbers_match(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    use serde_json::Value;
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => {
            left.as_f64().map(f64::to_bits) == right.as_f64().map(f64::to_bits)
        }
        (Value::Array(left), Value::Array(right)) => left
            .iter()
            .zip(right)
            .all(|(left, right)| numbers_match(left, right)),
        (Value::Object(left), Value::Object(right)) => {
            left.iter()
                .zip(right)
                .all(|((key, value), (right_key, right_value))| {
                    key == right_key && numbers_match(value, right_value)
                })
        }
        _ => true,
    }
}
#[derive(Clone)]
pub struct JournalWriterEncoding {
    roots: JournalSnapshotRoots,
    view: JournalSnapshotView,
    history_view: JournalSequenceView,
    usage_view: JournalSequenceView,
    manifest_ids: Vec<JournalObjectId>,
    extension_ids: BTreeMap<String, JournalObjectId>,
    manifest_json: Vec<JournalJsonEncoding>,
    extension_json: BTreeMap<String, JournalJsonEncoding>,
    mutation_hint: Option<JournalWriterMutationHint>,
    request_cache: Option<Arc<RequestEncodingCache>>,
}

#[derive(Clone)]
enum RequestSource {
    Checkpoint(Arc<crate::checkpoint::CheckpointMetadata>),
    Owned(Arc<ProviderRequest>),
}
impl RequestSource {
    fn request(&self) -> &ProviderRequest {
        match self {
            Self::Checkpoint(metadata) => match &metadata.state {
                TurnState::CallingModel { request, .. } => request,
                _ => unreachable!(),
            },
            Self::Owned(request) => request,
        }
    }
}
#[derive(Clone)]
struct RequestEncodingCache {
    source: Arc<RequestSource>,
    message_view: JournalSequenceView,
    tool_view: JournalSequenceView,
}
#[derive(Clone)]
struct JournalWriterMutationHint {
    base: JournalSnapshotView,
    history_start: usize,
    usage_start: usize,
    manifest_overlap: usize,
    extension_changes: BTreeSet<String>,
}
enum TypedJsonSource {
    Manifest(Arc<TurnManifest>),
    State {
        namespace: String,
        value: Arc<VersionedSessionState>,
    },
}
// This proof binds private typed construction to one immutable input/encoding
// pair. It conveys semantic equality only, never storage membership or authority.
struct TypedJsonProof {
    source: TypedJsonSource,
    root: JournalObjectId,
    node: Arc<JsonEncodingNode>,
    witness: Arc<JsonWitness>,
    canonical_len: usize,
    depth: u32,
}
impl TypedJsonProof {
    fn binds(&self, encoding: &JournalJsonEncoding) -> bool {
        self.root == encoding.root
            && self.root == encoding.witness.object.id
            && Arc::ptr_eq(&self.node, &encoding.node)
            && Arc::ptr_eq(&self.witness, &encoding.witness)
            && self.canonical_len == encoding.canonical_len
            && self.depth == encoding.depth
    }
}

#[derive(Clone)]
pub struct JournalJsonEncoding {
    root: JournalObjectId,
    node: Arc<JsonEncodingNode>,
    witness: Arc<JsonWitness>,
    canonical_len: usize,
    typed: Option<Arc<TypedJsonProof>>,
    depth: u32,
}

#[derive(Clone)]
enum JsonEncodingNode {
    Inline,
    Scalar,
    Array(JsonArrayEncodingCache),
    Object {
        entries: Vec<JsonObjectEntryEncodingCache>,
        sequence: JournalSequence,
    },
}

#[derive(Clone)]
struct JsonArrayEncodingCache {
    items: super::persistent::Map<usize, JsonItemEncodingCache>,
    levels: super::persistent::Map<(usize, usize), JournalSequence>,
    level_counts: Vec<usize>,
    root: JournalJsonArray,
    canonical_len: usize,
    depth_counts: BTreeMap<u32, usize>,
}

#[derive(Clone)]
struct JsonObjectEntryEncodingCache {
    key: String,
    item: JsonItemEncodingCache,
    object: JournalObjectId,
}

#[derive(Clone)]
enum JsonItemEncodingCache {
    Inline { bytes: usize, depth: u32 },
    Reference(Arc<JournalJsonEncoding>),
}

#[derive(Default)]
pub struct JournalWriterAssembler {
    pub objects: BTreeMap<JournalObjectId, JournalObject>,
    /// Candidate structured metadata reachable from reference-only deltas.
    pub structured: BTreeSet<JournalObjectId>,
    hints: JournalPreparationHints,
    json_witnesses: BTreeMap<JournalObjectId, Arc<JsonWitness>>,
    request_cache: Option<Arc<RequestEncodingCache>>,
    checkpoint_source: Option<Arc<crate::checkpoint::CheckpointMetadata>>,
}

impl JournalWriterAssembler {
    pub fn add<T: Serialize>(
        &mut self,
        kind: JournalObjectKind,
        value: &T,
    ) -> Result<JournalObjectId, RuntimeError> {
        let object = JournalObject::encode(kind, value)?;
        #[cfg(feature = "test-support")]
        crate::journal::profile::event("rt.work.encoded_objects", 1, object.bytes.len() as u64);
        match self.objects.get(&object.id) {
            Some(existing) if existing.bytes != object.bytes => Err(RuntimeError::conflict(
                "journal object identity was reused for different bytes",
            )),
            Some(_) => Ok(object.id),
            None => {
                let id = object.id.clone();
                self.objects.insert(id.clone(), object);
                Ok(id)
            }
        }
    }

    fn add_structured<T: Serialize>(
        &mut self,
        kind: JournalObjectKind,
        value: &T,
    ) -> Result<JournalObjectId, RuntimeError> {
        let id = self.add(kind, value)?;
        let object = self.objects.get(&id).ok_or_else(conflict)?.clone();
        use super::engine::{ExpectedLeaf, ExpectedObject, object_links};
        let expected = match kind {
            JournalObjectKind::JsonValue | JournalObjectKind::JsonInline => {
                ExpectedObject::Leaf(ExpectedLeaf::JsonValue)
            }
            JournalObjectKind::JsonObjectEntry => {
                ExpectedObject::Leaf(ExpectedLeaf::JsonObjectEntry)
            }
            JournalObjectKind::ManifestReference => ExpectedObject::Leaf(ExpectedLeaf::Manifest),
            JournalObjectKind::ExtensionReference => ExpectedObject::Leaf(ExpectedLeaf::Extension),
            JournalObjectKind::JsonBranch => ExpectedObject::Sequence(ExpectedLeaf::JsonValue),
            JournalObjectKind::Sequence => {
                let node: JournalSequenceNode = object.decode(kind)?;
                let leaf = match node {
                    JournalSequenceNode::Leaf { .. } | JournalSequenceNode::Branch { .. } => {
                        ExpectedLeaf::JsonObjectEntry
                    }
                    _ => ExpectedLeaf::JsonValue,
                };
                ExpectedObject::Sequence(leaf)
            }
            _ => return Err(conflict()),
        };
        let children = object_links(&object, expected)?
            .into_iter()
            .map(|(id, _)| self.json_witnesses.get(&id).cloned().ok_or_else(conflict))
            .collect::<Result<Vec<_>, _>>()?;
        self.json_witnesses.insert(
            id.clone(),
            Arc::new(JsonWitness {
                object,
                children,
                kind,
                decoded: Mutex::new(None),
            }),
        );
        self.structured.insert(id.clone());
        Ok(id)
    }

    fn sequence(&mut self, items: Vec<JournalObjectId>) -> Result<JournalSequence, RuntimeError> {
        if items.is_empty() {
            return Ok(JournalSequence::default());
        }
        let mut trees = Vec::new();
        for items in items.chunks(JOURNAL_FANOUT) {
            let root = self.add(
                JournalObjectKind::Sequence,
                &JournalSequenceNode::Leaf {
                    items: items.to_vec(),
                },
            )?;
            trees.push(JournalSequence {
                root: Some(root),
                len: items.len() as u64,
                height: 0,
            });
        }
        for _ in 0..JOURNAL_MAX_TREE_HEIGHT {
            let mut parents = Vec::new();
            for children in trees.chunks(2) {
                let root = self.add(
                    JournalObjectKind::Sequence,
                    &JournalSequenceNode::Branch {
                        children: children.to_vec(),
                    },
                )?;
                parents.push(JournalSequence {
                    root: Some(root),
                    len: children.iter().map(|child| child.len).sum(),
                    height: children[0].height.checked_add(1).ok_or_else(|| {
                        RuntimeError::conflict("journal sequence height overflow")
                    })?,
                });
            }
            trees = parents;
        }
        if trees.len() != 1 {
            return Err(RuntimeError::conflict(
                "journal sequence exceeds the supported persistent index",
            ));
        }
        trees
            .pop()
            .ok_or_else(|| RuntimeError::conflict("journal sequence is empty"))
    }

    fn structured_sequence_cached(
        &mut self,
        values: &[Value],
        previous: Option<(&[Value], &JsonArrayEncodingCache)>,
    ) -> Result<(JournalJsonArray, JsonArrayEncodingCache, bool), RuntimeError> {
        self.structured_sequence_inputs_cached(
            ArraySource::Values(values),
            previous.map(|(values, cache)| (ArraySource::Values(values), cache)),
        )
    }
    fn structured_sequence_inputs_cached(
        &mut self,
        values: ArraySource<'_>,
        previous: Option<(ArraySource<'_>, &JsonArrayEncodingCache)>,
    ) -> Result<(JournalJsonArray, JsonArrayEncodingCache, bool), RuntimeError> {
        use super::persistent::Map;
        let previous = previous.filter(|(values, cache)| values.len() == cache.items.len());
        #[cfg(feature = "test-support")]
        if let Some((prior, _)) = previous {
            crate::journal::profile::event(
                "rt.work.unchanged_json_array_items_considered",
                prior.len().min(values.len()) as u64,
                0,
            );
        }
        let old = previous.map(|(_, cache)| cache);
        let mut items = old.map_or_else(Map::default, |c| c.items.clone());
        let mut depth_counts = old.map_or_else(BTreeMap::new, |cache| cache.depth_counts.clone());
        let mut item_bytes =
            old.map_or(0, |c| c.canonical_len - 2 - c.items.len().saturating_sub(1));
        let removed = items
            .iter_from(&values.len())
            .map(|(index, item)| (*index, item.value_len()))
            .collect::<Vec<_>>();
        for (index, bytes) in removed {
            adjust_depth(
                &mut depth_counts,
                items.get(&index).ok_or_else(conflict)?.depth(),
                false,
            )?;
            items.remove(&index);
            item_bytes = item_bytes.checked_sub(bytes).ok_or_else(conflict)?;
        }
        let mut dirty = BTreeSet::new();
        for index in 0..values.len() {
            let prior = previous.and_then(|(values, cache)| {
                (index < values.len())
                    .then(|| cache.items.get(&index))
                    .flatten()
                    .map(|cache| (values, cache))
            });
            let unchanged = prior.filter(|(old, _)| values.same_typed(old, index));
            let (cache, same) =
                if let Some((_, cache @ JsonItemEncodingCache::Reference(reference))) = unchanged {
                    self.register_witness(&reference.witness);
                    (cache.clone(), true)
                } else {
                    let value = values.value(index)?;
                    let prior_value = prior.map(|(source, _)| source.value(index)).transpose()?;
                    let (_, cache, same) = self.json_array_item_cached(
                        value.as_ref(),
                        prior_value
                            .as_ref()
                            .zip(prior)
                            .map(|(value, (_, cache))| (value.as_ref(), cache)),
                    )?;
                    (cache, same)
                };
            if !same || !items.contains_key(&index) {
                if let Some(old) = items.get(&index) {
                    adjust_depth(&mut depth_counts, old.depth(), false)?;
                    item_bytes = item_bytes
                        .checked_sub(old.value_len())
                        .ok_or_else(conflict)?;
                }
                item_bytes = item_bytes
                    .checked_add(cache.value_len())
                    .ok_or_else(conflict)?;
                adjust_depth(&mut depth_counts, cache.depth(), true)?;
                items.insert(index, cache);
                dirty.insert(index / 2);
            }
        }
        let canonical_len = 2usize
            .checked_add(values.len().saturating_sub(1))
            .and_then(|n| n.checked_add(item_bytes))
            .ok_or_else(conflict)?;
        let array_item = |index: usize| -> Result<JournalJsonItem, RuntimeError> {
            match items.get(&index).ok_or_else(conflict)? {
                JsonItemEncodingCache::Reference(cache) => Ok(JournalJsonItem::Reference {
                    object: cache.root.clone(),
                }),
                JsonItemEncodingCache::Inline { .. } => Err(conflict()),
            }
        };
        if values.len() <= JOURNAL_FANOUT {
            let root = JournalJsonArray {
                len: values.len() as u64,
                height: 0,
                node: JournalSequenceNode::JsonLeaf {
                    items: (0..values.len())
                        .map(array_item)
                        .collect::<Result<Vec<_>, _>>()?,
                },
            };
            let unchanged = old.is_some_and(|cache| cache.root == root);
            return Ok((
                root.clone(),
                JsonArrayEncodingCache {
                    items,
                    levels: Map::default(),
                    level_counts: vec![],
                    root,
                    canonical_len,
                    depth_counts,
                },
                unchanged,
            ));
        }
        let reusable = old.filter(|c| c.root.len > JOURNAL_FANOUT as u64);
        let mut levels = reusable.map_or_else(Map::default, |c| c.levels.clone());
        let mut level_counts = Vec::new();
        let mut count = values.len().div_ceil(2);
        if reusable.is_none() {
            dirty.extend(0..count);
        }
        if old.is_some_and(|c| c.items.len() != values.len()) {
            dirty.insert(values.len().saturating_sub(1) / 2);
        }
        dirty.retain(|index| *index < count);
        for &index in &dirty {
            let start = index * 2;
            let leaves = (start..values.len().min(start + 2))
                .map(array_item)
                .collect::<Result<Vec<_>, _>>()?;
            let root = self.add_structured(
                JournalObjectKind::Sequence,
                &JournalSequenceNode::JsonValueBranch { items: leaves },
            )?;
            let tree = JournalSequence {
                root: Some(root),
                len: (values.len() - start).min(2) as u64,
                height: 1,
            };
            if levels.get(&(0, index)) != Some(&tree) {
                levels.insert((0, index), tree);
            }
        }
        let mut level = 0usize;
        loop {
            level_counts.push(count);
            if count <= 2 {
                break;
            }
            if level >= JOURNAL_MAX_TREE_HEIGHT as usize {
                return Err(conflict());
            }
            let mut parents = dirty.iter().map(|index| index / 2).collect::<BTreeSet<_>>();
            let old_count = reusable
                .and_then(|cache| cache.level_counts.get(level + 1))
                .copied()
                .unwrap_or(0);
            parents.extend(old_count..count.div_ceil(2));
            for &index in &parents {
                let start = index * 2;
                let children = (start..count.min(start + 2))
                    .map(|i| levels.get(&(level, i)).cloned().ok_or_else(conflict))
                    .collect::<Result<Vec<_>, _>>()?;
                let roots = children
                    .iter()
                    .map(|child| {
                        child
                            .root
                            .as_ref()
                            .map(JournalCompactObjectId::from_object)
                            .ok_or_else(conflict)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let root =
                    self.add_structured(JournalObjectKind::JsonBranch, &JournalJsonBranch(roots))?;
                let tree = JournalSequence {
                    root: Some(root),
                    len: children.iter().map(|child| child.len).sum(),
                    height: children[0].height.checked_add(1).ok_or_else(conflict)?,
                };
                if levels.get(&(level + 1, index)) != Some(&tree) {
                    levels.insert((level + 1, index), tree);
                }
            }
            dirty = parents;
            count = count.div_ceil(2);
            level += 1;
        }
        if let Some(old) = reusable {
            for (level, &old_count) in old.level_counts.iter().enumerate() {
                let keep = level_counts.get(level).copied().unwrap_or(0);
                for index in keep..old_count {
                    levels.remove(&(level, index));
                }
            }
        }
        let children = (0..count)
            .map(|i| levels.get(&(level, i)).cloned().ok_or_else(conflict))
            .collect::<Result<Vec<_>, _>>()?;
        let child_roots = children
            .iter()
            .map(|child| {
                child
                    .root
                    .as_ref()
                    .map(JournalCompactObjectId::from_object)
                    .ok_or_else(conflict)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let root = JournalJsonArray {
            len: values.len() as u64,
            height: children[0].height.checked_add(1).ok_or_else(conflict)?,
            node: JournalSequenceNode::JsonBranch {
                children: child_roots,
            },
        };
        let unchanged = old.is_some_and(|cache| cache.root == root);
        Ok((
            root.clone(),
            JsonArrayEncodingCache {
                items,
                levels,
                level_counts,
                root,
                canonical_len,
                depth_counts,
            },
            unchanged,
        ))
    }

    fn structured_object_sequence(
        &mut self,
        items: Vec<JournalObjectId>,
    ) -> Result<JournalSequence, RuntimeError> {
        if items.is_empty() {
            return Ok(JournalSequence::default());
        }
        let mut trees = Vec::new();
        for items in items.chunks(JOURNAL_FANOUT) {
            let root = self.add_structured(
                JournalObjectKind::Sequence,
                &JournalSequenceNode::Leaf {
                    items: items.to_vec(),
                },
            )?;
            trees.push(JournalSequence {
                root: Some(root),
                len: items.len() as u64,
                height: 0,
            });
        }
        while trees.len() > 1 {
            let mut parents = Vec::new();
            for children in trees.chunks(JOURNAL_FANOUT) {
                let root = self.add_structured(
                    JournalObjectKind::Sequence,
                    &JournalSequenceNode::Branch {
                        children: children.to_vec(),
                    },
                )?;
                parents.push(JournalSequence {
                    root: Some(root),
                    len: children.iter().map(|child| child.len).sum(),
                    height: children[0].height.checked_add(1).ok_or_else(|| {
                        RuntimeError::conflict("journal sequence height overflow")
                    })?,
                });
            }
            trees = parents;
        }
        trees
            .pop()
            .ok_or_else(|| RuntimeError::conflict("journal sequence is empty"))
    }

    #[doc(hidden)]
    pub fn json_value_cached(
        &mut self,
        value: &Value,
        previous: Option<(&Value, &JournalJsonEncoding)>,
    ) -> Result<JournalJsonEncoding, RuntimeError> {
        json_depth(value)?;
        self.json_value_cached_inner(value, previous)
    }
    fn json_value_cached_inner(
        &mut self,
        value: &Value,
        previous: Option<(&Value, &JournalJsonEncoding)>,
    ) -> Result<JournalJsonEncoding, RuntimeError> {
        if let Some((prior, cache)) = previous {
            self.register_witness(&cache.witness);
            if journal_json_identical(prior, value) {
                return Ok(cache.clone());
            }
        }
        match value {
            Value::Array(values) => {
                let prior = previous.and_then(|(value, cache)| {
                    let Value::Array(values) = value else {
                        return None;
                    };
                    let JsonEncodingNode::Array(array) = cache.node.as_ref() else {
                        return None;
                    };
                    Some((values.as_slice(), array))
                });
                let (items, cache, unchanged) = self.structured_sequence_cached(values, prior)?;
                let root = if unchanged {
                    previous
                        .map(|(_, cache)| cache.root.clone())
                        .ok_or_else(|| RuntimeError::conflict("JSON value cache is incomplete"))?
                } else {
                    self.add_structured(
                        JournalObjectKind::JsonValue,
                        &JournalJsonValue::Array { items },
                    )?
                };
                let canonical_len = cache.canonical_len;
                let depth = cache.depth()?;
                Ok(JournalJsonEncoding {
                    witness: self
                        .json_witnesses
                        .get(&root)
                        .cloned()
                        .ok_or_else(conflict)?,
                    root,
                    node: Arc::new(JsonEncodingNode::Array(cache)),
                    typed: None,
                    depth,
                    canonical_len,
                })
            }
            Value::Object(values) => {
                let prior = previous.and_then(|(value, cache)| {
                    let Value::Object(values) = value else {
                        return None;
                    };
                    let JsonEncodingNode::Object { entries, sequence } = cache.node.as_ref() else {
                        return None;
                    };
                    Some((values, entries.as_slice(), sequence))
                });
                let mut canonical_len = 2usize
                    .checked_add(values.len().saturating_sub(1))
                    .ok_or_else(conflict)?;
                let mut entries = Vec::with_capacity(values.len());
                let mut entry_ids = Vec::with_capacity(values.len());
                let mut unchanged = prior.is_some_and(|(prior_values, entries, _)| {
                    prior_values.len() == values.len() && entries.len() == values.len()
                });
                // Index the prior map once; `iter().nth(index)` per entry made
                // a wide object quadratic.
                let prior_pairs =
                    prior.map(|(prior_values, _, _)| prior_values.iter().collect::<Vec<_>>());
                for (index, (key, value)) in values.iter().enumerate() {
                    let prior_entry = prior.and_then(|(_, entries, _)| {
                        let (prior_key, prior_value) = *prior_pairs.as_ref()?.get(index)?;
                        let entry = entries.get(index)?;
                        (prior_key == key && entry.key == *key).then_some((
                            prior_value,
                            &entry.item,
                            entry,
                        ))
                    });
                    let (item, item_cache, same) = self.json_item_cached(
                        value,
                        prior_entry.map(|(value, cache, _)| (value, cache)),
                    )?;
                    let object = if same {
                        prior_entry
                            .map(|(_, _, entry)| entry.object.clone())
                            .ok_or_else(|| {
                                RuntimeError::conflict("JSON object cache is incomplete")
                            })?
                    } else {
                        self.add_structured(
                            JournalObjectKind::JsonObjectEntry,
                            &JournalJsonObjectEntry {
                                key: key.clone(),
                                value: item,
                            },
                        )?
                    };
                    canonical_len = canonical_len
                        .checked_add(encoding::canonical_journal_json_len(key)?)
                        .and_then(|n| n.checked_add(1))
                        .and_then(|n| n.checked_add(item_cache.value_len()))
                        .ok_or_else(conflict)?;
                    unchanged &= same;
                    entry_ids.push(object.clone());
                    entries.push(JsonObjectEntryEncodingCache {
                        key: key.clone(),
                        item: item_cache,
                        object,
                    });
                }
                let sequence = if unchanged {
                    prior
                        .map(|(_, _, sequence)| sequence.clone())
                        .ok_or_else(|| RuntimeError::conflict("JSON object cache is incomplete"))?
                } else {
                    self.structured_object_sequence(entry_ids)?
                };
                let root = if unchanged {
                    previous
                        .map(|(_, cache)| cache.root.clone())
                        .ok_or_else(|| RuntimeError::conflict("JSON value cache is incomplete"))?
                } else {
                    self.add_structured(
                        JournalObjectKind::JsonValue,
                        &JournalJsonValue::Object {
                            entries: sequence.clone(),
                        },
                    )?
                };
                let depth = container_depth(entries.iter().map(|entry| entry.item.depth()))?;
                Ok(JournalJsonEncoding {
                    witness: self
                        .json_witnesses
                        .get(&root)
                        .cloned()
                        .ok_or_else(conflict)?,
                    root,
                    node: Arc::new(JsonEncodingNode::Object { entries, sequence }),
                    typed: None,
                    depth,
                    canonical_len,
                })
            }
            scalar => {
                let unchanged = previous.is_some_and(|(value, cache)| {
                    matches!(cache.node.as_ref(), JsonEncodingNode::Scalar)
                        && journal_json_identical(value, scalar)
                });
                let root = if unchanged {
                    previous
                        .map(|(_, cache)| cache.root.clone())
                        .ok_or_else(|| RuntimeError::conflict("JSON value cache is incomplete"))?
                } else {
                    self.add_structured(
                        JournalObjectKind::JsonValue,
                        &JournalJsonValue::Scalar {
                            scalar: scalar.clone(),
                        },
                    )?
                };
                Ok(JournalJsonEncoding {
                    witness: self
                        .json_witnesses
                        .get(&root)
                        .cloned()
                        .ok_or_else(conflict)?,
                    root,
                    node: Arc::new(JsonEncodingNode::Scalar),
                    typed: None,
                    depth: 0,
                    canonical_len: encoding::canonical_journal_json_len(scalar)?,
                })
            }
        }
    }

    fn json_item_cached(
        &mut self,
        value: &Value,
        previous: Option<(&Value, &JsonItemEncodingCache)>,
    ) -> Result<(JournalJsonItem, JsonItemEncodingCache, bool), RuntimeError> {
        if journal_json_can_inline(value)? {
            let unchanged = previous.is_some_and(|(prior, cache)| {
                matches!(cache, JsonItemEncodingCache::Inline { .. })
                    && journal_json_identical(prior, value)
            });
            return Ok((
                JournalJsonItem::Inline {
                    value: value.clone(),
                },
                JsonItemEncodingCache::Inline {
                    bytes: encoding::canonical_journal_value_len(value)?,
                    depth: json_depth(value)?,
                },
                unchanged,
            ));
        }
        let prior = previous.and_then(|(value, cache)| {
            let JsonItemEncodingCache::Reference(cache) = cache else {
                return None;
            };
            Some((value, cache.as_ref()))
        });
        let cache = self.json_value_cached_inner(value, prior)?;
        let unchanged = prior.is_some_and(|(_, prior)| prior.root == cache.root);
        Ok((
            JournalJsonItem::Reference {
                object: cache.root.clone(),
            },
            JsonItemEncodingCache::Reference(Arc::new(cache)),
            unchanged,
        ))
    }

    fn json_array_item_cached(
        &mut self,
        value: &Value,
        previous: Option<(&Value, &JsonItemEncodingCache)>,
    ) -> Result<(JournalJsonItem, JsonItemEncodingCache, bool), RuntimeError> {
        if let Some((prior, JsonItemEncodingCache::Reference(cache))) = previous {
            if journal_json_identical(prior, value) {
                self.register_witness(&cache.witness);
                return Ok((
                    JournalJsonItem::Reference {
                        object: cache.root.clone(),
                    },
                    JsonItemEncodingCache::Reference(Arc::clone(cache)),
                    true,
                ));
            }
        }
        if !journal_json_can_inline(value)? {
            return self.json_item_cached(value, previous);
        }
        let prior = previous.and_then(|(value, cache)| {
            let JsonItemEncodingCache::Reference(cache) = cache else {
                return None;
            };
            matches!(cache.node.as_ref(), JsonEncodingNode::Inline)
                .then_some((value, cache.as_ref()))
        });
        let unchanged = prior.is_some_and(|(prior, _)| journal_json_identical(prior, value));
        let cache = if unchanged {
            prior
                .map(|(_, cache)| cache.clone())
                .ok_or_else(|| RuntimeError::conflict("JSON value cache is incomplete"))?
        } else {
            let root = self.add_structured(
                JournalObjectKind::JsonInline,
                &JournalJsonInline(value.clone()),
            )?;
            JournalJsonEncoding {
                witness: self
                    .json_witnesses
                    .get(&root)
                    .cloned()
                    .ok_or_else(conflict)?,
                root,
                node: Arc::new(JsonEncodingNode::Inline),
                typed: None,
                canonical_len: encoding::canonical_journal_value_len(value)?,
                depth: json_depth(value)?,
            }
        };
        Ok((
            JournalJsonItem::Reference {
                object: cache.root.clone(),
            },
            JsonItemEncodingCache::Reference(Arc::new(cache)),
            unchanged,
        ))
    }

    fn extension_cached(
        &mut self,
        namespace: &str,
        value: &crate::store::VersionedSessionState,
        previous: Option<(&crate::store::VersionedSessionState, &JournalJsonEncoding)>,
    ) -> Result<(JournalObjectId, Option<JournalJsonEncoding>), RuntimeError> {
        self.extension_shared_cached(namespace, Arc::new(value.clone()), previous)
    }
    fn extension_shared_cached(
        &mut self,
        namespace: &str,
        source: Arc<crate::store::VersionedSessionState>,
        previous: Option<(&crate::store::VersionedSessionState, &JournalJsonEncoding)>,
    ) -> Result<(JournalObjectId, Option<JournalJsonEncoding>), RuntimeError> {
        let value = source.as_ref();
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.writer.previous_cache");
        if namespace != RUNTIME_PREVIOUS_CACHE_NAMESPACE {
            return Ok((self.add(JournalObjectKind::Extension, value)?, None));
        }
        if value.sensitivity != crate::store::SessionStateSensitivity::RedactionSafe {
            return Err(RuntimeError::conflict(
                "runtime previous-cache state is not redaction-safe",
            ));
        }
        let certified = previous.is_some_and(|(previous, cache)| {
            previous.revision == value.revision && previous.sensitivity == value.sensitivity
                && cache.typed.as_ref().is_some_and(|proof| proof.binds(cache) && matches!(&proof.source,
                    TypedJsonSource::State { namespace: bound, value: old } if bound == namespace && std::ptr::eq(old.as_ref(), previous)))
        });
        let input = JsonSource::state(value)?;
        let prior_input = previous
            .map(|(value, _)| JsonSource::state(value))
            .transpose()?;
        let composed = self
            .source_cached(
                &input,
                prior_input
                    .as_ref()
                    .zip(previous)
                    .map(|(input, (_, cache))| (input, cache)),
            )
            .and_then(|cache| self.check_composed(value, cache, certified));
        let mut cache = match composed {
            Ok(cache) => cache,
            // An uncertified prior pairing that does not describe its value
            // takes the complete path instead of failing the preparation.
            Err(_) if previous.is_some() && !certified => {
                return self.extension_shared_cached(namespace, source, None);
            }
            Err(error) => return Err(error),
        };
        #[cfg(feature = "test-support")]
        crate::journal::profile::event(
            "rt.work.typed_json_certified_compositions",
            u64::from(certified),
            0,
        );
        cache.typed = Some(Arc::new(TypedJsonProof {
            source: TypedJsonSource::State {
                namespace: namespace.to_owned(),
                value: source.clone(),
            },
            root: cache.root.clone(),
            node: cache.node.clone(),
            witness: cache.witness.clone(),
            canonical_len: cache.canonical_len,
            depth: cache.depth,
        }));
        let bound_value = source;
        let id = self.add_structured(
            JournalObjectKind::ExtensionReference,
            &ReferencedSessionState {
                value: cache.root.clone(),
            },
        )?;
        self.hints.extensions.insert(
            id.clone(),
            Arc::new(ExtensionEvidence {
                bound_value: bound_value.clone(),
                witness: self.json_witnesses.get(&id).cloned().ok_or_else(conflict)?,
                namespace: namespace.to_owned(),
                value: bound_value,
                bytes: cache.canonical_len,
            }),
        );
        Ok((id, Some(cache)))
    }

    fn manifest_cached(
        &mut self,
        manifest: &TurnManifest,
        previous: Option<(&TurnManifest, &JournalJsonEncoding)>,
    ) -> Result<(JournalObjectId, JournalJsonEncoding), RuntimeError> {
        self.manifest_shared_cached(Arc::new(manifest.clone()), previous)
    }
    fn manifest_shared_cached(
        &mut self,
        source: Arc<TurnManifest>,
        previous: Option<(&TurnManifest, &JournalJsonEncoding)>,
    ) -> Result<(JournalObjectId, JournalJsonEncoding), RuntimeError> {
        let manifest = source.as_ref();
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.writer.manifest_cached");
        let certified = manifest.manifest.schema_version
            == crate::manifest::MANIFEST_SCHEMA_VERSION
            && previous.is_some_and(|(previous, cache)| {
                cache.typed.as_ref().is_some_and(|proof| {
                    proof.binds(cache)
                        && matches!(&proof.source,
                TypedJsonSource::Manifest(old) if old.manifest.schema_version == crate::manifest::MANIFEST_SCHEMA_VERSION && std::ptr::eq(old.as_ref(), previous))
                })
            });
        let trusted_identity = if certified {
            previous.and_then(|(old, _)| old.manifest.cache_identity.as_ref())
        } else {
            None
        };
        let input = JsonSource::manifest(manifest, trusted_identity)?;
        let prior_input = previous
            .map(|(manifest, _)| {
                JsonSource::manifest(
                    manifest,
                    if certified {
                        manifest.manifest.cache_identity.as_ref()
                    } else {
                        None
                    },
                )
            })
            .transpose()?;
        let composed = self
            .source_cached(
                &input,
                prior_input
                    .as_ref()
                    .zip(previous)
                    .map(|(input, (_, cache))| (input, cache)),
            )
            .and_then(|cache| self.check_composed(manifest, cache, certified));
        let mut cache = match composed {
            Ok(cache) => cache,
            // An uncertified prior pairing that does not describe its value
            // takes the complete path instead of failing the preparation.
            Err(_) if previous.is_some() && !certified => {
                return self.manifest_shared_cached(source, None);
            }
            Err(error) => return Err(error),
        };
        #[cfg(feature = "test-support")]
        crate::journal::profile::event(
            "rt.work.typed_json_certified_compositions",
            u64::from(certified),
            0,
        );
        cache.typed = Some(Arc::new(TypedJsonProof {
            source: TypedJsonSource::Manifest(source.clone()),
            root: cache.root.clone(),
            node: cache.node.clone(),
            witness: cache.witness.clone(),
            canonical_len: cache.canonical_len,
            depth: cache.depth,
        }));
        let bound_value = source;
        let id = self.add_structured(
            JournalObjectKind::ManifestReference,
            &ReferencedTurnManifest {
                value: cache.root.clone(),
            },
        )?;
        self.hints.manifests.insert(
            id.clone(),
            Arc::new(ManifestEvidence {
                bound_value: bound_value.clone(),
                witness: self.json_witnesses.get(&id).cloned().ok_or_else(conflict)?,
                value: bound_value,
                bytes: cache.canonical_len,
            }),
        );
        Ok((id, cache))
    }

    /// Registers a composed typed encoding and checks it against its value.
    ///
    /// Certified compositions skip the round trip in release builds; debug
    /// and test builds always run it, so a hand-mirrored serde attribute that
    /// drifts from the typed source fails loudly instead of diverging ids.
    fn check_composed<T: serde::de::DeserializeOwned + Serialize>(
        &mut self,
        value: &T,
        cache: JournalJsonEncoding,
        certified: bool,
    ) -> Result<JournalJsonEncoding, RuntimeError> {
        self.register_witness(&cache.witness);
        if cache.root != cache.witness.object.id {
            return Err(conflict());
        }
        if !certified || cfg!(any(test, debug_assertions)) {
            let (bytes, depth) = check_typed_json_roundtrip(value, &cache.witness)?;
            if cache.depth != depth || cache.canonical_len != bytes {
                return Err(conflict());
            }
        }
        Ok(cache)
    }

    pub fn map(&mut self, entries: Vec<JournalMapEntry>) -> Result<JournalMap, RuntimeError> {
        if entries.is_empty() {
            return Ok(JournalMap::default());
        }
        let mut trees = Vec::new();
        for entries in entries.chunks(JOURNAL_FANOUT) {
            let root = self.add(
                JournalObjectKind::Map,
                &JournalMapNode::Leaf {
                    entries: entries.to_vec(),
                },
            )?;
            trees.push(JournalMapChild {
                first_key: entries[0].key.clone(),
                tree: JournalMap {
                    root: Some(root),
                    len: entries.len() as u64,
                    height: 0,
                },
            });
        }
        while trees.len() > 1 {
            let mut parents = Vec::new();
            for children in trees.chunks(JOURNAL_FANOUT) {
                let root = self.add(
                    JournalObjectKind::Map,
                    &JournalMapNode::Branch {
                        children: children.to_vec(),
                    },
                )?;
                parents.push(JournalMapChild {
                    first_key: children[0].first_key.clone(),
                    tree: JournalMap {
                        root: Some(root),
                        len: children.iter().map(|child| child.tree.len).sum(),
                        height: children[0]
                            .tree
                            .height
                            .checked_add(1)
                            .ok_or_else(|| RuntimeError::conflict("journal map height overflow"))?,
                    },
                });
            }
            trees = parents;
        }
        trees
            .pop()
            .map(|child| child.tree)
            .ok_or_else(|| RuntimeError::conflict("journal map is empty"))
    }

    pub fn snapshot(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
    ) -> Result<JournalSnapshotRoots, RuntimeError> {
        self.snapshot_cached(snapshot, planned_step_count, None, None)
            .map(|(roots, _)| roots)
    }

    #[doc(hidden)]
    pub fn snapshot_cached(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
        previous: Option<&SessionSnapshot>,
        previous_encoding: Option<&JournalWriterEncoding>,
    ) -> Result<(JournalSnapshotRoots, JournalWriterEncoding), RuntimeError> {
        let view = previous
            .zip(previous_encoding)
            .and_then(|(snapshot, encoding)| {
                (encoding.view.materialize() == *snapshot).then_some(&encoding.view)
            });
        self.snapshot_view_cached(snapshot, planned_step_count, view, previous_encoding)
    }

    #[doc(hidden)]
    pub fn snapshot_view_cached(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
        previous: Option<&JournalSnapshotView>,
        previous_encoding: Option<&JournalWriterEncoding>,
    ) -> Result<(JournalSnapshotRoots, JournalWriterEncoding), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.writer.snapshot_cached");
        let previous = previous
            .zip(previous_encoding)
            .filter(|(previous, encoding)| {
                previous.session() == &snapshot.id
                    && encoding.view.same_version(previous)
                    && encoding.roots.history.len == previous.history_len() as u64
                    && encoding.roots.usage.len == previous.usage_len() as u64
                    && encoding.manifest_ids.len() == previous.manifest_len()
                    && encoding.manifest_json.len() == previous.manifest_len()
                    && encoding.extension_ids.len() == previous.extension_len()
            });

        self.request_cache = previous.and_then(|(_, cache)| cache.request_cache.clone());

        let history_prefix = previous.filter(|(prior, _)| {
            prior.history_len() <= snapshot.history.len()
                && prior
                    .history_messages()
                    .zip(&snapshot.history[..prior.history_len()])
                    .all(|(a, b)| messages_match(std::slice::from_ref(a), std::slice::from_ref(b)))
        });
        let history_start = history_prefix.map_or(0, |(prior, _)| prior.history_len());
        #[cfg(feature = "test-support")]
        if let Some((prior, _)) = previous {
            crate::journal::profile::event(
                "rt.work.unchanged_history_prefix_nodes",
                prior.history_len() as u64,
                0,
            );
        }
        let appended = snapshot.history[history_start..]
            .iter()
            .map(|message| self.add(JournalObjectKind::Message, message))
            .collect::<Result<Vec<_>, _>>()?;
        let history_chain = history_prefix.map_or_else(
            || journal_history_chain(&appended),
            |(_, cache)| journal_history_chain_append(&cache.roots.history_chain, &appended),
        );
        let (history_view, objects) = history_prefix.map_or_else(
            || JournalSequenceView::from_items(appended.clone()),
            |(_, cache)| cache.history_view.append(&appended),
        )?;
        for object in objects {
            self.objects.insert(object.id.clone(), object);
        }
        let history = history_view.sequence();
        let records = snapshot.usage.records();
        let usage_prefix = previous.filter(|(prior, _)| {
            prior.usage_len() <= records.len()
                && prior
                    .usage_records()
                    .eq(records[..prior.usage_len()].iter())
        });
        let usage_start = usage_prefix.map_or(0, |(prior, _)| prior.usage_len());
        let appended = records[usage_start..]
            .iter()
            .map(|record| self.add(JournalObjectKind::UsageRecord, record))
            .collect::<Result<Vec<_>, _>>()?;
        let (usage_view, objects) = usage_prefix.map_or_else(
            || JournalSequenceView::from_items(appended.clone()),
            |(_, cache)| cache.usage_view.append(&appended),
        )?;
        for object in objects {
            self.objects.insert(object.id.clone(), object);
        }
        let usage = usage_view.sequence();

        let overlap = previous.map_or(0, |(previous, _)| {
            retained_overlap_view(previous, &snapshot.manifests)
        });
        let mut manifest_ids = previous.map_or_else(Vec::new, |(previous, encoding)| {
            encoding.manifest_ids[previous.manifest_len() - overlap..].to_vec()
        });
        let mut manifest_json = previous.map_or_else(Vec::new, |(previous, encoding)| {
            encoding.manifest_json[previous.manifest_len() - overlap..].to_vec()
        });
        for (index, manifest) in snapshot.manifests[overlap..].iter().enumerate() {
            let absolute = overlap + index;
            let prior = if absolute > 0 {
                Some((
                    &snapshot.manifests[absolute - 1],
                    &manifest_json[absolute - 1],
                ))
            } else {
                previous.and_then(|(previous, encoding)| {
                    Some((
                        previous.manifest(previous.manifest_len().checked_sub(1)?)?,
                        encoding.manifest_json.last()?,
                    ))
                })
            };
            let (id, cache) = self.manifest_cached(manifest, prior)?;
            manifest_ids.push(id);
            manifest_json.push(cache);
        }
        let manifests = previous
            .filter(|(previous, _)| {
                previous.manifest_len() == snapshot.manifests.len()
                    && (0..previous.manifest_len())
                        .all(|i| previous.manifest(i) == Some(&snapshot.manifests[i]))
            })
            .map_or_else(
                || self.sequence(manifest_ids.clone()),
                |(_, encoding)| Ok(encoding.roots.manifests.clone()),
            )?;
        if planned_step_count < manifests.len {
            return Err(RuntimeError::conflict(
                "journal planned-step counter trails its retained window",
            ));
        }

        let mut extension_ids = BTreeMap::new();
        let mut extension_json = BTreeMap::new();
        for (key, value) in &snapshot.extension_state {
            let (id, json) = match previous {
                Some((previous, encoding))
                    if state_identical(previous.extension(key), Some(value)) =>
                {
                    (
                        encoding.extension_ids.get(key).cloned().ok_or_else(|| {
                            RuntimeError::conflict(
                                "native journal extension encoding cache is incomplete",
                            )
                        })?,
                        encoding.extension_json.get(key).cloned(),
                    )
                }
                _ => self.extension_cached(
                    key,
                    value,
                    previous.and_then(|(previous, encoding)| {
                        Some((previous.extension(key)?, encoding.extension_json.get(key)?))
                    }),
                )?,
            };
            if let Some(json) = json {
                extension_json.insert(key.clone(), json);
            }
            extension_ids.insert(key.clone(), id);
        }
        let extension_entries = extension_ids
            .iter()
            .map(|(key, value)| JournalMapEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let extensions = previous
            .filter(|(previous, _)| {
                previous.extension_len() == snapshot.extension_state.len()
                    && previous.extensions().all(|(key, value)| {
                        state_identical(Some(value), snapshot.extension_state.get(key))
                    })
            })
            .map_or_else(
                || self.map(extension_entries),
                |(_, encoding)| Ok(encoding.roots.extensions.clone()),
            )?;
        let roots = JournalSnapshotRoots {
            history,
            history_chain,
            usage,
            extensions,
            manifests,
            planned_step_count,
            identity: snapshot.identity.clone(),
            updated: snapshot.updated,
        };
        let mutation_hint = previous
            .filter(|_| history_prefix.is_some() && usage_prefix.is_some())
            .map(|(base, _)| {
                let extension_changes = base
                    .extensions()
                    .map(|(key, _)| key)
                    .chain(snapshot.extension_state.keys())
                    .filter(|key| {
                        !state_identical(base.extension(key), snapshot.extension_state.get(*key))
                    })
                    .cloned()
                    .collect();
                JournalWriterMutationHint {
                    base: base.clone(),
                    history_start,
                    usage_start,
                    manifest_overlap: overlap,
                    extension_changes,
                }
            });
        let view = if let Some(hint) = &mutation_hint {
            let mut view = hint.base.clone();
            for message in &snapshot.history[history_start..] {
                view.history.push(message.clone())?;
            }
            for record in &records[usage_start..] {
                view.usage.push(record.clone())?;
            }
            view.manifests.truncate_front(overlap)?;
            for manifest in &snapshot.manifests[overlap..] {
                view.manifests.push(manifest.clone())?;
            }
            for key in &hint.extension_changes {
                view.extension_state
                    .set(key.clone(), snapshot.extension_state.get(key).cloned())?;
            }
            view.advance_version();
            view.identity = snapshot.identity.clone();
            view.updated = snapshot.updated;
            view
        } else {
            JournalSnapshotView::from_snapshot(snapshot.clone())?
        };
        Ok((
            roots.clone(),
            JournalWriterEncoding {
                roots,
                view,
                history_view,
                usage_view,
                manifest_ids,
                extension_ids,
                manifest_json,
                extension_json,
                mutation_hint,
                request_cache: self.request_cache.clone(),
            },
        ))
    }

    #[doc(hidden)]
    pub fn snapshot_shared_cached(
        &mut self,
        snapshot: &JournalSnapshotView,
        planned_step_count: u64,
        previous: Option<&JournalSnapshotView>,
        previous_encoding: Option<&JournalWriterEncoding>,
    ) -> Result<(JournalSnapshotRoots, JournalWriterEncoding), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.writer.snapshot_cached");
        let previous = previous
            .zip(previous_encoding)
            .filter(|(previous, encoding)| {
                previous.session() == snapshot.session()
                    && encoding.view.same_version(previous)
                    && encoding.roots.history.len == previous.history_len() as u64
                    && encoding.roots.usage.len == previous.usage_len() as u64
                    && encoding.manifest_ids.len() == previous.manifest_len()
                    && encoding.manifest_json.len() == previous.manifest_len()
                    && encoding.extension_ids.len() == previous.extension_len()
            });

        self.request_cache = previous.and_then(|(_, cache)| cache.request_cache.clone());

        let history_prefix = previous.filter(|(prior, _)| {
            prior.history.is_prefix_with(&snapshot.history, |a, b| {
                messages_match(std::slice::from_ref(a), std::slice::from_ref(b))
            })
        });
        let history_start = history_prefix.map_or(0, |(prior, _)| prior.history_len());
        let appended = snapshot
            .history
            .iter_from(history_start)
            .map(|message| self.add(JournalObjectKind::Message, message))
            .collect::<Result<Vec<_>, _>>()?;
        let history_chain = history_prefix.map_or_else(
            || journal_history_chain(&appended),
            |(_, cache)| journal_history_chain_append(&cache.roots.history_chain, &appended),
        );
        let (history_view, objects) = history_prefix.map_or_else(
            || JournalSequenceView::from_items(appended.clone()),
            |(_, cache)| cache.history_view.append(&appended),
        )?;
        for object in objects {
            self.objects.insert(object.id.clone(), object);
        }
        let history = history_view.sequence();
        let usage_prefix = previous
            .filter(|(prior, _)| prior.usage.is_prefix_with(&snapshot.usage, PartialEq::eq));
        let usage_start = usage_prefix.map_or(0, |(prior, _)| prior.usage_len());
        let appended = snapshot
            .usage
            .iter_from(usage_start)
            .map(|record| self.add(JournalObjectKind::UsageRecord, record))
            .collect::<Result<Vec<_>, _>>()?;
        let (usage_view, objects) = usage_prefix.map_or_else(
            || JournalSequenceView::from_items(appended.clone()),
            |(_, cache)| cache.usage_view.append(&appended),
        )?;
        for object in objects {
            self.objects.insert(object.id.clone(), object);
        }
        let usage = usage_view.sequence();

        let overlap = previous.map_or(0, |(previous, _)| {
            (0..=previous.manifest_len().min(snapshot.manifest_len()))
                .rev()
                .find(|overlap| {
                    (0..*overlap).all(|i| {
                        previous.manifests.shared_position(
                            previous.manifest_len() - overlap + i,
                            &snapshot.manifests,
                            i,
                        ) || previous.manifest(previous.manifest_len() - overlap + i)
                            == snapshot.manifest(i)
                    })
                })
                .unwrap_or(0)
        });
        let mut manifest_ids = previous.map_or_else(Vec::new, |(previous, encoding)| {
            encoding.manifest_ids[previous.manifest_len() - overlap..].to_vec()
        });
        let mut manifest_json = previous.map_or_else(Vec::new, |(previous, encoding)| {
            encoding.manifest_json[previous.manifest_len() - overlap..].to_vec()
        });
        for absolute in overlap..snapshot.manifest_len() {
            let prior = if absolute > 0 {
                Some((
                    snapshot.manifest(absolute - 1).expect("prior manifest"),
                    &manifest_json[absolute - 1],
                ))
            } else {
                previous.and_then(|(previous, encoding)| {
                    Some((
                        previous.manifest(previous.manifest_len().checked_sub(1)?)?,
                        encoding.manifest_json.last()?,
                    ))
                })
            };
            let (id, cache) = self.manifest_shared_cached(
                snapshot.manifests.shared(absolute).ok_or_else(conflict)?,
                prior,
            )?;
            manifest_ids.push(id);
            manifest_json.push(cache);
        }
        let manifests = previous
            .filter(|(previous, _)| {
                previous.manifests.same(&snapshot.manifests)
                    || (overlap == snapshot.manifest_len()
                        && previous.manifest_len() == snapshot.manifest_len())
            })
            .map_or_else(
                || self.sequence(manifest_ids.clone()),
                |(_, encoding)| Ok(encoding.roots.manifests.clone()),
            )?;
        if planned_step_count < manifests.len {
            return Err(RuntimeError::conflict(
                "journal planned-step counter trails its retained window",
            ));
        }

        let mut extension_ids = BTreeMap::new();
        let mut extension_json = BTreeMap::new();
        for (key, value) in snapshot.extension_state.iter() {
            let (id, json) = match previous {
                Some((previous, encoding))
                    if previous
                        .extension_state
                        .same_value(key, &snapshot.extension_state)
                        || state_identical(previous.extension(key), Some(value)) =>
                {
                    (
                        encoding.extension_ids.get(key).cloned().ok_or_else(|| {
                            RuntimeError::conflict(
                                "native journal extension encoding cache is incomplete",
                            )
                        })?,
                        encoding.extension_json.get(key).cloned(),
                    )
                }
                _ => self.extension_shared_cached(
                    key,
                    snapshot.extension_shared(key).ok_or_else(conflict)?,
                    previous.and_then(|(previous, encoding)| {
                        Some((previous.extension(key)?, encoding.extension_json.get(key)?))
                    }),
                )?,
            };
            if let Some(json) = json {
                extension_json.insert(key.clone(), json);
            }
            extension_ids.insert(key.clone(), id);
        }
        let extension_entries = extension_ids
            .iter()
            .map(|(key, value)| JournalMapEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let extensions = previous
            .filter(|(_, encoding)| encoding.extension_ids == extension_ids)
            .map_or_else(
                || self.map(extension_entries),
                |(_, encoding)| Ok(encoding.roots.extensions.clone()),
            )?;
        let roots = JournalSnapshotRoots {
            history,
            history_chain,
            usage,
            extensions,
            manifests,
            planned_step_count,
            identity: snapshot.identity.clone(),
            updated: snapshot.updated,
        };
        let mutation_hint = previous
            .filter(|_| history_prefix.is_some() && usage_prefix.is_some())
            .map(|(base, _)| {
                let extension_changes = base
                    .extensions()
                    .map(|(key, _)| key)
                    .chain(snapshot.extension_state.iter().map(|(key, _)| key))
                    .filter(|key| {
                        !base
                            .extension_state
                            .same_value(key, &snapshot.extension_state)
                            && !state_identical(
                                base.extension(key),
                                snapshot.extension_state.get(key),
                            )
                    })
                    .cloned()
                    .collect();
                JournalWriterMutationHint {
                    base: base.clone(),
                    history_start,
                    usage_start,
                    manifest_overlap: overlap,
                    extension_changes,
                }
            });
        let view = snapshot.clone();
        Ok((
            roots.clone(),
            JournalWriterEncoding {
                roots,
                view,
                history_view,
                usage_view,
                manifest_ids,
                extension_ids,
                manifest_json,
                extension_json,
                mutation_hint,
                request_cache: self.request_cache.clone(),
            },
        ))
    }

    fn request(&mut self, request: &ProviderRequest) -> Result<JournalObjectId, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.writer.request");
        let previous = self.request_cache.clone();
        let message_base = previous.as_ref().filter(|cache| {
            cache.source.request().messages.len() <= request.messages.len()
                && cache
                    .source
                    .request()
                    .messages
                    .iter()
                    .zip(&request.messages[..cache.source.request().messages.len()])
                    .all(|(a, b)| messages_match(std::slice::from_ref(a), std::slice::from_ref(b)))
        });
        #[cfg(feature = "test-support")]
        if let Some(cache) = &previous {
            crate::journal::profile::event(
                "rt.work.unchanged_request_prefix_nodes",
                cache.source.request().messages.len() as u64,
                0,
            );
        }
        let message_start = message_base.map_or(0, |cache| cache.source.request().messages.len());
        let mut added = Vec::new();
        for message in &request.messages[message_start..] {
            added.push(self.add(JournalObjectKind::Message, message)?);
        }
        let (message_view, objects) = message_base.map_or_else(
            || JournalSequenceView::from_items(added.clone()),
            |cache| cache.message_view.append(&added),
        )?;
        for object in objects {
            self.objects.insert(object.id.clone(), object);
        }
        let tool_base = previous.as_ref().filter(|cache| {
            cache.source.request().tools.len() <= request.tools.len()
                && cache
                    .source
                    .request()
                    .tools
                    .iter()
                    .zip(&request.tools[..cache.source.request().tools.len()])
                    .all(|(a, b)| {
                        a == b && journal_json_identical(&a.input_schema, &b.input_schema)
                    })
        });
        let tool_start = tool_base.map_or(0, |cache| cache.source.request().tools.len());
        let mut added = Vec::new();
        for tool in &request.tools[tool_start..] {
            added.push(self.add(JournalObjectKind::ToolSchema, tool)?);
        }
        let (tool_view, objects) = tool_base.map_or_else(
            || JournalSequenceView::from_items(added.clone()),
            |cache| cache.tool_view.append(&added),
        )?;
        for object in objects {
            self.objects.insert(object.id.clone(), object);
        }
        let messages = message_view.sequence();
        let tools = tool_view.sequence();
        let source = self.checkpoint_source.as_ref().filter(|metadata| {
            matches!(&metadata.state, TurnState::CallingModel { request: bound, .. } if std::ptr::eq(bound, request))
        }).map_or_else(|| RequestSource::Owned(Arc::new(request.clone())), |metadata| RequestSource::Checkpoint(metadata.clone()));
        self.request_cache = Some(Arc::new(RequestEncodingCache {
            source: Arc::new(source),
            message_view,
            tool_view,
        }));
        let settings = ProviderRequest {
            model: request.model.clone(),
            cache_identity: request.cache_identity.clone(),
            cache_boundary: request.cache_boundary,
            messages: Vec::new(),
            tools: Vec::new(),
            hosted_tools: request.hosted_tools.clone(),
            tool_choice: request.tool_choice.clone(),
            sampling: request.sampling.clone(),
            reasoning: request.reasoning.clone(),
            structured_output: request.structured_output.clone(),
            max_output_tokens: request.max_output_tokens,
            stop: request.stop.clone(),
            vendor_extensions: request.vendor_extensions.clone(),
        };
        let settings = self.add(JournalObjectKind::RequestSettings, &settings)?;
        self.add(
            JournalObjectKind::Request,
            &ReferencedProviderRequest {
                messages,
                tools,
                settings,
            },
        )
    }

    fn state(
        &mut self,
        state: &TurnState,
        fingerprint: agent_runtime_registry::Fingerprint,
    ) -> Result<JournalObjectId, RuntimeError> {
        let raw = match state {
            TurnState::CallingModel {
                request_id, step, ..
            } => serde_json::json!({"state":"calling_model","request_id":request_id,"step":step}),
            _ => serde_json::to_value(state)?,
        };
        let Value::Object(mut fields) = raw else {
            return Err(RuntimeError::conflict(
                "turn state did not serialize as an object",
            ));
        };
        let tag = fields
            .remove("state")
            .and_then(|tag| tag.as_str().map(str::to_owned))
            .ok_or_else(|| RuntimeError::conflict("turn state tag is missing"))?;
        let mut references = BTreeMap::new();
        if let TurnState::CallingModel { request, .. } = state {
            references.insert(
                "request".to_owned(),
                JournalStateValue::Request {
                    object: self.request(request)?,
                },
            );
        }
        for (name, value) in fields {
            let reference = if tag == "calling_model" && name == "request" {
                JournalStateValue::Request {
                    object: self.request(&serde_json::from_value(value)?)?,
                }
            } else if let Value::Array(values) = value {
                let items = values
                    .iter()
                    .map(|value| self.add(JournalObjectKind::StateValue, value))
                    .collect::<Result<Vec<_>, _>>()?;
                JournalStateValue::Sequence {
                    sequence: self.sequence(items)?,
                }
            } else {
                JournalStateValue::Object {
                    object: self.add(JournalObjectKind::StateValue, &value)?,
                }
            };
            references.insert(name, reference);
        }
        let id = self.add(
            JournalObjectKind::TurnState,
            &ReferencedTurnState {
                tag,
                fields: references,
            },
        )?;
        self.hints.states.insert(
            id.clone(),
            Arc::new(StateEvidence {
                value: self
                    .checkpoint_source
                    .as_ref()
                    .filter(|source| std::ptr::eq(&source.state, state))
                    .map_or_else(
                        || StateValueView::Owned(Arc::new(state.clone())),
                        |source| StateValueView::Checkpoint(source.clone()),
                    ),
                fingerprint,
                encoded: self.objects.get(&id).ok_or_else(conflict)?.bytes.clone(),
            }),
        );
        Ok(id)
    }

    pub fn checkpoint(
        &mut self,
        checkpoint: &TurnCheckpoint,
        roots: JournalSnapshotRoots,
    ) -> Result<ReferencedTurnCheckpoint, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.writer.checkpoint");
        self.checkpoint_source = None;
        let state_fingerprint = checkpoint.state.operation_fingerprint();
        checkpoint
            .validation_evidence(Some(&state_fingerprint))
            .validate(&checkpoint.snapshot)?;
        self.checkpoint_references(checkpoint, roots, state_fingerprint)
    }

    #[doc(hidden)]
    pub fn checkpoint_view(
        &mut self,
        checkpoint: &crate::checkpoint::JournalCheckpoint,
        roots: JournalSnapshotRoots,
    ) -> Result<ReferencedTurnCheckpoint, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.writer.checkpoint");
        self.checkpoint_source = Some(checkpoint.metadata_arc());
        let fingerprint = checkpoint.state.operation_fingerprint();
        checkpoint
            .validation_evidence(Some(&fingerprint))
            .validate(checkpoint.snapshot())?;
        self.checkpoint_view_references(checkpoint, roots, fingerprint)
    }

    /// Encodes a checkpoint the way a buggy host would submit it, without the
    /// writer-side validation, so tests can prove the engine rejects it.
    #[cfg(test)]
    pub(crate) fn checkpoint_without_validation(
        &mut self,
        checkpoint: &TurnCheckpoint,
        roots: JournalSnapshotRoots,
    ) -> Result<ReferencedTurnCheckpoint, RuntimeError> {
        self.checkpoint_source = None;
        let state_fingerprint = checkpoint.state.operation_fingerprint();
        self.checkpoint_references(checkpoint, roots, state_fingerprint)
    }

    fn checkpoint_references(
        &mut self,
        checkpoint: &TurnCheckpoint,
        roots: JournalSnapshotRoots,
        state_fingerprint: agent_runtime_registry::Fingerprint,
    ) -> Result<ReferencedTurnCheckpoint, RuntimeError> {
        Ok(ReferencedTurnCheckpoint {
            schema_version: JOURNAL_SCHEMA_VERSION,
            transition_revision: checkpoint.transition_revision,
            session: checkpoint.session.clone(),
            turn: checkpoint.turn.clone(),
            state_revision: checkpoint.state_revision,
            operation_fingerprint: checkpoint.operation_fingerprint.clone(),
            active_history_start: checkpoint.active_history_start,
            internal_input: checkpoint
                .internal_input
                .as_ref()
                .map(|input| self.add(JournalObjectKind::InternalInput, input))
                .transpose()?,
            visible_output: checkpoint.visible_output,
            state: self.state(&checkpoint.state, state_fingerprint)?,
            snapshot: roots,
            deadline: checkpoint.deadline,
            watermark: checkpoint.watermark.clone(),
            updated: checkpoint.updated,
        })
    }
    fn checkpoint_view_references(
        &mut self,
        checkpoint: &crate::checkpoint::JournalCheckpoint,
        roots: JournalSnapshotRoots,
        state_fingerprint: agent_runtime_registry::Fingerprint,
    ) -> Result<ReferencedTurnCheckpoint, RuntimeError> {
        Ok(ReferencedTurnCheckpoint {
            schema_version: JOURNAL_SCHEMA_VERSION,
            transition_revision: checkpoint.transition_revision,
            session: checkpoint.session.clone(),
            turn: checkpoint.turn.clone(),
            state_revision: checkpoint.state_revision,
            operation_fingerprint: checkpoint.operation_fingerprint.clone(),
            active_history_start: checkpoint.active_history_start,
            internal_input: checkpoint
                .internal_input
                .as_ref()
                .map(|input| self.add(JournalObjectKind::InternalInput, input))
                .transpose()?,
            visible_output: checkpoint.visible_output,
            state: self.state(&checkpoint.state, state_fingerprint)?,
            snapshot: roots,
            deadline: checkpoint.deadline,
            watermark: checkpoint.watermark.clone(),
            updated: checkpoint.updated,
        })
    }
}

impl JournalWriterEncoding {
    #[doc(hidden)]
    pub fn snapshot_view(&self) -> JournalSnapshotView {
        self.view.clone()
    }
    pub fn manifest_ids(&self) -> &[JournalObjectId] {
        &self.manifest_ids
    }
    pub fn extension_id(&self, key: &str) -> Option<&JournalObjectId> {
        self.extension_ids.get(key)
    }
}
impl JournalJsonEncoding {
    pub fn root(&self) -> &JournalObjectId {
        &self.root
    }
}
impl JournalWriterAssembler {
    pub fn preparation_hints(&self) -> JournalPreparationHints {
        self.hints.clone()
    }
    #[doc(hidden)]
    pub fn reachable_drafts(
        &self,
        roots: &JournalSnapshotRoots,
        checkpoint: Option<&ReferencedTurnCheckpoint>,
        known: &BTreeSet<JournalObjectId>,
        structured_only: bool,
    ) -> Result<Vec<JournalObject>, RuntimeError> {
        use super::engine::{ExpectedLeaf, ExpectedObject, object_links, snapshot_links};
        let mut pending = snapshot_links(roots);
        if let Some(checkpoint) = checkpoint {
            pending.push((
                checkpoint.state.clone(),
                ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::TurnState)),
            ));
            if let Some(input) = &checkpoint.internal_input {
                pending.push((
                    input.clone(),
                    ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::InternalInput)),
                ));
            }
        }
        let mut visited = BTreeSet::new();
        let mut retained = BTreeSet::new();
        while let Some((id, expected)) = pending.pop() {
            if known.contains(&id) || !visited.insert((id.clone(), expected)) {
                continue;
            }
            let Some(object) = self.objects.get(&id) else {
                continue;
            };
            retained.insert(id);
            pending.extend(object_links(object, expected)?);
        }
        Ok(retained
            .into_iter()
            .filter(|id| !structured_only || self.structured.contains(id))
            .filter_map(|id| self.objects.get(&id).cloned())
            .collect())
    }
}
impl std::fmt::Debug for JournalWriterEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalWriterEncoding")
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for JournalJsonEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalJsonEncoding")
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for JournalWriterAssembler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalWriterAssembler")
            .field("objects", &self.objects.len())
            .finish_non_exhaustive()
    }
}

impl JournalWriterEncoding {
    fn bound_hint(&self, base: &JournalSnapshotView) -> Option<&JournalWriterMutationHint> {
        self.mutation_hint
            .as_ref()
            .filter(|h| h.base.same_version(base))
    }
    #[doc(hidden)]
    pub fn append_starts(&self, base: &JournalSnapshotView) -> Option<(usize, usize)> {
        self.bound_hint(base)
            .map(|h| (h.history_start, h.usage_start))
    }
    #[doc(hidden)]
    pub fn manifest_overlap(&self, base: &JournalSnapshotView) -> Option<usize> {
        self.bound_hint(base).map(|h| h.manifest_overlap)
    }
    #[doc(hidden)]
    pub fn extension_changes(
        &self,
        base: &JournalSnapshotView,
    ) -> Option<impl Iterator<Item = &String>> {
        self.bound_hint(base).map(|h| h.extension_changes.iter())
    }
}

impl JsonItemEncodingCache {
    fn value_len(&self) -> usize {
        match self {
            Self::Inline { bytes, .. } => *bytes,
            Self::Reference(cache) => cache.canonical_len,
        }
    }
}
impl JournalJsonEncoding {
    /// Exact canonical semantic length, independent of object framing.
    pub fn canonical_len(&self) -> usize {
        self.canonical_len
    }
}

impl JournalWriterEncoding {
    /// Completes the immutable encoding version after optional execution state.
    #[doc(hidden)]
    pub fn finish_preparation(&mut self, assembler: &JournalWriterAssembler) {
        self.request_cache = assembler.request_cache.clone();
    }
}

// The immutable graph binds the typed Arc to the encoder's intended bodies.
// Engine validation stops at the base closure, never at lease/cache membership.
pub(in crate::journal) struct JsonWitness {
    object: JournalObject,
    children: Vec<Arc<JsonWitness>>,
    kind: JournalObjectKind,
    // Lives only for one round-trip check; see `release_decoded`.
    decoded: Mutex<Option<DecodedJson>>,
}
impl JsonWitness {
    pub(in crate::journal) fn kind(&self) -> JournalObjectKind {
        self.kind
    }
    pub(in crate::journal) fn decode<T>(&self, kind: JournalObjectKind) -> Result<T, RuntimeError>
    where
        T: serde::de::DeserializeOwned + Serialize + Clone + Send + Sync + 'static,
    {
        if let Some((cached_kind, ty, value)) =
            self.decoded.lock().map_err(|_| conflict())?.as_ref()
        {
            if *cached_kind == kind && *ty == TypeId::of::<T>() {
                return value.downcast_ref::<T>().cloned().ok_or_else(conflict);
            }
        }
        let value = self.object.decode::<T>(kind)?;
        #[cfg(feature = "test-support")]
        crate::journal::profile::event(
            "rt.work.writer_json_objects_decoded",
            1,
            self.object.bytes.len() as u64,
        );
        *self.decoded.lock().map_err(|_| conflict())? =
            Some((kind, TypeId::of::<T>(), Box::new(value.clone())));
        Ok(value)
    }
}
impl JournalWriterAssembler {
    fn register_witness(&mut self, root: &Arc<JsonWitness>) {
        let mut pending = vec![root.clone()];
        while let Some(node) = pending.pop() {
            if self.json_witnesses.contains_key(&node.object.id) {
                continue;
            }
            pending.extend(node.children.iter().cloned());
            self.json_witnesses.insert(node.object.id.clone(), node);
        }
    }
}
fn witness_value(witness: &Arc<JsonWitness>) -> Result<Value, RuntimeError> {
    let mut nodes = BTreeMap::new();
    let mut pending = vec![witness];
    while let Some(node) = pending.pop() {
        if nodes
            .insert(node.object.id.clone(), Arc::clone(node))
            .is_none()
        {
            pending.extend(&node.children);
        }
    }
    let stored = super::engine::ObjectStore::default();
    let reachable = super::engine::ReachableSet::default();
    let bodies = BTreeMap::new();
    super::engine::DraftReader::new(&stored, &bodies, &reachable)
        .with_json_witnesses(&nodes)
        .json_value(&witness.object.id, 0)
}
fn check_typed_json_roundtrip<T: serde::de::DeserializeOwned + Serialize>(
    source: &T,
    witness: &Arc<JsonWitness>,
) -> Result<(usize, u32), RuntimeError> {
    #[cfg(feature = "test-support")]
    let _profile = crate::journal::profile::enter_sync("rt.writer.json_roundtrip");
    let expected = canonical_journal_json(source)?;
    let encoded = witness_value(witness);
    // Witnesses outlive this check in JSON caches, proofs and hints; the
    // decoded copies must not.
    release_decoded(witness);
    let encoded = encoded?;
    let depth = json_depth(&encoded)?;
    if canonical_journal_json(&encoded)? != expected {
        return Err(conflict());
    }
    let decoded: T = serde_json::from_value(encoded).map_err(|_| conflict())?;
    if canonical_journal_json(&decoded)? != expected {
        return Err(conflict());
    }
    Ok((expected.len(), depth))
}
type DecodedJson = (JournalObjectKind, TypeId, Box<dyn Any + Send + Sync>);
fn release_decoded(root: &Arc<JsonWitness>) {
    let mut pending = vec![root.as_ref()];
    let mut seen = BTreeSet::new();
    while let Some(node) = pending.pop() {
        if !seen.insert(&node.object.id) {
            continue;
        }
        if let Ok(mut decoded) = node.decoded.lock() {
            decoded.take();
        }
        pending.extend(node.children.iter().map(Arc::as_ref));
    }
}
#[cfg(test)]
fn check_json_roundtrip<T: serde::de::DeserializeOwned + Serialize>(
    value: &Value,
    witness: &Arc<JsonWitness>,
) -> Result<(), RuntimeError> {
    let encoded = witness_value(witness)?;
    if canonical_journal_json(&encoded)? != canonical_journal_json(value)? {
        return Err(conflict());
    }
    let decoded: T = serde_json::from_value(encoded).map_err(|_| conflict())?;
    if canonical_journal_json(&decoded)? != canonical_journal_json(value)? {
        return Err(conflict());
    }
    Ok(())
}
impl JsonWitness {
    fn validate(
        &self,
        drafts: &super::engine::DraftReader<'_>,
        verified: &impl Fn(&JournalObjectId) -> bool,
    ) -> Result<(), RuntimeError> {
        let mut pending = vec![self];
        let mut seen = BTreeSet::new();
        while let Some(node) = pending.pop() {
            if verified(&node.object.id) || !seen.insert(&node.object.id) {
                continue;
            }
            #[cfg(feature = "test-support")]
            crate::journal::profile::event("rt.work.hint_json_nodes_checked", 1, 0);
            if drafts.encoded_object(&node.object.id)?.bytes != node.object.bytes {
                return Err(conflict());
            }
            pending.extend(node.children.iter().map(Arc::as_ref));
        }
        Ok(())
    }
}
impl ManifestEvidence {
    pub(in crate::journal) fn validate(
        &self,
        id: &JournalObjectId,
        drafts: &super::engine::DraftReader<'_>,
        verified: &impl Fn(&JournalObjectId) -> bool,
    ) -> Result<(), RuntimeError> {
        if self.witness.object.id != *id || !Arc::ptr_eq(&self.value, &self.bound_value) {
            return Err(conflict());
        }
        self.witness.validate(drafts, verified)
    }
}
impl ExtensionEvidence {
    pub(in crate::journal) fn validate(
        &self,
        namespace: &str,
        id: &JournalObjectId,
        drafts: &super::engine::DraftReader<'_>,
        verified: &impl Fn(&JournalObjectId) -> bool,
    ) -> Result<(), RuntimeError> {
        if self.witness.object.id != *id
            || namespace != RUNTIME_PREVIOUS_CACHE_NAMESPACE
            || self.namespace != namespace
            || self.value.sensitivity != crate::store::SessionStateSensitivity::RedactionSafe
            || !Arc::ptr_eq(&self.value, &self.bound_value)
        {
            return Err(conflict());
        }
        self.witness.validate(drafts, verified)
    }
}

#[cfg(test)]
impl JournalPreparationHints {
    /// Replaces every manifest hint value without its witness, as a buggy or
    /// hostile in-crate producer would.
    pub(crate) fn forge_manifest_values(&mut self, forged: &TurnManifest) {
        for evidence in self.manifests.values_mut() {
            *evidence = Arc::new(ManifestEvidence {
                bound_value: evidence.bound_value.clone(),
                witness: evidence.witness.clone(),
                value: Arc::new(forged.clone()),
                bytes: evidence.bytes,
            });
        }
    }
}
#[cfg(test)]
impl JournalPreparationHints {
    /// Keeps every manifest hint value but swaps its witness for the witness
    /// of the referenced JSON root, whose id differs from the hint's key.
    pub(crate) fn forge_manifest_witness_roots(&mut self) {
        for evidence in self.manifests.values_mut() {
            *evidence = Arc::new(ManifestEvidence {
                bound_value: evidence.bound_value.clone(),
                witness: evidence.witness.children[0].clone(),
                value: evidence.value.clone(),
                bytes: evidence.bytes,
            });
        }
    }
}
#[cfg(test)]
impl JournalPreparationHints {
    /// Pairs every state hint with bytes other than the ones it was encoded
    /// to, as a mixed-up preparation would.
    pub(crate) fn forge_state_encodings(&mut self) {
        for evidence in self.states.values_mut() {
            let mut encoded = evidence.encoded.clone();
            encoded.push(b' ');
            *evidence = Arc::new(StateEvidence {
                value: evidence.value.clone(),
                fingerprint: evidence.fingerprint.clone(),
                encoded,
            });
        }
    }
}
#[cfg(test)]
impl JournalWriterAssembler {
    pub(crate) fn has_request_cache(&self) -> bool {
        self.request_cache.is_some()
    }
}

fn json_depth(value: &Value) -> Result<u32, RuntimeError> {
    let mut pending = vec![(value, 0u32)];
    let mut maximum = 0;
    while let Some((value, depth)) = pending.pop() {
        if depth > JOURNAL_MAX_TREE_HEIGHT {
            return Err(conflict());
        }
        maximum = maximum.max(depth);
        match value {
            Value::Array(values) => pending.extend(values.iter().map(|value| (value, depth + 1))),
            Value::Object(values) => {
                pending.extend(values.values().map(|value| (value, depth + 1)))
            }
            _ => {}
        }
    }
    Ok(maximum)
}
fn container_depth(values: impl Iterator<Item = u32>) -> Result<u32, RuntimeError> {
    let depth = values.max().map_or(0, |depth| depth + 1);
    if depth > JOURNAL_MAX_TREE_HEIGHT {
        return Err(conflict());
    }
    Ok(depth)
}
fn adjust_depth(
    counts: &mut BTreeMap<u32, usize>,
    depth: u32,
    add: bool,
) -> Result<(), RuntimeError> {
    if add {
        let count = counts.entry(depth).or_default();
        *count = count.checked_add(1).ok_or_else(conflict)?;
    } else {
        let count = counts.get_mut(&depth).ok_or_else(conflict)?;
        *count = count.checked_sub(1).ok_or_else(conflict)?;
        if *count == 0 {
            counts.remove(&depth);
        }
    }
    Ok(())
}
impl JsonItemEncodingCache {
    fn depth(&self) -> u32 {
        match self {
            Self::Inline { depth, .. } => *depth,
            Self::Reference(value) => value.depth,
        }
    }
}
impl JsonArrayEncodingCache {
    fn depth(&self) -> Result<u32, RuntimeError> {
        container_depth(self.depth_counts.keys().copied())
    }
}

#[cfg(test)]
mod review_tests {
    use super::*;
    use crate::checkpoint::TurnCheckpoint;
    use crate::clock::Timestamp;
    use crate::ids::{SessionId, TurnId};
    use crate::manifest::{CapabilityResolution, ModelResolution, RunManifest};
    use crate::provider::ModelId;
    use agent_runtime_registry::{Fingerprint, RegistryRevision};

    fn snapshot() -> SessionSnapshot {
        SessionSnapshot {
            id: SessionId::new("session-1"),
            history: vec![Message::user("hello")],
            usage: Default::default(),
            identity: Default::default(),
            manifests: vec![],
            extension_state: Default::default(),
            updated: Timestamp(1),
        }
    }
    fn manifest(index: usize) -> TurnManifest {
        let fp = Fingerprint::of(index.to_string());
        TurnManifest::new(
            TurnId::new(format!("t{index}")),
            RunManifest::new(
                fp.clone(),
                fp.clone(),
                ModelResolution::new("p", ModelId::new("m"), fp.clone(), BTreeMap::new()),
                CapabilityResolution::new(RegistryRevision::new("r")),
                fp.clone(),
                fp,
            ),
        )
    }
    fn cached(value: &Value) -> JournalJsonEncoding {
        JournalWriterAssembler::default()
            .json_value_cached(value, None)
            .unwrap()
    }

    #[test]
    fn typed_provenance_matches_complete_readers_and_refuses_rebound_sources() {
        use crate::manifest::ContextSegmentRecord;
        use crate::provider::{
            CacheEndpointIdentity, CacheIdentity, CacheIdentityFragment, CacheIdentityTool,
        };
        let mut previous: Option<(Arc<TurnManifest>, JournalJsonEncoding)> = None;
        for len in [0, 1, 63, 64, 65, 127, 128, 129, 257, 1025, 257, 65, 0] {
            let mut current = manifest(len);
            current.manifest.segments = (0..len)
                .map(|i| {
                    ContextSegmentRecord::new(
                        format!("history:{i}"),
                        "history",
                        crate::manifest::SegmentSensitivity::Internal,
                        Fingerprint::of(i.to_string()),
                        i as u32,
                    )
                })
                .collect();
            let fragments = (0..len.min(4096))
                .map(|i| {
                    CacheIdentityFragment::new(
                        format!("history:{i}"),
                        Fingerprint::of(i.to_string()),
                    )
                })
                .collect::<Vec<_>>();
            current.manifest.cache_identity = Some(
                CacheIdentity::builder(
                    "p",
                    ModelId::new("m"),
                    CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("r")),
                    RegistryRevision::new("r"),
                    Fingerprint::of("profile"),
                )
                .stable_prefix(fragments.clone())
                .stable_history(fragments)
                .tools([CacheIdentityTool::new("tool", "description", "{}", 0)])
                .build(),
            );
            let current = Arc::new(current);
            let mut writer = JournalWriterAssembler::default();
            let (root, cache) = writer
                .manifest_shared_cached(
                    current.clone(),
                    previous.as_ref().map(|(v, c)| (v.as_ref(), c)),
                )
                .unwrap();
            check_typed_json_roundtrip(current.as_ref(), &cache.witness).unwrap();
            assert_eq!(
                cache.canonical_len,
                super::super::semantic::SemanticLength::semantic_len(current.as_ref()).unwrap()
            );
            let mut cold = JournalWriterAssembler::default();
            let (oracle, _) = cold.manifest_shared_cached(current.clone(), None).unwrap();
            assert_eq!(root, oracle, "length {len}");
            if let Some((old, old_cache)) = &previous {
                assert!(old_cache.typed.as_ref().unwrap().binds(old_cache));
                let other = Arc::new(manifest(9_999));
                assert!(
                    !matches!(&old_cache.typed.as_ref().unwrap().source, TypedJsonSource::Manifest(source) if std::ptr::eq(source.as_ref(), other.as_ref()))
                );
                let mut forged = old_cache.clone();
                forged.root = cache.root.clone();
                if forged.root != old_cache.root {
                    assert!(!forged.typed.as_ref().unwrap().binds(&forged));
                }
                let mut invalid = current.as_ref().clone();
                invalid.manifest.cache_identity = Some(
                    CacheIdentity::builder(
                        "",
                        ModelId::new("m"),
                        CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("r")),
                        RegistryRevision::new("r"),
                        Fingerprint::of("profile"),
                    )
                    .build(),
                );
                assert!(
                    JournalWriterAssembler::default()
                        .manifest_shared_cached(Arc::new(invalid), Some((old.as_ref(), old_cache)))
                        .is_err()
                );
                // L11: a mismatched prior pairing falls back to the complete
                // path and yields the cold encoding.
                let rebound = Arc::new(manifest(9_999));
                let (rebound_root, _) = JournalWriterAssembler::default()
                    .manifest_shared_cached(rebound.clone(), Some((rebound.as_ref(), old_cache)))
                    .unwrap();
                let (rebound_oracle, _) = JournalWriterAssembler::default()
                    .manifest_shared_cached(rebound, None)
                    .unwrap();
                assert_eq!(rebound_root, rebound_oracle);
            }
            previous = Some((current, cache));
        }
        let mut previous: Option<(Arc<VersionedSessionState>, JournalJsonEncoding)> = None;
        for len in [0, 1, 64, 65, 128, 129, 513, 65, 0] {
            let state = Arc::new(VersionedSessionState::new(RegistryRevision::new("r"), serde_json::json!({"items": (0..len).map(|i| serde_json::json!({"i": i, "n": if i%2 == 0 { -0.0 } else { 0.0 }})).collect::<Vec<_>>()})).redaction_safe());
            let mut writer = JournalWriterAssembler::default();
            let (root, cache) = writer
                .extension_shared_cached(
                    RUNTIME_PREVIOUS_CACHE_NAMESPACE,
                    state.clone(),
                    previous.as_ref().map(|(s, c)| (s.as_ref(), c)),
                )
                .unwrap();
            let cache = cache.unwrap();
            check_typed_json_roundtrip(state.as_ref(), &cache.witness).unwrap();
            let (oracle, _) = JournalWriterAssembler::default()
                .extension_shared_cached(RUNTIME_PREVIOUS_CACHE_NAMESPACE, state.clone(), None)
                .unwrap();
            assert_eq!(root, oracle);
            assert_eq!(
                cache.canonical_len,
                super::super::semantic::SemanticLength::semantic_len(state.as_ref()).unwrap()
            );
            previous = Some((state, cache));
        }
    }

    /// Every optional manifest field toggled between consecutive certified
    /// compositions encodes exactly as the complete `to_value` path (M1).
    #[test]
    fn certified_manifest_chain_with_toggled_optional_fields_matches_complete_path() {
        use crate::content::{InternalTurnSensitivity, InternalTurnSource};
        use crate::manifest::{
            ActivatedCapability, ContextSegmentRecord, ManifestReason, SegmentSensitivity,
            SummaryCoverage,
        };
        use crate::provider::{CacheEndpointIdentity, CacheIdentity, CacheIdentityFragment};
        use agent_runtime_registry::RegistryId;
        const FIELDS: u32 = 6;
        let build = |mask: u32, step: usize| {
            let mut value = manifest(step);
            let on = |bit: u32| mask & (1 << bit) != 0;
            if on(0) {
                value.manifest.activation = vec![ActivatedCapability::new(
                    RegistryId::skill("s"),
                    RegistryRevision::new(format!("r{step}")),
                )];
            }
            if on(1) {
                value.manifest.segments = (0..(step % 70 + 1))
                    .map(|i| {
                        ContextSegmentRecord::new(
                            format!("history:{i}"),
                            "history",
                            SegmentSensitivity::Internal,
                            Fingerprint::of(format!("{step}:{i}")),
                            i as u32,
                        )
                    })
                    .collect();
            }
            if on(2) {
                value.manifest.summaries = vec![SummaryCoverage::new(
                    crate::manifest::SegmentId::new("summary"),
                    vec![crate::manifest::SegmentId::new("history:0")],
                )];
            }
            if on(3) {
                let fragments = (0..(step % 5 + 1))
                    .map(|i| {
                        CacheIdentityFragment::new(format!("h{i}"), Fingerprint::of(i.to_string()))
                    })
                    .collect::<Vec<_>>();
                value.manifest.cache_identity = Some(
                    CacheIdentity::builder(
                        "p",
                        ModelId::new("m"),
                        CacheEndpointIdentity::from_opaque("endpoint", RegistryRevision::new("r")),
                        RegistryRevision::new("r"),
                        Fingerprint::of("profile"),
                    )
                    .stable_prefix(fragments.clone())
                    .stable_history(fragments)
                    .build(),
                );
            }
            if on(4) {
                value.manifest.reasons = vec![ManifestReason::Filtered {
                    subject: RegistryId::skill("f"),
                    detail: format!("d{step}"),
                }];
            }
            if on(5) {
                value.internal_source = Some(InternalTurnSource {
                    kind: "goal".into(),
                    id: format!("g{step}"),
                    revision: RegistryRevision::new("r"),
                    sensitivity: InternalTurnSensitivity::Public,
                    goal: None,
                });
            }
            Arc::new(value)
        };
        // Start with every field present (so only certified compositions see
        // empty fields), then Gray-code order toggles one field per step, then
        // every mask is visited again in reverse.
        let masks = std::iter::once((1u32 << FIELDS) - 1)
            .chain((0..(1u32 << FIELDS)).map(|i| i ^ (i >> 1)))
            .chain((0..(1u32 << FIELDS)).rev())
            .collect::<Vec<_>>();
        let mut previous: Option<(Arc<TurnManifest>, JournalJsonEncoding)> = None;
        let mut certified = 0;
        for (step, mask) in masks.into_iter().enumerate() {
            let current = build(mask, step);
            certified += usize::from(previous.as_ref().is_some_and(|(_, cache)| {
                cache.typed.as_ref().is_some_and(|proof| proof.binds(cache))
            }));
            let (_, cache) = JournalWriterAssembler::default()
                .manifest_shared_cached(
                    current.clone(),
                    previous.as_ref().map(|(v, c)| (v.as_ref(), c)),
                )
                .unwrap();
            let mut complete = super::super::engine::ObjectAssembler::default();
            let oracle = complete
                .json_value(&serde_json::to_value(current.as_ref()).unwrap())
                .unwrap();
            assert_eq!(cache.root, oracle, "mask {mask:06b} step {step}");
            assert_eq!(
                cache.canonical_len,
                canonical_journal_json(current.as_ref()).unwrap().len()
            );
            previous = Some((current, cache));
        }
        assert!(certified > 100);
    }

    /// Typed segment arrays under random mid-array edits, inserts and
    /// deletes encode exactly as the complete path, chained through both
    /// certified and uncertified compositions.
    #[test]
    fn typed_segment_arrays_match_complete_path_under_random_edits() {
        use crate::manifest::{ContextSegmentRecord, SegmentSensitivity};
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = |bound: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % bound.max(1) as u64) as usize
        };
        let segment = |label: usize| {
            ContextSegmentRecord::new(
                format!("history:{label}"),
                "history",
                SegmentSensitivity::Internal,
                Fingerprint::of(label.to_string()),
                label as u32,
            )
        };
        let mut segments = (0..150).map(segment).collect::<Vec<_>>();
        let mut label = 1_000;
        let mut previous: Option<(Arc<TurnManifest>, JournalJsonEncoding)> = None;
        for step in 0..160 {
            for _ in 0..(1 + next(4)) {
                label += 1;
                match next(4) {
                    0 if !segments.is_empty() => {
                        let at = next(segments.len());
                        segments[at] = segment(label);
                    }
                    1 => {
                        let at = next(segments.len() + 1);
                        segments.insert(at, segment(label));
                    }
                    2 if !segments.is_empty() => {
                        let at = next(segments.len());
                        segments.remove(at);
                    }
                    _ => {
                        let from = next(segments.len() + 1);
                        let to = (from + next(70)).min(segments.len());
                        segments.drain(from..to);
                    }
                }
            }
            if step % 37 == 0 {
                segments.extend((0..130).map(|i| segment(label + i + 1)));
                label += 131;
            }
            let mut value = manifest(step);
            value.manifest.segments = segments.clone();
            let current = Arc::new(value);
            // Every fifth step drops the pairing to exercise uncertified
            // composition from the same cache.
            let prior = previous
                .as_ref()
                .filter(|_| step % 5 != 0)
                .map(|(v, c)| (v.as_ref(), c));
            let (_, cache) = JournalWriterAssembler::default()
                .manifest_shared_cached(current.clone(), prior)
                .unwrap();
            let mut complete = super::super::engine::ObjectAssembler::default();
            let oracle = complete
                .json_value(&serde_json::to_value(current.as_ref()).unwrap())
                .unwrap();
            assert_eq!(cache.root, oracle, "step {step} len {}", segments.len());
            previous = Some((current, cache));
        }
    }

    /// Witness decode caches never outlive a round-trip check (M3).
    #[test]
    fn retained_witnesses_hold_no_decoded_copies() {
        fn assert_released(root: &Arc<JsonWitness>) {
            let mut pending = vec![root.clone()];
            while let Some(node) = pending.pop() {
                assert!(node.decoded.lock().unwrap().is_none());
                pending.extend(node.children.iter().cloned());
            }
        }
        let mut previous: Option<(Arc<TurnManifest>, JournalJsonEncoding)> = None;
        let mut writer = JournalWriterAssembler::default();
        for step in 0..8 {
            let mut value = manifest(step);
            value.manifest.segments = (0..(step * 20))
                .map(|i| {
                    crate::manifest::ContextSegmentRecord::new(
                        format!("history:{i}"),
                        "history",
                        crate::manifest::SegmentSensitivity::Internal,
                        Fingerprint::of(i.to_string()),
                        i as u32,
                    )
                })
                .collect();
            let current = Arc::new(value);
            // Alternate certified and uncertified (unpaired) compositions.
            let prior = previous
                .as_ref()
                .filter(|_| step % 2 == 0)
                .map(|(v, c)| (v.as_ref(), c));
            let (_, cache) = writer
                .manifest_shared_cached(current.clone(), prior)
                .unwrap();
            assert_released(&cache.witness);
            previous = Some((current, cache));
        }
        let state = Arc::new(
            VersionedSessionState::new(
                RegistryRevision::new("r"),
                serde_json::json!({"items": (0..300).collect::<Vec<_>>()}),
            )
            .redaction_safe(),
        );
        let (_, cache) = writer
            .extension_shared_cached(RUNTIME_PREVIOUS_CACHE_NAMESPACE, state, None)
            .unwrap();
        assert_released(&cache.unwrap().witness);
        for witness in writer.json_witnesses.values() {
            assert_released(witness);
        }
        for evidence in writer.hints.manifests.values() {
            assert_released(&evidence.witness);
        }
    }

    #[test]
    fn certified_composition_preserves_the_complete_readers_depth_limit() {
        let base = Arc::new(
            VersionedSessionState::new(RegistryRevision::new("r"), Value::Null).redaction_safe(),
        );
        let cache = JournalWriterAssembler::default()
            .extension_shared_cached(RUNTIME_PREVIOUS_CACHE_NAMESPACE, base.clone(), None)
            .unwrap()
            .1
            .unwrap();
        for empty in [false, true] {
            for depth in 0..36 {
                let mut value = if empty {
                    serde_json::json!([])
                } else {
                    Value::Null
                };
                for _ in 0..depth {
                    value = Value::Array(vec![value]);
                }
                let current = Arc::new(
                    VersionedSessionState::new(RegistryRevision::new("r"), value).redaction_safe(),
                );
                let warm = JournalWriterAssembler::default().extension_shared_cached(
                    RUNTIME_PREVIOUS_CACHE_NAMESPACE,
                    current.clone(),
                    Some((base.as_ref(), &cache)),
                );
                let mut complete = super::super::engine::ObjectAssembler::default();
                let root = complete
                    .json_value(&serde_json::to_value(current.as_ref()).unwrap())
                    .unwrap();
                let stored = super::super::engine::ObjectStore::default();
                let reachable = super::super::engine::ReachableSet::default();
                let oracle =
                    super::super::engine::DraftReader::new(&stored, &complete.objects, &reachable)
                        .json_value(&root, 0);
                assert_eq!(warm.is_ok(), oracle.is_ok(), "depth {depth}, empty {empty}");
                if let Ok((_, Some(encoded))) = warm {
                    check_typed_json_roundtrip(current.as_ref(), &encoded.witness).unwrap();
                }
            }
        }
    }

    #[test]
    fn prepared_requests_share_the_checkpoint_owner_instead_of_copying_messages() {
        let owned = crate::checkpoint::tests::checkpoint_corpus()
            .into_iter()
            .find(|c| matches!(c.state, TurnState::CallingModel { .. }))
            .unwrap();
        let shared = crate::checkpoint::JournalCheckpoint::from_checkpoint(owned.clone()).unwrap();
        let mut writer = JournalWriterAssembler::default();
        let roots = writer
            .snapshot_shared_cached(&shared.snapshot(), 0, None, None)
            .unwrap()
            .0;
        let refs = writer.checkpoint_view(&shared, roots.clone()).unwrap();
        let request = writer.request_cache.as_ref().unwrap();
        assert!(
            matches!(request.source.as_ref(), RequestSource::Checkpoint(owner) if Arc::ptr_eq(owner, &shared.metadata_arc()))
        );
        assert!(writer.hints.states.values().any(|state| matches!(&state.value, StateValueView::Checkpoint(owner) if Arc::ptr_eq(owner, &shared.metadata_arc()))));
        let mut complete = JournalWriterAssembler::default();
        assert_eq!(refs, complete.checkpoint(&owned, roots).unwrap());
    }

    #[test]
    fn typed_witness_decoding_preserves_full_roundtrips_and_expected_types() {
        let mut previous: Option<(Value, JournalJsonEncoding)> = None;
        for len in [0, 1, 64, 65, 128, 129, 257, 513, 65, 0] {
            let value = serde_json::json!({"items": (0..len).map(|i| serde_json::json!({"index": i, "n": if i % 2 == 0 { -0.0 } else { 0.0 }, "escaped": "\"\n😀"})).collect::<Vec<_>>()});
            let mut assembler = JournalWriterAssembler::default();
            let cache = assembler
                .json_value_cached(&value, previous.as_ref().map(|(v, c)| (v, c)))
                .unwrap();
            check_json_roundtrip::<Value>(&value, &cache.witness).unwrap();
            check_json_roundtrip::<Value>(&value, &cache.witness).unwrap();
            let mut bodies = BTreeMap::new();
            let mut pending = vec![&cache.witness];
            while let Some(node) = pending.pop() {
                if bodies
                    .insert(node.object.id.clone(), node.object.bytes.clone())
                    .is_none()
                {
                    pending.extend(&node.children);
                }
            }
            let stored = super::super::engine::ObjectStore::default();
            let reachable = super::super::engine::ReachableSet::default();
            let complete = super::super::engine::DraftReader::new(&stored, &bodies, &reachable)
                .json_value(&cache.root, 0)
                .unwrap();
            assert_eq!(
                canonical_journal_json(&complete).unwrap(),
                canonical_journal_json(&value).unwrap()
            );
            assert!(
                cache
                    .witness
                    .decode::<JournalJsonInline>(JournalObjectKind::JsonInline)
                    .is_err()
            );
            let decoded = cache
                .witness
                .decode::<Value>(JournalObjectKind::JsonValue)
                .unwrap();
            let typed = cache
                .witness
                .decode::<JournalJsonValue>(JournalObjectKind::JsonValue)
                .unwrap();
            assert_eq!(
                canonical_journal_json(&decoded).unwrap(),
                canonical_journal_json(&typed).unwrap()
            );
            let mut tampered = value.clone();
            tampered["changed"] = Value::Bool(true);
            assert!(check_json_roundtrip::<Value>(&tampered, &cache.witness).is_err());
            previous = Some((value, cache));
        }
    }

    // M1: a cached encoding that claims one value but holds another value's
    // bytes (a stale or buggy JSON cache) never becomes a hint; since round-2
    // L11 the writer re-encodes through the complete path instead of failing.
    #[test]
    fn a_cached_encoding_whose_bytes_differ_from_its_value_is_refused() {
        let honest = manifest(1);
        let honest_json = serde_json::to_value(&honest).unwrap();
        let other_json = serde_json::to_value(manifest(2)).unwrap();
        let forged = cached(&other_json);
        let (_, recovered) = JournalWriterAssembler::default()
            .manifest_cached(&honest, Some((&honest, &forged)))
            .unwrap();
        assert_eq!(recovered.root, cached(&honest_json).root);
        assert_ne!(recovered.root, forged.root);
        let (_, accepted) = JournalWriterAssembler::default()
            .manifest_cached(&honest, Some((&honest, &cached(&honest_json))))
            .unwrap();
        assert_eq!(accepted.root, cached(&honest_json).root);

        let state = |value| {
            let mut state =
                VersionedSessionState::new(RegistryRevision::new("r"), serde_json::json!(value));
            state.sensitivity = crate::store::SessionStateSensitivity::RedactionSafe;
            state
        };
        let (honest, other) = (state("honest"), state("other"));
        let forged = cached(&serde_json::to_value(&other).unwrap());
        let (_, recovered) = JournalWriterAssembler::default()
            .extension_cached(
                RUNTIME_PREVIOUS_CACHE_NAMESPACE,
                &honest,
                Some((&honest, &forged)),
            )
            .unwrap();
        let recovered = recovered.unwrap();
        assert_ne!(recovered.root, forged.root);
        assert_eq!(
            recovered.root,
            cached(&serde_json::to_value(&honest).unwrap()).root
        );
        assert!(
            JournalWriterAssembler::default()
                .extension_cached(
                    RUNTIME_PREVIOUS_CACHE_NAMESPACE,
                    &honest,
                    Some((&honest, &cached(&serde_json::to_value(&honest).unwrap())))
                )
                .is_ok()
        );
    }

    // M2: under serde_json/preserve_order a key-order-only change is a value
    // change and must be sent and reflected in the writer's view. Without
    // that feature serde_json maps are sorted and nothing changes.
    #[test]
    fn key_order_only_extension_changes_are_reported_and_viewed() {
        let mut first = serde_json::Map::new();
        first.insert("a".into(), 1.into());
        first.insert("b".into(), 2.into());
        let mut second = serde_json::Map::new();
        second.insert("b".into(), 2.into());
        second.insert("a".into(), 1.into());
        let preserves_order = second.keys().next().map(String::as_str) == Some("b");
        // The previous-cache namespace is stored as a structured JSON tree
        // and referenced by id; the change set must follow value identity.
        let ns = RUNTIME_PREVIOUS_CACHE_NAMESPACE;
        let state = |value| {
            let mut state =
                VersionedSessionState::new(RegistryRevision::new("r"), Value::Object(value));
            state.sensitivity = crate::store::SessionStateSensitivity::RedactionSafe;
            state
        };
        let mut base = snapshot();
        base.extension_state.insert(ns.into(), state(first));
        let (_, base_encoding) = JournalWriterAssembler::default()
            .snapshot_view_cached(&base, 0, None, None)
            .unwrap();
        let base_view = base_encoding.snapshot_view();
        let mut next = base.clone();
        next.extension_state.insert(ns.into(), state(second));
        let (_, encoding) = JournalWriterAssembler::default()
            .snapshot_view_cached(&next, 0, Some(&base_view), Some(&base_encoding))
            .unwrap();
        let changes = encoding
            .extension_changes(&base_view)
            .expect("the encoding is bound to its base")
            .cloned()
            .collect::<Vec<_>>();
        let keys = encoding
            .snapshot_view()
            .extension(ns)
            .unwrap()
            .value
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        if preserves_order {
            assert_eq!(changes, [ns]);
            assert_eq!(keys, ["b", "a"]);
        } else {
            assert!(changes.is_empty());
        }
    }

    // M4: a view is reused only with the encoding of that same version, and a
    // request cache only from an accepted previous encoding.
    #[test]
    fn a_view_paired_with_another_versions_encoding_is_not_reused() {
        let calling = crate::checkpoint::tests::checkpoint_corpus()
            .into_iter()
            .find(|c| matches!(c.state, TurnState::CallingModel { .. }))
            .unwrap();
        let encode = |snapshot: &SessionSnapshot, checkpoint: Option<&TurnCheckpoint>| {
            let mut assembler = JournalWriterAssembler::default();
            let (roots, mut encoding) = assembler
                .snapshot_view_cached(snapshot, 0, None, None)
                .unwrap();
            if let Some(checkpoint) = checkpoint {
                assembler.checkpoint(checkpoint, roots).unwrap();
                encoding.finish_preparation(&assembler);
            }
            encoding
        };
        let mut a = snapshot();
        a.history.push(Message::user("a"));
        let mut b = snapshot();
        b.history.push(Message::user("b"));
        let view_a = encode(&a, None).snapshot_view();
        let encoding_b = encode(&b, Some(&calling));
        let view_b = encoding_b.snapshot_view();
        let mut next = a.clone();
        next.history.push(Message::user("next"));

        let mut mismatched = JournalWriterAssembler::default();
        let (roots, _) = mismatched
            .snapshot_view_cached(&next, 0, Some(&view_a), Some(&encoding_b))
            .unwrap();
        let (expected, _) = JournalWriterAssembler::default()
            .snapshot_view_cached(&next, 0, None, None)
            .unwrap();
        assert_eq!(roots, expected);
        assert!(!mismatched.has_request_cache());

        let mut matched = JournalWriterAssembler::default();
        let mut next_b = b.clone();
        next_b.history.push(Message::user("next"));
        matched
            .snapshot_view_cached(&next_b, 0, Some(&view_b), Some(&encoding_b))
            .unwrap();
        assert!(matched.has_request_cache());
    }
}
