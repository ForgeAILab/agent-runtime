//! Canonical writer preparation and immutable, hash-bound semantic evidence.
use super::*;
use crate::checkpoint::{TurnCheckpoint, TurnState};
use crate::content::{ContentPart, Message};
use crate::error::RuntimeError;
use crate::provider::ProviderRequest;
use crate::store::{SessionSnapshot, TurnManifest, VersionedSessionState};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
macro_rules! writer_profile {
    ($label:expr,$body:block) => {{
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync($label);
        $body
    }};
}
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
pub(crate) struct StateEvidence {
    pub(crate) value: Arc<TurnState>,
    pub(crate) fingerprint: agent_runtime_registry::Fingerprint,
}
pub(crate) struct ManifestEvidence {
    pub(crate) value: Arc<TurnManifest>,
    pub(crate) bytes: usize,
}
pub(crate) struct ExtensionEvidence {
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
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| parts_match(&left.content, &right.content))
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
            _ => true,
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
struct RequestEncodingCache {
    messages: super::semantic::Sequence<Message>,
    message_view: JournalSequenceView,
    tools: super::semantic::Sequence<crate::provider::ToolSchema>,
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
#[derive(Clone)]
pub struct JournalJsonEncoding {
    root: JournalObjectId,
    node: Arc<JsonEncodingNode>,
    value: Option<Arc<Value>>,
    canonical_len: usize,
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
    items: Vec<JsonItemEncodingCache>,
    levels: Vec<Vec<JournalSequence>>,
    root: JournalJsonArray,
    canonical_len: usize,
}

#[derive(Clone)]
struct JsonObjectEntryEncodingCache {
    key: String,
    item: JsonItemEncodingCache,
    object: JournalObjectId,
}

#[derive(Clone)]
enum JsonItemEncodingCache {
    Inline { bytes: usize },
    Reference(Arc<JournalJsonEncoding>),
}

#[derive(Default)]
pub struct JournalWriterAssembler {
    pub objects: BTreeMap<JournalObjectId, JournalObject>,
    /// Candidate structured metadata reachable from reference-only deltas.
    pub structured: BTreeSet<JournalObjectId>,
    hints: JournalPreparationHints,
    request_cache: Option<Arc<RequestEncodingCache>>,
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
        let previous = previous.filter(|(values, cache)| values.len() == cache.items.len());
        let mut canonical_len = 2usize
            .checked_add(values.len().saturating_sub(1))
            .ok_or_else(conflict)?;
        if values.is_empty() {
            let root = JournalJsonArray {
                len: 0,
                height: 0,
                node: JournalSequenceNode::JsonLeaf { items: Vec::new() },
            };
            let unchanged =
                previous.is_some_and(|(values, cache)| values.is_empty() && cache.root == root);
            return Ok((
                root.clone(),
                JsonArrayEncodingCache {
                    items: Vec::new(),
                    levels: Vec::new(),
                    root,
                    canonical_len,
                },
                unchanged,
            ));
        }

        let mut item_caches = Vec::with_capacity(values.len());
        if values.len() <= JOURNAL_FANOUT {
            let mut items = Vec::with_capacity(values.len());
            let mut unchanged =
                previous.is_some_and(|(prior_values, _)| prior_values.len() == values.len());
            for (index, value) in values.iter().enumerate() {
                let prior = previous.and_then(|(prior_values, cache)| {
                    Some((prior_values.get(index)?, cache.items.get(index)?))
                });
                let (item, cache, same) = self.json_array_item_cached(value, prior)?;
                items.push(item);
                canonical_len = canonical_len
                    .checked_add(cache.value_len())
                    .ok_or_else(conflict)?;
                item_caches.push(cache);
                unchanged &= same;
            }
            let root = JournalJsonArray {
                len: values.len() as u64,
                height: 0,
                node: JournalSequenceNode::JsonLeaf { items },
            };
            unchanged &= previous.is_some_and(|(_, cache)| cache.root == root);
            return Ok((
                root.clone(),
                JsonArrayEncodingCache {
                    items: item_caches,
                    levels: Vec::new(),
                    root,
                    canonical_len,
                },
                unchanged,
            ));
        }

        let mut bottom = Vec::with_capacity(values.len().div_ceil(2));
        for (chunk_index, chunk) in values.chunks(2).enumerate() {
            let mut items = Vec::with_capacity(chunk.len());
            let mut unchanged = previous.is_some();
            for (offset, value) in chunk.iter().enumerate() {
                let index = chunk_index * 2 + offset;
                let prior = previous.and_then(|(prior_values, cache)| {
                    Some((prior_values.get(index)?, cache.items.get(index)?))
                });
                let (item, cache, same) = self.json_array_item_cached(value, prior)?;
                items.push(item);
                canonical_len = canonical_len
                    .checked_add(cache.value_len())
                    .ok_or_else(conflict)?;
                item_caches.push(cache);
                unchanged &= same;
            }
            // Reuse only a cached bottom node of exactly this width: a prior
            // array that was a single leaf has no bottom level, and a tail
            // chunk that shrank from two items to one must not keep both.
            let cached = previous
                .filter(|_| unchanged)
                .and_then(|(_, cache)| cache.levels.first()?.get(chunk_index))
                .filter(|tree| tree.len == chunk.len() as u64)
                .cloned();
            let tree = match cached {
                Some(tree) => tree,
                None => {
                    let root = self.add_structured(
                        JournalObjectKind::Sequence,
                        &JournalSequenceNode::JsonValueBranch { items },
                    )?;
                    JournalSequence {
                        root: Some(root),
                        len: chunk.len() as u64,
                        height: 1,
                    }
                }
            };
            bottom.push(tree);
        }

        let mut levels = vec![bottom];
        while levels.last().is_some_and(|level| level.len() > 2) {
            let level_index = levels.len() - 1;
            let current = levels.last().expect("level exists");
            if current[0].height >= JOURNAL_MAX_TREE_HEIGHT {
                return Err(RuntimeError::conflict(
                    "structured journal sequence exceeds the supported persistent index",
                ));
            }
            let mut parents = Vec::with_capacity(current.len().div_ceil(2));
            for (group, children) in current.chunks(2).enumerate() {
                // A cached parent is reusable only when it was built from
                // exactly these children: the prior group at this position
                // must have the same width, not merely a matching prefix.
                let cached = previous.and_then(|(_, cache)| {
                    let level = cache.levels.get(level_index)?;
                    let start = group * 2;
                    let prior = level.get(start..level.len().min(start + 2))?;
                    if prior != children {
                        return None;
                    }
                    cache.levels.get(level_index + 1)?.get(group).cloned()
                });
                let tree = if let Some(tree) = cached {
                    tree
                } else {
                    let child_roots = children
                        .iter()
                        .map(|child| {
                            child
                                .root
                                .as_ref()
                                .map(JournalCompactObjectId::from_object)
                                .ok_or_else(|| {
                                    RuntimeError::conflict(
                                        "structured journal branch has an empty child",
                                    )
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let root = self.add_structured(
                        JournalObjectKind::JsonBranch,
                        &JournalJsonBranch(child_roots),
                    )?;
                    JournalSequence {
                        root: Some(root),
                        len: children.iter().map(|child| child.len).sum(),
                        height: children[0].height.checked_add(1).ok_or_else(|| {
                            RuntimeError::conflict("journal sequence height overflow")
                        })?,
                    }
                };
                parents.push(tree);
            }
            levels.push(parents);
        }
        let children = levels
            .last()
            .filter(|level| !level.is_empty())
            .ok_or_else(|| RuntimeError::conflict("journal sequence is empty"))?;
        let child_roots = children
            .iter()
            .map(|child| {
                child
                    .root
                    .as_ref()
                    .map(JournalCompactObjectId::from_object)
                    .ok_or_else(|| {
                        RuntimeError::conflict("structured journal branch has an empty child")
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let root = JournalJsonArray {
            len: values.len() as u64,
            height: children[0]
                .height
                .checked_add(1)
                .ok_or_else(|| RuntimeError::conflict("journal sequence height overflow"))?,
            node: JournalSequenceNode::JsonBranch {
                children: child_roots,
            },
        };
        let unchanged = previous.is_some_and(|(prior_values, cache)| {
            prior_values.len() == values.len() && cache.root == root
        });
        Ok((
            root.clone(),
            JsonArrayEncodingCache {
                items: item_caches,
                levels,
                root,
                canonical_len,
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

    pub fn json_value_cached(
        &mut self,
        value: &Value,
        previous: Option<(&Value, &JournalJsonEncoding)>,
    ) -> Result<JournalJsonEncoding, RuntimeError> {
        if let Some((prior, cache)) = previous {
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
                Ok(JournalJsonEncoding {
                    root,
                    node: Arc::new(JsonEncodingNode::Array(cache)),
                    value: None,
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
                Ok(JournalJsonEncoding {
                    root,
                    node: Arc::new(JsonEncodingNode::Object { entries, sequence }),
                    value: None,
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
                    root,
                    node: Arc::new(JsonEncodingNode::Scalar),
                    value: None,
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
        let cache = self.json_value_cached(value, prior)?;
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
                root,
                node: Arc::new(JsonEncodingNode::Inline),
                value: None,
                canonical_len: encoding::canonical_journal_value_len(value)?,
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
        let value_json = serde_json::to_value(value)?;
        let previous_json = match previous {
            Some((_, cache)) => cache.value.as_ref().map(|value| (Arc::clone(value), cache)),
            None => None,
        };
        let mut cache = self.json_value_cached(
            &value_json,
            previous_json
                .as_ref()
                .map(|(value, cache)| (value.as_ref(), *cache)),
        )?;
        cache.value = Some(Arc::new(value_json));
        let id = self.add_structured(
            JournalObjectKind::ExtensionReference,
            &ReferencedSessionState {
                value: cache.root.clone(),
            },
        )?;
        self.hints.extensions.insert(
            id.clone(),
            Arc::new(ExtensionEvidence {
                namespace: namespace.to_owned(),
                value: Arc::new(value.clone()),
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
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.writer.manifest_cached");
        let value = writer_profile!("rt.writer.manifest_to_value", {
            serde_json::to_value(manifest)
        })?;
        let previous_value = previous
            .and_then(|(_, cache)| cache.value.as_ref().map(|value| (Arc::clone(value), cache)));
        let mut cache = self.json_value_cached(
            &value,
            previous_value
                .as_ref()
                .map(|(value, cache)| (value.as_ref(), *cache)),
        )?;
        cache.value = Some(Arc::new(value));
        let id = self.add_structured(
            JournalObjectKind::ManifestReference,
            &ReferencedTurnManifest {
                value: cache.root.clone(),
            },
        )?;
        self.hints.manifests.insert(
            id.clone(),
            Arc::new(ManifestEvidence {
                value: Arc::new(manifest.clone()),
                bytes: cache.canonical_len,
            }),
        );
        Ok((id, cache))
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

    pub fn snapshot_cached(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
        previous: Option<&SessionSnapshot>,
        previous_encoding: Option<&JournalWriterEncoding>,
    ) -> Result<(JournalSnapshotRoots, JournalWriterEncoding), RuntimeError> {
        let view = previous
            .cloned()
            .map(JournalSnapshotView::from_snapshot)
            .transpose()?;
        self.snapshot_view_cached(
            snapshot,
            planned_step_count,
            view.as_ref(),
            previous_encoding,
        )
    }

    pub fn snapshot_view_cached(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
        previous: Option<&JournalSnapshotView>,
        previous_encoding: Option<&JournalWriterEncoding>,
    ) -> Result<(JournalSnapshotRoots, JournalWriterEncoding), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.writer.snapshot_cached");
        self.request_cache = previous_encoding.and_then(|cache| cache.request_cache.clone());
        let previous = previous
            .zip(previous_encoding)
            .filter(|(previous, encoding)| {
                previous.session() == &snapshot.id
                    && encoding.roots.history.len == previous.history_len() as u64
                    && encoding.roots.usage.len == previous.usage_len() as u64
                    && encoding.manifest_ids.len() == previous.manifest_len()
                    && encoding.manifest_json.len() == previous.manifest_len()
                    && encoding.extension_ids.len() == previous.extension_len()
            });

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
            .map(|(base, encoding)| {
                let extension_changes = encoding
                    .extension_ids
                    .keys()
                    .chain(extension_ids.keys())
                    .filter(|key| encoding.extension_ids.get(*key) != extension_ids.get(*key))
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
        Ok((
            roots.clone(),
            JournalWriterEncoding {
                roots,
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
        use super::semantic::Sequence;
        let previous = self.request_cache.clone();
        let message_base = previous.as_ref().filter(|cache| {
            cache.messages.len() <= request.messages.len()
                && cache
                    .messages
                    .iter()
                    .zip(&request.messages[..cache.messages.len()])
                    .all(|(a, b)| messages_match(std::slice::from_ref(a), std::slice::from_ref(b)))
        });
        #[cfg(feature = "test-support")]
        if let Some(cache) = &previous {
            crate::journal::profile::event(
                "rt.work.unchanged_request_prefix_nodes",
                cache.messages.len() as u64,
                0,
            );
        }
        let message_start = message_base.map_or(0, |cache| cache.messages.len());
        let mut semantic_messages =
            message_base.map_or_else(Sequence::default, |cache| cache.messages.clone());
        let mut added = Vec::new();
        for message in &request.messages[message_start..] {
            added.push(self.add(JournalObjectKind::Message, message)?);
            semantic_messages.push(message.clone())?;
        }
        let (message_view, objects) = message_base.map_or_else(
            || JournalSequenceView::from_items(added.clone()),
            |cache| cache.message_view.append(&added),
        )?;
        for object in objects {
            self.objects.insert(object.id.clone(), object);
        }
        let tool_base = previous.as_ref().filter(|cache| {
            cache.tools.len() <= request.tools.len()
                && cache
                    .tools
                    .iter()
                    .zip(&request.tools[..cache.tools.len()])
                    .all(|(a, b)| {
                        a == b && journal_json_identical(&a.input_schema, &b.input_schema)
                    })
        });
        let tool_start = tool_base.map_or(0, |cache| cache.tools.len());
        let mut semantic_tools =
            tool_base.map_or_else(Sequence::default, |cache| cache.tools.clone());
        let mut added = Vec::new();
        for tool in &request.tools[tool_start..] {
            added.push(self.add(JournalObjectKind::ToolSchema, tool)?);
            semantic_tools.push(tool.clone())?;
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
        self.request_cache = Some(Arc::new(RequestEncodingCache {
            messages: semantic_messages,
            message_view,
            tools: semantic_tools,
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
                value: Arc::new(state.clone()),
                fingerprint,
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
        let state_fingerprint = checkpoint.state.operation_fingerprint();
        checkpoint
            .validation_evidence(Some(&state_fingerprint))
            .validate(&checkpoint.snapshot)?;
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

impl JournalPreparationHints {
    /// Applies runtime-owned typed changes to their captured immutable base.
    /// This produces semantic data only; publication still validates roots,
    /// membership, policy, checkpoint evidence and durable fences.
    #[doc(hidden)]
    pub fn prepare_snapshot(
        &self,
        base: &JournalSnapshotView,
        delta: &JournalTransitionDelta,
        next: &SessionSnapshot,
        overlap: Option<usize>,
    ) -> Result<JournalSnapshotView, RuntimeError> {
        if delta.bootstrap.is_some() {
            return JournalSnapshotView::from_snapshot(next.clone());
        }
        let mut view = base.clone();
        for value in &delta.history_append {
            view.history.push(value.clone())?;
        }
        for value in &delta.usage_append {
            view.usage.push(value.clone())?;
        }
        for value in &delta.manifests_append {
            view.manifests.push(value.clone())?;
        }
        let overlap = overlap.unwrap_or_else(|| retained_overlap_view(base, &next.manifests));
        if delta.manifest_references.len() != next.manifests.len() - overlap {
            return Err(conflict());
        }
        for (index, id) in delta.manifest_references.iter().enumerate() {
            if let Some(evidence) = self.manifests.get(id) {
                view.manifests
                    .push_shared(evidence.value.clone(), evidence.bytes)?;
            } else {
                view.manifests
                    .push(next.manifests[overlap + index].clone())?;
            }
        }
        view.manifests.truncate_front(next.manifests.len())?;
        for key in delta
            .extension_updates
            .keys()
            .chain(delta.extension_references.keys())
        {
            if let Some(evidence) = delta
                .extension_references
                .get(key)
                .and_then(Option::as_ref)
                .and_then(|id| self.extensions.get(id))
                .filter(|e| e.namespace == *key)
            {
                view.extension_state.set_shared(
                    key.clone(),
                    evidence.value.clone(),
                    evidence.bytes,
                )?;
            } else {
                view.extension_state
                    .set(key.clone(), next.extension_state.get(key).cloned())?;
            }
        }
        view.identity = next.identity.clone();
        view.updated = next.updated;
        Ok(view)
    }
}

impl JournalWriterEncoding {
    fn bound_hint(&self, base: &JournalSnapshotView) -> Option<&JournalWriterMutationHint> {
        self.mutation_hint.as_ref().filter(|h| {
            h.base.session() == base.session()
                && h.base.history.same(&base.history)
                && h.base.usage.same(&base.usage)
                && h.base.manifests.same(&base.manifests)
                && h.base.extension_state.same(&base.extension_state)
        })
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
            Self::Inline { bytes } => *bytes,
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
