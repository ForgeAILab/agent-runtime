//! Borrowed typed inputs for structured journal metadata. No owned JSON mirror
//! is retained alongside a manifest or versioned component state.
use super::*;
use crate::manifest::{ContextSegmentRecord, RunManifest};
use crate::provider::{CacheIdentity, CacheIdentityFragment, CacheIdentityTool};
use std::borrow::Cow;

#[derive(Clone, Copy)]
pub(super) enum ArraySource<'a> {
    Values(&'a [Value]),
    Segments(&'a [ContextSegmentRecord]),
    Fragments(&'a [CacheIdentityFragment]),
    Tools(&'a [CacheIdentityTool]),
}
impl<'a> ArraySource<'a> {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Values(v) => v.len(),
            Self::Segments(v) => v.len(),
            Self::Fragments(v) => v.len(),
            Self::Tools(v) => v.len(),
        }
    }
    pub(super) fn value(&self, index: usize) -> Result<Cow<'a, Value>, RuntimeError> {
        Ok(match self {
            Self::Values(v) => Cow::Borrowed(v.get(index).ok_or_else(conflict)?),
            Self::Segments(v) => {
                Cow::Owned(serde_json::to_value(v.get(index).ok_or_else(conflict)?)?)
            }
            Self::Fragments(v) => {
                Cow::Owned(serde_json::to_value(v.get(index).ok_or_else(conflict)?)?)
            }
            Self::Tools(v) => Cow::Owned(serde_json::to_value(v.get(index).ok_or_else(conflict)?)?),
        })
    }
    pub(super) fn same_typed(&self, other: &Self, index: usize) -> bool {
        match (self, other) {
            (Self::Segments(a), Self::Segments(b)) => {
                a.get(index).zip(b.get(index)).is_some_and(|(a, b)| {
                    let ContextSegmentRecord {
                        id,
                        kind,
                        sensitivity,
                        content_hash,
                        tokens,
                    } = a;
                    let ContextSegmentRecord {
                        id: bid,
                        kind: bkind,
                        sensitivity: bsensitivity,
                        content_hash: bhash,
                        tokens: btokens,
                    } = b;
                    id == bid
                        && kind == bkind
                        && sensitivity == bsensitivity
                        && content_hash == bhash
                        && tokens == btokens
                })
            }
            (Self::Fragments(a), Self::Fragments(b)) => {
                a.get(index).zip(b.get(index)).is_some_and(|(a, b)| {
                    let CacheIdentityFragment { id, hash } = a;
                    let CacheIdentityFragment {
                        id: bid,
                        hash: bhash,
                    } = b;
                    id == bid && hash == bhash
                })
            }
            (Self::Tools(a), Self::Tools(b)) => {
                a.get(index).zip(b.get(index)).is_some_and(|(a, b)| {
                    let CacheIdentityTool {
                        name,
                        description,
                        schema,
                        ordinal,
                    } = a;
                    let CacheIdentityTool {
                        name: bname,
                        description: bdescription,
                        schema: bschema,
                        ordinal: bordinal,
                    } = b;
                    name == bname
                        && description == bdescription
                        && schema == bschema
                        && ordinal == bordinal
                })
            }
            _ => false,
        }
    }
}

pub(super) enum JsonSource<'a> {
    Borrowed(&'a Value),
    Owned(Value),
    Object(Vec<(String, JsonSource<'a>)>),
    Array(ArraySource<'a>),
}
impl<'a> JsonSource<'a> {
    fn fields(header: Value, mut replacements: BTreeMap<&str, Self>) -> Result<Self, RuntimeError> {
        let Value::Object(fields) = header else {
            return Err(conflict());
        };
        let fields = fields
            .into_iter()
            .map(|(key, value)| {
                let source = replacements
                    .remove(key.as_str())
                    .unwrap_or(Self::Owned(value));
                (key, source)
            })
            .collect();
        if !replacements.is_empty() {
            return Err(conflict());
        }
        Ok(Self::Object(fields))
    }
    pub(super) fn state(value: &'a VersionedSessionState) -> Result<Self, RuntimeError> {
        let VersionedSessionState {
            revision,
            sensitivity,
            value: data,
        } = value;
        let header = VersionedSessionState {
            revision: revision.clone(),
            sensitivity: *sensitivity,
            value: Value::Null,
        };
        Self::fields(
            serde_json::to_value(header)?,
            BTreeMap::from([("value", Self::Borrowed(data))]),
        )
    }
    fn identity(
        value: &'a CacheIdentity,
        trusted: Option<&CacheIdentity>,
    ) -> Result<Self, RuntimeError> {
        if !trusted.is_some_and(|old| std::ptr::eq(old, value) || old == value) {
            value.validate().map_err(RuntimeError::conflict)?;
        }
        let (prefix, tools, history) = value.journal_projections();
        Self::fields(
            value.journal_header()?,
            BTreeMap::from([
                ("stable_prefix", Self::Array(ArraySource::Fragments(prefix))),
                ("tools", Self::Array(ArraySource::Tools(tools))),
                (
                    "stable_history",
                    Self::Array(ArraySource::Fragments(history)),
                ),
            ]),
        )
    }
    pub(super) fn manifest(
        value: &'a TurnManifest,
        trusted_identity: Option<&CacheIdentity>,
    ) -> Result<Self, RuntimeError> {
        #[derive(Serialize)]
        struct Header<'a> {
            schema_version: u32,
            registry_snapshot: &'a agent_runtime_registry::Fingerprint,
            scoped_view: &'a agent_runtime_registry::Fingerprint,
            model: &'a crate::manifest::ModelResolution,
            capability_resolution: &'a crate::manifest::CapabilityResolution,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            activation: &'a Vec<crate::manifest::ActivatedCapability>,
            policy_revisions: &'a crate::manifest::PolicyRevisions,
            #[serde(skip_serializing_if = "Option::is_none")]
            segments: Option<()>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            summaries: &'a Vec<crate::manifest::SummaryCoverage>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            lossless_summaries: &'a Vec<crate::manifest::LosslessSummaryRecord>,
            context_fingerprint: &'a agent_runtime_registry::Fingerprint,
            cache_plan_fingerprint: &'a agent_runtime_registry::Fingerprint,
            #[serde(skip_serializing_if = "Option::is_none")]
            cache_identity: Option<()>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            reasons: &'a Vec<crate::manifest::ManifestReason>,
        }
        let TurnManifest {
            turn,
            manifest,
            internal_source,
        } = value;
        let RunManifest {
            schema_version,
            registry_snapshot,
            scoped_view,
            model,
            capability_resolution,
            activation,
            policy_revisions,
            segments,
            summaries,
            lossless_summaries,
            context_fingerprint,
            cache_plan_fingerprint,
            cache_identity,
            reasons,
        } = manifest;
        let header = Header {
            schema_version: *schema_version,
            registry_snapshot,
            scoped_view,
            model,
            capability_resolution,
            activation,
            policy_revisions,
            segments: (!segments.is_empty()).then_some(()),
            summaries,
            lossless_summaries,
            context_fingerprint,
            cache_plan_fingerprint,
            cache_identity: cache_identity.as_ref().map(|_| ()),
            reasons,
        };
        let mut replacements = BTreeMap::new();
        if !segments.is_empty() {
            replacements.insert("segments", Self::Array(ArraySource::Segments(segments)));
        }
        if let Some(identity) = cache_identity {
            replacements.insert(
                "cache_identity",
                Self::identity(identity, trusted_identity)?,
            );
        }
        let manifest = Self::fields(serde_json::to_value(header)?, replacements)?;
        #[derive(Serialize)]
        struct TurnHeader<'a> {
            turn: &'a crate::ids::TurnId,
            manifest: (),
            #[serde(skip_serializing_if = "Option::is_none")]
            internal_source: &'a Option<crate::content::InternalTurnSource>,
        }
        Self::fields(
            serde_json::to_value(TurnHeader {
                turn,
                manifest: (),
                internal_source,
            })?,
            BTreeMap::from([("manifest", manifest)]),
        )
    }
    fn value_ref(&self) -> Option<&Value> {
        match self {
            Self::Borrowed(v) => Some(v),
            Self::Owned(v) => Some(v),
            _ => None,
        }
    }
    fn inline(&self) -> Result<Option<Value>, RuntimeError> {
        match self {
            Self::Borrowed(value) => Ok(journal_json_can_inline(value)?.then(|| (*value).clone())),
            Self::Owned(value) => Ok(journal_json_can_inline(value)?.then(|| value.clone())),
            Self::Array(values) => {
                if values.len() > JOURNAL_FANOUT {
                    return Ok(None);
                }
                let mut out = Vec::new();
                for index in 0..values.len() {
                    out.push(values.value(index)?.into_owned());
                    if !journal_json_can_inline(&Value::Array(out.clone()))? {
                        return Ok(None);
                    }
                }
                Ok(Some(Value::Array(out)))
            }
            Self::Object(fields) => {
                let mut out = serde_json::Map::new();
                for (key, value) in fields {
                    let Some(value) = value.inline()? else {
                        return Ok(None);
                    };
                    out.insert(key.clone(), value);
                    if !journal_json_can_inline(&Value::Object(out.clone()))? {
                        return Ok(None);
                    }
                }
                Ok(Some(Value::Object(out)))
            }
        }
    }
}

impl JournalWriterAssembler {
    fn source_item(
        &mut self,
        value: &JsonSource<'_>,
        prior: Option<(&JsonSource<'_>, &JsonItemEncodingCache)>,
    ) -> Result<(JournalJsonItem, JsonItemEncodingCache, bool), RuntimeError> {
        if let Some(inline) = value.inline()? {
            let same = if let Some((old, JsonItemEncodingCache::Inline { .. })) = prior {
                old.inline()?
                    .is_some_and(|old| journal_json_identical(&old, &inline))
            } else {
                false
            };
            return Ok((
                JournalJsonItem::Inline {
                    value: inline.clone(),
                },
                JsonItemEncodingCache::Inline {
                    bytes: encoding::canonical_journal_value_len(&inline)?,
                    depth: json_depth(&inline)?,
                },
                same,
            ));
        }
        let prior = prior.and_then(|(value, cache)| match cache {
            JsonItemEncodingCache::Reference(cache) => Some((value, cache.as_ref())),
            _ => None,
        });
        let cache = self.source_cached(value, prior)?;
        let same = prior.is_some_and(|(_, old)| old.root == cache.root);
        Ok((
            JournalJsonItem::Reference {
                object: cache.root.clone(),
            },
            JsonItemEncodingCache::Reference(Arc::new(cache)),
            same,
        ))
    }
    pub(super) fn source_cached(
        &mut self,
        value: &JsonSource<'_>,
        prior: Option<(&JsonSource<'_>, &JournalJsonEncoding)>,
    ) -> Result<JournalJsonEncoding, RuntimeError> {
        if let Some(raw) = value.value_ref() {
            return self.json_value_cached(
                raw,
                prior.and_then(|(value, cache)| value.value_ref().map(|value| (value, cache))),
            );
        }
        if let Some((_, cache)) = prior {
            self.register_witness(&cache.witness);
        }
        match value {
            JsonSource::Array(values) => {
                let previous =
                    prior.and_then(|(value, cache)| match (value, cache.node.as_ref()) {
                        (JsonSource::Array(values), JsonEncodingNode::Array(array)) => {
                            Some((*values, array))
                        }
                        _ => None,
                    });
                let (items, array, same) =
                    self.structured_sequence_inputs_cached(*values, previous)?;
                if same {
                    return prior.map(|(_, c)| c.clone()).ok_or_else(conflict);
                }
                let root = self.add_structured(
                    JournalObjectKind::JsonValue,
                    &JournalJsonValue::Array { items },
                )?;
                Ok(JournalJsonEncoding {
                    witness: self
                        .json_witnesses
                        .get(&root)
                        .cloned()
                        .ok_or_else(conflict)?,
                    root,
                    canonical_len: array.canonical_len,
                    depth: array.depth()?,
                    node: Arc::new(JsonEncodingNode::Array(array)),
                    typed: None,
                })
            }
            JsonSource::Object(fields) => {
                let previous =
                    prior.and_then(|(value, cache)| match (value, cache.node.as_ref()) {
                        (
                            JsonSource::Object(fields),
                            JsonEncodingNode::Object { entries, sequence },
                        ) => Some((fields, entries, sequence)),
                        _ => None,
                    });
                let mut len = 2usize
                    .checked_add(fields.len().saturating_sub(1))
                    .ok_or_else(conflict)?;
                let mut entries = Vec::new();
                let mut ids = Vec::new();
                let mut same = previous.is_some_and(|(old, entries, _)| {
                    old.len() == fields.len() && entries.len() == fields.len()
                });
                for (index, (key, value)) in fields.iter().enumerate() {
                    let old = previous.and_then(|(fields, entries, _)| {
                        let (old_key, old_value) = fields.get(index)?;
                        let entry = entries.get(index)?;
                        (key == old_key && key == &entry.key).then_some((old_value, entry))
                    });
                    let (item, cache, unchanged) =
                        self.source_item(value, old.map(|(value, entry)| (value, &entry.item)))?;
                    let object = if unchanged {
                        old.map(|(_, entry)| entry.object.clone())
                            .ok_or_else(conflict)?
                    } else {
                        self.add_structured(
                            JournalObjectKind::JsonObjectEntry,
                            &JournalJsonObjectEntry {
                                key: key.clone(),
                                value: item,
                            },
                        )?
                    };
                    len = len
                        .checked_add(encoding::canonical_journal_json_len(key)?)
                        .and_then(|n| n.checked_add(1))
                        .and_then(|n| n.checked_add(cache.value_len()))
                        .ok_or_else(conflict)?;
                    ids.push(object.clone());
                    entries.push(JsonObjectEntryEncodingCache {
                        key: key.clone(),
                        item: cache,
                        object,
                    });
                    same &= unchanged;
                }
                if same {
                    return prior.map(|(_, c)| c.clone()).ok_or_else(conflict);
                }
                let sequence = self.structured_object_sequence(ids)?;
                let root = self.add_structured(
                    JournalObjectKind::JsonValue,
                    &JournalJsonValue::Object {
                        entries: sequence.clone(),
                    },
                )?;
                let depth = container_depth(entries.iter().map(|entry| entry.item.depth()))?;
                Ok(JournalJsonEncoding {
                    witness: self
                        .json_witnesses
                        .get(&root)
                        .cloned()
                        .ok_or_else(conflict)?,
                    root,
                    canonical_len: len,
                    node: Arc::new(JsonEncodingNode::Object { entries, sequence }),
                    typed: None,
                    depth,
                })
            }
            _ => Err(conflict()),
        }
    }
}
