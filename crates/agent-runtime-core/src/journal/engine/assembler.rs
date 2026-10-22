use super::*;

#[derive(Default)]
pub(in crate::journal) struct ObjectAssembler {
    pub(in crate::journal) objects: BTreeMap<JournalObjectId, Vec<u8>>,
}

impl ObjectAssembler {
    pub(in crate::journal) fn add<T: Serialize>(
        &mut self,
        kind: JournalObjectKind,
        value: &T,
    ) -> Result<JournalObjectId, RuntimeError> {
        let object = JournalObject::encode(kind, value)?;
        #[cfg(feature = "test-support")]
        crate::journal::profile::event("rt.work.encoded_objects", 1, object.bytes.len() as u64);
        self.objects
            .entry(object.id.clone())
            .or_insert(object.bytes);
        Ok(object.id)
    }

    pub(in crate::journal) fn sequence(
        &mut self,
        items: Vec<JournalObjectId>,
    ) -> Result<JournalSequence, RuntimeError> {
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
                    height: children[0].height.checked_add(1).ok_or_else(conflict)?,
                });
            }
            trees = parents;
        }
        if trees.len() != 1 {
            return Err(conflict());
        }
        trees.pop().ok_or_else(conflict)
    }

    pub(in crate::journal) fn structured_sequence(
        &mut self,
        values: &[serde_json::Value],
    ) -> Result<JournalJsonArray, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.assembler.structured_sequence");
        if values.is_empty() {
            return Ok(JournalJsonArray {
                len: 0,
                height: 0,
                node: JournalSequenceNode::JsonLeaf { items: Vec::new() },
            });
        }
        if values.len() <= JOURNAL_FANOUT {
            let items = values
                .iter()
                .map(|value| self.json_array_item(value))
                .collect::<Result<Vec<_>, _>>()?;
            let len = items.len() as u64;
            return Ok(JournalJsonArray {
                len,
                height: 0,
                node: JournalSequenceNode::JsonLeaf { items },
            });
        }
        let mut trees = Vec::new();
        // One compact value object per leaf keeps append work independent of
        // the prior tail while avoiding scalar/key subobjects inside it.
        for values in values.chunks(2) {
            let items = values
                .iter()
                .map(|value| self.json_array_item(value))
                .collect::<Result<Vec<_>, _>>()?;
            let root = self.add(
                JournalObjectKind::Sequence,
                &JournalSequenceNode::JsonValueBranch { items },
            )?;
            trees.push(JournalSequence {
                root: Some(root),
                len: values.len() as u64,
                height: 1,
            });
        }
        while trees.len() > 2 {
            if trees[0].height >= JOURNAL_MAX_TREE_HEIGHT {
                return Err(conflict());
            }
            let mut parents = Vec::new();
            for children in trees.chunks(2) {
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
                let root = self.add(
                    JournalObjectKind::JsonBranch,
                    &JournalJsonBranch(child_roots),
                )?;
                parents.push(JournalSequence {
                    root: Some(root),
                    len: children.iter().map(|child| child.len).sum(),
                    height: children[0].height.checked_add(1).ok_or_else(conflict)?,
                });
            }
            trees = parents;
        }
        let children = trees;
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
        Ok(JournalJsonArray {
            len: values.len() as u64,
            height: children[0].height.checked_add(1).ok_or_else(conflict)?,
            node: JournalSequenceNode::JsonBranch {
                children: child_roots,
            },
        })
    }

    pub(in crate::journal) fn structured_object_sequence(
        &mut self,
        items: Vec<JournalObjectId>,
    ) -> Result<JournalSequence, RuntimeError> {
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
        while trees.len() > 1 {
            let mut parents = Vec::new();
            for children in trees.chunks(JOURNAL_FANOUT) {
                let root = self.add(
                    JournalObjectKind::Sequence,
                    &JournalSequenceNode::Branch {
                        children: children.to_vec(),
                    },
                )?;
                parents.push(JournalSequence {
                    root: Some(root),
                    len: children.iter().map(|child| child.len).sum(),
                    height: children[0].height.checked_add(1).ok_or_else(conflict)?,
                });
            }
            trees = parents;
        }
        trees.pop().ok_or_else(conflict)
    }

    pub(in crate::journal) fn map(
        &mut self,
        entries: Vec<JournalMapEntry>,
    ) -> Result<JournalMap, RuntimeError> {
        if entries.is_empty() {
            return Ok(JournalMap::default());
        }
        if entries.iter().any(|entry| entry.key.is_empty())
            || entries.windows(2).any(|pair| pair[0].key >= pair[1].key)
        {
            return Err(conflict());
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
                            .ok_or_else(conflict)?,
                    },
                });
            }
            trees = parents;
        }
        Ok(trees.pop().ok_or_else(conflict)?.tree)
    }

    pub(in crate::journal) fn json_value(
        &mut self,
        value: &serde_json::Value,
    ) -> Result<JournalObjectId, RuntimeError> {
        let node = match value {
            serde_json::Value::Array(values) => JournalJsonValue::Array {
                items: self.structured_sequence(values)?,
            },
            serde_json::Value::Object(values) => {
                let entries = values
                    .iter()
                    .map(|(key, value)| {
                        let value = self.json_item(value)?;
                        self.add(
                            JournalObjectKind::JsonObjectEntry,
                            &JournalJsonObjectEntry {
                                key: key.clone(),
                                value,
                            },
                        )
                    })
                    .collect::<Result<Vec<_>, RuntimeError>>()?;
                JournalJsonValue::Object {
                    entries: self.structured_object_sequence(entries)?,
                }
            }
            scalar => JournalJsonValue::Scalar {
                scalar: scalar.clone(),
            },
        };
        self.add(JournalObjectKind::JsonValue, &node)
    }

    pub(in crate::journal) fn json_item(
        &mut self,
        value: &serde_json::Value,
    ) -> Result<JournalJsonItem, RuntimeError> {
        if journal_json_can_inline(value)? {
            Ok(JournalJsonItem::Inline {
                value: value.clone(),
            })
        } else {
            Ok(JournalJsonItem::Reference {
                object: self.json_value(value)?,
            })
        }
    }

    pub(in crate::journal) fn json_array_item(
        &mut self,
        value: &serde_json::Value,
    ) -> Result<JournalJsonItem, RuntimeError> {
        if journal_json_can_inline(value)? {
            let object = self.add(
                JournalObjectKind::JsonInline,
                &JournalJsonInline(value.clone()),
            )?;
            Ok(JournalJsonItem::Reference { object })
        } else {
            self.json_item(value)
        }
    }

    pub(in crate::journal) fn extension(
        &mut self,
        namespace: &str,
        value: &crate::store::VersionedSessionState,
    ) -> Result<JournalObjectId, RuntimeError> {
        if namespace != RUNTIME_PREVIOUS_CACHE_NAMESPACE {
            return self.add(JournalObjectKind::Extension, value);
        }
        if value.sensitivity != crate::store::SessionStateSensitivity::RedactionSafe {
            return Err(conflict());
        }
        let value = serde_json::to_value(value).map_err(|_| conflict())?;
        let value = self.json_value(&value)?;
        self.add(
            JournalObjectKind::ExtensionReference,
            &ReferencedSessionState { value },
        )
    }

    pub(in crate::journal) fn manifest(
        &mut self,
        manifest: &crate::store::TurnManifest,
    ) -> Result<JournalObjectId, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.assembler.manifest");
        let value = serde_json::to_value(manifest).map_err(|_| conflict())?;
        let value = self.json_value(&value)?;
        self.add(
            JournalObjectKind::ManifestReference,
            &ReferencedTurnManifest { value },
        )
    }

    #[cfg(test)]
    pub(in crate::journal) fn snapshot(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
    ) -> Result<JournalSnapshotRoots, RuntimeError> {
        self.snapshot_cached(snapshot, planned_step_count, None, None, None)
            .map(|(roots, _)| roots)
    }

    pub(in crate::journal) fn snapshot_cached(
        &mut self,
        snapshot: &SessionSnapshot,
        planned_step_count: u64,
        previous: Option<&SessionSnapshot>,
        previous_encoding: Option<&SnapshotEncodingCache>,
        overrides: Option<&JournalTransitionDelta>,
    ) -> Result<(JournalSnapshotRoots, SnapshotEncodingCache), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.assembler.snapshot_cached");
        let previous = previous
            .zip(previous_encoding)
            .filter(|(previous, encoding)| {
                previous.id == snapshot.id
                    && encoding.history_ids.len() == previous.history.len()
                    && encoding.usage_ids.len() == previous.usage.records().len()
                    && encoding.manifest_ids.len() == previous.manifests.len()
                    && encoding.extension_ids.len() == previous.extension_state.len()
            });

        let (history_ids, history, history_chain) = match previous {
            Some((previous, encoding)) if previous.history == snapshot.history => {
                #[cfg(feature = "test-support")]
                crate::journal::profile::event(
                    "rt.work.unchanged_history_prefix_nodes",
                    previous.history.len() as u64,
                    0,
                );
                (
                    encoding.history_ids.clone(),
                    encoding.roots.history.clone(),
                    encoding.roots.history_chain.clone(),
                )
            }
            Some((previous, encoding))
                if previous.history.len() <= snapshot.history.len()
                    && previous.history == snapshot.history[..previous.history.len()] =>
            {
                #[cfg(feature = "test-support")]
                crate::journal::profile::event(
                    "rt.work.unchanged_history_prefix_nodes",
                    previous.history.len() as u64,
                    0,
                );
                let mut ids = encoding.history_ids.clone();
                ids.extend(
                    snapshot.history[previous.history.len()..]
                        .iter()
                        .map(|message| self.add(JournalObjectKind::Message, message))
                        .collect::<Result<Vec<_>, _>>()?,
                );
                let tree = self.sequence(ids.clone())?;
                let chain = journal_history_chain(&ids);
                (ids, tree, chain)
            }
            _ => {
                let ids = snapshot
                    .history
                    .iter()
                    .map(|message| self.add(JournalObjectKind::Message, message))
                    .collect::<Result<Vec<_>, _>>()?;
                let tree = self.sequence(ids.clone())?;
                let chain = journal_history_chain(&ids);
                (ids, tree, chain)
            }
        };

        let records = snapshot.usage.records();
        let (usage_ids, usage) = match previous {
            Some((previous, encoding)) if previous.usage.records() == records => {
                (encoding.usage_ids.clone(), encoding.roots.usage.clone())
            }
            Some((previous, encoding))
                if previous.usage.records().len() <= records.len()
                    && previous.usage.records() == &records[..previous.usage.records().len()] =>
            {
                let mut ids = encoding.usage_ids.clone();
                ids.extend(
                    records[previous.usage.records().len()..]
                        .iter()
                        .map(|record| self.add(JournalObjectKind::UsageRecord, record))
                        .collect::<Result<Vec<_>, _>>()?,
                );
                let tree = self.sequence(ids.clone())?;
                (ids, tree)
            }
            _ => {
                let ids = records
                    .iter()
                    .map(|record| self.add(JournalObjectKind::UsageRecord, record))
                    .collect::<Result<Vec<_>, _>>()?;
                let tree = self.sequence(ids.clone())?;
                (ids, tree)
            }
        };

        let overlap = if let Some((previous, _)) = previous {
            let max = previous.manifests.len().min(snapshot.manifests.len());
            (0..=max)
                .rev()
                .find(|overlap| {
                    previous.manifests[previous.manifests.len() - overlap..]
                        == snapshot.manifests[..*overlap]
                })
                .unwrap_or(0)
        } else {
            0
        };
        let mut manifest_ids = previous.map_or_else(Vec::new, |(previous, encoding)| {
            encoding.manifest_ids[previous.manifests.len() - overlap..].to_vec()
        });
        let new_manifest_count = snapshot.manifests.len() - overlap;
        if let Some(references) = overrides
            .map(|delta| delta.manifest_references.as_slice())
            .filter(|references| references.len() == new_manifest_count)
        {
            manifest_ids.extend_from_slice(references);
        } else {
            manifest_ids.extend(
                snapshot.manifests[overlap..]
                    .iter()
                    .map(|manifest| self.manifest(manifest))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        let manifests = previous
            .filter(|(previous, _)| previous.manifests == snapshot.manifests)
            .map_or_else(
                || self.sequence(manifest_ids.clone()),
                |(_, encoding)| Ok(encoding.roots.manifests.clone()),
            )?;
        if planned_step_count < manifests.len {
            return Err(conflict());
        }

        let mut extension_ids = BTreeMap::new();
        for (key, value) in &snapshot.extension_state {
            let id = match previous {
                Some((previous, encoding))
                    if previous
                        .extension_state
                        .get(key)
                        .is_some_and(|previous| state_identical(previous, value)) =>
                {
                    encoding
                        .extension_ids
                        .get(key)
                        .cloned()
                        .ok_or_else(conflict)?
                }
                _ if overrides
                    .and_then(|delta| delta.extension_references.get(key))
                    .and_then(Option::as_ref)
                    .is_some() =>
                {
                    overrides
                        .and_then(|delta| delta.extension_references.get(key))
                        .and_then(Option::as_ref)
                        .cloned()
                        .ok_or_else(conflict)?
                }
                _ => self.extension(key, value)?,
            };
            extension_ids.insert(key.clone(), id);
        }
        let entries = extension_ids
            .iter()
            .map(|(key, value)| JournalMapEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let extensions = previous
            .filter(|(previous, _)| {
                previous.extension_state.len() == snapshot.extension_state.len()
                    && previous
                        .extension_state
                        .iter()
                        .zip(&snapshot.extension_state)
                        .all(|((lk, lv), (rk, rv))| lk == rk && state_identical(lv, rv))
            })
            .map_or_else(
                || self.map(entries),
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
        Ok((
            roots.clone(),
            SnapshotEncodingCache {
                roots,
                history_ids,
                usage_ids,
                manifest_ids,
                extension_ids,
            },
        ))
    }
}
