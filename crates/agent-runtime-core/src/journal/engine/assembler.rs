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
        _previous: Option<&SessionSnapshot>,
        _previous_encoding: Option<&SnapshotEncodingCache>,
        _overrides: Option<&JournalTransitionDelta>,
    ) -> Result<(JournalSnapshotRoots, SnapshotEncodingCache), RuntimeError> {
        self.snapshot_view(
            &JournalSnapshotView::from_snapshot(snapshot.clone())?,
            planned_step_count,
            None,
            None,
        )
    }

    pub(in crate::journal) fn snapshot_view(
        &mut self,
        snapshot: &JournalSnapshotView,
        planned_step_count: u64,
        previous: Option<(&JournalSnapshotView, &SnapshotEncodingCache)>,
        delta: Option<&JournalTransitionDelta>,
    ) -> Result<(JournalSnapshotRoots, SnapshotEncodingCache), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.assembler.snapshot_cached");
        use crate::journal::semantic::Sequence;
        let history_start = previous.map_or(0, |(v, _)| v.history.len());
        let usage_start = previous.map_or(0, |(v, _)| v.usage.len());
        let mut history_ids =
            previous.map_or_else(Sequence::default, |(_, c)| c.history_ids.clone());
        let mut usage_ids = previous.map_or_else(Sequence::default, |(_, c)| c.usage_ids.clone());
        let mut added_history = Vec::new();
        for i in history_start..snapshot.history.len() {
            let id = self.add(
                JournalObjectKind::Message,
                snapshot.history.get(i).ok_or_else(conflict)?,
            )?;
            history_ids.push(id.clone())?;
            added_history.push(id);
        }
        let mut added_usage = Vec::new();
        for i in usage_start..snapshot.usage.len() {
            let id = self.add(
                JournalObjectKind::UsageRecord,
                snapshot.usage.get(i).ok_or_else(conflict)?,
            )?;
            usage_ids.push(id.clone())?;
            added_usage.push(id);
        }
        let (history_view, objects) = previous.map_or_else(
            || JournalSequenceView::from_items(added_history.clone()),
            |(_, c)| c.history_view.append(&added_history),
        )?;
        for object in objects {
            self.objects.insert(object.id, object.bytes);
        }
        let (usage_view, objects) = previous.map_or_else(
            || JournalSequenceView::from_items(added_usage.clone()),
            |(_, c)| c.usage_view.append(&added_usage),
        )?;
        for object in objects {
            self.objects.insert(object.id, object.bytes);
        }
        let history_chain = previous.map_or_else(
            || journal_history_chain(&added_history),
            |(_, c)| journal_history_chain_append(&c.roots.history_chain, &added_history),
        );
        let new_manifests = delta
            .filter(|d| d.bootstrap.is_none() && previous.is_some())
            .map(|d| d.manifests_append.len() + d.manifest_references.len());
        let mut manifest_ids = previous
            .filter(|_| new_manifests.is_some())
            .map_or_else(Sequence::default, |(_, c)| c.manifest_ids.clone());
        let count = new_manifests.unwrap_or(snapshot.manifests.len());
        let retained = snapshot.manifests.len().saturating_sub(count);
        let skipped = count.saturating_sub(snapshot.manifests.len());
        if previous.is_some() && new_manifests.is_some() {
            manifest_ids.truncate_front(retained)?;
        }
        for i in retained..snapshot.manifests.len() {
            let reference = delta.and_then(|d| {
                (skipped + i - retained)
                    .checked_sub(d.manifests_append.len())
                    .and_then(|index| d.manifest_references.get(index))
            });
            let id = match reference {
                Some(id) => id.clone(),
                None => self.manifest(snapshot.manifests.get(i).ok_or_else(conflict)?)?,
            };
            manifest_ids.push(id)?;
        }
        let manifests = if previous.is_some_and(|(v, _)| snapshot.manifests.same(&v.manifests)) {
            previous.expect("present").1.roots.manifests.clone()
        } else {
            self.sequence(manifest_ids.iter().cloned().collect())?
        };
        if planned_step_count < manifests.len {
            return Err(conflict());
        }
        let mut extension_ids =
            previous.map_or_else(BTreeMap::new, |(_, c)| c.extension_ids.clone());
        if let Some(delta) = delta.filter(|d| d.bootstrap.is_none() && previous.is_some()) {
            for key in delta
                .extension_updates
                .keys()
                .chain(delta.extension_references.keys())
            {
                if let Some(value) = snapshot.extension_state.get(key) {
                    let id = match delta.extension_references.get(key).and_then(Option::as_ref) {
                        Some(id) => id.clone(),
                        None => self.extension(key, value)?,
                    };
                    extension_ids.insert(key.clone(), id);
                } else {
                    extension_ids.remove(key);
                }
            }
        } else {
            extension_ids.clear();
            for (key, value) in snapshot.extension_state.iter() {
                extension_ids.insert(key.clone(), self.extension(key, value)?);
            }
        }
        let extensions = if previous.is_some_and(|(_, c)| c.extension_ids == extension_ids) {
            previous.expect("present").1.roots.extensions.clone()
        } else {
            self.map(
                extension_ids
                    .iter()
                    .map(|(key, value)| JournalMapEntry {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            )?
        };
        let roots = JournalSnapshotRoots {
            history: history_view.sequence(),
            history_chain,
            usage: usage_view.sequence(),
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
                history_view,
                usage_view,
                policy_revision: None,
            },
        ))
    }
}
