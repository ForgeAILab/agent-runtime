use super::*;

/// Resolves candidate structured records from already-projected reachable
/// objects plus the current commit's drafts. Drafts are never made readable in
/// the ordinary domain; after host policy runs, only the projected assembler's
/// objects are installed.
pub(in crate::journal) struct DraftReader<'a> {
    pub(in crate::journal) stored: &'a ObjectStore,
    pub(in crate::journal) supplied: &'a BTreeMap<JournalObjectId, Vec<u8>>,
    pub(in crate::journal) reachable: &'a ReachableSet,
}

pub(in crate::journal) fn draft_json_branch_children(
    tree: &JournalSequence,
    children: Vec<JournalCompactObjectId>,
) -> Result<Vec<JournalSequence>, RuntimeError> {
    if tree.height == 0 || children.is_empty() || children.len() > 2 {
        return Err(conflict());
    }
    let child_height = tree.height - 1;
    let capacity = 1u64.checked_shl(child_height).ok_or_else(conflict)?;
    if tree.len == 0 || tree.len > capacity.checked_mul(2).ok_or_else(conflict)? {
        return Err(conflict());
    }
    let left_len = tree.len.min(capacity);
    let right_len = tree.len - left_len;
    let expected = if right_len == 0 { 1 } else { 2 };
    if children.len() != expected {
        return Err(conflict());
    }
    children
        .into_iter()
        .enumerate()
        .map(|(index, root)| {
            Ok(JournalSequence {
                root: Some(root.to_object()?),
                len: [left_len, right_len][index],
                height: child_height,
            })
        })
        .collect::<Result<Vec<_>, RuntimeError>>()
}

impl<'a> DraftReader<'a> {
    pub(in crate::journal) fn new(
        stored: &'a ObjectStore,
        supplied: &'a BTreeMap<JournalObjectId, Vec<u8>>,
        reachable: &'a ReachableSet,
    ) -> Self {
        Self {
            stored,
            supplied,
            reachable,
        }
    }

    pub(in crate::journal) fn object<T: serde::de::DeserializeOwned + Serialize>(
        &self,
        id: &JournalObjectId,
        kind: JournalObjectKind,
    ) -> Result<T, RuntimeError> {
        let bytes = self
            .supplied
            .get(id)
            .or_else(|| {
                self.reachable
                    .contains(id)
                    .then(|| self.stored.get(id))
                    .flatten()
            })
            .cloned()
            .ok_or_else(conflict)?;
        JournalObject {
            id: id.clone(),
            bytes,
        }
        .decode(kind)
    }

    pub(in crate::journal) fn encoded_object(
        &self,
        id: &JournalObjectId,
    ) -> Result<JournalObject, RuntimeError> {
        let bytes = self
            .supplied
            .get(id)
            .or_else(|| {
                self.reachable
                    .contains(id)
                    .then(|| self.stored.get(id))
                    .flatten()
            })
            .cloned()
            .ok_or_else(conflict)?;
        Ok(JournalObject {
            id: id.clone(),
            bytes,
        })
    }

    pub(in crate::journal) fn sequence(
        &self,
        sequence: &JournalSequence,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        if (sequence.root.is_none() != (sequence.len == 0))
            || (sequence.len == 0 && sequence.height != 0)
            || sequence.height > JOURNAL_MAX_TREE_HEIGHT
        {
            return Err(conflict());
        }
        let mut pending = vec![sequence.clone()];
        let mut items = Vec::new();
        while let Some(tree) = pending.pop() {
            let Some(root) = &tree.root else {
                continue;
            };
            let node: JournalSequenceNode = self.object(root, JournalObjectKind::Sequence)?;
            match node {
                JournalSequenceNode::Leaf { items: leaves } => {
                    if tree.height != 0
                        || leaves.is_empty()
                        || leaves.len() > JOURNAL_FANOUT
                        || leaves.len() as u64 != tree.len
                    {
                        return Err(conflict());
                    }
                    items.extend(leaves);
                }
                JournalSequenceNode::JsonLeaf { .. } => return Err(conflict()),
                JournalSequenceNode::JsonValueBranch { .. } => return Err(conflict()),
                JournalSequenceNode::JsonBranch { .. } => return Err(conflict()),
                JournalSequenceNode::Branch { children } => {
                    if tree.height == 0 || children.is_empty() || children.len() > JOURNAL_FANOUT {
                        return Err(conflict());
                    }
                    let mut len = 0u64;
                    for child in &children {
                        if child.root.is_none() || child.len == 0 || child.height != tree.height - 1
                        {
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
            if items.len() as u64 > sequence.len {
                return Err(conflict());
            }
        }
        if items.len() as u64 != sequence.len {
            return Err(conflict());
        }
        Ok(items)
    }

    pub(in crate::journal) fn json_array(
        &self,
        array: JournalJsonArray,
    ) -> Result<Vec<JournalJsonItem>, RuntimeError> {
        if array.height > JOURNAL_MAX_TREE_HEIGHT {
            return Err(conflict());
        }
        let total = array.len;
        let mut pending = vec![(
            JournalSequence {
                root: None,
                len: array.len,
                height: array.height,
            },
            Some(array.node),
        )];
        let mut items = Vec::new();
        while let Some((tree, inline)) = pending.pop() {
            let root_inline = inline.is_some();
            let node = match inline {
                Some(node) => node,
                None => {
                    let root = tree.root.as_ref().ok_or_else(conflict)?;
                    if tree.height == 1 {
                        self.object(root, JournalObjectKind::Sequence)?
                    } else {
                        let branch: JournalJsonBranch =
                            self.object(root, JournalObjectKind::JsonBranch)?;
                        JournalSequenceNode::JsonBranch { children: branch.0 }
                    }
                }
            };
            match node {
                JournalSequenceNode::JsonLeaf { items: leaves } => {
                    if ((!leaves.is_empty() || !root_inline || tree.len != 0)
                        && (leaves.is_empty() || leaves.len() > JOURNAL_FANOUT))
                        || tree.height != 0
                        || leaves.len() as u64 != tree.len
                    {
                        return Err(conflict());
                    }
                    items.extend(leaves);
                }
                JournalSequenceNode::JsonValueBranch { items: leaves } => {
                    if leaves.is_empty()
                        || leaves.len() > 2
                        || tree.height != 1
                        || leaves.len() as u64 != tree.len
                    {
                        return Err(conflict());
                    }
                    items.extend(leaves);
                }
                JournalSequenceNode::Leaf { .. } => return Err(conflict()),
                JournalSequenceNode::JsonBranch { children } => {
                    let children = draft_json_branch_children(&tree, children)?;
                    pending.extend(children.into_iter().rev().map(|child| (child, None)));
                }
                JournalSequenceNode::Branch { .. } => return Err(conflict()),
            }
            if items.len() as u64 > total {
                return Err(conflict());
            }
        }
        if items.len() as u64 != total {
            return Err(conflict());
        }
        Ok(items)
    }

    pub(in crate::journal) fn json_value(
        &self,
        id: &JournalObjectId,
        depth: u32,
    ) -> Result<serde_json::Value, RuntimeError> {
        if depth > JOURNAL_MAX_TREE_HEIGHT {
            return Err(conflict());
        }
        let object = self.encoded_object(id)?;
        if object.kind()? == JournalObjectKind::JsonInline {
            let inline: JournalJsonInline = object.decode(JournalObjectKind::JsonInline)?;
            validate_inline_json(&inline.0, depth, u64::MAX)?;
            return Ok(inline.0);
        }
        match object.decode::<JournalJsonValue>(JournalObjectKind::JsonValue)? {
            JournalJsonValue::Scalar { scalar } => {
                if scalar.is_array() || scalar.is_object() {
                    return Err(conflict());
                }
                Ok(scalar)
            }
            JournalJsonValue::Array { items } => self
                .json_array(items)?
                .into_iter()
                .map(|item| self.json_item(item, depth + 1))
                .collect::<Result<Vec<_>, _>>()
                .map(serde_json::Value::Array),
            JournalJsonValue::Object { entries } => {
                let mut values = serde_json::Map::new();
                for entry in self.sequence(&entries)? {
                    let entry: JournalJsonObjectEntry =
                        self.object(&entry, JournalObjectKind::JsonObjectEntry)?;
                    if values.contains_key(&entry.key) {
                        return Err(conflict());
                    }
                    values.insert(entry.key, self.json_item(entry.value, depth + 1)?);
                }
                Ok(serde_json::Value::Object(values))
            }
        }
    }

    pub(in crate::journal) fn json_item(
        &self,
        item: JournalJsonItem,
        depth: u32,
    ) -> Result<serde_json::Value, RuntimeError> {
        match item {
            JournalJsonItem::Inline { value } => {
                validate_inline_json(&value, depth, u64::MAX)?;
                Ok(value)
            }
            JournalJsonItem::Reference { object } => self.json_value(&object, depth),
        }
    }

    pub(in crate::journal) fn extension(
        &self,
        namespace: &str,
        id: &JournalObjectId,
    ) -> Result<crate::store::VersionedSessionState, RuntimeError> {
        if namespace != RUNTIME_PREVIOUS_CACHE_NAMESPACE {
            return Err(conflict());
        }
        let reference: ReferencedSessionState =
            self.object(id, JournalObjectKind::ExtensionReference)?;
        let value = self.json_value(&reference.value, 0)?;
        let state: crate::store::VersionedSessionState =
            serde_json::from_value(value.clone()).map_err(|_| conflict())?;
        if state.sensitivity != crate::store::SessionStateSensitivity::RedactionSafe
            || canonical_journal_json(&state)? != canonical_journal_json(&value)?
        {
            return Err(conflict());
        }
        Ok(state)
    }

    pub(in crate::journal) fn manifest(
        &self,
        id: &JournalObjectId,
    ) -> Result<crate::store::TurnManifest, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.draft.manifest");
        #[cfg(feature = "test-support")]
        crate::journal::profile::event("rt.work.manifest_decoded", 1, 0);
        let reference: ReferencedTurnManifest =
            self.object(id, JournalObjectKind::ManifestReference)?;
        let value = self.json_value(&reference.value, 0)?;
        let manifest: crate::store::TurnManifest =
            serde_json::from_value(value.clone()).map_err(|_| conflict())?;
        if canonical_journal_json(&manifest)? != canonical_journal_json(&value)? {
            return Err(conflict());
        }
        Ok(manifest)
    }
}
