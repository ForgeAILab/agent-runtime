use super::*;

pub(in crate::journal) fn stored_object(
    state: &State,
    id: &JournalObjectId,
) -> Result<JournalObject, RuntimeError> {
    Ok(JournalObject {
        id: id.clone(),
        bytes: state.objects.get(id).cloned().ok_or_else(conflict)?,
    })
}

#[derive(Debug, Clone, Copy)]
pub(in crate::journal) enum ExpectedLeaf {
    Kind(JournalObjectKind),
    Extension,
    Manifest,
    JsonValue,
    JsonObjectEntry,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::journal) enum ExpectedObject {
    Leaf(ExpectedLeaf),
    Sequence(ExpectedLeaf),
    Map(ExpectedLeaf),
}

pub(in crate::journal) fn snapshot_links(
    snapshot: &JournalSnapshotRoots,
) -> Vec<(JournalObjectId, ExpectedObject)> {
    let mut links = Vec::new();
    for (root, expected) in [
        (
            &snapshot.history.root,
            ExpectedObject::Sequence(ExpectedLeaf::Kind(JournalObjectKind::Message)),
        ),
        (
            &snapshot.usage.root,
            ExpectedObject::Sequence(ExpectedLeaf::Kind(JournalObjectKind::UsageRecord)),
        ),
        (
            &snapshot.extensions.root,
            ExpectedObject::Map(ExpectedLeaf::Extension),
        ),
        (
            &snapshot.manifests.root,
            ExpectedObject::Sequence(ExpectedLeaf::Manifest),
        ),
    ] {
        if let Some(root) = root {
            links.push((root.clone(), expected));
        }
    }
    links
}

pub(in crate::journal) fn sequence_links(
    sequence: &JournalSequence,
    item: ExpectedLeaf,
) -> Vec<(JournalObjectId, ExpectedObject)> {
    sequence
        .root
        .iter()
        .cloned()
        .map(|root| (root, ExpectedObject::Sequence(item)))
        .collect()
}

pub(in crate::journal) fn json_array_links(
    array: JournalJsonArray,
) -> Result<Vec<(JournalObjectId, ExpectedObject)>, RuntimeError> {
    if array.height > JOURNAL_MAX_TREE_HEIGHT {
        return Err(conflict());
    }
    match array.node {
        JournalSequenceNode::JsonLeaf { items }
        | JournalSequenceNode::JsonValueBranch { items } => {
            if items.len() as u64 != array.len {
                return Err(conflict());
            }
            let mut links = Vec::new();
            for item in items {
                match item {
                    JournalJsonItem::Inline { value } => {
                        validate_inline_json(&value, 0, u64::MAX)?;
                    }
                    JournalJsonItem::Reference { object } => {
                        links.push((object, ExpectedObject::Leaf(ExpectedLeaf::JsonValue)));
                    }
                }
            }
            Ok(links)
        }
        JournalSequenceNode::JsonBranch { children } => children
            .into_iter()
            .map(|id| {
                Ok((
                    id.to_object()?,
                    ExpectedObject::Sequence(ExpectedLeaf::JsonValue),
                ))
            })
            .collect(),
        JournalSequenceNode::Leaf { .. } | JournalSequenceNode::Branch { .. } => Err(conflict()),
    }
}

pub(in crate::journal) fn expected_matches(
    expected: ExpectedObject,
    kind: JournalObjectKind,
) -> bool {
    let leaf_matches = |leaf| match leaf {
        ExpectedLeaf::Kind(expected) => kind == expected,
        ExpectedLeaf::Extension => matches!(
            kind,
            JournalObjectKind::Extension | JournalObjectKind::ExtensionReference
        ),
        ExpectedLeaf::Manifest => matches!(
            kind,
            JournalObjectKind::Manifest | JournalObjectKind::ManifestReference
        ),
        ExpectedLeaf::JsonValue => {
            matches!(
                kind,
                JournalObjectKind::JsonValue | JournalObjectKind::JsonInline
            )
        }
        ExpectedLeaf::JsonObjectEntry => kind == JournalObjectKind::JsonObjectEntry,
    };
    match expected {
        ExpectedObject::Leaf(leaf) => leaf_matches(leaf),
        ExpectedObject::Sequence(ExpectedLeaf::JsonValue) => {
            matches!(
                kind,
                JournalObjectKind::Sequence | JournalObjectKind::JsonBranch
            )
        }
        ExpectedObject::Sequence(_) => kind == JournalObjectKind::Sequence,
        ExpectedObject::Map(_) => kind == JournalObjectKind::Map,
    }
}

pub(in crate::journal) fn object_links(
    object: &JournalObject,
    expected: ExpectedObject,
) -> Result<Vec<(JournalObjectId, ExpectedObject)>, RuntimeError> {
    let kind = object.kind()?;
    if !expected_matches(expected, kind) {
        return Err(conflict());
    }
    let links = match expected {
        ExpectedObject::Sequence(item) => {
            if kind == JournalObjectKind::JsonBranch {
                if !matches!(item, ExpectedLeaf::JsonValue) {
                    return Err(conflict());
                }
                let branch: JournalJsonBranch = object.decode(kind)?;
                return branch
                    .0
                    .into_iter()
                    .map(|id| {
                        Ok((
                            id.to_object()?,
                            ExpectedObject::Sequence(ExpectedLeaf::JsonValue),
                        ))
                    })
                    .collect();
            }
            match object.decode::<JournalSequenceNode>(JournalObjectKind::Sequence)? {
                JournalSequenceNode::Leaf { items } => items
                    .into_iter()
                    .map(|id| (id, ExpectedObject::Leaf(item)))
                    .collect(),
                JournalSequenceNode::JsonLeaf { items }
                | JournalSequenceNode::JsonValueBranch { items }
                    if matches!(item, ExpectedLeaf::JsonValue) =>
                {
                    let mut links = Vec::new();
                    for item in items {
                        match item {
                            JournalJsonItem::Inline { value } => {
                                validate_inline_json(&value, 0, u64::MAX)?;
                            }
                            JournalJsonItem::Reference { object } => {
                                links.push((object, ExpectedObject::Leaf(ExpectedLeaf::JsonValue)))
                            }
                        }
                    }
                    links
                }
                JournalSequenceNode::JsonLeaf { .. }
                | JournalSequenceNode::JsonValueBranch { .. } => return Err(conflict()),
                JournalSequenceNode::JsonBranch { children }
                    if matches!(item, ExpectedLeaf::JsonValue) =>
                {
                    children
                        .into_iter()
                        .map(|id| {
                            Ok((
                                id.to_object()?,
                                ExpectedObject::Sequence(ExpectedLeaf::JsonValue),
                            ))
                        })
                        .collect::<Result<Vec<_>, RuntimeError>>()?
                }
                JournalSequenceNode::JsonBranch { .. } => return Err(conflict()),
                JournalSequenceNode::Branch { children } => children
                    .iter()
                    .flat_map(|child| sequence_links(child, item))
                    .collect(),
            }
        }
        ExpectedObject::Map(value) => {
            match object.decode::<JournalMapNode>(JournalObjectKind::Map)? {
                JournalMapNode::Leaf { entries } => entries
                    .into_iter()
                    .map(|entry| (entry.value, ExpectedObject::Leaf(value)))
                    .collect(),
                JournalMapNode::Branch { children } => children
                    .into_iter()
                    .filter_map(|child| {
                        child
                            .tree
                            .root
                            .map(|root| (root, ExpectedObject::Map(value)))
                    })
                    .collect(),
            }
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Request)) => {
            let request: ReferencedProviderRequest = object.decode(JournalObjectKind::Request)?;
            let mut links = sequence_links(
                &request.messages,
                ExpectedLeaf::Kind(JournalObjectKind::Message),
            );
            links.extend(sequence_links(
                &request.tools,
                ExpectedLeaf::Kind(JournalObjectKind::ToolSchema),
            ));
            links.push((
                request.settings,
                ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::RequestSettings)),
            ));
            links
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::TurnState)) => {
            let state: ReferencedTurnState = object.decode(JournalObjectKind::TurnState)?;
            let mut links = Vec::new();
            for value in state.fields.into_values() {
                match value {
                    JournalStateValue::Object { object } => {
                        links.push((
                            object,
                            ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::StateValue)),
                        ));
                    }
                    JournalStateValue::Sequence { sequence } => {
                        links.extend(sequence_links(
                            &sequence,
                            ExpectedLeaf::Kind(JournalObjectKind::StateValue),
                        ));
                    }
                    JournalStateValue::Request { object } => {
                        links.push((
                            object,
                            ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Request)),
                        ));
                    }
                }
            }
            links
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Checkpoint)) => {
            let checkpoint: ReferencedTurnCheckpoint =
                object.decode(JournalObjectKind::Checkpoint)?;
            let mut links = snapshot_links(&checkpoint.snapshot);
            links.push((
                checkpoint.state,
                ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::TurnState)),
            ));
            if let Some(input) = checkpoint.internal_input {
                links.push((
                    input,
                    ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::InternalInput)),
                ));
            }
            links
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Head)) => {
            let head: JournalHead = object.decode(JournalObjectKind::Head)?;
            head.validate()?;
            let mut links = snapshot_links(&head.snapshot);
            links.push((
                head.batch,
                ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Batch)),
            ));
            if let Some(checkpoint) = head.checkpoint {
                links.push((
                    checkpoint,
                    ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Checkpoint)),
                ));
            }
            links
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Batch)) => {
            let batch: JournalBatch = object.decode(JournalObjectKind::Batch)?;
            let mut links = snapshot_links(&batch.snapshot);
            if let Some(checkpoint) = batch.checkpoint {
                links.push((
                    checkpoint,
                    ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Checkpoint)),
                ));
            }
            links
        }
        ExpectedObject::Leaf(ExpectedLeaf::Extension) => match kind {
            JournalObjectKind::Extension => {
                validate_leaf(object, kind)?;
                Vec::new()
            }
            JournalObjectKind::ExtensionReference => {
                let reference: ReferencedSessionState = object.decode(kind)?;
                vec![(
                    reference.value,
                    ExpectedObject::Leaf(ExpectedLeaf::JsonValue),
                )]
            }
            _ => return Err(conflict()),
        },
        ExpectedObject::Leaf(ExpectedLeaf::Manifest) => match kind {
            JournalObjectKind::Manifest => {
                validate_leaf(object, kind)?;
                Vec::new()
            }
            JournalObjectKind::ManifestReference => {
                let reference: ReferencedTurnManifest = object.decode(kind)?;
                vec![(
                    reference.value,
                    ExpectedObject::Leaf(ExpectedLeaf::JsonValue),
                )]
            }
            _ => return Err(conflict()),
        },
        ExpectedObject::Leaf(ExpectedLeaf::JsonValue) => {
            if kind == JournalObjectKind::JsonInline {
                let inline: JournalJsonInline = object.decode(kind)?;
                validate_inline_json(&inline.0, 0, u64::MAX)?;
                return Ok(Vec::new());
            }
            match object.decode::<JournalJsonValue>(kind)? {
                JournalJsonValue::Scalar { scalar } => {
                    if scalar.is_array() || scalar.is_object() {
                        return Err(conflict());
                    }
                    Vec::new()
                }
                JournalJsonValue::Array { items } => json_array_links(items)?,
                JournalJsonValue::Object { entries } => {
                    sequence_links(&entries, ExpectedLeaf::JsonObjectEntry)
                }
            }
        }
        ExpectedObject::Leaf(ExpectedLeaf::JsonObjectEntry) => {
            let entry: JournalJsonObjectEntry = object.decode(kind)?;
            match entry.value {
                JournalJsonItem::Inline { value } => {
                    validate_inline_json(&value, 0, u64::MAX)?;
                    Vec::new()
                }
                JournalJsonItem::Reference { object } => {
                    vec![(object, ExpectedObject::Leaf(ExpectedLeaf::JsonValue))]
                }
            }
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::Message))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::UsageRecord))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::Extension))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::Manifest))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::RequestSettings))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::ToolSchema))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::StateValue))
        | ExpectedObject::Leaf(ExpectedLeaf::Kind(kind @ JournalObjectKind::InternalInput)) => {
            validate_leaf(object, kind)?;
            Vec::new()
        }
        ExpectedObject::Leaf(ExpectedLeaf::Kind(
            JournalObjectKind::Sequence
            | JournalObjectKind::JsonBranch
            | JournalObjectKind::JsonInline
            | JournalObjectKind::Map
            | JournalObjectKind::ExtensionReference
            | JournalObjectKind::ManifestReference
            | JournalObjectKind::JsonValue
            | JournalObjectKind::JsonObjectEntry,
        )) => {
            return Err(conflict());
        }
    };
    Ok(links)
}

pub(in crate::journal) fn draft_reachable_closure(
    stored: &ObjectStore,
    supplied: &BTreeMap<JournalObjectId, Vec<u8>>,
    reachable: &ReachableSet,
    roots: Vec<(JournalObjectId, ExpectedObject)>,
) -> Result<BTreeSet<JournalObjectId>, RuntimeError> {
    let mut closure = BTreeSet::new();
    let mut pending = roots;
    while let Some((id, expected)) = pending.pop() {
        if !closure.insert(id.clone()) {
            continue;
        }
        let Some(bytes) = supplied.get(&id).cloned() else {
            // A pinned object's closure is pinned and was validated when it
            // was published; it cannot lead to a draft, and the caller has
            // already decoded these references with their expected kinds.
            if reachable.contains(&id) && stored.get(&id).is_some() {
                continue;
            }
            return Err(conflict());
        };
        let object = JournalObject { id, bytes };
        if !expected_matches(expected, object.kind()?) {
            return Err(conflict());
        }
        pending.extend(object_links(&object, expected)?);
    }
    Ok(closure)
}

pub(in crate::journal) fn reachable_closure(
    objects: &ObjectStore,
    root: &JournalObjectId,
    admitted: Option<&BTreeSet<JournalObjectId>>,
    trusted: Option<&ReachableSet>,
) -> Result<BTreeSet<JournalObjectId>, RuntimeError> {
    let mut reachable = BTreeSet::new();
    let mut pending = vec![(
        root.clone(),
        ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Head)),
    )];
    while let Some((id, expected)) = pending.pop() {
        if admitted.is_some_and(|admitted| !admitted.contains(&id))
            && trusted.is_none_or(|trusted| !trusted.contains(&id))
        {
            return Err(conflict());
        }
        if !reachable.insert(id.clone()) {
            continue;
        }
        if trusted.is_some_and(|trusted| trusted.contains(&id)) {
            continue;
        }
        let object = JournalObject {
            id: id.clone(),
            bytes: objects.get(&id).cloned().ok_or_else(conflict)?,
        };
        if !expected_matches(expected, object.kind()?) {
            return Err(conflict());
        }
        pending.extend(object_links(&object, expected)?);
    }
    Ok(reachable)
}

pub(in crate::journal) fn validate_leaf(
    object: &JournalObject,
    kind: JournalObjectKind,
) -> Result<(), RuntimeError> {
    match kind {
        JournalObjectKind::Message => {
            object.decode::<crate::content::Message>(kind)?;
        }
        JournalObjectKind::UsageRecord => {
            object.decode::<crate::usage::UsageRecord>(kind)?;
        }
        JournalObjectKind::Extension => {
            object.decode::<crate::store::VersionedSessionState>(kind)?;
        }
        JournalObjectKind::Manifest => {
            object.decode::<crate::store::TurnManifest>(kind)?;
        }
        JournalObjectKind::RequestSettings => {
            let request = object.decode::<crate::provider::ProviderRequest>(kind)?;
            if !request.messages.is_empty() || !request.tools.is_empty() {
                return Err(conflict());
            }
        }
        JournalObjectKind::ToolSchema => {
            object.decode::<crate::provider::ToolSchema>(kind)?;
        }
        JournalObjectKind::StateValue => {
            object.decode::<serde_json::Value>(kind)?;
        }
        JournalObjectKind::InternalInput => {
            object.decode::<crate::content::InternalTurnInput>(kind)?;
        }
        _ => return Err(conflict()),
    }
    Ok(())
}
