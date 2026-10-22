use super::*;
use std::sync::Arc;

#[derive(Debug)]
struct Node {
    tree: JournalSequence,
    content: Content,
}
#[derive(Debug)]
enum Content {
    Leaf(Vec<JournalObjectId>),
    Branch(Vec<Arc<Node>>),
}

/// Immutable encoding paths for the frozen schema-4 canonical sequence.
/// Cloning a view shares its bounded-height tree; appends copy only tail paths.
#[derive(Debug, Clone, Default)]
pub struct JournalSequenceView {
    root: Option<Arc<Node>>,
}
impl JournalSequenceView {
    /// The canonical descriptor of this version.
    pub fn sequence(&self) -> JournalSequence {
        self.root
            .as_ref()
            .map_or_else(JournalSequence::default, |n| n.tree.clone())
    }
    /// Encodes a complete sequence. The returned objects use schema 4 unchanged.
    pub fn from_items(
        items: Vec<JournalObjectId>,
    ) -> Result<(Self, Vec<JournalObject>), crate::error::RuntimeError> {
        let mut objects = Vec::new();
        if items.is_empty() {
            return Ok((Self::default(), objects));
        }
        let mut level = Vec::new();
        for items in items.chunks(JOURNAL_FANOUT) {
            level.push(Self::leaf(items.to_vec(), &mut objects)?);
        }
        for _ in 0..JOURNAL_MAX_TREE_HEIGHT {
            let mut parents = Vec::new();
            for children in level.chunks(2) {
                parents.push(Self::branch(children.to_vec(), &mut objects)?);
            }
            level = parents;
        }
        if level.len() != 1 {
            return Err(conflict());
        }
        Ok((Self { root: level.pop() }, objects))
    }
    /// Appends objects using only new leaves and changed canonical paths.
    pub fn append(
        &self,
        items: &[JournalObjectId],
    ) -> Result<(Self, Vec<JournalObject>), crate::error::RuntimeError> {
        if self.root.is_none() {
            return Self::from_items(items.to_vec());
        }
        let mut objects = Vec::new();
        let mut root = self.root.clone().expect("nonempty");
        let mut remaining = items;
        while !remaining.is_empty() {
            let (next, used) = Self::append_node(
                Some(&root),
                JOURNAL_MAX_TREE_HEIGHT,
                remaining,
                &mut objects,
            )?;
            if used == 0 {
                return Err(conflict());
            }
            root = next;
            remaining = &remaining[used..];
        }
        // Earlier tail chunks can leave intermediate ancestors that are not
        // part of the final canonical root. Protected drafts must be exact.
        let generated: std::collections::BTreeSet<_> =
            objects.iter().map(|o| o.id.clone()).collect();
        let mut keep = std::collections::BTreeSet::new();
        let mut pending = vec![Arc::clone(&root)];
        while let Some(node) = pending.pop() {
            let id = node.tree.root.as_ref().ok_or_else(conflict)?;
            if !generated.contains(id) || !keep.insert(id.clone()) {
                continue;
            }
            if let Content::Branch(children) = &node.content {
                pending.extend(children.iter().cloned());
            }
        }
        objects.retain(|object| keep.contains(&object.id));
        Ok((Self { root: Some(root) }, objects))
    }
    fn leaf(
        items: Vec<JournalObjectId>,
        objects: &mut Vec<JournalObject>,
    ) -> Result<Arc<Node>, crate::error::RuntimeError> {
        let object = JournalObject::encode(
            JournalObjectKind::Sequence,
            &JournalSequenceNode::Leaf {
                items: items.clone(),
            },
        )?;
        #[cfg(feature = "test-support")]
        crate::journal::profile::event("rt.work.encoded_objects", 1, object.bytes.len() as u64);
        let tree = JournalSequence {
            root: Some(object.id.clone()),
            len: items.len() as u64,
            height: 0,
        };
        objects.push(object);
        Ok(Arc::new(Node {
            tree,
            content: Content::Leaf(items),
        }))
    }
    fn branch(
        children: Vec<Arc<Node>>,
        objects: &mut Vec<JournalObject>,
    ) -> Result<Arc<Node>, crate::error::RuntimeError> {
        let trees = children.iter().map(|c| c.tree.clone()).collect::<Vec<_>>();
        let len = trees
            .iter()
            .try_fold(0u64, |n, c| n.checked_add(c.len).ok_or_else(conflict))?;
        let height = trees[0].height.checked_add(1).ok_or_else(conflict)?;
        let object = JournalObject::encode(
            JournalObjectKind::Sequence,
            &JournalSequenceNode::Branch { children: trees },
        )?;
        #[cfg(feature = "test-support")]
        crate::journal::profile::event("rt.work.encoded_objects", 1, object.bytes.len() as u64);
        let tree = JournalSequence {
            root: Some(object.id.clone()),
            len,
            height,
        };
        objects.push(object);
        Ok(Arc::new(Node {
            tree,
            content: Content::Branch(children),
        }))
    }
    fn append_node(
        prior: Option<&Arc<Node>>,
        height: u32,
        items: &[JournalObjectId],
        objects: &mut Vec<JournalObject>,
    ) -> Result<(Arc<Node>, usize), crate::error::RuntimeError> {
        if height == 0 {
            let mut leaves = match prior.map(|n| &n.content) {
                Some(Content::Leaf(v)) => v.clone(),
                None => vec![],
                _ => return Err(conflict()),
            };
            let used = (JOURNAL_FANOUT - leaves.len()).min(items.len());
            leaves.extend_from_slice(&items[..used]);
            return Ok((Self::leaf(leaves, objects)?, used));
        }
        let mut children = match prior.map(|n| &n.content) {
            Some(Content::Branch(v)) => v.clone(),
            None => vec![],
            _ => return Err(conflict()),
        };
        let capacity = (JOURNAL_FANOUT as u64)
            .checked_shl(height - 1)
            .ok_or_else(conflict)?;
        let index = if children.last().is_some_and(|n| n.tree.len < capacity) {
            children.len() - 1
        } else {
            children.len()
        };
        if index >= 2 {
            return Err(conflict());
        }
        let (child, used) = Self::append_node(children.get(index), height - 1, items, objects)?;
        if index == children.len() {
            children.push(child);
        } else {
            children[index] = child;
        }
        Ok((Self::branch(children, objects)?, used))
    }
}
