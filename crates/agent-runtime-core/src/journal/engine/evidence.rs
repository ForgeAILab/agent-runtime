use super::*;
use crate::journal::persistent::Map;

type Edge = (JournalObjectId, ExpectedObject);
#[derive(Debug, Clone)]
struct Activation {
    incoming: u64,
    children: Arc<Vec<Edge>>,
}

/// Exact typed reachability of one root. Counts refer to active parents, so
/// duplicate edges and several expected kinds for a body remain independent.
#[derive(Debug, Clone, Default)]
pub(in crate::journal) struct ClosureEvidence {
    root: Option<Edge>,
    active: Map<Edge, Activation>,
    members: Map<JournalObjectId, u64>,
    pub(super) entered: BTreeSet<JournalObjectId>,
}
impl ClosureEvidence {
    pub(super) fn is_head_root(&self, root: &JournalObjectId) -> bool {
        self.root.as_ref().is_some_and(|(id, expected)| {
            id == root
                && *expected == ExpectedObject::Leaf(ExpectedLeaf::Kind(JournalObjectKind::Head))
        })
    }
    pub(in crate::journal) fn len(&self) -> usize {
        self.members.len()
    }
    pub(in crate::journal) fn contains(&self, id: &JournalObjectId) -> bool {
        self.members.contains_key(id)
    }
    pub(super) fn member_index(&self, id: &JournalObjectId) -> Option<usize> {
        self.members.index_of(id)
    }
    pub(in crate::journal) fn iter(&self) -> impl Iterator<Item = &JournalObjectId> {
        self.members.keys()
    }
    /// The member set alone, sharing its persistent structure, without the
    /// activation map that only incremental updates need.
    pub(super) fn membership_only(&self) -> Self {
        Self {
            root: None,
            active: Map::default(),
            members: self.members.clone(),
            entered: BTreeSet::new(),
        }
    }
    #[cfg(test)]
    pub(super) fn is_membership_only(&self) -> bool {
        self.root.is_none() && self.active.is_empty()
    }
    #[cfg(test)]
    pub(super) fn replace_member_for_test(
        &mut self,
        removed: &JournalObjectId,
        added: JournalObjectId,
    ) {
        assert!(self.members.contains_key(removed));
        self.members.remove(removed);
        self.members.insert(added, 1);
    }
    /// Whether both proofs share one immutable member set.
    pub(super) fn shares_members(&self, other: &Self) -> bool {
        self.members.ptr_eq(&other.members)
    }
    pub(super) fn without_changes(&self) -> Self {
        Self {
            root: self.root.clone(),
            active: self.active.clone(),
            members: self.members.clone(),
            entered: BTreeSet::new(),
        }
    }
    pub(super) fn charge(&self) -> usize {
        self.members
            .shallow_bytes()
            .saturating_add(self.active.shallow_bytes())
            .saturating_add((self.members.len() + self.active.len()).saturating_mul(64))
    }
    pub(super) fn update(
        base: Option<&Self>,
        objects: &mut ObjectStore,
        root: JournalObjectId,
        expected: ExpectedObject,
    ) -> Result<Self, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::journal::profile::enter_sync("rt.links.memoized_closure");
        let mut next = base.map_or_else(Self::default, Self::without_changes);
        let old = next.root.replace((root.clone(), expected));
        let mut pending = vec![(root, expected)];
        while let Some(edge) = pending.pop() {
            #[cfg(feature = "test-support")]
            crate::journal::profile::event("rt.work.closure_edge_checked", 1, 0);
            if let Some(active) = next.active.get(&edge) {
                let mut active = active.clone();
                active.incoming = active.incoming.checked_add(1).ok_or_else(conflict)?;
                next.active.insert(edge, active);
                continue;
            }
            let children = cached_links(objects, &edge.0, edge.1)?;
            pending.extend(children.iter().cloned());
            let count = next.members.get(&edge.0).copied().unwrap_or(0);
            if count == 0 {
                next.entered.insert(edge.0.clone());
            }
            next.members
                .insert(edge.0.clone(), count.checked_add(1).ok_or_else(conflict)?);
            next.active.insert(
                edge,
                Activation {
                    incoming: 1,
                    children,
                },
            );
        }
        if let Some(old) = old {
            pending.push(old);
        }
        while let Some(edge) = pending.pop() {
            #[cfg(feature = "test-support")]
            crate::journal::profile::event("rt.work.closure_edge_removed", 1, 0);
            let active = next.active.get(&edge).ok_or_else(conflict)?.clone();
            if active.incoming > 1 {
                next.active.insert(
                    edge,
                    Activation {
                        incoming: active.incoming - 1,
                        ..active
                    },
                );
                continue;
            }
            next.active.remove(&edge);
            let count = *next.members.get(&edge.0).ok_or_else(conflict)?;
            if count == 1 {
                next.members.remove(&edge.0);
                next.entered.remove(&edge.0);
            } else {
                next.members.insert(edge.0, count - 1);
            }
            pending.extend(active.children.iter().cloned());
        }
        Ok(next)
    }
}
impl PartialEq for ClosureEvidence {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl Eq for ClosureEvidence {}

pub(super) fn cached_links(
    objects: &mut ObjectStore,
    id: &JournalObjectId,
    expected: ExpectedObject,
) -> Result<Arc<Vec<Edge>>, RuntimeError> {
    let bytes = objects.get(id).ok_or_else(conflict)?;
    if let Some(entry) = objects
        .links
        .get(id)
        .and_then(|v| v.iter().find(|e| e.expected == expected))
    {
        return Ok(entry.children.clone());
    }
    let object = JournalObject {
        id: id.clone(),
        bytes: bytes.clone(),
    };
    let children = Arc::new(object_links(&object, expected)?);
    let mut variants = objects.links.get(id).cloned().unwrap_or_default();
    variants.push(CachedLinks {
        expected,
        children: children.clone(),
    });
    objects.cache_links(id.clone(), variants)?;
    Ok(children)
}
