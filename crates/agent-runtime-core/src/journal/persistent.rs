//! Path-copying AVL indexes. Versions own trees, never chains of prior versions.
use std::sync::Arc;

#[derive(Debug)]
struct Node<K, V> {
    key: K,
    value: V,
    left: Option<Arc<Self>>,
    right: Option<Arc<Self>>,
    height: u8,
    len: usize,
}
#[derive(Debug)]
pub(crate) struct Map<K, V>(Option<Arc<Node<K, V>>>);
impl<K, V> Clone for Map<K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<K, V> Default for Map<K, V> {
    fn default() -> Self {
        Self(None)
    }
}
fn height<K, V>(node: &Option<Arc<Node<K, V>>>) -> u8 {
    node.as_ref().map_or(0, |node| node.height)
}
fn len<K, V>(node: &Option<Arc<Node<K, V>>>) -> usize {
    node.as_ref().map_or(0, |node| node.len)
}
impl<K: Ord + Clone, V: Clone> Map<K, V> {
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        }
    }
    fn node(
        key: K,
        value: V,
        left: Option<Arc<Node<K, V>>>,
        right: Option<Arc<Node<K, V>>>,
    ) -> Arc<Node<K, V>> {
        #[cfg(feature = "test-support")]
        crate::journal::profile::event("rt.work.persistent_nodes_created", 1, 0);
        Arc::new(Node {
            height: 1 + height(&left).max(height(&right)),
            len: 1 + len(&left) + len(&right),
            key,
            value,
            left,
            right,
        })
    }
    fn balanced(
        key: K,
        value: V,
        mut left: Option<Arc<Node<K, V>>>,
        mut right: Option<Arc<Node<K, V>>>,
    ) -> Arc<Node<K, V>> {
        if height(&left) > height(&right) + 1 {
            let child = left.as_ref().expect("left heavy");
            if height(&child.left) < height(&child.right) {
                let pivot = child.right.as_ref().expect("right heavy child");
                left = Some(Self::node(
                    pivot.key.clone(),
                    pivot.value.clone(),
                    Some(Self::node(
                        child.key.clone(),
                        child.value.clone(),
                        child.left.clone(),
                        pivot.left.clone(),
                    )),
                    pivot.right.clone(),
                ));
            }
            let child = left.expect("left heavy");
            return Self::node(
                child.key.clone(),
                child.value.clone(),
                child.left.clone(),
                Some(Self::node(key, value, child.right.clone(), right)),
            );
        }
        if height(&right) > height(&left) + 1 {
            let child = right.as_ref().expect("right heavy");
            if height(&child.right) < height(&child.left) {
                let pivot = child.left.as_ref().expect("left heavy child");
                right = Some(Self::node(
                    pivot.key.clone(),
                    pivot.value.clone(),
                    pivot.left.clone(),
                    Some(Self::node(
                        child.key.clone(),
                        child.value.clone(),
                        pivot.right.clone(),
                        child.right.clone(),
                    )),
                ));
            }
            let child = right.expect("right heavy");
            return Self::node(
                child.key.clone(),
                child.value.clone(),
                Some(Self::node(key, value, left, child.left.clone())),
                child.right.clone(),
            );
        }
        Self::node(key, value, left, right)
    }
    pub(crate) fn shallow_bytes(&self) -> usize {
        self.len()
            .saturating_mul(std::mem::size_of::<Node<K, V>>() + 2 * std::mem::size_of::<usize>())
    }
    pub(crate) fn len(&self) -> usize {
        len(&self.0)
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_none()
    }
    pub(crate) fn get(&self, key: &K) -> Option<&V> {
        let mut node = self.0.as_deref();
        while let Some(current) = node {
            match key.cmp(&current.key) {
                std::cmp::Ordering::Less => node = current.left.as_deref(),
                std::cmp::Ordering::Equal => return Some(&current.value),
                std::cmp::Ordering::Greater => node = current.right.as_deref(),
            }
        }
        None
    }
    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }
    /// Sorted position of an existing key, without walking its prefix.
    pub(crate) fn index_of(&self, key: &K) -> Option<usize> {
        let mut node = self.0.as_deref();
        let mut before = 0;
        while let Some(current) = node {
            match key.cmp(&current.key) {
                std::cmp::Ordering::Less => node = current.left.as_deref(),
                std::cmp::Ordering::Equal => return Some(before + len(&current.left)),
                std::cmp::Ordering::Greater => {
                    before += 1 + len(&current.left);
                    node = current.right.as_deref();
                }
            }
        }
        None
    }
    pub(crate) fn insert(&mut self, key: K, value: V) {
        fn put<K: Ord + Clone, V: Clone>(
            node: &Option<Arc<Node<K, V>>>,
            key: K,
            value: V,
        ) -> Arc<Node<K, V>> {
            let Some(node) = node else {
                return Map::node(key, value, None, None);
            };
            match key.cmp(&node.key) {
                std::cmp::Ordering::Less => Map::balanced(
                    node.key.clone(),
                    node.value.clone(),
                    Some(put(&node.left, key, value)),
                    node.right.clone(),
                ),
                std::cmp::Ordering::Equal => {
                    Map::node(key, value, node.left.clone(), node.right.clone())
                }
                std::cmp::Ordering::Greater => Map::balanced(
                    node.key.clone(),
                    node.value.clone(),
                    node.left.clone(),
                    Some(put(&node.right, key, value)),
                ),
            }
        }
        self.0 = Some(put(&self.0, key, value));
    }
    pub(crate) fn remove(&mut self, key: &K) {
        fn erase<K: Ord + Clone, V: Clone>(
            node: &Option<Arc<Node<K, V>>>,
            key: &K,
        ) -> Option<Arc<Node<K, V>>> {
            let node = node.as_ref()?;
            Some(match key.cmp(&node.key) {
                std::cmp::Ordering::Less => Map::balanced(
                    node.key.clone(),
                    node.value.clone(),
                    erase(&node.left, key),
                    node.right.clone(),
                ),
                std::cmp::Ordering::Greater => Map::balanced(
                    node.key.clone(),
                    node.value.clone(),
                    node.left.clone(),
                    erase(&node.right, key),
                ),
                std::cmp::Ordering::Equal => {
                    if node.left.is_none() {
                        return node.right.clone();
                    }
                    if node.right.is_none() {
                        return node.left.clone();
                    }
                    let mut successor = node.right.as_deref().expect("right child");
                    while let Some(left) = successor.left.as_deref() {
                        successor = left;
                    }
                    Map::balanced(
                        successor.key.clone(),
                        successor.value.clone(),
                        node.left.clone(),
                        erase(&node.right, &successor.key),
                    )
                }
            })
        }
        self.0 = erase(&self.0, key);
    }
    pub(crate) fn iter(&self) -> Iter<'_, K, V> {
        let mut iter = Iter { stack: Vec::new() };
        iter.descend(self.0.as_deref());
        iter
    }
    pub(crate) fn iter_from(&self, first: &K) -> Iter<'_, K, V> {
        let mut iter = Iter { stack: Vec::new() };
        let mut node = self.0.as_deref();
        while let Some(current) = node {
            if current.key < *first {
                node = current.right.as_deref();
            } else {
                iter.stack.push(current);
                node = current.left.as_deref();
            }
        }
        iter
    }

    pub(crate) fn shared_before(
        &self,
        other: &Self,
        end: &K,
        same: impl Fn(&V, &V) -> bool,
    ) -> bool {
        fn visit<K: Ord, V>(
            node: &Option<Arc<Node<K, V>>>,
            other: &Option<Arc<Node<K, V>>>,
            end: &K,
            same: &impl Fn(&V, &V) -> bool,
        ) -> bool {
            let Some(node) = node else {
                return true;
            };
            let mut matching = other.as_ref();
            while let Some(candidate) = matching {
                match node.key.cmp(&candidate.key) {
                    std::cmp::Ordering::Less => matching = candidate.left.as_ref(),
                    std::cmp::Ordering::Greater => matching = candidate.right.as_ref(),
                    std::cmp::Ordering::Equal => break,
                }
            }
            if matching.is_some_and(|candidate| Arc::ptr_eq(node, candidate)) {
                return true;
            }
            if !visit(&node.left, other, end, same) {
                return false;
            }
            if node.key >= *end {
                return true;
            }
            matching.is_some_and(|candidate| same(&node.value, &candidate.value))
                && visit(&node.right, other, end, same)
        }
        visit(&self.0, &other.0, end, &same)
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v)| v)
    }
    pub(crate) fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(k, _)| k)
    }
}
pub(crate) struct Iter<'a, K, V> {
    stack: Vec<&'a Node<K, V>>,
}
impl<'a, K, V> Iter<'a, K, V> {
    fn descend(&mut self, mut node: Option<&'a Node<K, V>>) {
        while let Some(current) = node {
            self.stack.push(current);
            node = current.left.as_deref();
        }
    }
}
impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;
        self.descend(node.right.as_deref());
        Some((&node.key, &node.value))
    }
}
impl<K: Ord + Clone, V: Clone> FromIterator<(K, V)> for Map<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut map = Self::default();
        for (k, v) in iter {
            map.insert(k, v);
        }
        map
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn versions_match_ordered_map_under_random_edits() {
        let mut map = Map::default();
        let mut oracle = std::collections::BTreeMap::new();
        let mut seed = 123456789u64;
        let mut versions = Vec::new();
        for i in 0..20000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let key = (seed % 1000) as usize;
            if seed & 4 == 0 {
                map.remove(&key);
                oracle.remove(&key);
            } else {
                map.insert(key, i);
                oracle.insert(key, i);
            }
            if i % 1000 == 0 {
                versions.push((map.clone(), oracle.clone()));
            }
            assert_eq!(map.len(), oracle.len());
            assert!(height(&map.0) < 20);
        }
        versions.push((map, oracle));
        for (map, oracle) in versions {
            for (index, key) in oracle.keys().enumerate() {
                assert_eq!(map.index_of(key), Some(index));
            }
            assert_eq!(map.index_of(&1000), None);
            assert_eq!(
                map.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
                oracle.into_iter().collect::<Vec<_>>()
            );
        }
    }
}

#[cfg(test)]
mod prefix_tests {
    use super::*;

    #[test]
    fn shared_prefix_comparison_visits_only_changed_paths() {
        let mut base = Map::default();
        for i in 0..4000 {
            base.insert(i, Arc::new(i));
        }
        let mut next = base.clone();
        next.insert(4000, Arc::new(4000));
        let visits = std::cell::Cell::new(0);
        assert!(base.shared_before(&next, &4000, |a, b| {
            visits.set(visits.get() + 1);
            Arc::ptr_eq(a, b)
        }));
        assert!(
            visits.get() <= 32,
            "unchanged prefix was walked: {}",
            visits.get()
        );
        for i in [0, 1, 31, 32, 3999] {
            let mut rewritten = next.clone();
            rewritten.insert(i, Arc::new(i + 1));
            assert!(!base.shared_before(&rewritten, &4000, |a, b| a == b));
        }
        next.remove(&31);
        assert!(!base.shared_before(&next, &4000, |a, b| a == b));
        assert_eq!(
            base.iter_from(&3997).map(|(k, _)| *k).collect::<Vec<_>>(),
            vec![3997, 3998, 3999]
        );
    }
}
