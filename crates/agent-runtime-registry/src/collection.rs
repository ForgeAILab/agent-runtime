//! The generic name-keyed collection mechanism.
//!
//! [`Registry<T>`] is a name-keyed collection with three guarantees every
//! flat capability catalog wants: **fail-closed** registration (a duplicate
//! name is rejected, first wins), **insertion-order preservation** (so
//! advertisement and iteration are deterministic), and **sealing** into an
//! immutable, cheaply-shareable [`Sealed<T>`]. It works for any entry that has
//! a stable name via [`Named`].
//!
//! This mechanism predates the kernel's namespaced [`crate::RegistryId`] and
//! is name-keyed rather than domain-and-name-keyed, so it stays useful for
//! flat, single-domain catalogs (a tool set, a skill set) that don't need
//! cross-domain identity or layered sealing. Its error type,
//! [`NameConflict`], is deliberately distinct from [`crate::RegistryError`]:
//! the two mechanisms fail for different reasons and neither should shadow
//! the other.

use std::collections::HashSet;
use std::sync::Arc;

/// A value with a stable, unique name — the only requirement to live in a
/// [`Registry`].
pub trait Named {
    /// The entry's stable name.
    fn name(&self) -> &str;
}

/// Why a registration into a [`Registry`] was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NameConflict {
    /// An entry with this name is already registered; the first one wins.
    Duplicate {
        /// The conflicting name.
        name: String,
    },
}

impl std::fmt::Display for NameConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NameConflict::Duplicate { name } => write!(f, "duplicate entry name `{name}`"),
        }
    }
}

impl std::error::Error for NameConflict {}

/// A mutable, name-keyed builder for a set of `T`.
#[derive(Debug)]
pub struct Registry<T: Named> {
    seen: HashSet<String>,
    entries: Vec<T>,
}

impl<T: Named> Default for Registry<T> {
    fn default() -> Self {
        Self {
            seen: HashSet::new(),
            entries: Vec::new(),
        }
    }
}

impl<T: Named> Registry<T> {
    /// A new, empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `entry`. Fails with [`NameConflict::Duplicate`] if its name is
    /// already taken; the first registration wins.
    pub fn register(&mut self, entry: T) -> Result<(), NameConflict> {
        let name = entry.name().to_owned();
        if !self.seen.insert(name.clone()) {
            return Err(NameConflict::Duplicate { name });
        }
        self.entries.push(entry);
        Ok(())
    }

    /// Registers many entries, failing on the first conflict.
    pub fn register_all(
        &mut self,
        entries: impl IntoIterator<Item = T>,
    ) -> Result<(), NameConflict> {
        for entry in entries {
            self.register(entry)?;
        }
        Ok(())
    }

    /// Whether an entry with `name` is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.seen.contains(name)
    }

    /// The number of registered entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Freezes the registry into an immutable, shareable set.
    pub fn seal(self) -> Sealed<T> {
        Sealed {
            entries: Arc::from(self.entries.into_boxed_slice()),
        }
    }
}

/// An immutable, cheaply-cloneable set of `T` with deterministic order.
#[derive(Debug)]
pub struct Sealed<T> {
    entries: Arc<[T]>,
}

// Manual `Clone` so cloning is a single `Arc` bump and never requires `T: Clone`.
impl<T> Clone for Sealed<T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}

impl<T> Sealed<T> {
    /// An empty sealed set.
    pub fn empty() -> Self {
        Self {
            entries: Arc::from(Vec::new().into_boxed_slice()),
        }
    }

    /// The number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates entries in registration order.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.entries.iter()
    }
}

impl<T: Named> Sealed<T> {
    /// Looks up an entry by name (order-preserving linear scan).
    pub fn get(&self, name: &str) -> Option<&T> {
        self.entries.iter().find(|e| e.name() == name)
    }

    /// Whether an entry with `name` is present.
    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Entry names in registration order.
    pub fn names(&self) -> Vec<&str> {
        self.entries.iter().map(Named::name).collect()
    }
}

impl<'a, T> IntoIterator for &'a Sealed<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct Item(&'static str);
    impl Named for Item {
        fn name(&self) -> &str {
            self.0
        }
    }

    #[test]
    fn duplicate_names_are_rejected_first_wins() {
        let mut reg = Registry::new();
        reg.register(Item("a")).unwrap();
        let err = reg.register(Item("a")).unwrap_err();
        assert_eq!(
            err,
            NameConflict::Duplicate {
                name: "a".to_string()
            }
        );
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn registration_order_is_preserved() {
        let mut reg = Registry::new();
        reg.register_all([Item("a"), Item("b"), Item("c")]).unwrap();
        let sealed = reg.seal();
        assert_eq!(sealed.names(), ["a", "b", "c"]);
        assert!(sealed.contains("b"));
        assert_eq!(sealed.get("c"), Some(&Item("c")));
        assert!(sealed.get("z").is_none());
    }

    #[test]
    fn sealed_clone_shares_storage() {
        let mut reg = Registry::new();
        reg.register(Item("a")).unwrap();
        let sealed = reg.seal();
        let cloned = sealed.clone();
        assert_eq!(sealed.len(), cloned.len());
        assert_eq!((&cloned).into_iter().count(), 1);
    }
}
