//! Structured failures for sealing a registry and resolving aliases.
//!
//! Every variant refers only to information already present in the offending
//! declarations — an id, a source, an alias path — so a caller can present it
//! without re-deriving context, and two equivalent seal attempts always fail
//! identically. Sealing is fail-closed: any [`RegistryError`] means the
//! builder produced no snapshot at all, not a partially resolved one.

use std::fmt;

use crate::id::{RegistryId, RegistrySource};

/// Why sealing a [`crate::RegistryBuilder`] failed.
///
/// Non-exhaustive: new failure modes can be added without a breaking change.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[non_exhaustive]
pub enum RegistryError {
    /// Two declarations from the same source layer claim the same id.
    DuplicateInLayer {
        /// The conflicting id.
        id: RegistryId,
        /// The layer that declared it twice.
        source: RegistrySource,
    },
    /// A higher-precedence declaration shadows a lower one with no explicit
    /// override relationship.
    UnauthorizedOverride {
        /// The conflicting id.
        id: RegistryId,
        /// The layer that would be shadowed.
        existing: RegistrySource,
        /// The layer attempting to replace it.
        replacement: RegistrySource,
    },
    /// An `overrides` declaration names a layer with no entry at that id.
    OverrideTargetMissing {
        /// The id whose override target is missing.
        id: RegistryId,
        /// The layer named as the override target.
        declared: RegistrySource,
    },
    /// Following an alias chain revisited an id already on the path.
    AliasCycle {
        /// The alias path, in traversal order, ending with the repeated id.
        path: Vec<RegistryId>,
    },
    /// An alias's declared id already names a real entry.
    AliasConflictsWithEntry {
        /// The conflicting alias id.
        alias: RegistryId,
    },
    /// An alias resolves to an id with neither an entry nor a further alias.
    UnknownAliasTarget {
        /// The alias that fails to resolve.
        alias: RegistryId,
        /// The dead-end id it points to.
        target: RegistryId,
    },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegistryError::DuplicateInLayer { id, source } => {
                write!(
                    f,
                    "duplicate declaration for `{id}` within the `{source}` layer"
                )
            }
            RegistryError::UnauthorizedOverride {
                id,
                existing,
                replacement,
            } => {
                write!(
                    f,
                    "`{replacement}` shadows the `{existing}` entry for `{id}` with no explicit override"
                )
            }
            RegistryError::OverrideTargetMissing { id, declared } => {
                write!(
                    f,
                    "`{id}` declares an override of the `{declared}` layer, but no such entry exists"
                )
            }
            RegistryError::AliasCycle { path } => {
                let rendered = path
                    .iter()
                    .map(RegistryId::to_string)
                    .collect::<Vec<_>>()
                    .join(" -> ");
                write!(f, "alias cycle: {rendered}")
            }
            RegistryError::AliasConflictsWithEntry { alias } => {
                write!(
                    f,
                    "alias `{alias}` collides with a real entry of the same id"
                )
            }
            RegistryError::UnknownAliasTarget { alias, target } => {
                write!(
                    f,
                    "alias `{alias}` targets `{target}`, which resolves to neither an entry nor another alias"
                )
            }
        }
    }
}

impl std::error::Error for RegistryError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_in_layer_names_the_id_and_source() {
        let err = RegistryError::DuplicateInLayer {
            id: RegistryId::tool("browser"),
            source: RegistrySource::Plugin,
        };
        assert_eq!(
            err.to_string(),
            "duplicate declaration for `tool:browser` within the `plugin` layer"
        );
    }

    #[test]
    fn unauthorized_override_names_both_the_shadowed_and_shadowing_layer() {
        let err = RegistryError::UnauthorizedOverride {
            id: RegistryId::tool("browser"),
            existing: RegistrySource::BuiltIn,
            replacement: RegistrySource::Plugin,
        };
        assert_eq!(
            err.to_string(),
            "`plugin` shadows the `built_in` entry for `tool:browser` with no explicit override"
        );
    }

    #[test]
    fn alias_cycle_renders_the_full_path() {
        let err = RegistryError::AliasCycle {
            path: vec![
                RegistryId::tool("a"),
                RegistryId::tool("b"),
                RegistryId::tool("a"),
            ],
        };
        assert_eq!(err.to_string(), "alias cycle: tool:a -> tool:b -> tool:a");
    }
}
