//! Opaque LCM identities and deterministic revision primitives.

use std::fmt;

use agent_runtime_registry::{Fingerprint, FingerprintHasher};
use serde::{Deserialize, Deserializer, Serialize};

/// Maximum bounded length for host-supplied opaque identities.
pub const MAX_LCM_ID_CHARS: usize = 256;

/// Validation failure for an opaque identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LcmIdError {
    /// Stable field label.
    pub kind: &'static str,
    /// Length observed.
    pub length: usize,
}

impl fmt::Display for LcmIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} identity length {} is outside 1..={}",
            self.kind, self.length, MAX_LCM_ID_CHARS
        )
    }
}

impl std::error::Error for LcmIdError {}

macro_rules! opaque_id {
    ($name:ident, $label:literal) => {
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Wraps a host-generated opaque identifier.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the opaque identifier without interpreting it.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Returns whether the identifier is empty or whitespace-only.
            pub fn is_empty(&self) -> bool {
                self.0.trim().is_empty()
            }

            /// Validates bounded, non-blank identity input.
            pub fn validate(&self) -> Result<(), LcmIdError> {
                let length = self.0.chars().count();
                if length == 0 || length > MAX_LCM_ID_CHARS || self.0.trim().is_empty() {
                    Err(LcmIdError {
                        kind: $label,
                        length,
                    })
                } else {
                    Ok(())
                }
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                // Host labels can contain secrets or user text. Diagnostics
                // retain only a stable non-reversible fingerprint.
                formatter
                    .debug_tuple($label)
                    .field(&Fingerprint::of(self.0.as_bytes()))
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

opaque_id!(LcmTimelineId, "LcmTimelineId");
opaque_id!(LcmEntryId, "LcmEntryId");
opaque_id!(LcmNodeId, "LcmNodeId");
opaque_id!(LcmOperationId, "LcmOperationId");

/// A monotonically increasing timeline sequence position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LcmSequence(u64);

impl LcmSequence {
    /// Creates a sequence position.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The numeric sequence position.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the immediately following position, if representable.
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the immediately preceding position, if representable.
    pub const fn previous(self) -> Option<Self> {
        match self.0.checked_sub(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl From<u64> for LcmSequence {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<LcmSequence> for u64 {
    fn from(value: LcmSequence) -> Self {
        value.get()
    }
}

impl fmt::Display for LcmSequence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// An inclusive, contiguous sequence range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct LcmRange {
    /// First covered sequence position.
    pub start: LcmSequence,
    /// Last covered sequence position.
    pub end: LcmSequence,
}

impl<'de> Deserialize<'de> for LcmRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireRange {
            start: LcmSequence,
            end: LcmSequence,
        }

        let range = WireRange::deserialize(deserializer)?;
        Self::new(range.start, range.end).map_err(serde::de::Error::custom)
    }
}

/// Validation error for an invalid inclusive sequence range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LcmRangeError {
    /// The supplied start position.
    pub start: LcmSequence,
    /// The supplied end position.
    pub end: LcmSequence,
}

impl fmt::Display for LcmRangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "LCM range start {} is after end {}",
            self.start, self.end
        )
    }
}

impl std::error::Error for LcmRangeError {}

impl LcmRange {
    /// Creates an inclusive range, rejecting reversed bounds.
    pub const fn new(start: LcmSequence, end: LcmSequence) -> Result<Self, LcmRangeError> {
        if start.get() > end.get() {
            Err(LcmRangeError { start, end })
        } else {
            Ok(Self { start, end })
        }
    }

    /// Alias emphasizing that both endpoints are included.
    pub const fn inclusive(start: LcmSequence, end: LcmSequence) -> Result<Self, LcmRangeError> {
        Self::new(start, end)
    }

    /// A one-position range.
    pub const fn single(sequence: LcmSequence) -> Self {
        Self {
            start: sequence,
            end: sequence,
        }
    }

    /// Number of positions in the range.
    pub const fn len(self) -> u64 {
        self.end
            .get()
            .saturating_sub(self.start.get())
            .saturating_add(1)
    }

    /// Whether this inclusive range contains no positions.
    pub const fn is_empty(self) -> bool {
        false
    }

    /// Whether this range contains `sequence`.
    pub const fn contains(self, sequence: LcmSequence) -> bool {
        sequence.get() >= self.start.get() && sequence.get() <= self.end.get()
    }

    /// Whether this range is directly adjacent to `other`.
    pub fn is_adjacent_to(self, other: Self) -> bool {
        match self.end.next() {
            Some(next) if next == other.start => true,
            _ => match other.end.next() {
                Some(next) => next == self.start,
                None => false,
            },
        }
    }

    /// Whether this range overlaps `other`.
    pub const fn overlaps(self, other: Self) -> bool {
        self.start.get() <= other.end.get() && other.start.get() <= self.end.get()
    }

    /// Returns the smallest range containing both ranges.
    pub const fn hull(self, other: Self) -> Self {
        Self {
            start: if self.start.get() <= other.start.get() {
                self.start
            } else {
                other.start
            },
            end: if self.end.get() >= other.end.get() {
                self.end
            } else {
                other.end
            },
        }
    }
}

/// The revision of the immutable timeline plus derived DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LcmRevision(u64);

impl LcmRevision {
    /// The revision before any durable mutation.
    pub const INITIAL: Self = Self(0);

    /// Creates a revision.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Numeric revision value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the immediately following revision, if representable.
    ///
    /// Mutation code must fail closed when the revision space is exhausted;
    /// silently saturating would let a successful mutation retain the same
    /// compare-and-swap revision.
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl Default for LcmRevision {
    fn default() -> Self {
        Self::INITIAL
    }
}

impl fmt::Display for LcmRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Alias used where a revision specifically refers to the DAG.
pub type LcmDagRevision = LcmRevision;

/// Opaque continuation for a bounded expansion response.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LcmExpansionCursor {
    /// Node whose direct children/entries are being expanded.
    pub node_id: LcmNodeId,
    /// Number of direct children/entries already returned.
    pub offset: usize,
    /// Fingerprint of the expansion input at cursor creation.
    pub source_fingerprint: Fingerprint,
}

/// Stable operation fingerprint used for idempotent append and compaction
/// ownership.  It is metadata only and never contains source bodies.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LcmOperationFingerprint(Fingerprint);

impl LcmOperationFingerprint {
    /// Wraps a precomputed fingerprint.
    pub fn new(fingerprint: Fingerprint) -> Self {
        Self(fingerprint)
    }

    /// Computes a framed fingerprint from ordered operation fields.
    pub fn from_fields<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        let mut hasher = FingerprintHasher::new();
        for field in fields {
            hasher.field(field);
        }
        Self(hasher.finish())
    }

    /// The underlying stable fingerprint.
    pub fn as_fingerprint(&self) -> &Fingerprint {
        &self.0
    }

    /// The hexadecimal representation.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<Fingerprint> for LcmOperationFingerprint {
    fn from(value: Fingerprint) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for LcmOperationFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_rejects_gaps_and_computes_length() {
        let range = LcmRange::new(LcmSequence::new(4), LcmSequence::new(8)).unwrap();
        assert_eq!(range.len(), 5);
        assert!(range.contains(LcmSequence::new(6)));
        assert!(!range.contains(LcmSequence::new(9)));
        assert!(!range.is_adjacent_to(LcmRange::single(LcmSequence::new(10))));
    }

    #[test]
    fn operation_fingerprints_frame_fields() {
        assert_ne!(
            LcmOperationFingerprint::from_fields(["ab", "c"]),
            LcmOperationFingerprint::from_fields(["a", "bc"])
        );
    }

    #[test]
    fn revision_advance_fails_at_numeric_limit() {
        assert_eq!(LcmRevision::new(41).next(), Some(LcmRevision::new(42)));
        assert_eq!(LcmRevision::new(u64::MAX).next(), None);
    }
}
