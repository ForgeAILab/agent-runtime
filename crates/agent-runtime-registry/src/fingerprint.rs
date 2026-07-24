//! Stable, dependency-free content fingerprints.
//!
//! A [`Fingerprint`] is the one identity primitive every layer of the runtime
//! reuses to say "these inputs are the same": sealed registry snapshots, scoped
//! views, resolved model profiles, activation sets, and context plans. It is a
//! 128-bit FNV-1a digest rendered as 32 lowercase hex characters.
//!
//! Two properties matter and are both tested:
//!
//! - **Stability** — the value depends only on the bytes fed in, never on
//!   pointer addresses, hash seeds, iteration order of a `HashMap`, or the
//!   platform. The same inputs fingerprint identically across processes and
//!   runs, which is what makes replay and cache-prefix comparison meaningful.
//! - **Unambiguity** — [`FingerprintHasher::field`] length-prefixes every part,
//!   so `("ab", "c")` and `("a", "bc")` never collide by concatenation.
//!
//! This is a *fingerprint*, not a cryptographic hash: it detects change and
//! accidental collision, and must not be used as a security boundary.

use std::fmt;

/// The FNV-1a 128-bit offset basis.
const OFFSET_BASIS: u128 = 0x6c62272e07bb014262b821756295c58d;
/// The FNV-1a 128-bit prime.
const PRIME: u128 = 0x0000000001000000000000000000013b;

/// A stable 128-bit content fingerprint, rendered as 32 lowercase hex chars.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Fingerprint(String);

impl Fingerprint {
    /// The fingerprint of a single byte string.
    pub fn of(bytes: impl AsRef<[u8]>) -> Self {
        let mut hasher = FingerprintHasher::new();
        hasher.bytes(bytes.as_ref());
        hasher.finish()
    }

    /// The fingerprint of an ordered sequence of fields.
    ///
    /// Order is significant: callers that fingerprint a set must sort it first.
    pub fn of_fields<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        let mut hasher = FingerprintHasher::new();
        for field in fields {
            hasher.field(field.as_ref());
        }
        hasher.finish()
    }

    /// The hex representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Wraps an already-computed hex fingerprint (for deserialization and
    /// persisted manifests). No validation is performed.
    pub fn from_hex(hex: impl Into<String>) -> Self {
        Self(hex.into())
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// An incremental builder for a [`Fingerprint`].
#[derive(Debug, Clone)]
pub struct FingerprintHasher {
    state: u128,
}

impl Default for FingerprintHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl FingerprintHasher {
    /// A new hasher at the FNV-1a offset basis.
    pub fn new() -> Self {
        Self {
            state: OFFSET_BASIS,
        }
    }

    /// Absorbs raw bytes with no framing.
    pub fn bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= *byte as u128;
            self.state = self.state.wrapping_mul(PRIME);
        }
    }

    /// Absorbs one length-prefixed field, so adjacent fields cannot be confused
    /// with a single longer field.
    pub fn field(&mut self, bytes: impl AsRef<[u8]>) -> &mut Self {
        let bytes = bytes.as_ref();
        self.bytes(&(bytes.len() as u64).to_le_bytes());
        self.bytes(bytes);
        self
    }

    /// Absorbs a `name = value` pair as two framed fields.
    pub fn pair(&mut self, name: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> &mut Self {
        self.field(name);
        self.field(value);
        self
    }

    /// Absorbs a nested fingerprint as one framed field.
    pub fn nested(&mut self, fingerprint: &Fingerprint) -> &mut Self {
        self.field(fingerprint.as_str());
        self
    }

    /// Finishes the digest.
    pub fn finish(&self) -> Fingerprint {
        Fingerprint(format!("{:032x}", self.state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_input_fingerprints_identically() {
        assert_eq!(Fingerprint::of("abc"), Fingerprint::of("abc"));
        assert_ne!(Fingerprint::of("abc"), Fingerprint::of("abd"));
    }

    #[test]
    fn fingerprint_is_thirty_two_hex_chars() {
        let fp = Fingerprint::of("abc");
        assert_eq!(fp.as_str().len(), 32);
        assert!(fp.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn framing_prevents_concatenation_collisions() {
        assert_ne!(
            Fingerprint::of_fields(["ab", "c"]),
            Fingerprint::of_fields(["a", "bc"]),
        );
    }

    #[test]
    fn field_order_is_significant() {
        assert_ne!(
            Fingerprint::of_fields(["a", "b"]),
            Fingerprint::of_fields(["b", "a"]),
        );
    }

    #[test]
    fn nested_fingerprints_compose() {
        let inner = Fingerprint::of("inner");
        let mut hasher = FingerprintHasher::new();
        hasher.pair("kind", "outer").nested(&inner);
        let outer = hasher.finish();
        assert_ne!(outer, inner);

        let mut again = FingerprintHasher::new();
        again.pair("kind", "outer").nested(&inner);
        assert_eq!(outer, again.finish());
    }

    #[test]
    fn hex_roundtrips_through_from_hex() {
        let fp = Fingerprint::of("abc");
        assert_eq!(Fingerprint::from_hex(fp.as_str()), fp);
    }
}
