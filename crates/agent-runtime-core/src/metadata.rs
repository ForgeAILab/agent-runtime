//! Redaction-safe metadata.
//!
//! Metadata attached to events, errors, and provider attempts is safe to emit:
//! values inserted as *redacted* never keep their cleartext — they serialize
//! and display as `"[redacted]"`. Vendor metadata captured from a provider is
//! additionally bounded in size so a hostile or noisy provider cannot bloat the
//! event stream.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// A single metadata value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetaValue {
    /// A textual value.
    Text(String),
    /// An integer value.
    Int(i64),
    /// A boolean value.
    Bool(bool),
    /// A value that was intentionally redacted; renders as `"[redacted]"`.
    #[serde(serialize_with = "serialize_redacted")]
    Redacted,
}

fn serialize_redacted<S: serde::Serializer>(s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(REDACTED)
}

const REDACTED: &str = "[redacted]";

impl From<&str> for MetaValue {
    fn from(v: &str) -> Self {
        MetaValue::Text(v.to_owned())
    }
}
impl From<String> for MetaValue {
    fn from(v: String) -> Self {
        MetaValue::Text(v)
    }
}
impl From<i64> for MetaValue {
    fn from(v: i64) -> Self {
        MetaValue::Int(v)
    }
}
impl From<u64> for MetaValue {
    fn from(v: u64) -> Self {
        MetaValue::Int(v as i64)
    }
}
impl From<bool> for MetaValue {
    fn from(v: bool) -> Self {
        MetaValue::Bool(v)
    }
}

impl fmt::Display for MetaValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetaValue::Text(t) => f.write_str(t),
            MetaValue::Int(i) => write!(f, "{i}"),
            MetaValue::Bool(b) => write!(f, "{b}"),
            MetaValue::Redacted => f.write_str(REDACTED),
        }
    }
}

/// An ordered, redaction-safe metadata bag.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Metadata {
    entries: BTreeMap<String, MetaValue>,
}

impl Metadata {
    /// An empty metadata bag.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a safe, non-secret value.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<MetaValue>) -> &mut Self {
        self.entries.insert(key.into(), value.into());
        self
    }

    /// Records that `key` was present but redacts its value. The cleartext is
    /// never stored, so this is safe even if the source value was a secret.
    pub fn redact(&mut self, key: impl Into<String>) -> &mut Self {
        self.entries.insert(key.into(), MetaValue::Redacted);
        self
    }

    /// Builder-style [`Metadata::insert`].
    pub fn with(mut self, key: impl Into<String>, value: impl Into<MetaValue>) -> Self {
        self.insert(key, value);
        self
    }

    /// Returns the value for `key`, if present.
    pub fn get(&self, key: &str) -> Option<&MetaValue> {
        self.entries.get(key)
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the bag is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates entries in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &MetaValue)> {
        self.entries.iter()
    }

    /// Merges `other` into `self`, with `other` winning on key conflicts.
    pub fn merge(&mut self, other: Metadata) {
        for (k, v) in other.entries {
            self.entries.insert(k, v);
        }
    }

    /// Captures arbitrary vendor JSON as bounded, safe metadata: at most
    /// `limits.max_entries` top-level keys are kept and each value is truncated
    /// to `limits.max_value_len` characters. Keys listed in `redact_keys` are
    /// stored redacted. Nested structures are stringified before truncation.
    pub fn capture_vendor(
        value: &serde_json::Value,
        limits: VendorLimits,
        redact_keys: &[&str],
    ) -> Metadata {
        let mut out = Metadata::new();
        let serde_json::Value::Object(map) = value else {
            return out;
        };
        for (key, val) in map.iter().take(limits.max_entries) {
            if redact_keys.contains(&key.as_str()) {
                out.redact(key.clone());
                continue;
            }
            let rendered = match val {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let truncated: String = rendered.chars().take(limits.max_value_len).collect();
            out.insert(key.clone(), truncated);
        }
        out
    }
}

/// Bounds applied when capturing vendor metadata.
#[derive(Debug, Clone, Copy)]
pub struct VendorLimits {
    /// Maximum number of top-level keys kept.
    pub max_entries: usize,
    /// Maximum characters kept per value.
    pub max_value_len: usize,
}

impl Default for VendorLimits {
    fn default() -> Self {
        Self {
            max_entries: 16,
            max_value_len: 256,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacted_values_never_leak() {
        let mut m = Metadata::new();
        m.insert("model", "gpt-x").redact("api_key");
        let json = serde_json::to_value(&m).unwrap();
        assert_eq!(json["model"], "gpt-x");
        assert_eq!(json["api_key"], "[redacted]");
    }

    #[test]
    fn vendor_capture_is_bounded_and_redacts() {
        let raw = serde_json::json!({
            "a": "x".repeat(1000),
            "authorization": "secret-token",
            "b": 1, "c": 2, "d": 3
        });
        let limits = VendorLimits {
            max_entries: 3,
            max_value_len: 8,
        };
        let m = Metadata::capture_vendor(&raw, limits, &["authorization"]);
        assert!(m.len() <= 3);
        assert_eq!(m.get("authorization"), Some(&MetaValue::Redacted));
        if let Some(MetaValue::Text(a)) = m.get("a") {
            assert!(a.chars().count() <= 8);
        }
    }
}
