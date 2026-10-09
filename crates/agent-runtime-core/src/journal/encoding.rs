use std::fmt;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{JOURNAL_SCHEMA_VERSION, JournalCommit, conflict};
use crate::error::RuntimeError;

#[path = "ordering.rs"]
mod ordering;

/// Frozen compact UTF-8 JSON encoding with sorted object keys and ordered arrays.
/// JSON numeric types retain their serde representation, including `-0.0`.
/// Strings are never normalized; signed reasoning remains exact UTF-8 content.
/// Object envelopes optionally bind opaque JSON map order as `ordered_objects`
/// path/key-list metadata for existing byte-order-dependent fingerprints. Path
/// components are string keys or integer array indices, sorted by typed path;
/// declared struct field order is reconstructed by serde rather than recorded.
pub const JOURNAL_ENCODING: &str = "journal-json-1";
/// SHA-256 object domain. Hash input is this prefix followed by u64-BE length
/// plus encoding bytes, u64-BE length plus kind bytes, u32-BE schema version,
/// then u64-BE length plus the complete canonical object-envelope bytes.
pub const JOURNAL_OBJECT_DOMAIN: &[u8] = b"agent-runtime/session-journal/object\0";

macro_rules! digest_type {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);
        impl $name {
            /// Parses exactly 64 lowercase hexadecimal SHA-256 digits.
            pub fn parse(value: impl Into<String>) -> Result<Self, RuntimeError> {
                let value = value.into();
                if value.len() != 64
                    || !value
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return Err(conflict());
                }
                Ok(Self(value))
            }
            /// Constructs from raw SHA-256 bytes, without granting read authority.
            pub fn from_bytes(bytes: [u8; 32]) -> Self {
                let mut value = String::with_capacity(64);
                const HEX: &[u8] = b"0123456789abcdef";
                for b in bytes {
                    value.push(HEX[(b >> 4) as usize] as char);
                    value.push(HEX[(b & 15) as usize] as char);
                }
                Self(value)
            }
            /// Explicitly exposes the digest for storage lookup; never default logging.
            pub fn as_str(&self) -> &str {
                &self.0
            }
            /// The raw digest bytes, used for durable hash chaining.
            pub fn to_bytes(&self) -> [u8; 32] {
                let mut bytes = [0; 32];
                for (slot, pair) in bytes.iter_mut().zip(self.0.as_bytes().chunks_exact(2)) {
                    let digit = |b: u8| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
                    *slot = (digit(pair[0]) << 4) | digit(pair[1]);
                }
                bytes
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                Self::parse(String::deserialize(d)?)
                    .map_err(|_| serde::de::Error::custom("invalid journal digest"))
            }
        }
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "([redacted])"))
            }
        }
    };
}
digest_type!(
    JournalObjectId,
    "Content address of one immutable typed/versioned object; not read authority."
);
digest_type!(
    JournalDigest,
    "Domain-separated SHA-256 ordering/idempotency evidence, not a runtime Fingerprint."
);

/// Explicit type domain of a journal object envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalObjectKind {
    /// One canonical message (possibly signed reasoning).
    Message,
    /// One accounted usage unit.
    UsageRecord,
    /// One versioned extension value with sensitivity intact.
    Extension,
    /// One retained planned-step diagnostic.
    Manifest,
    /// Bounded persistent sequence node.
    Sequence,
    /// Bounded persistent map node.
    Map,
    /// Final planned request with ordered references.
    Request,
    /// Request settings, with empty messages/tools.
    RequestSettings,
    /// One advertised tool schema.
    ToolSchema,
    /// Referenced direct turn state.
    TurnState,
    /// One exact state field or ordered field-list item.
    StateValue,
    /// Exact attributed internal input.
    InternalInput,
    /// Referenced protected checkpoint envelope.
    Checkpoint,
    /// Retained published head.
    Head,
    /// Immutable transition batch metadata.
    Batch,
}

impl JournalObjectKind {
    /// Frozen lowercase type tag used in the encoding and SHA-256 preimage.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::UsageRecord => "usage_record",
            Self::Extension => "extension",
            Self::Manifest => "manifest",
            Self::Sequence => "sequence",
            Self::Map => "map",
            Self::Request => "request",
            Self::RequestSettings => "request_settings",
            Self::ToolSchema => "tool_schema",
            Self::TurnState => "turn_state",
            Self::StateValue => "state_value",
            Self::InternalInput => "internal_input",
            Self::Checkpoint => "checkpoint",
            Self::Head => "head",
            Self::Batch => "batch",
        }
    }
}

/// Canonical JSON bytes, independent of serde_json's `preserve_order` feature.
/// Only JSON-compatible finite values are supported. Object keys sort by UTF-8
/// bytes; arrays preserve order and string contents are not normalized.
pub fn canonical_journal_json<T: Serialize>(value: &T) -> Result<Vec<u8>, RuntimeError> {
    let value = serde_json::to_value(value).map_err(|_| conflict())?;
    let mut bytes = Vec::new();
    write_value(&value, &mut bytes)?;
    Ok(bytes)
}

fn write_value(value: &Value, bytes: &mut Vec<u8>) -> Result<(), RuntimeError> {
    match value {
        Value::Object(map) => {
            bytes.push(b'{');
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_unstable_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
            for (i, (key, value)) in entries.into_iter().enumerate() {
                if i != 0 {
                    bytes.push(b',');
                }
                serde_json::to_writer(&mut *bytes, key).map_err(|_| conflict())?;
                bytes.push(b':');
                write_value(value, bytes)?;
            }
            bytes.push(b'}');
        }
        Value::Array(values) => {
            bytes.push(b'[');
            for (i, value) in values.iter().enumerate() {
                if i != 0 {
                    bytes.push(b',');
                }
                write_value(value, bytes)?;
            }
            bytes.push(b']');
        }
        _ => serde_json::to_writer(bytes, value).map_err(|_| conflict())?,
    }
    Ok(())
}

fn hash_field(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

/// Hashes canonical object-envelope bytes with the frozen domain/type/version
/// framing. Does not by itself attest that the supplied bytes are canonical.
pub fn journal_object_id(
    kind: JournalObjectKind,
    schema_version: u32,
    bytes: &[u8],
) -> JournalObjectId {
    let mut hash = Sha256::new();
    hash.update(JOURNAL_OBJECT_DOMAIN);
    hash_field(&mut hash, JOURNAL_ENCODING.as_bytes());
    hash_field(&mut hash, kind.as_str().as_bytes());
    hash.update(schema_version.to_be_bytes());
    hash_field(&mut hash, bytes);
    JournalObjectId::from_bytes(hash.finalize().into())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    encoding: String,
    kind: JournalObjectKind,
    schema_version: u32,
    value: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    ordered_objects: Vec<ordering::ObjectOrder>,
}

/// Immutable canonical object draft. Debug reports length only, never content/ID.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalObject {
    /// Candidate content address; protected stores verify, ordinary stores remap.
    pub id: JournalObjectId,
    /// Complete typed/versioned canonical object envelope, not just payload JSON.
    pub bytes: Vec<u8>,
}
impl fmt::Debug for JournalObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JournalObject")
            .field("encoded_bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}
impl JournalObject {
    /// Encodes a journal-schema-4 typed object, including opaque Value map order where
    /// execution fingerprints require it. Prepares bytes only; writes no store.
    pub fn encode<T: Serialize>(kind: JournalObjectKind, value: &T) -> Result<Self, RuntimeError> {
        let value = serde_json::to_value(value).map_err(|_| conflict())?;
        let ordered_objects = ordering::capture(kind, &value);
        let envelope = Envelope {
            encoding: JOURNAL_ENCODING.to_owned(),
            kind,
            schema_version: JOURNAL_SCHEMA_VERSION,
            value,
            ordered_objects,
        };
        let bytes = canonical_journal_json(&envelope)?;
        Ok(Self {
            id: journal_object_id(kind, JOURNAL_SCHEMA_VERSION, &bytes),
            bytes,
        })
    }

    /// Verifies digest, exact type/schema, canonical bytes (including duplicate
    /// keys/numeric spelling), then deserializes. Failures carry no payload/ID.
    pub fn decode<T: DeserializeOwned + Serialize>(
        &self,
        kind: JournalObjectKind,
    ) -> Result<T, RuntimeError> {
        let mut envelope: Envelope = serde_json::from_slice(&self.bytes).map_err(|_| conflict())?;
        if envelope.encoding != JOURNAL_ENCODING
            || envelope.schema_version != JOURNAL_SCHEMA_VERSION
            || envelope.kind != kind
            || journal_object_id(kind, JOURNAL_SCHEMA_VERSION, &self.bytes) != self.id
            || canonical_journal_json(&envelope)? != self.bytes
        {
            return Err(conflict());
        }
        ordering::restore(kind, &mut envelope.value, &envelope.ordered_objects)?;
        let decoded = serde_json::from_value(envelope.value.clone()).map_err(|_| conflict())?;
        // Schema-4 objects must not silently drop unknown fields or invent
        // absent defaults while reconstructing protected execution content.
        if canonical_journal_json(&decoded)? != canonical_journal_json(&envelope.value)? {
            return Err(conflict());
        }
        Ok(decoded)
    }
}

/// Durable chain seed and append fold over ordered message IDs. Hash input for
/// each append is `agent-runtime/session-journal/history-step\0`, previous raw
/// 32-byte digest, then raw 32-byte object ID; the empty seed hashes
/// `agent-runtime/session-journal/history\0journal-json-1\0`.
pub fn journal_history_chain<'a>(
    items: impl IntoIterator<Item = &'a JournalObjectId>,
) -> JournalDigest {
    let mut digest: [u8; 32] =
        Sha256::digest(b"agent-runtime/session-journal/history\0journal-json-1\0").into();
    for item in items {
        let mut hash = Sha256::new();
        hash.update(b"agent-runtime/session-journal/history-step\0");
        hash.update(digest);
        hash.update(item.to_bytes());
        digest = hash.finalize().into();
    }
    JournalDigest::from_bytes(digest)
}

impl JournalCommit {
    /// Computes the private retry-input digest excluding `digest` itself. Host
    /// projection must not replace it with a projected root hash. Input includes
    /// exact ordered drafts, delta and checkpoint, including operation/fence.
    pub fn input_digest(&self) -> Result<JournalDigest, RuntimeError> {
        #[derive(Serialize)]
        struct Input<'a> {
            batch: &'a super::JournalBatch,
            objects: &'a [JournalObject],
            delta: &'a super::JournalTransitionDelta,
            checkpoint: &'a Option<super::ReferencedTurnCheckpoint>,
        }
        let bytes = canonical_journal_json(&Input {
            batch: &self.batch,
            objects: &self.objects,
            delta: &self.delta,
            checkpoint: &self.checkpoint,
        })?;
        let mut hash = Sha256::new();
        hash.update(b"agent-runtime/session-journal/commit\0");
        hash_field(&mut hash, JOURNAL_ENCODING.as_bytes());
        hash.update(JOURNAL_SCHEMA_VERSION.to_be_bytes());
        hash_field(&mut hash, &bytes);
        Ok(JournalDigest::from_bytes(hash.finalize().into()))
    }
}
