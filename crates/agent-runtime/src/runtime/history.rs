//! Private immutable views over the unchanged public owned history surface.

use std::sync::{Arc, Mutex, Weak};

use agent_runtime_core::content::{ContentPart, Message};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_registry::{Fingerprint, FingerprintHasher};

#[derive(Default)]
pub(crate) struct HistoryGenerations {
    current: Option<Arc<HistoryGeneration>>,
    cleanup: Option<(
        agent_runtime_core::ids::SessionId,
        Weak<crate::harness::LcmCoordinator>,
    )>,
}

// Never expose source content through execution-context Debug.
impl std::fmt::Debug for HistoryGenerations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryGenerations").finish_non_exhaustive()
    }
}

pub(crate) struct HistoryGeneration {
    pub(crate) history: Arc<[Message]>,
    // Shared only by verified append successors. Hashes exclude the closing
    // array bracket, and are initialized lazily for LCM sessions only.
    hashes: Arc<Mutex<Vec<FingerprintHasher>>>,
}

impl HistoryGenerations {
    pub(crate) fn register_cleanup(
        &mut self,
        session: &agent_runtime_core::ids::SessionId,
        lcm: &Arc<crate::harness::LcmCoordinator>,
    ) {
        if self.cleanup.is_none() {
            self.cleanup = Some((session.clone(), Arc::downgrade(lcm)));
        }
    }

    pub(crate) fn capture(&mut self, history: &[Message]) -> Arc<HistoryGeneration> {
        let previous = self.current.as_ref();
        if let Some(previous) = previous {
            if messages_match(&previous.history, history) {
                return previous.clone();
            }
        }
        let hashes = previous
            .filter(|previous| {
                history
                    .get(..previous.history.len())
                    .is_some_and(|prefix| messages_match(&previous.history, prefix))
            })
            .map(|previous| previous.hashes.clone())
            .unwrap_or_default();
        let generation = Arc::new(HistoryGeneration {
            history: Arc::from(history),
            hashes,
        });
        self.current = Some(generation.clone());
        generation
    }
}

// Message PartialEq alone is insufficient evidence of byte-identical JSON:
// serde_json floating numbers compare -0.0 and +0.0 as equal. Preserve exact
// tool arguments without allocating/serializing old history on every capture.
// Also compare object iteration order when a host enables preserve_order.
fn messages_match(left: &[Message], right: &[Message]) -> bool {
    left == right
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| parts_match(&left.content, &right.content))
}

fn parts_match(left: &[ContentPart], right: &[ContentPart]) -> bool {
    left.iter()
        .zip(right)
        .all(|(left, right)| match (left, right) {
            (ContentPart::ToolCall(left), ContentPart::ToolCall(right)) => {
                numbers_match(&left.arguments, &right.arguments)
            }
            (ContentPart::ToolResult(left), ContentPart::ToolResult(right)) => {
                parts_match(&left.content, &right.content)
            }
            _ => true,
        })
}

fn numbers_match(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    use serde_json::Value;
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => {
            left.as_f64().map(f64::to_bits) == right.as_f64().map(f64::to_bits)
        }
        (Value::Array(left), Value::Array(right)) => left
            .iter()
            .zip(right)
            .all(|(left, right)| numbers_match(left, right)),
        (Value::Object(left), Value::Object(right)) => {
            left.iter()
                .zip(right)
                .all(|((key, value), (right_key, right_value))| {
                    key == right_key && numbers_match(value, right_value)
                })
        }
        _ => true,
    }
}

impl Drop for HistoryGenerations {
    fn drop(&mut self) {
        if let (Some((session, lcm)), Some(generation)) = (&self.cleanup, &self.current) {
            if let Some(lcm) = lcm.upgrade() {
                lcm.release_history(session, generation);
            }
        }
    }
}

impl HistoryGeneration {
    pub(crate) fn lineage(&self) -> Weak<Mutex<Vec<FingerprintHasher>>> {
        Arc::downgrade(&self.hashes)
    }

    pub(crate) fn fingerprint(&self, len: usize) -> Result<Fingerprint, RuntimeError> {
        if len > self.history.len() {
            return Err(RuntimeError::conflict(
                "LCM history fingerprint frontier is invalid",
            ));
        }
        let mut hashes = self.hashes.lock().expect("history fingerprints poisoned");
        if hashes.is_empty() {
            let mut opening = FingerprintHasher::new();
            opening.bytes(b"[");
            hashes.push(opening);
        }
        while hashes.len() <= len {
            let index = hashes.len() - 1;
            let mut next = hashes[index].clone();
            if index > 0 {
                next.bytes(b",");
            }
            next.bytes(&serde_json::to_vec(&self.history[index]).map_err(|error| {
                RuntimeError::internal(format!(
                    "failed to fingerprint canonical LCM history: {error}"
                ))
            })?);
            hashes.push(next);
        }
        let mut closed = hashes[len].clone();
        closed.bytes(b"]");
        Ok(closed.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::content::{ContentPart, ToolCall, ToolResultBlock};

    #[test]
    fn generations_share_views_and_preserve_signed_owned_values() {
        let mut cache = HistoryGenerations::default();
        let mut history = vec![Message::assistant(vec![ContentPart::Reasoning {
            text: "sealed thought".into(),
            redacted: true,
            signature: Some("exact signature".into()),
        }])];
        let held = cache.capture(&history);
        let owned = held.history.to_vec();
        let bytes = serde_json::to_vec(&owned).unwrap();
        assert!(Arc::ptr_eq(&held, &cache.capture(&history)));
        history.push(Message::user("next"));
        let next = cache.capture(&history);
        assert!(held.lineage().ptr_eq(&next.lineage()));
        assert_eq!(serde_json::to_vec(&held.history.as_ref()).unwrap(), bytes);
        assert_eq!(serde_json::to_vec(&owned).unwrap(), bytes);
        history[0] = Message::user("replacement");
        assert!(!next.lineage().ptr_eq(&cache.capture(&history).lineage()));
        history.truncate(1);
        assert!(!next.lineage().ptr_eq(&cache.capture(&history).lineage()));
        assert_eq!(serde_json::to_vec(&held.history.as_ref()).unwrap(), bytes);
    }

    #[test]
    fn incremental_json_fingerprints_match_legacy_bytes_for_every_content_part() {
        let call = ToolCall {
            id: "call\"\\\n".into(),
            name: "tool".into(),
            arguments: serde_json::json!({"z": [true, null, "\t雪"], "a": {"b": 1}}),
        };
        let fixtures = vec![
            Message::system("\"\\\n\r\t\u{0000}雪🦀"),
            Message::assistant(vec![
                ContentPart::text("text"),
                ContentPart::Reasoning {
                    text: "reasoning".into(),
                    redacted: false,
                    signature: None,
                },
                ContentPart::Reasoning {
                    text: "sealed".into(),
                    redacted: true,
                    signature: Some("s\"\\".into()),
                },
                ContentPart::Image {
                    url: "data:image/png;base64,AA==".into(),
                    detail: Some("high".into()),
                },
                ContentPart::Image {
                    url: "image".into(),
                    detail: None,
                },
                ContentPart::ToolCall(call.clone()),
            ]),
            Message::tool_result(ToolResultBlock {
                call_id: call.id,
                name: call.name,
                content: vec![ContentPart::text("\nresult")],
                is_error: true,
            }),
            Message::assistant(vec![]),
        ];
        let mut cache = HistoryGenerations::default();
        for len in 0..=fixtures.len() {
            let generation = cache.capture(&fixtures[..len]);
            for prefix in 0..=len {
                assert_eq!(
                    generation.fingerprint(prefix).unwrap(),
                    Fingerprint::of(serde_json::to_vec(&fixtures[..prefix]).unwrap())
                );
            }
        }
        let mut changed = fixtures;
        changed.swap(0, 1);
        let generation = cache.capture(&changed);
        assert_eq!(
            generation.fingerprint(changed.len()).unwrap(),
            Fingerprint::of(serde_json::to_vec(&changed).unwrap())
        );
        changed.truncate(1);
        let generation = cache.capture(&changed);
        assert_eq!(
            generation.fingerprint(1).unwrap(),
            Fingerprint::of(serde_json::to_vec(&changed).unwrap())
        );
        assert!(generation.fingerprint(2).is_err());
        // Golden FNV digest of the exact legacy empty-array encoding.
        assert_eq!(
            cache.capture(&[]).fingerprint(0).unwrap().as_str(),
            "0880950eb2ab1be95aa0733055956c75"
        );
    }

    #[test]
    fn equal_floating_arguments_with_different_json_bytes_start_a_new_lineage() {
        let mut generations = HistoryGenerations::default();
        let call = |zero| {
            Message::assistant(vec![ContentPart::ToolCall(ToolCall {
                id: "call".into(),
                name: "tool".into(),
                arguments: serde_json::json!({"nested": [zero]}),
            })])
        };
        let negative = vec![call(-0.0)];
        let positive = vec![call(0.0)];
        assert_eq!(negative, positive);
        assert_ne!(
            serde_json::to_vec(&negative).unwrap(),
            serde_json::to_vec(&positive).unwrap()
        );
        let old = generations.capture(&negative);
        let new = generations.capture(&positive);
        assert!(!old.lineage().ptr_eq(&new.lineage()));
        assert_ne!(old.fingerprint(1).unwrap(), new.fingerprint(1).unwrap());
        assert_eq!(
            new.fingerprint(1).unwrap(),
            Fingerprint::of(serde_json::to_vec(&positive).unwrap())
        );
    }
}
