//! Assembles fragmented tool-call deltas into validated tool calls.
//!
//! Adapted from the accumulation logic in Nyx `crates/nyx-provider/src/openai.rs`
//! (`OpenAiToolCallAccumulator`), but the assembled calls are **surfaced** to
//! the runtime as validated [`ToolCall`]s (the donor discarded them) and
//! malformed arguments produce a structured [`ProviderError`] as the spec
//! requires.

use std::collections::BTreeMap;

use serde_json::Value;

use agent_runtime_core::content::ToolCall;
use agent_runtime_core::ids::ToolCallId;
use agent_runtime_core::provider::{ProviderError, ProviderErrorKind};

use crate::ids::IdMinter;

#[derive(Debug, Default)]
struct Slot {
    id: Option<String>,
    name: String,
    arguments: String,
}

/// Accumulates tool-call fragments keyed by their stream index.
#[derive(Debug, Default)]
pub struct ToolCallAssembler {
    slots: BTreeMap<u32, Slot>,
}

impl ToolCallAssembler {
    /// Feeds one fragment.
    pub fn push(&mut self, index: u32, id: Option<String>, name: Option<String>, arguments: &str) {
        let slot = self.slots.entry(index).or_default();
        if id.is_some() {
            slot.id = id;
        }
        if let Some(name) = name {
            if !name.is_empty() {
                slot.name = name;
            }
        }
        slot.arguments.push_str(arguments);
    }

    /// Whether any fragments were seen.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Validates and returns the assembled calls in index order. Missing ids are
    /// minted deterministically; malformed JSON arguments produce a structured
    /// [`ProviderErrorKind::MalformedStream`] error.
    pub fn finish(self, minter: &IdMinter) -> Result<Vec<ToolCall>, ProviderError> {
        let mut calls = Vec::new();
        for (index, slot) in self.slots {
            if slot.name.is_empty() {
                return Err(ProviderError::new(
                    ProviderErrorKind::MalformedStream,
                    format!("tool call at index {index} has no name"),
                ));
            }
            let trimmed = slot.arguments.trim();
            let arguments: Value = if trimmed.is_empty() {
                Value::Object(Default::default())
            } else {
                serde_json::from_str(trimmed).map_err(|e| {
                    ProviderError::new(
                        ProviderErrorKind::MalformedStream,
                        format!("tool call `{}` has malformed arguments: {e}", slot.name),
                    )
                })?
            };
            let id = slot
                .id
                .map(ToolCallId::new)
                .unwrap_or_else(|| minter.tool_call());
            calls.push(ToolCall {
                id,
                name: slot.name,
                arguments,
            });
        }
        Ok(calls)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assembles_fragments_into_one_validated_call() {
        let mut a = ToolCallAssembler::default();
        a.push(0, Some("c1".into()), Some("read".into()), "{\"p\":");
        a.push(0, None, None, "1}");
        let calls = a.finish(&IdMinter::new()).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "read");
        assert_eq!(calls[0].arguments, serde_json::json!({"p": 1}));
    }

    #[test]
    fn malformed_arguments_produce_structured_error() {
        let mut a = ToolCallAssembler::default();
        a.push(0, Some("c1".into()), Some("read".into()), "{not json");
        let err = a.finish(&IdMinter::new()).unwrap_err();
        assert_eq!(err.kind, ProviderErrorKind::MalformedStream);
    }

    #[test]
    fn missing_id_is_minted() {
        let mut a = ToolCallAssembler::default();
        a.push(0, None, Some("read".into()), "{}");
        let calls = a.finish(&IdMinter::new()).unwrap();
        assert_eq!(calls[0].id.as_str(), "call-1");
    }
}
