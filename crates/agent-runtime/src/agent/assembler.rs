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

/// Internal classification: only model-authored JSON syntax admits correction.
pub(crate) struct AssemblyError {
    pub(crate) error: ProviderError,
    pub(crate) correction: Option<String>,
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
        self.finish_with_correction(minter)
            .map_err(|error| error.error)
    }

    pub(crate) fn finish_with_correction(
        self,
        minter: &IdMinter,
    ) -> Result<Vec<ToolCall>, AssemblyError> {
        // Structural errors anywhere in the batch must not be hidden behind a
        // syntax error in an earlier slot.
        if let Some((index, _)) = self.slots.iter().find(|(_, slot)| slot.name.is_empty()) {
            return Err(AssemblyError {
                error: ProviderError::new(
                    ProviderErrorKind::MalformedStream,
                    format!("tool call at index {index} has no name"),
                ),
                correction: None,
            });
        }
        let mut calls = Vec::new();
        for (index, slot) in self.slots {
            let trimmed = slot.arguments.trim();
            let arguments: Value = if trimmed.is_empty() {
                Value::Object(Default::default())
            } else {
                serde_json::from_str(trimmed).map_err(|e| {
                    AssemblyError {
                        error: ProviderError::new(
                            ProviderErrorKind::MalformedStream,
                            format!("tool call at index {index} has malformed arguments: {e}"),
                        ),
                        // No raw argument text or model-supplied names: even a
                        // parser diagnostic can echo an offending value.
                        correction: Some(format!(
                            "Your previous response contained invalid JSON arguments for tool \
                             call at index {index} (line {}, column {}). None of that response's \
                             tool calls were executed. Try again with complete, valid JSON \
                             arguments. Split large commands or file contents into smaller \
                             tool calls so the arguments fit within the output limit.",
                            e.line(),
                            e.column(),
                        )),
                    }
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
