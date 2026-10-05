//! Private provider-boundary aliases; canonical runtime data is never mutated.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use agent_runtime_core::content::ContentPart;
use agent_runtime_core::provider::{
    ProviderRequest, ProviderStream, ProviderStreamEvent, ToolChoice,
};
use agent_runtime_registry::Fingerprint;
use async_stream::stream;
use futures_util::StreamExt;

const MAX_NAME_BYTES: usize = 64;

fn valid(name: &str) -> bool {
    !name.is_empty() && name.len() <= MAX_NAME_BYTES && name.bytes().all(allowed)
}

fn allowed(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}

#[derive(Default)]
pub(crate) struct ToolNames {
    to_wire: BTreeMap<String, String>,
    to_canonical: BTreeMap<String, String>,
}

impl ToolNames {
    pub(crate) fn new(request: &ProviderRequest) -> Self {
        let mut names = Self::default();
        // Registered names own the namespace. History growth must not disturb
        // their aliases (and therefore the provider's cached tool prefix).
        names.allocate(request.tools.iter().map(|tool| tool.name.clone()).collect());
        let mut historical = BTreeSet::new();
        for message in &request.messages {
            for part in &message.content {
                match part {
                    ContentPart::ToolCall(call) => {
                        historical.insert(call.name.clone());
                    }
                    ContentPart::ToolResult(result) => {
                        historical.insert(result.name.clone());
                    }
                    _ => {}
                }
            }
        }
        if let ToolChoice::Named(name) = &request.tool_choice {
            historical.insert(name.clone());
        }
        names.allocate(historical);
        names
    }

    fn allocate(&mut self, names: BTreeSet<String>) {
        // Reserve all valid names before generating any aliases.
        for name in names.iter().filter(|name| valid(name)) {
            if !self.to_wire.contains_key(name) && !self.to_canonical.contains_key(name) {
                self.insert(name.clone(), name.clone());
            }
        }
        for name in names {
            if self.to_wire.contains_key(&name) {
                continue;
            }
            let mut prefix: String = name
                .bytes()
                .map(|byte| if allowed(byte) { byte as char } else { '_' })
                .collect();
            if prefix.is_empty() {
                prefix.push_str("tool");
            }
            let fingerprint = Fingerprint::of(&name);
            let mut disambiguator = 0u64;
            loop {
                let suffix = if disambiguator == 0 {
                    format!("_{}", fingerprint.as_str())
                } else {
                    format!("_{}_{disambiguator}", fingerprint.as_str())
                };
                let candidate = format!(
                    "{}{}",
                    &prefix[..prefix.len().min(MAX_NAME_BYTES - suffix.len())],
                    suffix
                );
                if !self.to_canonical.contains_key(&candidate) {
                    self.insert(name, candidate);
                    break;
                }
                disambiguator += 1;
            }
        }
    }

    fn insert(&mut self, canonical: String, wire: String) {
        self.to_canonical.insert(wire.clone(), canonical.clone());
        self.to_wire.insert(canonical, wire);
    }

    fn wire<'a>(&'a self, name: &'a str) -> &'a str {
        self.to_wire.get(name).map_or(name, String::as_str)
    }

    fn canonical(&self, name: String) -> String {
        self.to_canonical.get(&name).cloned().unwrap_or(name)
    }

    pub(crate) fn project<'a>(&self, request: &'a ProviderRequest) -> Cow<'a, ProviderRequest> {
        if self
            .to_wire
            .iter()
            .all(|(canonical, wire)| canonical == wire)
        {
            return Cow::Borrowed(request);
        }
        let mut wire = request.clone();
        for tool in &mut wire.tools {
            tool.name = self.wire(&tool.name).to_owned();
        }
        if let ToolChoice::Named(name) = &mut wire.tool_choice {
            *name = self.wire(name).to_owned();
        }
        for message in &mut wire.messages {
            for part in &mut message.content {
                match part {
                    ContentPart::ToolCall(call) => {
                        call.name = self.wire(&call.name).to_owned();
                    }
                    ContentPart::ToolResult(result) => {
                        result.name = self.wire(&result.name).to_owned();
                    }
                    _ => {}
                }
            }
        }
        Cow::Owned(wire)
    }

    pub(crate) fn restore_stream(
        self,
        mut input: ProviderStream,
        fragmented_names: bool,
    ) -> ProviderStream {
        if self
            .to_wire
            .iter()
            .all(|(canonical, wire)| canonical == wire)
        {
            return input;
        }
        Box::pin(stream! {
            let mut pending_names = BTreeMap::<u32, String>::new();
            while let Some(mut event) = input.next().await {
                if let ProviderStreamEvent::ToolCallDelta { index, name: Some(name), .. } = &mut event {
                    if fragmented_names {
                        let pending = pending_names.entry(*index).or_default();
                        // Some gateways repeat a complete name with each delta.
                        // Only a recognized full name is a repeat, so identical
                        // fragments of an unfinished name still concatenate.
                        if pending != name || !self.to_canonical.contains_key(pending) {
                            pending.push_str(name);
                        }
                        if let ProviderStreamEvent::ToolCallDelta { name, .. } = &mut event { *name = None; }
                    } else {
                        *name = self.canonical(std::mem::take(name));
                    }
                }
                if matches!(event, ProviderStreamEvent::Finish { .. }) {
                    for (index, name) in std::mem::take(&mut pending_names) {
                        yield ProviderStreamEvent::ToolCallDelta { index, id:None, name:Some(self.canonical(name)), arguments_fragment:String::new() };
                    }
                }
                yield event;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_core::provider::{ModelId, ToolSchema};
    use serde_json::json;

    #[test]
    fn generated_alias_collision_with_valid_name_is_disambiguated_deterministically() {
        let canonical = "memory.search";
        let reserved = format!("memory_search_{}", Fingerprint::of(canonical).as_str());
        let mut request = ProviderRequest::new(ModelId::new("model"), vec![]);
        request.tools = [canonical, reserved.as_str()]
            .into_iter()
            .map(|name| ToolSchema {
                name: name.into(),
                description: String::new(),
                input_schema: json!({}),
            })
            .collect();
        let names = ToolNames::new(&request);
        assert_eq!(names.wire(&reserved), reserved);
        assert_ne!(names.wire(canonical), reserved);
        assert!(valid(names.wire(canonical)));
        assert_eq!(names.canonical(names.wire(canonical).into()), canonical);
        request.tools.reverse();
        assert_eq!(
            ToolNames::new(&request).wire(canonical),
            names.wire(canonical)
        );
    }

    #[test]
    fn growing_history_does_not_change_registered_wire_names() {
        let mut request = ProviderRequest::new(ModelId::new("model"), vec![]);
        request.tools.push(ToolSchema {
            name: "memory.search".into(),
            description: String::new(),
            input_schema: json!({}),
        });
        let names = ToolNames::new(&request);
        let wire = names.wire("memory.search").to_owned();
        request
            .messages
            .push(agent_runtime_core::content::Message::assistant(vec![
                ContentPart::ToolCall(agent_runtime_core::content::ToolCall {
                    id: agent_runtime_core::ids::ToolCallId::new("c1"),
                    name: wire.clone(),
                    arguments: json!({}),
                }),
            ]));
        let later = ToolNames::new(&request);
        assert_eq!(later.wire("memory.search"), wire);
        assert_ne!(later.wire(&wire), wire);
    }

    #[test]
    fn empty_unicode_and_overlong_names_are_bounded_and_distinct() {
        let mut request = ProviderRequest::new(ModelId::new("model"), vec![]);
        let names = [
            String::new(),
            "记忆.搜索".into(),
            "x".repeat(500),
            "-".into(),
        ];
        request.tools = names
            .iter()
            .map(|name| ToolSchema {
                name: name.clone(),
                description: String::new(),
                input_schema: json!({}),
            })
            .collect();
        let mapping = ToolNames::new(&request);
        let wires: BTreeSet<_> = names.iter().map(|name| mapping.wire(name)).collect();
        assert_eq!(wires.len(), names.len());
        assert!(wires.into_iter().all(valid));
    }
}
