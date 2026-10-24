//! Private provider-boundary names; canonical runtime data is never mutated.

use std::borrow::Cow;
use std::collections::BTreeMap;

use agent_runtime_core::content::ContentPart;
use agent_runtime_core::provider::{
    ProviderError, ProviderErrorKind, ProviderRequest, ProviderStream, ProviderStreamEvent,
    ToolChoice,
};
use agent_runtime_core::tool::provider_tool_name;
use async_stream::stream;
use futures_util::StreamExt;

#[derive(Default)]
pub(crate) struct ToolNames {
    to_wire: BTreeMap<String, String>,
    to_canonical: BTreeMap<String, String>,
}

/// Rejects a request whose tools would reach the provider under one name.
///
/// The wire name is readable and has no disambiguating suffix, so a collision
/// cannot be repaired here; it is a registration mistake to report.
pub(crate) fn validate(request: &ProviderRequest) -> Result<(), ProviderError> {
    let mut seen = BTreeMap::<String, &str>::new();
    for tool in &request.tools {
        let wire = provider_tool_name(&tool.name);
        if wire.is_empty() {
            return Err(ProviderError::new(
                ProviderErrorKind::BadRequest,
                "a tool has an empty name",
            ));
        }
        if let Some(other) = seen.insert(wire.clone(), &tool.name) {
            return Err(ProviderError::new(
                ProviderErrorKind::BadRequest,
                format!(
                    "tools `{other}` and `{}` share the provider name `{wire}`",
                    tool.name
                ),
            ));
        }
    }
    Ok(())
}

impl ToolNames {
    pub(crate) fn new(request: &ProviderRequest) -> Self {
        let mut names = Self::default();
        // Registered names own the reverse mapping, so growing history can
        // never change which tool a model call resolves to.
        for tool in &request.tools {
            names.insert(&tool.name, true);
        }
        for message in &request.messages {
            for part in &message.content {
                match part {
                    ContentPart::ToolCall(call) => names.insert(&call.name, false),
                    ContentPart::ToolResult(result) => names.insert(&result.name, false),
                    _ => {}
                }
            }
        }
        if let ToolChoice::Named(name) = &request.tool_choice {
            names.insert(name, false);
        }
        names
    }

    fn insert(&mut self, canonical: &str, registered: bool) {
        if self.to_wire.contains_key(canonical) {
            return;
        }
        let wire = provider_tool_name(canonical);
        if registered {
            self.to_canonical
                .entry(wire.clone())
                .or_insert_with(|| canonical.to_owned());
        }
        self.to_wire.insert(canonical.to_owned(), wire);
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
    use agent_runtime_core::content::{Message, ToolCall};
    use agent_runtime_core::ids::ToolCallId;
    use agent_runtime_core::provider::{ModelId, ToolSchema};
    use serde_json::json;

    fn request(names: &[&str]) -> ProviderRequest {
        let mut request = ProviderRequest::new(ModelId::new("model"), vec![]);
        request.tools = names
            .iter()
            .map(|name| ToolSchema {
                name: (*name).into(),
                description: String::new(),
                input_schema: json!({}),
            })
            .collect();
        request
    }

    #[test]
    fn wire_names_are_readable_and_carry_no_suffix() {
        let names = ToolNames::new(&request(&["registry.activate", "read"]));
        assert_eq!(names.wire("registry.activate"), "registry_activate");
        assert_eq!(names.wire("read"), "read");
        assert_eq!(
            names.canonical("registry_activate".into()),
            "registry.activate"
        );
        // A model that repeats the dotted name from prose still reaches the tool.
        assert_eq!(
            names.canonical("registry.activate".into()),
            "registry.activate"
        );
    }

    #[test]
    fn tools_sharing_a_provider_name_are_rejected() {
        let error = validate(&request(&["memory.search", "memory_search"])).unwrap_err();
        assert_eq!(error.kind, ProviderErrorKind::BadRequest);
        assert!(error.message.contains("memory.search"), "{error}");
        assert!(error.message.contains("memory_search"), "{error}");
        assert!(validate(&request(&[""])).is_err());
        let long = "x".repeat(100);
        let longer = format!("{long}y");
        assert!(validate(&request(&[&long, &longer])).is_err());
        assert!(validate(&request(&["memory.search", "read"])).is_ok());
    }

    #[test]
    fn growing_history_does_not_change_registered_wire_names() {
        let mut request = request(&["memory_search"]);
        let before = ToolNames::new(&request).wire("memory_search").to_owned();
        // A historical call from a tool that no longer exists maps to the
        // same provider name but must not take over the reverse mapping.
        request
            .messages
            .push(Message::assistant(vec![ContentPart::ToolCall(ToolCall {
                id: ToolCallId::new("c1"),
                name: "memory.search".into(),
                arguments: json!({}),
            })]));
        let later = ToolNames::new(&request);
        assert_eq!(later.wire("memory_search"), before);
        assert_eq!(later.wire("memory.search"), before);
        assert_eq!(later.canonical(before), "memory_search");
    }
}
