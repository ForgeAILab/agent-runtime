//! Shared, deterministic rendering of public hosted-search citations.

use std::collections::BTreeSet;

use agent_runtime_core::provider::{
    HostedSource, HostedTool, ProviderError, ProviderErrorKind, ProviderRequest,
};
use serde_json::Value;

/// Rejects declarations this adapter family cannot map to a hosted tool.
pub(crate) fn validate_tools(request: &ProviderRequest) -> Result<(), ProviderError> {
    for tool in &request.hosted_tools {
        match tool {
            HostedTool::WebSearch { .. } => {}
            _ => {
                return Err(ProviderError::new(
                    ProviderErrorKind::Unsupported,
                    "unsupported hosted tool",
                ));
            }
        }
    }
    Ok(())
}

/// Extracts only a source URL and title from provider wire data.
pub(crate) fn source(value: &Value, url_field: &str) -> Option<HostedSource> {
    Some(HostedSource {
        url: value.get(url_field)?.as_str()?.to_owned(),
        title: value
            .get("title")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

/// Converts a provider character index to a UTF-8 byte boundary.
pub(crate) fn char_offset(text: &str, index: usize) -> usize {
    text.char_indices()
        .nth(index)
        .map_or(text.len(), |(offset, _)| offset)
}

/// Appends each URL once per text block at its first cited span.
///
/// Offsets are byte boundaries in the original text. Stable sorting preserves
/// provider order for multiple sources attached to the same span.
pub(crate) fn render(text: &str, mut citations: Vec<(usize, HostedSource)>) -> String {
    citations.sort_by_key(|(end, _)| *end);
    let mut seen = BTreeSet::new();
    let mut rendered = String::new();
    let mut cursor = 0;
    for (end, source) in citations {
        if end > text.len() || !text.is_char_boundary(end) || !seen.insert(source.url.clone()) {
            continue;
        }
        rendered.push_str(&text[cursor..end]);
        cursor = end;
        let host = source
            .url
            .split_once("://")
            .map_or(source.url.as_str(), |(_, tail)| tail)
            .split('/')
            .next()
            .unwrap_or(&source.url);
        let title = source
            .title
            .as_deref()
            .filter(|title| !title.is_empty())
            .unwrap_or(host);
        let title = title
            .replace('\\', "\\\\")
            .replace('[', "\\[")
            .replace(']', "\\]");
        let url = source
            .url
            .replace(' ', "%20")
            .replace('(', "%28")
            .replace(')', "%29");
        rendered.push_str(&format!(" [{title}]({url})"));
    }
    rendered.push_str(&text[cursor..]);
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn citations_are_stable_deduplicated_and_unicode_safe() {
        let a = HostedSource {
            title: None,
            url: "https://example.com/a".into(),
        };
        let b = HostedSource {
            title: Some("B".into()),
            url: "https://b.test".into(),
        };
        assert_eq!(
            render("éabc", vec![(4, a.clone()), (2, b), (5, a)]),
            "é [B](https://b.test)ab [example.com](https://example.com/a)c"
        );
    }
}
