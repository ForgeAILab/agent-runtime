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

/// Appends each URL at its first cited span, once per assistant message.
///
/// `linked` holds the URLs earlier text blocks of the same message already
/// cited; a source cited again in a later block is not linked again. Offsets
/// are byte boundaries in the original text. Stable sorting preserves
/// provider order for multiple sources attached to the same span.
pub(crate) fn render(
    text: &str,
    mut citations: Vec<(usize, HostedSource)>,
    linked: &mut BTreeSet<String>,
) -> String {
    citations.sort_by_key(|(end, _)| *end);
    let mut rendered = String::new();
    let mut cursor = 0;
    for (end, source) in citations {
        if end > text.len() || !text.is_char_boundary(end) || !linked.insert(source.url.clone()) {
            continue;
        }
        let url = source
            .url
            .replace(' ', "%20")
            .replace('(', "%28")
            .replace(')', "%29");
        if text.contains(&source.url) || text.contains(&url) {
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
        rendered.push_str(&format!(" [{title}]({url})"));
    }
    rendered.push_str(&text[cursor..]);
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosted_citation_preserves_an_existing_source_link() {
        let text = "([blog.rust-lang.org](https://blog.rust-lang.org/releases/))";
        let source = HostedSource {
            title: Some("The Rust Release Announcements".into()),
            url: "https://blog.rust-lang.org/releases/".into(),
        };
        let rendered = render(
            text,
            vec![(text.len(), source.clone()), (text.len(), source)],
            &mut BTreeSet::new(),
        );
        assert_eq!(rendered, text);
        assert_eq!(
            rendered
                .matches("https://blog.rust-lang.org/releases/")
                .count(),
            1
        );
    }

    #[test]
    fn hosted_citation_still_appends_a_different_source() {
        let text = "([blog.rust-lang.org](https://blog.rust-lang.org/releases/))";
        let rendered = render(
            text,
            vec![
                (
                    text.len(),
                    HostedSource {
                        title: Some("The Rust Release Announcements".into()),
                        url: "https://blog.rust-lang.org/releases/".into(),
                    },
                ),
                (
                    text.len(),
                    HostedSource {
                        title: Some("Rust".into()),
                        url: "https://www.rust-lang.org/".into(),
                    },
                ),
            ],
            &mut BTreeSet::new(),
        );
        assert_eq!(
            rendered,
            format!("{text} [Rust](https://www.rust-lang.org/)")
        );
    }

    #[test]
    fn hosted_citation_recognizes_raw_and_escaped_urls() {
        let source = HostedSource {
            title: Some("A [source]".into()),
            url: "https://example.com/a (b)".into(),
        };
        for text in [
            "See https://example.com/a (b).",
            "See [source](https://example.com/a%20%28b%29).",
        ] {
            assert_eq!(
                render(text, vec![(3, source.clone())], &mut BTreeSet::new()),
                text
            );
        }
    }

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
            render(
                "éabc",
                vec![(4, a.clone()), (2, b), (5, a)],
                &mut BTreeSet::new()
            ),
            "é [B](https://b.test)ab [example.com](https://example.com/a)c"
        );
    }

    #[test]
    fn a_source_is_linked_once_per_message_across_blocks() {
        let page = HostedSource {
            title: Some("releases.rs".into()),
            url: "https://releases.rs/".into(),
        };
        let other = HostedSource {
            title: None,
            url: "https://blog.rust-lang.org/".into(),
        };
        let mut linked = BTreeSet::new();
        assert_eq!(
            render("First.", vec![(6, page.clone())], &mut linked),
            "First. [releases.rs](https://releases.rs/)"
        );
        assert_eq!(
            render("Second.", vec![(7, page), (7, other)], &mut linked),
            "Second. [blog.rust-lang.org](https://blog.rust-lang.org/)"
        );
    }
}
