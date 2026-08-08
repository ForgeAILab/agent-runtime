//! Turning a server's tool names into names a provider will accept.
//!
//! Two names come out of one remote tool. The registry id
//! (`mcp:<server>/<tool>`) is internal and keeps the server visible in
//! dependency resolution. The model-facing name (`mcp__<server>__<tool>`) goes
//! on the wire to a provider.
//!
//! The separator is a double underscore rather than a dot because Anthropic and
//! OpenAI both restrict tool names to `[a-zA-Z0-9_-]`; a dot is rejected at the
//! provider boundary. Names are *validated*, never rewritten: a server that
//! advertises an unusable name is told so, because silently renaming would have
//! the model call an identity the server never agreed to.

use crate::error::McpError;

/// The separator between `mcp`, the server, and the tool.
pub const SEPARATOR: &str = "__";

/// The widest model-facing name any supported provider accepts. Anthropic's
/// limit is the tightest of the three, and the runtime's own check is 256, so
/// staying under this keeps every path happy.
pub const MAX_MODEL_FACING_NAME_CHARS: usize = 128;

/// The prefix every remote tool carries.
pub const PREFIX: &str = "mcp";

/// Whether a character may appear in a name sent to a provider.
fn is_permitted(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// Checks one segment — a server name or a tool name — against the grammar
/// every supported provider shares.
///
/// The double-underscore separator is rejected inside a segment: allowing it
/// would make `mcp__a__b__c` ambiguous between server `a` / tool `b__c` and
/// server `a__b` / tool `c`.
pub fn validate_segment(server: &str, label: &str, segment: &str) -> Result<(), McpError> {
    let unusable = |reason: String| McpError::UnusableTool {
        server: server.to_owned(),
        reason,
    };

    if segment.is_empty() {
        return Err(unusable(format!("{label} name is empty")));
    }
    if let Some(c) = segment.chars().find(|c| !is_permitted(*c)) {
        return Err(unusable(format!(
            "{label} name `{segment}` contains `{c}`, which providers do not \
             accept in a tool name"
        )));
    }
    if segment.contains(SEPARATOR) {
        return Err(unusable(format!(
            "{label} name `{segment}` contains `{SEPARATOR}`, which separates \
             the server from the tool"
        )));
    }
    Ok(())
}

/// The name a provider sees for one of a server's tools.
pub fn model_facing_name(server: &str, tool: &str) -> Result<String, McpError> {
    validate_segment(server, "server", server)?;
    validate_segment(server, "tool", tool)?;

    let name = format!("{PREFIX}{SEPARATOR}{server}{SEPARATOR}{tool}");
    if name.chars().count() > MAX_MODEL_FACING_NAME_CHARS {
        return Err(McpError::UnusableTool {
            server: server.to_owned(),
            reason: format!(
                "tool `{tool}` would produce a {} character name, over the {MAX_MODEL_FACING_NAME_CHARS} character limit",
                name.chars().count()
            ),
        });
    }
    Ok(name)
}

/// The local part of a remote tool's registry id, under the `mcp:` domain.
pub fn registry_name(server: &str, tool: &str) -> String {
    format!("{server}/{tool}")
}

/// Splits a model-facing name back into its server and tool.
///
/// Returns `None` for any name this module did not produce, which is how a
/// caller tells a remote tool from a built-in.
pub fn split_model_facing_name(name: &str) -> Option<(&str, &str)> {
    let rest = name.strip_prefix(PREFIX)?.strip_prefix(SEPARATOR)?;
    let (server, tool) = rest.split_once(SEPARATOR)?;
    if server.is_empty() || tool.is_empty() {
        return None;
    }
    Some((server, tool))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_round_trips() {
        let name = model_facing_name("github", "create_issue").unwrap();
        assert_eq!(name, "mcp__github__create_issue");
        assert_eq!(
            split_model_facing_name(&name),
            Some(("github", "create_issue"))
        );
    }

    #[test]
    fn a_built_in_name_does_not_look_remote() {
        assert_eq!(split_model_facing_name("shell"), None);
        assert_eq!(split_model_facing_name("mcp__onlyserver"), None);
    }

    #[test]
    fn a_dot_is_rejected_because_providers_reject_it() {
        let error = model_facing_name("github", "create.issue").unwrap_err();
        assert!(matches!(error, McpError::UnusableTool { .. }));
        assert!(error.to_string().contains('.'));
    }

    #[test]
    fn a_separator_inside_a_segment_is_rejected_as_ambiguous() {
        // `mcp__a__b__c` must not be readable two ways.
        assert!(model_facing_name("a__b", "c").is_err());
        assert!(model_facing_name("a", "b__c").is_err());
    }

    #[test]
    fn an_over_long_name_is_rejected_rather_than_truncated() {
        let long = "t".repeat(MAX_MODEL_FACING_NAME_CHARS);
        let error = model_facing_name("github", &long).unwrap_err();
        assert!(error.to_string().contains("over the"));
    }

    #[test]
    fn an_empty_segment_is_rejected() {
        assert!(model_facing_name("", "search").is_err());
        assert!(model_facing_name("github", "").is_err());
    }

    #[test]
    fn a_registry_name_keeps_the_server_visible() {
        assert_eq!(
            registry_name("github", "create_issue"),
            "github/create_issue"
        );
    }
}
