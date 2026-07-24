//! Deriving a bounded routing query from user input and host routing hints.
//!
//! A [`RoutingQuery`] is the one input every retrieval and selection stage in
//! this module shares. It keeps user-turn text and host-declared routing
//! hints in separate buckets deliberately: a hint is a deliberate, structured
//! signal from the host (a detected intent, a UI affordance the user just
//! clicked), while a term is only an incidental word from free text. Keeping
//! them apart lets the retriever weigh the two differently instead of
//! flattening every signal into one bag of words.
//!
//! Deriving a query never touches the registry: it is pure text
//! normalization, so the same input always yields the same query, which is
//! what makes retrieval built on top of it deterministic.

use std::collections::BTreeSet;

/// The maximum number of normalized terms kept in one bucket of a
/// [`RoutingQuery`] — bounding retrieval's scoring work the same way a
/// [`agent_runtime_registry::RegistryCard`] bounds its own term lists.
pub const MAX_QUERY_TERMS: usize = 64;

/// The minimum character length of a kept term; shorter fragments (stray
/// punctuation, single letters) are noise for keyword/tag/affordance
/// matching.
const MIN_TERM_CHARS: usize = 2;

/// A normalized retrieval query: bounded terms derived from the current user
/// input, plus bounded terms derived from host-provided routing hints.
///
/// Both buckets are lowercased, deduplicated, sorted, and capped at
/// [`MAX_QUERY_TERMS`], so two logically identical inputs always produce an
/// identical query and an identical [`crate::capability::retrieval::retrieve`]
/// result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoutingQuery {
    terms: Vec<String>,
    hints: Vec<String>,
}

impl RoutingQuery {
    /// Derives a query from the current user input and host-provided routing
    /// hints (for example, a detected intent tag or a UI affordance name).
    pub fn derive<H, S>(user_input: &str, hints: H) -> Self
    where
        H: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            terms: normalize_tokens(tokenize(user_input)),
            hints: normalize_tokens(hints.into_iter().map(Into::into)),
        }
    }

    /// A query with no terms and no hints. Matches nothing.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Normalized terms derived from user input.
    pub fn terms(&self) -> &[String] {
        &self.terms
    }

    /// Normalized terms derived from host routing hints.
    pub fn hints(&self) -> &[String] {
        &self.hints
    }

    /// Whether this query carries no terms and no hints at all.
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty() && self.hints.is_empty()
    }

    /// Every normalized term, user text and hints combined, sorted and
    /// deduplicated. Used by card-level search, which does not distinguish
    /// the two sources.
    pub fn all_terms(&self) -> Vec<String> {
        let mut all: Vec<String> = self
            .terms
            .iter()
            .cloned()
            .chain(self.hints.iter().cloned())
            .collect();
        all.sort();
        all.dedup();
        all
    }
}

/// Splits `input` on anything that is not alphanumeric, `-`, or `_`.
fn tokenize(input: &str) -> Vec<String> {
    input
        .split(|c: char| !(c.is_alphanumeric() || c == '-' || c == '_'))
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Lowercases, trims, drops anything shorter than [`MIN_TERM_CHARS`], sorts,
/// deduplicates, and caps a token list at [`MAX_QUERY_TERMS`].
fn normalize_tokens(tokens: impl IntoIterator<Item = String>) -> Vec<String> {
    let set: BTreeSet<String> = tokens
        .into_iter()
        .map(|t| t.trim().to_lowercase())
        .filter(|t| t.chars().count() >= MIN_TERM_CHARS)
        .collect();
    set.into_iter().take(MAX_QUERY_TERMS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deriving_the_same_input_twice_yields_an_identical_query() {
        let a = RoutingQuery::derive("Search the Web for today's news", ["web-search"]);
        let b = RoutingQuery::derive("Search the Web for today's news", ["web-search"]);
        assert_eq!(a, b);
    }

    #[test]
    fn terms_and_hints_are_normalized_sorted_and_deduplicated() {
        let query = RoutingQuery::derive("Browse Browse the web", ["Browse", "web-search"]);
        assert_eq!(query.terms(), ["browse", "the", "web"]);
        assert_eq!(query.hints(), ["browse", "web-search"]);
    }

    #[test]
    fn short_fragments_and_punctuation_are_dropped() {
        let query = RoutingQuery::derive("a! b, research.", Vec::<String>::new());
        assert_eq!(query.terms(), ["research"]);
    }

    #[test]
    fn all_terms_merges_both_buckets_without_duplicates() {
        let query = RoutingQuery::derive("web search", ["search"]);
        assert_eq!(query.all_terms(), ["search", "web"]);
    }

    #[test]
    fn an_empty_query_carries_no_terms_or_hints() {
        assert!(RoutingQuery::empty().is_empty());
        assert!(!RoutingQuery::derive("research", Vec::<String>::new()).is_empty());
    }
}
