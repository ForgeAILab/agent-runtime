//! The bounded searchable card every registry entry publishes.
//!
//! A [`RegistryCard`] is what search sees. It is deliberately small and
//! deliberately *bounded*: a skill's card names its instruction file, it does
//! not contain it. That is what lets the index describe a thousand
//! capabilities without any of them costing model context until one is
//! actually activated.
//!
//! Card text is **untrusted input**. A plugin manifest, an MCP server
//! description, and a remote catalog record all land here, so every field is
//! length-bounded at construction and none of it is ever treated as privileged
//! instruction.

use crate::fingerprint::{Fingerprint, FingerprintHasher};
use crate::id::{EntryProvenance, RegistryId};

/// The maximum length of a card title, in characters.
pub const MAX_TITLE_CHARS: usize = 120;
/// The maximum length of a card summary, in characters.
pub const MAX_SUMMARY_CHARS: usize = 512;
/// The maximum number of tags or keywords on one card.
pub const MAX_TERMS: usize = 32;
/// The maximum length of one tag or keyword, in characters.
pub const MAX_TERM_CHARS: usize = 48;

/// Bounded, searchable metadata for one registry entry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RegistryCard {
    /// The entry's namespaced identity.
    pub id: RegistryId,
    /// Which layer declared it, at which revision, overriding what.
    pub provenance: EntryProvenance,
    /// A short human-facing title.
    pub title: String,
    /// A routing description: what this entry is for.
    pub summary: String,
    /// Sorted, deduplicated classification tags.
    #[cfg_attr(feature = "serde", serde(default))]
    pub tags: Vec<String>,
    /// Sorted, deduplicated retrieval keywords.
    #[cfg_attr(feature = "serde", serde(default))]
    pub keywords: Vec<String>,
}

impl RegistryCard {
    /// A card for `id`, with every text field truncated to its bound.
    pub fn new(
        id: RegistryId,
        provenance: EntryProvenance,
        title: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            id,
            provenance,
            title: bound(title.into(), MAX_TITLE_CHARS),
            summary: bound(summary.into(), MAX_SUMMARY_CHARS),
            tags: Vec::new(),
            keywords: Vec::new(),
        }
    }

    /// Adds classification tags, bounded and normalized.
    pub fn with_tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags = normalize_terms(tags);
        self
    }

    /// Adds retrieval keywords, bounded and normalized.
    pub fn with_keywords<I, S>(mut self, keywords: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.keywords = normalize_terms(keywords);
        self
    }

    /// Whether any of `terms` matches this card's name, tags, or keywords.
    /// Matching is case-insensitive and exact per term; substring and semantic
    /// matching are the retriever's business, not the card's.
    pub fn matches_any(&self, terms: &[String]) -> bool {
        terms.iter().any(|term| {
            let term = term.to_lowercase();
            self.id.name.to_lowercase() == term
                || self.tags.contains(&term)
                || self.keywords.contains(&term)
        })
    }

    /// Absorbs this card into a fingerprint.
    pub fn fingerprint_into(&self, hasher: &mut FingerprintHasher) {
        self.id.fingerprint_into(hasher);
        self.provenance.fingerprint_into(hasher);
        hasher.pair("title", &self.title);
        hasher.pair("summary", &self.summary);
        for tag in &self.tags {
            hasher.pair("tag", tag);
        }
        for keyword in &self.keywords {
            hasher.pair("keyword", keyword);
        }
    }

    /// This card's own fingerprint.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut hasher = FingerprintHasher::new();
        self.fingerprint_into(&mut hasher);
        hasher.finish()
    }
}

/// Truncates `text` to `max` characters, respecting char boundaries.
fn bound(text: String, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((byte, _)) => text[..byte].to_owned(),
        None => text,
    }
}

/// Lowercases, trims, bounds, sorts, and deduplicates a term list, then caps it
/// at [`MAX_TERMS`]. Normalizing here is what makes card fingerprints stable
/// regardless of the order a declaration happened to list its tags in.
fn normalize_terms<I, S>(terms: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut out: Vec<String> = terms
        .into_iter()
        .map(|term| bound(term.into().trim().to_lowercase(), MAX_TERM_CHARS))
        .filter(|term| !term.is_empty())
        .collect();
    out.sort();
    out.dedup();
    out.truncate(MAX_TERMS);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{RegistryRevision, RegistrySource};

    fn card() -> RegistryCard {
        RegistryCard::new(
            RegistryId::skill("web-research"),
            EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1")),
            "Web research",
            "Searches the web and summarizes findings",
        )
        .with_tags(["Research", "web"])
        .with_keywords(["search", "browse", "search"])
    }

    #[test]
    fn terms_are_normalized_sorted_and_deduplicated() {
        let card = card();
        assert_eq!(card.tags, ["research", "web"]);
        assert_eq!(card.keywords, ["browse", "search"]);
    }

    #[test]
    fn term_order_does_not_change_the_fingerprint() {
        let a = RegistryCard::new(
            RegistryId::skill("s"),
            EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1")),
            "t",
            "s",
        )
        .with_tags(["b", "a"]);
        let b = RegistryCard::new(
            RegistryId::skill("s"),
            EntryProvenance::new(RegistrySource::BuiltIn, RegistryRevision::new("1")),
            "t",
            "s",
        )
        .with_tags(["a", "b"]);
        assert_eq!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn a_changed_revision_changes_the_card_fingerprint() {
        let a = card();
        let mut b = card();
        b.provenance.revision = RegistryRevision::new("2");
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn matching_is_case_insensitive_across_name_tags_and_keywords() {
        let card = card();
        assert!(card.matches_any(&["WEB".to_string()]));
        assert!(card.matches_any(&["browse".to_string()]));
        assert!(card.matches_any(&["web-research".to_string()]));
        assert!(!card.matches_any(&["database".to_string()]));
    }

    #[test]
    fn untrusted_text_is_truncated_to_its_bound() {
        let long = "x".repeat(MAX_SUMMARY_CHARS * 2);
        let card = RegistryCard::new(
            RegistryId::tool("t"),
            EntryProvenance::new(RegistrySource::Plugin, RegistryRevision::new("1")),
            long.clone(),
            long,
        );
        assert_eq!(card.title.chars().count(), MAX_TITLE_CHARS);
        assert_eq!(card.summary.chars().count(), MAX_SUMMARY_CHARS);
    }

    #[test]
    fn truncation_respects_char_boundaries() {
        let card = RegistryCard::new(
            RegistryId::tool("t"),
            EntryProvenance::new(RegistrySource::Plugin, RegistryRevision::new("1")),
            "é".repeat(MAX_TITLE_CHARS + 10),
            "",
        );
        assert_eq!(card.title.chars().count(), MAX_TITLE_CHARS);
    }
}
