//! Deterministic baseline retrieval over an already-filtered capability view.
//!
//! [`retrieve`] never sees an entry the caller's [`RegistryView`] excluded —
//! it only iterates and searches what the view already decided is visible,
//! which is what guarantees a denied or unready capability can never surface
//! through retrieval, no matter how well its (untrusted) card text matches the
//! query. This module does not re-implement policy filtering; it consumes it.
//!
//! Matching is a pure function of the query and each card's declared name,
//! tags, keywords, affordances, modalities, and dependencies: same query plus
//! same cards always yields the same ranked candidates, with ties broken by
//! [`RegistryId`] — never by hash order, never by a clock. An optional
//! injected [`EmbeddingIndex`] (see [`crate::capability::embedding`]) may add
//! or rerank candidates on top of this baseline, but the baseline itself never
//! depends on one being configured.

use std::collections::BTreeSet;

use agent_runtime_ability::AbilityDescriptor;
use agent_runtime_ability::descriptor::{Affordance, Modality};
use agent_runtime_registry::{Fingerprint, FingerprintHasher, RegistryId, RegistryView};

use crate::capability::embedding::EmbeddingIndex;
use crate::capability::query::RoutingQuery;

/// Identifies the deterministic baseline retriever's own matching semantics.
/// Recorded on every [`RetrievalResult`] so a plan can always say which
/// retriever produced a candidate, even when no embedding index is
/// configured. Bump this if scoring changes in a way that could reorder
/// results for the same inputs.
pub const DETERMINISTIC_RETRIEVER_REVISION: &str = "capability-retrieval.deterministic.v1";

const NAME_WEIGHT: u32 = 10;
const AFFORDANCE_WEIGHT: u32 = 6;
const KEYWORD_WEIGHT: u32 = 4;
const TAG_WEIGHT: u32 = 3;
const MODALITY_WEIGHT: u32 = 2;
const DEPENDENCY_WEIGHT: u32 = 1;
const HINT_BONUS_WEIGHT: u32 = 8;

/// Which mechanism contributed one candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrieverSource {
    /// Surfaced only by the always-available deterministic matcher.
    Deterministic,
    /// Surfaced only by an injected embedding/index implementation.
    Embedding,
    /// Surfaced by both; the deterministic score and the embedding score were
    /// combined.
    Both,
}

/// Why one candidate matched a [`RoutingQuery`] — every field the resolver or
/// a caller might want to explain without re-deriving it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MatchReasons {
    /// Whether the query matched the card's own local name.
    pub name: bool,
    /// Tags the query matched.
    pub tags: Vec<String>,
    /// Keywords the query matched.
    pub keywords: Vec<String>,
    /// Declared affordances the query matched.
    pub affordances: Vec<Affordance>,
    /// Declared modalities the query matched.
    pub modalities: Vec<Modality>,
    /// Dependency alternative ids the query matched by name.
    pub dependencies: Vec<RegistryId>,
    /// Host routing hints that contributed to any of the above.
    pub hints: Vec<String>,
}

impl MatchReasons {
    /// Whether nothing at all matched.
    fn is_empty(&self) -> bool {
        !self.name
            && self.tags.is_empty()
            && self.keywords.is_empty()
            && self.affordances.is_empty()
            && self.modalities.is_empty()
            && self.dependencies.is_empty()
    }

    fn score(&self) -> u32 {
        let mut score = 0u32;
        if self.name {
            score += NAME_WEIGHT;
        }
        score += TAG_WEIGHT * self.tags.len() as u32;
        score += KEYWORD_WEIGHT * self.keywords.len() as u32;
        score += AFFORDANCE_WEIGHT * self.affordances.len() as u32;
        score += MODALITY_WEIGHT * self.modalities.len() as u32;
        score += DEPENDENCY_WEIGHT * self.dependencies.len() as u32;
        score += HINT_BONUS_WEIGHT * self.hints.len() as u32;
        score
    }
}

/// One capability surfaced by retrieval: its descriptor, a deterministic
/// score, which retriever(s) produced it, and why it matched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievedCandidate {
    /// The candidate's descriptor, cloned from the (already-filtered) view.
    pub descriptor: AbilityDescriptor,
    /// The combined relevance score. Higher ranks first.
    pub score: u32,
    /// Which retriever(s) surfaced this candidate.
    pub source: RetrieverSource,
    /// Why the deterministic baseline matched, if it did.
    pub matched: MatchReasons,
}

/// An embedding index's own model and index revisions, recorded whenever an
/// injected index contributed to a [`RetrievalResult`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingRevision {
    /// The embedding model's revision.
    pub model: String,
    /// The index's own revision.
    pub index: String,
}

/// The outcome of one retrieval pass: ranked, authorized candidates plus
/// which retriever(s) produced them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalResult {
    /// A fingerprint over the query that produced this result.
    pub query_fingerprint: Fingerprint,
    /// The deterministic baseline retriever's own revision (always recorded,
    /// even when an embedding index also contributed).
    pub deterministic_revision: &'static str,
    /// The embedding index's revision, if one was configured and consulted.
    pub embedding_revision: Option<EmbeddingRevision>,
    /// Ranked candidates: highest score first, ties broken by [`RegistryId`].
    pub candidates: Vec<RetrievedCandidate>,
}

fn query_fingerprint(query: &RoutingQuery) -> Fingerprint {
    let mut hasher = FingerprintHasher::new();
    hasher.pair("kind", "routing_query");
    for term in query.terms() {
        hasher.pair("term", term);
    }
    for hint in query.hints() {
        hasher.pair("hint", hint);
    }
    hasher.finish()
}

/// Whether `term` is present in the sorted, deduplicated slice `haystack`.
fn contains(haystack: &[String], term: &str) -> bool {
    haystack
        .binary_search_by(|candidate| candidate.as_str().cmp(term))
        .is_ok()
}

/// Computes why (and how strongly) `descriptor` matches `query`, over its
/// name, tags, keywords, affordances, modalities, and dependency
/// alternatives.
fn match_reasons(descriptor: &AbilityDescriptor, query: &RoutingQuery) -> MatchReasons {
    let all_terms = query.all_terms();
    let hints: BTreeSet<&str> = query.hints().iter().map(String::as_str).collect();

    let name_lower = descriptor.id().name.to_lowercase();
    let name = contains(&all_terms, &name_lower);

    let tags: Vec<String> = descriptor
        .card()
        .tags
        .iter()
        .filter(|tag| contains(&all_terms, tag))
        .cloned()
        .collect();
    let keywords: Vec<String> = descriptor
        .card()
        .keywords
        .iter()
        .filter(|keyword| contains(&all_terms, keyword))
        .cloned()
        .collect();
    let affordances: Vec<Affordance> = descriptor
        .affordances()
        .iter()
        .filter(|affordance| contains(&all_terms, affordance.as_str()))
        .cloned()
        .collect();
    let modalities: Vec<Modality> = descriptor
        .input_modalities()
        .iter()
        .chain(descriptor.output_modalities())
        .filter(|modality| contains(&all_terms, modality.as_str()))
        .cloned()
        .collect();
    let dependencies: Vec<RegistryId> = descriptor
        .dependencies()
        .iter()
        .flat_map(|dependency| dependency.alternatives())
        .filter(|id| contains(&all_terms, &id.name.to_lowercase()))
        .cloned()
        .collect();

    let mut matched_terms: Vec<String> = Vec::new();
    if name {
        matched_terms.push(name_lower.clone());
    }
    matched_terms.extend(tags.iter().cloned());
    matched_terms.extend(keywords.iter().cloned());
    matched_terms.extend(affordances.iter().map(|a| a.as_str().to_owned()));
    matched_terms.extend(modalities.iter().map(|m| m.as_str().to_owned()));
    matched_terms.extend(dependencies.iter().map(|id| id.name.to_lowercase()));

    let mut matched_hints: Vec<String> = matched_terms
        .into_iter()
        .filter(|term| hints.contains(term.as_str()))
        .collect();
    matched_hints.sort();
    matched_hints.dedup();

    MatchReasons {
        name,
        tags,
        keywords,
        affordances,
        modalities,
        dependencies,
        hints: matched_hints,
    }
}

/// Runs the deterministic baseline over every card `view` authorizes, then
/// (if `embedding` is supplied) merges in whatever it contributes. An
/// unauthorized id can never appear in the result: embedding candidates are
/// re-checked against `view` before being merged in, exactly like the
/// baseline only ever iterates `view` in the first place.
pub fn retrieve(
    view: &RegistryView<AbilityDescriptor>,
    query: &RoutingQuery,
    embedding: Option<&dyn EmbeddingIndex>,
) -> RetrievalResult {
    let mut candidates: Vec<RetrievedCandidate> = view
        .iter()
        .filter_map(|entry| {
            let matched = match_reasons(entry.payload(), query);
            if matched.is_empty() {
                return None;
            }
            Some(RetrievedCandidate {
                descriptor: entry.payload().clone(),
                score: matched.score(),
                source: RetrieverSource::Deterministic,
                matched,
            })
        })
        .collect();

    let mut embedding_revision = None;
    if let Some(index) = embedding {
        embedding_revision = Some(EmbeddingRevision {
            model: index.model_revision().to_owned(),
            index: index.index_revision().to_owned(),
        });
        for embedded in index.search(query, view) {
            let Some(entry) = view.get(&embedded.id) else {
                // Never surface an id the view does not authorize, even if
                // an injected index suggests one.
                continue;
            };
            if let Some(existing) = candidates
                .iter_mut()
                .find(|candidate| candidate.descriptor.id() == &embedded.id)
            {
                existing.score = existing.score.max(embedded.score);
                existing.source = RetrieverSource::Both;
            } else {
                candidates.push(RetrievedCandidate {
                    descriptor: entry.payload().clone(),
                    score: embedded.score,
                    source: RetrieverSource::Embedding,
                    matched: MatchReasons::default(),
                });
            }
        }
    }

    candidates.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| a.descriptor.id().cmp(b.descriptor.id()))
    });

    RetrievalResult {
        query_fingerprint: query_fingerprint(query),
        deterministic_revision: DETERMINISTIC_RETRIEVER_REVISION,
        embedding_revision,
        candidates,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::fixtures;

    /// Spec scenario: "Embeddings are unavailable". With no embedding index
    /// configured, deterministic retrieval alone still returns the
    /// authorized, relevant card, and the result names the deterministic
    /// retriever's own revision.
    #[test]
    fn deterministic_retrieval_finds_relevant_authorized_cards_without_an_embedding_index() {
        let view = fixtures::research_view();
        let query = RoutingQuery::derive("I need to search the web for research", ["web-search"]);

        let result = retrieve(&view, &query, None);

        assert_eq!(
            result.deterministic_revision,
            DETERMINISTIC_RETRIEVER_REVISION
        );
        assert!(result.embedding_revision.is_none());
        assert!(
            result
                .candidates
                .iter()
                .any(|c| c.descriptor.id() == &fixtures::search_skill_id())
        );
    }

    #[test]
    fn the_same_query_and_cards_always_produce_the_same_ranked_candidates() {
        let view = fixtures::research_view();
        let query = RoutingQuery::derive("search the web and browse", ["research"]);

        let first = retrieve(&view, &query, None);
        let second = retrieve(&view, &query, None);

        let first_ids: Vec<_> = first
            .candidates
            .iter()
            .map(|c| c.descriptor.id().clone())
            .collect();
        let second_ids: Vec<_> = second
            .candidates
            .iter()
            .map(|c| c.descriptor.id().clone())
            .collect();
        assert_eq!(first_ids, second_ids);
    }

    #[test]
    fn ties_are_broken_by_registry_id_never_by_iteration_order() {
        let view = fixtures::redundant_search_skills_view();
        let query = RoutingQuery::derive("search the web", Vec::<String>::new());

        let result = retrieve(&view, &query, None);
        let tied: Vec<_> = result
            .candidates
            .iter()
            .filter(|c| c.score == result.candidates[0].score)
            .map(|c| c.descriptor.id().clone())
            .collect();
        let mut sorted = tied.clone();
        sorted.sort();
        assert_eq!(tied, sorted);
    }

    #[test]
    fn a_denied_entry_never_appears_among_candidates_even_when_its_keywords_match() {
        let view = fixtures::view_with_denied_entry();
        let query = RoutingQuery::derive("paid search", Vec::<String>::new());

        let result = retrieve(&view, &query, None);

        assert!(
            result
                .candidates
                .iter()
                .all(|c| c.descriptor.id() != &fixtures::denied_tool_id())
        );
    }

    #[test]
    fn an_embedding_index_cannot_surface_an_id_the_view_does_not_authorize() {
        let view = fixtures::view_with_denied_entry();
        let query = RoutingQuery::empty();
        let embedding = crate::capability::embedding::FixtureEmbeddingIndex::new("m1", "i1")
            .with_candidate(fixtures::denied_tool_id(), 100);

        let result = retrieve(&view, &query, Some(&embedding));

        assert!(
            result
                .candidates
                .iter()
                .all(|c| c.descriptor.id() != &fixtures::denied_tool_id())
        );
    }
}
