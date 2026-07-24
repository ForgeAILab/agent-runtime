//! An optional embedding/vector index augmenting the deterministic baseline.
//!
//! An [`EmbeddingIndex`] may add candidates the deterministic matcher missed
//! or rerank the ones it found, but it is never load-bearing:
//! [`crate::capability::retrieval::retrieve`] produces a complete,
//! deterministic result with `embedding: None`, and only ever consults this
//! trait when a caller supplies an implementation. Every candidate it
//! contributes is re-checked against the caller's [`RegistryView`] before
//! being merged in, so an index cannot smuggle an unauthorized id past the
//! view that already filtered it out.

use std::fmt;

use agent_runtime_ability::AbilityDescriptor;
use agent_runtime_registry::{RegistryId, RegistryView};

use crate::capability::query::RoutingQuery;

/// One candidate an embedding/index implementation contributes: an id plus
/// its own relevance score, on whatever scale that implementation defines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingCandidate {
    /// The candidate's registry id.
    pub id: RegistryId,
    /// The index's own relevance score for this candidate.
    pub score: u32,
}

/// An optional embedding or vector index consulted on top of the
/// deterministic baseline retriever.
///
/// Implementations must not consult a clock, random source, or any other
/// non-deterministic input: the reproducibility guarantee retrieval offers
/// depends on the same query against the same view always producing the
/// same contribution from this trait.
pub trait EmbeddingIndex: fmt::Debug + Send + Sync {
    /// The embedding model's revision, recorded on every result this index
    /// contributes to, so a stale or mismatched model stays observable.
    fn model_revision(&self) -> &str;

    /// The index's own revision — bumped on rebuild even when the model
    /// itself does not change.
    fn index_revision(&self) -> &str;

    /// Candidates this index would add or rerank for `query`. Callers must
    /// treat every returned id as a *suggestion*: only ids `view` also
    /// authorizes are ever merged into a retrieval result.
    fn search(
        &self,
        query: &RoutingQuery,
        view: &RegistryView<AbilityDescriptor>,
    ) -> Vec<EmbeddingCandidate>;
}

/// A deterministic fixture embedding index for tests: a fixed table of
/// `(id, score)` pairs, returned verbatim regardless of the query text.
#[derive(Debug, Clone, Default)]
pub struct FixtureEmbeddingIndex {
    model_revision: String,
    index_revision: String,
    table: Vec<(RegistryId, u32)>,
}

impl FixtureEmbeddingIndex {
    /// An empty fixture index at the given model/index revisions.
    pub fn new(model_revision: impl Into<String>, index_revision: impl Into<String>) -> Self {
        Self {
            model_revision: model_revision.into(),
            index_revision: index_revision.into(),
            table: Vec::new(),
        }
    }

    /// Adds a fixed `(id, score)` entry the index always returns.
    pub fn with_candidate(mut self, id: RegistryId, score: u32) -> Self {
        self.table.push((id, score));
        self
    }
}

impl EmbeddingIndex for FixtureEmbeddingIndex {
    fn model_revision(&self) -> &str {
        &self.model_revision
    }

    fn index_revision(&self) -> &str {
        &self.index_revision
    }

    fn search(
        &self,
        _query: &RoutingQuery,
        view: &RegistryView<AbilityDescriptor>,
    ) -> Vec<EmbeddingCandidate> {
        self.table
            .iter()
            .filter(|(id, _)| view.get(id).is_some())
            .map(|(id, score)| EmbeddingCandidate {
                id: id.clone(),
                score: *score,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::fixtures;

    #[test]
    fn a_fixture_index_returns_its_fixed_table_regardless_of_query_text() {
        let view = fixtures::research_view();
        let index = FixtureEmbeddingIndex::new("model-1", "index-1")
            .with_candidate(fixtures::search_skill_id(), 42);

        let first = index.search(&RoutingQuery::empty(), &view);
        let second = index.search(
            &RoutingQuery::derive("anything at all", Vec::<String>::new()),
            &view,
        );

        assert_eq!(first, second);
        assert_eq!(
            first,
            vec![EmbeddingCandidate {
                id: fixtures::search_skill_id(),
                score: 42
            }]
        );
    }

    #[test]
    fn a_fixture_index_never_returns_an_id_the_view_excludes() {
        let view = fixtures::view_with_denied_entry();
        let index = FixtureEmbeddingIndex::new("model-1", "index-1")
            .with_candidate(fixtures::denied_tool_id(), 99);

        assert!(index.search(&RoutingQuery::empty(), &view).is_empty());
    }
}
