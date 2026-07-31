//! Bounded, sensitivity-aware host memory contribution.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;

use agent_runtime_context::{
    CacheClass, ContextFragment, ContextLane, ContextPosition, FragmentContent, FragmentKind,
    FragmentSource, Sensitivity,
};
use agent_runtime_core::content::Message;
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::ids::{SessionId, TurnId};
use agent_runtime_registry::RegistryRevision;

use super::pipeline::{ComponentDescriptor, ContextContributor, ContextPatch, ContextView};

/// Maximum records returned by one generic memory source.
pub const MAX_MEMORY_RECORDS: usize = 16;
/// Maximum characters in one record.
pub const MAX_MEMORY_RECORD_CHARS: usize = 4_096;
/// Aggregate character ceiling per provider boundary.
pub const MAX_MEMORY_TOTAL_CHARS: usize = 16_384;
/// Maximum stable record-id length.
pub const MAX_MEMORY_ID_CHARS: usize = 96;

/// Immutable query passed to a host-owned retrieval policy.
#[derive(Clone)]
pub struct MemoryQuery {
    /// Owning session.
    pub session: SessionId,
    /// Active turn.
    pub turn: TurnId,
    /// Canonical history at the provider safe boundary.
    pub history: Arc<[Message]>,
}

impl fmt::Debug for MemoryQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryQuery")
            .field("session", &self.session)
            .field("turn", &self.turn)
            .field("history_messages", &self.history.len())
            .finish()
    }
}

/// One host-selected memory record.
#[derive(Clone, PartialEq, Eq)]
pub struct MemoryRecord {
    /// Stable source-local id.
    pub id: String,
    /// Exact content revision.
    pub revision: RegistryRevision,
    /// Bounded content.
    pub content: String,
    /// Content-handling requirement.
    pub sensitivity: Sensitivity,
    /// Lower values are retained first under structural pressure.
    pub priority: i32,
}

impl fmt::Debug for MemoryRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryRecord")
            .field("id_chars", &self.id.chars().count())
            .field("revision", &self.revision)
            .field("content_chars", &self.content.chars().count())
            .field("sensitivity", &self.sensitivity)
            .field("priority", &self.priority)
            .finish()
    }
}

/// Host-owned storage, retrieval, ranking, retention, and user-control policy.
///
/// The generic harness only validates and contributes the returned records.
/// It never writes memory, infers a source, or folds records into canonical
/// conversation history.
#[async_trait]
pub trait MemorySource: Send + Sync + fmt::Debug {
    /// Stable source id used in the ordered pipeline.
    fn id(&self) -> &str;

    /// Retrieval implementation/schema revision.
    fn revision(&self) -> RegistryRevision;

    /// Retrieves already-ranked records for this boundary.
    async fn retrieve(&self, query: &MemoryQuery) -> Result<Vec<MemoryRecord>, RuntimeError>;
}

/// Generic context contributor around one host policy.
#[derive(Clone)]
pub struct MemoryContributor {
    source: Arc<dyn MemorySource>,
}

impl fmt::Debug for MemoryContributor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryContributor")
            .field("source", &self.source.id())
            .field("revision", &self.source.revision())
            .finish_non_exhaustive()
    }
}

impl MemoryContributor {
    /// Wraps one host-owned source.
    pub fn new(source: Arc<dyn MemorySource>) -> Result<Self, RuntimeError> {
        let id = source.id();
        if id.trim().is_empty()
            || id.chars().count() > 96
            || !id
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
        {
            return Err(RuntimeError::config(
                "memory source id must be 1..=96 ASCII slug characters",
            ));
        }
        Ok(Self { source })
    }
}

#[async_trait]
impl ContextContributor for MemoryContributor {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new(
            format!("harness.memory.{}", self.source.id()),
            self.source.revision(),
        )
    }

    async fn contribute(&self, view: &ContextView) -> Result<ContextPatch, RuntimeError> {
        let records = self
            .source
            .retrieve(&MemoryQuery {
                session: view.session.clone(),
                turn: view.turn.clone(),
                history: view.history.clone(),
            })
            .await?;
        if records.len() > MAX_MEMORY_RECORDS {
            return Err(RuntimeError::conflict(format!(
                "memory source `{}` returned {} records; maximum is {MAX_MEMORY_RECORDS}",
                self.source.id(),
                records.len()
            )));
        }

        let mut ids = BTreeSet::new();
        let mut total_chars = 0usize;
        let mut fragments = Vec::with_capacity(records.len());
        for (index, record) in records.into_iter().enumerate() {
            let id_chars = record.id.chars().count();
            let content_chars = record.content.chars().count();
            if record.id.trim().is_empty()
                || id_chars > MAX_MEMORY_ID_CHARS
                || !ids.insert(record.id.clone())
            {
                return Err(RuntimeError::conflict(format!(
                    "memory source `{}` returned an empty, duplicate, or oversized record id",
                    self.source.id()
                )));
            }
            if content_chars == 0 || content_chars > MAX_MEMORY_RECORD_CHARS {
                return Err(RuntimeError::conflict(format!(
                    "memory record `{}` must contain 1..={MAX_MEMORY_RECORD_CHARS} characters",
                    record.id
                )));
            }
            total_chars = total_chars.saturating_add(content_chars);
            if total_chars > MAX_MEMORY_TOTAL_CHARS {
                return Err(RuntimeError::conflict(format!(
                    "memory source `{}` exceeded the aggregate {MAX_MEMORY_TOTAL_CHARS}-character bound",
                    self.source.id()
                )));
            }
            let cache_class = if record.sensitivity == Sensitivity::Secret {
                CacheClass::NoCache
            } else {
                CacheClass::Ephemeral
            };
            fragments.push(
                ContextFragment::new(
                    format!("harness:memory:{}:{}", self.source.id(), record.id),
                    FragmentKind::Memory,
                    FragmentSource::Host,
                    record.revision,
                    FragmentContent::Text(record.content),
                )
                .optional()
                .with_priority(record.priority)
                .with_position(ContextPosition::new(
                    ContextLane::Memory,
                    2_000 + index as u64,
                ))
                .with_cache_class(cache_class)
                .with_sensitivity(record.sensitivity),
            );
        }
        Ok(ContextPatch::new(fragments))
    }
}

#[cfg(test)]
mod tests {
    use agent_runtime_core::content::Message;
    use agent_runtime_core::ids::{SessionId, TurnId};
    use agent_runtime_registry::Fingerprint;

    use super::*;

    #[derive(Debug)]
    struct FixedMemory {
        records: Vec<MemoryRecord>,
    }

    #[async_trait]
    impl MemorySource for FixedMemory {
        fn id(&self) -> &str {
            "fixed"
        }

        fn revision(&self) -> RegistryRevision {
            RegistryRevision::new("fixed-v1")
        }

        async fn retrieve(&self, _query: &MemoryQuery) -> Result<Vec<MemoryRecord>, RuntimeError> {
            Ok(self.records.clone())
        }
    }

    fn view() -> ContextView {
        ContextView {
            session: SessionId::new("s"),
            turn: TurnId::new("t"),
            history: Arc::from(vec![Message::user("continue")]),
            activation: Fingerprint::of("activation"),
            state: None,
        }
    }

    #[tokio::test]
    async fn records_stay_out_of_history_and_keep_sensitivity() {
        let contributor = MemoryContributor::new(Arc::new(FixedMemory {
            records: vec![MemoryRecord {
                id: "preference".into(),
                revision: RegistryRevision::new("r1"),
                content: "Prefer focused tests.".into(),
                sensitivity: Sensitivity::Sensitive,
                priority: 2,
            }],
        }))
        .unwrap();
        let patch = contributor.contribute(&view()).await.unwrap();
        assert_eq!(patch.fragments.len(), 1);
        assert_eq!(patch.fragments[0].kind, FragmentKind::Memory);
        assert_eq!(patch.fragments[0].sensitivity, Sensitivity::Sensitive);
        assert!(patch.fragments[0].requirement == agent_runtime_context::Requirement::Optional);
    }

    #[tokio::test]
    async fn duplicate_ids_fail_closed() {
        let record = MemoryRecord {
            id: "same".into(),
            revision: RegistryRevision::new("r1"),
            content: "bounded".into(),
            sensitivity: Sensitivity::Public,
            priority: 0,
        };
        let contributor = MemoryContributor::new(Arc::new(FixedMemory {
            records: vec![record.clone(), record],
        }))
        .unwrap();
        assert!(contributor.contribute(&view()).await.is_err());
    }
}
