use super::*;
use agent_runtime_core::event::ModelFallbackReason;

pub(super) const ROUTE_STATE_NAMESPACE: &str = "runtime.core.model_route";

pub(super) fn contains_images(parts: &[ContentPart]) -> bool {
    parts.iter().any(|part| match part {
        ContentPart::Image { .. } => true,
        ContentPart::ToolResult(result) => contains_images(&result.content),
        _ => false,
    })
}

impl TurnMachine {
    pub(super) fn model_fallback_error(&self, mut error: RuntimeError) -> RuntimeError {
        if !self.fallback_failures.is_empty() {
            error.message = format!(
                "model fallback exhausted: {}; {}",
                self.fallback_failures.join("; "),
                error.message
            );
        }
        error
    }
    pub(super) fn emit_primary_model(&self) {
        if self.driver.fallback_routes.is_empty() {
            return;
        }
        let profile = self.driver.planner(&self.execution).profile();
        self.emitter.emit(
            Some(self.turn_id.clone()),
            RuntimeEvent::ModelProfileResolved {
                provider: profile.provider.clone(),
                model: profile.model.clone(),
                profile: profile.fingerprint(),
            },
        );
    }

    pub(super) fn restore_checkpoint_model(mut self) -> Self {
        if self.checkpoint.as_ref().is_some_and(|checkpoint| {
            checkpoint.transition_revision
                == agent_runtime_core::checkpoint::TURN_TRANSITION_REVISION
        }) {
            self.driver.fallback_routes = Arc::default();
            self.driver.first_response_timeout_ms = None;
            return self;
        }
        if self.checkpoint.as_ref().is_some_and(|checkpoint| {
            matches!(
                checkpoint.state,
                TurnState::Completing { .. }
                    | TurnState::PublishingTerminal { .. }
                    | TurnState::Terminal { .. }
            )
        }) {
            return self;
        }
        let Some(record) = self
            .checkpoint
            .as_ref()
            .and_then(|checkpoint| {
                checkpoint
                    .snapshot
                    .extension_state
                    .get(ROUTE_STATE_NAMESPACE)
            })
            .cloned()
        else {
            return self;
        };
        let index = record.value["index"]
            .as_u64()
            .and_then(|index| usize::try_from(index).ok());
        let route = index
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| self.driver.fallback_routes.get(index))
            .cloned();
        let valid = record.revision.as_str() == "model-route-1"
            && record.value["turn"] == serde_json::json!(self.turn_id)
            && route.as_ref().is_some_and(|route| {
                record.value["provider"] == serde_json::json!(route.provider())
                    && record.value["model"] == serde_json::json!(route.model())
                    && record.value["profile"]
                        == serde_json::json!(route.planner.profile().fingerprint())
            });
        if !valid {
            self.model_route_error = Some(RuntimeError::conflict(
                "saved model fallback binding changed; unfinished work was not replayed",
            ));
            return self;
        }
        self.fallback_index = index.expect("validated route index");
        self.fallback_failures =
            serde_json::from_value(record.value["failures"].clone()).unwrap_or_default();
        self.apply_model_route(route.expect("validated route"));
        self
    }

    fn apply_model_route(&mut self, route: crate::runtime::ModelRoute) {
        self.driver.planner(&self.execution).retire_cache_baseline();
        let mut config = (*self.driver.config).clone();
        config.model = route.config.model.clone();
        config.reasoning = route.config.reasoning.clone();
        config.max_output_tokens = route.config.max_output_tokens;
        config.hosted_tools = route.config.hosted_tools.clone();
        config.sampling = route.config.sampling.clone();
        self.driver.config = Arc::new(config);
        self.driver.provider = route.provider;
        self.driver.route_planner =
            Some(Arc::new(route.planner.fork_session(self.emitter.session())));
        self.driver.first_response_timeout_ms = route.first_response_timeout_ms;
    }

    pub(super) fn advance_model(
        &mut self,
        reason: ModelFallbackReason,
        elapsed_ms: u64,
        detail: &str,
    ) -> bool {
        let outgoing = self.driver.planner(&self.execution).profile();
        let from_provider = outgoing.provider.clone();
        let from_model = outgoing.model.clone();
        self.fallback_failures
            .push(format!("{from_provider}/{from_model}: {detail}"));
        let Some(route) = self
            .driver
            .fallback_routes
            .get(self.fallback_index)
            .cloned()
        else {
            return false;
        };
        self.fallback_index += 1;
        let timeout = self.driver.first_response_timeout_ms;
        self.apply_model_route(route);
        let incoming = self.driver.planner(&self.execution).profile();
        let turn = Some(self.turn_id.clone());
        self.emitter.emit(
            turn.clone(),
            RuntimeEvent::ModelFallback {
                from_provider,
                from_model,
                to_provider: incoming.provider.clone(),
                to_model: incoming.model.clone(),
                candidate_index: self.fallback_index,
                total_candidates: self.driver.fallback_routes.len() + 1,
                reason,
                elapsed_ms,
                first_response_timeout_ms: timeout,
            },
        );
        self.emitter.emit(
            turn,
            RuntimeEvent::ModelProfileResolved {
                provider: incoming.provider.clone(),
                model: incoming.model.clone(),
                profile: incoming.fingerprint(),
            },
        );
        true
    }
}

pub(super) struct FirstResponseBudget {
    started: Timestamp,
    monotonic: tokio::time::Instant,
    timeout_ms: Option<u64>,
}

impl FirstResponseBudget {
    pub(super) fn new(clock: &dyn Clock, timeout_ms: Option<u64>) -> Self {
        Self {
            started: clock.now(),
            monotonic: tokio::time::Instant::now(),
            timeout_ms,
        }
    }

    pub(super) fn elapsed(&self, clock: &dyn Clock) -> u64 {
        clock
            .now()
            .as_millis()
            .saturating_sub(self.started.as_millis())
            .max(
                self.monotonic
                    .elapsed()
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            )
    }

    pub(super) fn remaining(&self, clock: &dyn Clock) -> Option<u64> {
        self.timeout_ms
            .map(|ms| ms.saturating_sub(self.elapsed(clock)))
    }

    pub(super) fn disarm(&mut self) {
        self.timeout_ms = None;
    }
}

pub(super) async fn wait_budget(milliseconds: Option<u64>) {
    match milliseconds {
        Some(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        None => pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::fake::{FakeProvider, ScriptedStream};
    use crate::runtime::{RuntimeBuilder, StartSession};
    use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
    use agent_runtime_core::checkpoint::{CheckpointStore, TurnCheckpoint};
    use agent_runtime_core::ids::{AttemptId, SessionId};
    use agent_runtime_core::provider::{Capabilities, ModelId, PromptCacheControl};
    use async_trait::async_trait;

    #[derive(Debug, Default)]
    struct CapturedCheckpoints(Mutex<Vec<TurnCheckpoint>>);

    #[async_trait]
    impl CheckpointStore for CapturedCheckpoints {
        async fn load_latest(
            &self,
            session: &SessionId,
        ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|checkpoint| checkpoint.session == *session)
                .cloned())
        }
        async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
            checkpoint.validate()?;
            let mut records = self.0.lock().unwrap();
            if let Some(previous) = records.last().filter(|previous| {
                previous.session == checkpoint.session && previous.turn == checkpoint.turn
            }) {
                if previous.state_revision > checkpoint.state_revision {
                    return Err(RuntimeError::conflict(
                        "checkpoint revision moved backwards",
                    ));
                }
                if previous.state_revision == checkpoint.state_revision {
                    return if previous.operation_fingerprint == checkpoint.operation_fingerprint {
                        Ok(())
                    } else {
                        Err(RuntimeError::conflict("checkpoint operation changed"))
                    };
                }
            }
            records.push(checkpoint.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn fallback_preserves_the_sessions_cache_registry_in_checkpoints() {
        let profile = |provider, model| {
            ResolvedModelProfile::explicit(
                provider,
                ModelId::new(model),
                ModelLimits::new(32768, 30000, 1024),
            )
        };
        let backup = RuntimeBuilder::new(ModelId::new("secondary"))
            .provider(Arc::new(FakeProvider::text_reply("done")))
            .model_profile(profile("backup", "secondary"))
            .build()
            .unwrap();
        let primary = Arc::new(FakeProvider::new(
            "primary",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![ProviderStreamEvent::Error {
                error: ProviderError::new(ProviderErrorKind::LimitExhausted, "quota"),
            }])],
        ));
        let checkpoints = Arc::new(CapturedCheckpoints::default());
        let runtime = RuntimeBuilder::new(ModelId::new("primary"))
            .provider(primary)
            .model_profile(profile("first", "primary"))
            .checkpoint_store(checkpoints.clone())
            .model_fallbacks(vec![backup.model_route(None)], None)
            .build()
            .unwrap();
        let session = runtime.start_session(StartSession::new()).await.unwrap();
        let identity = CacheIdentity::legacy(
            Fingerprint::of("prior-profile"),
            "first",
            ModelId::new("primary"),
            Vec::new(),
            PromptCacheControl::Implicit,
        );
        runtime
            .cache()
            .record_evidence(
                session.id(),
                CacheAvailabilityEvidence::stream(
                    identity,
                    RequestId::new("prior-request"),
                    AttemptId::new("prior-attempt"),
                    0,
                    Some(4),
                    None,
                ),
            )
            .unwrap();
        let prior = runtime.cache().persisted_session(session.id()).unwrap();
        session.run(UserInput::text("hello")).await.unwrap();
        let saved = checkpoints
            .0
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .snapshot
            .clone();
        assert_eq!(
            saved.extension_state[crate::cache::CACHE_MECHANISM_STATE_NAMESPACE],
            prior
        );
    }
}
