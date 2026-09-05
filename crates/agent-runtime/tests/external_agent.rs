//! Conformance for turns executed by an external agent backend.
//!
//! These drive the real runtime with a scripted backend, so they assert the
//! canonical event stream and history a host actually observes — not the
//! backend's own output.

#![cfg(feature = "external-agent")]

use std::sync::{Arc, Mutex};

use agent_runtime::agent::external::{
    ExternalAgentBackend, ExternalAgentEvent, ExternalSessionId, ExternalTurnRequest,
    ExternalTurnStream,
};
use agent_runtime::provider::fake::FakeProvider;
use agent_runtime::runtime::{RuntimeBuilder, StartSession};
use agent_runtime_core::catalog::{ModelLimits, ResolvedModelProfile};
use agent_runtime_core::content::{ContentPart, UserInput};
use agent_runtime_core::error::RuntimeError;
use agent_runtime_core::event::{RuntimeEvent, TurnFinish};
use agent_runtime_core::provider::{Capabilities, ModelId};
use agent_runtime_core::usage::{CounterKind, UsageDelta, UsageSource};
use async_trait::async_trait;
use futures_util::StreamExt;

/// Replays a fixed event script and records what it was offered for resume.
#[derive(Debug)]
struct ScriptedBackend {
    script: Mutex<Vec<Vec<ExternalAgentEvent>>>,
    offered: Mutex<Vec<Option<String>>>,
}

impl ScriptedBackend {
    fn new(turns: Vec<Vec<ExternalAgentEvent>>) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(turns),
            offered: Mutex::new(Vec::new()),
        })
    }

    fn offered(&self) -> Vec<Option<String>> {
        self.offered.lock().expect("offered poisoned").clone()
    }
}

#[async_trait]
impl ExternalAgentBackend for ScriptedBackend {
    async fn run_turn(
        &self,
        request: ExternalTurnRequest,
    ) -> Result<ExternalTurnStream, RuntimeError> {
        self.offered
            .lock()
            .expect("offered poisoned")
            .push(request.resume.map(|id| id.as_str().to_owned()));
        let mut script = self.script.lock().expect("script poisoned");
        let events = if script.is_empty() {
            Vec::new()
        } else {
            script.remove(0)
        };
        Ok(Box::pin(futures_util::stream::iter(events)))
    }
}

async fn runtime_with(backend: Arc<ScriptedBackend>) -> agent_runtime::runtime::Runtime {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        Vec::new(),
    ));
    // The runtime still resolves model identity and limits even though the
    // provider is never called to produce a turn.
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .external_agent(backend)
        .build()
        .expect("a runtime with an external agent backend")
}

fn session_id(value: &str) -> ExternalSessionId {
    ExternalSessionId::new(value).expect("a bounded identity")
}

#[tokio::test]
async fn an_external_turn_streams_its_work_and_commits_one_assistant_message() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::SessionStarted {
            session: session_id("thread-1"),
        },
        ExternalAgentEvent::Text {
            text: "reading the file".to_owned(),
        },
        ExternalAgentEvent::ToolInvoked {
            id: "call-1".to_owned(),
            name: "Read".to_owned(),
            detail: serde_json::json!({"file_path": "config.txt"}),
        },
        ExternalAgentEvent::ToolCompleted {
            id: "call-1".to_owned(),
            ok: true,
            detail: serde_json::json!({"exit_code": 0}),
        },
        ExternalAgentEvent::Text {
            text: " — the version is 3".to_owned(),
        },
        ExternalAgentEvent::Usage {
            usage: UsageDelta::new()
                .with(CounterKind::InputUncached, 21)
                .with(CounterKind::InputCached, 12)
                .with(CounterKind::Output, 5),
        },
        ExternalAgentEvent::Completed,
    ]]);

    let runtime = runtime_with(backend.clone()).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");
    let mut events = session.subscribe();

    session
        .run(UserInput::text("what version is it?"))
        .await
        .expect("the external turn completes");

    let mut seen = Vec::new();
    while let Some(envelope) = events.next().await {
        let terminal = matches!(envelope.payload, RuntimeEvent::TurnCompleted { .. });
        seen.push(envelope.payload);
        if terminal {
            break;
        }
    }

    // The agent's work is visible as it happens, not as one lump at the end.
    assert!(seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::ExternalSessionStarted { session } if session == "thread-1"
    )));
    assert!(seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::ExternalToolInvoked { name, .. } if name == "Read"
    )));
    assert!(
        seen.iter()
            .any(|event| matches!(event, RuntimeEvent::ExternalToolCompleted { ok, .. } if *ok))
    );
    let text: String = seen
        .iter()
        .filter_map(|event| match event {
            RuntimeEvent::ExternalText { text } => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "reading the file — the version is 3");

    // A turn that called no provider must not claim it did.
    assert!(!seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::ProviderAttemptStarted { .. }
            | RuntimeEvent::ContextPlanned { .. }
            | RuntimeEvent::ToolCallRequested { .. }
    )));

    assert!(seen.iter().any(|event| matches!(
        event,
        RuntimeEvent::TurnCompleted {
            finish: TurnFinish::Completed,
            visible_output: true
        }
    )));

    // Exactly one assistant message, carrying the accumulated text.
    let snapshot = session.snapshot();
    let assistant: Vec<_> = snapshot
        .history
        .iter()
        .filter(|message| matches!(message.role, agent_runtime_core::content::Role::Assistant))
        .collect();
    assert_eq!(assistant.len(), 1);
    assert!(assistant[0].content.iter().any(|part| matches!(
        part,
        ContentPart::Text { text } if text == "reading the file — the version is 3"
    )));

    // Usage is attributed to the external agent, not to a provider attempt.
    let records = snapshot.usage.records();
    let external: Vec<_> = records
        .iter()
        .filter(|record| record.source == UsageSource::ExternalAgent)
        .collect();
    assert_eq!(external.len(), 1);
    assert_eq!(external[0].delta.get(CounterKind::InputCached), 12);
    assert!(
        !records
            .iter()
            .any(|record| record.source == UsageSource::ProviderAttempt)
    );

    session.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn a_failed_external_turn_leaves_no_partial_answer_in_history() {
    let backend = ScriptedBackend::new(vec![vec![
        ExternalAgentEvent::Text {
            text: "I started to answer".to_owned(),
        },
        ExternalAgentEvent::Failed {
            message: "upstream agent exited".to_owned(),
        },
    ]]);

    let runtime = runtime_with(backend).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");

    session
        .run(UserInput::text("do the thing"))
        .await
        .expect("the turn reaches a terminal state");

    let snapshot = session.snapshot();
    assert!(
        !snapshot
            .history
            .iter()
            .any(|message| matches!(message.role, agent_runtime_core::content::Role::Assistant)),
        "a failed turn must not leave a partial answer posing as the agent's response"
    );

    session.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn a_stream_without_a_terminal_event_fails_rather_than_passing_for_success() {
    // No terminal event at all: a broken backend contract.
    let backend = ScriptedBackend::new(vec![vec![ExternalAgentEvent::Text {
        text: "truncated".to_owned(),
    }]]);

    let runtime = runtime_with(backend).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");
    let mut events = session.subscribe();

    session
        .run(UserInput::text("do the thing"))
        .await
        .expect("the turn reaches a terminal state");

    let mut finish = None;
    while let Some(envelope) = events.next().await {
        if let RuntimeEvent::TurnCompleted { finish: seen, .. } = envelope.payload {
            finish = Some(seen);
            break;
        }
    }
    assert_eq!(finish, Some(TurnFinish::Failed));

    session.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn the_next_turn_is_offered_the_identity_the_backend_reported() {
    let backend = ScriptedBackend::new(vec![
        vec![
            ExternalAgentEvent::SessionStarted {
                session: session_id("thread-1"),
            },
            ExternalAgentEvent::Text {
                text: "first".to_owned(),
            },
            ExternalAgentEvent::Completed,
        ],
        // The backend could not resume, and says so by reporting a new id.
        vec![
            ExternalAgentEvent::SessionStarted {
                session: session_id("thread-2"),
            },
            ExternalAgentEvent::Text {
                text: "second".to_owned(),
            },
            ExternalAgentEvent::Completed,
        ],
        vec![
            ExternalAgentEvent::Text {
                text: "third".to_owned(),
            },
            ExternalAgentEvent::Completed,
        ],
    ]);

    let runtime = runtime_with(backend.clone()).await;
    let session = runtime
        .start_session(StartSession::new())
        .await
        .expect("a session");

    for prompt in ["one", "two", "three"] {
        session
            .run(UserInput::text(prompt))
            .await
            .expect("the external turn completes");
    }

    assert_eq!(
        backend.offered(),
        vec![
            // Nothing stored yet.
            None,
            // The first turn's identity.
            Some("thread-1".to_owned()),
            // Replaced, because the backend reported a different one.
            Some("thread-2".to_owned()),
        ]
    );

    session.shutdown().await.expect("a clean shutdown");
}
