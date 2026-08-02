//! Smith adapter contract suite (cross-consumer compatibility gate).

use std::sync::Arc;

use serde_json::json;

use agent_runtime::prelude::*;
use agent_runtime_testkit::conformance::{event_schema, runtime as rt};
use agent_runtime_testkit::{RecordingObserver, consumers, scenarios};

#[tokio::test]
async fn smith_adapter_passes_shared_conformance() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::fake_tool_then_text(
        "echo",
        &json!({"x": 1}),
        "done",
    ));
    let runtime = consumers::smith::build(provider, observer.clone()).expect("smith runtime");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hi")).await.unwrap();

    let payloads = observer.payloads();
    rt::assert_terminates(&payloads);
    assert!(rt::has_tool_completed(&payloads, "echo"));
    event_schema::assert_versioned_and_roundtrips(&observer.events());
}

#[tokio::test]
async fn smith_adapter_consumes_typed_steering_without_a_future_turn() {
    let observer = RecordingObserver::shared();
    let provider = Arc::new(scenarios::SteeringBarrierProvider::new(vec![
        scenarios::stop_events("first"),
        scenarios::stop_events("second"),
    ]));
    let runtime = consumers::smith::build(provider.clone(), observer.clone()).expect("runtime");
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    let turn = session.send(UserInput::text("initial")).unwrap();
    provider.wait_for_first_request().await;
    let receipt = session
        .steer_current_turn(Some(turn.id()), UserInput::text("real user correction"))
        .expect("steer");
    provider.release_first();
    turn.completed().await;

    let events = observer.events();
    let committed = events
        .iter()
        .position(|event| {
            matches!(
                &event.payload,
                RuntimeEvent::TurnSteerCommitted { steer, .. } if steer == &receipt.id
            )
        })
        .expect("committed disposition");
    let terminal = events
        .iter()
        .position(|event| matches!(event.payload, RuntimeEvent::TurnCompleted { .. }))
        .expect("terminal");
    assert!(committed < terminal);
    assert!(events.iter().all(|event| {
        !serde_json::to_string(event)
            .expect("event")
            .contains("real user correction")
    }));
}
