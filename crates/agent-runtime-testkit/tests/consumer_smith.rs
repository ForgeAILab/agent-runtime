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
