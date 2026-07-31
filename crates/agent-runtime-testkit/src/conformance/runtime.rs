//! Runtime conformance: driving a session and inspecting the canonical event
//! sequence.

use futures_util::StreamExt;

use agent_runtime::runtime::SessionHandle;
use agent_runtime_core::content::UserInput;
use agent_runtime_core::event::RuntimeEvent;

/// Sends `input` and collects the canonical event payloads for the turn, ending
/// at the first `TurnCompleted`.
pub async fn run_turn_collect(session: &SessionHandle, input: UserInput) -> Vec<RuntimeEvent> {
    let mut stream = session.subscribe();
    let _turn = session.send(input).unwrap();
    let mut payloads = Vec::new();
    while let Some(env) = stream.next().await {
        let is_end = matches!(env.payload, RuntimeEvent::TurnCompleted { .. });
        payloads.push(env.payload);
        if is_end {
            break;
        }
    }
    payloads
}

/// Asserts the payload sequence ends in a `TurnCompleted`.
pub fn assert_terminates(payloads: &[RuntimeEvent]) {
    assert!(
        matches!(payloads.last(), Some(RuntimeEvent::TurnCompleted { .. })),
        "turn must terminate with TurnCompleted; got {:?}",
        payloads.last()
    );
}

/// The number of `ToolCallRequested` events in a payload sequence.
pub fn count_tool_requests(payloads: &[RuntimeEvent]) -> usize {
    payloads
        .iter()
        .filter(|e| matches!(e, RuntimeEvent::ToolCallRequested { .. }))
        .count()
}

/// Whether the sequence contains a `ToolCallCompleted` for `name`.
pub fn has_tool_completed(payloads: &[RuntimeEvent], name: &str) -> bool {
    payloads
        .iter()
        .any(|e| matches!(e, RuntimeEvent::ToolCallCompleted { name: n, .. } if n == name))
}
