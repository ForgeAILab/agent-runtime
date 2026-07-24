//! Bounded-shutdown conformance.

use futures_util::StreamExt;

use agent_runtime::runtime::SessionHandle;
use agent_runtime_core::content::UserInput;
use agent_runtime_core::event::RuntimeEvent;

/// Starts a turn against a blocking provider, then shuts the session down and
/// asserts shutdown completes (bounded) and emits a terminal `SessionShutdown`.
pub async fn assert_bounded_shutdown(session: &SessionHandle) {
    let mut stream = session.subscribe();
    let _turn = session.send(UserInput::text("go"));

    // Ensure the turn is actually running before shutting down.
    let _first = stream.next().await;

    session.shutdown().await.expect("shutdown succeeds");

    // The terminal SessionShutdown must arrive.
    while let Some(env) = stream.next().await {
        if matches!(env.payload, RuntimeEvent::SessionShutdown) {
            return;
        }
    }
    panic!("shutdown did not emit SessionShutdown");
}
