//! Cancellation conformance.

use futures_util::StreamExt;

use agent_runtime::runtime::SessionHandle;
use agent_runtime_core::cancel::CancelReason;
use agent_runtime_core::content::UserInput;
use agent_runtime_core::event::{RuntimeEvent, TurnFinish};

/// Starts a turn (against a blocking provider), cancels it once it is running,
/// and asserts the turn terminates with a cancelled `TurnCompleted`.
pub async fn assert_cancel_terminates(session: &SessionHandle) {
    let mut stream = session.subscribe();
    let _turn = session.send(UserInput::text("go")).unwrap();

    let mut cancel_issued = false;
    while let Some(env) = stream.next().await {
        match &env.payload {
            RuntimeEvent::TextDelta { .. } | RuntimeEvent::ProviderAttemptStarted { .. }
                if !cancel_issued =>
            {
                session
                    .interrupt_current_turn(CancelReason::UserRequested)
                    .expect("the running turn has a cancellation handle");
                cancel_issued = true;
            }
            RuntimeEvent::TurnCompleted { finish, .. } => {
                assert!(
                    matches!(finish, TurnFinish::Cancelled { .. }),
                    "expected a cancelled turn, got {finish:?}"
                );
                return;
            }
            _ => {}
        }
    }
    panic!("turn never completed after cancellation");
}
