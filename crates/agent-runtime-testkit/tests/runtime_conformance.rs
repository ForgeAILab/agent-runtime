//! End-to-end runtime conformance against the spec scenarios.

include!("runtime_conformance/support.rs");

#[path = "runtime_conformance/interaction.rs"]
mod interaction;
#[path = "runtime_conformance/local_action.rs"]
mod local_action;
#[path = "runtime_conformance/provider_loop.rs"]
mod provider_loop;
#[path = "runtime_conformance/recovery.rs"]
mod recovery;
#[path = "runtime_conformance/session.rs"]
mod session;
