//! Deterministic test infrastructure and conformance suites for the shared
//! agent runtime.
//!
//! `agent-runtime-testkit` provides a controllable clock, an event recorder, a
//! deterministic workspace, in-memory stores, neutral tools, a replay HTTP
//! transport, shared fake-provider scenarios, and reusable conformance suites
//! that any provider, tool, or runtime composition can be checked against. It
//! also ships neutral consumer adapter fixtures (Smith, Nyx, Open Forge) that
//! exercise the public contracts without importing any consumer-domain types.
#![forbid(unsafe_code)]

pub mod clock;
pub mod conformance;
pub mod consumers;
pub mod recorder;
pub mod scenarios;
pub mod stores;
pub mod tools;
pub mod transport;
pub mod workspace;

pub use clock::ManualClock;
pub use recorder::RecordingObserver;
pub use stores::{InMemoryCheckpointStore, InMemorySecretStore, InMemorySessionStore};
pub use transport::ReplayTransport;
pub use workspace::MemoryWorkspace;
