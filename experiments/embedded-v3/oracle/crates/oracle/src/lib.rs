//! Candidate-neutral oracle for the embedded runtime value experiment.
//!
//! The oracle owns transport timestamps, validates every protocol transition,
//! and writes the exact NDJSON exchanged with a candidate. Candidate adapters
//! are deliberately not dependencies of this workspace.

pub mod analysis;
pub mod authority_harness;
pub mod automaton;
pub mod evidence;
pub mod full_evidence;
pub mod full_runner;
pub mod measurement;
pub mod protocol;
pub mod sampling;
mod state_oracle;
pub mod transport;
mod wire_bounds;
pub mod workload;

pub use automaton::{Oracle, OracleError, RequestPhase};
pub use protocol::*;
pub use transport::{
    CandidateProcess, OracleSession, ReceivedEvent, SentControl, SessionError, TimeoutTable,
    TransportError,
};
