mod authority;
mod guest;
#[allow(dead_code)]
mod protocol;
mod runtime;

use std::collections::HashSet;
use std::env;
use std::io::{self, BufRead};
use std::process;

use anyhow::{bail, Context, Result};

use protocol::*;
use runtime::{Emitter, Runtime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Session {
    New,
    Greeted,
    Running,
    Shutdown,
}

fn run() -> Result<()> {
    let emitter = Emitter::stdout();
    let artifact_root = env::current_dir()?.join("guest-artifacts");
    let runtime = Runtime::new(emitter.clone(), artifact_root);
    let stdin = io::stdin();
    let mut session = Session::New;
    let mut requests = HashSet::new();
    for raw in stdin.lock().lines() {
        let raw = raw.context("read control line")?;
        let envelope = match decode_control_line(&raw) {
            Ok(envelope) => envelope,
            Err(error) => {
                emitter.fatal(RequestId(0), "invalid_control", format!("{error:#}"));
                bail!("invalid control line");
            }
        };
        validate_control_envelope(&envelope)
            .map_err(|error| anyhow::anyhow!("control validation failed: {error}"))?;
        if !requests.insert(envelope.request_id.0) {
            emitter.fatal(
                envelope.request_id,
                "request_reuse",
                "request_id was already used",
            );
            bail!("request_id reuse");
        }
        let request_id = envelope.request_id;
        let result = match (session, envelope.message) {
            (Session::New, ControlMessage::Hello(control)) => {
                if control.expected_candidate != CandidateKind::Extism {
                    bail!("oracle expected another candidate");
                }
                emitter.emit(
                    request_id,
                    EventMessage::Hello(HelloEvent {
                        candidate: CandidateKind::Extism,
                        implementation_version: "extism-1.30.0-embedded-v3".into(),
                    }),
                )?;
                session = Session::Greeted;
                Ok(())
            }
            (Session::Greeted, ControlMessage::Init(control)) => {
                runtime.initialize(request_id, control)?;
                session = Session::Running;
                Ok(())
            }
            (Session::Running, ControlMessage::CreateTenant(control)) => {
                runtime.create_tenant(request_id, control);
                Ok(())
            }
            (Session::Running, ControlMessage::Command(control)) => {
                runtime.command(request_id, control)
            }
            (Session::Running, ControlMessage::InjectFault(control)) => {
                runtime.inject_fault(request_id, control)
            }
            (Session::Running, ControlMessage::Rollout(control)) => {
                runtime.rollout(request_id, control)
            }
            (Session::Running, ControlMessage::Snapshot(control)) => {
                runtime.snapshot(request_id, control)
            }
            (Session::Running, ControlMessage::SetDequeueGate(control)) => {
                runtime.set_gate(request_id, control)
            }
            (Session::Running, ControlMessage::AuthorityProbe(control)) => {
                runtime.authority_probe(request_id, control)
            }
            (Session::Running, ControlMessage::Quiesce(_)) => runtime.quiesce(request_id),
            (Session::Running, ControlMessage::TeardownTenant(control)) => {
                runtime.teardown(request_id, control)
            }
            (Session::Running, ControlMessage::Shutdown(_)) => {
                runtime.shutdown(request_id)?;
                session = Session::Shutdown;
                Ok(())
            }
            (_, message) => bail!("control {message:?} is invalid in session {session:?}"),
        };
        if let Err(error) = result {
            emitter.fatal(request_id, "control_failed", format!("{error:#}"));
            return Err(error);
        }
        if session == Session::Shutdown {
            return Ok(());
        }
    }
    bail!("stdin closed before shutdown")
}

fn main() {
    if let Err(error) = run() {
        eprintln!("embedded-v3 Extism candidate failed: {error:#}");
        process::exit(1);
    }
}
