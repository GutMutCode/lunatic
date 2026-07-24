use std::collections::HashMap;
use std::env;
use std::io::{self, BufRead};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};

use embedded_v3_direct_wasmtime::protocol::{
    AuthorityProbeAcceptedEvent, AuthorityProbeErrorClass, AuthorityProbeResult,
    AuthorityProbeStage, AuthorityProbeTerminalEvent, CandidateKind, CommandRejectedEvent,
    ControlEnvelope, ControlMessage, EventMessage, InitializedEvent, RejectionReason, RequestId,
    RolloutCommittedEvent, RolloutInDoubtEvent, RolloutRejectedEvent, RolloutRolledBackEvent,
    RolloutStartedEvent, SnapshotMissingEvent, EMPTY_SHA256,
};
use embedded_v3_direct_wasmtime::runtime::{
    authority_absent_at_link, implementation_hello, EventSink, Identity, SharedRuntime,
};
use embedded_v3_direct_wasmtime::tenant::TenantHandle;
use embedded_v3_direct_wasmtime::{protocol, runtime};

const MAX_NDJSON_LINE_BYTES: usize = 1_048_576;

struct Candidate {
    sink: EventSink,
    runtime: Option<Arc<SharedRuntime>>,
    tenants: Arc<Mutex<HashMap<u32, TenantHandle>>>,
    greeted: bool,
    initialized: bool,
}

impl Candidate {
    fn new(sink: EventSink) -> Self {
        Self {
            sink,
            runtime: None,
            tenants: Arc::new(Mutex::new(HashMap::new())),
            greeted: false,
            initialized: false,
        }
    }

    fn handle(&mut self, envelope: ControlEnvelope) -> Result<bool> {
        let request_id = envelope.request_id;
        match envelope.message {
            ControlMessage::Hello(control) => {
                if self.greeted || control.expected_candidate != CandidateKind::RawWasmtime {
                    return Err(anyhow!("invalid hello or candidate identity"));
                }
                self.sink.emit(request_id, implementation_hello())?;
                self.greeted = true;
            }
            ControlMessage::Init(control) => {
                if !self.greeted || self.initialized {
                    return Err(anyhow!("init is out of session order"));
                }
                let artifact_root = env::current_dir()
                    .context("resolve candidate process current directory")?
                    .join("guest-artifacts");
                let runtime = SharedRuntime::initialize(&artifact_root)?;
                self.runtime = Some(runtime);
                self.initialized = true;
                self.sink.emit(
                    request_id,
                    EventMessage::Initialized(InitializedEvent {
                        run_id: control.run_id,
                    }),
                )?;
            }
            ControlMessage::CreateTenant(control) => {
                let runtime = self.running_runtime()?;
                let artifact = runtime.catalog.by_sha(&control.artifact_sha256)?;
                let identity = Identity::new(
                    control.logical_version,
                    control.build_id,
                    control.artifact_sha256,
                    &artifact,
                );
                let handle = TenantHandle::spawn(
                    request_id,
                    control.tenant_id,
                    control.incarnation,
                    identity,
                    artifact,
                    runtime,
                    self.sink.clone(),
                );
                let mut tenants = self.tenants()?;
                if tenants.insert(control.tenant_id.0, handle).is_some() {
                    return Err(anyhow!("create targeted an active tenant"));
                }
            }
            ControlMessage::Command(control) => {
                self.require_running()?;
                let handle = self.tenants()?.get(&control.tenant_id.0).cloned();
                match handle {
                    Some(handle) => handle.admit(request_id, control, &self.sink)?,
                    None => self.sink.emit(
                        request_id,
                        EventMessage::CommandRejected(CommandRejectedEvent {
                            tenant_id: control.tenant_id,
                            incarnation: control.incarnation,
                            command_id: control.command_id,
                            reason: RejectionReason::TenantMissing,
                        }),
                    )?,
                }
            }
            ControlMessage::InjectFault(control) => {
                let runtime = self.running_runtime()?;
                let artifact = runtime
                    .catalog
                    .by_sha(&control.replacement_artifact_sha256)?;
                let identity = Identity::new(
                    control.replacement_logical_version.clone(),
                    control.replacement_build_id.clone(),
                    control.replacement_artifact_sha256.clone(),
                    &artifact,
                );
                let handle = self
                    .tenants()?
                    .get(&control.tenant_id.0)
                    .cloned()
                    .ok_or_else(|| anyhow!("fault target missing"))?;
                handle.inject_fault(request_id, control, artifact, identity)?;
            }
            ControlMessage::Rollout(control) => self.rollout(request_id, control)?,
            ControlMessage::Snapshot(control) => {
                self.require_running()?;
                let handle = self.tenants()?.get(&control.tenant_id.0).cloned();
                match handle {
                    Some(handle) => handle.snapshot(request_id, control)?,
                    None => self.sink.emit(
                        request_id,
                        EventMessage::SnapshotMissing(SnapshotMissingEvent {
                            tenant_id: control.tenant_id,
                        }),
                    )?,
                }
            }
            ControlMessage::SetDequeueGate(control) => {
                self.require_running()?;
                self.tenants()?
                    .get(&control.tenant_id.0)
                    .cloned()
                    .ok_or_else(|| anyhow!("dequeue gate target missing"))?
                    .set_gate(request_id, control)?;
            }
            ControlMessage::AuthorityProbe(control) => {
                let runtime = self.running_runtime()?;
                self.sink.emit(
                    request_id,
                    EventMessage::AuthorityProbeAccepted(AuthorityProbeAcceptedEvent {
                        attempt_id: control.attempt_id,
                        probe: control.probe,
                        artifact_sha256: control.artifact_sha256.clone(),
                        parameter_sha256: control.parameter_sha256.clone(),
                        production_policy_sha256: control.production_policy_sha256.clone(),
                    }),
                )?;
                let sink = self.sink.clone();
                tokio::spawn(async move {
                    let result =
                        authority_absent_at_link(&runtime, control.probe, &control.artifact_bytes);
                    let message = match result {
                        Ok(()) => {
                            EventMessage::AuthorityProbeTerminal(AuthorityProbeTerminalEvent {
                                attempt_id: control.attempt_id,
                                probe: control.probe,
                                artifact_sha256: control.artifact_sha256,
                                parameter_sha256: control.parameter_sha256,
                                production_policy_sha256: control.production_policy_sha256,
                                stage: AuthorityProbeStage::Instantiate,
                                result: AuthorityProbeResult::AbsentAtLink,
                                error_class: AuthorityProbeErrorClass::UnknownImport,
                                guest_return_sha256: EMPTY_SHA256.into(),
                            })
                        }
                        Err(error) => EventMessage::Fatal(protocol::FatalEvent {
                            code: "authority_probe_invalid".into(),
                            message: error.to_string(),
                        }),
                    };
                    let _ = sink.emit(request_id, message);
                });
            }
            ControlMessage::Quiesce(_) => {
                self.require_running()?;
                let tenants = self.tenants.clone();
                let sink = self.sink.clone();
                tokio::spawn(async move {
                    loop {
                        let quiescent = tenants
                            .lock()
                            .ok()
                            .map(|tenants| {
                                tenants
                                    .values()
                                    .all(|tenant| tenant.is_quiescent().unwrap_or(false))
                            })
                            .unwrap_or(false);
                        if quiescent {
                            let _ = sink.emit(
                                request_id,
                                EventMessage::Quiesced(protocol::QuiescedEvent {}),
                            );
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                });
            }
            ControlMessage::TeardownTenant(control) => {
                self.require_running()?;
                let handle = self
                    .tenants()?
                    .remove(&control.tenant_id.0)
                    .ok_or_else(|| anyhow!("teardown target missing"))?;
                handle.teardown(request_id, control.incarnation)?;
            }
            ControlMessage::Shutdown(_) => {
                self.require_running()?;
                if !self.tenants()?.is_empty() {
                    return Err(anyhow!("shutdown requires zero active tenants"));
                }
                self.sink.emit(
                    request_id,
                    EventMessage::ShutdownComplete(protocol::ShutdownCompleteEvent {}),
                )?;
                if let Some(runtime) = &self.runtime {
                    runtime.stop_ticker()?;
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn rollout(&self, request_id: RequestId, control: protocol::RolloutControl) -> Result<()> {
        let runtime = self.running_runtime()?;
        let artifact = match runtime
            .catalog
            .rollout(&control.artifact_sha256, &control.artifact_ref)
        {
            Ok(artifact) => artifact,
            Err(error) => {
                return self.sink.emit(
                    request_id,
                    EventMessage::RolloutRejected(RolloutRejectedEvent {
                        rollout_id: control.rollout_id,
                        reason: error.to_string(),
                    }),
                );
            }
        };
        let handles = {
            let tenants = self.tenants()?;
            let mut handles = Vec::with_capacity(control.targets.len());
            for target in &control.targets {
                let Some(handle) = tenants.get(&target.0).cloned() else {
                    return self.sink.emit(
                        request_id,
                        EventMessage::RolloutRejected(RolloutRejectedEvent {
                            rollout_id: control.rollout_id,
                            reason: format!("rollout target {} missing", target.0),
                        }),
                    );
                };
                let on_from_version = handle
                    .admission
                    .lock()
                    .map_err(|_| anyhow!("tenant admission lock poisoned"))?
                    .identity
                    .logical_version
                    == control.from_version;
                if !on_from_version {
                    return self.sink.emit(
                        request_id,
                        EventMessage::RolloutRejected(RolloutRejectedEvent {
                            rollout_id: control.rollout_id,
                            reason: format!("tenant {} is not on from_version", target.0),
                        }),
                    );
                }
                handles.push(handle);
            }
            handles
        };
        let identity = Identity::new(
            control.to_version.clone(),
            control.to_build_id.clone(),
            control.artifact_sha256.clone(),
            &artifact,
        );
        self.sink.emit(
            request_id,
            EventMessage::RolloutStarted(RolloutStartedEvent {
                rollout_id: control.rollout_id.clone(),
            }),
        )?;
        let sink = self.sink.clone();
        tokio::spawn(async move {
            let result =
                coordinate_rollout(request_id, &control, &handles, artifact, identity, &sink).await;
            if let Err(error) = result {
                let _ = sink.emit(
                    request_id,
                    EventMessage::RolloutInDoubt(RolloutInDoubtEvent {
                        rollout_id: control.rollout_id,
                        reason: error.to_string(),
                    }),
                );
            }
        });
        Ok(())
    }

    fn require_running(&self) -> Result<()> {
        if self.initialized {
            Ok(())
        } else {
            Err(anyhow!("candidate is not initialized"))
        }
    }

    fn running_runtime(&self) -> Result<Arc<SharedRuntime>> {
        self.require_running()?;
        self.runtime
            .clone()
            .ok_or_else(|| anyhow!("shared runtime missing"))
    }

    fn tenants(&self) -> Result<std::sync::MutexGuard<'_, HashMap<u32, TenantHandle>>> {
        self.tenants
            .lock()
            .map_err(|_| anyhow!("tenant map lock poisoned"))
    }
}

async fn coordinate_rollout(
    request_id: RequestId,
    control: &protocol::RolloutControl,
    handles: &[TenantHandle],
    artifact: runtime::Artifact,
    identity: Identity,
    sink: &EventSink,
) -> Result<()> {
    let mut preparations = Vec::with_capacity(handles.len());
    for handle in handles {
        preparations.push(handle.prepare_rollout(
            request_id,
            control.clone(),
            artifact.clone(),
            identity.clone(),
        )?);
    }
    let mut failed = false;
    let mut failure_reasons = Vec::new();
    for receiver in preparations {
        let result = receiver.await.context("rollout prepare reply dropped")?;
        failed |= !result.success;
        if !result.success {
            failure_reasons.push(result.reason);
        }
    }
    let mut finishes = Vec::with_capacity(handles.len());
    for handle in handles {
        finishes.push(handle.finish_rollout(!failed)?);
    }
    let mut finish_error = None;
    for receiver in finishes {
        match receiver.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => finish_error = Some(error.to_string()),
            Err(error) => finish_error = Some(error.to_string()),
        }
    }
    if let Some(error) = finish_error {
        return Err(anyhow!("rollout settlement failed: {error}"));
    }
    if failed {
        sink.emit(
            request_id,
            EventMessage::RolloutRolledBack(RolloutRolledBackEvent {
                rollout_id: control.rollout_id.clone(),
                restored_version: control.from_version.clone(),
                reason: if failure_reasons.is_empty() {
                    "activation_failed".into()
                } else {
                    "activation_failed_at_target".into()
                },
            }),
        )
    } else {
        sink.emit(
            request_id,
            EventMessage::RolloutCommitted(RolloutCommittedEvent {
                rollout_id: control.rollout_id.clone(),
                version: control.to_version.clone(),
            }),
        )
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let sink = EventSink::stdout();
    let mut candidate = Candidate::new(sink.clone());
    let stdin = io::stdin();
    let mut input = stdin.lock();
    while let Some(line) = read_bounded_record(&mut input)? {
        let envelope = protocol::decode_control_line(&line).context("decode control line")?;
        let request_id = envelope.request_id;
        match candidate.handle(envelope) {
            Ok(shutdown) if shutdown => return Ok(()),
            Ok(_) => {}
            Err(error) => {
                let _ = sink.emit(
                    request_id,
                    EventMessage::Fatal(protocol::FatalEvent {
                        code: "candidate_contract_error".into(),
                        message: error.to_string(),
                    }),
                );
                return Err(error);
            }
        }
    }
    Err(anyhow!("stdin closed before shutdown"))
}

fn read_bounded_record<R: BufRead>(reader: &mut R) -> Result<Option<String>> {
    let mut record = Vec::with_capacity(4_096);
    loop {
        let available = reader.fill_buf().context("read candidate control bytes")?;
        if available.is_empty() {
            return if record.is_empty() {
                Ok(None)
            } else {
                Err(anyhow!("stdin ended inside an NDJSON record"))
            };
        }
        if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
            if record.len() + newline > MAX_NDJSON_LINE_BYTES {
                return Err(anyhow!("control line exceeds bounded ingress"));
            }
            record.extend_from_slice(&available[..newline]);
            reader.consume(newline + 1);
            return String::from_utf8(record)
                .map(Some)
                .context("control record is not UTF-8");
        }
        if record.len() + available.len() > MAX_NDJSON_LINE_BYTES {
            return Err(anyhow!("control line exceeds bounded ingress"));
        }
        let consumed = available.len();
        record.extend_from_slice(available);
        reader.consume(consumed);
    }
}
