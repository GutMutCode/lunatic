use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, oneshot};

use crate::protocol::{
    CommandAcceptedEvent, CommandCompletedEvent, CommandControl, CommandFailedEvent,
    CommandOperation, CommandRejectedEvent, CommandResult, DequeueGateSetEvent, EventMessage,
    ExecutionFailedEvent, FailureObservedEvent, FaultFailureReason, FaultKind, IncarnationToken,
    InjectFaultControl, RejectionReason, RequestId, RolloutControl, RolloutTargetReadyEvent,
    SetDequeueGateControl, SnapshotControl, SnapshotPresentEvent, SnapshotStaleEvent,
    TenantCreatedEvent, TenantId, TenantReadyEvent, TenantTornDownEvent,
};
use crate::runtime::{
    sha256_hex, Artifact, EventSink, FaultObserver, Guest, Identity, SharedRuntime,
};

pub const MAILBOX_CAPACITY: usize = 64;
const PAYLOAD_LIMIT: usize = 1_024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Fingerprint {
    operation: CommandOperation,
    payload_sha256: String,
}

pub struct AdmissionState {
    pub incarnation: IncarnationToken,
    pub identity: Identity,
    accepting: bool,
    outstanding: usize,
    known: HashMap<u64, Fingerprint>,
}

#[derive(Clone)]
pub struct TenantHandle {
    command_tx: mpsc::Sender<CommandWork>,
    control_tx: mpsc::UnboundedSender<TenantControl>,
    pub admission: Arc<Mutex<AdmissionState>>,
}

struct CommandWork {
    request_id: RequestId,
    control: CommandControl,
}

pub struct PrepareResult {
    pub success: bool,
    pub reason: String,
}

enum TenantControl {
    Gate {
        request_id: RequestId,
        control: SetDequeueGateControl,
    },
    Snapshot {
        request_id: RequestId,
        control: SnapshotControl,
    },
    Fault {
        request_id: RequestId,
        control: InjectFaultControl,
        artifact: Artifact,
        identity: Identity,
    },
    PrepareRollout {
        request_id: RequestId,
        control: RolloutControl,
        artifact: Artifact,
        identity: Identity,
        reply: oneshot::Sender<PrepareResult>,
    },
    FinishRollout {
        commit: bool,
        reply: oneshot::Sender<Result<()>>,
    },
    Teardown {
        request_id: RequestId,
        incarnation: IncarnationToken,
    },
}

struct CachedCommand {
    result: CommandResult,
}

enum PendingRollout {
    Prepared {
        guest: Box<Guest>,
        identity: Identity,
    },
    Failed,
}

struct TenantStart {
    request_id: RequestId,
    tenant_id: TenantId,
    incarnation: IncarnationToken,
    identity: Identity,
    artifact: Artifact,
    runtime: Arc<SharedRuntime>,
    sink: EventSink,
    admission: Arc<Mutex<AdmissionState>>,
}

struct TenantWorker {
    tenant_id: TenantId,
    incarnation: IncarnationToken,
    identity: Identity,
    counter: i64,
    guest: Guest,
    cached: HashMap<u64, CachedCommand>,
    gate_closed: bool,
    pending_rollout: Option<PendingRollout>,
    runtime: Arc<SharedRuntime>,
    sink: EventSink,
    admission: Arc<Mutex<AdmissionState>>,
}

impl TenantHandle {
    pub fn spawn(
        request_id: RequestId,
        tenant_id: TenantId,
        incarnation: IncarnationToken,
        identity: Identity,
        artifact: Artifact,
        runtime: Arc<SharedRuntime>,
        sink: EventSink,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(MAILBOX_CAPACITY);
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let admission = Arc::new(Mutex::new(AdmissionState {
            incarnation,
            identity: identity.clone(),
            accepting: true,
            outstanding: 0,
            known: HashMap::new(),
        }));
        let worker_admission = admission.clone();
        tokio::spawn(async move {
            if let Err(error) = tenant_task(
                TenantStart {
                    request_id,
                    tenant_id,
                    incarnation,
                    identity,
                    artifact,
                    runtime,
                    sink: sink.clone(),
                    admission: worker_admission,
                },
                command_rx,
                control_rx,
            )
            .await
            {
                let _ = sink.emit(
                    request_id,
                    EventMessage::Fatal(crate::protocol::FatalEvent {
                        code: "tenant_worker_failed".into(),
                        message: error.to_string(),
                    }),
                );
            }
        });
        Self {
            command_tx,
            control_tx,
            admission,
        }
    }

    pub fn admit(
        &self,
        request_id: RequestId,
        control: CommandControl,
        sink: &EventSink,
    ) -> Result<()> {
        let mut admission = self
            .admission
            .lock()
            .map_err(|_| anyhow!("tenant admission lock poisoned"))?;
        let rejection = if !admission.accepting {
            Some(RejectionReason::ShuttingDown)
        } else if control.incarnation != admission.incarnation {
            Some(RejectionReason::StaleIncarnation)
        } else if control.payload.len() > PAYLOAD_LIMIT {
            Some(RejectionReason::PayloadTooLarge)
        } else {
            None
        };
        if let Some(reason) = rejection {
            return sink.emit(request_id, rejected(&control, reason));
        }
        let fingerprint = Fingerprint {
            operation: control.operation.clone(),
            payload_sha256: control.payload_sha256.clone(),
        };
        if admission
            .known
            .get(&control.command_id.0)
            .is_some_and(|known| known != &fingerprint)
        {
            return sink.emit(
                request_id,
                rejected(&control, RejectionReason::CommandIdConflict),
            );
        }
        let inserted = admission
            .known
            .insert(control.command_id.0, fingerprint)
            .is_none();
        let context = (control.tenant_id, control.incarnation, control.command_id);
        match self.command_tx.try_reserve() {
            Ok(permit) => {
                admission.outstanding += 1;
                let published = publish_after_accept(
                    permit,
                    CommandWork {
                        request_id,
                        control,
                    },
                    || {
                        sink.emit(
                            request_id,
                            EventMessage::CommandAccepted(CommandAcceptedEvent {
                                tenant_id: context.0,
                                incarnation: context.1,
                                command_id: context.2,
                            }),
                        )
                    },
                );
                if let Err(error) = published {
                    admission.outstanding -= 1;
                    if inserted {
                        admission.known.remove(&context.2 .0);
                    }
                    return Err(error);
                }
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(())) => {
                if inserted {
                    admission.known.remove(&control.command_id.0);
                }
                sink.emit(
                    request_id,
                    rejected(&control, RejectionReason::Backpressure),
                )
            }
            Err(mpsc::error::TrySendError::Closed(())) => {
                if inserted {
                    admission.known.remove(&control.command_id.0);
                }
                sink.emit(
                    request_id,
                    rejected(&control, RejectionReason::ShuttingDown),
                )
            }
        }
    }

    pub fn set_gate(&self, request_id: RequestId, control: SetDequeueGateControl) -> Result<()> {
        self.control_tx
            .send(TenantControl::Gate {
                request_id,
                control,
            })
            .map_err(|_| anyhow!("tenant control channel closed"))
    }

    pub fn snapshot(&self, request_id: RequestId, control: SnapshotControl) -> Result<()> {
        self.control_tx
            .send(TenantControl::Snapshot {
                request_id,
                control,
            })
            .map_err(|_| anyhow!("tenant control channel closed"))
    }

    pub fn inject_fault(
        &self,
        request_id: RequestId,
        control: InjectFaultControl,
        artifact: Artifact,
        identity: Identity,
    ) -> Result<()> {
        self.control_tx
            .send(TenantControl::Fault {
                request_id,
                control,
                artifact,
                identity,
            })
            .map_err(|_| anyhow!("tenant control channel closed"))
    }

    pub fn prepare_rollout(
        &self,
        request_id: RequestId,
        control: RolloutControl,
        artifact: Artifact,
        identity: Identity,
    ) -> Result<oneshot::Receiver<PrepareResult>> {
        let (reply, receiver) = oneshot::channel();
        self.control_tx
            .send(TenantControl::PrepareRollout {
                request_id,
                control,
                artifact,
                identity,
                reply,
            })
            .map_err(|_| anyhow!("tenant control channel closed"))?;
        Ok(receiver)
    }

    pub fn finish_rollout(&self, commit: bool) -> Result<oneshot::Receiver<Result<()>>> {
        let (reply, receiver) = oneshot::channel();
        self.control_tx
            .send(TenantControl::FinishRollout { commit, reply })
            .map_err(|_| anyhow!("tenant control channel closed"))?;
        Ok(receiver)
    }

    pub fn teardown(&self, request_id: RequestId, incarnation: IncarnationToken) -> Result<()> {
        {
            let mut admission = self
                .admission
                .lock()
                .map_err(|_| anyhow!("tenant admission lock poisoned"))?;
            admission.accepting = false;
        }
        self.control_tx
            .send(TenantControl::Teardown {
                request_id,
                incarnation,
            })
            .map_err(|_| anyhow!("tenant control channel closed"))
    }

    pub fn is_quiescent(&self) -> Result<bool> {
        let admission = self
            .admission
            .lock()
            .map_err(|_| anyhow!("tenant admission lock poisoned"))?;
        Ok(admission.outstanding == 0)
    }
}

pub fn publish_after_accept<T>(
    permit: mpsc::Permit<'_, T>,
    work: T,
    emit_accepted: impl FnOnce() -> Result<()>,
) -> Result<()> {
    emit_accepted()?;
    permit.send(work);
    Ok(())
}

async fn tenant_task(
    start: TenantStart,
    mut command_rx: mpsc::Receiver<CommandWork>,
    mut control_rx: mpsc::UnboundedReceiver<TenantControl>,
) -> Result<()> {
    let TenantStart {
        request_id,
        tenant_id,
        incarnation,
        identity,
        artifact,
        runtime,
        sink,
        admission,
    } = start;
    sink.emit(
        request_id,
        EventMessage::TenantCreated(TenantCreatedEvent {
            tenant_id,
            incarnation,
        }),
    )?;
    let guest = match Guest::instantiate(&runtime, &artifact, &identity, tenant_id, 0) {
        Ok(guest) => guest,
        Err(failure) => {
            if failure.activation_observed {
                sink.emit(request_id, identity.activation(tenant_id, incarnation))?;
            }
            return Err(anyhow!(
                "initial guest activation failed: {}",
                failure.detail
            ));
        }
    };
    sink.emit(request_id, identity.activation(tenant_id, incarnation))?;
    sink.emit(
        request_id,
        EventMessage::TenantReady(TenantReadyEvent {
            tenant_id,
            incarnation,
        }),
    )?;
    let mut worker = TenantWorker {
        tenant_id,
        incarnation,
        identity,
        counter: 0,
        guest,
        cached: HashMap::new(),
        gate_closed: false,
        pending_rollout: None,
        runtime,
        sink,
        admission,
    };
    loop {
        if worker.gate_closed || worker.pending_rollout.is_some() {
            let Some(control) = control_rx.recv().await else {
                return Ok(());
            };
            if worker.handle_control(control)? {
                return Ok(());
            }
            continue;
        }
        tokio::select! {
            biased;
            control = control_rx.recv() => {
                let Some(control) = control else { return Ok(()); };
                if worker.handle_control(control)? { return Ok(()); }
            }
            command = command_rx.recv() => {
                let Some(command) = command else { return Ok(()); };
                worker.handle_command(command)?;
            }
        }
    }
}

impl TenantWorker {
    fn handle_command(&mut self, work: CommandWork) -> Result<()> {
        if let Some(cached) = self.cached.get(&work.control.command_id.0) {
            let mut result = cached.result.clone();
            result.deduplicated = true;
            self.sink.emit(
                work.request_id,
                EventMessage::CommandCompleted(CommandCompletedEvent {
                    tenant_id: self.tenant_id,
                    incarnation: self.incarnation,
                    command_id: work.control.command_id,
                    result,
                }),
            )?;
            return self.mark_terminal();
        }
        let increment = match work.control.operation {
            CommandOperation::Increment(ref operation) if operation.delta == 1 => true,
            CommandOperation::Read(_) => false,
            CommandOperation::Increment(_) => {
                return self.command_failed(work, "core guest supports frozen delta one only");
            }
        };
        let expected = if increment {
            self.counter
                .checked_add(1)
                .ok_or_else(|| anyhow!("tenant counter overflow"))?
        } else {
            self.counter
        };
        let call = self
            .guest
            .call_command(work.control.command_id.0, increment)
            .and_then(|(counter, _)| {
                self.guest.verify_identity(&self.identity)?;
                if counter != expected {
                    return Err(anyhow!("guest counter {counter} differs from {expected}"));
                }
                Ok(counter)
            });
        let counter = match call {
            Ok(counter) => counter,
            Err(error) => return self.command_failed(work, &error.to_string()),
        };
        self.counter = counter;
        let result = self.command_result(counter, false);
        self.cached.insert(
            work.control.command_id.0,
            CachedCommand {
                result: result.clone(),
            },
        );
        self.sink.emit(
            work.request_id,
            EventMessage::CommandCompleted(CommandCompletedEvent {
                tenant_id: self.tenant_id,
                incarnation: self.incarnation,
                command_id: work.control.command_id,
                result,
            }),
        )?;
        self.mark_terminal()
    }

    fn command_failed(&mut self, work: CommandWork, error: &str) -> Result<()> {
        self.sink.emit(
            work.request_id,
            EventMessage::CommandFailed(CommandFailedEvent {
                tenant_id: self.tenant_id,
                incarnation: work.control.incarnation,
                command_id: work.control.command_id,
                error: error.into(),
            }),
        )?;
        self.mark_terminal()
    }

    fn mark_terminal(&self) -> Result<()> {
        let mut admission = self
            .admission
            .lock()
            .map_err(|_| anyhow!("tenant admission lock poisoned"))?;
        admission.outstanding = admission
            .outstanding
            .checked_sub(1)
            .ok_or_else(|| anyhow!("accepted command accounting underflow"))?;
        Ok(())
    }

    fn command_result(&self, counter: i64, deduplicated: bool) -> CommandResult {
        CommandResult {
            incarnation: self.incarnation,
            generation: self.incarnation.value().into(),
            counter,
            logical_version: self.identity.logical_version.clone(),
            build_id: self.identity.build_id.clone(),
            deduplicated,
            business_result_sha256: business_result_digest(self.incarnation.value(), counter),
        }
    }

    fn handle_control(&mut self, control: TenantControl) -> Result<bool> {
        match control {
            TenantControl::Gate {
                request_id,
                control,
            } => self.gate(request_id, control)?,
            TenantControl::Snapshot {
                request_id,
                control,
            } => self.snapshot(request_id, control)?,
            TenantControl::Fault {
                request_id,
                control,
                artifact,
                identity,
            } => self.fault(request_id, control, artifact, identity)?,
            TenantControl::PrepareRollout {
                request_id,
                control,
                artifact,
                identity,
                reply,
            } => self.prepare_rollout(request_id, control, artifact, identity, reply)?,
            TenantControl::FinishRollout { commit, reply } => {
                let result = self.finish_rollout(commit);
                let failed = result.is_err();
                let _ = reply.send(result);
                if failed {
                    return Err(anyhow!("rollout finish failed"));
                }
            }
            TenantControl::Teardown {
                request_id,
                incarnation,
            } => {
                if incarnation != self.incarnation {
                    return Err(anyhow!("teardown used stale incarnation"));
                }
                self.sink.emit(
                    request_id,
                    EventMessage::TenantTornDown(TenantTornDownEvent {
                        tenant_id: self.tenant_id,
                        incarnation,
                    }),
                )?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn gate(&mut self, request_id: RequestId, control: SetDequeueGateControl) -> Result<()> {
        if control.incarnation != self.incarnation {
            return Err(anyhow!("gate used stale incarnation"));
        }
        self.gate_closed = control.closed;
        self.sink.emit(
            request_id,
            EventMessage::DequeueGateSet(DequeueGateSetEvent {
                tenant_id: self.tenant_id,
                incarnation: self.incarnation,
                closed: self.gate_closed,
            }),
        )
    }

    fn snapshot(&mut self, request_id: RequestId, control: SnapshotControl) -> Result<()> {
        if let Some(expected) = control.expected_incarnation {
            if expected != self.incarnation {
                return self.sink.emit(
                    request_id,
                    EventMessage::SnapshotStale(SnapshotStaleEvent {
                        tenant_id: self.tenant_id,
                        expected_incarnation: expected,
                        actual_incarnation: self.incarnation,
                    }),
                );
            }
        }
        let counter = self.guest.call_snapshot(request_id.0)?;
        self.guest.verify_identity(&self.identity)?;
        if counter != self.counter {
            return Err(anyhow!("guest snapshot counter mismatch"));
        }
        self.sink.emit(
            request_id,
            EventMessage::SnapshotPresent(SnapshotPresentEvent {
                tenant_id: self.tenant_id,
                incarnation: self.incarnation,
                state: self.command_result(counter, false),
            }),
        )
    }

    fn fault(
        &mut self,
        request_id: RequestId,
        control: InjectFaultControl,
        artifact: Artifact,
        identity: Identity,
    ) -> Result<()> {
        if control.incarnation != self.incarnation {
            return Err(anyhow!("fault control used stale incarnation"));
        }
        if control.incarnation.checked_next() != Some(control.expected_replacement_incarnation) {
            return Err(anyhow!(
                "fault control replacement incarnation is not the next token"
            ));
        }
        let reason = match control.fault {
            FaultKind::Trap(_) => {
                self.guest.call_trap()?;
                FaultFailureReason::GuestTrap
            }
            FaultKind::CpuHog(_) => {
                let observer = FaultObserver {
                    sink: self.sink.clone(),
                    request_id,
                    fault_id: control.fault_id.0,
                    tenant_id: self.tenant_id,
                    incarnation: self.incarnation,
                };
                self.guest.call_cpu_loop(observer)?;
                FaultFailureReason::CpuDeadline
            }
        };
        self.sink.emit(
            request_id,
            EventMessage::ExecutionFailed(ExecutionFailedEvent {
                fault_id: control.fault_id,
                tenant_id: self.tenant_id,
                incarnation: self.incarnation,
                reason,
            }),
        )?;
        self.sink.emit(
            request_id,
            EventMessage::FailureObserved(FailureObservedEvent {
                fault_id: control.fault_id,
                tenant_id: self.tenant_id,
                failed_incarnation: self.incarnation,
            }),
        )?;
        let replacement =
            Guest::instantiate(&self.runtime, &artifact, &identity, self.tenant_id, 0)
                .map_err(|failure| anyhow!("replacement activation failed: {}", failure.detail))?;
        self.sink.emit(
            request_id,
            identity.activation(self.tenant_id, control.expected_replacement_incarnation),
        )?;
        self.guest = replacement;
        self.identity = identity.clone();
        self.incarnation = control.expected_replacement_incarnation;
        self.counter = 0;
        self.cached.clear();
        self.gate_closed = false;
        {
            let mut admission = self
                .admission
                .lock()
                .map_err(|_| anyhow!("tenant admission lock poisoned"))?;
            admission.incarnation = self.incarnation;
            admission.identity = identity;
            admission.known.clear();
        }
        self.sink.emit(
            request_id,
            EventMessage::TenantReady(TenantReadyEvent {
                tenant_id: self.tenant_id,
                incarnation: self.incarnation,
            }),
        )
    }

    fn prepare_rollout(
        &mut self,
        request_id: RequestId,
        control: RolloutControl,
        artifact: Artifact,
        identity: Identity,
        reply: oneshot::Sender<PrepareResult>,
    ) -> Result<()> {
        if self.pending_rollout.is_some() {
            let _ = reply.send(PrepareResult {
                success: false,
                reason: "another rollout is pending".into(),
            });
            return Ok(());
        }
        match Guest::instantiate(
            &self.runtime,
            &artifact,
            &identity,
            self.tenant_id,
            self.counter,
        ) {
            Ok(guest) => {
                self.sink.emit(
                    request_id,
                    identity.activation(self.tenant_id, self.incarnation),
                )?;
                self.sink.emit(
                    request_id,
                    EventMessage::RolloutTargetReady(RolloutTargetReadyEvent {
                        rollout_id: control.rollout_id,
                        tenant_id: self.tenant_id,
                        incarnation: self.incarnation,
                        logical_version: identity.logical_version.clone(),
                        build_id: identity.build_id.clone(),
                        artifact_sha256: identity.artifact_sha256.clone(),
                    }),
                )?;
                self.pending_rollout = Some(PendingRollout::Prepared {
                    guest: Box::new(guest),
                    identity,
                });
                let _ = reply.send(PrepareResult {
                    success: true,
                    reason: String::new(),
                });
            }
            Err(failure) => {
                if failure.activation_observed {
                    self.sink.emit(
                        request_id,
                        identity.activation(self.tenant_id, self.incarnation),
                    )?;
                }
                self.pending_rollout = Some(PendingRollout::Failed);
                let _ = reply.send(PrepareResult {
                    success: false,
                    reason: failure.detail,
                });
            }
        }
        Ok(())
    }

    fn finish_rollout(&mut self, commit: bool) -> Result<()> {
        let pending = self
            .pending_rollout
            .take()
            .ok_or_else(|| anyhow!("rollout finish without prepare"))?;
        if commit {
            let PendingRollout::Prepared { guest, identity } = pending else {
                return Err(anyhow!("cannot commit failed activation"));
            };
            self.guest = *guest;
            self.identity = identity.clone();
            let mut admission = self
                .admission
                .lock()
                .map_err(|_| anyhow!("tenant admission lock poisoned"))?;
            admission.identity = identity;
        }
        Ok(())
    }
}

fn rejected(control: &CommandControl, reason: RejectionReason) -> EventMessage {
    EventMessage::CommandRejected(CommandRejectedEvent {
        tenant_id: control.tenant_id,
        incarnation: control.incarnation,
        command_id: control.command_id,
        reason,
    })
}

fn business_result_digest(generation: u64, counter: i64) -> String {
    sha256_hex(format!(r#"{{"counter":{counter},"generation":{generation}}}"#).as_bytes())
}
