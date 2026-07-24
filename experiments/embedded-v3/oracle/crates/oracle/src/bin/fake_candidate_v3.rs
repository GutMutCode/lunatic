use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, BufRead, Write};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use embedded_v3_oracle::protocol::*;
use sha2::{Digest, Sha256};

const MAX_PAYLOAD_BYTES: usize = 1024;
const CLOSED_GATE_CAPACITY: usize = 64;
const MAX_CONTROL_LINE_BYTES: usize = 2 * 1024 * 1024;
const CPU_FAULT_DELAY: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionPhase {
    AwaitHello,
    AwaitInit,
    Running,
    Shutdown,
}

#[derive(Debug, Clone)]
struct CachedCommand {
    payload: Vec<u8>,
    payload_sha256: String,
    operation: CommandOperation,
    counter: i64,
}

#[derive(Debug, Clone)]
struct Tenant {
    incarnation: IncarnationToken,
    counter: i64,
    logical_version: String,
    build_id: String,
    artifact_sha256: String,
    completed: HashMap<CommandId, CachedCommand>,
    dequeue_closed: bool,
    queued: VecDeque<(RequestId, CommandControl)>,
}

impl Tenant {
    fn fresh(
        incarnation: IncarnationToken,
        logical_version: String,
        build_id: String,
        artifact_sha256: String,
    ) -> Self {
        Self {
            incarnation,
            counter: 0,
            logical_version,
            build_id,
            artifact_sha256,
            completed: HashMap::new(),
            dequeue_closed: false,
            queued: VecDeque::with_capacity(CLOSED_GATE_CAPACITY),
        }
    }

    fn result(&self, counter: i64, deduplicated: bool) -> CommandResult {
        let generation = self.incarnation.value();
        CommandResult {
            incarnation: self.incarnation,
            generation: DecimalU64(generation),
            counter,
            logical_version: self.logical_version.clone(),
            build_id: self.build_id.clone(),
            deduplicated,
            business_result_sha256: business_result_sha256(generation, counter),
        }
    }
}

#[derive(Debug)]
struct FaultCompletion {
    request_id: RequestId,
    fault_id: DecimalU64,
    tenant_id: TenantId,
    failed_incarnation: IncarnationToken,
    replacement_logical_version: String,
    replacement_build_id: String,
    replacement_artifact_sha256: String,
}

#[derive(Debug)]
struct PendingCpuFault {
    due: Instant,
    completion: FaultCompletion,
}

#[derive(Debug)]
struct FakeCandidate {
    phase: SessionPhase,
    next_event_seq: u64,
    tenant_capacity: usize,
    last_incarnations: HashMap<TenantId, IncarnationToken>,
    tenants: HashMap<TenantId, Tenant>,
    pending_cpu_faults: Vec<PendingCpuFault>,
    quiesced: bool,
}

impl FakeCandidate {
    fn new() -> Self {
        Self {
            phase: SessionPhase::AwaitHello,
            next_event_seq: 1,
            tenant_capacity: 0,
            last_incarnations: HashMap::new(),
            tenants: HashMap::new(),
            pending_cpu_faults: Vec::new(),
            quiesced: false,
        }
    }

    fn emit(&mut self, request_id: RequestId, message: EventMessage) -> io::Result<()> {
        let event = EventEnvelope::new(self.next_event_seq, request_id.0, message);
        self.next_event_seq = self
            .next_event_seq
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "event sequence overflow"))?;

        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        serde_json::to_writer(&mut stdout, &event).map_err(io::Error::other)?;
        stdout.write_all(b"\n")?;
        stdout.flush()
    }

    fn fatal(
        &mut self,
        request_id: RequestId,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> io::Result<bool> {
        self.emit(
            request_id,
            EventMessage::Fatal(FatalEvent {
                code: code.into(),
                message: message.into(),
            }),
        )?;
        self.phase = SessionPhase::Shutdown;
        Ok(false)
    }

    fn handle(&mut self, control: ControlEnvelope) -> io::Result<bool> {
        let request_id = control.request_id;
        match control.message {
            ControlMessage::Hello(hello) => self.hello(request_id, hello),
            ControlMessage::Init(init) => self.init(request_id, init),
            ControlMessage::CreateTenant(create) => self.create_tenant(request_id, create),
            ControlMessage::Command(command) => self.command(request_id, command),
            ControlMessage::InjectFault(fault) => self.inject_fault(request_id, fault),
            ControlMessage::Rollout(rollout) => self.rollout(request_id, rollout),
            ControlMessage::Snapshot(snapshot) => self.snapshot(request_id, snapshot),
            ControlMessage::SetDequeueGate(gate) => self.set_dequeue_gate(request_id, gate),
            ControlMessage::AuthorityProbe(probe) => self.authority_probe(request_id, probe),
            ControlMessage::Quiesce(_) => self.quiesce(request_id),
            ControlMessage::TeardownTenant(teardown) => self.teardown_tenant(request_id, teardown),
            ControlMessage::Shutdown(_) => self.shutdown(request_id),
        }
    }

    fn require_running(&mut self, request_id: RequestId, operation: &str) -> io::Result<bool> {
        if self.phase == SessionPhase::Running {
            Ok(true)
        } else {
            self.fatal(
                request_id,
                "invalid_session_phase",
                format!("{operation} is not valid in phase {:?}", self.phase),
            )
        }
    }

    fn hello(&mut self, request_id: RequestId, hello: HelloControl) -> io::Result<bool> {
        if self.phase != SessionPhase::AwaitHello {
            return self.fatal(
                request_id,
                "invalid_session_phase",
                "hello must be the first control",
            );
        }
        if hello.expected_candidate != CandidateIdentity::fake_test_double() {
            return self.fatal(
                request_id,
                "candidate_mismatch",
                format!(
                    "oracle expected {}, but this process is fake",
                    hello.expected_candidate
                ),
            );
        }
        if hello.oracle_name.is_empty() {
            return self.fatal(request_id, "invalid_hello", "oracle_name must not be empty");
        }

        self.emit(
            request_id,
            EventMessage::Hello(HelloEvent {
                candidate: CandidateIdentity::fake_test_double(),
                implementation_version: env!("CARGO_PKG_VERSION").into(),
            }),
        )?;
        self.phase = SessionPhase::AwaitInit;
        Ok(true)
    }

    fn init(&mut self, request_id: RequestId, init: InitControl) -> io::Result<bool> {
        if self.phase != SessionPhase::AwaitInit {
            return self.fatal(
                request_id,
                "invalid_session_phase",
                "init must follow hello exactly once",
            );
        }
        if init.run_id.is_empty() || init.tenant_capacity == 0 {
            return self.fatal(
                request_id,
                "invalid_init",
                "run_id must be non-empty and tenant_capacity must be positive",
            );
        }

        self.tenant_capacity = init.tenant_capacity as usize;
        self.emit(
            request_id,
            EventMessage::Initialized(InitializedEvent {
                run_id: init.run_id,
            }),
        )?;
        self.phase = SessionPhase::Running;
        Ok(true)
    }

    fn create_tenant(
        &mut self,
        request_id: RequestId,
        create: CreateTenantControl,
    ) -> io::Result<bool> {
        if !self.require_running(request_id, "create_tenant")? {
            return Ok(false);
        }
        if self.quiesced {
            return self.fatal(
                request_id,
                "create_after_quiesce",
                "tenant creation is forbidden after quiesce",
            );
        }
        if self.tenants.contains_key(&create.tenant_id) {
            return self.fatal(
                request_id,
                "tenant_already_exists",
                format!("tenant {} is already active", create.tenant_id.0),
            );
        }
        if self.tenants.len() >= self.tenant_capacity {
            return self.fatal(
                request_id,
                "tenant_capacity_exceeded",
                format!("tenant capacity {} exceeded", self.tenant_capacity),
            );
        }

        let expected = match self.last_incarnations.get(&create.tenant_id) {
            Some(last) => match last.checked_next() {
                Some(next) => next,
                None => {
                    return self.fatal(
                        request_id,
                        "incarnation_overflow",
                        format!("tenant {} incarnation overflow", create.tenant_id.0),
                    );
                }
            },
            None => IncarnationToken::new(0),
        };
        if create.incarnation != expected {
            return self.fatal(
                request_id,
                "invalid_oracle_incarnation",
                format!(
                    "tenant {} expected oracle-issued incarnation {}, got {}",
                    create.tenant_id.0, expected, create.incarnation
                ),
            );
        }

        self.tenants.insert(
            create.tenant_id,
            Tenant::fresh(
                create.incarnation,
                create.logical_version.clone(),
                create.build_id.clone(),
                create.artifact_sha256.clone(),
            ),
        );
        self.last_incarnations
            .insert(create.tenant_id, create.incarnation);
        self.emit(
            request_id,
            EventMessage::TenantCreated(TenantCreatedEvent {
                tenant_id: create.tenant_id,
                incarnation: create.incarnation,
            }),
        )?;
        self.emit(
            request_id,
            EventMessage::ActivationStarted(ActivationStartedEvent {
                tenant_id: create.tenant_id,
                incarnation: create.incarnation,
                logical_version: create.logical_version,
                build_id: create.build_id,
                artifact_sha256: create.artifact_sha256,
            }),
        )?;
        self.emit(
            request_id,
            EventMessage::TenantReady(TenantReadyEvent {
                tenant_id: create.tenant_id,
                incarnation: create.incarnation,
            }),
        )?;
        Ok(true)
    }

    fn command(&mut self, request_id: RequestId, command: CommandControl) -> io::Result<bool> {
        if !self.require_running(request_id, "command")? {
            return Ok(false);
        }
        if self.quiesced {
            self.emit(
                request_id,
                EventMessage::CommandRejected(CommandRejectedEvent {
                    tenant_id: command.tenant_id,
                    incarnation: command.incarnation,
                    command_id: command.command_id,
                    reason: RejectionReason::ShuttingDown,
                }),
            )?;
            return Ok(true);
        }

        let Some(tenant) = self.tenants.get(&command.tenant_id) else {
            self.emit(
                request_id,
                EventMessage::CommandRejected(CommandRejectedEvent {
                    tenant_id: command.tenant_id,
                    incarnation: command.incarnation,
                    command_id: command.command_id,
                    reason: RejectionReason::TenantMissing,
                }),
            )?;
            return Ok(true);
        };
        if tenant.incarnation != command.incarnation {
            self.emit(
                request_id,
                EventMessage::CommandRejected(CommandRejectedEvent {
                    tenant_id: command.tenant_id,
                    incarnation: command.incarnation,
                    command_id: command.command_id,
                    reason: RejectionReason::StaleIncarnation,
                }),
            )?;
            return Ok(true);
        }

        if command.payload.len() > MAX_PAYLOAD_BYTES {
            self.emit(
                request_id,
                EventMessage::CommandRejected(CommandRejectedEvent {
                    tenant_id: command.tenant_id,
                    incarnation: command.incarnation,
                    command_id: command.command_id,
                    reason: RejectionReason::PayloadTooLarge,
                }),
            )?;
            return Ok(true);
        }

        let actual_payload_sha256 = payload_sha256(&command.payload);
        let command_conflicts = actual_payload_sha256 != command.payload_sha256
            || tenant
                .completed
                .get(&command.command_id)
                .is_some_and(|cached| {
                    cached.payload != command.payload
                        || cached.payload_sha256 != command.payload_sha256
                        || cached.operation != command.operation
                })
            || tenant.queued.iter().any(|(_, queued)| {
                queued.command_id == command.command_id
                    && (queued.payload != command.payload
                        || queued.payload_sha256 != command.payload_sha256
                        || queued.operation != command.operation)
            });
        if command_conflicts {
            self.emit(
                request_id,
                EventMessage::CommandRejected(CommandRejectedEvent {
                    tenant_id: command.tenant_id,
                    incarnation: command.incarnation,
                    command_id: command.command_id,
                    reason: RejectionReason::CommandIdConflict,
                }),
            )?;
            return Ok(true);
        }

        if tenant.dequeue_closed && tenant.queued.len() >= CLOSED_GATE_CAPACITY {
            self.emit(
                request_id,
                EventMessage::CommandRejected(CommandRejectedEvent {
                    tenant_id: command.tenant_id,
                    incarnation: command.incarnation,
                    command_id: command.command_id,
                    reason: RejectionReason::Backpressure,
                }),
            )?;
            return Ok(true);
        }

        self.emit(
            request_id,
            EventMessage::CommandAccepted(CommandAcceptedEvent {
                tenant_id: command.tenant_id,
                incarnation: command.incarnation,
                command_id: command.command_id,
            }),
        )?;

        let dequeue_closed = self
            .tenants
            .get(&command.tenant_id)
            .expect("tenant was checked before admission")
            .dequeue_closed;
        if dequeue_closed {
            self.tenants
                .get_mut(&command.tenant_id)
                .expect("tenant was checked before admission")
                .queued
                .push_back((request_id, command));
            return Ok(true);
        }

        let outcome = {
            let tenant = self
                .tenants
                .get_mut(&command.tenant_id)
                .expect("tenant and incarnation were checked before admission");

            if let Some(cached) = tenant.completed.get(&command.command_id) {
                let same_input = cached.payload == command.payload
                    && cached.payload_sha256 == actual_payload_sha256
                    && cached.operation == command.operation;
                if !same_input {
                    Err("command_id_payload_mismatch")
                } else {
                    Ok(tenant.result(cached.counter, true))
                }
            } else {
                let counter = match &command.operation {
                    CommandOperation::Increment(increment) => {
                        match tenant.counter.checked_add(increment.delta) {
                            Some(counter) => counter,
                            None => return self.emit_overflow_failure(request_id, &command),
                        }
                    }
                    CommandOperation::Read(_) => tenant.counter,
                };
                tenant.counter = counter;
                tenant.completed.insert(
                    command.command_id,
                    CachedCommand {
                        payload: command.payload.clone(),
                        payload_sha256: actual_payload_sha256,
                        operation: command.operation.clone(),
                        counter,
                    },
                );
                Ok(tenant.result(counter, false))
            }
        };

        match outcome {
            Ok(result) => self.emit(
                request_id,
                EventMessage::CommandCompleted(CommandCompletedEvent {
                    tenant_id: command.tenant_id,
                    incarnation: command.incarnation,
                    command_id: command.command_id,
                    result,
                }),
            )?,
            Err(error) => {
                eprintln!("fake-candidate-v3: protocol blocker: no typed {error} rejection");
                self.emit_command_failed(request_id, &command, error)?;
            }
        }
        Ok(true)
    }

    fn complete_queued_command(
        &mut self,
        request_id: RequestId,
        command: CommandControl,
    ) -> io::Result<bool> {
        let actual_payload_sha256 = payload_sha256(&command.payload);
        let outcome = {
            let tenant = self
                .tenants
                .get_mut(&command.tenant_id)
                .expect("queued command tenant must remain active until gate drain");

            if let Some(cached) = tenant.completed.get(&command.command_id) {
                let same_input = cached.payload == command.payload
                    && cached.payload_sha256 == actual_payload_sha256
                    && cached.operation == command.operation;
                if !same_input {
                    Err("command_id_payload_mismatch")
                } else {
                    Ok(tenant.result(cached.counter, true))
                }
            } else {
                let counter = match &command.operation {
                    CommandOperation::Increment(increment) => {
                        match tenant.counter.checked_add(increment.delta) {
                            Some(counter) => counter,
                            None => return self.emit_overflow_failure(request_id, &command),
                        }
                    }
                    CommandOperation::Read(_) => tenant.counter,
                };
                tenant.counter = counter;
                tenant.completed.insert(
                    command.command_id,
                    CachedCommand {
                        payload: command.payload.clone(),
                        payload_sha256: actual_payload_sha256,
                        operation: command.operation.clone(),
                        counter,
                    },
                );
                Ok(tenant.result(counter, false))
            }
        };

        match outcome {
            Ok(result) => self.emit(
                request_id,
                EventMessage::CommandCompleted(CommandCompletedEvent {
                    tenant_id: command.tenant_id,
                    incarnation: command.incarnation,
                    command_id: command.command_id,
                    result,
                }),
            )?,
            Err(error) => {
                eprintln!("fake-candidate-v3: queued command invariant failed: {error}");
                self.emit_command_failed(request_id, &command, error)?;
            }
        }
        Ok(true)
    }

    fn emit_overflow_failure(
        &mut self,
        request_id: RequestId,
        command: &CommandControl,
    ) -> io::Result<bool> {
        self.emit_command_failed(request_id, command, "counter_overflow")?;
        Ok(true)
    }

    fn emit_command_failed(
        &mut self,
        request_id: RequestId,
        command: &CommandControl,
        error: &str,
    ) -> io::Result<()> {
        self.emit(
            request_id,
            EventMessage::CommandFailed(CommandFailedEvent {
                tenant_id: command.tenant_id,
                incarnation: command.incarnation,
                command_id: command.command_id,
                error: error.into(),
            }),
        )
    }

    fn inject_fault(
        &mut self,
        request_id: RequestId,
        fault: InjectFaultControl,
    ) -> io::Result<bool> {
        if !self.require_running(request_id, "inject_fault")? {
            return Ok(false);
        }
        if self.quiesced {
            return self.fatal(
                request_id,
                "fault_after_quiesce",
                "fault injection is forbidden after quiesce",
            );
        }

        let Some(tenant) = self.tenants.get(&fault.tenant_id) else {
            return self.fatal(
                request_id,
                "fault_target_missing",
                format!("fault target tenant {} is missing", fault.tenant_id.0),
            );
        };
        if tenant.incarnation != fault.incarnation {
            return self.fatal(
                request_id,
                "fault_incarnation_mismatch",
                format!(
                    "fault target {} has incarnation {}, got {}",
                    fault.tenant_id.0, tenant.incarnation, fault.incarnation
                ),
            );
        }
        let Some(expected_replacement) = fault.incarnation.checked_next() else {
            return self.fatal(
                request_id,
                "incarnation_overflow",
                format!("tenant {} incarnation overflow", fault.tenant_id.0),
            );
        };
        if fault.expected_replacement_incarnation != expected_replacement {
            return self.fatal(
                request_id,
                "invalid_replacement_incarnation",
                format!(
                    "fault replacement for tenant {} must be {}, got {}",
                    fault.tenant_id.0, expected_replacement, fault.expected_replacement_incarnation
                ),
            );
        }

        self.tenants
            .remove(&fault.tenant_id)
            .expect("fault target was checked above");
        match fault.fault {
            FaultKind::Trap(_) => {
                self.complete_fault(
                    FaultCompletion {
                        request_id,
                        fault_id: fault.fault_id,
                        tenant_id: fault.tenant_id,
                        failed_incarnation: fault.incarnation,
                        replacement_logical_version: fault.replacement_logical_version,
                        replacement_build_id: fault.replacement_build_id,
                        replacement_artifact_sha256: fault.replacement_artifact_sha256,
                    },
                    FaultFailureReason::GuestTrap,
                )?;
            }
            FaultKind::CpuHog(_) => {
                self.emit(
                    request_id,
                    EventMessage::ExecutionStarted(ExecutionStartedEvent {
                        fault_id: fault.fault_id,
                        tenant_id: fault.tenant_id,
                        incarnation: fault.incarnation,
                        origin: ExecutionStartOrigin::GuestFirstActionObserver,
                    }),
                )?;
                self.pending_cpu_faults.push(PendingCpuFault {
                    due: Instant::now() + CPU_FAULT_DELAY,
                    completion: FaultCompletion {
                        request_id,
                        fault_id: fault.fault_id,
                        tenant_id: fault.tenant_id,
                        failed_incarnation: fault.incarnation,
                        replacement_logical_version: fault.replacement_logical_version,
                        replacement_build_id: fault.replacement_build_id,
                        replacement_artifact_sha256: fault.replacement_artifact_sha256,
                    },
                });
            }
        }
        Ok(true)
    }

    fn complete_fault(
        &mut self,
        completion: FaultCompletion,
        reason: FaultFailureReason,
    ) -> io::Result<()> {
        let FaultCompletion {
            request_id,
            fault_id,
            tenant_id,
            failed_incarnation,
            replacement_logical_version,
            replacement_build_id,
            replacement_artifact_sha256,
        } = completion;
        self.emit(
            request_id,
            EventMessage::ExecutionFailed(ExecutionFailedEvent {
                fault_id,
                tenant_id,
                incarnation: failed_incarnation,
                reason,
            }),
        )?;
        self.emit(
            request_id,
            EventMessage::FailureObserved(FailureObservedEvent {
                fault_id,
                tenant_id,
                failed_incarnation,
            }),
        )?;

        let replacement_incarnation = failed_incarnation
            .checked_next()
            .expect("replacement incarnation was validated before fault admission");

        let replacement = Tenant::fresh(
            replacement_incarnation,
            replacement_logical_version.clone(),
            replacement_build_id.clone(),
            replacement_artifact_sha256.clone(),
        );
        self.tenants.insert(tenant_id, replacement);
        self.last_incarnations
            .insert(tenant_id, replacement_incarnation);
        self.emit(
            request_id,
            EventMessage::ActivationStarted(ActivationStartedEvent {
                tenant_id,
                incarnation: replacement_incarnation,
                logical_version: replacement_logical_version,
                build_id: replacement_build_id,
                artifact_sha256: replacement_artifact_sha256,
            }),
        )?;
        self.emit(
            request_id,
            EventMessage::TenantReady(TenantReadyEvent {
                tenant_id,
                incarnation: replacement_incarnation,
            }),
        )
    }

    fn finish_due_cpu_faults(&mut self) -> io::Result<()> {
        let now = Instant::now();
        self.pending_cpu_faults
            .sort_by_key(|fault| (fault.due, fault.completion.fault_id.0));
        while self
            .pending_cpu_faults
            .first()
            .is_some_and(|fault| fault.due <= now)
        {
            let fault = self.pending_cpu_faults.remove(0);
            self.complete_fault(fault.completion, FaultFailureReason::CpuDeadline)?;
        }
        Ok(())
    }

    fn next_cpu_deadline(&self) -> Option<Instant> {
        self.pending_cpu_faults.iter().map(|fault| fault.due).min()
    }

    fn rollout(&mut self, request_id: RequestId, rollout: RolloutControl) -> io::Result<bool> {
        if !self.require_running(request_id, "rollout")? {
            return Ok(false);
        }
        if self.quiesced {
            self.emit(
                request_id,
                EventMessage::RolloutRejected(RolloutRejectedEvent {
                    rollout_id: rollout.rollout_id,
                    reason: "candidate is quiesced".into(),
                }),
            )?;
            return Ok(true);
        }

        let unique_targets: HashSet<_> = rollout.targets.iter().copied().collect();
        if unique_targets.len() != rollout.targets.len() {
            self.emit(
                request_id,
                EventMessage::RolloutRejected(RolloutRejectedEvent {
                    rollout_id: rollout.rollout_id,
                    reason: "duplicate rollout target".into(),
                }),
            )?;
            return Ok(true);
        }
        for tenant_id in &rollout.targets {
            let Some(tenant) = self.tenants.get(tenant_id) else {
                self.emit(
                    request_id,
                    EventMessage::RolloutRejected(RolloutRejectedEvent {
                        rollout_id: rollout.rollout_id,
                        reason: format!("tenant {} missing", tenant_id.0),
                    }),
                )?;
                return Ok(true);
            };
            if tenant.logical_version != rollout.from_version {
                self.emit(
                    request_id,
                    EventMessage::RolloutRejected(RolloutRejectedEvent {
                        rollout_id: rollout.rollout_id,
                        reason: format!(
                            "tenant {} version is {}, expected {}",
                            tenant_id.0, tenant.logical_version, rollout.from_version
                        ),
                    }),
                )?;
                return Ok(true);
            }
        }

        self.emit(
            request_id,
            EventMessage::RolloutStarted(RolloutStartedEvent {
                rollout_id: rollout.rollout_id.clone(),
            }),
        )?;

        if rollout.artifact_ref.contains("bad") {
            self.emit(
                request_id,
                EventMessage::RolloutRolledBack(RolloutRolledBackEvent {
                    rollout_id: rollout.rollout_id,
                    restored_version: rollout.from_version,
                    reason: "deterministic bad artifact activation failure".into(),
                }),
            )?;
            return Ok(true);
        }

        let targets: Vec<_> = rollout
            .targets
            .iter()
            .map(|tenant_id| {
                let tenant = self
                    .tenants
                    .get_mut(tenant_id)
                    .expect("rollout targets were validated");
                // Rollout changes neither incarnation nor completed business
                // state. Only guest identity advances.
                tenant.logical_version = rollout.to_version.clone();
                tenant.build_id = rollout.to_build_id.clone();
                tenant.artifact_sha256 = rollout.artifact_sha256.clone();
                (*tenant_id, tenant.incarnation)
            })
            .collect();
        for (tenant_id, incarnation) in targets {
            self.emit(
                request_id,
                EventMessage::ActivationStarted(ActivationStartedEvent {
                    tenant_id,
                    incarnation,
                    logical_version: rollout.to_version.clone(),
                    build_id: rollout.to_build_id.clone(),
                    artifact_sha256: rollout.artifact_sha256.clone(),
                }),
            )?;
            self.emit(
                request_id,
                EventMessage::RolloutTargetReady(RolloutTargetReadyEvent {
                    rollout_id: rollout.rollout_id.clone(),
                    tenant_id,
                    incarnation,
                    logical_version: rollout.to_version.clone(),
                    build_id: rollout.to_build_id.clone(),
                    artifact_sha256: rollout.artifact_sha256.clone(),
                }),
            )?;
        }
        self.emit(
            request_id,
            EventMessage::RolloutCommitted(RolloutCommittedEvent {
                rollout_id: rollout.rollout_id,
                version: rollout.to_version,
            }),
        )?;
        Ok(true)
    }

    fn snapshot(&mut self, request_id: RequestId, snapshot: SnapshotControl) -> io::Result<bool> {
        if !self.require_running(request_id, "snapshot")? {
            return Ok(false);
        }

        let message = match self.tenants.get(&snapshot.tenant_id) {
            None => EventMessage::SnapshotMissing(SnapshotMissingEvent {
                tenant_id: snapshot.tenant_id,
            }),
            Some(tenant)
                if snapshot
                    .expected_incarnation
                    .is_some_and(|expected| expected != tenant.incarnation) =>
            {
                EventMessage::SnapshotStale(SnapshotStaleEvent {
                    tenant_id: snapshot.tenant_id,
                    expected_incarnation: snapshot
                        .expected_incarnation
                        .expect("guard proved the expected incarnation exists"),
                    actual_incarnation: tenant.incarnation,
                })
            }
            Some(tenant) => EventMessage::SnapshotPresent(SnapshotPresentEvent {
                tenant_id: snapshot.tenant_id,
                incarnation: tenant.incarnation,
                state: tenant.result(tenant.counter, false),
            }),
        };
        self.emit(request_id, message)?;
        Ok(true)
    }

    fn set_dequeue_gate(
        &mut self,
        request_id: RequestId,
        gate: SetDequeueGateControl,
    ) -> io::Result<bool> {
        if !self.require_running(request_id, "set_dequeue_gate")? {
            return Ok(false);
        }
        let Some(tenant) = self.tenants.get(&gate.tenant_id) else {
            return self.fatal(
                request_id,
                "dequeue_gate_target_missing",
                format!("tenant {} is missing", gate.tenant_id.0),
            );
        };
        if tenant.incarnation != gate.incarnation {
            return self.fatal(
                request_id,
                "dequeue_gate_incarnation_mismatch",
                format!(
                    "tenant {} has incarnation {}, got {}",
                    gate.tenant_id.0, tenant.incarnation, gate.incarnation
                ),
            );
        }
        let queued = {
            let tenant = self
                .tenants
                .get_mut(&gate.tenant_id)
                .expect("gate target was checked above");
            tenant.dequeue_closed = gate.closed;
            if gate.closed {
                VecDeque::new()
            } else {
                std::mem::take(&mut tenant.queued)
            }
        };

        self.emit(
            request_id,
            EventMessage::DequeueGateSet(DequeueGateSetEvent {
                tenant_id: gate.tenant_id,
                incarnation: gate.incarnation,
                closed: gate.closed,
            }),
        )?;
        for (queued_request_id, queued_command) in queued {
            self.complete_queued_command(queued_request_id, queued_command)?;
        }

        Ok(true)
    }

    fn authority_probe(
        &mut self,
        request_id: RequestId,
        probe: AuthorityProbeControl,
    ) -> io::Result<bool> {
        if !self.require_running(request_id, "authority_probe")? {
            return Ok(false);
        }

        self.emit(
            request_id,
            EventMessage::AuthorityProbeAccepted(AuthorityProbeAcceptedEvent {
                attempt_id: probe.attempt_id,
                probe: probe.probe,
                artifact_sha256: probe.artifact_sha256.clone(),
                parameter_sha256: probe.parameter_sha256.clone(),
                production_policy_sha256: probe.production_policy_sha256.clone(),
            }),
        )?;
        self.emit(
            request_id,
            EventMessage::AuthorityProbeTerminal(AuthorityProbeTerminalEvent {
                attempt_id: probe.attempt_id,
                probe: probe.probe,
                artifact_sha256: probe.artifact_sha256,
                parameter_sha256: probe.parameter_sha256,
                production_policy_sha256: probe.production_policy_sha256,
                stage: AuthorityProbeStage::Instantiate,
                result: AuthorityProbeResult::AbsentAtLink,
                error_class: AuthorityProbeErrorClass::UnknownImport,
                guest_return_sha256: EMPTY_SHA256.into(),
            }),
        )?;
        Ok(true)
    }

    fn quiesce(&mut self, request_id: RequestId) -> io::Result<bool> {
        if !self.require_running(request_id, "quiesce")? {
            return Ok(false);
        }
        if !self.pending_cpu_faults.is_empty() {
            return self.fatal(
                request_id,
                "quiesce_with_pending_fault",
                "cannot quiesce while a CPU fault is pending",
            );
        }
        self.quiesced = true;
        self.emit(request_id, EventMessage::Quiesced(QuiescedEvent {}))?;
        Ok(true)
    }

    fn teardown_tenant(
        &mut self,
        request_id: RequestId,
        teardown: TeardownTenantControl,
    ) -> io::Result<bool> {
        if !self.require_running(request_id, "teardown_tenant")? {
            return Ok(false);
        }
        let Some(tenant) = self.tenants.get(&teardown.tenant_id) else {
            return self.fatal(
                request_id,
                "teardown_target_missing",
                format!("tenant {} is missing", teardown.tenant_id.0),
            );
        };
        if tenant.incarnation != teardown.incarnation {
            return self.fatal(
                request_id,
                "teardown_incarnation_mismatch",
                format!(
                    "tenant {} has incarnation {}, got {}",
                    teardown.tenant_id.0, tenant.incarnation, teardown.incarnation
                ),
            );
        }

        self.tenants.remove(&teardown.tenant_id);
        self.last_incarnations
            .insert(teardown.tenant_id, teardown.incarnation);
        self.emit(
            request_id,
            EventMessage::TenantTornDown(TenantTornDownEvent {
                tenant_id: teardown.tenant_id,
                incarnation: teardown.incarnation,
            }),
        )?;
        Ok(true)
    }

    fn shutdown(&mut self, request_id: RequestId) -> io::Result<bool> {
        if !self.require_running(request_id, "shutdown")? {
            return Ok(false);
        }
        if !self.pending_cpu_faults.is_empty() {
            return self.fatal(
                request_id,
                "shutdown_with_pending_fault",
                "cannot shutdown while a CPU fault is pending",
            );
        }
        self.emit(
            request_id,
            EventMessage::ShutdownComplete(ShutdownCompleteEvent {}),
        )?;
        self.phase = SessionPhase::Shutdown;
        Ok(false)
    }
}

#[derive(Debug)]
enum ReaderMessage {
    Line(String),
    Error(io::Error),
    Eof,
}

fn spawn_stdin_reader() -> Receiver<ReaderMessage> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let message = match line {
                Ok(line) => ReaderMessage::Line(line),
                Err(error) => ReaderMessage::Error(error),
            };
            let terminal = matches!(message, ReaderMessage::Error(_));
            if sender.send(message).is_err() || terminal {
                return;
            }
        }
        let _ = sender.send(ReaderMessage::Eof);
    });
    receiver
}

fn receive_next(
    receiver: &Receiver<ReaderMessage>,
    deadline: Option<Instant>,
) -> Result<Option<ReaderMessage>, mpsc::RecvError> {
    let Some(deadline) = deadline else {
        return receiver.recv().map(Some);
    };
    let timeout = deadline.saturating_duration_since(Instant::now());
    match receiver.recv_timeout(timeout) {
        Ok(message) => Ok(Some(message)),
        Err(RecvTimeoutError::Timeout) => Ok(None),
        Err(RecvTimeoutError::Disconnected) => Err(mpsc::RecvError),
    }
}

fn business_result_sha256(generation: u64, counter: i64) -> String {
    let canonical = format!(r#"{{"counter":{counter},"generation":{generation}}}"#);
    sha256_hex(canonical.as_bytes())
}

fn payload_sha256(payload: &[u8]) -> String {
    sha256_hex(payload)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
    }
    hex
}

fn main() -> io::Result<()> {
    eprintln!("fake-candidate-v3: strict stdin/stdout transport online");
    let receiver = spawn_stdin_reader();
    let mut candidate = FakeCandidate::new();

    loop {
        candidate.finish_due_cpu_faults()?;
        let next = match receive_next(&receiver, candidate.next_cpu_deadline()) {
            Ok(Some(message)) => message,
            Ok(None) => continue,
            Err(_) => ReaderMessage::Eof,
        };

        match next {
            ReaderMessage::Line(line) => {
                if line.len() > MAX_CONTROL_LINE_BYTES {
                    candidate.fatal(
                        RequestId(0),
                        "control_line_too_large",
                        format!(
                            "control line is {} bytes; maximum is {}",
                            line.len(),
                            MAX_CONTROL_LINE_BYTES
                        ),
                    )?;
                    break;
                }
                let control = match decode_control_line(&line) {
                    Ok(control) => control,
                    Err(error) => {
                        candidate.fatal(RequestId(0), "invalid_control", error.to_string())?;
                        break;
                    }
                };
                if !candidate.handle(control)? {
                    break;
                }
            }
            ReaderMessage::Error(error) => {
                candidate.fatal(RequestId(0), "stdin_error", error.to_string())?;
                break;
            }
            ReaderMessage::Eof => {
                if candidate.phase != SessionPhase::Shutdown {
                    candidate.fatal(
                        RequestId(0),
                        "unexpected_eof",
                        "stdin closed before shutdown completed",
                    )?;
                }
                break;
            }
        }
    }
    Ok(())
}
