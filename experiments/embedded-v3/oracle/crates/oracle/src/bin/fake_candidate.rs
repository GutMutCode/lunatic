use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::thread;
use std::time::Duration;

use embedded_v3_oracle::protocol::*;

#[derive(Debug)]
struct Tenant {
    incarnation: IncarnationToken,
    generation: u64,
    counter: i64,
    completed: HashMap<CommandId, CommandResult>,
}

struct FakeCandidate {
    next_event_seq: u64,
    next_generation: HashMap<TenantId, u64>,
    tenants: HashMap<TenantId, Tenant>,
}

impl FakeCandidate {
    fn new() -> Self {
        Self {
            next_event_seq: 1,
            next_generation: HashMap::new(),
            tenants: HashMap::new(),
        }
    }

    fn emit(&mut self, request_id: RequestId, message: EventMessage) -> io::Result<()> {
        let event = EventEnvelope::new(self.next_event_seq, request_id.0, message);
        self.next_event_seq += 1;
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        serde_json::to_writer(&mut stdout, &event).map_err(io::Error::other)?;
        stdout.write_all(b"\n")?;
        stdout.flush()
    }

    fn create_token(&mut self, tenant_id: TenantId) -> (u64, IncarnationToken) {
        let generation = self.next_generation.entry(tenant_id).or_insert(0);
        *generation += 1;
        let token =
            IncarnationToken::new(format!("tenant-{}-incarnation-{}", tenant_id.0, generation))
                .expect("deterministic token is valid");
        (*generation, token)
    }

    fn handle(&mut self, control: ControlEnvelope) -> io::Result<bool> {
        let request_id = control.request_id;
        match control.message {
            ControlMessage::Hello(_) => self.emit(
                request_id,
                EventMessage::Hello(HelloEvent {
                    candidate: CandidateKind::Fake,
                    implementation_version: env!("CARGO_PKG_VERSION").into(),
                }),
            )?,
            ControlMessage::Init(init) => self.emit(
                request_id,
                EventMessage::Initialized(InitializedEvent {
                    run_id: init.run_id,
                }),
            )?,
            ControlMessage::CreateTenant(create) => {
                let (generation, incarnation) = self.create_token(create.tenant_id);
                self.tenants.insert(
                    create.tenant_id,
                    Tenant {
                        incarnation: incarnation.clone(),
                        generation,
                        counter: 0,
                        completed: HashMap::new(),
                    },
                );
                self.emit(
                    request_id,
                    EventMessage::TenantCreated(TenantCreatedEvent {
                        tenant_id: create.tenant_id,
                        incarnation: incarnation.clone(),
                    }),
                )?;
                self.emit(
                    request_id,
                    EventMessage::TenantReady(TenantReadyEvent {
                        tenant_id: create.tenant_id,
                        incarnation,
                    }),
                )?;
            }
            ControlMessage::Command(command) => {
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

                self.emit(
                    request_id,
                    EventMessage::CommandAccepted(CommandAcceptedEvent {
                        tenant_id: command.tenant_id,
                        incarnation: command.incarnation.clone(),
                        command_id: command.command_id,
                    }),
                )?;

                let tenant = self
                    .tenants
                    .get_mut(&command.tenant_id)
                    .expect("tenant was checked above");
                let duplicate = tenant.completed.get(&command.command_id).cloned();
                let result = if let Some(result) = duplicate {
                    // Deliberately late enough for the transport smoke test to
                    // prove it waits for the terminal event, but below the SLO.
                    thread::sleep(Duration::from_millis(25));
                    result
                } else {
                    if let CommandOperation::Increment(increment) = command.operation {
                        tenant.counter += increment.delta;
                    }
                    let result = CommandResult {
                        generation: tenant.generation,
                        counter: tenant.counter,
                    };
                    tenant.completed.insert(command.command_id, result.clone());
                    result
                };
                self.emit(
                    request_id,
                    EventMessage::CommandCompleted(CommandCompletedEvent {
                        tenant_id: command.tenant_id,
                        incarnation: command.incarnation,
                        command_id: command.command_id,
                        result,
                    }),
                )?;
            }
            ControlMessage::InjectFault(fault) => {
                self.emit(
                    request_id,
                    EventMessage::ExecutionStarted(ExecutionStartedEvent {
                        tenant_id: fault.tenant_id,
                        incarnation: fault.incarnation.clone(),
                    }),
                )?;
                self.emit(
                    request_id,
                    EventMessage::ExecutionFailed(ExecutionFailedEvent {
                        tenant_id: fault.tenant_id,
                        incarnation: fault.incarnation.clone(),
                        reason: "injected by fake candidate".into(),
                    }),
                )?;
                self.emit(
                    request_id,
                    EventMessage::FailureObserved(FailureObservedEvent {
                        tenant_id: fault.tenant_id,
                        failed_incarnation: fault.incarnation,
                    }),
                )?;
                let (generation, incarnation) = self.create_token(fault.tenant_id);
                self.tenants.insert(
                    fault.tenant_id,
                    Tenant {
                        incarnation: incarnation.clone(),
                        generation,
                        counter: 0,
                        completed: HashMap::new(),
                    },
                );
                self.emit(
                    request_id,
                    EventMessage::TenantReady(TenantReadyEvent {
                        tenant_id: fault.tenant_id,
                        incarnation,
                    }),
                )?;
            }
            ControlMessage::Rollout(rollout) => {
                self.emit(
                    request_id,
                    EventMessage::RolloutStarted(RolloutStartedEvent {
                        rollout_id: rollout.rollout_id.clone(),
                    }),
                )?;
                for tenant_id in rollout.targets {
                    let Some(incarnation) = self
                        .tenants
                        .get(&tenant_id)
                        .map(|tenant| tenant.incarnation.clone())
                    else {
                        self.emit(
                            request_id,
                            EventMessage::RolloutInDoubt(RolloutInDoubtEvent {
                                rollout_id: rollout.rollout_id,
                                reason: format!("tenant {} missing", tenant_id.0),
                            }),
                        )?;
                        return Ok(true);
                    };
                    self.emit(
                        request_id,
                        EventMessage::RolloutTargetReady(RolloutTargetReadyEvent {
                            rollout_id: rollout.rollout_id.clone(),
                            tenant_id,
                            incarnation,
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
            }
            ControlMessage::Snapshot(snapshot) => match self.tenants.get(&snapshot.tenant_id) {
                None => self.emit(
                    request_id,
                    EventMessage::SnapshotMissing(SnapshotMissingEvent {
                        tenant_id: snapshot.tenant_id,
                    }),
                )?,
                Some(tenant)
                    if snapshot.expected_incarnation.as_ref() != Some(&tenant.incarnation)
                        && snapshot.expected_incarnation.is_some() =>
                {
                    self.emit(
                        request_id,
                        EventMessage::SnapshotStale(SnapshotStaleEvent {
                            tenant_id: snapshot.tenant_id,
                            expected_incarnation: snapshot
                                .expected_incarnation
                                .expect("guard proved expected token exists"),
                            actual_incarnation: tenant.incarnation.clone(),
                        }),
                    )?
                }
                Some(tenant) => self.emit(
                    request_id,
                    EventMessage::SnapshotPresent(SnapshotPresentEvent {
                        tenant_id: snapshot.tenant_id,
                        incarnation: tenant.incarnation.clone(),
                        state: CommandResult {
                            generation: tenant.generation,
                            counter: tenant.counter,
                        },
                    }),
                )?,
            },
            ControlMessage::TeardownTenant(teardown) => {
                if self
                    .tenants
                    .get(&teardown.tenant_id)
                    .is_some_and(|tenant| tenant.incarnation == teardown.incarnation)
                {
                    self.tenants.remove(&teardown.tenant_id);
                }
                self.emit(
                    request_id,
                    EventMessage::TenantTornDown(TenantTornDownEvent {
                        tenant_id: teardown.tenant_id,
                        incarnation: teardown.incarnation,
                    }),
                )?;
            }
            ControlMessage::Shutdown(_) => {
                self.emit(
                    request_id,
                    EventMessage::ShutdownComplete(ShutdownCompleteEvent::default()),
                )?;
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn main() -> io::Result<()> {
    eprintln!("fake-candidate: stderr capture online");
    let stdin = io::stdin();
    let mut candidate = FakeCandidate::new();
    for line in stdin.lock().lines() {
        let line = line?;
        let control = match decode_control_line(&line) {
            Ok(control) => control,
            Err(error) => {
                candidate.emit(
                    RequestId(0),
                    EventMessage::Fatal(FatalEvent {
                        code: "invalid_control".into(),
                        message: error.to_string(),
                    }),
                )?;
                break;
            }
        };
        if !candidate.handle(control)? {
            break;
        }
    }
    Ok(())
}
