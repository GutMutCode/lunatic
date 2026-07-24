use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryFrom;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, ensure, Context, Result};
use embedded_v3_authority_guests::Probe;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::authority_harness::{
    protocol_kind, AuthorityHarness, NoEffectEvidence, PositiveControlEvidence,
};
use crate::automaton::RequestPhase;
use crate::protocol::{
    AuthorityProbeControl, AuthorityProbeResult, AuthorityProbeTerminalEvent, CandidateIdentity,
    CandidateKind, CommandControl, CommandId, CommandOperation, CommandResult, ControlEnvelope,
    ControlMessage, CpuHogFault, CreateTenantControl, DecimalU64, EventMessage,
    ExecutionStartOrigin, FaultFailureReason, FaultKind, HelloControl, IncarnationToken,
    IncrementOperation, InitControl, InjectFaultControl, QuiesceControl, ReadOperation,
    RejectionReason, RequestId, RolloutControl, SetDequeueGateControl, ShutdownControl,
    SnapshotControl, TeardownTenantControl, TenantId, TrapFault,
};
use crate::transport::{OracleSession, ReceivedEvent, SentControl, SentOrEvent};
use crate::workload::{
    deterministic_payload, validate_authority_canary, validate_cpu_observation,
    validate_worker_trace_cardinality, worker_command_id, AdmissionOutcome,
    AuthorityCanaryEvidence, CpuStartOrigin, GuestBusinessResult, ModelCommand, ModelOperation,
    PlanKind, PlannedAction, StateOracle, WorkerLogicalTrace, WorkerOperation, WorkerRetryReason,
    WorkerTransportOutcome, WorkerTransportTrace, EMBEDDED_SCENARIO_SHA256,
};

pub const ARTIFACT_A_SHA256: &str =
    "9bce3d46eceef79abe98d88d16c2a8ac75ec1ffd68a7f328cc34a9c1a1fae0d1";
pub const ARTIFACT_B_SHA256: &str =
    "5c918a4ea3730ac7109188ec94108a135fb4a537a713bcbf27fae325a5ea511e";
pub const ARTIFACT_BAD_A_SHA256: &str =
    "d3675abac308b7a20513f2ff65042c9b0e9e84195c16ec5256f8472a15653a2d";
pub const ARTIFACT_BAD_B_SHA256: &str =
    "ab3274086121b6ff8af75b331c5c101b5216c187b86f8ea12cd06673e72dc557";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedTerminal {
    Completed,
    Backpressure,
    PayloadTooLarge,
    StaleIncarnation,
    TenantMissing,
}

#[derive(Debug, Clone)]
pub struct CommandTiming {
    pub request_id: RequestId,
    pub tenant_id: TenantId,
    pub command_id: u64,
    pub written_ns: u64,
    pub accepted_ns: Option<u64>,
    pub terminal_ns: u64,
    pub terminal_event_seq: u64,
    pub result: Option<GuestBusinessResult>,
    pub rejection: Option<RejectionReason>,
}

#[derive(Debug, Default)]
pub struct TimingSamples {
    pub shared_init_ns: u64,
    pub warm_create_ns: Vec<u64>,
    pub normal_ns: Vec<u64>,
    pub normal_total_ns: u64,
    pub pressure_admission_ns: Vec<u64>,
    pub sibling_ns: Vec<u64>,
    pub trap_replacement_ready_ns: Vec<u64>,
    pub cpu_replacement_ready_ns: Vec<u64>,
    pub cpu_failed_terminal_ns: Vec<u64>,
    pub failed_update_unavailability_ns: Vec<u64>,
    pub valid_update_unavailability_ns: Vec<u64>,
    pub failed_rollout_ns: Vec<u64>,
    pub valid_rollout_ns: Vec<u64>,
    pub failed_mixed_window_ns: Vec<u64>,
    pub valid_mixed_window_ns: Vec<u64>,
}

#[derive(Debug, Default)]
pub struct WorkloadCounts {
    pub controls_sent: u64,
    pub events_received: u64,
    pub accepted_commands: u64,
    pub terminal_accepted_commands: u64,
    pub update_worker_logical_operations: u64,
    pub update_worker_transport_attempts: u64,
    pub update_worker_retries: u64,
}

#[derive(Debug, Serialize)]
pub struct AuthorityRunEvidence {
    pub positive_action_ordinal: u64,
    pub canary_action_ordinal: u64,
    pub label: String,
    pub positive_control: PositiveControlEvidence,
    pub no_effect: NoEffectEvidence,
    pub canary: AuthorityCanaryEvidence,
}

#[derive(Debug)]
struct PendingCommand {
    control: ControlEnvelope,
    sent: SentControl,
    accepted_ns: Option<u64>,
    deadline: Instant,
    result: Option<GuestBusinessResult>,
    rejection: Option<RejectionReason>,
}

#[derive(Debug)]
struct PendingCreate {
    tenant: TenantId,
    incarnation: u64,
    sent: SentControl,
    phase: RequestPhase,
    deadline: Instant,
    created: bool,
}

pub struct WorkloadRunner {
    session: OracleSession,
    candidate: CandidateKind,
    block: u32,
    next_request_id: u64,
    model: StateOracle,
    pub timings: TimingSamples,
    pub counts: WorkloadCounts,
}

impl WorkloadRunner {
    pub fn new(session: OracleSession, candidate: CandidateKind, block: u32) -> Result<Self> {
        ensure!(block < 10, "block must be in 0..9");
        Ok(Self {
            session,
            candidate,
            block,
            next_request_id: 1,
            model: StateOracle::new(),
            timings: TimingSamples::default(),
            counts: WorkloadCounts::default(),
        })
    }

    pub fn session(&self) -> &OracleSession {
        &self.session
    }

    pub fn session_mut(&mut self) -> &mut OracleSession {
        &mut self.session
    }

    pub fn model(&self) -> &StateOracle {
        &self.model
    }

    pub fn into_parts(self) -> (OracleSession, StateOracle, TimingSamples, WorkloadCounts) {
        (self.session, self.model, self.timings, self.counts)
    }

    fn envelope(&mut self, message: ControlMessage) -> Result<ControlEnvelope> {
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("request_id overflow"))?;
        Ok(ControlEnvelope::new(request_id, message))
    }

    fn round_trip_control(
        &mut self,
        message: ControlMessage,
    ) -> Result<(SentControl, Vec<ReceivedEvent>)> {
        let control = self.envelope(message)?;
        self.counts.controls_sent += 1;
        let (sent, events) = self
            .session
            .round_trip_timed(&control)
            .with_context(|| format!("request {} failed", control.request_id.0))?;
        self.observe_events(&events)?;
        Ok((sent, events))
    }

    pub fn hello(&mut self) -> Result<u64> {
        let (_, hello_events) = self.round_trip_control(ControlMessage::Hello(HelloControl {
            oracle_name: "embedded-v3-full-oracle".to_owned(),
            expected_candidate: CandidateIdentity::Production(self.candidate),
        }))?;
        terminal_timestamp(&hello_events)
    }

    pub fn init(&mut self, run_id: &str) -> Result<u64> {
        let (init_sent, init_events) =
            self.round_trip_control(ControlMessage::Init(InitControl {
                run_id: run_id.to_owned(),
                tenant_capacity: 32,
            }))?;
        let init_terminal = terminal_timestamp(&init_events)?;
        self.timings.shared_init_ns = positive_delta(init_sent.sent_at_ns, init_terminal)?;
        Ok(init_terminal)
    }

    pub fn hello_and_init(&mut self, run_id: &str) -> Result<(u64, u64)> {
        let hello_terminal = self.hello()?;
        let init_terminal = self.init(run_id)?;
        Ok((hello_terminal, init_terminal))
    }

    pub fn create_tenant(&mut self, tenant: TenantId) -> Result<(u64, u64)> {
        self.create_tenant_inner(tenant, true)
    }

    fn create_tenant_unmeasured(&mut self, tenant: TenantId) -> Result<(u64, u64)> {
        self.create_tenant_inner(tenant, false)
    }

    fn create_tenant_inner(
        &mut self,
        tenant: TenantId,
        record_warm_create: bool,
    ) -> Result<(u64, u64)> {
        let incarnation = self.model.expected_create_incarnation(tenant)?;
        let (sent, events) =
            self.round_trip_control(ControlMessage::CreateTenant(CreateTenantControl {
                tenant_id: tenant,
                incarnation: IncarnationToken::new(incarnation),
                logical_version: "A".to_owned(),
                build_id: "A".to_owned(),
                artifact_sha256: ARTIFACT_A_SHA256.to_owned(),
            }))?;
        let created = events
            .iter()
            .find_map(|event| match &event.envelope.message {
                EventMessage::TenantCreated(value) => Some(value),
                _ => None,
            });
        let created = created.ok_or_else(|| anyhow!("create emitted no tenant_created event"))?;
        ensure!(
            created.tenant_id == tenant && created.incarnation.value() == incarnation,
            "create echoed a forged tenant identity"
        );
        self.model
            .create(tenant, incarnation, "A", "A")
            .context("model rejected tenant create")?;
        let terminal_ns = terminal_timestamp(&events)?;
        let duration = positive_delta(sent.sent_at_ns, terminal_ns)?;
        if record_warm_create {
            self.timings.warm_create_ns.push(duration);
        }
        Ok((incarnation, terminal_ns))
    }

    fn command_control(
        &self,
        tenant: TenantId,
        incarnation: u64,
        command_id: u64,
        operation: ModelOperation,
        payload: Vec<u8>,
    ) -> CommandControl {
        let wire_operation = match operation {
            ModelOperation::Increment(delta) => {
                CommandOperation::Increment(IncrementOperation { delta })
            }
            ModelOperation::Read => CommandOperation::Read(ReadOperation::default()),
        };
        CommandControl {
            tenant_id: tenant,
            incarnation: IncarnationToken::new(incarnation),
            command_id: CommandId(command_id),
            operation: wire_operation,
            payload_sha256: sha256_hex(&payload),
            payload,
        }
    }

    fn command_from_action(
        &self,
        action: &PlannedAction,
        incarnation: u64,
        operation: ModelOperation,
    ) -> Result<CommandControl> {
        let tenant = TenantId(
            action
                .tenant_id
                .ok_or_else(|| anyhow!("planned command has no tenant"))?,
        );
        let command_id = action
            .command_id
            .as_deref()
            .ok_or_else(|| anyhow!("planned command has no command_id"))?
            .parse::<u64>()
            .context("planned command_id is not u64")?;
        let payload_bytes = action
            .payload_bytes
            .ok_or_else(|| anyhow!("planned command has no payload length"))?;
        let payload = deterministic_payload(self.block, command_id, payload_bytes)?;
        ensure!(
            action.payload_sha256.as_deref() == Some(sha256_hex(&payload).as_str()),
            "planned payload digest drift"
        );
        Ok(self.command_control(tenant, incarnation, command_id, operation, payload))
    }

    fn send_command_control(&mut self, control: CommandControl) -> Result<PendingCommand> {
        let envelope = self.envelope(ControlMessage::Command(control.clone()))?;
        let model_operation = match control.operation {
            CommandOperation::Increment(value) => ModelOperation::Increment(value.delta),
            CommandOperation::Read(_) => ModelOperation::Read,
        };
        self.model.begin_command(ModelCommand {
            request_id: envelope.request_id.0,
            tenant_id: control.tenant_id,
            incarnation: control.incarnation.value(),
            command_id: control.command_id.0,
            operation: model_operation,
            payload: control.payload.clone(),
            payload_sha256: control.payload_sha256.clone(),
        })?;
        self.counts.controls_sent += 1;
        let sent = self
            .session
            .send(&envelope)
            .with_context(|| format!("send command request {}", envelope.request_id.0))?;
        Ok(PendingCommand {
            control: envelope,
            sent,
            accepted_ns: None,
            deadline: Instant::now() + Duration::from_millis(100),
            result: None,
            rejection: None,
        })
    }

    pub fn round_trip_action_command(
        &mut self,
        action: &PlannedAction,
        incarnation: u64,
        operation: ModelOperation,
        expected: ExpectedTerminal,
    ) -> Result<CommandTiming> {
        let control = self.command_from_action(action, incarnation, operation)?;
        self.round_trip_command_control(control, expected)
    }

    pub fn round_trip_command_control(
        &mut self,
        control: CommandControl,
        expected: ExpectedTerminal,
    ) -> Result<CommandTiming> {
        let pending = self.send_command_control(control)?;
        let request_id = pending.control.request_id;
        let mut completed = self.drive_pending(BTreeMap::from([(request_id, pending)]))?;
        let timing = completed
            .remove(&request_id)
            .ok_or_else(|| anyhow!("command did not produce a timing record"))?;
        validate_expected_terminal(&timing, expected)?;
        Ok(timing)
    }

    fn send_action_command(
        &mut self,
        action: &PlannedAction,
        incarnation: u64,
        operation: ModelOperation,
    ) -> Result<(RequestId, PendingCommand)> {
        let control = self.command_from_action(action, incarnation, operation)?;
        let pending = self.send_command_control(control)?;
        Ok((pending.control.request_id, pending))
    }

    pub fn drive_action_batch(
        &mut self,
        actions: &[&PlannedAction],
        operation: ModelOperation,
    ) -> Result<Vec<CommandTiming>> {
        let mut pending = BTreeMap::new();
        for action in actions {
            let tenant = TenantId(action.tenant_id.ok_or_else(|| anyhow!("missing tenant"))?);
            let incarnation = self
                .model
                .incarnation(tenant)
                .ok_or_else(|| anyhow!("batch target {} is inactive", tenant.0))?;
            let (request_id, command) =
                self.send_action_command(action, incarnation, operation.clone())?;
            pending.insert(request_id, command);
        }
        let completed = self.drive_pending(pending)?;
        actions
            .iter()
            .map(|action| {
                let command_id = action
                    .command_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("missing command id"))?
                    .parse::<u64>()?;
                completed
                    .values()
                    .find(|timing| timing.result.is_some() && timing.command_id == command_id)
                    .cloned()
                    .ok_or_else(|| anyhow!("batch command {command_id} has no completion"))
            })
            .collect()
    }

    fn drive_pending(
        &mut self,
        mut pending: BTreeMap<RequestId, PendingCommand>,
    ) -> Result<BTreeMap<RequestId, CommandTiming>> {
        let mut completed = BTreeMap::new();
        while !pending.is_empty() {
            let now = Instant::now();
            let next_deadline = pending
                .values()
                .map(|item| item.deadline)
                .min()
                .expect("pending is nonempty");
            ensure!(now < next_deadline, "command phase deadline expired");
            let event = self
                .session
                .receive(next_deadline.saturating_duration_since(now))
                .context("receiving command batch event")?;
            self.counts.events_received += 1;
            let request_id = event.envelope.request_id;
            self.observe_event(&event)?;

            if let Some(item) = pending.get_mut(&request_id) {
                match &event.envelope.message {
                    EventMessage::CommandAccepted(_) => {
                        item.accepted_ns = Some(event.received_at_ns);
                        item.deadline = Instant::now() + Duration::from_millis(250);
                    }
                    EventMessage::CommandCompleted(value) => {
                        item.result = Some(guest_result(&value.result));
                    }
                    EventMessage::CommandRejected(value) => {
                        item.rejection = Some(value.reason);
                    }
                    EventMessage::CommandFailed(value) => {
                        bail!("accepted command failed: {}", value.error);
                    }
                    _ => {}
                }
            }

            if self.session.oracle().is_terminal(request_id) {
                if let Some(item) = pending.remove(&request_id) {
                    completed.insert(
                        request_id,
                        CommandTiming {
                            request_id,
                            tenant_id: match &item.control.message {
                                ControlMessage::Command(control) => control.tenant_id,
                                _ => unreachable!("pending command contains command control"),
                            },
                            command_id: match &item.control.message {
                                ControlMessage::Command(control) => control.command_id.0,
                                _ => unreachable!("pending command contains command control"),
                            },
                            written_ns: item.sent.sent_at_ns,
                            accepted_ns: item.accepted_ns,
                            terminal_ns: event.received_at_ns,
                            terminal_event_seq: event.envelope.event_seq.0,
                            result: item.result,
                            rejection: item.rejection,
                        },
                    );
                }
            }
        }
        Ok(completed)
    }

    fn observe_events(&mut self, events: &[ReceivedEvent]) -> Result<()> {
        for event in events {
            self.counts.events_received += 1;
            self.observe_event(event)?;
        }
        Ok(())
    }

    fn observe_event(&mut self, event: &ReceivedEvent) -> Result<()> {
        match &event.envelope.message {
            EventMessage::CommandAccepted(_) => {
                self.model
                    .observe_admission(event.envelope.request_id.0, AdmissionOutcome::Accepted)?;
                self.counts.accepted_commands += 1;
            }
            EventMessage::CommandRejected(value) => {
                let outcome = match value.reason {
                    RejectionReason::Backpressure => AdmissionOutcome::RetryableRejection,
                    RejectionReason::PayloadTooLarge => AdmissionOutcome::Oversize,
                    RejectionReason::CommandIdConflict => AdmissionOutcome::CommandIdConflict,
                    RejectionReason::StaleIncarnation => AdmissionOutcome::StaleIncarnation,
                    RejectionReason::TenantMissing => AdmissionOutcome::TenantMissing,
                    RejectionReason::ShuttingDown => {
                        bail!("unexpected shutting_down rejection")
                    }
                };
                self.model
                    .observe_admission(event.envelope.request_id.0, outcome)?;
            }
            EventMessage::CommandCompleted(value) => {
                let result = guest_result(&value.result);
                self.model
                    .observe_completed(event.envelope.request_id.0, &result)?;
                self.counts.terminal_accepted_commands += 1;
            }
            EventMessage::CommandFailed(value) => {
                bail!(
                    "command {} failed after admission: {}",
                    value.command_id.0,
                    value.error
                )
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod rollout_observation_tests {
    use super::{frozen_identity, validate_rollout_observation};
    use crate::protocol::{
        ActivationStartedEvent, EventEnvelope, EventMessage, IncarnationToken,
        RolloutCommittedEvent, RolloutRolledBackEvent, RolloutStartedEvent,
        RolloutTargetReadyEvent, TenantId,
    };
    use crate::transport::ReceivedEvent;

    const ATTEMPT: u32 = 42;

    fn received(sequence: u64, message: EventMessage) -> ReceivedEvent {
        ReceivedEvent {
            envelope: EventEnvelope::new(sequence, 1_u64, message),
            received_at_ns: sequence,
            raw_line: String::new(),
        }
    }

    fn start(sequence: u64) -> ReceivedEvent {
        received(
            sequence,
            EventMessage::RolloutStarted(RolloutStartedEvent {
                rollout_id: format!("rollout-{ATTEMPT}"),
            }),
        )
    }

    fn activation(sequence: u64, tenant: u32, version: &str) -> ReceivedEvent {
        let (_, artifact_sha256) = frozen_identity(version).unwrap();
        received(
            sequence,
            EventMessage::ActivationStarted(ActivationStartedEvent {
                tenant_id: TenantId(tenant),
                incarnation: IncarnationToken::new(0),
                logical_version: version.to_owned(),
                build_id: version.to_owned(),
                artifact_sha256: artifact_sha256.to_owned(),
            }),
        )
    }

    fn ready(sequence: u64, tenant: u32, version: &str) -> ReceivedEvent {
        let (_, artifact_sha256) = frozen_identity(version).unwrap();
        received(
            sequence,
            EventMessage::RolloutTargetReady(RolloutTargetReadyEvent {
                rollout_id: format!("rollout-{ATTEMPT}"),
                tenant_id: TenantId(tenant),
                incarnation: IncarnationToken::new(0),
                logical_version: version.to_owned(),
                build_id: version.to_owned(),
                artifact_sha256: artifact_sha256.to_owned(),
            }),
        )
    }

    fn commit(sequence: u64, version: &str) -> ReceivedEvent {
        received(
            sequence,
            EventMessage::RolloutCommitted(RolloutCommittedEvent {
                rollout_id: format!("rollout-{ATTEMPT}"),
                version: version.to_owned(),
            }),
        )
    }

    fn rollback(sequence: u64, restored_version: &str) -> ReceivedEvent {
        received(
            sequence,
            EventMessage::RolloutRolledBack(RolloutRolledBackEvent {
                rollout_id: format!("rollout-{ATTEMPT}"),
                restored_version: restored_version.to_owned(),
                reason: "injected tenant 7 activation failure".to_owned(),
            }),
        )
    }

    fn valid_events(activation_count: u8) -> Vec<ReceivedEvent> {
        let mut events = vec![start(1)];
        let mut sequence = 2;
        for tenant in 0..2 {
            for _ in 0..activation_count {
                events.push(activation(sequence, tenant, "B"));
                sequence += 1;
            }
            events.push(ready(sequence, tenant, "B"));
            sequence += 1;
        }
        events.push(commit(sequence, "B"));
        events
    }

    #[test]
    fn valid_rollout_accepts_one_activation_per_target() {
        validate_rollout_observation(
            &valid_events(1),
            ATTEMPT,
            "B",
            true,
            &[TenantId(0), TenantId(1)],
        )
        .unwrap();
    }

    #[test]
    fn valid_rollout_accepts_two_identical_activations_per_target() {
        validate_rollout_observation(
            &valid_events(2),
            ATTEMPT,
            "B",
            true,
            &[TenantId(0), TenantId(1)],
        )
        .unwrap();
    }

    #[test]
    fn valid_rollout_rejects_third_activation() {
        let error = validate_rollout_observation(
            &valid_events(3),
            ATTEMPT,
            "B",
            true,
            &[TenantId(0), TenantId(1)],
        )
        .unwrap_err();
        assert!(error.to_string().contains("more than two"));
    }

    #[test]
    fn failed_rollout_accepts_preflight_activation_prefix_without_ready() {
        let mut events = vec![start(1)];
        for tenant in 0..=7 {
            events.push(activation(events.len() as u64 + 1, tenant, "bad_B"));
        }
        events.push(rollback(events.len() as u64 + 1, "B"));
        let targets: Vec<_> = (0..16).map(TenantId).collect();
        validate_rollout_observation(&events, ATTEMPT, "bad_B", false, &targets).unwrap();
    }

    #[test]
    fn failed_rollout_accepts_progressive_ready_prefix() {
        let mut events = vec![start(1)];
        for tenant in 0..=7 {
            events.push(activation(events.len() as u64 + 1, tenant, "bad_B"));
            if tenant < 7 {
                events.push(ready(events.len() as u64 + 1, tenant, "bad_B"));
            }
        }
        events.push(rollback(events.len() as u64 + 1, "B"));
        let targets: Vec<_> = (0..16).map(TenantId).collect();
        validate_rollout_observation(&events, ATTEMPT, "bad_B", false, &targets).unwrap();
    }

    #[test]
    fn failed_rollout_rejects_tenant_seven_ready() {
        let events = vec![
            start(1),
            activation(2, 7, "bad_B"),
            ready(3, 7, "bad_B"),
            rollback(4, "B"),
        ];
        let error = validate_rollout_observation(&events, ATTEMPT, "bad_B", false, &[TenantId(7)])
            .unwrap_err();
        assert!(error.to_string().contains("tenant 7 ready"));
    }

    #[test]
    fn failed_rollout_rejects_rollback_without_tenant_seven_activation() {
        let events = vec![start(1), activation(2, 0, "bad_B"), rollback(3, "B")];
        let error = validate_rollout_observation(
            &events,
            ATTEMPT,
            "bad_B",
            false,
            &[TenantId(0), TenantId(7)],
        )
        .unwrap_err();
        assert!(error.to_string().contains("tenant 7 activation"));
    }
}

impl WorkloadRunner {
    #[allow(clippy::too_many_arguments)]
    fn finish_update_proofs(
        &mut self,
        plan: &[PlannedAction],
        attempt: u32,
        valid: bool,
        old_version: &str,
        final_version: &str,
        targets: Vec<TenantId>,
        anchors: BTreeMap<TenantId, CommandTiming>,
        replays: BTreeMap<TenantId, CommandTiming>,
        dispatch: UpdateDispatch,
    ) -> Result<()> {
        ensure!(
            dispatch.terminal
                == if valid {
                    ObservedRolloutTerminal::Committed
                } else {
                    ObservedRolloutTerminal::RolledBack
                },
            "rollout terminal class drift"
        );
        let proof_started_ns = replays
            .values()
            .map(|timing| timing.written_ns)
            .min()
            .ok_or_else(|| anyhow!("cache proof has no start"))?;
        ensure!(
            dispatch.drain_completed_ns <= proof_started_ns,
            "cache proof started before worker drain"
        );

        let proof_actions = update_actions(plan, attempt, PlanKind::UpdateProofSnapshot)?;
        ensure!(proof_actions.len() == 48, "read proof cardinality drift");
        let mut proof_waves = Vec::new();
        for wave in 0..3_u32 {
            let actions: Vec<_> = proof_actions
                .iter()
                .copied()
                .filter(|action| action.proof_wave == Some(wave))
                .collect();
            ensure!(actions.len() == 16, "read proof wave cardinality drift");
            let timings = self.drive_action_batch(&actions, ModelOperation::Read)?;
            let by_tenant: BTreeMap<_, _> = timings
                .into_iter()
                .map(|timing| (timing.tenant_id, timing))
                .collect();
            ensure!(by_tenant.len() == 16, "read proof tenant set drift");
            for timing in by_tenant.values() {
                ensure!(
                    timing.result.as_ref().is_some_and(|result| {
                        !result.deduplicated && result.logical_version == final_version
                    }),
                    "read proof exposed mixed or stale version"
                );
            }
            proof_waves.push(by_tenant);
        }

        let replay_terminal_ns = replays
            .values()
            .map(|timing| timing.terminal_ns)
            .max()
            .ok_or_else(|| anyhow!("cache proof has no terminal"))?;
        let proof_terminal_ns = proof_waves
            .last()
            .and_then(|wave| wave.values().map(|timing| timing.terminal_ns).max())
            .ok_or_else(|| anyhow!("read proof has no terminal"))?;
        let external_terminal_ns = dispatch
            .candidate_terminal_ns
            .max(dispatch.drain_completed_ns)
            .max(replay_terminal_ns)
            .max(proof_terminal_ns);
        let rollout_duration = positive_delta(dispatch.rollout_flush_ns, external_terminal_ns)?;
        ensure!(
            rollout_duration <= 2_000_000_000,
            "rollout external proof exceeded the absolute 2s deadline"
        );

        let first_proof = proof_waves
            .first()
            .ok_or_else(|| anyhow!("missing first read proof wave"))?;
        let mut version_observations = Vec::new();
        for tenant in &targets {
            let anchor = anchors
                .get(tenant)
                .ok_or_else(|| anyhow!("missing anchor"))?;
            let replay = replays
                .get(tenant)
                .ok_or_else(|| anyhow!("missing replay"))?;
            let worker_records = dispatch
                .records
                .get(tenant)
                .ok_or_else(|| anyhow!("missing worker trace"))?;

            let mut traces = Vec::with_capacity(worker_records.len() + 1);
            let anchor_payload = deterministic_payload(self.block, anchor.command_id, 64)?;
            traces.push(WorkerLogicalTrace {
                sequence: 0,
                operation: WorkerOperation::Read,
                command_id: anchor.command_id,
                payload_bytes: 64,
                payload_sha256: sha256_hex(&anchor_payload),
                rollout_flush_ns: dispatch.rollout_flush_ns,
                candidate_terminal_ns: dispatch.candidate_terminal_ns,
                drain_completed_ns: dispatch.drain_completed_ns,
                proof_started_ns,
                transports: vec![WorkerTransportTrace {
                    request_id: anchor.request_id.0,
                    command_id: anchor.command_id,
                    payload_sha256: sha256_hex(&anchor_payload),
                    issued_ns: anchor.written_ns,
                    outcome: WorkerTransportOutcome::AcceptedTerminal {
                        terminal_ns: anchor.terminal_ns,
                    },
                }],
            });
            let mut completion_ns = vec![anchor.terminal_ns];
            for record in worker_records {
                let timing = record
                    .timing
                    .as_ref()
                    .ok_or_else(|| anyhow!("worker logical operation did not become terminal"))?;
                let result = timing
                    .result
                    .as_ref()
                    .ok_or_else(|| anyhow!("worker completion has no guest result"))?;
                if !valid && tenant.0 == 7 {
                    ensure!(
                        result.logical_version == old_version,
                        "failed activation tenant served the bad artifact"
                    );
                }
                version_observations.push((
                    timing.terminal_event_seq,
                    timing.terminal_ns,
                    *tenant,
                    result.logical_version.clone(),
                ));
                completion_ns.push(timing.terminal_ns);
                traces.push(WorkerLogicalTrace {
                    sequence: record.sequence,
                    operation: record.operation,
                    command_id: record.command_id,
                    payload_bytes: 64,
                    payload_sha256: record.payload_sha256.clone(),
                    rollout_flush_ns: dispatch.rollout_flush_ns,
                    candidate_terminal_ns: dispatch.candidate_terminal_ns,
                    drain_completed_ns: dispatch.drain_completed_ns,
                    proof_started_ns,
                    transports: record.transports.clone(),
                });
            }
            let cardinality =
                validate_worker_trace_cardinality(self.block, attempt, tenant.0, &traces)?;
            self.counts.update_worker_logical_operations += cardinality.logical_operations;
            ensure!(
                cardinality.continuous_logical_operations == worker_records.len() as u64,
                "worker trace-derived logical cardinality drift"
            );

            completion_ns.push(replay.terminal_ns);
            let first = first_proof
                .get(tenant)
                .ok_or_else(|| anyhow!("missing first proof completion"))?;
            completion_ns.push(first.terminal_ns);
            completion_ns.sort_unstable();
            let mut maximum_gap = 0_u64;
            for pair in completion_ns.windows(2) {
                if pair[1] >= dispatch.rollout_flush_ns && pair[0] <= first.terminal_ns {
                    maximum_gap = maximum_gap.max(positive_delta(pair[0], pair[1])?);
                }
            }
            ensure!(maximum_gap > 0, "update unavailability gap is empty");
            if valid {
                self.timings
                    .valid_update_unavailability_ns
                    .push(maximum_gap);
            } else {
                self.timings
                    .failed_update_unavailability_ns
                    .push(maximum_gap);
            }
        }

        for wave in &proof_waves {
            for (tenant, timing) in wave {
                let result = timing
                    .result
                    .as_ref()
                    .ok_or_else(|| anyhow!("proof completion has no result"))?;
                version_observations.push((
                    timing.terminal_event_seq,
                    timing.terminal_ns,
                    *tenant,
                    result.logical_version.clone(),
                ));
            }
        }
        let mixed_window = mixed_window_ns(old_version, final_version, &version_observations)?;
        if valid {
            self.timings.valid_rollout_ns.push(rollout_duration);
            self.timings.valid_mixed_window_ns.push(mixed_window);
        } else {
            self.timings.failed_rollout_ns.push(rollout_duration);
            self.timings.failed_mixed_window_ns.push(mixed_window);
        }
        Ok(())
    }
}

fn mixed_window_ns(
    old_version: &str,
    final_version: &str,
    observations: &[(u64, u64, TenantId, String)],
) -> Result<u64> {
    let mut ordered = observations.to_vec();
    ordered.sort_by_key(|entry| entry.0);
    let mut versions: BTreeMap<_, _> = (0..16)
        .map(|tenant| (TenantId(tenant), old_version.to_owned()))
        .collect();
    let mut first_entry = None;
    let mut last_exit = None;
    let mut mixed = false;
    for (_, timestamp_ns, tenant, version) in ordered {
        versions.insert(tenant, version);
        let now_mixed = versions.values().collect::<BTreeSet<_>>().len() > 1;
        if now_mixed && !mixed && first_entry.is_none() {
            first_entry = Some(timestamp_ns);
        }
        if !now_mixed && mixed {
            last_exit = Some(timestamp_ns);
        }
        mixed = now_mixed;
    }
    ensure!(!mixed, "tenant version vector remained mixed at read proof");
    ensure!(
        versions.values().all(|version| version == final_version),
        "read proof did not converge to the final version"
    );
    match first_entry {
        None => Ok(0),
        Some(start) => positive_delta(
            start,
            last_exit.ok_or_else(|| anyhow!("mixed vector never exited"))?,
        ),
    }
}

fn validate_rollout_observation(
    events: &[ReceivedEvent],
    attempt: u32,
    target_version: &str,
    valid: bool,
    targets: &[TenantId],
) -> Result<()> {
    let rollout_id = format!("rollout-{attempt}");
    let (_, expected_sha256) = frozen_identity(target_version)?;
    let target_set: BTreeSet<_> = targets.iter().copied().collect();
    let mut starts = 0_u32;
    let mut activations = BTreeMap::new();
    let mut ready = BTreeSet::new();
    let mut terminals = 0_u32;
    for event in events {
        match &event.envelope.message {
            EventMessage::RolloutStarted(value) => {
                ensure!(value.rollout_id == rollout_id, "rollout start ID drift");
                starts += 1;
            }
            EventMessage::ActivationStarted(value) => {
                ensure!(
                    target_set.contains(&value.tenant_id),
                    "activation target drift"
                );
                ensure!(
                    value.logical_version == target_version
                        && value.build_id == target_version
                        && value.artifact_sha256 == expected_sha256,
                    "activation identity drift"
                );
                let activation_count = activations.entry(value.tenant_id).or_insert(0_u8);
                *activation_count = activation_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("activation marker count overflow"))?;
                ensure!(
                    *activation_count <= 2,
                    "tenant emitted more than two activation markers"
                );
            }
            EventMessage::RolloutTargetReady(value) => {
                ensure!(value.rollout_id == rollout_id, "target-ready ID drift");
                ensure!(target_set.contains(&value.tenant_id), "ready target drift");
                ensure!(
                    value.logical_version == target_version
                        && value.build_id == target_version
                        && value.artifact_sha256 == expected_sha256,
                    "ready artifact identity drift"
                );
                ensure!(
                    ready.insert(value.tenant_id),
                    "duplicate target-ready marker"
                );
            }
            EventMessage::RolloutCommitted(value) => {
                ensure!(valid && value.rollout_id == rollout_id, "unexpected commit");
                terminals += 1;
            }
            EventMessage::RolloutRolledBack(value) => {
                ensure!(
                    !valid && value.rollout_id == rollout_id,
                    "unexpected rollback"
                );
                terminals += 1;
            }
            EventMessage::RolloutInDoubt(_) | EventMessage::RolloutRejected(_) => {
                bail!("unsafe or rejected rollout terminal")
            }
            _ => bail!("non-rollout event was retained in rollout observation"),
        }
    }
    ensure!(
        starts == 1 && terminals == 1,
        "rollout start/terminal cardinality drift"
    );
    if valid {
        let activated_targets: BTreeSet<_> = activations.keys().copied().collect();
        ensure!(
            activated_targets == target_set,
            "valid rollout did not activate every target"
        );
        ensure!(
            ready == target_set,
            "valid rollout did not ready every target"
        );
    } else {
        ensure!(
            activations.contains_key(&TenantId(7)),
            "failed rollout never exercised tenant 7 activation"
        );
        ensure!(
            !ready.contains(&TenantId(7)),
            "failed rollout reported tenant 7 ready"
        );
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupRssTarget {
    pub cycle: u32,
    pub teardown_terminal_ns: u64,
    pub post_barrier_ns: u64,
    pub target_monotonic_ns: [u64; 3],
}

impl WorkloadRunner {
    fn quiesce(&mut self) -> Result<u64> {
        let (_, events) = self.round_trip_control(ControlMessage::Quiesce(QuiesceControl {}))?;
        ensure!(
            events.len() == 1 && matches!(events[0].envelope.message, EventMessage::Quiesced(_)),
            "quiesce did not return exactly one quiesced terminal"
        );
        for tenant in 0..32 {
            ensure!(
                self.model.accepted_outstanding(TenantId(tenant)) == 0,
                "quiesce left accepted commands outstanding"
            );
        }
        terminal_timestamp(&events)
    }

    fn teardown_tenant(&mut self, tenant: TenantId, incarnation: u64) -> Result<u64> {
        let (_, events) =
            self.round_trip_control(ControlMessage::TeardownTenant(TeardownTenantControl {
                tenant_id: tenant,
                incarnation: IncarnationToken::new(incarnation),
            }))?;
        let torn_down = events
            .iter()
            .find_map(|event| match &event.envelope.message {
                EventMessage::TenantTornDown(value) => Some(value),
                _ => None,
            });
        let value = torn_down.ok_or_else(|| anyhow!("teardown emitted no tenant_torn_down"))?;
        ensure!(
            value.tenant_id == tenant && value.incarnation.value() == incarnation,
            "teardown echoed the wrong endpoint"
        );
        self.model.teardown(tenant, incarnation)?;
        terminal_timestamp(&events)
    }

    pub fn run_main_teardown(&mut self, plan: &[PlannedAction]) -> Result<u64> {
        let actions = actions_of(plan, PlanKind::MainTeardown);
        ensure!(actions.len() == 32, "main teardown cardinality drift");
        let mut last_terminal_ns = 0;
        for action in actions {
            let tenant = TenantId(action.tenant_id.ok_or_else(|| anyhow!("missing tenant"))?);
            let incarnation = self
                .model
                .incarnation(tenant)
                .ok_or_else(|| anyhow!("main teardown target missing"))?;
            last_terminal_ns = self.teardown_tenant(tenant, incarnation)?;
        }
        ensure!(
            self.model.active_tenants() == 0,
            "main teardown left active tenants"
        );
        ensure!(
            actions_of(plan, PlanKind::MainActiveZero).len() == 1,
            "main active-zero proof cardinality drift"
        );
        Ok(last_terminal_ns)
    }

    pub fn run_cleanup(&mut self, plan: &[PlannedAction]) -> Result<Vec<CleanupRssTarget>> {
        ensure!(
            self.model.active_tenants() == 0,
            "cleanup requires active count zero"
        );
        let mut retained_targets = Vec::new();
        for cycle in 0..35_u32 {
            let creates = cleanup_actions(plan, cycle, PlanKind::CleanupCreate)?;
            ensure!(creates.len() == 32, "cleanup create cardinality drift");
            for action in creates {
                let tenant = TenantId(action.tenant_id.ok_or_else(|| anyhow!("missing tenant"))?);
                self.create_tenant_unmeasured(tenant)?;
            }

            let increments = cleanup_actions(plan, cycle, PlanKind::CleanupIncrement)?;
            ensure!(
                increments.len() == 32,
                "cleanup increment cardinality drift"
            );
            let increment_timings =
                self.drive_action_batch(&increments, ModelOperation::Increment(1))?;
            ensure!(
                increment_timings.iter().all(|timing| {
                    timing
                        .result
                        .as_ref()
                        .is_some_and(|result| !result.deduplicated && result.counter == 1)
                }),
                "cleanup increment did not start from zero exactly once"
            );
            self.quiesce()?;

            let snapshots = cleanup_actions(plan, cycle, PlanKind::CleanupSnapshot)?;
            ensure!(snapshots.len() == 32, "cleanup snapshot cardinality drift");
            for action in snapshots {
                let tenant = TenantId(action.tenant_id.ok_or_else(|| anyhow!("missing tenant"))?);
                let (snapshot, _) = self.snapshot_tenant(tenant)?;
                ensure!(
                    snapshot.counter == 1,
                    "cleanup snapshot lost accepted state"
                );
            }

            let teardowns = cleanup_actions(plan, cycle, PlanKind::CleanupTeardown)?;
            ensure!(teardowns.len() == 32, "cleanup teardown cardinality drift");
            let old_incarnations: BTreeMap<_, _> = teardowns
                .iter()
                .map(|action| {
                    let tenant =
                        TenantId(action.tenant_id.ok_or_else(|| anyhow!("missing tenant"))?);
                    let incarnation = self
                        .model
                        .incarnation(tenant)
                        .ok_or_else(|| anyhow!("cleanup teardown target missing"))?;
                    Ok((tenant, incarnation))
                })
                .collect::<Result<_>>()?;
            let mut last_teardown_ns = 0;
            for action in teardowns {
                let tenant = TenantId(action.tenant_id.ok_or_else(|| anyhow!("missing tenant"))?);
                last_teardown_ns = self.teardown_tenant(
                    tenant,
                    *old_incarnations
                        .get(&tenant)
                        .ok_or_else(|| anyhow!("missing old incarnation"))?,
                )?;
            }

            let stale_actions = cleanup_actions(plan, cycle, PlanKind::CleanupStaleProbe)?;
            ensure!(stale_actions.len() == 32, "cleanup stale cardinality drift");
            let mut pending = BTreeMap::new();
            for action in stale_actions {
                let tenant = TenantId(action.tenant_id.ok_or_else(|| anyhow!("missing tenant"))?);
                let incarnation = *old_incarnations
                    .get(&tenant)
                    .ok_or_else(|| anyhow!("missing stale incarnation"))?;
                let control =
                    self.command_from_action(action, incarnation, ModelOperation::Increment(1))?;
                let command = self.send_command_control(control)?;
                pending.insert(command.control.request_id, command);
            }
            let stale = self.drive_pending(pending)?;
            ensure!(stale.len() == 32, "cleanup stale result cardinality drift");
            for timing in stale.values() {
                validate_expected_terminal(timing, ExpectedTerminal::TenantMissing)?;
            }
            let post_barrier_ns = stale
                .values()
                .map(|timing| timing.terminal_ns)
                .max()
                .ok_or_else(|| anyhow!("cleanup stale proof has no terminal"))?;
            ensure!(
                self.model.active_tenants() == 0,
                "cleanup left active tenants"
            );
            ensure!(
                cleanup_actions(plan, cycle, PlanKind::CleanupActiveZero)?.len() == 1,
                "cleanup active-zero cardinality drift"
            );

            let rss_actions = cleanup_actions(plan, cycle, PlanKind::CleanupPostRss)?;
            let offsets: Vec<_> = rss_actions
                .iter()
                .map(|action| action.sample_offset_ms)
                .collect();
            ensure!(
                offsets == [Some(1_000), Some(1_100), Some(1_200)],
                "cleanup RSS offsets drift"
            );
            let targets = [
                last_teardown_ns + 1_000_000_000,
                last_teardown_ns + 1_100_000_000,
                last_teardown_ns + 1_200_000_000,
            ];
            ensure!(
                post_barrier_ns < targets[0],
                "old-endpoint and active-zero proof overlapped post-cleanup RSS sampling"
            );
            wait_until_monotonic(
                self.session.process().measurement_parts().0,
                targets[2] + 20_000_000,
            );
            if cycle >= 5 {
                retained_targets.push(CleanupRssTarget {
                    cycle,
                    teardown_terminal_ns: last_teardown_ns,
                    post_barrier_ns,
                    target_monotonic_ns: targets,
                });
            }
        }
        ensure!(
            retained_targets.len() == 30,
            "retained cleanup RSS cardinality drift"
        );
        Ok(retained_targets)
    }

    pub fn shutdown(&mut self) -> Result<u64> {
        ensure!(
            self.model.active_tenants() == 0,
            "shutdown with active tenants"
        );
        let (_, events) = self.round_trip_control(ControlMessage::Shutdown(ShutdownControl {}))?;
        ensure!(
            events.len() == 1
                && matches!(
                    events[0].envelope.message,
                    EventMessage::ShutdownComplete(_)
                ),
            "shutdown did not return exactly one shutdown_complete terminal"
        );
        terminal_timestamp(&events)
    }
}

fn cleanup_actions(
    plan: &[PlannedAction],
    cycle: u32,
    kind: PlanKind,
) -> Result<Vec<&PlannedAction>> {
    let actions: Vec<_> = plan
        .iter()
        .filter(|action| action.kind == kind && action.cycle == Some(cycle))
        .collect();
    ensure!(
        !actions.is_empty(),
        "missing {kind:?} actions for cleanup cycle {cycle}"
    );
    Ok(actions)
}

fn wait_until_monotonic(clock: crate::transport::MonotonicClock, target_ns: u64) {
    loop {
        let now = clock.now_ns();
        if now >= target_ns {
            return;
        }
        let remaining = target_ns - now;
        thread::sleep(Duration::from_nanos(remaining.min(50_000_000)));
    }
}

fn guest_result(result: &CommandResult) -> GuestBusinessResult {
    GuestBusinessResult {
        incarnation: result.incarnation.value(),
        generation: result.generation.0,
        counter: result.counter,
        logical_version: result.logical_version.clone(),
        build_id: result.build_id.clone(),
        deduplicated: result.deduplicated,
        business_result_sha256: result.business_result_sha256.clone(),
    }
}

fn validate_expected_terminal(timing: &CommandTiming, expected: ExpectedTerminal) -> Result<()> {
    let matches = match expected {
        ExpectedTerminal::Completed => timing.result.is_some() && timing.rejection.is_none(),
        ExpectedTerminal::Backpressure => {
            timing.result.is_none() && timing.rejection == Some(RejectionReason::Backpressure)
        }
        ExpectedTerminal::PayloadTooLarge => {
            timing.result.is_none() && timing.rejection == Some(RejectionReason::PayloadTooLarge)
        }
        ExpectedTerminal::StaleIncarnation => {
            timing.result.is_none() && timing.rejection == Some(RejectionReason::StaleIncarnation)
        }
        ExpectedTerminal::TenantMissing => {
            timing.result.is_none() && timing.rejection == Some(RejectionReason::TenantMissing)
        }
    };
    ensure!(matches, "command terminal did not match {expected:?}");
    Ok(())
}

pub fn terminal_timestamp(events: &[ReceivedEvent]) -> Result<u64> {
    events
        .last()
        .map(|event| event.received_at_ns)
        .ok_or_else(|| anyhow!("request emitted no events"))
}

pub fn positive_delta(start_ns: u64, finish_ns: u64) -> Result<u64> {
    let duration = finish_ns
        .checked_sub(start_ns)
        .ok_or_else(|| anyhow!("timestamp order is negative"))?;
    ensure!(duration > 0, "duration is not positive");
    Ok(duration)
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn frozen_identity(version: &str) -> Result<(&'static str, &'static str)> {
    match version {
        "A" => Ok(("guest-artifacts/tenant-a.wasm", ARTIFACT_A_SHA256)),
        "B" => Ok(("guest-artifacts/tenant-b.wasm", ARTIFACT_B_SHA256)),
        "bad_A" => Ok(("guest-artifacts/tenant-bad-a.wasm", ARTIFACT_BAD_A_SHA256)),
        "bad_B" => Ok(("guest-artifacts/tenant-bad-b.wasm", ARTIFACT_BAD_B_SHA256)),
        _ => bail!("unknown frozen artifact {version}"),
    }
}

pub fn expected_candidate_order(block: u32) -> Result<[CandidateKind; 3]> {
    crate::workload::block_order(block)
        .map(|order| order.candidates)
        .map_err(Into::into)
}

pub fn assert_scenario_identity(value: &str) -> Result<()> {
    ensure!(
        value == EMBEDDED_SCENARIO_SHA256,
        "scenario digest does not match frozen workload"
    );
    Ok(())
}

impl WorkloadRunner {
    pub fn run_initial_normal_pressure(&mut self, plan: &[PlannedAction]) -> Result<u64> {
        let ready_terminal_ns = self.run_initial_create(plan)?;
        self.run_normal_pressure(plan)?;
        Ok(ready_terminal_ns)
    }

    pub fn run_initial_create(&mut self, plan: &[PlannedAction]) -> Result<u64> {
        let initial = actions_of(plan, PlanKind::InitialCreate);
        ensure!(initial.len() == 32, "initial create cardinality drift");
        let mut pending = BTreeMap::new();
        for action in initial {
            let tenant = TenantId(action.tenant_id.ok_or_else(|| anyhow!("missing tenant"))?);
            let incarnation = self.model.expected_create_incarnation(tenant)?;
            let control = self.envelope(ControlMessage::CreateTenant(CreateTenantControl {
                tenant_id: tenant,
                incarnation: IncarnationToken::new(incarnation),
                logical_version: "A".to_owned(),
                build_id: "A".to_owned(),
                artifact_sha256: ARTIFACT_A_SHA256.to_owned(),
            }))?;
            self.counts.controls_sent += 1;
            let sent = self
                .session
                .send(&control)
                .with_context(|| format!("send concurrent create for tenant {}", tenant.0))?;
            let phase = self
                .session
                .request_phase(control.request_id)
                .ok_or_else(|| anyhow!("registered create has no phase"))?;
            let timeout = self
                .session
                .request_timeout(control.request_id, phase)
                .ok_or_else(|| anyhow!("registered create has no timeout"))?;
            pending.insert(
                control.request_id,
                PendingCreate {
                    tenant,
                    incarnation,
                    sent,
                    phase,
                    deadline: Instant::now() + timeout,
                    created: false,
                },
            );
        }

        let mut ready_terminal_ns = 0;
        while !pending.is_empty() {
            let (next_request, deadline) = pending
                .iter()
                .min_by_key(|(_, create)| create.deadline)
                .map(|(request_id, create)| (*request_id, create.deadline))
                .ok_or_else(|| anyhow!("concurrent create pending set disappeared"))?;
            let now = Instant::now();
            if now >= deadline {
                self.session.expire_request(next_request)?;
                bail!("concurrent create request {} timed out", next_request.0);
            }
            let event = self
                .session
                .receive(deadline.saturating_duration_since(now))
                .with_context(|| format!("drive concurrent create request {}", next_request.0))?;
            self.observe_events(std::slice::from_ref(&event))?;
            let request_id = event.envelope.request_id;
            let create = pending
                .get_mut(&request_id)
                .ok_or_else(|| anyhow!("create phase received an unrelated request event"))?;
            match &event.envelope.message {
                EventMessage::TenantCreated(value) => {
                    ensure!(
                        value.tenant_id == create.tenant
                            && value.incarnation.value() == create.incarnation,
                        "create echoed a forged tenant identity"
                    );
                    ensure!(!create.created, "create emitted tenant_created twice");
                    create.created = true;
                }
                EventMessage::ActivationStarted(_) => {}
                EventMessage::TenantReady(value) => {
                    ensure!(
                        value.tenant_id == create.tenant
                            && value.incarnation.value() == create.incarnation,
                        "ready echoed a forged tenant identity"
                    );
                }
                other => bail!("unexpected concurrent create event: {other:?}"),
            }

            let new_phase = self
                .session
                .request_phase(request_id)
                .ok_or_else(|| anyhow!("create request lost its phase"))?;
            if new_phase == RequestPhase::Terminal {
                let create = pending
                    .remove(&request_id)
                    .ok_or_else(|| anyhow!("terminal create disappeared"))?;
                ensure!(create.created, "create became ready without tenant_created");
                self.model
                    .create(create.tenant, create.incarnation, "A", "A")
                    .context("model rejected concurrent tenant create")?;
                let duration = positive_delta(create.sent.sent_at_ns, event.received_at_ns)?;
                self.timings.warm_create_ns.push(duration);
                ready_terminal_ns = ready_terminal_ns.max(event.received_at_ns);
            } else if new_phase != create.phase {
                create.phase = new_phase;
                let timeout = self
                    .session
                    .request_timeout(request_id, new_phase)
                    .ok_or_else(|| anyhow!("create phase has no timeout"))?;
                create.deadline = Instant::now() + timeout;
            }
        }
        ensure!(
            self.timings.warm_create_ns.len() == 32,
            "warm create sample drift"
        );
        Ok(ready_terminal_ns)
    }

    pub fn run_normal_pressure(&mut self, plan: &[PlannedAction]) -> Result<()> {
        let warmup = actions_of(plan, PlanKind::NormalWarmupIncrement);
        ensure!(warmup.len() == 1_024, "warmup cardinality drift");
        for round in warmup.chunks(32) {
            let completed = self.drive_action_batch(round, ModelOperation::Increment(1))?;
            ensure!(
                completed.iter().all(|timing| timing.result.is_some()),
                "warmup command did not complete"
            );
        }

        let measured = actions_of(plan, PlanKind::NormalIncrement);
        ensure!(measured.len() == 10_240, "normal cardinality drift");
        let mut normal_start = None;
        let mut normal_finish = 0;
        for round in measured.chunks(32) {
            let completed = self.drive_action_batch(round, ModelOperation::Increment(1))?;
            for timing in completed {
                validate_expected_terminal(&timing, ExpectedTerminal::Completed)?;
                normal_start = Some(
                    normal_start
                        .map_or(timing.written_ns, |start: u64| start.min(timing.written_ns)),
                );
                normal_finish = normal_finish.max(timing.terminal_ns);
                self.timings
                    .normal_ns
                    .push(positive_delta(timing.written_ns, timing.terminal_ns)?);
            }
        }
        self.timings.normal_total_ns = positive_delta(
            normal_start.ok_or_else(|| anyhow!("normal phase has no start"))?,
            normal_finish,
        )?;

        self.run_delayed_duplicates(plan)?;
        self.run_pressure(plan)?;
        Ok(())
    }

    fn run_delayed_duplicates(&mut self, plan: &[PlannedAction]) -> Result<()> {
        let delayed: Vec<_> = plan
            .iter()
            .filter(|action| {
                matches!(
                    action.kind,
                    PlanKind::DelayedOriginalDuplicate
                        | PlanKind::CrossTenantFirst
                        | PlanKind::CrossTenantDuplicate
                )
            })
            .collect();
        ensure!(delayed.len() == 240, "delayed duplicate cardinality drift");
        for group in delayed.chunks(3) {
            ensure!(
                group[0].kind == PlanKind::DelayedOriginalDuplicate
                    && group[1].kind == PlanKind::CrossTenantFirst
                    && group[2].kind == PlanKind::CrossTenantDuplicate,
                "delayed duplicate order drift"
            );
            for (index, action) in group.iter().enumerate() {
                let tenant = TenantId(action.tenant_id.ok_or_else(|| anyhow!("missing tenant"))?);
                let incarnation = self
                    .model
                    .incarnation(tenant)
                    .ok_or_else(|| anyhow!("delayed target missing"))?;
                let timing = self.round_trip_action_command(
                    action,
                    incarnation,
                    ModelOperation::Increment(1),
                    ExpectedTerminal::Completed,
                )?;
                let result = timing
                    .result
                    .ok_or_else(|| anyhow!("delayed command has no result"))?;
                ensure!(
                    result.deduplicated == (index != 1),
                    "delayed duplicate cache scope is wrong"
                );
            }
        }
        Ok(())
    }

    fn run_pressure(&mut self, plan: &[PlannedAction]) -> Result<()> {
        let oversize = one_action(plan, PlanKind::Oversize1025Rejected)?;
        let tenant = TenantId(
            oversize
                .tenant_id
                .ok_or_else(|| anyhow!("missing tenant"))?,
        );
        let incarnation = self
            .model
            .incarnation(tenant)
            .ok_or_else(|| anyhow!("pressure target missing"))?;
        self.round_trip_action_command(
            oversize,
            incarnation,
            ModelOperation::Read,
            ExpectedTerminal::PayloadTooLarge,
        )?;

        let boundary = one_action(plan, PlanKind::Boundary1024Accepted)?;
        self.round_trip_action_command(
            boundary,
            incarnation,
            ModelOperation::Read,
            ExpectedTerminal::Completed,
        )?;

        let gate_close = one_action(plan, PlanKind::PressureGateClose)?;
        ensure!(gate_close.tenant_id == Some(tenant.0), "gate target drift");
        self.round_trip_control(ControlMessage::SetDequeueGate(SetDequeueGateControl {
            tenant_id: tenant,
            incarnation: IncarnationToken::new(incarnation),
            closed: true,
        }))?;
        self.model.set_dequeue_gate(tenant, incarnation, true)?;

        let accepted = actions_of(plan, PlanKind::PressureAccepted);
        ensure!(accepted.len() == 64, "pressure accepted cardinality drift");
        let mut pending = BTreeMap::new();
        for action in accepted {
            let (request_id, command) =
                self.send_action_command(action, incarnation, ModelOperation::Increment(1))?;
            pending.insert(request_id, command);
        }
        self.drive_until_all_accepted(&mut pending)?;
        let mut by_acceptance: Vec<_> = pending
            .values()
            .map(|item| {
                let accepted_ns = item
                    .accepted_ns
                    .ok_or_else(|| anyhow!("accepted pressure command lacks timestamp"))?;
                self.timings
                    .pressure_admission_ns
                    .push(positive_delta(item.sent.sent_at_ns, accepted_ns)?);
                Ok((accepted_ns, item.control.request_id))
            })
            .collect::<Result<_>>()?;
        by_acceptance.sort_by_key(|entry| entry.0);
        ensure!(by_acceptance.len() == 64, "pressure admission count drift");

        let overflow = one_action(plan, PlanKind::PressureOverflowRejected)?;
        self.round_trip_action_command(
            overflow,
            incarnation,
            ModelOperation::Increment(1),
            ExpectedTerminal::Backpressure,
        )?;

        self.round_trip_control(ControlMessage::SetDequeueGate(SetDequeueGateControl {
            tenant_id: tenant,
            incarnation: IncarnationToken::new(incarnation),
            closed: false,
        }))?;
        self.model.set_dequeue_gate(tenant, incarnation, false)?;

        let drained = self.drive_pending(pending)?;
        let mut fifo: Vec<_> = drained.values().collect();
        fifo.sort_by_key(|timing| timing.terminal_ns);
        let expected_ids: Vec<_> = (0..64).map(|index| 1_000_000 + index).collect();
        let observed_ids: Vec<_> = fifo.iter().map(|timing| timing.command_id).collect();
        ensure!(observed_ids == expected_ids, "pressure drain was not FIFO");
        ensure!(
            fifo.iter().all(|timing| timing.result.is_some()),
            "accepted pressure command did not become terminal"
        );

        let retry = one_action(plan, PlanKind::PressureRetry)?;
        ensure!(
            retry.command_id == overflow.command_id
                && retry.payload_sha256 == overflow.payload_sha256,
            "pressure retry changed command identity or bytes"
        );
        self.round_trip_action_command(
            retry,
            incarnation,
            ModelOperation::Increment(1),
            ExpectedTerminal::Completed,
        )?;
        Ok(())
    }

    fn drive_until_all_accepted(
        &mut self,
        pending: &mut BTreeMap<RequestId, PendingCommand>,
    ) -> Result<()> {
        while pending.values().any(|item| item.accepted_ns.is_none()) {
            let now = Instant::now();
            let next_deadline = pending
                .values()
                .filter(|item| item.accepted_ns.is_none())
                .map(|item| item.deadline)
                .min()
                .ok_or_else(|| anyhow!("no admission deadline"))?;
            ensure!(now < next_deadline, "pressure admission deadline expired");
            let event = self
                .session
                .receive(next_deadline.saturating_duration_since(now))
                .context("receiving pressure admission")?;
            self.counts.events_received += 1;
            let request_id = event.envelope.request_id;
            self.observe_event(&event)?;
            let item = pending
                .get_mut(&request_id)
                .ok_or_else(|| anyhow!("pressure emitted event for unknown request"))?;
            match &event.envelope.message {
                EventMessage::CommandAccepted(_) => {
                    item.accepted_ns = Some(event.received_at_ns);
                    item.deadline = Instant::now() + Duration::from_millis(250);
                }
                EventMessage::CommandRejected(value) => {
                    bail!(
                        "one of the first 64 pressure commands was rejected: {:?}",
                        value.reason
                    )
                }
                EventMessage::CommandCompleted(_) => {
                    bail!("pressure command completed while dequeue gate was closed")
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn actions_of(plan: &[PlannedAction], kind: PlanKind) -> Vec<&PlannedAction> {
    plan.iter().filter(|action| action.kind == kind).collect()
}

fn authority_probe_from_label(label: &str) -> Result<Probe> {
    match label {
        "wasi_p1_fs_read" => Ok(Probe::WasiP1FsRead),
        "wasi_p1_fs_mutate" => Ok(Probe::WasiP1FsMutate),
        "lunatic_tcp" => Ok(Probe::LunaticTcp),
        "lunatic_udp" => Ok(Probe::LunaticUdp),
        "lunatic_sqlite_create" => Ok(Probe::LunaticSqliteCreate),
        "extism_http" => Ok(Probe::ExtismHttp),
        other => bail!("unknown authority plan label {other}"),
    }
}

fn one_action(plan: &[PlannedAction], kind: PlanKind) -> Result<&PlannedAction> {
    let actions = actions_of(plan, kind);
    ensure!(actions.len() == 1, "{kind:?} must occur exactly once");
    Ok(actions[0])
}

struct FaultPairResult {
    fault_written_ns: u64,
    fault_events: Vec<ReceivedEvent>,
    sibling: CommandTiming,
}

struct FaultExpectation {
    fault_id: u64,
    tenant: TenantId,
    old_incarnation: u64,
    replacement_incarnation: u64,
    cpu: bool,
    write_ns: u64,
}

impl WorkloadRunner {
    pub fn run_faults(&mut self, plan: &[PlannedAction]) -> Result<()> {
        for cycle in 0..30 {
            self.run_one_fault(plan, cycle, false)?;
            self.run_one_fault(plan, cycle, true)?;
        }
        Ok(())
    }

    fn run_one_fault(&mut self, plan: &[PlannedAction], cycle: u32, cpu: bool) -> Result<()> {
        let label = if cpu { "cpu" } else { "trap" };
        let fault_kind = if cpu {
            PlanKind::CpuFault
        } else {
            PlanKind::TrapFault
        };
        let seed = fault_action(plan, cycle, PlanKind::FaultSeed, Some(label))?;
        let tenant = TenantId(
            seed.tenant_id
                .ok_or_else(|| anyhow!("fault seed has no tenant"))?,
        );
        let old_incarnation = self
            .model
            .incarnation(tenant)
            .ok_or_else(|| anyhow!("fault target missing"))?;
        self.round_trip_action_command(
            seed,
            old_incarnation,
            ModelOperation::Increment(1),
            ExpectedTerminal::Completed,
        )?;

        let fault = fault_action(plan, cycle, fault_kind, None)?;
        ensure!(fault.tenant_id == Some(tenant.0), "fault target drift");
        let sibling_action = fault_action(plan, cycle, PlanKind::SiblingProbe, Some(label))?;
        let replacement_incarnation = old_incarnation
            .checked_add(1)
            .ok_or_else(|| anyhow!("fault incarnation overflow"))?;
        let fault_id = u64::from(cycle) * 2 + u64::from(cpu) + 1;
        let pair = self.send_fault_and_sibling(
            fault_id,
            tenant,
            old_incarnation,
            replacement_incarnation,
            cpu,
            sibling_action,
        )?;
        validate_expected_terminal(&pair.sibling, ExpectedTerminal::Completed)?;
        self.timings.sibling_ns.push(positive_delta(
            pair.sibling.written_ns,
            pair.sibling.terminal_ns,
        )?);

        let ready_ns = validate_fault_events(
            &pair.fault_events,
            &FaultExpectation {
                fault_id,
                tenant,
                old_incarnation,
                replacement_incarnation,
                cpu,
                write_ns: pair.fault_written_ns,
            },
            &mut self.timings,
        )?;
        ensure!(
            ready_ns >= pair.fault_written_ns,
            "replacement ready preceded fault write"
        );
        self.model.recover(tenant, replacement_incarnation)?;

        let stale = fault_action(plan, cycle, PlanKind::OldIncarnationStale, Some(label))?;
        self.round_trip_action_command(
            stale,
            old_incarnation,
            ModelOperation::Increment(1),
            ExpectedTerminal::StaleIncarnation,
        )?;
        let fresh = fault_action(plan, cycle, PlanKind::NewIncarnationMutation, Some(label))?;
        let fresh_timing = self.round_trip_action_command(
            fresh,
            replacement_incarnation,
            ModelOperation::Increment(1),
            ExpectedTerminal::Completed,
        )?;
        ensure!(
            fresh_timing
                .result
                .as_ref()
                .is_some_and(|result| !result.deduplicated && result.counter == 1),
            "replacement command was not fresh"
        );
        let duplicate = fault_action(plan, cycle, PlanKind::NewIncarnationDuplicate, Some(label))?;
        let duplicate_timing = self.round_trip_action_command(
            duplicate,
            replacement_incarnation,
            ModelOperation::Increment(1),
            ExpectedTerminal::Completed,
        )?;
        ensure!(
            duplicate_timing
                .result
                .as_ref()
                .is_some_and(|result| result.deduplicated && result.counter == 1),
            "replacement duplicate did not return the fresh cached result"
        );
        Ok(())
    }

    fn send_fault_and_sibling(
        &mut self,
        fault_id: u64,
        tenant: TenantId,
        incarnation: u64,
        replacement_incarnation: u64,
        cpu: bool,
        sibling_action: &PlannedAction,
    ) -> Result<FaultPairResult> {
        let fault = self.envelope(ControlMessage::InjectFault(InjectFaultControl {
            fault_id: DecimalU64(fault_id),
            tenant_id: tenant,
            incarnation: IncarnationToken::new(incarnation),
            expected_replacement_incarnation: IncarnationToken::new(replacement_incarnation),
            replacement_logical_version: "A".to_owned(),
            replacement_build_id: "A".to_owned(),
            replacement_artifact_sha256: ARTIFACT_A_SHA256.to_owned(),
            fault: if cpu {
                FaultKind::CpuHog(CpuHogFault::default())
            } else {
                FaultKind::Trap(TrapFault::default())
            },
        }))?;
        self.counts.controls_sent += 1;
        let fault_sent = self.session.send(&fault).context("send fault control")?;

        let sibling_tenant = TenantId(
            sibling_action
                .tenant_id
                .ok_or_else(|| anyhow!("sibling action has no tenant"))?,
        );
        let sibling_incarnation = self
            .model
            .incarnation(sibling_tenant)
            .ok_or_else(|| anyhow!("sibling target missing"))?;
        let (_, mut sibling) =
            self.send_action_command(sibling_action, sibling_incarnation, ModelOperation::Read)?;
        let sibling_request = sibling.control.request_id;

        let overall_deadline = Instant::now() + Duration::from_millis(500);
        let mut fault_done = false;
        let mut sibling_done = false;
        let mut fault_events = Vec::new();
        let mut sibling_timing = None;
        while !(fault_done && sibling_done) {
            let now = Instant::now();
            let deadline = if sibling_done {
                overall_deadline
            } else {
                overall_deadline.min(sibling.deadline)
            };
            ensure!(now < deadline, "fault/sibling deadline expired");
            let event = self
                .session
                .receive(deadline.saturating_duration_since(now))
                .context("receiving fault/sibling event")?;
            self.counts.events_received += 1;
            let request_id = event.envelope.request_id;
            self.observe_event(&event)?;

            if request_id == fault.request_id {
                fault_events.push(event.clone());
                fault_done = self.session.oracle().is_terminal(request_id);
            } else if request_id == sibling_request {
                match &event.envelope.message {
                    EventMessage::CommandAccepted(_) => {
                        sibling.accepted_ns = Some(event.received_at_ns);
                        sibling.deadline = Instant::now() + Duration::from_millis(250);
                    }
                    EventMessage::CommandCompleted(value) => {
                        sibling.result = Some(guest_result(&value.result));
                    }
                    EventMessage::CommandRejected(value) => {
                        sibling.rejection = Some(value.reason);
                    }
                    EventMessage::CommandFailed(value) => {
                        bail!("sibling command failed: {}", value.error)
                    }
                    _ => {}
                }
                sibling_done = self.session.oracle().is_terminal(request_id);
                if sibling_done {
                    sibling_timing = Some(CommandTiming {
                        request_id,
                        tenant_id: sibling_tenant,
                        command_id: match &sibling.control.message {
                            ControlMessage::Command(control) => control.command_id.0,
                            _ => unreachable!("sibling is a command"),
                        },
                        written_ns: sibling.sent.sent_at_ns,
                        accepted_ns: sibling.accepted_ns,
                        terminal_ns: event.received_at_ns,
                        terminal_event_seq: event.envelope.event_seq.0,
                        result: sibling.result.clone(),
                        rejection: sibling.rejection,
                    });
                }
            } else {
                bail!("fault pair observed an unrelated request {request_id:?}");
            }
        }
        Ok(FaultPairResult {
            fault_written_ns: fault_sent.sent_at_ns,
            fault_events,
            sibling: sibling_timing.ok_or_else(|| anyhow!("sibling did not become terminal"))?,
        })
    }
}

fn fault_action<'a>(
    plan: &'a [PlannedAction],
    cycle: u32,
    kind: PlanKind,
    label: Option<&str>,
) -> Result<&'a PlannedAction> {
    let matches: Vec<_> = plan
        .iter()
        .filter(|action| {
            action.kind == kind
                && action.cycle == Some(cycle)
                && label.is_none_or(|expected| action.label.as_deref() == Some(expected))
        })
        .collect();
    ensure!(
        matches.len() == 1,
        "fault action {kind:?}/{cycle}/{label:?} cardinality drift"
    );
    Ok(matches[0])
}

fn validate_fault_events(
    events: &[ReceivedEvent],
    expected: &FaultExpectation,
    timings: &mut TimingSamples,
) -> Result<u64> {
    let mut started = None;
    let mut failed = None;
    let mut observed = false;
    let mut ready = None;
    for event in events {
        match &event.envelope.message {
            EventMessage::ExecutionStarted(value) => {
                ensure!(
                    value.fault_id.0 == expected.fault_id
                        && value.tenant_id == expected.tenant
                        && value.incarnation.value() == expected.old_incarnation
                        && value.origin == ExecutionStartOrigin::GuestFirstActionObserver,
                    "forged execution_started"
                );
                ensure!(
                    started.replace(event.received_at_ns).is_none(),
                    "duplicate start"
                );
            }
            EventMessage::ExecutionFailed(value) => {
                ensure!(
                    value.fault_id.0 == expected.fault_id
                        && value.tenant_id == expected.tenant
                        && value.incarnation.value() == expected.old_incarnation
                        && value.reason
                            == if expected.cpu {
                                FaultFailureReason::CpuDeadline
                            } else {
                                FaultFailureReason::GuestTrap
                            },
                    "forged execution_failed"
                );
                ensure!(
                    failed.replace(event.received_at_ns).is_none(),
                    "duplicate failure"
                );
            }
            EventMessage::FailureObserved(value) => {
                ensure!(
                    value.fault_id.0 == expected.fault_id
                        && value.tenant_id == expected.tenant
                        && value.failed_incarnation.value() == expected.old_incarnation,
                    "forged failure_observed"
                );
                ensure!(!observed, "duplicate failure_observed");
                observed = true;
            }
            EventMessage::TenantReady(value) => {
                ensure!(
                    value.tenant_id == expected.tenant
                        && value.incarnation.value() == expected.replacement_incarnation,
                    "forged replacement ready"
                );
                ensure!(
                    ready.replace(event.received_at_ns).is_none(),
                    "duplicate ready"
                );
            }
            _ => {}
        }
    }
    let failed_ns = failed.ok_or_else(|| anyhow!("fault emitted no execution_failed"))?;
    ensure!(observed, "fault emitted no failure_observed");
    let ready_ns = ready.ok_or_else(|| anyhow!("fault emitted no tenant_ready"))?;
    let recovery = positive_delta(expected.write_ns, ready_ns)?;
    if expected.cpu {
        let started_ns = started.ok_or_else(|| anyhow!("CPU fault emitted no guest marker"))?;
        validate_cpu_observation(
            expected.write_ns,
            expected.write_ns,
            started_ns,
            failed_ns,
            ready_ns,
            CpuStartOrigin::GuestFirstActionObserver,
        )?;
        timings.cpu_replacement_ready_ns.push(recovery);
        timings
            .cpu_failed_terminal_ns
            .push(positive_delta(started_ns, failed_ns)?);
    } else {
        ensure!(
            started.is_none(),
            "trap fault emitted forbidden execution_started"
        );
        ensure!(recovery <= 500_000_000, "trap recovery exceeded 500ms");
        timings.trap_replacement_ready_ns.push(recovery);
    }
    Ok(ready_ns)
}

#[derive(Debug)]
struct WorkerRecord {
    sequence: u64,
    operation: WorkerOperation,
    command_id: u64,
    payload_sha256: String,
    transports: Vec<WorkerTransportTrace>,
    timing: Option<CommandTiming>,
}

#[derive(Debug)]
struct PreparedWorker {
    control: ControlEnvelope,
    model_command: ModelCommand,
    record: WorkerRecord,
}

#[derive(Debug)]
struct PreparedWorkerRetry {
    control: ControlEnvelope,
    model_command: ModelCommand,
    record_index: usize,
    record: WorkerRecord,
}

#[derive(Debug)]
enum PreparedWorkerDispatch {
    New(PreparedWorker),
    Retry(PreparedWorkerRetry),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackpressureDisposition {
    Retry,
    DropAfterTerminal,
}

#[derive(Debug)]
enum NextWorker {
    Sequence(u64),
    Prepared(Box<PreparedWorkerDispatch>),
}

fn initial_worker_queue(targets: &[TenantId]) -> BTreeMap<TenantId, NextWorker> {
    targets
        .iter()
        .copied()
        .map(|tenant| (tenant, NextWorker::Sequence(1)))
        .collect()
}

fn take_next_worker(
    rollout_terminal_observed: bool,
    next_workers: &mut BTreeMap<TenantId, NextWorker>,
) -> Option<(TenantId, NextWorker)> {
    if rollout_terminal_observed {
        next_workers.clear();
        return None;
    }
    let tenant = next_workers
        .iter()
        .find_map(|(tenant, worker)| {
            matches!(
                worker,
                NextWorker::Prepared(prepared)
                    if matches!(prepared.as_ref(), PreparedWorkerDispatch::Retry(_))
            )
            .then_some(*tenant)
        })
        .or_else(|| next_workers.first_key_value().map(|(tenant, _)| *tenant))?;
    next_workers.remove(&tenant).map(|worker| (tenant, worker))
}

fn record_worker_backpressure(
    record: &mut WorkerRecord,
    transport: WorkerTransportTrace,
    rollout_terminal_observed: bool,
) -> Result<BackpressureDisposition> {
    ensure!(
        matches!(
            transport.outcome,
            WorkerTransportOutcome::RetryableRejection {
                reason: WorkerRetryReason::Backpressure,
                ..
            }
        ),
        "worker backpressure trace has a non-retryable outcome"
    );
    let retry_already_sent = match record.transports.as_slice() {
        [] => false,
        [first] => {
            ensure!(
                matches!(
                    first.outcome,
                    WorkerTransportOutcome::RetryableRejection {
                        reason: WorkerRetryReason::Backpressure,
                        ..
                    }
                ),
                "worker retry did not follow its first backpressure rejection"
            );
            true
        }
        _ => bail!("worker retried more than once"),
    };
    record.transports.push(transport);
    if rollout_terminal_observed {
        return Ok(BackpressureDisposition::DropAfterTerminal);
    }
    ensure!(
        !retry_already_sent,
        "worker retry was rejected before rollout terminal"
    );
    Ok(BackpressureDisposition::Retry)
}

fn record_worker_transport_sent(counts: &mut WorkloadCounts, is_retry: bool) -> Result<()> {
    counts.update_worker_transport_attempts = counts
        .update_worker_transport_attempts
        .checked_add(1)
        .ok_or_else(|| anyhow!("update worker transport count overflow"))?;
    if is_retry {
        counts.update_worker_retries = counts
            .update_worker_retries
            .checked_add(1)
            .ok_or_else(|| anyhow!("update worker retry count overflow"))?;
    }
    Ok(())
}

#[cfg(test)]
mod rollout_worker_dispatch_tests {
    use super::{
        initial_worker_queue, record_worker_backpressure, record_worker_transport_sent,
        sha256_hex, take_next_worker, BackpressureDisposition, NextWorker,
        PreparedWorkerDispatch, PreparedWorkerRetry, WorkerRecord, WorkloadCounts,
    };
    use crate::protocol::{
        CommandControl, CommandOperation, ControlEnvelope, ControlMessage, IncarnationToken,
        IncrementOperation, TenantId,
    };
    use crate::workload::{
        deterministic_payload, validate_worker_trace_cardinality, worker_command_id, ModelCommand,
        ModelOperation, WorkerLogicalTrace, WorkerOperation, WorkerTransportOutcome,
        WorkerTransportTrace,
    };

    fn backpressure_transport(
        request_id: u64,
        command_id: u64,
        issued_ns: u64,
        rejected_ns: u64,
    ) -> WorkerTransportTrace {
        WorkerTransportTrace {
            request_id,
            command_id,
            payload_sha256: "payload".to_owned(),
            issued_ns,
            outcome: WorkerTransportOutcome::RetryableRejection {
                reason: crate::workload::WorkerRetryReason::Backpressure,
                rejected_ns,
            },
        }
    }

    fn retry_entry(tenant: TenantId, record_index: usize) -> NextWorker {
        let command_id = 17_u64;
        let payload = vec![7_u8; 64];
        let payload_sha256 = sha256_hex(&payload);
        let command = CommandControl {
            tenant_id: tenant,
            incarnation: IncarnationToken::new(3),
            command_id: command_id.into(),
            operation: CommandOperation::Increment(IncrementOperation { delta: 1 }),
            payload: payload.clone(),
            payload_sha256: payload_sha256.clone(),
        };
        NextWorker::Prepared(Box::new(PreparedWorkerDispatch::Retry(
            PreparedWorkerRetry {
                control: ControlEnvelope::new(99_u64, ControlMessage::Command(command)),
                model_command: ModelCommand {
                    request_id: 99,
                    tenant_id: tenant,
                    incarnation: 3,
                    command_id,
                    operation: ModelOperation::Increment(1),
                    payload,
                    payload_sha256: payload_sha256.clone(),
                },
                record_index,
                record: WorkerRecord {
                    sequence: 1,
                    operation: WorkerOperation::IncrementOne,
                    command_id,
                    payload_sha256,
                    transports: Vec::new(),
                    timing: None,
                },
            },
        )))
    }

    #[test]
    fn terminal_between_worker_thirteen_and_fourteen_stops_issue_and_anchor_only_is_valid() {
        let targets: Vec<_> = (0..16).map(TenantId).collect();
        let mut queue = initial_worker_queue(&targets);
        let mut counts = WorkloadCounts::default();
        for _ in &targets {
            record_worker_transport_sent(&mut counts, false).unwrap();
        }
        let mut issued = Vec::new();
        for expected_tenant in 0..14 {
            let (tenant, worker) =
                take_next_worker(false, &mut queue).expect("worker should be issuable");
            assert_eq!(tenant, TenantId(expected_tenant));
            assert!(matches!(worker, NextWorker::Sequence(1)));
            record_worker_transport_sent(&mut counts, false).unwrap();
            issued.push(tenant);
        }
        assert!(take_next_worker(true, &mut queue).is_none());
        assert!(queue.is_empty());
        assert_eq!(issued, (0..14).map(TenantId).collect::<Vec<_>>());
        assert_eq!(counts.update_worker_transport_attempts, 30);
        assert_eq!(counts.update_worker_retries, 0);

        let block = 0;
        let attempt = 0;
        let tenant = 14;
        let command_id = worker_command_id(attempt, tenant, 0).unwrap();
        let payload_sha256 =
            sha256_hex(&deterministic_payload(block, command_id, 64).unwrap());
        let trace = WorkerLogicalTrace {
            sequence: 0,
            operation: WorkerOperation::Read,
            command_id,
            payload_bytes: 64,
            payload_sha256: payload_sha256.clone(),
            rollout_flush_ns: 10,
            candidate_terminal_ns: 20,
            drain_completed_ns: 20,
            proof_started_ns: 30,
            transports: vec![WorkerTransportTrace {
                request_id: 1,
                command_id,
                payload_sha256,
                issued_ns: 1,
                outcome: WorkerTransportOutcome::AcceptedTerminal { terminal_ns: 2 },
            }],
        };
        let cardinality =
            validate_worker_trace_cardinality(block, attempt, tenant, &[trace]).unwrap();
        assert_eq!(cardinality.logical_operations, 1);
        assert_eq!(cardinality.continuous_logical_operations, 0);
        assert_eq!(cardinality.transport_attempts, 1);
        assert_eq!(cardinality.retries, 0);
    }

    #[test]
    fn terminal_between_rejection_and_retry_drops_retry_without_issue() {
        let mut counts = WorkloadCounts::default();
        record_worker_transport_sent(&mut counts, false).unwrap();
        let mut priority_queue = initial_worker_queue(&[TenantId(0)]);
        priority_queue.insert(TenantId(15), retry_entry(TenantId(15), 4));
        let (tenant, worker) = take_next_worker(false, &mut priority_queue).unwrap();
        assert_eq!(tenant, TenantId(15));
        match worker {
            NextWorker::Prepared(prepared) => match *prepared {
                PreparedWorkerDispatch::Retry(retry) => assert_eq!(retry.record_index, 4),
                PreparedWorkerDispatch::New(_) => panic!("retry lost dispatch priority"),
            },
            NextWorker::Sequence(_) => panic!("retry lost dispatch priority"),
        }

        let mut terminal_queue = initial_worker_queue(&[TenantId(0)]);
        terminal_queue.insert(TenantId(15), retry_entry(TenantId(15), 4));
        assert!(take_next_worker(true, &mut terminal_queue).is_none());
        assert!(terminal_queue.is_empty());
        assert_eq!(counts.update_worker_transport_attempts, 1);
        assert_eq!(counts.update_worker_retries, 0);
    }

    #[test]
    fn terminal_before_first_rejection_drops_incomplete_tail_without_retry() {
        let mut counts = WorkloadCounts::default();
        record_worker_transport_sent(&mut counts, false).unwrap();
        let mut records = vec![WorkerRecord {
            sequence: 1,
            operation: WorkerOperation::IncrementOne,
            command_id: 17,
            payload_sha256: "payload".to_owned(),
            transports: Vec::new(),
            timing: None,
        }];
        let disposition = record_worker_backpressure(
            records.last_mut().unwrap(),
            backpressure_transport(1, 17, 5, 30),
            true,
        )
        .unwrap();
        assert_eq!(disposition, BackpressureDisposition::DropAfterTerminal);
        let dropped = records.pop().unwrap();
        assert_eq!(dropped.transports.len(), 1);
        assert!(records.is_empty());
        assert_eq!(counts.update_worker_transport_attempts, 1);
        assert_eq!(counts.update_worker_retries, 0);
    }

    #[test]
    fn retry_sent_then_terminal_then_rejection_drops_without_second_retry() {
        let mut counts = WorkloadCounts::default();
        record_worker_transport_sent(&mut counts, false).unwrap();
        record_worker_transport_sent(&mut counts, true).unwrap();
        let first_rejection = backpressure_transport(1, 17, 5, 10);
        let mut records = vec![WorkerRecord {
            sequence: 1,
            operation: WorkerOperation::IncrementOne,
            command_id: 17,
            payload_sha256: "payload".to_owned(),
            transports: vec![first_rejection.clone()],
            timing: None,
        }];
        let disposition = record_worker_backpressure(
            records.last_mut().unwrap(),
            backpressure_transport(2, 17, 15, 30),
            true,
        )
        .unwrap();
        assert_eq!(disposition, BackpressureDisposition::DropAfterTerminal);
        let dropped = records.pop().unwrap();
        assert_eq!(dropped.transports.len(), 2);
        assert!(records.is_empty());
        assert_eq!(counts.update_worker_transport_attempts, 2);
        assert_eq!(counts.update_worker_retries, 1);

        let mut before_terminal = WorkerRecord {
            sequence: 1,
            operation: WorkerOperation::IncrementOne,
            command_id: 17,
            payload_sha256: "payload".to_owned(),
            transports: vec![first_rejection],
            timing: None,
        };
        let error = record_worker_backpressure(
            &mut before_terminal,
            backpressure_transport(2, 17, 15, 20),
            false,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("retry was rejected before rollout terminal"));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservedRolloutTerminal {
    Committed,
    RolledBack,
}

#[derive(Debug)]
struct UpdateDispatch {
    rollout_flush_ns: u64,
    candidate_terminal_ns: u64,
    drain_completed_ns: u64,
    terminal: ObservedRolloutTerminal,
    rollout_events: Vec<ReceivedEvent>,
    records: BTreeMap<TenantId, Vec<WorkerRecord>>,
}

impl WorkloadRunner {
    fn snapshot_tenant(&mut self, tenant: TenantId) -> Result<(GuestBusinessResult, u64)> {
        let incarnation = self
            .model
            .incarnation(tenant)
            .ok_or_else(|| anyhow!("snapshot target {} is inactive", tenant.0))?;
        let (_, events) = self.round_trip_control(ControlMessage::Snapshot(SnapshotControl {
            tenant_id: tenant,
            expected_incarnation: Some(IncarnationToken::new(incarnation)),
        }))?;
        let mut present = events
            .iter()
            .filter_map(|event| match &event.envelope.message {
                EventMessage::SnapshotPresent(value) => Some(value),
                _ => None,
            });
        let value = present
            .next()
            .ok_or_else(|| anyhow!("snapshot did not return snapshot_present"))?;
        ensure!(
            present.next().is_none(),
            "snapshot returned duplicate state"
        );
        ensure!(
            value.tenant_id == tenant && value.incarnation.value() == incarnation,
            "snapshot echoed the wrong tenant endpoint"
        );
        let observed = guest_result(&value.state);
        let expected = self
            .model
            .snapshot(tenant)
            .ok_or_else(|| anyhow!("model snapshot target disappeared"))?;
        ensure!(
            observed == expected,
            "snapshot state differed from the model"
        );
        Ok((observed, terminal_timestamp(&events)?))
    }

    pub fn run_updates(&mut self, plan: &[PlannedAction]) -> Result<()> {
        for attempt in 0..60_u32 {
            self.run_update_attempt(plan, attempt)?;
        }
        ensure!(
            self.timings.failed_update_unavailability_ns.len() == 480
                && self.timings.valid_update_unavailability_ns.len() == 480
                && self.timings.failed_rollout_ns.len() == 30
                && self.timings.valid_rollout_ns.len() == 30
                && self.timings.failed_mixed_window_ns.len() == 30
                && self.timings.valid_mixed_window_ns.len() == 30,
            "update metric cardinality drift"
        );
        Ok(())
    }

    pub fn run_authority(
        &mut self,
        plan: &[PlannedAction],
        harness: &AuthorityHarness,
        production_policy_sha256: &str,
    ) -> Result<Vec<AuthorityRunEvidence>> {
        ensure!(
            production_policy_sha256.len() == 64
                && production_policy_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "production policy SHA-256 is not canonical lowercase hexadecimal"
        );
        let positives = actions_of(plan, PlanKind::AuthorityPositiveControl);
        let canaries = actions_of(plan, PlanKind::AuthorityCanary);
        ensure!(
            positives.len() == 6 && canaries.len() == 6,
            "authority plan cardinality drift"
        );

        positives
            .into_iter()
            .zip(canaries)
            .enumerate()
            .map(|(index, (positive_action, canary_action))| {
                let positive_label = positive_action
                    .label
                    .as_deref()
                    .ok_or_else(|| anyhow!("authority positive control has no label"))?;
                let canary_label = canary_action
                    .label
                    .as_deref()
                    .ok_or_else(|| anyhow!("authority canary has no label"))?;
                ensure!(
                    positive_label == canary_label,
                    "authority positive/canary pairing drift"
                );
                let probe = authority_probe_from_label(positive_label)?;
                self.run_authority_attempt(
                    positive_action,
                    canary_action,
                    index,
                    probe,
                    harness,
                    production_policy_sha256,
                )
            })
            .collect()
    }

    fn run_authority_attempt(
        &mut self,
        positive_action: &PlannedAction,
        canary_action: &PlannedAction,
        index: usize,
        probe: Probe,
        harness: &AuthorityHarness,
        production_policy_sha256: &str,
    ) -> Result<AuthorityRunEvidence> {
        let positive_control = harness
            .run_positive_control(probe)
            .with_context(|| format!("positive authority control {}", probe.as_str()))?;
        let artifact = harness.artifact(probe)?;
        ensure!(
            positive_control.kind == artifact.kind
                && positive_control.artifact_sha256 == artifact.artifact_sha256
                && positive_control.parameter_sha256 == artifact.parameter_sha256,
            "positive control and candidate canary bytes are not identical"
        );
        let watch = harness.start_denial_watch(probe)?;

        let authority = self.envelope(ControlMessage::AuthorityProbe(AuthorityProbeControl {
            attempt_id: DecimalU64(canary_action.ordinal),
            probe: protocol_kind(probe),
            artifact_sha256: artifact.artifact_sha256,
            artifact_bytes: artifact.bytes,
            parameter_sha256: artifact.parameter_sha256,
            production_policy_sha256: production_policy_sha256.to_owned(),
        }))?;
        let authority_request = authority.request_id;
        let sibling_tenant = TenantId((self.block + u32::try_from(index)?) % 32);
        let sibling_incarnation = self
            .model
            .incarnation(sibling_tenant)
            .ok_or_else(|| anyhow!("authority sibling tenant is inactive"))?;
        let sibling = self.envelope(ControlMessage::Snapshot(SnapshotControl {
            tenant_id: sibling_tenant,
            expected_incarnation: Some(IncarnationToken::new(sibling_incarnation)),
        }))?;
        let sibling_request = sibling.request_id;
        let controls = [authority, sibling];
        self.counts.controls_sent += u64::try_from(controls.len())?;
        let receipts = self
            .session
            .send_batch(&controls)
            .context("send authority canary and sibling in one causal batch")?;
        ensure!(
            receipts.len() == 2,
            "authority batch receipt cardinality drift"
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut authority_accepted = false;
        let mut terminal: Option<AuthorityProbeTerminalEvent> = None;
        let mut sibling_state = None;
        while !self.session.oracle().is_terminal(authority_request)
            || !self.session.oracle().is_terminal(sibling_request)
        {
            let now = Instant::now();
            ensure!(
                now < deadline,
                "authority/sibling absolute deadline expired"
            );
            let event = self
                .session
                .receive(deadline.saturating_duration_since(now))
                .context("receive authority/sibling event")?;
            self.counts.events_received += 1;
            self.observe_event(&event)?;
            match (event.envelope.request_id, &event.envelope.message) {
                (request_id, EventMessage::AuthorityProbeAccepted(_))
                    if request_id == authority_request =>
                {
                    ensure!(!authority_accepted, "duplicate authority accepted event");
                    authority_accepted = true;
                }
                (request_id, EventMessage::AuthorityProbeTerminal(value))
                    if request_id == authority_request =>
                {
                    ensure!(terminal.is_none(), "duplicate authority terminal event");
                    terminal = Some(value.clone());
                }
                (request_id, EventMessage::SnapshotPresent(value))
                    if request_id == sibling_request =>
                {
                    ensure!(
                        sibling_state.is_none(),
                        "duplicate authority sibling snapshot"
                    );
                    ensure!(
                        value.tenant_id == sibling_tenant
                            && value.incarnation.value() == sibling_incarnation,
                        "authority sibling snapshot echoed another endpoint"
                    );
                    sibling_state = Some(guest_result(&value.state));
                }
                (request_id, _)
                    if request_id == authority_request || request_id == sibling_request => {}
                _ => bail!("authority phase received an unrelated request event"),
            }
        }
        ensure!(authority_accepted, "authority request was never accepted");
        let terminal = terminal.ok_or_else(|| anyhow!("authority request has no terminal"))?;
        let sibling_state =
            sibling_state.ok_or_else(|| anyhow!("authority sibling request has no snapshot"))?;
        let expected_sibling = self
            .model
            .snapshot(sibling_tenant)
            .ok_or_else(|| anyhow!("authority sibling disappeared from model"))?;
        ensure!(
            sibling_state == expected_sibling,
            "authority sibling snapshot differed from model"
        );

        let expected_policy_denial =
            self.candidate == CandidateKind::Extism && probe == Probe::ExtismHttp;
        ensure!(
            (expected_policy_denial && terminal.result == AuthorityProbeResult::PolicyDenied)
                || (!expected_policy_denial
                    && terminal.result == AuthorityProbeResult::AbsentAtLink),
            "candidate returned the wrong frozen authority denial mode"
        );
        let no_effect = watch.finish(Duration::from_millis(500))?;
        let canary = AuthorityCanaryEvidence {
            terminal,
            positive_control_detected: true,
            tail_complete: true,
            tail_elapsed_ms: no_effect.tail_millis,
            secret_or_digest_observed: false,
            external_effect_observed: false,
            sibling_probe_succeeded: true,
            production_linker_inventory_contains_exact_import: expected_policy_denial,
            actual_link_failure_observed: !expected_policy_denial,
            typed_production_policy_denial_observed: expected_policy_denial,
        };
        validate_authority_canary(&canary).context("validate authority canary evidence")?;
        Ok(AuthorityRunEvidence {
            positive_action_ordinal: positive_action.ordinal,
            canary_action_ordinal: canary_action.ordinal,
            label: probe.as_str().to_owned(),
            positive_control,
            no_effect,
            canary,
        })
    }

    fn run_update_attempt(&mut self, plan: &[PlannedAction], attempt: u32) -> Result<()> {
        let cycle = attempt / 2;
        let valid = attempt % 2 == 1;
        let (old_version, failed_version, new_version) = if cycle % 2 == 0 {
            ("A", "bad_B", "B")
        } else {
            ("B", "bad_A", "A")
        };
        let target_version = if valid { new_version } else { failed_version };
        let final_version = if valid { new_version } else { old_version };
        let targets: Vec<_> = (0..16).map(TenantId).collect();

        let baselines = update_actions(plan, attempt, PlanKind::UpdateBaselineSnapshot)?;
        ensure!(baselines.len() == 16, "update baseline cardinality drift");
        for action in baselines {
            let tenant = TenantId(action.tenant_id.ok_or_else(|| anyhow!("missing tenant"))?);
            let (snapshot, _) = self.snapshot_tenant(tenant)?;
            ensure!(
                snapshot.logical_version == old_version,
                "baseline was not uniformly on the old version"
            );
        }

        let anchor_actions = update_actions(plan, attempt, PlanKind::UpdateWorkerAnchorRead)?;
        ensure!(
            anchor_actions.len() == 16,
            "update anchor cardinality drift"
        );
        let anchor_timings = self.drive_action_batch(&anchor_actions, ModelOperation::Read)?;
        for _ in &anchor_timings {
            record_worker_transport_sent(&mut self.counts, false)?;
        }
        let anchors: BTreeMap<_, _> = anchor_timings
            .into_iter()
            .map(|timing| (timing.tenant_id, timing))
            .collect();
        ensure!(anchors.len() == 16, "update anchor tenant set drift");
        for timing in anchors.values() {
            ensure!(
                timing.result.as_ref().is_some_and(|result| {
                    !result.deduplicated && result.logical_version == old_version
                }),
                "anchor was not a fresh old-version read"
            );
        }

        let dispatch =
            self.dispatch_rollout_workers(attempt, &targets, old_version, target_version, valid)?;
        validate_rollout_observation(
            &dispatch.rollout_events,
            attempt,
            target_version,
            valid,
            &targets,
        )?;

        self.model
            .finish_rollout(&targets, final_version, final_version)?;
        self.model
            .settle_rollout(&targets, final_version, final_version)?;

        let replay_actions = update_actions(plan, attempt, PlanKind::UpdateCompletedReplay)?;
        ensure!(
            replay_actions.len() == 16,
            "update replay cardinality drift"
        );
        let replay_timings = self.drive_action_batch(&replay_actions, ModelOperation::Read)?;
        let replay_by_tenant: BTreeMap<_, _> = replay_timings
            .into_iter()
            .map(|timing| (timing.tenant_id, timing))
            .collect();
        ensure!(
            replay_by_tenant.len() == 16,
            "update replay tenant set drift"
        );
        for tenant in &targets {
            let anchor = anchors
                .get(tenant)
                .ok_or_else(|| anyhow!("missing anchor"))?;
            let replay = replay_by_tenant
                .get(tenant)
                .ok_or_else(|| anyhow!("missing replay"))?;
            let mut expected = anchor
                .result
                .clone()
                .ok_or_else(|| anyhow!("anchor has no result"))?;
            expected.deduplicated = true;
            ensure!(
                replay.result.as_ref() == Some(&expected),
                "rollout did not preserve the exact completed-command cache"
            );
        }

        self.finish_update_proofs(
            plan,
            attempt,
            valid,
            old_version,
            final_version,
            targets,
            anchors,
            replay_by_tenant,
            dispatch,
        )
    }
}

fn update_actions(
    plan: &[PlannedAction],
    attempt: u32,
    kind: PlanKind,
) -> Result<Vec<&PlannedAction>> {
    let actions: Vec<_> = plan
        .iter()
        .filter(|action| action.kind == kind && action.rollout_index == Some(attempt))
        .collect();
    ensure!(
        !actions.is_empty(),
        "missing {kind:?} actions for attempt {attempt}"
    );
    Ok(actions)
}

impl WorkloadRunner {
    fn prepare_worker_sequence(
        &mut self,
        attempt: u32,
        tenant: TenantId,
        sequence: u64,
    ) -> Result<PreparedWorker> {
        let operation = if sequence % 2 == 0 {
            WorkerOperation::Read
        } else {
            WorkerOperation::IncrementOne
        };
        let model_operation = match operation {
            WorkerOperation::Read => ModelOperation::Read,
            WorkerOperation::IncrementOne => ModelOperation::Increment(1),
        };
        let command_id = worker_command_id(attempt, tenant.0, sequence)?;
        let payload = deterministic_payload(self.block, command_id, 64)?;
        let payload_sha256 = sha256_hex(&payload);
        let incarnation = self
            .model
            .incarnation(tenant)
            .ok_or_else(|| anyhow!("worker target {} is inactive", tenant.0))?;
        let command = self.command_control(
            tenant,
            incarnation,
            command_id,
            model_operation.clone(),
            payload,
        );
        let control = self.envelope(ControlMessage::Command(command.clone()))?;
        Ok(PreparedWorker {
            model_command: ModelCommand {
                request_id: control.request_id.0,
                tenant_id: tenant,
                incarnation,
                command_id,
                operation: model_operation,
                payload: command.payload,
                payload_sha256: command.payload_sha256,
            },
            control,
            record: WorkerRecord {
                sequence,
                operation,
                command_id,
                payload_sha256,
                transports: Vec::new(),
                timing: None,
            },
        })
    }

    fn prepare_worker_retry(
        &mut self,
        command: CommandControl,
        record_index: usize,
        record: WorkerRecord,
    ) -> Result<PreparedWorkerRetry> {
        let model_operation = match &command.operation {
            CommandOperation::Increment(value) => ModelOperation::Increment(value.delta),
            CommandOperation::Read(_) => ModelOperation::Read,
        };
        let control = self.envelope(ControlMessage::Command(command.clone()))?;
        Ok(PreparedWorkerRetry {
            model_command: ModelCommand {
                request_id: control.request_id.0,
                tenant_id: command.tenant_id,
                incarnation: command.incarnation.value(),
                command_id: command.command_id.0,
                operation: model_operation,
                payload: command.payload,
                payload_sha256: command.payload_sha256,
            },
            control,
            record_index,
            record,
        })
    }

    fn dispatch_rollout_workers(
        &mut self,
        attempt: u32,
        targets: &[TenantId],
        old_version: &str,
        target_version: &str,
        valid: bool,
    ) -> Result<UpdateDispatch> {
        let (artifact_ref, artifact_sha256) = frozen_identity(target_version)?;
        let rollout_id = format!("rollout-{attempt}");
        let envelope = self.envelope(ControlMessage::Rollout(RolloutControl {
            rollout_id: rollout_id.clone(),
            targets: targets.to_vec(),
            from_version: old_version.to_owned(),
            to_version: target_version.to_owned(),
            to_build_id: target_version.to_owned(),
            artifact_ref: artifact_ref.to_owned(),
            artifact_sha256: artifact_sha256.to_owned(),
        }))?;
        self.counts.controls_sent += 1;
        let rollout_sent = self
            .session
            .send(&envelope)
            .context("send rollout control")?;
        self.model
            .begin_rollout(targets, target_version, target_version, true)?;

        let mut records: BTreeMap<TenantId, Vec<WorkerRecord>> = targets
            .iter()
            .copied()
            .map(|tenant| (tenant, Vec::new()))
            .collect();
        let mut pending: BTreeMap<RequestId, (TenantId, usize, PendingCommand)> = BTreeMap::new();

        let started_deadline = Instant::now() + Duration::from_millis(500);
        let external_deadline = Instant::now() + Duration::from_millis(2_000);

        let mut rollout_started = false;
        let mut rollout_terminal = None;
        let mut rollout_events = Vec::new();
        let mut next_workers = initial_worker_queue(targets);
        let mut last_worker_terminal_ns = rollout_sent.sent_at_ns;
        while rollout_terminal.is_none() || !pending.is_empty() {
            let now = Instant::now();
            let mut deadline = external_deadline;
            if !rollout_started {
                deadline = deadline.min(started_deadline);
            }
            if let Some(command_deadline) = pending.values().map(|item| item.2.deadline).min() {
                deadline = deadline.min(command_deadline);
            }
            ensure!(now < deadline, "rollout/worker absolute deadline expired");
            let event = if let Some((tenant, queued)) =
                take_next_worker(rollout_terminal.is_some(), &mut next_workers)
            {
                let prepared = match queued {
                    NextWorker::Sequence(sequence) => PreparedWorkerDispatch::New(
                        self.prepare_worker_sequence(attempt, tenant, sequence)?,
                    ),
                    NextWorker::Prepared(prepared) => *prepared,
                };
                let prepared_control = match &prepared {
                    PreparedWorkerDispatch::New(prepared) => &prepared.control,
                    PreparedWorkerDispatch::Retry(prepared) => &prepared.control,
                };
                match self
                    .session
                    .send_or_receive(prepared_control)
                    .context("atomically issue worker or consume queued event")?
                {
                    SentOrEvent::Event(event) => {
                        next_workers.insert(tenant, NextWorker::Prepared(Box::new(prepared)));
                        event
                    }
                    SentOrEvent::Sent(sent) => {
                        let (control, model_command, record_index, record, is_retry) =
                            match prepared {
                                PreparedWorkerDispatch::New(PreparedWorker {
                                    control,
                                    model_command,
                                    record,
                                }) => {
                                    let record_index = records
                                        .get(&tenant)
                                        .ok_or_else(|| anyhow!("worker target missing"))?
                                        .len();
                                    (control, model_command, record_index, record, false)
                                }
                                PreparedWorkerDispatch::Retry(PreparedWorkerRetry {
                                    control,
                                    model_command,
                                    record_index,
                                    record,
                                }) => (control, model_command, record_index, record, true),
                            };
                        self.model.begin_command(model_command)?;
                        self.counts.controls_sent += 1;
                        record_worker_transport_sent(&mut self.counts, is_retry)?;
                        let request_id = control.request_id;
                        let worker_records = records
                            .get_mut(&tenant)
                            .ok_or_else(|| anyhow!("worker target missing"))?;
                        ensure!(
                            worker_records.len() == record_index,
                            "worker retry record index drift"
                        );
                        worker_records.push(record);
                        ensure!(
                            pending
                                .insert(
                                    request_id,
                                    (
                                        tenant,
                                        record_index,
                                        PendingCommand {
                                            control,
                                            sent,
                                            accepted_ns: None,
                                            deadline: Instant::now()
                                                + Duration::from_millis(100),
                                            result: None,
                                            rejection: None,
                                        },
                                    ),
                                )
                                .is_none(),
                            "duplicate queued worker request"
                        );
                        continue;
                    }
                }
            } else {
                self.session
                    .receive(deadline.saturating_duration_since(now))
                    .context("receiving rollout/worker event")?
            };
            self.counts.events_received += 1;
            let request_id = event.envelope.request_id;
            self.observe_event(&event)?;

            if request_id == envelope.request_id {
                match &event.envelope.message {
                    EventMessage::RolloutStarted(value) => {
                        ensure!(value.rollout_id == rollout_id, "rollout start ID drift");
                        ensure!(!rollout_started, "duplicate rollout_started");
                        rollout_started = true;
                    }
                    EventMessage::RolloutCommitted(value) => {
                        ensure!(valid, "failed artifact unexpectedly committed");
                        ensure!(value.rollout_id == rollout_id, "commit ID drift");
                        ensure!(value.version == target_version, "commit version drift");
                        ensure!(rollout_terminal.is_none(), "duplicate rollout terminal");
                        rollout_terminal =
                            Some((ObservedRolloutTerminal::Committed, event.received_at_ns));
                    }
                    EventMessage::RolloutRolledBack(value) => {
                        ensure!(!valid, "valid artifact unexpectedly rolled back");
                        ensure!(value.rollout_id == rollout_id, "rollback ID drift");
                        ensure!(
                            value.restored_version == old_version,
                            "rollback version drift"
                        );
                        ensure!(!value.reason.is_empty(), "rollback reason is empty");
                        ensure!(rollout_terminal.is_none(), "duplicate rollout terminal");
                        rollout_terminal =
                            Some((ObservedRolloutTerminal::RolledBack, event.received_at_ns));
                    }
                    EventMessage::RolloutInDoubt(value) => {
                        bail!("rollout entered in_doubt: {}", value.reason)
                    }
                    EventMessage::RolloutRejected(value) => {
                        bail!(
                            "rollout was rejected instead of activated: {}",
                            value.reason
                        )
                    }
                    _ => {}
                }
                rollout_events.push(event);
                continue;
            }

            let (tenant, record_index, mut item) = pending
                .remove(&request_id)
                .ok_or_else(|| anyhow!("rollout phase emitted unrelated request {request_id:?}"))?;
            match &event.envelope.message {
                EventMessage::CommandAccepted(_) => {
                    item.accepted_ns = Some(event.received_at_ns);
                    item.deadline = Instant::now() + Duration::from_millis(250);
                }
                EventMessage::CommandCompleted(value) => {
                    item.result = Some(guest_result(&value.result));
                }
                EventMessage::CommandRejected(value) => {
                    item.rejection = Some(value.reason);
                }
                EventMessage::CommandFailed(value) => {
                    bail!(
                        "update worker command failed after admission: {}",
                        value.error
                    )
                }
                _ => {}
            }
            if !self.session.oracle().is_terminal(request_id) {
                pending.insert(request_id, (tenant, record_index, item));
                continue;
            }

            let control = match &item.control.message {
                ControlMessage::Command(control) => control.clone(),
                _ => unreachable!("worker pending item is a command"),
            };
            let timing = CommandTiming {
                request_id,
                tenant_id: tenant,
                command_id: control.command_id.0,
                written_ns: item.sent.sent_at_ns,
                accepted_ns: item.accepted_ns,
                terminal_ns: event.received_at_ns,
                terminal_event_seq: event.envelope.event_seq.0,
                result: item.result,
                rejection: item.rejection,
            };
            last_worker_terminal_ns = last_worker_terminal_ns.max(timing.terminal_ns);
            let record = records
                .get_mut(&tenant)
                .and_then(|values| values.get_mut(record_index))
                .ok_or_else(|| anyhow!("worker record index drift"))?;

            if timing.rejection == Some(RejectionReason::Backpressure) {
                let disposition = record_worker_backpressure(
                    record,
                    WorkerTransportTrace {
                        request_id: request_id.0,
                        command_id: control.command_id.0,
                        payload_sha256: control.payload_sha256.clone(),
                        issued_ns: timing.written_ns,
                        outcome: WorkerTransportOutcome::RetryableRejection {
                            reason: WorkerRetryReason::Backpressure,
                            rejected_ns: timing.terminal_ns,
                        },
                    },
                    rollout_terminal.is_some(),
                )?;
                let worker_records = records
                    .get_mut(&tenant)
                    .ok_or_else(|| anyhow!("worker retry target missing"))?;
                ensure!(
                    worker_records.len() == record_index + 1,
                    "worker retry must move the last in-flight logical record"
                );
                let retry_record = worker_records
                    .pop()
                    .ok_or_else(|| anyhow!("worker retry record disappeared"))?;
                if disposition == BackpressureDisposition::DropAfterTerminal {
                    continue;
                }
                let retry = self.prepare_worker_retry(control, record_index, retry_record)?;
                ensure!(
                    next_workers
                        .insert(
                            tenant,
                            NextWorker::Prepared(Box::new(PreparedWorkerDispatch::Retry(retry))),
                        )
                        .is_none(),
                    "tenant already had work queued when retry was required"
                );
                continue;
            }
            validate_expected_terminal(&timing, ExpectedTerminal::Completed)?;
            record.transports.push(WorkerTransportTrace {
                request_id: request_id.0,
                command_id: control.command_id.0,
                payload_sha256: control.payload_sha256,
                issued_ns: timing.written_ns,
                outcome: WorkerTransportOutcome::AcceptedTerminal {
                    terminal_ns: timing.terminal_ns,
                },
            });
            record.timing = Some(timing);

            if rollout_terminal.is_none() {
                let next_sequence = record
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("worker sequence overflow"))?;
                ensure!(
                    next_workers
                        .insert(tenant, NextWorker::Sequence(next_sequence))
                        .is_none(),
                    "tenant already had a queued next worker"
                );
            }
        }

        let (terminal, candidate_terminal_ns) =
            rollout_terminal.ok_or_else(|| anyhow!("rollout emitted no terminal"))?;
        let drain_completed_ns = candidate_terminal_ns.max(last_worker_terminal_ns);
        ensure!(
            positive_delta(rollout_sent.sent_at_ns, drain_completed_ns)? <= 2_000_000_000,
            "rollout worker drain exceeded the absolute 2s deadline"
        );
        Ok(UpdateDispatch {
            rollout_flush_ns: rollout_sent.sent_at_ns,
            candidate_terminal_ns,
            drain_completed_ns,
            terminal,
            rollout_events,
            records,
        })
    }
}
