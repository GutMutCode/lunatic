use std::collections::{BTreeMap, HashMap};

use thiserror::Error;

use crate::protocol::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestPhase {
    Pending,
    CreateCreated,
    CommandAccepted,
    FaultExecutionStarted,
    FaultFailed,
    FaultObserved,
    RolloutStarted,
    AuthorityProbeAccepted,
    Terminal,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionPhase {
    New,
    AwaitingHello,
    Greeted,
    AwaitingInit,
    Running,
    AwaitingShutdown,
    Shutdown,
    Fatal,
}

#[derive(Debug, Clone)]
enum State {
    Pending,
    CreateCreated(IncarnationToken),
    CommandAccepted,
    FaultExecutionStarted,
    FaultFailed,
    FaultObserved,
    RolloutStarted(BTreeMap<TenantId, IncarnationToken>),
    AuthorityProbeAccepted,
    Terminal,
    TimedOut,
}

impl State {
    fn public_phase(&self) -> RequestPhase {
        match self {
            Self::Pending => RequestPhase::Pending,
            Self::CreateCreated(_) => RequestPhase::CreateCreated,
            Self::CommandAccepted => RequestPhase::CommandAccepted,
            Self::FaultExecutionStarted => RequestPhase::FaultExecutionStarted,
            Self::FaultFailed => RequestPhase::FaultFailed,
            Self::FaultObserved => RequestPhase::FaultObserved,
            Self::RolloutStarted(_) => RequestPhase::RolloutStarted,
            Self::AuthorityProbeAccepted => RequestPhase::AuthorityProbeAccepted,
            Self::Terminal => RequestPhase::Terminal,
            Self::TimedOut => RequestPhase::TimedOut,
        }
    }
}

#[derive(Debug, Clone)]
struct ActivationRecord {
    marker: ActivationStartedEvent,
    count: u8,
}

#[derive(Debug, Clone)]
struct RequestState {
    control: ControlMessage,
    state: State,
    activations: BTreeMap<TenantId, ActivationRecord>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OracleError {
    #[error("oracle is poisoned by an earlier protocol violation: {0}")]
    Poisoned(String),
    #[error("request_id 0 is reserved for unsolicited fatal events")]
    ZeroRequestId,
    #[error("request_id {0} was already registered")]
    DuplicateRequest(RequestId),
    #[error("request_id {0} is not registered")]
    UnknownRequest(RequestId),
    #[error("event_seq gap or replay: expected {expected}, received {actual}")]
    EventSequence { expected: u64, actual: u64 },
    #[error("invalid session transition: {0}")]
    Session(String),
    #[error("wire contract violation: {0}")]
    Wire(String),
    #[error("invalid event for request_id {request_id}: {detail}")]
    Transition {
        request_id: RequestId,
        detail: String,
    },
    #[error("request_id {0} has already reached a terminal state")]
    DuplicateTerminal(RequestId),
    #[error("request_id {0} emitted an event after its timeout")]
    LateEvent(RequestId),
    #[error("only a fatal event may use request_id 0")]
    InvalidUnsolicitedEvent,
    #[error("fatal events must use request_id 0")]
    FatalMustBeUnsolicited,
}

/// Strict request/event automaton owned by the external runner.
#[derive(Debug)]
pub struct Oracle {
    next_event_seq: u64,
    requests: HashMap<RequestId, RequestState>,
    tenants: BTreeMap<TenantId, IncarnationToken>,
    last_incarnations: BTreeMap<TenantId, IncarnationToken>,
    session: SessionPhase,
    fatal: Option<FatalEvent>,
    poisoned: Option<String>,
}

impl Default for Oracle {
    fn default() -> Self {
        Self::new()
    }
}

impl Oracle {
    pub fn new() -> Self {
        Self {
            next_event_seq: 1,
            requests: HashMap::new(),
            tenants: BTreeMap::new(),
            last_incarnations: BTreeMap::new(),
            session: SessionPhase::New,
            fatal: None,
            poisoned: None,
        }
    }

    pub fn next_event_seq(&self) -> EventSeq {
        EventSeq(self.next_event_seq)
    }

    pub fn tenant_incarnation(&self, tenant_id: TenantId) -> Option<&IncarnationToken> {
        self.tenants.get(&tenant_id)
    }

    pub fn fatal(&self) -> Option<&FatalEvent> {
        self.fatal.as_ref()
    }

    pub fn phase(&self, request_id: RequestId) -> Option<RequestPhase> {
        self.requests
            .get(&request_id)
            .map(|request| request.state.public_phase())
    }

    pub fn is_terminal(&self, request_id: RequestId) -> bool {
        self.phase(request_id) == Some(RequestPhase::Terminal)
    }

    pub fn poison(&mut self, detail: impl Into<String>) -> OracleError {
        let detail = detail.into();
        self.poisoned = Some(detail.clone());
        OracleError::Poisoned(detail)
    }

    pub fn register_control(&mut self, control: &ControlEnvelope) -> Result<(), OracleError> {
        self.ensure_healthy()?;
        if let Err(detail) = validate_control_envelope(control) {
            return Err(self.violation(OracleError::Wire(detail)));
        }
        if control.request_id.0 == 0 {
            return Err(self.violation(OracleError::ZeroRequestId));
        }
        if self.requests.contains_key(&control.request_id) {
            return Err(self.violation(OracleError::DuplicateRequest(control.request_id)));
        }

        if let Err(error) = self.validate_control_for_session(&control.message) {
            return Err(self.violation(error));
        }

        self.requests.insert(
            control.request_id,
            RequestState {
                control: control.message.clone(),
                state: State::Pending,
                activations: BTreeMap::new(),
            },
        );
        Ok(())
    }

    pub fn expire_request(&mut self, request_id: RequestId) -> Result<(), OracleError> {
        self.ensure_healthy()?;
        let request = self
            .requests
            .get_mut(&request_id)
            .ok_or(OracleError::UnknownRequest(request_id))?;
        match request.state {
            State::Terminal => Err(OracleError::DuplicateTerminal(request_id)),
            State::TimedOut => Ok(()),
            _ => {
                request.state = State::TimedOut;
                Ok(())
            }
        }
    }

    pub fn observe_event(&mut self, event: &EventEnvelope) -> Result<(), OracleError> {
        self.ensure_healthy()?;
        if let Err(detail) = validate_event_envelope(event) {
            return Err(self.violation(OracleError::Wire(detail)));
        }

        if event.event_seq.0 != self.next_event_seq {
            return Err(self.violation(OracleError::EventSequence {
                expected: self.next_event_seq,
                actual: event.event_seq.0,
            }));
        }

        let result = self.observe_event_inner(event);
        match result {
            Ok(()) => {
                self.next_event_seq = self.next_event_seq.checked_add(1).ok_or_else(|| {
                    self.violation(OracleError::Session("event_seq overflow".into()))
                })?;
                Ok(())
            }
            Err(error) => Err(self.violation(error)),
        }
    }

    fn observe_event_inner(&mut self, event: &EventEnvelope) -> Result<(), OracleError> {
        if event.request_id.0 == 0 {
            return match &event.message {
                EventMessage::Fatal(fatal) => {
                    if matches!(self.session, SessionPhase::Shutdown | SessionPhase::Fatal) {
                        return Err(OracleError::Session(
                            "fatal event arrived after terminal session state".into(),
                        ));
                    }
                    self.fatal = Some(fatal.clone());
                    self.session = SessionPhase::Fatal;
                    Ok(())
                }
                _ => Err(OracleError::InvalidUnsolicitedEvent),
            };
        }

        if matches!(event.message, EventMessage::Fatal(_)) {
            return Err(OracleError::FatalMustBeUnsolicited);
        }
        if matches!(self.session, SessionPhase::Fatal | SessionPhase::Shutdown) {
            return Err(OracleError::Session(
                "event arrived after the session terminated".into(),
            ));
        }

        let request_id = event.request_id;
        let mut request = self
            .requests
            .remove(&request_id)
            .ok_or(OracleError::UnknownRequest(request_id))?;

        let result = match request.state {
            State::Terminal => Err(OracleError::DuplicateTerminal(request_id)),
            State::TimedOut => Err(OracleError::LateEvent(request_id)),
            _ => self.transition_request(request_id, &mut request, &event.message),
        };
        self.requests.insert(request_id, request);
        result
    }

    fn transition_request(
        &mut self,
        request_id: RequestId,
        request: &mut RequestState,
        event: &EventMessage,
    ) -> Result<(), OracleError> {
        let invalid = |detail: String| OracleError::Transition { request_id, detail };
        let activations = &mut request.activations;

        match (&request.control, &mut request.state, event) {
            (ControlMessage::Hello(control), State::Pending, EventMessage::Hello(response)) => {
                if response.candidate != control.expected_candidate {
                    return Err(invalid(format!(
                        "candidate mismatch: expected {}, received {}",
                        control.expected_candidate, response.candidate
                    )));
                }
                request.state = State::Terminal;
                self.session = SessionPhase::Greeted;
            }
            (
                ControlMessage::Init(control),
                State::Pending,
                EventMessage::Initialized(response),
            ) => {
                if response.run_id != control.run_id {
                    return Err(invalid(
                        "initialized run_id does not match init request".into(),
                    ));
                }
                request.state = State::Terminal;
                self.session = SessionPhase::Running;
            }
            (
                ControlMessage::CreateTenant(control),
                State::Pending,
                EventMessage::TenantCreated(response),
            ) => {
                ensure_tenant(request_id, control.tenant_id, response.tenant_id)?;
                let expected = match self.last_incarnations.get(&control.tenant_id).copied() {
                    Some(previous) => previous.checked_next().ok_or_else(|| {
                        invalid("incarnation overflow prevents tenant recreation".into())
                    })?,
                    None => IncarnationToken::new(0),
                };
                if control.incarnation != expected {
                    return Err(invalid(format!(
                        "oracle create control must issue incarnation {expected}, issued {}",
                        control.incarnation
                    )));
                }
                if response.incarnation != control.incarnation {
                    return Err(invalid(format!(
                        "create must echo oracle incarnation {expected}, received {}",
                        response.incarnation
                    )));
                }
                request.state = State::CreateCreated(response.incarnation);
            }
            (
                ControlMessage::CreateTenant(control),
                State::Pending | State::CreateCreated(_),
                EventMessage::ActivationStarted(response),
            ) => {
                ensure_activation_context(
                    request_id,
                    control.tenant_id,
                    control.incarnation,
                    &control.logical_version,
                    &control.build_id,
                    &control.artifact_sha256,
                    response,
                )?;
                record_activation(request_id, activations, response)?;
            }
            (
                ControlMessage::CreateTenant(control),
                State::CreateCreated(created_incarnation),
                EventMessage::TenantReady(response),
            ) => {
                ensure_tenant(request_id, control.tenant_id, response.tenant_id)?;
                if response.incarnation != *created_incarnation {
                    return Err(invalid(
                        "ready incarnation differs from created incarnation".into(),
                    ));
                }
                ensure_activation_recorded(request_id, activations, control.tenant_id)?;
                self.tenants.insert(control.tenant_id, response.incarnation);
                self.last_incarnations
                    .insert(control.tenant_id, response.incarnation);
                request.state = State::Terminal;
            }
            (
                ControlMessage::Command(control),
                State::Pending,
                EventMessage::CommandAccepted(response),
            ) => {
                ensure_command_context(request_id, control, response)?;
                request.state = State::CommandAccepted;
            }
            (
                ControlMessage::Command(control),
                State::Pending,
                EventMessage::CommandRejected(response),
            ) => {
                ensure_command_context(request_id, control, response)?;
                request.state = State::Terminal;
            }
            (
                ControlMessage::Command(control),
                State::CommandAccepted,
                EventMessage::CommandCompleted(response),
            ) => {
                ensure_command_context(request_id, control, response)?;
                request.state = State::Terminal;
            }
            (
                ControlMessage::Command(control),
                State::CommandAccepted,
                EventMessage::CommandFailed(response),
            ) => {
                ensure_command_context(request_id, control, response)?;
                request.state = State::Terminal;
            }
            (
                ControlMessage::InjectFault(control),
                State::Pending,
                EventMessage::ExecutionStarted(response),
            ) => {
                ensure_fault_context(request_id, control, response)?;
                if !matches!(control.fault, FaultKind::CpuHog(_)) {
                    return Err(invalid("trap fault must not emit execution_started".into()));
                }
                if response.fault_id != control.fault_id {
                    return Err(invalid("execution_started echoed another fault_id".into()));
                }
                if response.origin != ExecutionStartOrigin::GuestFirstActionObserver {
                    return Err(invalid(
                        "CPU execution_started was not observed at the guest first action".into(),
                    ));
                }
                request.state = State::FaultExecutionStarted;
            }
            (
                ControlMessage::InjectFault(control),
                State::Pending,
                EventMessage::ExecutionFailed(response),
            ) => {
                ensure_fault_context(request_id, control, response)?;
                if !matches!(control.fault, FaultKind::Trap(_))
                    || response.reason != FaultFailureReason::GuestTrap
                {
                    return Err(invalid("trap fault must fail with guest_trap".into()));
                }
                if response.fault_id != control.fault_id {
                    return Err(invalid("execution_failed echoed another fault_id".into()));
                }
                request.state = State::FaultFailed;
            }
            (
                ControlMessage::InjectFault(control),
                State::FaultExecutionStarted,
                EventMessage::ExecutionFailed(response),
            ) => {
                ensure_fault_context(request_id, control, response)?;
                if !matches!(control.fault, FaultKind::CpuHog(_))
                    || response.reason != FaultFailureReason::CpuDeadline
                {
                    return Err(invalid("CPU fault must fail with cpu_deadline".into()));
                }
                if response.fault_id != control.fault_id {
                    return Err(invalid("execution_failed echoed another fault_id".into()));
                }
                request.state = State::FaultFailed;
            }
            (
                ControlMessage::InjectFault(control),
                State::FaultFailed,
                EventMessage::FailureObserved(response),
            ) => {
                ensure_tenant(request_id, control.tenant_id, response.tenant_id)?;
                if response.fault_id != control.fault_id {
                    return Err(invalid("failure_observed echoed another fault_id".into()));
                }
                if response.failed_incarnation != control.incarnation {
                    return Err(invalid("failure observer named another incarnation".into()));
                }
                request.state = State::FaultObserved;
            }
            (
                ControlMessage::InjectFault(control),
                State::FaultObserved,
                EventMessage::ActivationStarted(response),
            ) => {
                ensure_activation_context(
                    request_id,
                    control.tenant_id,
                    control.expected_replacement_incarnation,
                    &control.replacement_logical_version,
                    &control.replacement_build_id,
                    &control.replacement_artifact_sha256,
                    response,
                )?;
                record_activation(request_id, activations, response)?;
            }
            (
                ControlMessage::InjectFault(control),
                State::FaultObserved,
                EventMessage::TenantReady(response),
            ) => {
                ensure_tenant(request_id, control.tenant_id, response.tenant_id)?;
                let expected_next = control.incarnation.checked_next().ok_or_else(|| {
                    invalid("incarnation overflow prevents fault recovery".into())
                })?;
                if control.expected_replacement_incarnation != expected_next {
                    return Err(invalid(format!(
                        "fault control must issue replacement incarnation {expected_next}"
                    )));
                }
                if response.incarnation != control.expected_replacement_incarnation {
                    return Err(invalid(format!(
                        "recovered tenant must echo oracle incarnation {}, received {}",
                        control.expected_replacement_incarnation, response.incarnation
                    )));
                }
                ensure_activation_recorded(request_id, activations, control.tenant_id)?;
                self.tenants.insert(control.tenant_id, response.incarnation);
                self.last_incarnations
                    .insert(control.tenant_id, response.incarnation);
                request.state = State::Terminal;
            }
            (
                ControlMessage::Rollout(control),
                State::Pending,
                EventMessage::RolloutStarted(response),
            ) => {
                ensure_rollout(request_id, &control.rollout_id, &response.rollout_id)?;
                request.state = State::RolloutStarted(BTreeMap::new());
            }
            (
                ControlMessage::Rollout(control),
                State::RolloutStarted(ready),
                EventMessage::ActivationStarted(response),
            ) => {
                if !control.targets.contains(&response.tenant_id) {
                    return Err(invalid(format!(
                        "rollout activation reported unexpected tenant {}",
                        response.tenant_id
                    )));
                }
                if ready.contains_key(&response.tenant_id) {
                    return Err(invalid(format!(
                        "activation marker for tenant {} arrived after target_ready",
                        response.tenant_id
                    )));
                }
                let current = self.tenants.get(&response.tenant_id).ok_or_else(|| {
                    invalid(format!(
                        "rollout activation reported missing tenant {}",
                        response.tenant_id
                    ))
                })?;
                ensure_activation_context(
                    request_id,
                    response.tenant_id,
                    *current,
                    &control.to_version,
                    &control.to_build_id,
                    &control.artifact_sha256,
                    response,
                )?;
                record_activation(request_id, activations, response)?;
            }
            (
                ControlMessage::Rollout(control),
                State::RolloutStarted(ready),
                EventMessage::RolloutTargetReady(response),
            ) => {
                ensure_rollout(request_id, &control.rollout_id, &response.rollout_id)?;
                if !control.targets.contains(&response.tenant_id) {
                    return Err(invalid(format!(
                        "rollout reported unexpected tenant {}",
                        response.tenant_id
                    )));
                }
                let current = self.tenants.get(&response.tenant_id).ok_or_else(|| {
                    invalid(format!(
                        "rollout reported missing tenant {} ready",
                        response.tenant_id
                    ))
                })?;
                if response.incarnation != *current {
                    return Err(invalid(format!(
                        "rollout changed tenant {} incarnation from {current} to {}",
                        response.tenant_id, response.incarnation
                    )));
                }
                if response.logical_version != control.to_version
                    || response.build_id != control.to_build_id
                    || response.artifact_sha256 != control.artifact_sha256
                {
                    return Err(invalid(
                        "rollout ready did not echo exact version/build/artifact SHA".into(),
                    ));
                }
                ensure_activation_recorded(request_id, activations, response.tenant_id)?;
                if ready
                    .insert(response.tenant_id, response.incarnation)
                    .is_some()
                {
                    return Err(invalid(format!(
                        "rollout reported tenant {} ready twice",
                        response.tenant_id
                    )));
                }
            }
            (
                ControlMessage::Rollout(control),
                State::RolloutStarted(ready),
                EventMessage::RolloutCommitted(response),
            ) => {
                ensure_rollout(request_id, &control.rollout_id, &response.rollout_id)?;
                if response.version != control.to_version {
                    return Err(invalid(
                        "committed version differs from target version".into(),
                    ));
                }
                if ready.len() != control.targets.len() {
                    return Err(invalid(format!(
                        "commit arrived after {}/{} targets became ready",
                        ready.len(),
                        control.targets.len()
                    )));
                }
                request.state = State::Terminal;
            }
            (
                ControlMessage::Rollout(control),
                State::RolloutStarted(_),
                EventMessage::RolloutRolledBack(response),
            ) => {
                ensure_rollout(request_id, &control.rollout_id, &response.rollout_id)?;
                if response.restored_version != control.from_version {
                    return Err(invalid(
                        "rollback did not report the prior version restored".into(),
                    ));
                }
                request.state = State::Terminal;
            }
            (
                ControlMessage::Rollout(control),
                State::RolloutStarted(_),
                EventMessage::RolloutInDoubt(response),
            ) => {
                ensure_rollout(request_id, &control.rollout_id, &response.rollout_id)?;
                request.state = State::Terminal;
            }
            (
                ControlMessage::Rollout(control),
                State::Pending,
                EventMessage::RolloutRejected(response),
            ) => {
                ensure_rollout(request_id, &control.rollout_id, &response.rollout_id)?;
                request.state = State::Terminal;
            }
            (
                ControlMessage::Snapshot(control),
                State::Pending,
                EventMessage::SnapshotPresent(response),
            ) => {
                ensure_tenant(request_id, control.tenant_id, response.tenant_id)?;
                if let Some(expected) = &control.expected_incarnation {
                    if expected != &response.incarnation {
                        return Err(invalid(
                            "snapshot present used an unexpected incarnation".into(),
                        ));
                    }
                }
                request.state = State::Terminal;
            }
            (
                ControlMessage::Snapshot(control),
                State::Pending,
                EventMessage::SnapshotMissing(response),
            ) => {
                ensure_tenant(request_id, control.tenant_id, response.tenant_id)?;
                request.state = State::Terminal;
            }
            (
                ControlMessage::Snapshot(control),
                State::Pending,
                EventMessage::SnapshotStale(response),
            ) => {
                ensure_tenant(request_id, control.tenant_id, response.tenant_id)?;
                let Some(expected) = &control.expected_incarnation else {
                    return Err(invalid(
                        "snapshot stale requires an expected incarnation".into(),
                    ));
                };
                if expected != &response.expected_incarnation {
                    return Err(invalid(
                        "snapshot stale echoed a different expected incarnation".into(),
                    ));
                }
                if response.actual_incarnation == response.expected_incarnation {
                    return Err(invalid(
                        "snapshot stale reported identical expected and actual tokens".into(),
                    ));
                }
                request.state = State::Terminal;
            }
            (
                ControlMessage::SetDequeueGate(control),
                State::Pending,
                EventMessage::DequeueGateSet(response),
            ) => {
                ensure_tenant(request_id, control.tenant_id, response.tenant_id)?;
                if response.incarnation != control.incarnation {
                    return Err(invalid("gate ack echoed another incarnation".into()));
                }
                if response.closed != control.closed {
                    return Err(invalid("gate ack echoed another gate state".into()));
                }
                request.state = State::Terminal;
            }
            (
                ControlMessage::AuthorityProbe(control),
                State::Pending,
                EventMessage::AuthorityProbeAccepted(response),
            ) => {
                ensure_authority_context(
                    request_id,
                    control,
                    response.attempt_id,
                    response.probe,
                    &response.artifact_sha256,
                    &response.parameter_sha256,
                    &response.production_policy_sha256,
                )?;
                request.state = State::AuthorityProbeAccepted;
            }
            (
                ControlMessage::AuthorityProbe(control),
                State::AuthorityProbeAccepted,
                EventMessage::AuthorityProbeTerminal(response),
            ) => {
                ensure_authority_context(
                    request_id,
                    control,
                    response.attempt_id,
                    response.probe,
                    &response.artifact_sha256,
                    &response.parameter_sha256,
                    &response.production_policy_sha256,
                )?;
                request.state = State::Terminal;
            }
            (ControlMessage::Quiesce(_), State::Pending, EventMessage::Quiesced(_)) => {
                request.state = State::Terminal;
            }
            (
                ControlMessage::TeardownTenant(control),
                State::Pending,
                EventMessage::TenantTornDown(response),
            ) => {
                ensure_tenant(request_id, control.tenant_id, response.tenant_id)?;
                if response.incarnation != control.incarnation {
                    return Err(invalid("teardown confirmed another incarnation".into()));
                }
                if self.tenants.get(&control.tenant_id) == Some(&control.incarnation) {
                    self.tenants.remove(&control.tenant_id);
                }
                request.state = State::Terminal;
            }
            (ControlMessage::Shutdown(_), State::Pending, EventMessage::ShutdownComplete(_)) => {
                request.state = State::Terminal;
                self.session = SessionPhase::Shutdown;
            }
            (_, state, response) => {
                return Err(invalid(format!(
                    "event {response:?} is not valid in phase {:?}",
                    state.public_phase()
                )));
            }
        }
        Ok(())
    }

    fn validate_control_for_session(
        &mut self,
        control: &ControlMessage,
    ) -> Result<(), OracleError> {
        match (self.session, control) {
            (SessionPhase::New, ControlMessage::Hello(_)) => {
                self.session = SessionPhase::AwaitingHello;
            }
            (SessionPhase::Greeted, ControlMessage::Init(_)) => {
                self.session = SessionPhase::AwaitingInit;
            }
            (SessionPhase::Running, ControlMessage::CreateTenant(create)) => {
                if self.tenants.contains_key(&create.tenant_id) {
                    return Err(OracleError::Session(format!(
                        "tenant {} already exists",
                        create.tenant_id
                    )));
                }
            }
            (SessionPhase::Running, ControlMessage::Rollout(rollout)) => {
                if rollout.targets.is_empty() {
                    return Err(OracleError::Session(
                        "rollout target list must not be empty".into(),
                    ));
                }
                let mut unique = rollout.targets.clone();
                unique.sort_unstable();
                unique.dedup();
                if unique.len() != rollout.targets.len() {
                    return Err(OracleError::Session(
                        "rollout target list contains duplicates".into(),
                    ));
                }
            }
            (SessionPhase::Running, ControlMessage::Shutdown(_)) => {
                self.session = SessionPhase::AwaitingShutdown;
            }
            (
                SessionPhase::Running,
                ControlMessage::Command(_)
                | ControlMessage::InjectFault(_)
                | ControlMessage::Snapshot(_)
                | ControlMessage::SetDequeueGate(_)
                | ControlMessage::AuthorityProbe(_)
                | ControlMessage::Quiesce(_)
                | ControlMessage::TeardownTenant(_),
            ) => {}
            (phase, message) => {
                return Err(OracleError::Session(format!(
                    "control {message:?} is not valid in session phase {phase:?}"
                )));
            }
        }
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<(), OracleError> {
        match &self.poisoned {
            Some(detail) => Err(OracleError::Poisoned(detail.clone())),
            None => Ok(()),
        }
    }

    fn violation(&mut self, error: OracleError) -> OracleError {
        self.poisoned = Some(error.to_string());
        error
    }
}

trait CommandContext {
    fn tenant_id(&self) -> TenantId;
    fn incarnation(&self) -> &IncarnationToken;
    fn command_id(&self) -> CommandId;
}

impl CommandContext for CommandAcceptedEvent {
    fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    fn incarnation(&self) -> &IncarnationToken {
        &self.incarnation
    }

    fn command_id(&self) -> CommandId {
        self.command_id
    }
}

impl CommandContext for CommandRejectedEvent {
    fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    fn incarnation(&self) -> &IncarnationToken {
        &self.incarnation
    }

    fn command_id(&self) -> CommandId {
        self.command_id
    }
}

impl CommandContext for CommandCompletedEvent {
    fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    fn incarnation(&self) -> &IncarnationToken {
        &self.incarnation
    }

    fn command_id(&self) -> CommandId {
        self.command_id
    }
}

impl CommandContext for CommandFailedEvent {
    fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    fn incarnation(&self) -> &IncarnationToken {
        &self.incarnation
    }

    fn command_id(&self) -> CommandId {
        self.command_id
    }
}

fn ensure_command_context(
    request_id: RequestId,
    expected: &CommandControl,
    actual: &impl CommandContext,
) -> Result<(), OracleError> {
    if expected.tenant_id != actual.tenant_id()
        || expected.incarnation != *actual.incarnation()
        || expected.command_id != actual.command_id()
    {
        return Err(OracleError::Transition {
            request_id,
            detail: "command response context does not match its request".into(),
        });
    }
    Ok(())
}

trait FaultContext {
    fn tenant_id(&self) -> TenantId;
    fn incarnation(&self) -> &IncarnationToken;
}

impl FaultContext for ExecutionStartedEvent {
    fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    fn incarnation(&self) -> &IncarnationToken {
        &self.incarnation
    }
}

impl FaultContext for ExecutionFailedEvent {
    fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    fn incarnation(&self) -> &IncarnationToken {
        &self.incarnation
    }
}

fn ensure_fault_context(
    request_id: RequestId,
    expected: &InjectFaultControl,
    actual: &impl FaultContext,
) -> Result<(), OracleError> {
    if expected.tenant_id != actual.tenant_id() || expected.incarnation != *actual.incarnation() {
        return Err(OracleError::Transition {
            request_id,
            detail: "fault response context does not match its request".into(),
        });
    }
    Ok(())
}

fn ensure_tenant(
    request_id: RequestId,
    expected: TenantId,
    actual: TenantId,
) -> Result<(), OracleError> {
    if expected != actual {
        return Err(OracleError::Transition {
            request_id,
            detail: format!("tenant mismatch: expected {expected}, received {actual}"),
        });
    }
    Ok(())
}

fn ensure_rollout(request_id: RequestId, expected: &str, actual: &str) -> Result<(), OracleError> {
    if expected != actual {
        return Err(OracleError::Transition {
            request_id,
            detail: format!("rollout mismatch: expected {expected:?}, received {actual:?}"),
        });
    }
    Ok(())
}

fn ensure_activation_context(
    request_id: RequestId,
    tenant_id: TenantId,
    incarnation: IncarnationToken,
    logical_version: &str,
    build_id: &str,
    artifact_sha256: &str,
    actual: &ActivationStartedEvent,
) -> Result<(), OracleError> {
    if actual.tenant_id != tenant_id
        || actual.incarnation != incarnation
        || actual.logical_version != logical_version
        || actual.build_id != build_id
        || actual.artifact_sha256 != artifact_sha256
    {
        return Err(OracleError::Transition {
            request_id,
            detail: "activation marker does not match its request identity".into(),
        });
    }
    Ok(())
}

fn record_activation(
    request_id: RequestId,
    activations: &mut BTreeMap<TenantId, ActivationRecord>,
    marker: &ActivationStartedEvent,
) -> Result<(), OracleError> {
    if let Some(record) = activations.get_mut(&marker.tenant_id) {
        if record.marker != *marker {
            return Err(OracleError::Transition {
                request_id,
                detail: format!(
                    "conflicting activation marker for tenant {}",
                    marker.tenant_id
                ),
            });
        }
        if record.count >= 4 {
            return Err(OracleError::Transition {
                request_id,
                detail: format!(
                    "tenant {} emitted more than four activation markers",
                    marker.tenant_id
                ),
            });
        }
        record.count += 1;
    } else {
        activations.insert(
            marker.tenant_id,
            ActivationRecord {
                marker: marker.clone(),
                count: 1,
            },
        );
    }
    Ok(())
}

fn ensure_activation_recorded(
    request_id: RequestId,
    activations: &BTreeMap<TenantId, ActivationRecord>,
    tenant_id: TenantId,
) -> Result<(), OracleError> {
    if activations.contains_key(&tenant_id) {
        Ok(())
    } else {
        Err(OracleError::Transition {
            request_id,
            detail: format!("tenant {tenant_id} became ready without activation_started evidence"),
        })
    }
}

fn ensure_authority_context(
    request_id: RequestId,
    control: &AuthorityProbeControl,
    attempt_id: DecimalU64,
    probe: AuthorityProbeKind,
    artifact_sha256: &str,
    parameter_sha256: &str,
    production_policy_sha256: &str,
) -> Result<(), OracleError> {
    if attempt_id != control.attempt_id
        || probe != control.probe
        || artifact_sha256 != control.artifact_sha256
        || parameter_sha256 != control.parameter_sha256
        || production_policy_sha256 != control.production_policy_sha256
    {
        Err(OracleError::Transition {
            request_id,
            detail: "authority response context does not exactly match its request".into(),
        })
    } else {
        Ok(())
    }
}
