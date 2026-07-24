use std::collections::{BTreeMap, BTreeSet, HashMap};

use sha2::{Digest, Sha256};

use crate::protocol::TenantId;
use crate::workload::{business_result_digest, WorkloadError, MAILBOX_CAPACITY, MAX_PAYLOAD_BYTES};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionOutcome {
    Accepted,
    RetryableRejection,
    StaleIncarnation,
    Oversize,
    TenantMissing,
    CommandIdConflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelOperation {
    Increment(i64),
    Read,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCommand {
    pub request_id: u64,
    pub tenant_id: TenantId,
    pub incarnation: u64,
    pub command_id: u64,
    pub operation: ModelOperation,
    pub payload: Vec<u8>,
    pub payload_sha256: String,
}

impl ModelCommand {
    fn validate_payload_digest(&self) -> Result<(), WorkloadError> {
        let actual = sha256_hex(&self.payload);
        if self.payload_sha256 == actual {
            Ok(())
        } else {
            Err(WorkloadError::State(format!(
                "request {} payload SHA-256 mismatch",
                self.request_id
            )))
        }
    }

    fn delta(&self) -> i64 {
        match self.operation {
            ModelOperation::Increment(delta) => delta,
            ModelOperation::Read => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuestBusinessResult {
    pub incarnation: u64,
    pub generation: u64,
    pub counter: i64,
    pub logical_version: String,
    pub build_id: String,
    pub deduplicated: bool,
    pub business_result_sha256: String,
}

#[derive(Debug, Clone)]
struct CachedResult {
    result: GuestBusinessResult,
    operation: ModelOperation,
    payload_sha256: String,
}

impl CachedResult {
    fn matches(&self, command: &ModelCommand) -> bool {
        self.operation == command.operation && self.payload_sha256 == command.payload_sha256
    }
}

#[derive(Debug, Clone)]
struct ModelTenant {
    incarnation: u64,
    generation: u64,
    counter: i64,
    logical_version: String,
    build_id: String,
    completed: HashMap<u64, CachedResult>,
}

#[derive(Debug, Clone)]
struct PendingCommand {
    command: ModelCommand,
    accepted: bool,
    fingerprint_conflict: bool,
    send_artifacts: BTreeSet<(String, String)>,
}

#[derive(Debug, Clone, Default)]
pub struct StateOracle {
    active: BTreeMap<TenantId, ModelTenant>,
    last_incarnation: BTreeMap<TenantId, u64>,
    pending: BTreeMap<u64, PendingCommand>,
    receive_artifacts: BTreeMap<TenantId, BTreeSet<(String, String)>>,
    dequeue_gate_closed: BTreeMap<TenantId, bool>,
    accepted_outstanding: BTreeMap<TenantId, u32>,
}

impl StateOracle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn expected_create_incarnation(&self, tenant: TenantId) -> Result<u64, WorkloadError> {
        if self.active.contains_key(&tenant) {
            return Err(WorkloadError::State(format!(
                "tenant {} already active",
                tenant.0
            )));
        }
        self.last_incarnation.get(&tenant).map_or(Ok(0), |value| {
            value
                .checked_add(1)
                .ok_or_else(|| WorkloadError::State("incarnation overflow".into()))
        })
    }

    pub fn create(
        &mut self,
        tenant: TenantId,
        echoed_incarnation: u64,
        version: &str,
        build_id: &str,
    ) -> Result<(), WorkloadError> {
        let expected = self.expected_create_incarnation(tenant)?;
        if echoed_incarnation != expected {
            return Err(WorkloadError::State(format!(
                "candidate chose incarnation {echoed_incarnation}; oracle required {expected}"
            )));
        }
        let artifact = (version.to_owned(), build_id.to_owned());
        self.last_incarnation.insert(tenant, expected);
        self.receive_artifacts
            .insert(tenant, BTreeSet::from([artifact]));
        self.dequeue_gate_closed.insert(tenant, false);
        self.accepted_outstanding.insert(tenant, 0);
        self.active.insert(
            tenant,
            ModelTenant {
                incarnation: expected,
                generation: expected,
                counter: 0,
                logical_version: version.into(),
                build_id: build_id.into(),
                completed: HashMap::new(),
            },
        );
        Ok(())
    }

    pub fn recover(
        &mut self,
        tenant: TenantId,
        echoed_incarnation: u64,
    ) -> Result<(), WorkloadError> {
        if self.accepted_outstanding.get(&tenant).copied().unwrap_or(0) != 0 {
            return Err(WorkloadError::State(
                "fault recovery with accepted commands still outstanding".into(),
            ));
        }
        let old = self
            .active
            .remove(&tenant)
            .ok_or_else(|| WorkloadError::State("recovering inactive tenant".into()))?;
        let expected = old
            .incarnation
            .checked_add(1)
            .ok_or_else(|| WorkloadError::State("incarnation overflow".into()))?;
        if echoed_incarnation != expected {
            return Err(WorkloadError::State(format!(
                "recovery incarnation must be {expected}, got {echoed_incarnation}"
            )));
        }
        self.last_incarnation.insert(tenant, expected);
        self.dequeue_gate_closed.insert(tenant, false);
        self.active.insert(
            tenant,
            ModelTenant {
                incarnation: expected,
                generation: expected,
                counter: 0,
                logical_version: old.logical_version,
                build_id: old.build_id,
                completed: HashMap::new(),
            },
        );
        Ok(())
    }

    pub fn set_dequeue_gate(
        &mut self,
        tenant: TenantId,
        incarnation: u64,
        closed: bool,
    ) -> Result<(), WorkloadError> {
        let active = self
            .active
            .get(&tenant)
            .ok_or_else(|| WorkloadError::State("gate target missing".into()))?;
        if active.incarnation != incarnation {
            return Err(WorkloadError::State(
                "gate control used stale incarnation".into(),
            ));
        }
        if closed && self.accepted_outstanding.get(&tenant).copied().unwrap_or(0) != 0 {
            return Err(WorkloadError::State(
                "gate closed with commands already outstanding".into(),
            ));
        }
        self.dequeue_gate_closed.insert(tenant, closed);
        Ok(())
    }

    pub fn begin_command(&mut self, command: ModelCommand) -> Result<(), WorkloadError> {
        if self.pending.contains_key(&command.request_id) {
            return Err(WorkloadError::State(format!(
                "request {} reused",
                command.request_id
            )));
        }
        command.validate_payload_digest()?;
        let fingerprint_conflict = self.pending.values().any(|pending| {
            pending.command.tenant_id == command.tenant_id
                && pending.command.incarnation == command.incarnation
                && pending.command.command_id == command.command_id
                && (pending.command.operation != command.operation
                    || pending.command.payload_sha256 != command.payload_sha256)
        });
        let send_artifacts = self
            .receive_artifacts
            .get(&command.tenant_id)
            .cloned()
            .unwrap_or_default();
        self.pending.insert(
            command.request_id,
            PendingCommand {
                command,
                accepted: false,
                fingerprint_conflict,
                send_artifacts,
            },
        );
        Ok(())
    }

    pub fn observe_admission(
        &mut self,
        request_id: u64,
        outcome: AdmissionOutcome,
    ) -> Result<(), WorkloadError> {
        let pending = self
            .pending
            .get_mut(&request_id)
            .ok_or_else(|| WorkloadError::State(format!("unknown request {request_id}")))?;
        if pending.accepted {
            return Err(WorkloadError::State("duplicate admission".into()));
        }
        let active = self.active.get(&pending.command.tenant_id);
        let expected = match active {
            None => AdmissionOutcome::TenantMissing,
            Some(tenant) if tenant.incarnation != pending.command.incarnation => {
                AdmissionOutcome::StaleIncarnation
            }
            Some(_) if pending.command.payload.len() > MAX_PAYLOAD_BYTES as usize => {
                AdmissionOutcome::Oversize
            }
            Some(_) if pending.fingerprint_conflict => AdmissionOutcome::CommandIdConflict,
            Some(tenant)
                if tenant
                    .completed
                    .get(&pending.command.command_id)
                    .is_some_and(|cached| !cached.matches(&pending.command)) =>
            {
                AdmissionOutcome::CommandIdConflict
            }
            Some(_)
                if self
                    .accepted_outstanding
                    .get(&pending.command.tenant_id)
                    .copied()
                    .unwrap_or(0)
                    >= MAILBOX_CAPACITY =>
            {
                AdmissionOutcome::RetryableRejection
            }
            Some(_) => AdmissionOutcome::Accepted,
        };
        if outcome != expected {
            return Err(WorkloadError::State(format!(
                "request {request_id} admission {outcome:?}; oracle required {expected:?}"
            )));
        }
        if outcome == AdmissionOutcome::Accepted {
            pending.accepted = true;
            let outstanding = self
                .accepted_outstanding
                .entry(pending.command.tenant_id)
                .or_default();
            *outstanding = outstanding
                .checked_add(1)
                .ok_or_else(|| WorkloadError::State("outstanding command overflow".into()))?;
        } else {
            self.pending.remove(&request_id);
        }
        Ok(())
    }

    pub fn observe_completed(
        &mut self,
        request_id: u64,
        result: &GuestBusinessResult,
    ) -> Result<(), WorkloadError> {
        let pending = self.pending.remove(&request_id).ok_or_else(|| {
            WorkloadError::State(format!("unknown or terminal request {request_id}"))
        })?;
        if !pending.accepted {
            return Err(WorkloadError::State(
                "terminal result without accepted admission".into(),
            ));
        }
        let outstanding = self
            .accepted_outstanding
            .get_mut(&pending.command.tenant_id)
            .ok_or_else(|| WorkloadError::State("missing outstanding counter".into()))?;
        *outstanding = outstanding
            .checked_sub(1)
            .ok_or_else(|| WorkloadError::State("outstanding command underflow".into()))?;
        if self
            .dequeue_gate_closed
            .get(&pending.command.tenant_id)
            .copied()
            .unwrap_or(false)
        {
            return Err(WorkloadError::State(
                "command completed while oracle dequeue gate was closed".into(),
            ));
        }

        let tenant = self
            .active
            .get_mut(&pending.command.tenant_id)
            .ok_or_else(|| WorkloadError::State("accepted tenant disappeared".into()))?;
        if let Some(cached) = tenant.completed.get(&pending.command.command_id).cloned() {
            if cached.result.generation != tenant.generation || !cached.matches(&pending.command) {
                return Err(WorkloadError::State(
                    "cache crossed an incarnation or command fingerprint boundary".into(),
                ));
            }
            let mut expected = cached.result;
            expected.deduplicated = true;
            if result != &expected {
                return Err(WorkloadError::State(format!(
                    "request {request_id} did not return the original cached result"
                )));
            }
            return Ok(());
        }
        let receive_artifacts = self
            .receive_artifacts
            .get(&pending.command.tenant_id)
            .ok_or_else(|| WorkloadError::State("missing receive artifact set".into()))?;
        let allowed_artifacts: BTreeSet<_> = pending
            .send_artifacts
            .intersection(receive_artifacts)
            .cloned()
            .collect();
        if !allowed_artifacts.contains(&(result.logical_version.clone(), result.build_id.clone())) {
            return Err(WorkloadError::State(
                "guest version/build pair is outside send/receive intersection".into(),
            ));
        }
        if result.incarnation != tenant.incarnation || result.generation != tenant.generation {
            return Err(WorkloadError::State(
                "guest result has forged incarnation/generation".into(),
            ));
        }

        let expected_counter = tenant
            .counter
            .checked_add(pending.command.delta())
            .ok_or_else(|| WorkloadError::State("counter overflow".into()))?;
        let expected_digest = business_result_digest(tenant.generation, expected_counter);
        if result.counter != expected_counter
            || result.deduplicated
            || result.business_result_sha256 != expected_digest
        {
            return Err(WorkloadError::State(format!(
                "request {request_id} forged business result"
            )));
        }
        tenant.counter = expected_counter;
        tenant.completed.insert(
            pending.command.command_id,
            CachedResult {
                result: result.clone(),
                operation: pending.command.operation,
                payload_sha256: pending.command.payload_sha256,
            },
        );
        tenant.logical_version = result.logical_version.clone();
        tenant.build_id = result.build_id.clone();
        Ok(())
    }

    pub fn expected_result(
        &self,
        command: &ModelCommand,
    ) -> Result<GuestBusinessResult, WorkloadError> {
        command.validate_payload_digest()?;
        let tenant = self
            .active
            .get(&command.tenant_id)
            .ok_or_else(|| WorkloadError::State("tenant missing".into()))?;
        if tenant.incarnation != command.incarnation {
            return Err(WorkloadError::State("stale command incarnation".into()));
        }
        if let Some(cached) = tenant.completed.get(&command.command_id) {
            if !cached.matches(command) {
                return Err(WorkloadError::State(
                    "command_id reused with another operation or payload".into(),
                ));
            }
            let mut result = cached.result.clone();
            result.deduplicated = true;
            return Ok(result);
        }
        let counter = tenant
            .counter
            .checked_add(command.delta())
            .ok_or_else(|| WorkloadError::State("counter overflow".into()))?;
        Ok(GuestBusinessResult {
            incarnation: tenant.incarnation,
            generation: tenant.generation,
            counter,
            logical_version: tenant.logical_version.clone(),
            build_id: tenant.build_id.clone(),
            deduplicated: false,
            business_result_sha256: business_result_digest(tenant.generation, counter),
        })
    }

    pub fn begin_rollout(
        &mut self,
        targets: &[TenantId],
        to_version: &str,
        to_build: &str,
        externally_servable: bool,
    ) -> Result<(), WorkloadError> {
        for tenant in targets {
            if !self.active.contains_key(tenant) {
                return Err(WorkloadError::State(format!(
                    "rollout target {} missing",
                    tenant.0
                )));
            }
            if externally_servable {
                self.receive_artifacts
                    .entry(*tenant)
                    .or_default()
                    .insert((to_version.into(), to_build.into()));
            }
        }
        Ok(())
    }

    pub fn finish_rollout(
        &mut self,
        targets: &[TenantId],
        version: &str,
        build: &str,
    ) -> Result<(), WorkloadError> {
        for tenant_id in targets {
            let tenant = self
                .active
                .get_mut(tenant_id)
                .ok_or_else(|| WorkloadError::State("rollout target missing".into()))?;
            tenant.logical_version = version.into();
            tenant.build_id = build.into();
        }
        Ok(())
    }

    pub fn settle_rollout(
        &mut self,
        targets: &[TenantId],
        version: &str,
        build: &str,
    ) -> Result<(), WorkloadError> {
        for tenant_id in targets {
            if self
                .accepted_outstanding
                .get(tenant_id)
                .copied()
                .unwrap_or(0)
                != 0
            {
                return Err(WorkloadError::State(
                    "rollout settled before accepted command drain".into(),
                ));
            }
            self.receive_artifacts
                .insert(*tenant_id, BTreeSet::from([(version.into(), build.into())]));
        }
        Ok(())
    }

    pub fn teardown(
        &mut self,
        tenant: TenantId,
        echoed_incarnation: u64,
    ) -> Result<(), WorkloadError> {
        let active = self
            .active
            .get(&tenant)
            .ok_or_else(|| WorkloadError::State("teardown of inactive tenant".into()))?;
        if active.incarnation != echoed_incarnation {
            return Err(WorkloadError::State(
                "teardown acknowledged wrong incarnation".into(),
            ));
        }
        if self.accepted_outstanding.get(&tenant).copied().unwrap_or(0) != 0 {
            return Err(WorkloadError::State(
                "teardown with accepted commands outstanding".into(),
            ));
        }
        self.active.remove(&tenant);
        self.receive_artifacts.remove(&tenant);
        self.dequeue_gate_closed.remove(&tenant);
        self.accepted_outstanding.remove(&tenant);
        Ok(())
    }

    pub fn active_tenants(&self) -> usize {
        self.active.len()
    }

    pub fn incarnation(&self, tenant: TenantId) -> Option<u64> {
        self.active.get(&tenant).map(|state| state.incarnation)
    }

    pub fn snapshot(&self, tenant: TenantId) -> Option<GuestBusinessResult> {
        self.active.get(&tenant).map(|state| GuestBusinessResult {
            incarnation: state.incarnation,
            generation: state.generation,
            counter: state.counter,
            logical_version: state.logical_version.clone(),
            build_id: state.build_id.clone(),
            deduplicated: false,
            business_result_sha256: business_result_digest(state.generation, state.counter),
        })
    }

    pub fn accepted_outstanding(&self, tenant: TenantId) -> u32 {
        self.accepted_outstanding.get(&tenant).copied().unwrap_or(0)
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
