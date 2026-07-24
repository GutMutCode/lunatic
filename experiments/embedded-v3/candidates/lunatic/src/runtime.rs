use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, ensure, Context, Result};
use lunatic_process::runtimes::wasmtime::{
    default_config, WasmtimeCompiledModule, WasmtimeInstance, WasmtimeRuntime,
};
use lunatic_process::runtimes::RawWasm;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::events::EventSink;
use crate::protocol::{
    CommandControl, CommandOperation, CommandResult, DecimalU64, IncarnationToken, RequestId,
    TenantId,
};
use crate::state::{ActivationIdentity, CandidateState};

pub const MAILBOX_CAPACITY: usize = 64;
pub const PAYLOAD_LIMIT: usize = 1_024;

struct ArtifactSpec {
    file: &'static str,
    sha256: &'static str,
    guest_version: i32,
    guest_build: i64,
    module_id: u64,
}

const ARTIFACT_SPECS: [ArtifactSpec; 4] = [
    ArtifactSpec {
        file: "tenant-a.wasm",
        sha256: "9bce3d46eceef79abe98d88d16c2a8ac75ec1ffd68a7f328cc34a9c1a1fae0d1",
        guest_version: 1,
        guest_build: 0x4133_5633_544e_5401,
        module_id: 1,
    },
    ArtifactSpec {
        file: "tenant-b.wasm",
        sha256: "5c918a4ea3730ac7109188ec94108a135fb4a537a713bcbf27fae325a5ea511e",
        guest_version: 2,
        guest_build: 0x4233_5633_544e_5402,
        module_id: 2,
    },
    ArtifactSpec {
        file: "tenant-bad-a.wasm",
        sha256: "d3675abac308b7a20513f2ff65042c9b0e9e84195c16ec5256f8472a15653a2d",
        guest_version: 1,
        guest_build: 0x4133_5633_4241_4401,
        module_id: 3,
    },
    ArtifactSpec {
        file: "tenant-bad-b.wasm",
        sha256: "ab3274086121b6ff8af75b331c5c101b5216c187b86f8ea12cd06673e72dc557",
        guest_version: 2,
        guest_build: 0x4233_5633_4241_4402,
        module_id: 4,
    },
];

pub struct Artifact {
    pub file: &'static str,
    pub sha256: &'static str,
    pub guest_version: i32,
    pub guest_build: i64,
    pub module: Arc<WasmtimeCompiledModule<CandidateState>>,
}

pub struct RuntimeSet {
    runtime: Arc<WasmtimeRuntime>,
    by_hash: HashMap<String, Arc<Artifact>>,
}

impl RuntimeSet {
    pub fn load(artifact_root: &Path) -> Result<Arc<Self>> {
        let runtime = Arc::new(WasmtimeRuntime::new(&default_config())?);
        let mut by_hash = HashMap::with_capacity(ARTIFACT_SPECS.len());
        for spec in ARTIFACT_SPECS {
            let path = artifact_root.join(spec.file);
            let bytes = fs::read(&path)
                .with_context(|| format!("reading core artifact {}", path.display()))?;
            ensure!(
                sha256_hex(&bytes) == spec.sha256,
                "core artifact digest drift"
            );
            let module = runtime
                .compile_module::<CandidateState>(RawWasm::new(Some(spec.module_id), bytes))?;
            let artifact = Arc::new(Artifact {
                file: spec.file,
                sha256: spec.sha256,
                guest_version: spec.guest_version,
                guest_build: spec.guest_build,
                module: Arc::new(module),
            });
            by_hash.insert(spec.sha256.to_owned(), artifact);
        }
        Ok(Arc::new(Self { runtime, by_hash }))
    }

    pub fn artifact(&self, sha256: &str) -> Option<Arc<Artifact>> {
        self.by_hash.get(sha256).cloned()
    }

    pub fn artifact_matches_ref(&self, artifact: &Artifact, reference: &str) -> bool {
        Path::new(reference) == Path::new("guest-artifacts").join(artifact.file)
    }

    pub async fn instantiate(
        &self,
        artifact: Arc<Artifact>,
        identity: ActivationIdentity,
        request_id: RequestId,
        emit_activation: bool,
        restore_counter: i64,
        sink: EventSink,
    ) -> Result<WasmtimeInstance<CandidateState>> {
        let state = CandidateState::new(
            Arc::clone(&self.runtime),
            Arc::clone(&artifact.module),
            identity,
            request_id,
            emit_activation,
            restore_counter,
            sink,
        );
        self.runtime.instantiate(&artifact.module, state).await
    }

    pub fn compile_authority(&self, bytes: Vec<u8>) -> Result<()> {
        self.runtime
            .compile_module::<CandidateState>(RawWasm::new(None, bytes))?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Fingerprint {
    operation: OperationKey,
    payload_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OperationKey {
    Increment(i64),
    Read,
}

impl From<&CommandOperation> for OperationKey {
    fn from(operation: &CommandOperation) -> Self {
        match operation {
            CommandOperation::Increment(value) => Self::Increment(value.delta),
            CommandOperation::Read(_) => Self::Read,
        }
    }
}

struct AdmissionState {
    incarnation: IncarnationToken,
    gate_closed: bool,
    outstanding: usize,
    next_ticket: u64,
    serving_ticket: u64,
    fingerprints: HashMap<u64, Fingerprint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Accepted(u64),
    StaleIncarnation,
    PayloadTooLarge,
    CommandIdConflict,
    Backpressure,
}

#[derive(Clone)]
struct CachedResult {
    result: CommandResult,
}

pub struct TenantRuntime {
    pub identity: ActivationIdentity,
    pub artifact: Arc<Artifact>,
    pub instance: WasmtimeInstance<CandidateState>,
    completed: HashMap<u64, CachedResult>,
}

impl TenantRuntime {
    pub fn new(
        identity: ActivationIdentity,
        artifact: Arc<Artifact>,
        instance: WasmtimeInstance<CandidateState>,
    ) -> Self {
        Self {
            identity,
            artifact,
            instance,
            completed: HashMap::new(),
        }
    }

    pub fn clear_completed(&mut self) {
        self.completed.clear();
    }
}

pub struct Tenant {
    admission: Mutex<AdmissionState>,
    runtime: AsyncMutex<TenantRuntime>,
    turn_changed: Notify,
}

impl Tenant {
    pub fn new(runtime: TenantRuntime) -> Self {
        Self {
            admission: Mutex::new(AdmissionState {
                incarnation: runtime.identity.incarnation,
                gate_closed: false,
                outstanding: 0,
                next_ticket: 0,
                serving_ticket: 0,
                fingerprints: HashMap::new(),
            }),
            runtime: AsyncMutex::new(runtime),
            turn_changed: Notify::new(),
        }
    }

    pub fn incarnation(&self) -> Result<IncarnationToken> {
        Ok(self.lock_admission()?.incarnation)
    }

    pub fn admit(&self, command: &CommandControl) -> Result<Admission> {
        let mut state = self.lock_admission()?;
        if command.incarnation != state.incarnation {
            return Ok(Admission::StaleIncarnation);
        }
        if command.payload.len() > PAYLOAD_LIMIT {
            return Ok(Admission::PayloadTooLarge);
        }
        let fingerprint = Fingerprint {
            operation: OperationKey::from(&command.operation),
            payload_sha256: command.payload_sha256.clone(),
        };
        if state
            .fingerprints
            .get(&command.command_id.0)
            .is_some_and(|known| known != &fingerprint)
        {
            return Ok(Admission::CommandIdConflict);
        }
        if state.outstanding >= MAILBOX_CAPACITY {
            return Ok(Admission::Backpressure);
        }
        state
            .fingerprints
            .entry(command.command_id.0)
            .or_insert(fingerprint);
        let ticket = state.next_ticket;
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .ok_or_else(|| anyhow!("tenant admission ticket overflow"))?;
        state.outstanding += 1;
        Ok(Admission::Accepted(ticket))
    }

    pub fn finish_command(&self, ticket: u64) -> Result<()> {
        let mut state = self.lock_admission()?;
        ensure!(
            state.serving_ticket == ticket,
            "tenant commands completed outside FIFO order"
        );
        state.outstanding = state
            .outstanding
            .checked_sub(1)
            .ok_or_else(|| anyhow!("tenant outstanding command underflow"))?;
        state.serving_ticket = state
            .serving_ticket
            .checked_add(1)
            .ok_or_else(|| anyhow!("tenant serving ticket overflow"))?;
        self.turn_changed.notify_waiters();
        Ok(())
    }

    pub fn set_gate(&self, incarnation: IncarnationToken, closed: bool) -> Result<()> {
        let mut state = self.lock_admission()?;
        ensure!(state.incarnation == incarnation, "stale gate incarnation");
        ensure!(
            !closed || state.outstanding == 0,
            "closing a busy tenant gate"
        );
        state.gate_closed = closed;
        Ok(())
    }

    pub fn release_gate_waiters(&self) {
        self.turn_changed.notify_waiters();
    }

    pub fn outstanding(&self) -> Result<usize> {
        Ok(self.lock_admission()?.outstanding)
    }

    pub fn reset_incarnation(&self, incarnation: IncarnationToken) -> Result<()> {
        let mut state = self.lock_admission()?;
        ensure!(
            state.outstanding == 0,
            "replacing tenant with outstanding commands"
        );
        state.incarnation = incarnation;
        state.gate_closed = false;
        state.fingerprints.clear();
        Ok(())
    }

    pub async fn execute(
        &self,
        ticket: u64,
        request_id: RequestId,
        command: &CommandControl,
    ) -> Result<CommandResult> {
        loop {
            let changed = self.turn_changed.notified();
            let ready = {
                let state = self.lock_admission()?;
                !state.gate_closed && state.serving_ticket == ticket
            };
            if ready {
                break;
            }
            changed.await;
        }
        let mut runtime = self.runtime.lock().await;
        if let Some(cached) = runtime.completed.get(&command.command_id.0) {
            let mut result = cached.result.clone();
            result.deduplicated = true;
            return Ok(result);
        }
        let handle = i64::try_from(request_id.0).context("request_id exceeds guest handle ABI")?;
        runtime.instance.state_mut().begin_command(handle);
        let (export, kind) = match &command.operation {
            CommandOperation::Increment(value) if value.delta == 1 => ("increment", 1),
            CommandOperation::Read(_) => ("probe", 2),
            CommandOperation::Increment(_) => bail!("core guest only supports delta one"),
        };
        runtime.instance.call_ref(export, Vec::new()).await?;
        let emitted = runtime.instance.state_mut().take_result()?;
        ensure!(emitted.kind == kind, "guest emitted another operation kind");
        let result = result_from_guest(&runtime.identity, emitted.counter, false);
        runtime.completed.insert(
            command.command_id.0,
            CachedResult {
                result: result.clone(),
            },
        );
        Ok(result)
    }

    pub async fn snapshot(&self, request_id: RequestId) -> Result<CommandResult> {
        let mut runtime = self.runtime.lock().await;
        let handle = i64::try_from(request_id.0).context("request_id exceeds guest handle ABI")?;
        runtime.instance.state_mut().begin_command(handle);
        runtime.instance.call_ref("snapshot", Vec::new()).await?;
        let emitted = runtime.instance.state_mut().take_result()?;
        ensure!(
            emitted.kind == 3,
            "guest snapshot emitted another operation kind"
        );
        Ok(result_from_guest(&runtime.identity, emitted.counter, false))
    }

    pub async fn lock_runtime(&self) -> tokio::sync::MutexGuard<'_, TenantRuntime> {
        self.runtime.lock().await
    }

    fn lock_admission(&self) -> Result<std::sync::MutexGuard<'_, AdmissionState>> {
        self.admission
            .lock()
            .map_err(|_| anyhow!("tenant admission lock poisoned"))
    }
}

pub fn activation_identity(
    tenant_id: TenantId,
    incarnation: IncarnationToken,
    logical_version: String,
    build_id: String,
    artifact: &Artifact,
) -> ActivationIdentity {
    ActivationIdentity {
        tenant_id,
        incarnation,
        logical_version,
        build_id,
        artifact_sha256: artifact.sha256.to_owned(),
        guest_version: artifact.guest_version,
        guest_build: artifact.guest_build,
    }
}

pub fn result_from_guest(
    identity: &ActivationIdentity,
    counter: i64,
    deduplicated: bool,
) -> CommandResult {
    let generation = identity.incarnation.value();
    CommandResult {
        incarnation: identity.incarnation,
        generation: DecimalU64(generation),
        counter,
        logical_version: identity.logical_version.clone(),
        build_id: identity.build_id.clone(),
        deduplicated,
        business_result_sha256: business_result_digest(generation, counter),
    }
}

fn business_result_digest(generation: u64, counter: i64) -> String {
    sha256_hex(format!(r#"{{"counter":{counter},"generation":{generation}}}"#).as_bytes())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
