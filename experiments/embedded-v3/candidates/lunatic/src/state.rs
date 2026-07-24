use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, ensure, Result};
use lunatic_process::config::ProcessConfig;
use lunatic_process::mailbox::MessageMailbox;
use lunatic_process::reloadable_state::ReloadableState;
use lunatic_process::runtimes::wasmtime::{WasmtimeCompiledModule, WasmtimeRuntime};
use lunatic_process::state::{
    mailboxes_with_limits, ConfigResources, ProcessState, SignalReceiver, SignalSender,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use wasmtime::{Caller, Linker, ResourceLimiter};

use crate::events::EventSink;
use crate::protocol::{
    ActivationStartedEvent, DecimalU64, EventMessage, ExecutionStartOrigin, ExecutionStartedEvent,
    IncarnationToken, RequestId, TenantId,
};

const MEMORY_LIMIT: usize = 65_536;

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct CandidateConfig {
    max_fuel: Option<u64>,
    max_memory: usize,
}

impl CandidateConfig {
    fn production() -> Self {
        Self {
            max_fuel: None,
            max_memory: MEMORY_LIMIT,
        }
    }
}

impl ProcessConfig for CandidateConfig {
    fn set_max_fuel(&mut self, value: Option<u64>) {
        self.max_fuel = value;
    }

    fn get_max_fuel(&self) -> Option<u64> {
        self.max_fuel
    }

    fn set_max_memory(&mut self, value: usize) {
        self.max_memory = value;
    }

    fn get_max_memory(&self) -> usize {
        self.max_memory
    }

    fn set_max_mailbox_messages(&mut self, _max: u32) {}

    fn get_max_mailbox_messages(&self) -> u32 {
        64
    }

    fn set_max_signal_queue(&mut self, _max: u32) {}

    fn get_max_signal_queue(&self) -> u32 {
        65
    }

    fn set_max_message_size(&mut self, _max: u64) {}

    fn get_max_message_size(&self) -> u64 {
        1_024
    }

    fn set_max_message_resources(&mut self, _max: u32) {}

    fn get_max_message_resources(&self) -> u32 {
        0
    }
}

#[derive(Clone)]
pub struct ActivationIdentity {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub logical_version: String,
    pub build_id: String,
    pub artifact_sha256: String,
    pub guest_version: i32,
    pub guest_build: i64,
}

#[derive(Debug, Clone)]
pub struct GuestResult {
    pub handle: i64,
    pub counter: i64,
    pub kind: i32,
    pub version: i32,
    pub build: i64,
}

#[derive(Clone)]
struct FaultObserver {
    request_id: RequestId,
    fault_id: DecimalU64,
    tenant_id: TenantId,
    incarnation: IncarnationToken,
}

pub struct CandidateState {
    runtime: Arc<WasmtimeRuntime>,
    module: Arc<WasmtimeCompiledModule<Self>>,
    config: Arc<CandidateConfig>,
    identity: ActivationIdentity,
    activation_request: RequestId,
    emit_activation: bool,
    sink: EventSink,
    restore_counter: i64,
    command_handle: i64,
    result: Option<GuestResult>,
    fault_observer: Option<FaultObserver>,
    initialized: bool,
    mailboxes: ((SignalSender, SignalReceiver), MessageMailbox),
    config_resources: ConfigResources<CandidateConfig>,
    registry: Arc<RwLock<HashMap<String, (u64, u64)>>>,
}

impl CandidateState {
    pub fn new(
        runtime: Arc<WasmtimeRuntime>,
        module: Arc<WasmtimeCompiledModule<Self>>,
        identity: ActivationIdentity,
        activation_request: RequestId,
        emit_activation: bool,
        restore_counter: i64,
        sink: EventSink,
    ) -> Self {
        Self {
            runtime,
            module,
            config: Arc::new(CandidateConfig::production()),
            identity,
            activation_request,
            emit_activation,
            sink,
            restore_counter,
            command_handle: 0,
            result: None,
            fault_observer: None,
            initialized: false,
            mailboxes: mailboxes_with_limits(65, 64, 1_024, 0),
            config_resources: ConfigResources::default(),
            registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn begin_command(&mut self, handle: i64) {
        self.command_handle = handle;
        self.result = None;
        self.fault_observer = None;
    }

    pub fn begin_fault(
        &mut self,
        handle: i64,
        request_id: RequestId,
        fault_id: DecimalU64,
        incarnation: IncarnationToken,
    ) {
        self.begin_command(handle);
        self.fault_observer = Some(FaultObserver {
            request_id,
            fault_id,
            tenant_id: self.identity.tenant_id,
            incarnation,
        });
    }

    pub fn take_result(&mut self) -> Result<GuestResult> {
        self.result
            .take()
            .ok_or_else(|| anyhow!("guest export did not emit a result"))
    }

    fn activation_started(&mut self, tenant: i32, version: i32, build: i64) -> Result<()> {
        ensure!(
            tenant >= 0 && tenant as u32 == self.identity.tenant_id.0,
            "guest activation tenant mismatch"
        );
        ensure!(
            version == self.identity.guest_version,
            "guest activation version mismatch"
        );
        ensure!(
            build == self.identity.guest_build,
            "guest activation build mismatch"
        );
        if self.emit_activation {
            self.sink.emit(
                self.activation_request,
                EventMessage::ActivationStarted(ActivationStartedEvent {
                    tenant_id: self.identity.tenant_id,
                    incarnation: self.identity.incarnation,
                    logical_version: self.identity.logical_version.clone(),
                    build_id: self.identity.build_id.clone(),
                    artifact_sha256: self.identity.artifact_sha256.clone(),
                }),
            )?;
        }
        Ok(())
    }

    fn emit_result(&mut self, result: GuestResult) -> Result<()> {
        ensure!(
            result.handle == self.command_handle,
            "guest result handle mismatch"
        );
        ensure!(
            result.version == self.identity.guest_version,
            "guest result version mismatch"
        );
        ensure!(
            result.build == self.identity.guest_build,
            "guest result build mismatch"
        );
        ensure!(self.result.is_none(), "guest emitted more than one result");
        self.result = Some(result);
        Ok(())
    }

    fn execution_started(&mut self, handle: i64) -> Result<()> {
        ensure!(
            handle == self.command_handle,
            "guest execution handle mismatch"
        );
        let observer = self
            .fault_observer
            .clone()
            .ok_or_else(|| anyhow!("guest execution marker outside a CPU fault"))?;
        self.sink.emit(
            observer.request_id,
            EventMessage::ExecutionStarted(ExecutionStartedEvent {
                fault_id: observer.fault_id,
                tenant_id: observer.tenant_id,
                incarnation: observer.incarnation,
                origin: ExecutionStartOrigin::GuestFirstActionObserver,
            }),
        )
    }
}

impl ReloadableState for CandidateState {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn deserialize_state(_bytes: &[u8]) -> Result<Self> {
        bail!("standalone state deserialization is not a valid candidate process")
    }
}

impl ResourceLimiter for CandidateState {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= self.config.max_memory)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= 1_024)
    }
}

impl ProcessState for CandidateState {
    type Config = CandidateConfig;

    fn new_state(
        &self,
        _module: Arc<WasmtimeCompiledModule<Self>>,
        _config: Arc<Self::Config>,
    ) -> Result<Self> {
        bail!("guest child spawning is denied by the production candidate policy")
    }

    fn register(linker: &mut Linker<Self>) -> Result<()> {
        linker.func_wrap("comparison", "tenant_id", |caller: Caller<'_, Self>| {
            caller.data().identity.tenant_id.0 as i32
        })?;
        linker.func_wrap(
            "comparison",
            "restore_counter",
            |caller: Caller<'_, Self>| caller.data().restore_counter,
        )?;
        linker.func_wrap(
            "comparison",
            "command_handle",
            |caller: Caller<'_, Self>| caller.data().command_handle,
        )?;
        linker.func_wrap(
            "comparison",
            "activation_started",
            |mut caller: Caller<'_, Self>,
             tenant: i32,
             version: i32,
             build: i64|
             -> wasmtime::Result<()> {
                caller
                    .data_mut()
                    .activation_started(tenant, version, build)
                    .map_err(|error| wasmtime::Error::msg(error.to_string()))
            },
        )?;
        linker.func_wrap(
            "comparison",
            "emit_result",
            |mut caller: Caller<'_, Self>,
             handle: i64,
             counter: i64,
             kind: i32,
             version: i32,
             build: i64|
             -> wasmtime::Result<()> {
                caller
                    .data_mut()
                    .emit_result(GuestResult {
                        handle,
                        counter,
                        kind,
                        version,
                        build,
                    })
                    .map_err(|error| wasmtime::Error::msg(error.to_string()))
            },
        )?;
        linker.func_wrap(
            "comparison",
            "execution_started",
            |mut caller: Caller<'_, Self>, handle: i64| -> wasmtime::Result<()> {
                caller
                    .data_mut()
                    .execution_started(handle)
                    .map_err(|error| wasmtime::Error::msg(error.to_string()))
            },
        )?;
        linker.func_wrap(
            "comparison",
            "bind_observer",
            |_caller: Caller<'_, Self>, _observer: i64| {},
        )?;
        linker.func_wrap("comparison", "next_command", |_caller: Caller<'_, Self>| {
            0i32
        })?;
        Ok(())
    }

    fn initialize(&mut self) {
        self.initialized = true;
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }

    fn runtime(&self) -> &WasmtimeRuntime {
        &self.runtime
    }

    fn module(&self) -> &Arc<WasmtimeCompiledModule<Self>> {
        &self.module
    }

    fn config(&self) -> &Arc<Self::Config> {
        &self.config
    }

    fn id(&self) -> u64 {
        u64::from(self.identity.tenant_id.0)
    }

    fn signal_mailbox(&self) -> &(SignalSender, SignalReceiver) {
        &self.mailboxes.0
    }

    fn message_mailbox(&self) -> &MessageMailbox {
        &self.mailboxes.1
    }

    fn config_resources(&self) -> &ConfigResources<Self::Config> {
        &self.config_resources
    }

    fn config_resources_mut(&mut self) -> &mut ConfigResources<Self::Config> {
        &mut self.config_resources
    }

    fn registry(&self) -> &Arc<RwLock<HashMap<String, (u64, u64)>>> {
        &self.registry
    }
}
