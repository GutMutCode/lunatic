use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use embedded_v3_core_guests::{artifact_bytes, spec, Variant};
use lunatic_process::config::ProcessConfig;
use lunatic_process::mailbox::MessageMailbox;
use lunatic_process::reloadable_state::ReloadableState;
use lunatic_process::runtimes::wasmtime::{WasmtimeCompiledModule, WasmtimeRuntime};
use lunatic_process::runtimes::RawWasm;
use lunatic_process::state::{ConfigResources, ProcessState, SignalReceiver, SignalSender};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use wasmtime::{Caller, Linker, ResourceLimiter, Val};

#[derive(Clone, Default, Deserialize, Serialize)]
struct TestConfig {
    max_fuel: Option<u64>,
    max_memory: usize,
}

impl ProcessConfig for TestConfig {
    fn set_max_fuel(&mut self, max_fuel: Option<u64>) {
        self.max_fuel = max_fuel;
    }

    fn get_max_fuel(&self) -> Option<u64> {
        self.max_fuel
    }

    fn set_max_memory(&mut self, max_memory: usize) {
        self.max_memory = max_memory;
    }

    fn get_max_memory(&self) -> usize {
        self.max_memory
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResultEvent {
    handle: i64,
    counter: i64,
    kind: i32,
    version: i32,
    build: i64,
}

struct LunaticState {
    config: Arc<TestConfig>,
    tenant: i32,
    restore: i64,
    handle: i64,
    initialized: bool,
    activations: Vec<(i32, i32, i64)>,
    results: Vec<ResultEvent>,
    execution_started: Vec<i64>,
    observer: Option<i64>,
    commands: VecDeque<(i32, i64)>,
}

impl LunaticState {
    fn new(tenant: i32, restore: i64, max_fuel: Option<u64>) -> Self {
        Self {
            config: Arc::new(TestConfig {
                max_fuel,
                max_memory: 65_536,
            }),
            tenant,
            restore,
            handle: 0,
            initialized: false,
            activations: Vec::new(),
            results: Vec::new(),
            execution_started: Vec::new(),
            observer: None,
            commands: VecDeque::new(),
        }
    }
}

impl ReloadableState for LunaticState {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn deserialize_state(_bytes: &[u8]) -> Result<Self> {
        Ok(Self::new(0, 0, None))
    }
}

impl ResourceLimiter for LunaticState {
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
        _desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(true)
    }
}

impl ProcessState for LunaticState {
    type Config = TestConfig;

    fn new_state(
        &self,
        _module: Arc<WasmtimeCompiledModule<Self>>,
        _config: Arc<Self::Config>,
    ) -> Result<Self> {
        Ok(Self::new(self.tenant, 0, self.config.max_fuel))
    }

    fn register(linker: &mut Linker<Self>) -> Result<()> {
        linker.func_wrap("comparison", "tenant_id", |caller: Caller<'_, Self>| {
            caller.data().tenant
        })?;
        linker.func_wrap(
            "comparison",
            "restore_counter",
            |caller: Caller<'_, Self>| caller.data().restore,
        )?;
        linker.func_wrap(
            "comparison",
            "command_handle",
            |caller: Caller<'_, Self>| caller.data().handle,
        )?;
        linker.func_wrap(
            "comparison",
            "activation_started",
            |mut caller: Caller<'_, Self>, tenant: i32, version: i32, build: i64| {
                caller.data_mut().activations.push((tenant, version, build));
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
             build: i64| {
                caller.data_mut().results.push(ResultEvent {
                    handle,
                    counter,
                    kind,
                    version,
                    build,
                });
            },
        )?;
        linker.func_wrap(
            "comparison",
            "execution_started",
            |mut caller: Caller<'_, Self>, handle: i64| {
                caller.data_mut().execution_started.push(handle);
            },
        )?;
        linker.func_wrap(
            "comparison",
            "bind_observer",
            |mut caller: Caller<'_, Self>, observer: i64| {
                caller.data_mut().observer = Some(observer);
            },
        )?;
        linker.func_wrap(
            "comparison",
            "next_command",
            |mut caller: Caller<'_, Self>| {
                let (opcode, handle) = caller.data_mut().commands.pop_front().unwrap_or((0, 0));
                caller.data_mut().handle = handle;
                opcode
            },
        )?;
        Ok(())
    }

    fn initialize(&mut self) {
        self.initialized = true;
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }

    fn runtime(&self) -> &WasmtimeRuntime {
        unreachable!("the ABI smoke does not spawn child processes")
    }

    fn module(&self) -> &Arc<WasmtimeCompiledModule<Self>> {
        unreachable!("the ABI smoke does not spawn child processes")
    }

    fn config(&self) -> &Arc<Self::Config> {
        &self.config
    }

    fn id(&self) -> u64 {
        self.tenant as u64
    }

    fn signal_mailbox(&self) -> &(SignalSender, SignalReceiver) {
        unreachable!("the ABI smoke calls an instance directly")
    }

    fn message_mailbox(&self) -> &MessageMailbox {
        unreachable!("the ABI smoke calls an instance directly")
    }

    fn config_resources(&self) -> &ConfigResources<Self::Config> {
        unreachable!("the ABI smoke has no config resources")
    }

    fn config_resources_mut(&mut self) -> &mut ConfigResources<Self::Config> {
        unreachable!("the ABI smoke has no config resources")
    }

    fn registry(&self) -> &Arc<RwLock<HashMap<String, (u64, u64)>>> {
        unreachable!("the ABI smoke has no registry")
    }
}

fn runtime() -> WasmtimeRuntime {
    let mut config = wasmtime::Config::new();
    config.consume_fuel(true);
    config.epoch_interruption(true);
    WasmtimeRuntime::new(&config).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_artifact_instantiates_and_calls_through_process_state_linker() {
    let runtime = runtime();
    let module = runtime
        .compile_module::<LunaticState>(RawWasm::new(None, artifact_bytes(Variant::A).unwrap()))
        .unwrap();
    let mut instance = runtime
        .instantiate(&module, LunaticState::new(5, 4, None))
        .await
        .unwrap();

    instance.call_ref("activation_check", vec![]).await.unwrap();
    assert!(instance.state().results.is_empty());
    instance.state_mut().handle = 101;
    instance.call_ref("increment", vec![]).await.unwrap();
    instance.state_mut().commands = VecDeque::from([(2, 102), (3, 103), (0, 0)]);
    instance.call_ref("run", vec![Val::I64(77)]).await.unwrap();

    assert!(instance.state().is_initialized());
    assert_eq!(instance.state().observer, Some(77));
    assert_eq!(
        instance
            .state()
            .results
            .iter()
            .map(|event| (
                event.handle,
                event.counter,
                event.kind,
                event.version,
                event.build
            ))
            .collect::<Vec<_>>(),
        [
            (101, 5, 1, 1, spec(Variant::A).build_marker),
            (102, 5, 2, 1, spec(Variant::A).build_marker),
            (103, 5, 3, 1, spec(Variant::A).build_marker),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn lunatic_fuel_bounds_cpu_loop_after_guest_marker() {
    let runtime = runtime();
    let module = runtime
        .compile_module::<LunaticState>(RawWasm::new(None, artifact_bytes(Variant::A).unwrap()))
        .unwrap();
    let mut instance = runtime
        .instantiate(&module, LunaticState::new(2, 0, Some(1)))
        .await
        .unwrap();
    instance.state_mut().handle = 404;

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        instance.call_ref("cpu_loop", vec![]),
    )
    .await
    .expect("Lunatic did not interrupt the CPU loop within two seconds");
    assert!(result.is_err());
    assert_eq!(instance.state().execution_started, [404]);
}

#[tokio::test(flavor = "multi_thread")]
async fn lunatic_linker_rejects_bad_tenant_seven_activation_only() {
    let runtime = runtime();
    let module = runtime
        .compile_module::<LunaticState>(RawWasm::new(None, artifact_bytes(Variant::BadA).unwrap()))
        .unwrap();
    assert!(runtime
        .instantiate(&module, LunaticState::new(7, 0, None))
        .await
        .is_err());
    let instance = runtime
        .instantiate(&module, LunaticState::new(6, 0, None))
        .await
        .unwrap();
    assert_eq!(instance.state().activations.len(), 1);
}
