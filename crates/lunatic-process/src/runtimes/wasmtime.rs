use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Result};
use wasmtime::ResourceLimiter;

use crate::{
    config::{ProcessConfig, UNIT_OF_COMPUTE_IN_INSTRUCTIONS},
    state::ProcessState,
    ExecutionResult, ResultValue,
};

use super::RawWasm;

/// Default node-wide ceiling for compiled modules retained by one runtime.
pub const DEFAULT_MAX_COMPILED_MODULES: usize = 1_024;
/// Default node-wide ceiling for source bytes retained by compiled modules.
pub const DEFAULT_MAX_COMPILED_MODULE_BYTES: usize = 512 * 1024 * 1024;
/// Default maximum source size accepted for one module compilation.
pub const DEFAULT_MAX_MODULE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompiledModuleLimits {
    pub modules: usize,
    pub source_bytes: usize,
    pub single_module_bytes: usize,
}

impl Default for CompiledModuleLimits {
    fn default() -> Self {
        Self {
            modules: DEFAULT_MAX_COMPILED_MODULES,
            source_bytes: DEFAULT_MAX_COMPILED_MODULE_BYTES,
            single_module_bytes: DEFAULT_MAX_MODULE_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompiledModuleUsage {
    pub modules: usize,
    pub source_bytes: usize,
}

struct CompiledModuleBudget {
    limits: CompiledModuleLimits,
    usage: Mutex<CompiledModuleUsage>,
}

impl CompiledModuleBudget {
    fn new(limits: CompiledModuleLimits) -> Self {
        Self {
            limits,
            usage: Mutex::new(CompiledModuleUsage::default()),
        }
    }

    fn usage(&self) -> CompiledModuleUsage {
        *self
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn try_reserve(self: &Arc<Self>, source_bytes: usize) -> Result<CompiledModuleReservation> {
        if source_bytes > self.limits.single_module_bytes {
            return Err(anyhow!(
                "module source size {source_bytes} exceeds single-module limit {}",
                self.limits.single_module_bytes
            ));
        }
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let modules = usage
            .modules
            .checked_add(1)
            .ok_or_else(|| anyhow!("compiled-module count overflow"))?;
        let retained_source_bytes = usage
            .source_bytes
            .checked_add(source_bytes)
            .ok_or_else(|| anyhow!("compiled-module source-byte accounting overflow"))?;

        if modules > self.limits.modules {
            return Err(anyhow!(
                "compiled-module limit ({}) reached",
                self.limits.modules
            ));
        }
        if retained_source_bytes > self.limits.source_bytes {
            return Err(anyhow!(
                "compiled-module source-byte limit ({}) exceeded",
                self.limits.source_bytes
            ));
        }

        usage.modules = modules;
        usage.source_bytes = retained_source_bytes;
        Ok(CompiledModuleReservation {
            budget: Arc::clone(self),
            source_bytes,
        })
    }

    fn release(&self, source_bytes: usize) {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        usage.modules = usage
            .modules
            .checked_sub(1)
            .expect("compiled-module reservation count underflow");
        usage.source_bytes = usage
            .source_bytes
            .checked_sub(source_bytes)
            .expect("compiled-module reservation byte underflow");
    }
}

struct CompiledModuleReservation {
    budget: Arc<CompiledModuleBudget>,
    source_bytes: usize,
}

impl Drop for CompiledModuleReservation {
    fn drop(&mut self) {
        self.budget.release(self.source_bytes);
    }
}

#[derive(Clone)]
pub struct WasmtimeRuntime {
    engine: wasmtime::Engine,
    compiled_module_budget: Arc<CompiledModuleBudget>,
}

impl WasmtimeRuntime {
    pub fn new(config: &wasmtime::Config) -> Result<Self> {
        Self::new_with_module_limits(config, CompiledModuleLimits::default())
    }

    pub fn new_with_module_limits(
        config: &wasmtime::Config,
        limits: CompiledModuleLimits,
    ) -> Result<Self> {
        let engine = wasmtime::Engine::new(config)?;
        // Each runtime owns a distinct Engine. Every Engine therefore needs its
        // own epoch ticker; a process-wide "started" flag leaves all later
        // runtimes unable to yield CPU-bound guests for signals or reloads.
        Self::start_epoch_ticker(engine.clone());

        Ok(Self {
            engine,
            compiled_module_budget: Arc::new(CompiledModuleBudget::new(limits)),
        })
    }

    pub fn engine_handle(&self) -> wasmtime::Engine {
        self.engine.clone()
    }

    pub fn engine(&self) -> &wasmtime::Engine {
        &self.engine
    }

    pub fn compiled_module_limits(&self) -> CompiledModuleLimits {
        self.compiled_module_budget.limits
    }

    pub fn compiled_module_usage(&self) -> CompiledModuleUsage {
        self.compiled_module_budget.usage()
    }

    /// Starts one epoch ticker shared by all processes using this Engine.
    fn start_epoch_ticker(engine: wasmtime::Engine) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
            log::debug!("Wasmtime engine epoch ticker started (10ms interval)");

            loop {
                interval.tick().await;
                // Increment epoch to trigger interruption points in all WASM instances
                engine.increment_epoch();
            }
        });
    }

    /// Compiles a wasm module to machine code and performs type-checking on host functions.
    pub fn compile_module<T>(&self, data: RawWasm) -> Result<WasmtimeCompiledModule<T>>
    where
        T: ProcessState + 'static,
    {
        let reservation = self
            .compiled_module_budget
            .try_reserve(data.as_slice().len())?;
        let module = wasmtime::Module::new(&self.engine, data.as_slice())?;
        let mut linker = wasmtime::Linker::new(&self.engine);
        // Register host functions to linker.
        <T as ProcessState>::register(&mut linker)?;
        let instance_pre = linker.instantiate_pre(&module)?;
        let compiled_module =
            WasmtimeCompiledModule::new_budgeted(data, module, instance_pre, reservation);
        Ok(compiled_module)
    }

    pub async fn instantiate<T>(
        &self,
        compiled_module: &WasmtimeCompiledModule<T>,
        state: T,
    ) -> Result<WasmtimeInstance<T>>
    where
        T: ProcessState + Send + ResourceLimiter + 'static,
    {
        let max_fuel = state.config().get_max_fuel();
        let mut store = wasmtime::Store::new(&self.engine, state);
        // Set limits of the store
        store.limiter(|state| state);
        // Modern Wasmtime stores start with zero fuel and trap when it is
        // exhausted. Preserve Lunatic's unit-based quota and cooperative yield
        // interval using the supported fuel APIs.
        let fuel = max_fuel
            .map(|max_fuel| max_fuel.saturating_mul(UNIT_OF_COMPUTE_IN_INSTRUCTIONS))
            .unwrap_or(u64::MAX);
        store.set_fuel(fuel)?;
        store.fuel_async_yield_interval(Some(UNIT_OF_COMPUTE_IN_INSTRUCTIONS))?;
        // Set epoch deadline for preemptive hot reload (every 1 epoch tick)
        store.set_epoch_deadline(1);

        // Set epoch interruption callback to yield for hot reload checks
        store.epoch_deadline_async_yield_and_update(1);

        // Create instance
        let instance = compiled_module
            .instantiator()
            .instantiate_async(&mut store)
            .await?;
        // Mark state as initialized
        store.data_mut().initialize();
        Ok(WasmtimeInstance { store, instance })
    }
}

#[derive(Clone)]
pub struct MemorySnapshot {
    pub memory: Vec<u8>,
    pub stack_ptr: Option<u32>,
    pub heap_ptr: Option<u32>,
    pub metadata: HashMap<String, Vec<u8>>,
}

pub struct WasmtimeCompiledModule<T> {
    inner: Arc<WasmtimeCompiledModuleInner<T>>,
}

pub struct WasmtimeCompiledModuleInner<T> {
    source: RawWasm,
    module: wasmtime::Module,
    instance_pre: wasmtime::InstancePre<T>,
    _reservation: Option<CompiledModuleReservation>,
}

impl<T> WasmtimeCompiledModule<T> {
    pub fn new(
        source: RawWasm,
        module: wasmtime::Module,
        instance_pre: wasmtime::InstancePre<T>,
    ) -> WasmtimeCompiledModule<T> {
        let inner = Arc::new(WasmtimeCompiledModuleInner {
            source,
            module,
            instance_pre,
            _reservation: None,
        });
        Self { inner }
    }

    fn new_budgeted(
        source: RawWasm,
        module: wasmtime::Module,
        instance_pre: wasmtime::InstancePre<T>,
        reservation: CompiledModuleReservation,
    ) -> WasmtimeCompiledModule<T> {
        let inner = Arc::new(WasmtimeCompiledModuleInner {
            source,
            module,
            instance_pre,
            _reservation: Some(reservation),
        });
        Self { inner }
    }

    pub fn exports(&self) -> impl ExactSizeIterator<Item = wasmtime::ExportType<'_>> {
        self.inner.module.exports()
    }

    pub fn source(&self) -> &RawWasm {
        &self.inner.source
    }

    pub fn instantiator(&self) -> &wasmtime::InstancePre<T> {
        &self.inner.instance_pre
    }

    /// Get the underlying wasmtime Module for compatibility checking
    pub fn module(&self) -> &wasmtime::Module {
        &self.inner.module
    }

    pub(super) fn has_unique_inner(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }
}

impl<T> Clone for WasmtimeCompiledModule<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct WasmtimeInstance<T>
where
    T: Send + 'static,
{
    store: wasmtime::Store<T>,
    instance: wasmtime::Instance,
}

impl<T> WasmtimeInstance<T>
where
    T: Send + 'static,
{
    /// Get the current process state from the instance
    pub fn state(&self) -> &T {
        self.store.data()
    }

    /// Get mutable access to the current process state
    pub fn state_mut(&mut self) -> &mut T {
        self.store.data_mut()
    }

    pub async fn call(mut self, function: &str, params: Vec<wasmtime::Val>) -> ExecutionResult<T> {
        let entry = self.instance.get_func(&mut self.store, function);

        if entry.is_none() {
            return ExecutionResult {
                state: self.store.into_data(),
                result: ResultValue::SpawnError(format!("Function '{function}' not found")),
            };
        }

        let result = entry
            .unwrap()
            .call_async(&mut self.store, &params, &mut [])
            .await;

        self.into_execution_result(result)
    }

    pub(crate) fn into_execution_result<E>(
        self,
        result: std::result::Result<(), E>,
    ) -> ExecutionResult<T>
    where
        E: Into<anyhow::Error>,
    {
        ExecutionResult {
            state: self.store.into_data(),
            result: match result {
                Ok(()) => ResultValue::Ok,
                Err(err) => {
                    let err = err.into();
                    // If the trap is a result of calling `proc_exit(0)`, treat it as an no-error finish.
                    match err.downcast_ref::<wasi_common::I32Exit>() {
                        Some(wasi_common::I32Exit(0)) => ResultValue::Ok,
                        _ => ResultValue::Failed(err.to_string()),
                    }
                }
            },
        }
    }

    /// Call a function without consuming the instance (for hot reload compatibility)
    pub async fn call_ref(&mut self, function: &str, params: Vec<wasmtime::Val>) -> Result<()> {
        let entry = self.instance.get_func(&mut self.store, function);

        if let Some(func) = entry {
            func.call_async(&mut self.store, &params, &mut []).await?;
        } else {
            return Err(anyhow::anyhow!("Function '{function}' not found"));
        }

        Ok(())
    }

    pub fn snapshot_memory(&mut self) -> Result<MemorySnapshot> {
        let memory = self
            .instance
            .get_memory(&mut self.store, "memory")
            .ok_or_else(|| anyhow::anyhow!("No memory export found"))?;

        let memory_data = memory.data(&self.store).to_vec();

        Ok(MemorySnapshot {
            memory: memory_data,
            // A cancelled Wasmtime fiber cannot resume its instruction pointer
            // or native control stack. Restoring an interrupted stack pointer
            // before re-entering the export would instead accumulate abandoned
            // frames. `__heap_base` is a layout constant, not runtime state.
            stack_ptr: None,
            heap_ptr: None,
            metadata: HashMap::new(),
        })
    }

    pub fn restore_memory(&mut self, snapshot: &MemorySnapshot) -> Result<()> {
        let memory = self
            .instance
            .get_memory(&mut self.store, "memory")
            .ok_or_else(|| anyhow::anyhow!("No memory export found"))?;

        let current_len = memory.data_size(&self.store);
        if current_len < snapshot.memory.len() {
            let missing = snapshot.memory.len() - current_len;
            const WASM_PAGE_SIZE: usize = 64 * 1024;
            let pages = missing.div_ceil(WASM_PAGE_SIZE) as u64;
            memory.grow(&mut self.store, pages)?;
        }

        let data = memory.data_mut(&mut self.store);
        anyhow::ensure!(
            data.len() >= snapshot.memory.len(),
            "replacement memory is smaller than the captured process memory"
        );
        data[..snapshot.memory.len()].copy_from_slice(&snapshot.memory);

        Ok(())
    }

    pub fn store(&self) -> &wasmtime::Store<T> {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut wasmtime::Store<T> {
        &mut self.store
    }

    pub fn instance(&self) -> &wasmtime::Instance {
        &self.instance
    }
}

pub fn default_config() -> wasmtime::Config {
    let mut config = wasmtime::Config::new();
    // Preserve the WebAssembly feature surface that Wasmtime 8 exposed by
    // default instead of implicitly widening guest capabilities on upgrade.
    config
        .wasm_threads(false)
        .wasm_relaxed_simd(false)
        .wasm_memory64(false)
        .wasm_extended_const(false)
        .wasm_tail_call(false)
        .wasm_component_model(false);
    config
        .debug_info(false)
        .consume_fuel(true)
        .epoch_interruption(true)
        .wasm_reference_types(true)
        .wasm_bulk_memory(true)
        .wasm_multi_value(true)
        .wasm_multi_memory(true)
        .wasm_simd(true)
        .cranelift_opt_level(wasmtime::OptLevel::SpeedAndSize)
        .allocation_strategy(wasmtime::InstanceAllocationStrategy::pooling())
        .memory_may_move(false);
    config
}

#[cfg(test)]
mod tests {
    use super::{CompiledModuleBudget, CompiledModuleLimits, CompiledModuleUsage};
    use std::sync::Arc;

    #[test]
    fn compiled_module_budget_is_atomic_and_released_by_raii() {
        let budget = Arc::new(CompiledModuleBudget::new(CompiledModuleLimits {
            modules: 1,
            source_bytes: 4,
            single_module_bytes: 4,
        }));
        let reservation = budget.try_reserve(4).unwrap();
        assert_eq!(
            budget.usage(),
            CompiledModuleUsage {
                modules: 1,
                source_bytes: 4,
            }
        );

        assert!(budget.try_reserve(1).is_err());
        assert_eq!(
            budget.usage(),
            CompiledModuleUsage {
                modules: 1,
                source_bytes: 4,
            }
        );

        drop(reservation);
        assert_eq!(budget.usage(), CompiledModuleUsage::default());
        let reservation = budget.try_reserve(3).unwrap();
        assert_eq!(budget.usage().source_bytes, 3);
        drop(reservation);
        assert_eq!(budget.usage(), CompiledModuleUsage::default());
    }

    #[test]
    fn compiled_module_budget_rejects_oversized_source_without_mutation() {
        let budget = Arc::new(CompiledModuleBudget::new(CompiledModuleLimits {
            modules: 2,
            source_bytes: 3,
            single_module_bytes: 3,
        }));
        assert!(budget.try_reserve(4).is_err());
        assert_eq!(budget.usage(), CompiledModuleUsage::default());
    }
}
