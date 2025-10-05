use std::{collections::HashMap, sync::Arc};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use wasmtime::ResourceLimiter;

use crate::{
    config::{ProcessConfig, UNIT_OF_COMPUTE_IN_INSTRUCTIONS},
    state::ProcessState,
    ExecutionResult, ResultValue,
};

use super::RawWasm;

/// Global flag to control the epoch ticker
static EPOCH_TICKER_STARTED: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
pub struct WasmtimeRuntime {
    engine: wasmtime::Engine,
}

impl WasmtimeRuntime {
    pub fn new(config: &wasmtime::Config) -> Result<Self> {
        let engine = wasmtime::Engine::new(config)?;
        
        // Start global epoch ticker once
        if !EPOCH_TICKER_STARTED.swap(true, Ordering::SeqCst) {
            Self::start_global_epoch_ticker(engine.clone());
        }
        
        Ok(Self { engine })
    }

    pub fn engine_handle(&self) -> wasmtime::Engine {
        self.engine.clone()
    }

    /// Starts a single global epoch ticker for all processes in the runtime.
    /// This replaces the per-process epoch ticker approach to reduce overhead.
    fn start_global_epoch_ticker(engine: wasmtime::Engine) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
            log::debug!("Global epoch ticker started (10ms interval)");
            
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
        T: ProcessState,
    {
        let module = wasmtime::Module::new(&self.engine, data.as_slice())?;
        let mut linker = wasmtime::Linker::new(&self.engine);
        // Register host functions to linker.
        <T as ProcessState>::register(&mut linker)?;
        let instance_pre = linker.instantiate_pre(&module)?;
        let compiled_module = WasmtimeCompiledModule::new(data, module, instance_pre);
        Ok(compiled_module)
    }

    pub async fn instantiate<T>(
        &self,
        compiled_module: &WasmtimeCompiledModule<T>,
        state: T,
    ) -> Result<WasmtimeInstance<T>>
    where
        T: ProcessState + Send + ResourceLimiter,
    {
        let max_fuel = state.config().get_max_fuel();
        let mut store = wasmtime::Store::new(&self.engine, state);
        // Set limits of the store
        store.limiter(|state| state);
        // Trap if out of fuel
        store.out_of_fuel_trap();
        // Define maximum fuel
        match max_fuel {
            Some(max_fuel) => {
                store.out_of_fuel_async_yield(max_fuel, UNIT_OF_COMPUTE_IN_INSTRUCTIONS)
            }
            // If no limit is specified use maximum
            None => store.out_of_fuel_async_yield(u64::MAX, UNIT_OF_COMPUTE_IN_INSTRUCTIONS),
        };
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
    T: Send,
{
    store: wasmtime::Store<T>,
    instance: wasmtime::Instance,
}

impl<T> WasmtimeInstance<T>
where
    T: Send,
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

        ExecutionResult {
            state: self.store.into_data(),
            result: match result {
                Ok(()) => ResultValue::Ok,
                Err(err) => {
                    // If the trap is a result of calling `proc_exit(0)`, treat it as an no-error finish.
                    match err.downcast_ref::<wasmtime_wasi::I32Exit>() {
                        Some(wasmtime_wasi::I32Exit(0)) => ResultValue::Ok,
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
        
        let stack_ptr = self.instance
            .get_global(&mut self.store, "__stack_pointer")
            .and_then(|g| g.get(&mut self.store).i32())
            .map(|v| v as u32);
        
        let heap_ptr = self.instance
            .get_global(&mut self.store, "__heap_base")
            .and_then(|g| g.get(&mut self.store).i32())
            .map(|v| v as u32);
        
        Ok(MemorySnapshot {
            memory: memory_data,
            stack_ptr,
            heap_ptr,
            metadata: HashMap::new(),
        })
    }

    pub fn restore_memory(&mut self, snapshot: &MemorySnapshot) -> Result<()> {
        let memory = self
            .instance
            .get_memory(&mut self.store, "memory")
            .ok_or_else(|| anyhow::anyhow!("No memory export found"))?;
        
        let data = memory.data_mut(&mut self.store);
        let copy_len = snapshot.memory.len().min(data.len());
        data[..copy_len].copy_from_slice(&snapshot.memory[..copy_len]);
        
        if let Some(stack_ptr) = snapshot.stack_ptr {
            if let Some(global) = self.instance.get_global(&mut self.store, "__stack_pointer") {
                global.set(&mut self.store, wasmtime::Val::I32(stack_ptr as i32))?;
            }
        }
        
        if let Some(heap_ptr) = snapshot.heap_ptr {
            if let Some(global) = self.instance.get_global(&mut self.store, "__heap_base") {
                global.set(&mut self.store, wasmtime::Val::I32(heap_ptr as i32))?;
            }
        }
        
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
    config
        .async_support(true)
        .debug_info(false)
        // The behavior of fuel running out is defined on the Store
        .consume_fuel(true)
        // Enable epoch interruption for preemptive hot reload
        .epoch_interruption(true)
        .wasm_reference_types(true)
        .wasm_bulk_memory(true)
        .wasm_multi_value(true)
        .wasm_multi_memory(true)
        .cranelift_opt_level(wasmtime::OptLevel::SpeedAndSize)
        // Allocate resources on demand because we can't predict how many process will exist
        .allocation_strategy(wasmtime::InstanceAllocationStrategy::OnDemand)
        // Always use static memories
        .static_memory_forced(true);
    config
}
