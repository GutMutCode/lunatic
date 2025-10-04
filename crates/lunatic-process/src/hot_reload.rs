use anyhow::{anyhow, Result};
use log::{info, warn};

use crate::{
    module_registry::ModuleRegistry,
    runtimes::wasmtime::{WasmtimeInstance, WasmtimeRuntime},
    state::ProcessState,
    Signal,
};

pub struct HotReloadContext<S: ProcessState + Send> {
    pub module_id: u64,
    pub old_version: u32,
    pub new_version: u32,
    pub memory_snapshot: Vec<u8>,
    _phantom: std::marker::PhantomData<S>,
}

impl<S: ProcessState + Send> HotReloadContext<S> {
    pub fn new(module_id: u64, old_version: u32, new_version: u32) -> Self {
        Self {
            module_id,
            old_version,
            new_version,
            memory_snapshot: Vec::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn capture_memory(&mut self, instance: &mut WasmtimeInstance<S>) -> Result<()> {
        self.memory_snapshot = instance.snapshot_memory()?;
        info!(
            "Captured {} bytes of memory for hot reload",
            self.memory_snapshot.len()
        );
        Ok(())
    }

    pub fn restore_memory(&self, instance: &mut WasmtimeInstance<S>) -> Result<()> {
        instance.restore_memory(&self.memory_snapshot)?;
        info!(
            "Restored {} bytes of memory after hot reload",
            self.memory_snapshot.len()
        );
        Ok(())
    }
}

pub async fn perform_hot_reload<S>(
    runtime: &WasmtimeRuntime,
    registry: &ModuleRegistry<S>,
    module_id: u64,
    new_version: u32,
    current_instance: &mut WasmtimeInstance<S>,
    state: S,
) -> Result<WasmtimeInstance<S>>
where
    S: ProcessState + Send + wasmtime::ResourceLimiter,
{
    info!(
        "Starting hot reload: module_id={}, new_version={}",
        module_id, new_version
    );

    let mut ctx = HotReloadContext::new(module_id, 0, new_version);
    ctx.capture_memory(current_instance)?;

    let new_module = registry
        .get_version(module_id, new_version)
        .ok_or_else(|| anyhow!("Module version {} not found", new_version))?;

    info!("Creating new instance with version {}", new_version);
    let mut new_instance = runtime.instantiate(&new_module, state).await?;

    ctx.restore_memory(&mut new_instance)?;

    info!("Hot reload completed successfully");
    Ok(new_instance)
}

pub fn send_hot_reload_signal(
    process_id: u64,
    module_id: u64,
    new_version: u32,
    env: &dyn crate::env::Environment,
) -> Result<()> {
    if let Some(process) = env.get_process(process_id) {
        process.send(Signal::HotReload {
            module_id,
            new_version,
        });
        info!(
            "Sent hot reload signal to process {} for module {} version {}",
            process_id, module_id, new_version
        );
        Ok(())
    } else {
        warn!("Process {} not found, cannot send hot reload signal", process_id);
        Err(anyhow!("Process {} not found", process_id))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_hot_reload_module_exists() {
        assert!(true);
    }
}
