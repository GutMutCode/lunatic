pub mod watcher;

use std::sync::Arc;

use anyhow::Result;
use lunatic_process::{
    env::Environment, module_registry::ModuleRegistry, runtimes::wasmtime::WasmtimeRuntime,
    state::ProcessState, Signal,
};

pub use watcher::{FileChangeEvent, FileWatcher};

/// Compile, register and broadcast a module update through the same boundary
/// used by `lunatic run --watch`.
///
/// Completion acknowledgement is intentionally not claimed here: the signal is
/// queued for each process and the process-local execution driver decides
/// whether the transaction commits or rolls back.
pub fn register_module_update<S>(
    runtime: &WasmtimeRuntime,
    registry: &ModuleRegistry<S>,
    env: &Arc<dyn Environment>,
    module_id: u64,
    bytes: Vec<u8>,
) -> Result<u32>
where
    S: ProcessState + Send + 'static,
{
    let module = runtime.compile_module(bytes.into())?;
    let version = registry.add_version(module_id, module);
    env.send_to_all(Signal::HotReload {
        module_id,
        new_version: version,
    });
    Ok(version)
}
