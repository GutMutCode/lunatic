pub mod watcher;

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use lunatic_process::{
    env::Environment, hot_reload::ReloadCoordinator, module_registry::ModuleRegistry,
    runtimes::wasmtime::WasmtimeRuntime, state::ProcessState,
};

pub use watcher::{FileChangeEvent, FileWatcher};

/// Compile, register and atomically apply a module update through the same boundary
/// used by `lunatic run --watch`.
pub async fn register_module_update<S>(
    runtime: &WasmtimeRuntime,
    registry: &ModuleRegistry<S>,
    env: &Arc<dyn Environment>,
    coordinator: &ReloadCoordinator<S>,
    module_id: u64,
    bytes: Vec<u8>,
) -> Result<u32>
where
    S: ProcessState + Send + Sync + 'static,
{
    let _transaction = registry.lock_transaction(module_id).await;
    if coordinator.is_reload_in_progress(module_id).await {
        return Err(anyhow!(
            "Module {} cannot accept another reload while its previous operation is active or in doubt",
            module_id
        ));
    }
    let old_version = registry
        .get_committed_version_number(module_id)
        .ok_or_else(|| anyhow!("Module {} has no committed version", module_id))?;
    let module = runtime.compile_module(bytes.into())?;
    let version = registry.add_version(module_id, module)?;
    let affected_processes = registry.processes_for_module(module_id, env.id());

    coordinator
        .perform_atomic_reload(
            env.as_ref(),
            module_id,
            old_version,
            version,
            affected_processes.clone(),
        )
        .await
        .with_context(|| {
            format!(
                "Module {} reload {} -> {} did not commit",
                module_id, old_version, version
            )
        })?;

    if let Err(commit_error) = registry.mark_committed(module_id, version) {
        // A registry commit is synchronous and should only fail if lifecycle
        // invariants were violated. Compensate anyway so processes and the
        // committed pointer cannot silently diverge.
        let rollback_result = coordinator
            .perform_atomic_reload(
                env.as_ref(),
                module_id,
                version,
                old_version,
                affected_processes,
            )
            .await;
        return match rollback_result {
            Ok(()) => Err(commit_error).context(format!(
                "Registry commit failed; all processes were restored to version {}",
                old_version
            )),
            Err(rollback_error) => Err(anyhow!(
                "Registry commit failed ({commit_error}); compensating rollback also failed ({rollback_error})"
            )),
        };
    }

    Ok(version)
}
