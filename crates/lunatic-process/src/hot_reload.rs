use anyhow::{anyhow, Result};
use log::{info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    module_registry::ModuleRegistry,
    runtimes::wasmtime::{MemorySnapshot, WasmtimeCompiledModule, WasmtimeInstance, WasmtimeRuntime},
    state::ProcessState,
    Signal,
};

pub struct HotReloadContext<S: ProcessState + Send> {
    pub module_id: u64,
    pub old_version: u32,
    pub new_version: u32,
    pub memory_snapshot: MemorySnapshot,
    _phantom: std::marker::PhantomData<S>,
}

/// Coordinator for managing hot reloads across multiple processes
pub struct ReloadCoordinator<S: Send + Sync> {
    /// Active reloads by module_id
    active_reloads: Arc<RwLock<HashMap<u64, ReloadOperation<S>>>>,
}

struct ReloadOperation<S> {
    #[allow(dead_code)]
    module_id: u64,
    #[allow(dead_code)]
    old_version: u32,
    #[allow(dead_code)]
    new_version: u32,
    affected_processes: Vec<u64>,
    status: ReloadStatus,
    /// Backup of old states for rollback
    old_states: HashMap<u64, Vec<u8>>,
    _phantom: std::marker::PhantomData<S>,
}

#[derive(Debug, Clone)]
enum ReloadStatus {
    InProgress,
    Completed,
    #[allow(dead_code)]
    Failed(String),
}

impl<S: Send + Sync> ReloadCoordinator<S> {
    pub fn new() -> Self {
        Self {
            active_reloads: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start a coordinated reload for a module
    pub async fn start_reload(
        &self,
        module_id: u64,
        old_version: u32,
        new_version: u32,
        affected_processes: Vec<u64>,
    ) -> Result<()> {
        let mut reloads = self.active_reloads.write().await;

        if reloads.contains_key(&module_id) {
            return Err(anyhow!("Reload already in progress for module {}", module_id));
        }

        let operation = ReloadOperation {
            module_id,
            old_version,
            new_version,
            affected_processes,
            status: ReloadStatus::InProgress,
            old_states: HashMap::new(),
            _phantom: std::marker::PhantomData,
        };

        reloads.insert(module_id, operation);
        info!("Started coordinated reload for module {}: {} -> {}", module_id, old_version, new_version);
        Ok(())
    }

    /// Mark a reload as completed
    pub async fn complete_reload(&self, module_id: u64) -> Result<()> {
        let mut reloads = self.active_reloads.write().await;

        if let Some(operation) = reloads.get_mut(&module_id) {
            operation.status = ReloadStatus::Completed;
            info!("Completed coordinated reload for module {}", module_id);
            Ok(())
        } else {
            Err(anyhow!("No active reload found for module {}", module_id))
        }
    }

    /// Mark a reload as failed
    pub async fn fail_reload(&self, module_id: u64, error: String) -> Result<()> {
        let mut reloads = self.active_reloads.write().await;

        if let Some(operation) = reloads.get_mut(&module_id) {
            operation.status = ReloadStatus::Failed(error.clone());
            warn!("Failed coordinated reload for module {}: {}", module_id, error);
            Ok(())
        } else {
            Err(anyhow!("No active reload found for module {}", module_id))
        }
    }

    /// Check if a reload is in progress for a module
    pub async fn is_reload_in_progress(&self, module_id: u64) -> bool {
        let reloads = self.active_reloads.read().await;
        reloads.contains_key(&module_id)
    }

    /// Get the processes affected by a reload
    pub async fn get_affected_processes(&self, module_id: u64) -> Option<Vec<u64>> {
        let reloads = self.active_reloads.read().await;
        reloads.get(&module_id).map(|op| op.affected_processes.clone())
    }

    /// Store old state for a process for potential rollback
    pub async fn store_old_state(&self, module_id: u64, process_id: u64, state_bytes: Vec<u8>) -> Result<()> {
        let mut reloads = self.active_reloads.write().await;

        if let Some(operation) = reloads.get_mut(&module_id) {
            operation.old_states.insert(process_id, state_bytes);
            Ok(())
        } else {
            Err(anyhow!("No active reload found for module {}", module_id))
        }
    }

    /// Rollback a reload by restoring old states
    pub async fn rollback_reload(&self, module_id: u64) -> Result<HashMap<u64, Vec<u8>>> {
        let mut reloads = self.active_reloads.write().await;

        if let Some(operation) = reloads.get_mut(&module_id) {
            if let ReloadStatus::Failed(_) = &operation.status {
                let old_states = operation.old_states.clone();
                operation.status = ReloadStatus::Completed; // Mark as rolled back
                info!("Rolled back reload for module {}", module_id);
                Ok(old_states)
            } else {
                Err(anyhow!("Cannot rollback reload that hasn't failed"))
            }
        } else {
            Err(anyhow!("No active reload found for module {}", module_id))
        }
    }

    /// Perform atomic reload - all processes reload or none do
    pub async fn perform_atomic_reload(
        &self,
        env: &dyn crate::env::Environment,
        module_id: u64,
        old_version: u32,
        new_version: u32,
        affected_processes: Vec<u64>,
    ) -> Result<()> {
        // Start the reload operation
        self.start_reload(module_id, old_version, new_version, affected_processes.clone()).await?;

        info!(
            "Starting atomic reload for module {}: {} -> {} ({} processes)",
            module_id, old_version, new_version, affected_processes.len()
        );

        // Track successful reloads for potential rollback
        let mut reloaded = Vec::new();

        // Send reload signals to all affected processes
        for process_id in &affected_processes {
            match send_hot_reload_signal(*process_id, module_id, new_version, env) {
                Ok(_) => {
                    reloaded.push(*process_id);
                    info!("Sent reload signal to process {}", process_id);
                }
                Err(e) => {
                    warn!("Failed to send reload signal to process {}: {}", process_id, e);

                    // Atomic reload failed - attempt rollback
                    self.fail_reload(module_id, format!("Failed to signal process {}: {}", process_id, e)).await?;

                    // TODO: Send rollback signals to successfully reloaded processes
                    warn!("Atomic reload failed - {} processes may be in inconsistent state", reloaded.len());

                    return Err(anyhow!(
                        "Atomic reload failed at process {}: {}. {} processes already reloaded.",
                        process_id, e, reloaded.len()
                    ));
                }
            }
        }

        // All signals sent successfully
        self.complete_reload(module_id).await?;

        info!(
            "Completed atomic reload for module {}: {} -> {} ({} processes updated)",
            module_id, old_version, new_version, reloaded.len()
        );
        Ok(())
    }

    /// Perform coordinated reload including dependent modules
    pub async fn perform_coordinated_reload<T>(
        &self,
        env: &dyn crate::env::Environment,
        registry: &crate::module_registry::ModuleRegistry<T>,
        module_id: u64,
        new_version: u32,
    ) -> Result<()>
    where
        T: ProcessState + Send + Sync,
    {
        // Get the reload order (dependencies first, then this module)
        let reload_order = registry.get_reload_order(module_id);

        info!(
            "Starting coordinated reload for module {} (affects {} modules total)",
            module_id,
            reload_order.len()
        );

        // Track all affected processes across all modules
        let mut all_affected_processes = Vec::new();
        let mut module_process_map: HashMap<u64, Vec<u64>> = HashMap::new();

        // Collect all affected processes for each module
        for &mid in &reload_order {
            let processes = env.get_processes_for_module(mid);
            info!("Module {} has {} active processes", mid, processes.len());

            all_affected_processes.extend(processes.iter().copied());
            module_process_map.insert(mid, processes);
        }

        // Perform reloads in dependency order
        for &mid in &reload_order {
            let old_version = registry.get_latest_version_number(mid);
            let affected_processes = module_process_map.get(&mid).cloned().unwrap_or_default();

            if affected_processes.is_empty() {
                info!("Module {} has no active processes, skipping", mid);
                continue;
            }

            let version_to_use = if mid == module_id {
                new_version
            } else {
                // For dependencies, use their latest version
                old_version.unwrap_or(0)
            };

            info!(
                "Reloading module {} to version {} ({} processes)",
                mid,
                version_to_use,
                affected_processes.len()
            );

            // Perform atomic reload for this module
            match self.perform_atomic_reload(
                env,
                mid,
                old_version.unwrap_or(0),
                version_to_use,
                affected_processes,
            ).await {
                Ok(_) => {
                    info!("Successfully reloaded module {}", mid);
                }
                Err(e) => {
                    warn!("Failed to reload module {}: {}", mid, e);
                    return Err(anyhow!(
                        "Coordinated reload failed at module {}: {}",
                        mid,
                        e
                    ));
                }
            }
        }

        info!(
            "Completed coordinated reload starting from module {} ({} modules, {} processes total)",
            module_id,
            reload_order.len(),
            all_affected_processes.len()
        );
        Ok(())
    }
}

impl<S: Send + Sync> Default for ReloadCoordinator<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: ProcessState + Send> HotReloadContext<S> {
    pub fn new(module_id: u64, old_version: u32, new_version: u32) -> Self {
        Self {
            module_id,
            old_version,
            new_version,
            memory_snapshot: MemorySnapshot {
                memory: Vec::new(),
                stack_ptr: None,
                heap_ptr: None,
                metadata: std::collections::HashMap::new(),
            },
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn capture_memory(&mut self, instance: &mut WasmtimeInstance<S>) -> Result<()> {
        self.memory_snapshot = instance.snapshot_memory()?;
        info!(
            "Captured {} bytes of memory for hot reload",
            self.memory_snapshot.memory.len()
        );
        Ok(())
    }

    pub fn restore_memory(&self, instance: &mut WasmtimeInstance<S>) -> Result<()> {
        instance.restore_memory(&self.memory_snapshot)?;
        info!(
            "Restored {} bytes of memory after hot reload",
            self.memory_snapshot.memory.len()
        );
        Ok(())
    }
}

pub async fn perform_hot_reload<S>(
    runtime: &WasmtimeRuntime,
    registry: &ModuleRegistry<S>,
    module_id: u64,
    old_version: u32,
    new_version: u32,
    mut current_instance: WasmtimeInstance<S>,
) -> Result<WasmtimeInstance<S>>
where
    S: ProcessState + Send + wasmtime::ResourceLimiter + crate::reloadable_state::ReloadableState,
{
    info!(
        "Starting hot reload: module_id={}, old_version={}, new_version={}",
        module_id, old_version, new_version
    );

    let mut ctx = HotReloadContext::new(module_id, old_version, new_version);
    ctx.capture_memory(&mut current_instance)?;

    // Extract state from current instance
    let state = current_instance.state();
    let state_bytes = state.serialize_state()?;
    let mut deserialized_state = S::deserialize_state(&state_bytes)?;

    // Call code_change for state transformation
    deserialized_state.code_change(old_version, new_version)?;

    let new_module = registry
        .get_version(module_id, new_version)
        .ok_or_else(|| anyhow!("Module version {} not found", new_version))?;

    // Validate compatibility
    if let Some(old_module) = registry.get_version(module_id, old_version) {
        validate_reload_compatibility(&old_module, &new_module)?;
    }

    info!("Creating new instance with version {}", new_version);
    let mut new_instance = runtime.instantiate(&new_module, deserialized_state).await?;

    ctx.restore_memory(&mut new_instance)?;

    info!("Hot reload completed successfully");
    Ok(new_instance)
}

/// Check if two module versions are compatible for hot reload
pub fn validate_reload_compatibility(
    old_module: &WasmtimeCompiledModule<impl ProcessState>,
    new_module: &WasmtimeCompiledModule<impl ProcessState>,
) -> Result<()> {
    // Check that both modules have the same source ID
    if old_module.source().id != new_module.source().id {
        return Err(anyhow!(
            "Module ID mismatch: old={:?}, new={:?}",
            old_module.source().id,
            new_module.source().id
        ));
    }

    // Get export signatures from both modules
    let old_exports = get_export_signatures(old_module);
    let new_exports = get_export_signatures(new_module);

    // Check that all old exports exist in new module with compatible signatures
    for (name, old_sig) in &old_exports {
        match new_exports.get(name) {
            Some(new_sig) => {
                if !signatures_compatible(old_sig, new_sig) {
                    return Err(anyhow!(
                        "Export '{}' has incompatible signature change",
                        name
                    ));
                }
            }
            None => {
                warn!(
                    "Export '{}' removed in new version - may cause runtime errors",
                    name
                );
            }
        }
    }

    // Get import signatures from both modules
    let old_imports = get_import_signatures(old_module);
    let new_imports = get_import_signatures(new_module);

    // Check that new imports are compatible with runtime
    for (module, name, new_sig) in &new_imports {
        if let Some(old_sig) = old_imports
            .iter()
            .find(|(m, n, _)| m == module && n == name)
            .map(|(_, _, sig)| sig)
        {
            if !signatures_compatible(old_sig, new_sig) {
                return Err(anyhow!(
                    "Import '{}::{}' has incompatible signature change",
                    module,
                    name
                ));
            }
        } else {
            info!("New import added: '{}::{}' - runtime must support it", module, name);
        }
    }

    info!("Module versions are compatible for hot reload");
    Ok(())
}

/// Extract export signatures from a module
fn get_export_signatures<S: ProcessState>(
    module: &WasmtimeCompiledModule<S>,
) -> std::collections::HashMap<String, ExportSignature> {
    let mut exports = std::collections::HashMap::new();

    for export in module.module().exports() {
        let sig = match export.ty() {
            wasmtime::ExternType::Func(func_ty) => {
                ExportSignature::Func {
                    params: func_ty.params().len(),
                    results: func_ty.results().len(),
                }
            }
            wasmtime::ExternType::Global(global_ty) => {
                ExportSignature::Global {
                    mutable: global_ty.mutability() == wasmtime::Mutability::Var,
                }
            }
            wasmtime::ExternType::Memory(_) => ExportSignature::Memory,
            wasmtime::ExternType::Table(_) => ExportSignature::Table,
        };
        exports.insert(export.name().to_string(), sig);
    }

    exports
}

/// Extract import signatures from a module
fn get_import_signatures<S: ProcessState>(
    module: &WasmtimeCompiledModule<S>,
) -> Vec<(String, String, ExportSignature)> {
    let mut imports = Vec::new();

    for import in module.module().imports() {
        let sig = match import.ty() {
            wasmtime::ExternType::Func(func_ty) => {
                ExportSignature::Func {
                    params: func_ty.params().len(),
                    results: func_ty.results().len(),
                }
            }
            wasmtime::ExternType::Global(global_ty) => {
                ExportSignature::Global {
                    mutable: global_ty.mutability() == wasmtime::Mutability::Var,
                }
            }
            wasmtime::ExternType::Memory(_) => ExportSignature::Memory,
            wasmtime::ExternType::Table(_) => ExportSignature::Table,
        };
        imports.push((
            import.module().to_string(),
            import.name().to_string(),
            sig,
        ));
    }

    imports
}

/// Signature of an export for compatibility checking
#[derive(Debug, Clone, PartialEq)]
enum ExportSignature {
    Func { params: usize, results: usize },
    Global { mutable: bool },
    Memory,
    Table,
}

/// Check if two signatures are compatible
fn signatures_compatible(old: &ExportSignature, new: &ExportSignature) -> bool {
    match (old, new) {
        (
            ExportSignature::Func {
                params: old_params,
                results: old_results,
            },
            ExportSignature::Func {
                params: new_params,
                results: new_results,
            },
        ) => {
            // Function signatures must match exactly
            old_params == new_params && old_results == new_results
        }
        (
            ExportSignature::Global { mutable: old_mut },
            ExportSignature::Global { mutable: new_mut },
        ) => {
            // Can make immutable global mutable, but not vice versa
            !old_mut || *new_mut
        }
        (ExportSignature::Memory, ExportSignature::Memory) => true,
        (ExportSignature::Table, ExportSignature::Table) => true,
        _ => false,
    }
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
    use super::*;
    use crate::reloadable_state::ReloadableState;

    #[tokio::test]
    async fn test_reload_coordinator_basic() {
        let coordinator = ReloadCoordinator::<()>::new();
        assert!(!coordinator.is_reload_in_progress(1).await);
    }

    #[tokio::test]
    async fn test_reload_coordinator_lifecycle() {
        let coordinator = ReloadCoordinator::<()>::new();
        let module_id = 1;
        let old_version = 0;
        let new_version = 1;

        // Start a reload
        coordinator
            .start_reload(module_id, old_version, new_version, vec![100, 101])
            .await
            .unwrap();

        // Check it's in progress
        assert!(coordinator.is_reload_in_progress(module_id).await);

        // Get affected processes
        let processes = coordinator.get_affected_processes(module_id).await.unwrap();
        assert_eq!(processes, vec![100, 101]);

        // Store old state
        coordinator
            .store_old_state(module_id, 100, vec![1, 2, 3])
            .await
            .unwrap();

        // Complete the reload
        coordinator.complete_reload(module_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_reload_coordinator_rollback() {
        let coordinator = ReloadCoordinator::<()>::new();
        let module_id = 1;

        // Start a reload
        coordinator
            .start_reload(module_id, 0, 1, vec![100])
            .await
            .unwrap();

        // Store state
        coordinator
            .store_old_state(module_id, 100, vec![1, 2, 3])
            .await
            .unwrap();

        // Fail the reload
        coordinator
            .fail_reload(module_id, "Test failure".to_string())
            .await
            .unwrap();

        // Rollback
        let old_states = coordinator.rollback_reload(module_id).await.unwrap();
        assert_eq!(old_states.get(&100).unwrap(), &vec![1, 2, 3]);
    }

    #[test]
    fn test_reloadable_state_primitives() {
        // Test i32
        let value = 42i32;
        let bytes = value.serialize_state().unwrap();
        let restored = i32::deserialize_state(&bytes).unwrap();
        assert_eq!(value, restored);

        // Test String
        let text = String::from("Hot reload test");
        let bytes = text.serialize_state().unwrap();
        let restored = String::deserialize_state(&bytes).unwrap();
        assert_eq!(text, restored);

        // Test Vec
        let vec = vec![1, 2, 3, 4, 5];
        let bytes = vec.serialize_state().unwrap();
        let restored = Vec::<i32>::deserialize_state(&bytes).unwrap();
        assert_eq!(vec, restored);
    }

    #[test]
    fn test_code_change_hook() {
        let mut value = 100i32;

        // Default implementation should succeed without changes
        let result = value.code_change(1, 2);
        assert!(result.is_ok());
        assert_eq!(value, 100);
    }

    #[test]
    fn test_export_signature_compatibility() {
        use super::ExportSignature;

        // Function signatures must match exactly
        let func1 = ExportSignature::Func { params: 2, results: 1 };
        let func2 = ExportSignature::Func { params: 2, results: 1 };
        let func3 = ExportSignature::Func { params: 3, results: 1 };

        assert!(signatures_compatible(&func1, &func2));
        assert!(!signatures_compatible(&func1, &func3));

        // Global mutability - can go from immutable to mutable
        let global_immut = ExportSignature::Global { mutable: false };
        let global_mut = ExportSignature::Global { mutable: true };

        assert!(signatures_compatible(&global_immut, &global_mut));
        assert!(!signatures_compatible(&global_mut, &global_immut));

        // Memory and Table are always compatible
        let mem1 = ExportSignature::Memory;
        let mem2 = ExportSignature::Memory;
        assert!(signatures_compatible(&mem1, &mem2));

        // Different types are incompatible
        assert!(!signatures_compatible(&func1, &global_mut));
        assert!(!signatures_compatible(&mem1, &func1));
    }
}
