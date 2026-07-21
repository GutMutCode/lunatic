use anyhow::{anyhow, Result};
use futures_util::{stream::FuturesUnordered, StreamExt};
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, RwLock};
use tokio::time::{sleep_until, Instant};

use crate::{
    module_registry::ModuleRegistry,
    resource_migration::ResourceMigrationSnapshot,
    runtimes::wasmtime::{
        MemorySnapshot, WasmtimeCompiledModule, WasmtimeInstance, WasmtimeRuntime,
    },
    state::ProcessState,
    ProcessReloadStatus, ReloadAcknowledgement, Signal,
};

pub struct HotReloadContext<S: ProcessState + Send> {
    pub module_id: u64,
    pub old_version: u32,
    pub new_version: u32,
    pub memory_snapshot: MemorySnapshot,
    pub resource_snapshot: Option<ResourceMigrationSnapshot>,
    _phantom: std::marker::PhantomData<S>,
}

const DEFAULT_APPLY_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_ROLLBACK_TIMEOUT: Duration = Duration::from_secs(30);

/// A process-level failure observed by the reload coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessReloadError {
    pub process_id: u64,
    pub message: String,
    /// The version reported by the process, when an acknowledgement arrived.
    /// Delivery failures and timeouts have no known version.
    pub known_version: Option<u32>,
}

/// Observable state of a coordinated reload.
///
/// `Committed` and `RolledBack` are terminal and allow a later operation for
/// the same module. `InDoubt` deliberately keeps the module blocked until an
/// operator or a future reconciliation API establishes a single version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReloadStatus {
    Applying,
    RollingBack {
        apply_errors: Vec<ProcessReloadError>,
    },
    Committed {
        acknowledgements: Vec<ReloadAcknowledgement>,
    },
    RolledBack {
        apply_errors: Vec<ProcessReloadError>,
        acknowledgements: Vec<ReloadAcknowledgement>,
    },
    InDoubt {
        apply_errors: Vec<ProcessReloadError>,
        rollback_errors: Vec<ProcessReloadError>,
        acknowledgements: Vec<ReloadAcknowledgement>,
    },
}

impl ReloadStatus {
    fn blocks_new_operation(&self) -> bool {
        matches!(
            self,
            Self::Applying | Self::RollingBack { .. } | Self::InDoubt { .. }
        )
    }
}

/// Coordinator for acknowledgement-based reloads across multiple processes.
pub struct ReloadCoordinator<S: Send + Sync> {
    operations: Arc<RwLock<HashMap<u64, ReloadOperation<S>>>>,
    apply_timeout: Duration,
    rollback_timeout: Duration,
}

struct ReloadOperation<S> {
    affected_processes: Vec<u64>,
    status: ReloadStatus,
    _phantom: std::marker::PhantomData<S>,
}

struct AcknowledgementCollection {
    acknowledgements: Vec<ReloadAcknowledgement>,
    errors: Vec<ProcessReloadError>,
}

impl<S: Send + Sync> ReloadCoordinator<S> {
    pub fn new() -> Self {
        Self::with_timeouts(DEFAULT_APPLY_TIMEOUT, DEFAULT_ROLLBACK_TIMEOUT)
    }

    pub fn with_timeouts(apply_timeout: Duration, rollback_timeout: Duration) -> Self {
        Self {
            operations: Arc::new(RwLock::new(HashMap::new())),
            apply_timeout,
            rollback_timeout,
        }
    }

    /// Reserve a module for a coordinated reload.
    ///
    /// Terminal committed/rolled-back records are replaced by the new
    /// operation. An in-progress or in-doubt operation keeps the module locked.
    pub async fn start_reload(
        &self,
        module_id: u64,
        old_version: u32,
        new_version: u32,
        mut affected_processes: Vec<u64>,
    ) -> Result<()> {
        affected_processes.sort_unstable();
        affected_processes.dedup();

        let mut operations = self.operations.write().await;
        if let Some(operation) = operations.get(&module_id) {
            if operation.status.blocks_new_operation() {
                return Err(anyhow!(
                    "Reload for module {} is blocked by status {:?}",
                    module_id,
                    operation.status
                ));
            }
        }

        operations.insert(
            module_id,
            ReloadOperation {
                affected_processes,
                status: ReloadStatus::Applying,
                _phantom: std::marker::PhantomData,
            },
        );
        drop(operations);

        info!(
            "Started coordinated reload for module {}: {} -> {}",
            module_id, old_version, new_version
        );
        Ok(())
    }

    /// Return the latest observable state for a module.
    pub async fn status(&self, module_id: u64) -> Option<ReloadStatus> {
        self.operations
            .read()
            .await
            .get(&module_id)
            .map(|operation| operation.status.clone())
    }

    /// Alias retained for callers that prefer a getter-style API.
    pub async fn get_status(&self, module_id: u64) -> Option<ReloadStatus> {
        self.status(module_id).await
    }

    /// Check whether a module is applying, rolling back, or awaiting manual
    /// reconciliation after an in-doubt result.
    pub async fn is_reload_in_progress(&self, module_id: u64) -> bool {
        self.operations
            .read()
            .await
            .get(&module_id)
            .is_some_and(|operation| operation.status.blocks_new_operation())
    }

    pub async fn get_affected_processes(&self, module_id: u64) -> Option<Vec<u64>> {
        self.operations
            .read()
            .await
            .get(&module_id)
            .map(|operation| operation.affected_processes.clone())
    }

    async fn set_status(&self, module_id: u64, status: ReloadStatus) -> Result<()> {
        let mut operations = self.operations.write().await;
        let operation = operations
            .get_mut(&module_id)
            .ok_or_else(|| anyhow!("No reload operation found for module {}", module_id))?;
        operation.status = status;
        Ok(())
    }

    fn dispatch_hot_reload(
        env: &dyn crate::env::Environment,
        process_id: u64,
        module_id: u64,
        old_version: u32,
        new_version: u32,
    ) -> oneshot::Receiver<ReloadAcknowledgement> {
        let (acknowledgement, receiver) = oneshot::channel();
        if let Some(process) = env.get_process(process_id) {
            let _ = process.send(Signal::HotReload {
                module_id,
                expected_version: Some(old_version),
                new_version,
                acknowledgement: Some(acknowledgement),
            });
        }
        // When lookup or mailbox delivery fails, dropping the sender closes the
        // receiver. That is an observed failure, never a successful reload.
        receiver
    }

    fn dispatch_rollback(
        env: &dyn crate::env::Environment,
        process_id: u64,
        module_id: u64,
        new_version: u32,
        old_version: u32,
    ) -> oneshot::Receiver<ReloadAcknowledgement> {
        let (acknowledgement, receiver) = oneshot::channel();
        if let Some(process) = env.get_process(process_id) {
            let _ = process.send(Signal::Rollback {
                module_id,
                expected_version: Some(new_version),
                target_version: old_version,
                acknowledgement: Some(acknowledgement),
            });
        }
        receiver
    }

    async fn collect_acknowledgements(
        receivers: Vec<(u64, oneshot::Receiver<ReloadAcknowledgement>)>,
        module_id: u64,
        target_version: u32,
        timeout: Duration,
        phase: &'static str,
    ) -> AcknowledgementCollection {
        let mut pending_ids: HashSet<u64> = receivers.iter().map(|(id, _)| *id).collect();
        let mut pending = receivers
            .into_iter()
            .map(|(process_id, receiver)| async move { (process_id, receiver.await) })
            .collect::<FuturesUnordered<_>>();
        let deadline = Instant::now() + timeout;
        let timer = sleep_until(deadline);
        tokio::pin!(timer);

        let mut acknowledgements = Vec::new();
        let mut errors = Vec::new();

        while !pending.is_empty() {
            tokio::select! {
                biased;
                result = pending.next() => {
                    let Some((process_id, result)) = result else {
                        break;
                    };
                    pending_ids.remove(&process_id);
                    match result {
                        Ok(acknowledgement) => {
                            let protocol_error = if acknowledgement.process_id != process_id {
                                Some(format!(
                                    "{} acknowledgement reported process {}",
                                    phase, acknowledgement.process_id
                                ))
                            } else if acknowledgement.module_id != module_id {
                                Some(format!(
                                    "{} acknowledgement reported module {}",
                                    phase, acknowledgement.module_id
                                ))
                            } else if !acknowledgement.status.is_success() {
                                match &acknowledgement.status {
                                    ProcessReloadStatus::Failed(message) => Some(message.clone()),
                                    _ => Some(format!("{} acknowledgement was not successful", phase)),
                                }
                            } else if acknowledgement.current_version != target_version {
                                Some(format!(
                                    "{} acknowledgement reported version {}, expected {}",
                                    phase, acknowledgement.current_version, target_version
                                ))
                            } else {
                                None
                            };

                            if let Some(message) = protocol_error {
                                errors.push(ProcessReloadError {
                                    process_id,
                                    message,
                                    known_version: Some(acknowledgement.current_version),
                                });
                            } else {
                                acknowledgements.push(acknowledgement);
                            }
                        }
                        Err(_) => errors.push(ProcessReloadError {
                            process_id,
                            message: format!("{} acknowledgement channel closed", phase),
                            known_version: None,
                        }),
                    }
                }
                _ = &mut timer => break,
            }
        }

        for process_id in pending_ids {
            errors.push(ProcessReloadError {
                process_id,
                message: format!("{} acknowledgement timed out after {:?}", phase, timeout),
                known_version: None,
            });
        }

        acknowledgements.sort_by_key(|acknowledgement| acknowledgement.process_id);
        errors.sort_by_key(|error| error.process_id);
        AcknowledgementCollection {
            acknowledgements,
            errors,
        }
    }

    /// Apply a module version to every target, waiting for process-level
    /// acknowledgement before reporting commit. Any apply failure causes a
    /// separately timed rollback request to the complete original target set.
    pub async fn perform_atomic_reload(
        &self,
        env: &dyn crate::env::Environment,
        module_id: u64,
        old_version: u32,
        new_version: u32,
        affected_processes: Vec<u64>,
    ) -> Result<()> {
        self.start_reload(module_id, old_version, new_version, affected_processes)
            .await?;

        let targets = self
            .get_affected_processes(module_id)
            .await
            .ok_or_else(|| anyhow!("No reload operation found for module {}", module_id))?;
        info!(
            "Starting acknowledged reload for module {}: {} -> {} ({} processes)",
            module_id,
            old_version,
            new_version,
            targets.len()
        );

        let apply_receivers = targets
            .iter()
            .map(|process_id| {
                (
                    *process_id,
                    Self::dispatch_hot_reload(
                        env,
                        *process_id,
                        module_id,
                        old_version,
                        new_version,
                    ),
                )
            })
            .collect();
        let apply = Self::collect_acknowledgements(
            apply_receivers,
            module_id,
            new_version,
            self.apply_timeout,
            "reload",
        )
        .await;

        if apply.errors.is_empty() {
            self.set_status(
                module_id,
                ReloadStatus::Committed {
                    acknowledgements: apply.acknowledgements,
                },
            )
            .await?;
            info!(
                "Committed acknowledged reload for module {}: {} -> {}",
                module_id, old_version, new_version
            );
            return Ok(());
        }

        let apply_errors = apply.errors;
        self.set_status(
            module_id,
            ReloadStatus::RollingBack {
                apply_errors: apply_errors.clone(),
            },
        )
        .await?;
        warn!(
            "Reload for module {} failed for {} process(es); rolling back all {} targets",
            module_id,
            apply_errors.len(),
            targets.len()
        );

        let rollback_receivers = targets
            .iter()
            .map(|process_id| {
                (
                    *process_id,
                    Self::dispatch_rollback(env, *process_id, module_id, new_version, old_version),
                )
            })
            .collect();
        let rollback = Self::collect_acknowledgements(
            rollback_receivers,
            module_id,
            old_version,
            self.rollback_timeout,
            "rollback",
        )
        .await;

        if rollback.errors.is_empty() {
            self.set_status(
                module_id,
                ReloadStatus::RolledBack {
                    apply_errors: apply_errors.clone(),
                    acknowledgements: rollback.acknowledgements,
                },
            )
            .await?;
            return Err(anyhow!(
                "Reload for module {} did not commit; rollback to version {} was confirmed for all {} targets: {}",
                module_id,
                old_version,
                targets.len(),
                format_process_errors(&apply_errors)
            ));
        }

        let rollback_errors = rollback.errors;
        self.set_status(
            module_id,
            ReloadStatus::InDoubt {
                apply_errors: apply_errors.clone(),
                rollback_errors: rollback_errors.clone(),
                acknowledgements: rollback.acknowledgements,
            },
        )
        .await?;
        Err(anyhow!(
            "Reload for module {} is in doubt after rollback to version {} failed: apply failures [{}]; rollback failures [{}]",
            module_id,
            old_version,
            format_process_errors(&apply_errors),
            format_process_errors(&rollback_errors)
        ))
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
            let processes = registry.processes_for_module(mid, env.id());
            info!("Module {} has {} active processes", mid, processes.len());

            all_affected_processes.extend(processes.iter().copied());
            module_process_map.insert(mid, processes);
        }

        // Perform reloads in dependency order
        for &mid in &reload_order {
            let _transaction = registry.lock_transaction(mid).await;
            let old_version = registry.get_committed_version_number(mid);
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
            match self
                .perform_atomic_reload(
                    env,
                    mid,
                    old_version.unwrap_or(0),
                    version_to_use,
                    affected_processes,
                )
                .await
            {
                Ok(_) => {
                    registry.mark_committed(mid, version_to_use)?;
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

fn format_process_errors(errors: &[ProcessReloadError]) -> String {
    errors
        .iter()
        .map(|error| match error.known_version {
            Some(version) => format!(
                "process {} at version {}: {}",
                error.process_id, version, error.message
            ),
            None => format!("process {}: {}", error.process_id, error.message),
        })
        .collect::<Vec<_>>()
        .join("; ")
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
            resource_snapshot: None,
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

    pub fn set_resource_snapshot(&mut self, snapshot: ResourceMigrationSnapshot) {
        if !snapshot.is_empty() {
            info!("Captured {} resources for migration", snapshot.count());
            self.resource_snapshot = Some(snapshot);
        }
    }

    pub fn get_resource_snapshot(&self) -> Option<&ResourceMigrationSnapshot> {
        self.resource_snapshot.as_ref()
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
            info!(
                "New import added: '{}::{}' - runtime must support it",
                module, name
            );
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
            wasmtime::ExternType::Func(func_ty) => ExportSignature::Func {
                params: func_ty.params().len(),
                results: func_ty.results().len(),
            },
            wasmtime::ExternType::Global(global_ty) => ExportSignature::Global {
                mutable: global_ty.mutability() == wasmtime::Mutability::Var,
            },
            wasmtime::ExternType::Memory(_) => ExportSignature::Memory,
            wasmtime::ExternType::Table(_) => ExportSignature::Table,
            wasmtime::ExternType::Tag(_) => ExportSignature::Tag,
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
            wasmtime::ExternType::Func(func_ty) => ExportSignature::Func {
                params: func_ty.params().len(),
                results: func_ty.results().len(),
            },
            wasmtime::ExternType::Global(global_ty) => ExportSignature::Global {
                mutable: global_ty.mutability() == wasmtime::Mutability::Var,
            },
            wasmtime::ExternType::Memory(_) => ExportSignature::Memory,
            wasmtime::ExternType::Table(_) => ExportSignature::Table,
            wasmtime::ExternType::Tag(_) => ExportSignature::Tag,
        };
        imports.push((import.module().to_string(), import.name().to_string(), sig));
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
    Tag,
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
        process
            .send(Signal::HotReload {
                module_id,
                expected_version: None,
                new_version,
                acknowledgement: None,
            })
            .map_err(|error| anyhow!("Failed to send hot reload signal: {error}"))?;
        info!(
            "Sent hot reload signal to process {} for module {} version {}",
            process_id, module_id, new_version
        );
        Ok(())
    } else {
        warn!(
            "Process {} not found, cannot send hot reload signal",
            process_id
        );
        Err(anyhow!("Process {} not found", process_id))
    }
}

pub fn send_rollback_signal(
    process_id: u64,
    module_id: u64,
    target_version: u32,
    env: &dyn crate::env::Environment,
) -> Result<()> {
    if let Some(process) = env.get_process(process_id) {
        process
            .send(Signal::Rollback {
                module_id,
                expected_version: None,
                target_version,
                acknowledgement: None,
            })
            .map_err(|error| anyhow!("Failed to send rollback signal: {error}"))?;
        info!(
            "Sent rollback signal to process {} for module {} version {}",
            process_id, module_id, target_version
        );
        Ok(())
    } else {
        warn!(
            "Process {} not found, cannot send rollback signal",
            process_id
        );
        Err(anyhow!("Process {} not found", process_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reloadable_state::ReloadableState;
    use crate::{env::Environment, env::LunaticEnvironment, Process, ReloadAckSender};
    use std::sync::Mutex as StdMutex;

    #[derive(Clone, Copy)]
    enum AckBehavior {
        Success,
        RejectApply,
        HoldApplyRejectRollback,
    }

    struct AckProcess {
        id: u64,
        behavior: AckBehavior,
        held_acknowledgements: StdMutex<Vec<ReloadAckSender>>,
        signals: StdMutex<Vec<&'static str>>,
    }

    impl AckProcess {
        fn new(id: u64, behavior: AckBehavior) -> Self {
            Self {
                id,
                behavior,
                held_acknowledgements: StdMutex::new(Vec::new()),
                signals: StdMutex::new(Vec::new()),
            }
        }

        fn signal_count(&self, signal: &'static str) -> usize {
            self.signals
                .lock()
                .unwrap()
                .iter()
                .filter(|observed| **observed == signal)
                .count()
        }
    }

    fn acknowledge(
        acknowledgement: Option<ReloadAckSender>,
        process_id: u64,
        module_id: u64,
        previous_version: u32,
        current_version: u32,
        status: ProcessReloadStatus,
    ) {
        if let Some(acknowledgement) = acknowledgement {
            let _ = acknowledgement.send(ReloadAcknowledgement {
                process_id,
                module_id,
                previous_version,
                current_version,
                status,
            });
        }
    }

    impl Process for AckProcess {
        fn id(&self) -> u64 {
            self.id
        }

        fn send(&self, signal: Signal) -> std::result::Result<(), crate::state::SignalSendError> {
            match signal {
                Signal::HotReload {
                    module_id,
                    expected_version,
                    new_version,
                    acknowledgement,
                } => {
                    self.signals.lock().unwrap().push("apply");
                    let old_version = expected_version.unwrap_or(0);
                    match self.behavior {
                        AckBehavior::Success => acknowledge(
                            acknowledgement,
                            self.id,
                            module_id,
                            old_version,
                            new_version,
                            ProcessReloadStatus::Applied,
                        ),
                        AckBehavior::RejectApply => acknowledge(
                            acknowledgement,
                            self.id,
                            module_id,
                            old_version,
                            old_version,
                            ProcessReloadStatus::Failed("injected apply rejection".into()),
                        ),
                        AckBehavior::HoldApplyRejectRollback => {
                            if let Some(acknowledgement) = acknowledgement {
                                self.held_acknowledgements
                                    .lock()
                                    .unwrap()
                                    .push(acknowledgement);
                            }
                        }
                    }
                }
                Signal::Rollback {
                    module_id,
                    expected_version,
                    target_version,
                    acknowledgement,
                } => {
                    self.signals.lock().unwrap().push("rollback");
                    match self.behavior {
                        AckBehavior::Success => acknowledge(
                            acknowledgement,
                            self.id,
                            module_id,
                            expected_version.unwrap_or(target_version),
                            target_version,
                            ProcessReloadStatus::Applied,
                        ),
                        AckBehavior::RejectApply => acknowledge(
                            acknowledgement,
                            self.id,
                            module_id,
                            target_version,
                            target_version,
                            ProcessReloadStatus::AlreadyAtTarget,
                        ),
                        AckBehavior::HoldApplyRejectRollback => acknowledge(
                            acknowledgement,
                            self.id,
                            module_id,
                            expected_version.unwrap_or(target_version),
                            expected_version.unwrap_or(target_version),
                            ProcessReloadStatus::Failed("injected rollback rejection".into()),
                        ),
                    }
                }
                _ => {}
            }
            Ok(())
        }
    }

    fn add_ack_process(
        environment: &LunaticEnvironment,
        id: u64,
        behavior: AckBehavior,
    ) -> Arc<AckProcess> {
        let process = Arc::new(AckProcess::new(id, behavior));
        environment.add_process(id, process.clone()).unwrap();
        process
    }

    #[tokio::test]
    async fn test_reload_coordinator_basic() {
        let coordinator = ReloadCoordinator::<()>::new();
        assert!(!coordinator.is_reload_in_progress(1).await);
        assert_eq!(coordinator.status(1).await, None);
    }

    #[tokio::test]
    async fn status_query_reports_applying_and_rejects_concurrent_reload() {
        let coordinator = ReloadCoordinator::<()>::new();
        coordinator
            .start_reload(1, 0, 1, vec![101, 100, 101])
            .await
            .unwrap();
        assert_eq!(coordinator.status(1).await, Some(ReloadStatus::Applying));
        assert!(coordinator.is_reload_in_progress(1).await);
        let processes = coordinator.get_affected_processes(1).await.unwrap();
        assert_eq!(processes, vec![100, 101]);
        assert!(coordinator.start_reload(1, 0, 2, vec![100]).await.is_err());
    }

    #[tokio::test]
    async fn all_acknowledgements_commit_and_allow_the_next_reload() {
        let environment = LunaticEnvironment::new(7);
        add_ack_process(&environment, 100, AckBehavior::Success);
        add_ack_process(&environment, 101, AckBehavior::Success);
        let coordinator = ReloadCoordinator::<()>::with_timeouts(
            Duration::from_millis(100),
            Duration::from_millis(100),
        );

        coordinator
            .perform_atomic_reload(&environment, 1, 0, 1, vec![100, 101])
            .await
            .unwrap();
        match coordinator.get_status(1).await.unwrap() {
            ReloadStatus::Committed { acknowledgements } => {
                assert_eq!(acknowledgements.len(), 2);
            }
            status => panic!("unexpected status: {status:?}"),
        }
        assert!(!coordinator.is_reload_in_progress(1).await);

        coordinator
            .perform_atomic_reload(&environment, 1, 1, 2, vec![100, 101])
            .await
            .unwrap();
        assert!(matches!(
            coordinator.status(1).await,
            Some(ReloadStatus::Committed { .. })
        ));
    }

    #[tokio::test]
    async fn nack_rolls_back_the_original_target_and_allows_retry() {
        let environment = LunaticEnvironment::new(7);
        let applied = add_ack_process(&environment, 100, AckBehavior::Success);
        let rejected = add_ack_process(&environment, 101, AckBehavior::RejectApply);
        let coordinator = ReloadCoordinator::<()>::with_timeouts(
            Duration::from_millis(100),
            Duration::from_millis(100),
        );

        let error = coordinator
            .perform_atomic_reload(&environment, 1, 0, 1, vec![100, 101])
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("rollback to version 0 was confirmed"));
        assert_eq!(applied.signal_count("rollback"), 1);
        assert_eq!(rejected.signal_count("rollback"), 1);
        match coordinator.status(1).await.unwrap() {
            ReloadStatus::RolledBack {
                apply_errors,
                acknowledgements,
            } => {
                assert_eq!(apply_errors.len(), 1);
                assert_eq!(apply_errors[0].process_id, 101);
                assert_eq!(acknowledgements.len(), 2);
            }
            status => panic!("unexpected status: {status:?}"),
        }
        assert!(!coordinator.is_reload_in_progress(1).await);
        assert!(coordinator.start_reload(1, 0, 2, vec![100]).await.is_ok());
    }

    #[tokio::test]
    async fn timeout_and_rollback_nack_leave_the_module_in_doubt() {
        let environment = LunaticEnvironment::new(7);
        let process = add_ack_process(&environment, 100, AckBehavior::HoldApplyRejectRollback);
        let coordinator = ReloadCoordinator::<()>::with_timeouts(
            Duration::from_millis(10),
            Duration::from_millis(100),
        );

        let error = coordinator
            .perform_atomic_reload(&environment, 1, 0, 1, vec![100])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("is in doubt"));
        assert_eq!(process.signal_count("rollback"), 1);
        match coordinator.status(1).await.unwrap() {
            ReloadStatus::InDoubt {
                apply_errors,
                rollback_errors,
                acknowledgements,
            } => {
                assert_eq!(apply_errors.len(), 1);
                assert!(apply_errors[0].message.contains("timed out"));
                assert_eq!(rollback_errors.len(), 1);
                assert_eq!(rollback_errors[0].known_version, Some(1));
                assert!(acknowledgements.is_empty());
            }
            status => panic!("unexpected status: {status:?}"),
        }
        assert!(coordinator.is_reload_in_progress(1).await);
        assert!(coordinator.start_reload(1, 0, 2, vec![100]).await.is_err());
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
        let func1 = ExportSignature::Func {
            params: 2,
            results: 1,
        };
        let func2 = ExportSignature::Func {
            params: 2,
            results: 1,
        };
        let func3 = ExportSignature::Func {
            params: 3,
            results: 1,
        };

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
