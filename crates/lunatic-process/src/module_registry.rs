use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{anyhow, ensure, Result};
use dashmap::DashMap;

use crate::runtimes::wasmtime::WasmtimeCompiledModule;
use crate::state::ProcessState;

pub struct ModuleVersion<S: ProcessState> {
    pub id: u64,
    pub version: u32,
    pub module: Arc<WasmtimeCompiledModule<S>>,
    pub loaded_at: SystemTime,
    processes: HashSet<ProcessKey>,
}

/// A process identity is scoped by its environment. Process IDs are only
/// unique inside one environment, while a module registry can be shared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProcessKey {
    pub environment_id: u64,
    pub process_id: u64,
}

impl<S: ProcessState> ModuleVersion<S> {
    pub fn new(id: u64, version: u32, module: Arc<WasmtimeCompiledModule<S>>) -> Self {
        Self {
            id,
            version,
            module,
            loaded_at: SystemTime::now(),
            processes: HashSet::new(),
        }
    }

    pub fn get_process_count(&self) -> usize {
        self.processes.len()
    }
}

struct ModuleHistory<S: ProcessState> {
    next_version: u32,
    committed_version: u32,
    versions: Vec<ModuleVersion<S>>,
}

pub struct ModuleRegistry<S: ProcessState> {
    modules: DashMap<u64, ModuleHistory<S>>,
    max_versions: usize,
    /// Serializes compile/register/apply/commit transactions per module.
    transaction_locks: DashMap<u64, Arc<tokio::sync::Mutex<()>>>,
    /// Dependencies: module_id -> list of modules that depend on it
    dependencies: DashMap<u64, Vec<u64>>,
}

impl<S: ProcessState> ModuleRegistry<S> {
    pub fn new() -> Self {
        Self {
            modules: DashMap::new(),
            max_versions: 2,
            transaction_locks: DashMap::new(),
            dependencies: DashMap::new(),
        }
    }

    pub fn with_max_versions(max_versions: usize) -> Self {
        Self {
            modules: DashMap::new(),
            // A running upgrade needs both the committed and candidate
            // version available until rollback is no longer possible.
            max_versions: max_versions.max(2),
            transaction_locks: DashMap::new(),
            dependencies: DashMap::new(),
        }
    }

    /// Hold this guard from the committed-version read through the final
    /// registry commit. The lock belongs to the registry so separate
    /// coordinators cannot race candidates for the same module.
    pub async fn lock_transaction(&self, id: u64) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = self
            .transaction_locks
            .entry(id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        lock.lock_owned().await
    }

    pub fn add_version(&self, id: u64, module: WasmtimeCompiledModule<S>) -> Result<u32> {
        let mut entry = self.modules.entry(id).or_insert_with(|| ModuleHistory {
            next_version: 0,
            committed_version: 0,
            versions: Vec::new(),
        });
        let version = entry.next_version;
        entry.next_version = entry
            .next_version
            .checked_add(1)
            .ok_or_else(|| anyhow!("Module {} exhausted its version ID space", id))?;
        let is_initial_version = entry.versions.is_empty();
        let module_version = ModuleVersion::new(id, version, Arc::new(module));
        entry.versions.push(module_version);
        if is_initial_version {
            entry.committed_version = version;
        }

        if entry.versions.len() > self.max_versions {
            self.cleanup_old_versions(&mut entry);
        }

        Ok(version)
    }

    pub fn get_latest(&self, id: u64) -> Option<Arc<WasmtimeCompiledModule<S>>> {
        self.modules
            .get(&id)
            .and_then(|history| history.versions.last().map(|v| v.module.clone()))
    }

    pub fn get_version(&self, id: u64, version: u32) -> Option<Arc<WasmtimeCompiledModule<S>>> {
        self.modules.get(&id).and_then(|history| {
            history
                .versions
                .iter()
                .find(|v| v.version == version)
                .map(|v| v.module.clone())
        })
    }

    pub fn get_latest_version_number(&self, id: u64) -> Option<u32> {
        self.modules
            .get(&id)
            .and_then(|history| history.versions.last().map(|v| v.version))
    }

    pub fn get_committed_version_number(&self, id: u64) -> Option<u32> {
        self.modules
            .get(&id)
            .map(|history| history.committed_version)
    }

    pub fn get_committed(&self, id: u64) -> Option<Arc<WasmtimeCompiledModule<S>>> {
        let history = self.modules.get(&id)?;
        let committed = history.committed_version;
        history
            .versions
            .iter()
            .find(|version| version.version == committed)
            .map(|version| version.module.clone())
    }

    pub fn mark_committed(&self, id: u64, version: u32) -> Result<()> {
        let mut history = self
            .modules
            .get_mut(&id)
            .ok_or_else(|| anyhow!("Module {} is not registered", id))?;
        ensure!(
            history
                .versions
                .iter()
                .any(|candidate| candidate.version == version),
            "Module {} version {} is not registered",
            id,
            version
        );
        history.committed_version = version;
        self.cleanup_old_versions(&mut history);
        Ok(())
    }

    pub fn version_count(&self, id: u64) -> usize {
        self.modules
            .get(&id)
            .map(|history| history.versions.len())
            .unwrap_or(0)
    }

    pub fn process_count(&self, id: u64, version: u32) -> Option<usize> {
        self.modules.get(&id).and_then(|history| {
            history
                .versions
                .iter()
                .find(|candidate| candidate.version == version)
                .map(ModuleVersion::get_process_count)
        })
    }

    pub fn register_process(&self, id: u64, version: u32, process: ProcessKey) -> Result<()> {
        let mut history = self
            .modules
            .get_mut(&id)
            .ok_or_else(|| anyhow!("Module {} is not registered", id))?;
        ensure!(
            !history
                .versions
                .iter()
                .any(|candidate| candidate.processes.contains(&process)),
            "Process {:?} is already registered for module {}",
            process,
            id
        );
        let target = history
            .versions
            .iter_mut()
            .find(|candidate| candidate.version == version)
            .ok_or_else(|| anyhow!("Module {} version {} is not registered", id, version))?;
        target.processes.insert(process);
        Ok(())
    }

    pub fn transition_process(
        &self,
        id: u64,
        old_version: u32,
        new_version: u32,
        process: ProcessKey,
    ) -> Result<()> {
        let mut history = self
            .modules
            .get_mut(&id)
            .ok_or_else(|| anyhow!("Module {} is not registered", id))?;
        let old_index = history
            .versions
            .iter()
            .position(|candidate| candidate.version == old_version)
            .ok_or_else(|| anyhow!("Module {} version {} is not registered", id, old_version))?;
        ensure!(
            history.versions[old_index].processes.contains(&process),
            "Process {:?} is not registered for module {} version {}",
            process,
            id,
            old_version
        );

        // Re-applying the currently committed version is an idempotent
        // operation, but it is only valid for a process that is actually
        // registered on that version. In particular, a missing process must
        // not be turned into a successful transition by this fast path.
        if old_version == new_version {
            return Ok(());
        }

        let new_index = history
            .versions
            .iter()
            .position(|candidate| candidate.version == new_version)
            .ok_or_else(|| anyhow!("Module {} version {} is not registered", id, new_version))?;
        ensure!(
            !history.versions[new_index].processes.contains(&process),
            "Process {:?} is already registered for module {} version {}",
            process,
            id,
            new_version
        );

        history.versions[old_index].processes.remove(&process);
        history.versions[new_index].processes.insert(process);
        self.cleanup_old_versions(&mut history);
        Ok(())
    }

    pub fn unregister_process(&self, id: u64, version: u32, process: ProcessKey) -> Result<()> {
        let mut history = self
            .modules
            .get_mut(&id)
            .ok_or_else(|| anyhow!("Module {} is not registered", id))?;
        let removed = history
            .versions
            .iter_mut()
            .find(|candidate| candidate.version == version)
            .ok_or_else(|| anyhow!("Module {} version {} is not registered", id, version))?
            .processes
            .remove(&process);
        ensure!(
            removed,
            "Process {:?} is not registered for module {} version {}",
            process,
            id,
            version
        );
        self.cleanup_old_versions(&mut history);
        Ok(())
    }

    pub fn processes_for_module(&self, id: u64, environment_id: u64) -> Vec<u64> {
        let Some(history) = self.modules.get(&id) else {
            return Vec::new();
        };
        let mut processes: Vec<_> = history
            .versions
            .iter()
            .flat_map(|version| version.processes.iter())
            .filter(|process| process.environment_id == environment_id)
            .map(|process| process.process_id)
            .collect();
        processes.sort_unstable();
        processes.dedup();
        processes
    }

    fn cleanup_old_versions(&self, history: &mut ModuleHistory<S>) {
        let keep_from = history.versions.len().saturating_sub(self.max_versions);
        let committed = history.committed_version;
        let mut index = 0usize;
        history.versions.retain(|version| {
            let is_recent = index >= keep_from;
            index += 1;
            version.version == committed || !version.processes.is_empty() || is_recent
        });
    }

    /// Add a dependency: dependent_module depends on dependency_module
    pub fn add_dependency(&self, dependent_module: u64, dependency_module: u64) {
        let mut deps = self.dependencies.entry(dependency_module).or_default();
        if !deps.contains(&dependent_module) {
            deps.push(dependent_module);
        }
    }

    /// Get modules that depend on the given module
    pub fn get_dependents(&self, module_id: u64) -> Vec<u64> {
        self.dependencies
            .get(&module_id)
            .map(|deps| deps.clone())
            .unwrap_or_default()
    }

    /// Get the reload order for a module and its dependents
    pub fn get_reload_order(&self, module_id: u64) -> Vec<u64> {
        let mut order = Vec::new();
        let mut visited = std::collections::HashSet::new();

        fn visit_module(
            registry: &ModuleRegistry<impl ProcessState>,
            module_id: u64,
            order: &mut Vec<u64>,
            visited: &mut std::collections::HashSet<u64>,
        ) {
            if visited.contains(&module_id) {
                return;
            }
            visited.insert(module_id);

            // First reload dependents
            for dependent in registry.get_dependents(module_id) {
                visit_module(registry, dependent, order, visited);
            }

            // Then reload this module
            order.push(module_id);
        }

        visit_module(self, module_id, &mut order, &mut visited);
        order
    }
}

impl<S: ProcessState> Default for ModuleRegistry<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use anyhow::Result;
    use serde::{Deserialize, Serialize};
    use tokio::sync::RwLock;
    use wasmtime::Linker;

    use super::*;
    use crate::{
        config::ProcessConfig,
        mailbox::MessageMailbox,
        reloadable_state::ReloadableState,
        runtimes::{
            wasmtime::{WasmtimeCompiledModule, WasmtimeRuntime},
            RawWasm,
        },
        state::{ConfigResources, ProcessState, SignalReceiver, SignalSender},
    };

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

    struct TestState;

    impl ReloadableState for TestState {
        fn serialize_state(&self) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        fn deserialize_state(_bytes: &[u8]) -> Result<Self> {
            Ok(Self)
        }
    }

    impl ProcessState for TestState {
        type Config = TestConfig;

        fn new_state(
            &self,
            _module: Arc<WasmtimeCompiledModule<Self>>,
            _config: Arc<Self::Config>,
        ) -> Result<Self> {
            Ok(Self)
        }

        fn register(_linker: &mut Linker<Self>) -> Result<()> {
            Ok(())
        }

        fn initialize(&mut self) {}

        fn is_initialized(&self) -> bool {
            false
        }

        fn runtime(&self) -> &WasmtimeRuntime {
            unreachable!("the registry tests never instantiate a process")
        }

        fn module(&self) -> &Arc<WasmtimeCompiledModule<Self>> {
            unreachable!("the registry tests never instantiate a process")
        }

        fn config(&self) -> &Arc<Self::Config> {
            unreachable!("the registry tests never instantiate a process")
        }

        fn id(&self) -> u64 {
            0
        }

        fn signal_mailbox(&self) -> &(SignalSender, SignalReceiver) {
            unreachable!("the registry tests never instantiate a process")
        }

        fn message_mailbox(&self) -> &MessageMailbox {
            unreachable!("the registry tests never instantiate a process")
        }

        fn config_resources(&self) -> &ConfigResources<Self::Config> {
            unreachable!("the registry tests never instantiate a process")
        }

        fn config_resources_mut(&mut self) -> &mut ConfigResources<Self::Config> {
            unreachable!("the registry tests never instantiate a process")
        }

        fn registry(&self) -> &Arc<RwLock<HashMap<String, (u64, u64)>>> {
            unreachable!("the registry tests never instantiate a process")
        }
    }

    fn compiled_module() -> WasmtimeCompiledModule<TestState> {
        const EMPTY_WASM_MODULE: &[u8] = b"\0asm\x01\0\0\0";

        let engine = wasmtime::Engine::default();
        let module = wasmtime::Module::new(&engine, EMPTY_WASM_MODULE).unwrap();
        let linker = Linker::<TestState>::new(&engine);
        let instance_pre = linker.instantiate_pre(&module).unwrap();
        WasmtimeCompiledModule::new(
            RawWasm::new(None, EMPTY_WASM_MODULE.to_vec()),
            module,
            instance_pre,
        )
    }

    fn add_versions(registry: &ModuleRegistry<TestState>, module_id: u64, count: u32) -> Vec<u32> {
        (0..count)
            .map(|_| registry.add_version(module_id, compiled_module()).unwrap())
            .collect()
    }

    #[test]
    fn version_ids_remain_monotonic_after_cleanup() {
        let registry = ModuleRegistry::with_max_versions(2);

        assert_eq!(add_versions(&registry, 7, 4), vec![0, 1, 2, 3]);
        assert_eq!(registry.get_latest_version_number(7), Some(3));
        assert_eq!(registry.get_committed_version_number(7), Some(0));

        // The committed version and the two newest candidates survive. The
        // removed slot must not cause the next version ID to be reused.
        assert!(registry.get_version(7, 0).is_some());
        assert!(registry.get_version(7, 1).is_none());
        assert!(registry.get_version(7, 2).is_some());
        assert!(registry.get_version(7, 3).is_some());
        assert_eq!(registry.version_count(7), 3);
        assert_eq!(registry.add_version(7, compiled_module()).unwrap(), 4);
        assert_eq!(registry.get_latest_version_number(7), Some(4));
    }

    #[test]
    fn cleanup_preserves_committed_active_and_recent_versions() {
        let registry = ModuleRegistry::with_max_versions(2);
        assert_eq!(add_versions(&registry, 9, 4), vec![0, 1, 2, 3]);

        let active = ProcessKey {
            environment_id: 4,
            process_id: 17,
        };
        registry.register_process(9, 0, active).unwrap();
        registry.mark_committed(9, 2).unwrap();

        // v0 is active, v2 is committed and v3 is recent. Adding v4 keeps all
        // three categories even though the registry retention is only two.
        assert_eq!(registry.add_version(9, compiled_module()).unwrap(), 4);
        assert!(registry.get_version(9, 0).is_some());
        assert!(registry.get_version(9, 2).is_some());
        assert!(registry.get_version(9, 3).is_some());
        assert!(registry.get_version(9, 4).is_some());

        registry.unregister_process(9, 0, active).unwrap();
        assert!(registry.get_version(9, 0).is_none());

        registry.mark_committed(9, 4).unwrap();
        assert!(registry.get_version(9, 2).is_none());
        assert!(registry.get_version(9, 3).is_some());
        assert!(registry.get_version(9, 4).is_some());
        assert_eq!(registry.version_count(9), 2);
    }

    #[test]
    fn process_membership_operations_are_checked_and_environment_scoped() {
        let registry = ModuleRegistry::new();
        assert_eq!(add_versions(&registry, 11, 2), vec![0, 1]);

        let first = ProcessKey {
            environment_id: 10,
            process_id: 1,
        };
        let second = ProcessKey {
            environment_id: 10,
            process_id: 2,
        };
        let same_id_other_environment = ProcessKey {
            environment_id: 20,
            process_id: 1,
        };

        registry.register_process(11, 0, first).unwrap();
        registry.register_process(11, 1, second).unwrap();
        registry
            .register_process(11, 0, same_id_other_environment)
            .unwrap();
        assert_eq!(registry.process_count(11, 0), Some(2));
        assert_eq!(registry.process_count(11, 1), Some(1));
        assert_eq!(registry.processes_for_module(11, 10), vec![1, 2]);
        assert_eq!(registry.processes_for_module(11, 20), vec![1]);
        assert!(registry.processes_for_module(11, 30).is_empty());

        assert!(registry.register_process(11, 0, first).is_err());
        assert!(registry.register_process(11, 99, first).is_err());
        assert!(registry.transition_process(11, 0, 1, second).is_err());
        assert!(registry.transition_process(11, 0, 99, first).is_err());

        registry.transition_process(11, 0, 1, first).unwrap();
        assert_eq!(registry.process_count(11, 0), Some(1));
        assert_eq!(registry.process_count(11, 1), Some(2));
        assert!(registry.transition_process(11, 0, 1, first).is_err());
        assert!(registry.unregister_process(11, 0, first).is_err());

        registry.unregister_process(11, 1, first).unwrap();
        assert!(registry.unregister_process(11, 1, first).is_err());
        assert_eq!(registry.process_count(11, 1), Some(1));
        assert_eq!(registry.processes_for_module(11, 10), vec![2]);

        assert!(registry
            .transition_process(
                11,
                0,
                0,
                ProcessKey {
                    environment_id: 99,
                    process_id: 99,
                }
            )
            .is_err());
        registry.transition_process(11, 1, 1, second).unwrap();
    }
}
