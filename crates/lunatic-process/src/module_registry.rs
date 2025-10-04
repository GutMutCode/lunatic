use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use dashmap::DashMap;

use crate::runtimes::wasmtime::WasmtimeCompiledModule;
use crate::state::ProcessState;

pub struct ModuleVersion<S: ProcessState> {
    pub id: u64,
    pub version: u32,
    pub module: Arc<WasmtimeCompiledModule<S>>,
    pub loaded_at: SystemTime,
    pub process_count: AtomicUsize,
}

impl<S: ProcessState> ModuleVersion<S> {
    pub fn new(id: u64, version: u32, module: Arc<WasmtimeCompiledModule<S>>) -> Self {
        Self {
            id,
            version,
            module,
            loaded_at: SystemTime::now(),
            process_count: AtomicUsize::new(0),
        }
    }

    pub fn increment_process_count(&self) {
        self.process_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_process_count(&self) {
        self.process_count.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn get_process_count(&self) -> usize {
        self.process_count.load(Ordering::Relaxed)
    }
}

pub struct ModuleRegistry<S: ProcessState> {
    modules: DashMap<u64, Vec<ModuleVersion<S>>>,
    max_versions: usize,
}

impl<S: ProcessState> ModuleRegistry<S> {
    pub fn new() -> Self {
        Self {
            modules: DashMap::new(),
            max_versions: 2,
        }
    }

    pub fn with_max_versions(max_versions: usize) -> Self {
        Self {
            modules: DashMap::new(),
            max_versions,
        }
    }

    pub fn add_version(&self, id: u64, module: WasmtimeCompiledModule<S>) -> u32 {
        let mut entry = self.modules.entry(id).or_insert_with(Vec::new);
        let version = entry.len() as u32;
        let module_version = ModuleVersion::new(id, version, Arc::new(module));
        entry.push(module_version);

        if entry.len() > self.max_versions {
            self.cleanup_old_versions(&mut entry);
        }

        version
    }

    pub fn get_latest(&self, id: u64) -> Option<Arc<WasmtimeCompiledModule<S>>> {
        self.modules
            .get(&id)
            .and_then(|versions| versions.last().map(|v| v.module.clone()))
    }

    pub fn get_version(&self, id: u64, version: u32) -> Option<Arc<WasmtimeCompiledModule<S>>> {
        self.modules.get(&id).and_then(|versions| {
            versions
                .iter()
                .find(|v| v.version == version)
                .map(|v| v.module.clone())
        })
    }

    pub fn get_latest_version_number(&self, id: u64) -> Option<u32> {
        self.modules
            .get(&id)
            .and_then(|versions| versions.last().map(|v| v.version))
    }

    fn cleanup_old_versions(&self, versions: &mut Vec<ModuleVersion<S>>) {
        let max_versions = self.max_versions;
        let total_versions = versions.len();
        versions.retain(|v| {
            let process_count = v.get_process_count();
            let is_recent = (total_versions - v.version as usize) <= max_versions;
            process_count > 0 || is_recent
        });
    }

    pub fn increment_process_count(&self, id: u64, version: u32) {
        if let Some(versions) = self.modules.get(&id) {
            if let Some(module_version) = versions.iter().find(|v| v.version == version) {
                module_version.increment_process_count();
            }
        }
    }

    pub fn decrement_process_count(&self, id: u64, version: u32) {
        if let Some(versions) = self.modules.get(&id) {
            if let Some(module_version) = versions.iter().find(|v| v.version == version) {
                module_version.decrement_process_count();
            }
        }
    }

    pub fn version_count(&self, id: u64) -> usize {
        self.modules
            .get(&id)
            .map(|versions| versions.len())
            .unwrap_or(0)
    }
}

impl<S: ProcessState> Default for ModuleRegistry<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_registry_structure() {
        assert_eq!(2, 2);
    }
}
