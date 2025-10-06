use super::global_process_id::GlobalProcessId;
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Registered name for a process in the distributed registry
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcessName(String);

impl ProcessName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for ProcessName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ProcessName {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Registration scope for process names
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RegistrationScope {
    /// Local to the current node only
    Local,
    /// Global across all nodes in the cluster
    Global,
}

/// Process registry entry with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryEntry {
    pub global_pid: GlobalProcessId,
    pub scope: RegistrationScope,
    pub registered_at: u64, // Unix timestamp in milliseconds
}

/// Distributed process registry providing location transparency
///
/// Supports both local and global name registration, mirroring Erlang's
/// registry semantics:
/// - Local registration: name is unique within the node
/// - Global registration: name is unique across the entire cluster
///
/// Example:
/// ```ignore
/// let registry = DistributedRegistry::new(node_id);
///
/// // Register locally
/// registry.register_local("logger", gpid)?;
///
/// // Register globally (requires coordination)
/// registry.register_global("database_manager", gpid)?;
///
/// // Lookup by name
/// if let Some(entry) = registry.lookup("logger") {
///     // Send message to process
/// }
/// ```
pub struct DistributedRegistry {
    #[allow(dead_code)] // Reserved for future cross-node coordination
    node_id: u64,
    /// Local name -> GlobalProcessId mappings
    local: Arc<DashMap<ProcessName, RegistryEntry>>,
    /// Global name -> GlobalProcessId mappings (requires cluster coordination)
    global: Arc<DashMap<ProcessName, RegistryEntry>>,
    /// Reverse lookup: GlobalProcessId -> registered names
    reverse: Arc<DashMap<GlobalProcessId, Vec<ProcessName>>>,
}

impl DistributedRegistry {
    /// Create a new distributed registry for the given node
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            local: Arc::new(DashMap::new()),
            global: Arc::new(DashMap::new()),
            reverse: Arc::new(DashMap::new()),
        }
    }

    /// Register a process with a local name (node-scoped)
    ///
    /// Returns an error if the name is already registered
    pub fn register_local(
        &self,
        name: impl Into<ProcessName>,
        global_pid: GlobalProcessId,
    ) -> Result<()> {
        let name = name.into();

        if self.local.contains_key(&name) {
            return Err(anyhow!(
                "Name '{}' already registered locally",
                name.as_str()
            ));
        }

        let entry = RegistryEntry {
            global_pid,
            scope: RegistrationScope::Local,
            registered_at: current_timestamp_ms(),
        };

        self.local.insert(name.clone(), entry);
        self.add_reverse_mapping(global_pid, name);

        Ok(())
    }

    /// Register a process with a global name (cluster-wide)
    ///
    /// Returns an error if the name is already registered globally
    ///
    /// Note: In a production system, this should coordinate with other nodes
    /// via the control plane to ensure uniqueness across the cluster
    pub fn register_global(
        &self,
        name: impl Into<ProcessName>,
        global_pid: GlobalProcessId,
    ) -> Result<()> {
        let name = name.into();

        if self.global.contains_key(&name) {
            return Err(anyhow!(
                "Name '{}' already registered globally",
                name.as_str()
            ));
        }

        let entry = RegistryEntry {
            global_pid,
            scope: RegistrationScope::Global,
            registered_at: current_timestamp_ms(),
        };

        self.global.insert(name.clone(), entry);
        self.add_reverse_mapping(global_pid, name);

        Ok(())
    }

    /// Unregister a process name
    ///
    /// Removes both local and global registrations if they exist
    pub fn unregister(&self, name: impl Into<ProcessName>) -> Result<()> {
        let name = name.into();

        let removed_local = self.local.remove(&name);
        let removed_global = self.global.remove(&name);

        if let Some((_, entry)) = removed_local {
            self.remove_reverse_mapping(entry.global_pid, &name);
            return Ok(());
        }

        if let Some((_, entry)) = removed_global {
            self.remove_reverse_mapping(entry.global_pid, &name);
            return Ok(());
        }

        Err(anyhow!("Name '{}' not registered", name.as_str()))
    }

    /// Unregister all names for a given process
    ///
    /// Called automatically when a process terminates
    pub fn unregister_process(&self, global_pid: GlobalProcessId) {
        if let Some((_, names)) = self.reverse.remove(&global_pid) {
            for name in names {
                self.local.remove(&name);
                self.global.remove(&name);
            }
        }
    }

    /// Lookup a process by name (checks local first, then global)
    pub fn lookup(&self, name: impl Into<ProcessName>) -> Option<RegistryEntry> {
        let name = name.into();

        // Check local registry first
        if let Some(entry) = self.local.get(&name) {
            return Some(entry.clone());
        }

        // Check global registry
        self.global.get(&name).map(|entry| entry.clone())
    }

    /// Lookup a process by name in local scope only
    pub fn lookup_local(&self, name: impl Into<ProcessName>) -> Option<RegistryEntry> {
        let name = name.into();
        self.local.get(&name).map(|entry| entry.clone())
    }

    /// Lookup a process by name in global scope only
    pub fn lookup_global(&self, name: impl Into<ProcessName>) -> Option<RegistryEntry> {
        let name = name.into();
        self.global.get(&name).map(|entry| entry.clone())
    }

    /// Get all registered names for a process
    pub fn get_names(&self, global_pid: GlobalProcessId) -> Vec<ProcessName> {
        self.reverse
            .get(&global_pid)
            .map(|names| names.clone())
            .unwrap_or_default()
    }

    /// Get all locally registered names
    pub fn local_names(&self) -> Vec<ProcessName> {
        self.local.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Get all globally registered names
    pub fn global_names(&self) -> Vec<ProcessName> {
        self.global
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get the number of locally registered names
    pub fn local_count(&self) -> usize {
        self.local.len()
    }

    /// Get the number of globally registered names
    pub fn global_count(&self) -> usize {
        self.global.len()
    }

    /// Clear all registrations (for testing/cleanup)
    pub fn clear(&self) {
        self.local.clear();
        self.global.clear();
        self.reverse.clear();
    }

    // Internal helpers

    fn add_reverse_mapping(&self, global_pid: GlobalProcessId, name: ProcessName) {
        self.reverse
            .entry(global_pid)
            .or_insert_with(Vec::new)
            .push(name);
    }

    fn remove_reverse_mapping(&self, global_pid: GlobalProcessId, name: &ProcessName) {
        if let Some(mut names) = self.reverse.get_mut(&global_pid) {
            names.retain(|n| n != name);
            if names.is_empty() {
                drop(names);
                self.reverse.remove(&global_pid);
            }
        }
    }
}

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_registration() {
        let registry = DistributedRegistry::new(1);
        let gpid = GlobalProcessId::new(1, 1, 100);

        assert!(registry.register_local("logger", gpid).is_ok());

        let entry = registry.lookup("logger").unwrap();
        assert_eq!(entry.global_pid, gpid);
        assert_eq!(entry.scope, RegistrationScope::Local);
    }

    #[test]
    fn test_global_registration() {
        let registry = DistributedRegistry::new(1);
        let gpid = GlobalProcessId::new(1, 1, 200);

        assert!(registry.register_global("db_manager", gpid).is_ok());

        let entry = registry.lookup_global("db_manager").unwrap();
        assert_eq!(entry.global_pid, gpid);
        assert_eq!(entry.scope, RegistrationScope::Global);
    }

    #[test]
    fn test_duplicate_registration_fails() {
        let registry = DistributedRegistry::new(1);
        let gpid1 = GlobalProcessId::new(1, 1, 100);
        let gpid2 = GlobalProcessId::new(1, 1, 200);

        registry.register_local("service", gpid1).unwrap();
        let result = registry.register_local("service", gpid2);
        assert!(result.is_err());
    }

    #[test]
    fn test_unregister() {
        let registry = DistributedRegistry::new(1);
        let gpid = GlobalProcessId::new(1, 1, 100);

        registry.register_local("temp", gpid).unwrap();
        assert!(registry.lookup("temp").is_some());

        registry.unregister("temp").unwrap();
        assert!(registry.lookup("temp").is_none());
    }

    #[test]
    fn test_unregister_process() {
        let registry = DistributedRegistry::new(1);
        let gpid = GlobalProcessId::new(1, 1, 100);

        registry.register_local("name1", gpid).unwrap();
        registry.register_global("name2", gpid).unwrap();

        assert_eq!(registry.get_names(gpid).len(), 2);

        registry.unregister_process(gpid);

        assert!(registry.lookup("name1").is_none());
        assert!(registry.lookup("name2").is_none());
        assert_eq!(registry.get_names(gpid).len(), 0);
    }

    #[test]
    fn test_reverse_lookup() {
        let registry = DistributedRegistry::new(1);
        let gpid = GlobalProcessId::new(1, 1, 100);

        registry.register_local("alias1", gpid).unwrap();
        registry.register_global("alias2", gpid).unwrap();

        let names = registry.get_names(gpid);
        assert_eq!(names.len(), 2);
        assert!(names.contains(&ProcessName::new("alias1")));
        assert!(names.contains(&ProcessName::new("alias2")));
    }

    #[test]
    fn test_local_vs_global_lookup() {
        let registry = DistributedRegistry::new(1);
        let gpid_local = GlobalProcessId::new(1, 1, 100);
        let gpid_global = GlobalProcessId::new(1, 1, 200);

        registry.register_local("local_only", gpid_local).unwrap();
        registry
            .register_global("global_only", gpid_global)
            .unwrap();

        assert!(registry.lookup_local("local_only").is_some());
        assert!(registry.lookup_local("global_only").is_none());

        assert!(registry.lookup_global("global_only").is_some());
        assert!(registry.lookup_global("local_only").is_none());

        // Generic lookup finds both
        assert!(registry.lookup("local_only").is_some());
        assert!(registry.lookup("global_only").is_some());
    }

    #[test]
    fn test_counters() {
        let registry = DistributedRegistry::new(1);
        let gpid1 = GlobalProcessId::new(1, 1, 100);
        let gpid2 = GlobalProcessId::new(1, 1, 200);

        registry.register_local("local1", gpid1).unwrap();
        registry.register_local("local2", gpid1).unwrap();
        registry.register_global("global1", gpid2).unwrap();

        assert_eq!(registry.local_count(), 2);
        assert_eq!(registry.global_count(), 1);
    }
}
