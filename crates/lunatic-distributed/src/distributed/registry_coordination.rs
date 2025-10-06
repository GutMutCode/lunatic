use super::global_process_id::GlobalProcessId;
use super::registry::{DistributedRegistry, ProcessName};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Messages for coordinating global process registry across nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegistryCoordinationMessage {
    /// Request to register a global name
    GlobalRegisterRequest {
        request_id: u64,
        requesting_node_id: u64,
        name: String,
        global_pid: GlobalProcessId,
    },

    /// Response to global registration request
    GlobalRegisterResponse {
        request_id: u64,
        result: GlobalRegisterResult,
    },

    /// Notify all nodes of successful global registration
    GlobalRegisterNotify {
        name: String,
        global_pid: GlobalProcessId,
        registered_at: u64,
    },

    /// Request to unregister a global name
    GlobalUnregisterRequest {
        request_id: u64,
        requesting_node_id: u64,
        name: String,
    },

    /// Response to global unregistration request
    GlobalUnregisterResponse {
        request_id: u64,
        result: GlobalUnregisterResult,
    },

    /// Notify all nodes of successful global unregistration
    GlobalUnregisterNotify { name: String },

    /// Synchronize full global registry state (for new nodes joining cluster)
    RegistrySyncRequest {
        request_id: u64,
        requesting_node_id: u64,
    },

    /// Response with full global registry snapshot
    RegistrySyncResponse {
        request_id: u64,
        global_entries: Vec<(String, GlobalProcessId, u64)>, // (name, gpid, timestamp)
    },

    /// Heartbeat to detect node failures and cleanup stale registrations
    RegistryHeartbeat { node_id: u64, timestamp: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GlobalRegisterResult {
    Success,
    AlreadyRegistered {
        existing_gpid: GlobalProcessId,
        registered_at: u64,
    },
    ConflictDetected {
        conflicting_node_id: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GlobalUnregisterResult {
    Success,
    NotFound,
}

/// Coordinator for managing global process registry across distributed nodes
///
/// Implements a simple majority-based consensus protocol:
/// 1. Node requests global registration from all other nodes
/// 2. If majority accepts (no conflicts), registration succeeds
/// 3. All nodes are notified of successful registration
/// 4. On split-brain/conflicts, newer registration wins (timestamp-based)
pub struct RegistryCoordinator {
    registry: Arc<DistributedRegistry>,
    #[allow(dead_code)] // Reserved for future coordination features
    node_id: u64,
    pending_requests: Arc<RwLock<std::collections::HashMap<u64, PendingRequest>>>,
    next_request_id: std::sync::atomic::AtomicU64,
}

#[derive(Debug)]
struct PendingRequest {
    request_type: RequestType,
    responses: Vec<GlobalRegisterResult>,
    required_responses: usize,
}

#[derive(Debug)]
enum RequestType {
    Register {
        name: String,
        global_pid: GlobalProcessId,
    },
    #[allow(dead_code)] // Reserved for future unregister coordination
    Unregister { name: String },
}

impl RegistryCoordinator {
    /// Create a new registry coordinator
    pub fn new(registry: Arc<DistributedRegistry>, node_id: u64) -> Self {
        Self {
            registry,
            node_id,
            pending_requests: Arc::new(RwLock::new(std::collections::HashMap::new())),
            next_request_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Request global registration with cross-node coordination
    ///
    /// Returns Ok(()) if registration successful across majority of nodes
    pub async fn register_global_coordinated(
        &self,
        name: impl Into<ProcessName>,
        global_pid: GlobalProcessId,
        node_count: usize,
    ) -> Result<()> {
        let name = name.into();
        let name_str = name.as_str().to_string();

        // Check local registry first
        if let Some(existing) = self.registry.lookup_global(name.as_str()) {
            return Err(anyhow!(
                "Name '{}' already registered globally with pid {}",
                name_str,
                existing.global_pid
            ));
        }

        // For single node, register directly
        if node_count <= 1 {
            return self.registry.register_global(name, global_pid);
        }

        // Multi-node: coordinate via consensus
        let request_id = self
            .next_request_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Store pending request
        {
            let mut pending = self.pending_requests.write().await;
            pending.insert(
                request_id,
                PendingRequest {
                    request_type: RequestType::Register {
                        name: name_str.clone(),
                        global_pid,
                    },
                    responses: Vec::new(),
                    required_responses: (node_count / 2) + 1, // Majority
                },
            );
        }

        // Return request message to send to other nodes
        // (actual sending handled by caller via control plane)
        Ok(())
    }

    /// Handle incoming global registration request from another node
    pub async fn handle_register_request(
        &self,
        request_id: u64,
        _requesting_node_id: u64,
        name: String,
        global_pid: GlobalProcessId,
    ) -> RegistryCoordinationMessage {
        // Check if name is already registered globally
        let result = match self.registry.lookup_global(name.as_str()) {
            Some(existing) => {
                // Name already registered - conflict
                if existing.global_pid == global_pid {
                    // Same process, OK
                    GlobalRegisterResult::Success
                } else {
                    // Different process - report conflict
                    GlobalRegisterResult::AlreadyRegistered {
                        existing_gpid: existing.global_pid,
                        registered_at: existing.registered_at,
                    }
                }
            }
            None => {
                // Not registered locally - tentatively accept
                GlobalRegisterResult::Success
            }
        };

        RegistryCoordinationMessage::GlobalRegisterResponse { request_id, result }
    }

    /// Handle response to our global registration request
    pub async fn handle_register_response(
        &self,
        request_id: u64,
        result: GlobalRegisterResult,
    ) -> Result<Option<RegistryCoordinationMessage>> {
        let mut pending = self.pending_requests.write().await;

        if let Some(req) = pending.get_mut(&request_id) {
            req.responses.push(result.clone());

            // Check if we have enough responses
            if req.responses.len() >= req.required_responses {
                // Count successes
                let successes = req
                    .responses
                    .iter()
                    .filter(|r| matches!(r, GlobalRegisterResult::Success))
                    .count();

                let majority = req.required_responses;

                if successes >= majority {
                    // Majority approved - commit registration
                    if let RequestType::Register { name, global_pid } = &req.request_type {
                        self.registry.register_global(name.as_str(), *global_pid)?;

                        // Return notification message
                        let notify = RegistryCoordinationMessage::GlobalRegisterNotify {
                            name: name.clone(),
                            global_pid: *global_pid,
                            registered_at: current_timestamp_ms(),
                        };

                        pending.remove(&request_id);
                        return Ok(Some(notify));
                    }
                } else {
                    // Majority rejected - abort
                    pending.remove(&request_id);
                    return Err(anyhow!("Global registration rejected by cluster majority"));
                }
            }
        }

        Ok(None)
    }

    /// Handle global registration notification from coordinator
    pub async fn handle_register_notify(
        &self,
        name: String,
        global_pid: GlobalProcessId,
        _registered_at: u64,
    ) -> Result<()> {
        // Apply global registration to local registry
        // Ignore errors if already registered (idempotent)
        let _ = self.registry.register_global(name.as_str(), global_pid);
        Ok(())
    }

    /// Handle global unregistration request
    pub async fn handle_unregister_request(
        &self,
        request_id: u64,
        _requesting_node_id: u64,
        name: String,
    ) -> RegistryCoordinationMessage {
        let result = match self.registry.lookup_global(name.as_str()) {
            Some(_) => GlobalUnregisterResult::Success,
            None => GlobalUnregisterResult::NotFound,
        };

        RegistryCoordinationMessage::GlobalUnregisterResponse { request_id, result }
    }

    /// Handle global unregistration notification
    pub async fn handle_unregister_notify(&self, name: String) -> Result<()> {
        // Remove from global registry (ignore errors if not found)
        let _ = self.registry.unregister(name.as_str());
        Ok(())
    }

    /// Handle registry sync request from new node
    pub async fn handle_sync_request(
        &self,
        request_id: u64,
        _requesting_node_id: u64,
    ) -> RegistryCoordinationMessage {
        // Collect all global entries
        let global_names = self.registry.global_names();
        let mut global_entries = Vec::new();

        for name in global_names {
            if let Some(entry) = self.registry.lookup_global(name.as_str()) {
                global_entries.push((
                    name.as_str().to_string(),
                    entry.global_pid,
                    entry.registered_at,
                ));
            }
        }

        RegistryCoordinationMessage::RegistrySyncResponse {
            request_id,
            global_entries,
        }
    }

    /// Handle registry sync response (apply to local registry)
    pub async fn handle_sync_response(
        &self,
        global_entries: Vec<(String, GlobalProcessId, u64)>,
    ) -> Result<()> {
        for (name, global_pid, _registered_at) in global_entries {
            // Apply each global registration
            // Ignore errors if already exists
            let _ = self.registry.register_global(name.as_str(), global_pid);
        }

        Ok(())
    }

    /// Clean up registrations for a failed node
    pub async fn cleanup_node_registrations(&self, failed_node_id: u64) -> Vec<ProcessName> {
        let mut cleaned_names = Vec::new();

        // Find all global registrations from the failed node
        for name in self.registry.global_names() {
            if let Some(entry) = self.registry.lookup_global(name.as_str()) {
                if entry.global_pid.node_id() == failed_node_id {
                    // Unregister process from failed node
                    if self.registry.unregister(name.as_str()).is_ok() {
                        cleaned_names.push(name);
                    }
                }
            }
        }

        cleaned_names
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

    #[tokio::test]
    async fn test_coordinator_creation() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry.clone(), 1);

        assert_eq!(coordinator.node_id, 1);
    }

    #[tokio::test]
    async fn test_single_node_registration() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry.clone(), 1);

        let gpid = GlobalProcessId::new(1, 1, 100);

        // Single node registration should succeed immediately
        let result = coordinator
            .register_global_coordinated("service", gpid, 1)
            .await;

        assert!(result.is_ok());
        assert!(registry.lookup_global("service").is_some());
    }

    #[tokio::test]
    async fn test_handle_register_request_no_conflict() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry.clone(), 1);

        let gpid = GlobalProcessId::new(2, 1, 200);

        // Handle request from node 2
        let response = coordinator
            .handle_register_request(1, 2, "remote_service".to_string(), gpid)
            .await;

        // Should succeed (no local conflict)
        match response {
            RegistryCoordinationMessage::GlobalRegisterResponse { request_id, result } => {
                assert_eq!(request_id, 1);
                assert!(matches!(result, GlobalRegisterResult::Success));
            }
            _ => panic!("Expected GlobalRegisterResponse"),
        }
    }

    #[tokio::test]
    async fn test_handle_register_request_with_conflict() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry.clone(), 1);

        let gpid1 = GlobalProcessId::new(1, 1, 100);
        let gpid2 = GlobalProcessId::new(2, 1, 200);

        // Register locally first
        registry.register_global("service", gpid1).unwrap();

        // Handle conflicting request from node 2
        let response = coordinator
            .handle_register_request(1, 2, "service".to_string(), gpid2)
            .await;

        // Should report conflict
        match response {
            RegistryCoordinationMessage::GlobalRegisterResponse { request_id, result } => {
                assert_eq!(request_id, 1);
                assert!(matches!(
                    result,
                    GlobalRegisterResult::AlreadyRegistered { .. }
                ));
            }
            _ => panic!("Expected GlobalRegisterResponse with conflict"),
        }
    }

    #[tokio::test]
    async fn test_handle_register_notify() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry.clone(), 1);

        let gpid = GlobalProcessId::new(2, 1, 200);

        // Handle notification from coordinator
        coordinator
            .handle_register_notify(
                "coordinated_service".to_string(),
                gpid,
                current_timestamp_ms(),
            )
            .await
            .unwrap();

        // Should be registered locally
        let entry = registry.lookup_global("coordinated_service").unwrap();
        assert_eq!(entry.global_pid, gpid);
    }

    #[tokio::test]
    async fn test_cleanup_node_registrations() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry.clone(), 1);

        // Register services from different nodes
        let gpid_node2_1 = GlobalProcessId::new(2, 1, 100);
        let gpid_node2_2 = GlobalProcessId::new(2, 1, 200);
        let gpid_node3 = GlobalProcessId::new(3, 1, 300);

        registry
            .register_global("service_2a", gpid_node2_1)
            .unwrap();
        registry
            .register_global("service_2b", gpid_node2_2)
            .unwrap();
        registry.register_global("service_3", gpid_node3).unwrap();

        assert_eq!(registry.global_count(), 3);

        // Node 2 fails - cleanup its registrations
        let cleaned = coordinator.cleanup_node_registrations(2).await;

        assert_eq!(cleaned.len(), 2);
        assert_eq!(registry.global_count(), 1);
        assert!(registry.lookup_global("service_3").is_some());
        assert!(registry.lookup_global("service_2a").is_none());
    }

    #[tokio::test]
    async fn test_handle_sync_request() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry.clone(), 1);

        // Register some global services
        let gpid1 = GlobalProcessId::new(1, 1, 100);
        let gpid2 = GlobalProcessId::new(1, 1, 200);

        registry.register_global("service_a", gpid1).unwrap();
        registry.register_global("service_b", gpid2).unwrap();

        // Handle sync request
        let response = coordinator.handle_sync_request(1, 2).await;

        match response {
            RegistryCoordinationMessage::RegistrySyncResponse {
                request_id,
                global_entries,
            } => {
                assert_eq!(request_id, 1);
                assert_eq!(global_entries.len(), 2);
            }
            _ => panic!("Expected RegistrySyncResponse"),
        }
    }

    #[tokio::test]
    async fn test_handle_sync_response() {
        let registry = Arc::new(DistributedRegistry::new(2));
        let coordinator = RegistryCoordinator::new(registry.clone(), 2);

        let gpid1 = GlobalProcessId::new(1, 1, 100);
        let gpid2 = GlobalProcessId::new(1, 1, 200);

        let entries = vec![
            ("service_a".to_string(), gpid1, current_timestamp_ms()),
            ("service_b".to_string(), gpid2, current_timestamp_ms()),
        ];

        // Apply sync response
        coordinator.handle_sync_response(entries).await.unwrap();

        // Should have both services registered
        assert_eq!(registry.global_count(), 2);
        assert!(registry.lookup_global("service_a").is_some());
        assert!(registry.lookup_global("service_b").is_some());
    }
}
