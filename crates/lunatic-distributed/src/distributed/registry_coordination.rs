use super::client::{Client, Inner};
use super::global_process_id::GlobalProcessId;
use super::registry::{DistributedRegistry, ProcessName};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Mutex, Notify};

const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(2);
const RETRY_INTERVAL: Duration = Duration::from_millis(100);
const SYNC_TIMEOUT: Duration = Duration::from_secs(2);
const NETWORK_SEND_TIMEOUT: Duration = Duration::from_millis(500);

type RegistrySnapshot = Vec<(String, GlobalProcessId, u64)>;

/// Messages exchanged over the authenticated node-to-node QUIC transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegistryCoordinationMessage {
    /// A caller proposes a registration to the elected coordinator.
    GlobalRegisterRequest {
        request_id: u64,
        requesting_node_id: u64,
        name: String,
        global_pid: GlobalProcessId,
    },
    /// The coordinator asks a follower to validate a proposal.
    GlobalRegisterPrepare {
        request_id: u64,
        requesting_node_id: u64,
        name: String,
        global_pid: GlobalProcessId,
    },
    /// A follower's vote on a proposal.
    GlobalRegisterResponse {
        request_id: u64,
        requesting_node_id: u64,
        responding_node_id: u64,
        result: GlobalRegisterResult,
    },
    /// Final result returned by the coordinator to the original caller.
    GlobalRegisterDecision {
        request_id: u64,
        result: GlobalRegisterResult,
    },
    /// A committed value broadcast by the coordinator.
    GlobalRegisterNotify {
        name: String,
        global_pid: GlobalProcessId,
        registered_at: u64,
    },
    GlobalUnregisterRequest {
        request_id: u64,
        requesting_node_id: u64,
        name: String,
    },
    GlobalUnregisterResponse {
        request_id: u64,
        result: GlobalUnregisterResult,
    },
    GlobalUnregisterNotify {
        name: String,
    },
    RegistrySyncRequest {
        request_id: u64,
        requesting_node_id: u64,
    },
    RegistrySyncResponse {
        request_id: u64,
        global_entries: RegistrySnapshot,
    },
    RegistryHeartbeat {
        node_id: u64,
        timestamp: u64,
    },
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

struct PendingVotes {
    responses: HashMap<u64, GlobalRegisterResult>,
    notify: Arc<Notify>,
}

/// Coordinates cluster-wide process names through a stable leader and quorum.
///
/// The lowest node ID in the current control-plane membership is the coordinator.
/// It serializes proposals per name and commits only after a strict majority has
/// accepted. Followers never expose a prepared value before the commit notify.
pub struct RegistryCoordinator {
    registry: Arc<DistributedRegistry>,
    node_id: u64,
    client: StdMutex<Option<Weak<Inner>>>,
    next_request_id: AtomicU64,
    pending_votes: Mutex<HashMap<(u64, u64), PendingVotes>>,
    pending_callers: Mutex<HashMap<u64, oneshot::Sender<GlobalRegisterResult>>>,
    pending_syncs: Mutex<HashMap<u64, oneshot::Sender<RegistrySnapshot>>>,
    name_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl RegistryCoordinator {
    pub fn new(registry: Arc<DistributedRegistry>, node_id: u64) -> Self {
        Self {
            registry,
            node_id,
            client: StdMutex::new(None),
            next_request_id: AtomicU64::new(1),
            pending_votes: Mutex::new(HashMap::new()),
            pending_callers: Mutex::new(HashMap::new()),
            pending_syncs: Mutex::new(HashMap::new()),
            name_locks: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn attach_client(&self, client: &Client) {
        *self.client.lock().expect("registry client lock poisoned") =
            Some(Arc::downgrade(&client.inner));
    }

    fn client(&self) -> Result<Client> {
        let inner = self
            .client
            .lock()
            .map_err(|_| anyhow!("Registry client lock poisoned"))?
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| anyhow!("Registry coordinator is not attached to a node client"))?;
        Ok(Client {
            node_id: super::client::NodeId(self.node_id),
            inner,
        })
    }

    fn members(&self) -> Result<Vec<u64>> {
        let mut members = self.client()?.registry_node_ids();
        if !members.contains(&self.node_id) {
            members.push(self.node_id);
        }
        members.sort_unstable();
        members.dedup();
        Ok(members)
    }

    fn leader(&self) -> Result<u64> {
        self.members()?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Registry cluster has no members"))
    }

    async fn send_to(&self, node_id: u64, message: RegistryCoordinationMessage) -> Result<()> {
        match tokio::time::timeout(
            NETWORK_SEND_TIMEOUT,
            self.client()?.send_registry_coordination(node_id, message),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(anyhow!(
                "Timed out sending registry coordination message to node {node_id}"
            )),
        }
    }

    /// Register a name through the cluster quorum.
    ///
    /// `node_count` remains in the API for compatibility; attached clients use
    /// the live control-plane member list as the source of truth.
    pub async fn register_global_coordinated(
        &self,
        name: impl Into<ProcessName>,
        global_pid: GlobalProcessId,
        node_count: usize,
    ) -> Result<()> {
        let name = name.into();
        let name_string = name.as_str().to_string();
        if let Some(existing) = self.registry.lookup_global(name.as_str()) {
            return Err(already_registered_error(&name_string, existing.global_pid));
        }

        if node_count <= 1
            && self
                .client
                .lock()
                .expect("registry client lock poisoned")
                .is_none()
        {
            return self.registry.register_global(name, global_pid);
        }

        let members = self.members()?;
        if members.len() <= 1 {
            return self.registry.register_global(name, global_pid);
        }
        let leader = members[0];
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);

        let result = if leader == self.node_id {
            self.coordinate_registration(request_id, self.node_id, name_string.clone(), global_pid)
                .await
        } else {
            let (sender, receiver) = oneshot::channel();
            self.pending_callers.lock().await.insert(request_id, sender);
            let request = RegistryCoordinationMessage::GlobalRegisterRequest {
                request_id,
                requesting_node_id: self.node_id,
                name: name_string.clone(),
                global_pid,
            };
            if let Err(error) = self.send_to(leader, request).await {
                self.pending_callers.lock().await.remove(&request_id);
                return Err(error.context("Failed to send global registration proposal"));
            }
            match tokio::time::timeout(REGISTRATION_TIMEOUT + Duration::from_secs(1), receiver)
                .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => {
                    return Err(anyhow!(
                        "Global registration coordinator dropped its decision"
                    ))
                }
                Err(_) => {
                    self.pending_callers.lock().await.remove(&request_id);
                    return Err(anyhow!(
                        "Global registration timed out waiting for coordinator"
                    ));
                }
            }
        };

        result_to_anyhow(&name_string, result)
    }

    async fn coordinate_registration(
        &self,
        request_id: u64,
        requesting_node_id: u64,
        name: String,
        global_pid: GlobalProcessId,
    ) -> GlobalRegisterResult {
        let name_lock = {
            let mut locks = self.name_locks.lock().await;
            locks
                .entry(name.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = name_lock.lock().await;

        if let Some(existing) = self.registry.lookup_global(name.as_str()) {
            return GlobalRegisterResult::AlreadyRegistered {
                existing_gpid: existing.global_pid,
                registered_at: existing.registered_at,
            };
        }

        let members = match self.members() {
            Ok(members) => members,
            Err(_) => {
                return GlobalRegisterResult::ConflictDetected {
                    conflicting_node_id: self.node_id,
                }
            }
        };
        let required = (members.len() / 2) + 1;
        let vote_key = (requesting_node_id, request_id);
        let wake = Arc::new(Notify::new());
        let mut responses = HashMap::new();
        responses.insert(self.node_id, GlobalRegisterResult::Success);
        self.pending_votes.lock().await.insert(
            vote_key,
            PendingVotes {
                responses,
                notify: wake.clone(),
            },
        );

        let prepare = RegistryCoordinationMessage::GlobalRegisterPrepare {
            request_id,
            requesting_node_id,
            name: name.clone(),
            global_pid,
        };
        let deadline = Instant::now() + REGISTRATION_TIMEOUT;

        loop {
            let (successes, response_count, first_rejection, missing) = {
                let pending = self.pending_votes.lock().await;
                let Some(votes) = pending.get(&vote_key) else {
                    return GlobalRegisterResult::ConflictDetected {
                        conflicting_node_id: self.node_id,
                    };
                };
                let successes = votes
                    .responses
                    .values()
                    .filter(|result| matches!(result, GlobalRegisterResult::Success))
                    .count();
                let first_rejection = votes
                    .responses
                    .values()
                    .find(|result| !matches!(result, GlobalRegisterResult::Success))
                    .cloned();
                let missing = members
                    .iter()
                    .copied()
                    .filter(|node_id| !votes.responses.contains_key(node_id))
                    .collect::<Vec<_>>();
                (successes, votes.responses.len(), first_rejection, missing)
            };

            if successes >= required {
                self.pending_votes.lock().await.remove(&vote_key);
                let registered_at = current_timestamp_ms();
                self.registry
                    .apply_global(name.clone(), global_pid, registered_at);
                let notify = RegistryCoordinationMessage::GlobalRegisterNotify {
                    name,
                    global_pid,
                    registered_at,
                };
                for node_id in members.into_iter().filter(|id| *id != self.node_id) {
                    if let Err(error) = self.send_to(node_id, notify.clone()).await {
                        log::debug!("Failed to notify node {node_id} of registry commit: {error}");
                    }
                }
                return GlobalRegisterResult::Success;
            }

            if response_count == members.len() || Instant::now() >= deadline {
                self.pending_votes.lock().await.remove(&vote_key);
                return first_rejection.unwrap_or(GlobalRegisterResult::ConflictDetected {
                    conflicting_node_id: self.node_id,
                });
            }

            for node_id in missing {
                if let Err(error) = self.send_to(node_id, prepare.clone()).await {
                    log::trace!("Registry prepare to node {node_id} failed: {error}");
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait = remaining.min(RETRY_INTERVAL);
            let _ = tokio::time::timeout(wait, wake.notified()).await;
        }
    }

    /// Dispatch one registry protocol message received from QUIC.
    pub async fn handle_message(
        &self,
        source_node_id: u64,
        message: RegistryCoordinationMessage,
    ) -> Result<()> {
        match message {
            RegistryCoordinationMessage::GlobalRegisterRequest {
                request_id,
                requesting_node_id,
                name,
                global_pid,
            } => {
                if source_node_id != requesting_node_id {
                    return Err(anyhow!(
                        "Registry request source did not match requesting node"
                    ));
                }
                let result = if self.leader()? == self.node_id {
                    self.coordinate_registration(request_id, requesting_node_id, name, global_pid)
                        .await
                } else {
                    GlobalRegisterResult::ConflictDetected {
                        conflicting_node_id: self.node_id,
                    }
                };
                self.send_to(
                    requesting_node_id,
                    RegistryCoordinationMessage::GlobalRegisterDecision { request_id, result },
                )
                .await?;
            }
            RegistryCoordinationMessage::GlobalRegisterPrepare {
                request_id,
                requesting_node_id,
                name,
                global_pid,
            } => {
                if source_node_id != self.leader()? {
                    return Err(anyhow!(
                        "Registry prepare did not come from the coordinator"
                    ));
                }
                let response = self
                    .handle_register_request(request_id, requesting_node_id, name, global_pid)
                    .await;
                self.send_to(source_node_id, response).await?;
            }
            RegistryCoordinationMessage::GlobalRegisterResponse {
                request_id,
                requesting_node_id,
                responding_node_id,
                result,
            } => {
                if source_node_id != responding_node_id {
                    return Err(anyhow!(
                        "Registry response source did not match responding node"
                    ));
                }
                self.record_vote(requesting_node_id, request_id, responding_node_id, result)
                    .await;
            }
            RegistryCoordinationMessage::GlobalRegisterDecision { request_id, result } => {
                if source_node_id != self.leader()? {
                    return Err(anyhow!(
                        "Registry decision did not come from the coordinator"
                    ));
                }
                if let Some(sender) = self.pending_callers.lock().await.remove(&request_id) {
                    let _ = sender.send(result);
                }
            }
            RegistryCoordinationMessage::GlobalRegisterNotify {
                name,
                global_pid,
                registered_at,
            } => {
                if source_node_id != self.leader()? {
                    return Err(anyhow!("Registry commit did not come from the coordinator"));
                }
                self.handle_register_notify(name, global_pid, registered_at)
                    .await?;
            }
            RegistryCoordinationMessage::GlobalUnregisterRequest {
                request_id,
                requesting_node_id,
                name,
            } => {
                let response = self
                    .handle_unregister_request(request_id, requesting_node_id, name)
                    .await;
                self.send_to(requesting_node_id, response).await?;
            }
            RegistryCoordinationMessage::GlobalUnregisterResponse { .. } => {}
            RegistryCoordinationMessage::GlobalUnregisterNotify { name } => {
                self.handle_unregister_notify(name).await?;
            }
            RegistryCoordinationMessage::RegistrySyncRequest {
                request_id,
                requesting_node_id,
            } => {
                if source_node_id != requesting_node_id || self.leader()? != self.node_id {
                    return Err(anyhow!("Invalid registry synchronization request"));
                }
                let response = self
                    .handle_sync_request(request_id, requesting_node_id)
                    .await;
                self.send_to(requesting_node_id, response).await?;
            }
            RegistryCoordinationMessage::RegistrySyncResponse {
                request_id,
                global_entries,
            } => {
                if source_node_id != self.leader()? {
                    return Err(anyhow!(
                        "Registry snapshot did not come from the coordinator"
                    ));
                }
                if let Some(sender) = self.pending_syncs.lock().await.remove(&request_id) {
                    let _ = sender.send(global_entries);
                }
            }
            RegistryCoordinationMessage::RegistryHeartbeat { .. } => {}
        }
        Ok(())
    }

    async fn record_vote(
        &self,
        requesting_node_id: u64,
        request_id: u64,
        responding_node_id: u64,
        result: GlobalRegisterResult,
    ) {
        let mut pending = self.pending_votes.lock().await;
        if let Some(votes) = pending.get_mut(&(requesting_node_id, request_id)) {
            votes.responses.entry(responding_node_id).or_insert(result);
            votes.notify.notify_one();
        }
    }

    /// Pull the coordinator's authoritative snapshot after a join or partition.
    pub async fn synchronize_from_coordinator(&self) -> Result<()> {
        let leader = self.leader()?;
        if leader == self.node_id {
            return Ok(());
        }
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending_syncs.lock().await.insert(request_id, sender);
        let request = RegistryCoordinationMessage::RegistrySyncRequest {
            request_id,
            requesting_node_id: self.node_id,
        };
        if let Err(error) = self.send_to(leader, request).await {
            self.pending_syncs.lock().await.remove(&request_id);
            return Err(error.context("Failed to request registry synchronization"));
        }
        let snapshot = match tokio::time::timeout(SYNC_TIMEOUT, receiver).await {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(_)) => return Err(anyhow!("Registry synchronization response was dropped")),
            Err(_) => {
                self.pending_syncs.lock().await.remove(&request_id);
                return Err(anyhow!("Registry synchronization timed out"));
            }
        };
        self.handle_sync_response(snapshot).await
    }

    pub async fn handle_register_request(
        &self,
        request_id: u64,
        requesting_node_id: u64,
        name: String,
        global_pid: GlobalProcessId,
    ) -> RegistryCoordinationMessage {
        let result = match self.registry.lookup_global(name.as_str()) {
            Some(existing) if existing.global_pid != global_pid => {
                GlobalRegisterResult::AlreadyRegistered {
                    existing_gpid: existing.global_pid,
                    registered_at: existing.registered_at,
                }
            }
            _ => GlobalRegisterResult::Success,
        };
        RegistryCoordinationMessage::GlobalRegisterResponse {
            request_id,
            requesting_node_id,
            responding_node_id: self.node_id,
            result,
        }
    }

    pub async fn handle_register_response(
        &self,
        request_id: u64,
        result: GlobalRegisterResult,
    ) -> Result<Option<RegistryCoordinationMessage>> {
        self.record_vote(self.node_id, request_id, self.node_id, result)
            .await;
        Ok(None)
    }

    pub async fn handle_register_notify(
        &self,
        name: String,
        global_pid: GlobalProcessId,
        registered_at: u64,
    ) -> Result<()> {
        self.registry.apply_global(name, global_pid, registered_at);
        Ok(())
    }

    pub async fn handle_unregister_request(
        &self,
        request_id: u64,
        _requesting_node_id: u64,
        name: String,
    ) -> RegistryCoordinationMessage {
        let result = if self.registry.lookup_global(name.as_str()).is_some() {
            GlobalUnregisterResult::Success
        } else {
            GlobalUnregisterResult::NotFound
        };
        RegistryCoordinationMessage::GlobalUnregisterResponse { request_id, result }
    }

    pub async fn handle_unregister_notify(&self, name: String) -> Result<()> {
        let _ = self.registry.unregister(name);
        Ok(())
    }

    pub async fn handle_sync_request(
        &self,
        request_id: u64,
        _requesting_node_id: u64,
    ) -> RegistryCoordinationMessage {
        let global_entries = self
            .registry
            .global_names()
            .into_iter()
            .filter_map(|name| {
                self.registry.lookup_global(name.as_str()).map(|entry| {
                    (
                        name.as_str().to_string(),
                        entry.global_pid,
                        entry.registered_at,
                    )
                })
            })
            .collect();
        RegistryCoordinationMessage::RegistrySyncResponse {
            request_id,
            global_entries,
        }
    }

    pub async fn handle_sync_response(&self, global_entries: RegistrySnapshot) -> Result<()> {
        self.registry.replace_global_snapshot(global_entries);
        Ok(())
    }

    pub async fn cleanup_node_registrations(&self, failed_node_id: u64) -> Vec<ProcessName> {
        let mut cleaned_names = Vec::new();
        for name in self.registry.global_names() {
            if let Some(entry) = self.registry.lookup_global(name.as_str()) {
                if entry.global_pid.node_id() == failed_node_id
                    && self.registry.unregister(name.as_str()).is_ok()
                {
                    cleaned_names.push(name);
                }
            }
        }
        cleaned_names
    }
}

fn result_to_anyhow(name: &str, result: GlobalRegisterResult) -> Result<()> {
    match result {
        GlobalRegisterResult::Success => Ok(()),
        GlobalRegisterResult::AlreadyRegistered { existing_gpid, .. } => {
            Err(already_registered_error(name, existing_gpid))
        }
        GlobalRegisterResult::ConflictDetected {
            conflicting_node_id,
        } => Err(anyhow!(
            "Global registration for '{name}' failed without a quorum; conflict reported by node {conflicting_node_id}"
        )),
    }
}

fn already_registered_error(name: &str, global_pid: GlobalProcessId) -> anyhow::Error {
    anyhow!("Name '{name}' already registered globally with pid {global_pid}")
}

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_node_registration_is_immediate() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry.clone(), 1);
        let gpid = GlobalProcessId::new(1, 1, 100);

        coordinator
            .register_global_coordinated("service", gpid, 1)
            .await
            .unwrap();

        assert_eq!(registry.lookup_global("service").unwrap().global_pid, gpid);
    }

    #[tokio::test]
    async fn committed_notification_replaces_stale_partition_state() {
        let registry = Arc::new(DistributedRegistry::new(2));
        let coordinator = RegistryCoordinator::new(registry.clone(), 2);
        let stale = GlobalProcessId::new(2, 1, 100);
        let committed = GlobalProcessId::new(1, 1, 200);
        registry.register_global("service", stale).unwrap();

        coordinator
            .handle_register_notify("service".into(), committed, 42)
            .await
            .unwrap();

        let entry = registry.lookup_global("service").unwrap();
        assert_eq!(entry.global_pid, committed);
        assert_eq!(entry.registered_at, 42);
    }

    #[tokio::test]
    async fn authoritative_sync_removes_stale_entries() {
        let registry = Arc::new(DistributedRegistry::new(2));
        let coordinator = RegistryCoordinator::new(registry.clone(), 2);
        registry
            .register_global("stale", GlobalProcessId::new(2, 1, 100))
            .unwrap();
        let current = GlobalProcessId::new(1, 1, 200);

        coordinator
            .handle_sync_response(vec![("current".into(), current, 7)])
            .await
            .unwrap();

        assert!(registry.lookup_global("stale").is_none());
        assert_eq!(
            registry.lookup_global("current").unwrap().global_pid,
            current
        );
    }
}
