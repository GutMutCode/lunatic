use super::client::{Client, Inner, OutboundSaturationError};
use super::global_process_id::GlobalProcessId;
use super::registry::{DistributedRegistry, ProcessName, RegistryCapacityError, RegistryLimits};
use anyhow::{anyhow, Context, Result};
use lunatic_common_api::{
    emit_audit_event, AuditAction, AuditEvent, AuditEventV1, AuditReason, AuditResult,
    AuditSubject, AuditTarget, AuditTargetKind, SensitiveData,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Mutex, Notify};

use crate::quic::VerifiedNodeId;

const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(2);
const RETRY_INTERVAL: Duration = Duration::from_millis(100);
const SYNC_TIMEOUT: Duration = Duration::from_secs(2);
const NETWORK_SEND_TIMEOUT: Duration = Duration::from_secs(1);
const PREPARE_SEND_TIMEOUT: Duration = Duration::from_millis(500);

type RegistrySnapshot = Vec<(String, GlobalProcessId, u64)>;

#[derive(Debug)]
struct RegistryConflictError(String);

impl fmt::Display for RegistryConflictError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RegistryConflictError {}

#[derive(Debug)]
struct RegistryTimeoutError(String);

impl fmt::Display for RegistryTimeoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RegistryTimeoutError {}

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
        expected_global_pid: GlobalProcessId,
    },
    GlobalUnregisterResponse {
        request_id: u64,
        result: GlobalUnregisterResult,
    },
    GlobalUnregisterNotify {
        name: String,
        expected_global_pid: GlobalProcessId,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GlobalRegisterResult {
    Success,
    AlreadyRegistered {
        existing_gpid: GlobalProcessId,
        registered_at: u64,
    },
    ConflictDetected {
        conflicting_node_id: u64,
    },
    ResourceExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GlobalUnregisterResult {
    Success,
    NotFound,
    OwnerChanged,
    ResourceExhausted,
}

struct PendingBudget {
    max: usize,
    used: AtomicUsize,
}

impl PendingBudget {
    fn new(max: usize) -> Self {
        Self {
            max,
            used: AtomicUsize::new(0),
        }
    }

    fn try_acquire(self: &Arc<Self>) -> std::result::Result<PendingLease, RegistryCapacityError> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used.checked_add(1).ok_or_else(|| {
                RegistryCapacityError::new("Registry pending-response accounting overflow")
            })?;
            if next > self.max {
                return Err(RegistryCapacityError::new(format!(
                    "Registry pending-response limit reached (max={})",
                    self.max
                )));
            }
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Ok(PendingLease {
                        budget: self.clone(),
                    })
                }
                Err(actual) => used = actual,
            }
        }
    }

    #[cfg(test)]
    fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

struct PendingLease {
    budget: Arc<PendingBudget>,
}

impl Drop for PendingLease {
    fn drop(&mut self) {
        let previous = self.budget.used.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "registry pending budget underflow");
    }
}

struct PendingVoteResponse {
    result: GlobalRegisterResult,
    _lease: PendingLease,
}

struct PendingVotes {
    token: u64,
    allowed_members: HashSet<u64>,
    responses: HashMap<u64, PendingVoteResponse>,
    capacity_exhausted: bool,
    notify: Arc<Notify>,
    _lease: PendingLease,
}

struct PendingCaller {
    token: u64,
    coordinator_node_id: u64,
    sender: oneshot::Sender<GlobalRegisterResult>,
    _lease: PendingLease,
}

struct PendingSync {
    token: u64,
    coordinator_node_id: u64,
    sender: oneshot::Sender<Result<()>>,
    _lease: PendingLease,
}

struct PendingUnregister {
    token: u64,
    coordinator_node_id: u64,
    sender: oneshot::Sender<GlobalUnregisterResult>,
    _lease: PendingLease,
}

type VoteKey = (u64, u64);
type PendingVoteMap = Arc<StdMutex<HashMap<VoteKey, PendingVotes>>>;
type PendingCallerMap = Arc<StdMutex<HashMap<u64, PendingCaller>>>;
type PendingSyncMap = Arc<StdMutex<HashMap<u64, PendingSync>>>;
type PendingUnregisterMap = Arc<StdMutex<HashMap<u64, PendingUnregister>>>;

struct PendingVoteGuard {
    map: PendingVoteMap,
    key: VoteKey,
    token: u64,
}

impl Drop for PendingVoteGuard {
    fn drop(&mut self) {
        let mut pending = lock_unpoisoned(&self.map);
        if pending
            .get(&self.key)
            .map(|entry| entry.token == self.token)
            .unwrap_or(false)
        {
            pending.remove(&self.key);
        }
    }
}

struct PendingCallerGuard {
    map: PendingCallerMap,
    request_id: u64,
    token: u64,
}

impl Drop for PendingCallerGuard {
    fn drop(&mut self) {
        let mut pending = lock_unpoisoned(&self.map);
        if pending
            .get(&self.request_id)
            .map(|entry| entry.token == self.token)
            .unwrap_or(false)
        {
            pending.remove(&self.request_id);
        }
    }
}

struct PendingSyncGuard {
    map: PendingSyncMap,
    request_id: u64,
    token: u64,
}

impl Drop for PendingSyncGuard {
    fn drop(&mut self) {
        let mut pending = lock_unpoisoned(&self.map);
        if pending
            .get(&self.request_id)
            .map(|entry| entry.token == self.token)
            .unwrap_or(false)
        {
            pending.remove(&self.request_id);
        }
    }
}

struct PendingUnregisterGuard {
    map: PendingUnregisterMap,
    request_id: u64,
    token: u64,
}

impl Drop for PendingUnregisterGuard {
    fn drop(&mut self) {
        let mut pending = lock_unpoisoned(&self.map);
        if pending
            .get(&self.request_id)
            .map(|entry| entry.token == self.token)
            .unwrap_or(false)
        {
            pending.remove(&self.request_id);
        }
    }
}

fn lock_unpoisoned<T>(mutex: &StdMutex<T>) -> StdMutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct PendingRegistryAudit {
    subject: AuditSubject,
    target: AuditTarget,
    emitted: bool,
}

impl PendingRegistryAudit {
    fn new(node_id: u64, global_pid: GlobalProcessId) -> Self {
        Self {
            subject: AuditSubject::new().with_node_id(node_id),
            target: AuditTarget::new(AuditTargetKind::DistributedRegistry)
                .with_node_id(global_pid.node_id())
                .with_environment_id(global_pid.environment_id())
                .with_process_id(global_pid.process_id())
                .with_sensitive_data(SensitiveData::Redacted),
            emitted: false,
        }
    }

    fn finish(mut self, result: AuditResult, reason: AuditReason) {
        self.emit(result, reason);
    }

    fn emit(&mut self, result: AuditResult, reason: AuditReason) {
        emit_audit_event(AuditEventV1::new(
            AuditEvent::DistributedRegistryChange,
            AuditAction::Register,
            result,
            reason,
            self.subject,
            self.target,
        ));
        self.emitted = true;
    }
}

fn classify_registration_error(error: &anyhow::Error) -> (AuditResult, AuditReason) {
    if error.downcast_ref::<RegistryConflictError>().is_some() {
        (AuditResult::Denied, AuditReason::Conflict)
    } else if error.downcast_ref::<RegistryCapacityError>().is_some()
        || error.downcast_ref::<OutboundSaturationError>().is_some()
    {
        (AuditResult::Denied, AuditReason::QuotaExceeded)
    } else if error.downcast_ref::<RegistryTimeoutError>().is_some() {
        (AuditResult::Failed, AuditReason::TimedOut)
    } else {
        (AuditResult::Failed, AuditReason::RuntimeFailure)
    }
}

impl Drop for PendingRegistryAudit {
    fn drop(&mut self) {
        if !self.emitted {
            self.emit(AuditResult::Cancelled, AuditReason::Cancelled);
        }
    }
}

/// Coordinates cluster-wide process names through a stable leader and quorum.
///
/// The lowest node ID in the current control-plane membership is the coordinator.
/// It serializes proposals on a fixed set of name stripes and commits only after
/// a strict majority has accepted. Followers never expose a prepared value before
/// the commit notification.
pub struct RegistryCoordinator {
    registry: Arc<DistributedRegistry>,
    limits: RegistryLimits,
    node_id: u64,
    client: StdMutex<Option<Weak<Inner>>>,
    next_request_id: AtomicU64,
    next_pending_token: AtomicU64,
    topology_epoch: AtomicU64,
    topology_fence: StdMutex<()>,
    pending_budget: Arc<PendingBudget>,
    pending_votes: PendingVoteMap,
    pending_callers: PendingCallerMap,
    pending_syncs: PendingSyncMap,
    pending_unregisters: PendingUnregisterMap,
    name_locks: Vec<Mutex<()>>,
}

impl RegistryCoordinator {
    pub fn new(registry: Arc<DistributedRegistry>, node_id: u64) -> Self {
        let limits = registry.limits();
        let mut name_locks = Vec::new();
        if name_locks.try_reserve_exact(limits.max_name_locks).is_ok() {
            name_locks.resize_with(limits.max_name_locks, || Mutex::new(()));
        }
        Self {
            registry,
            limits,
            node_id,
            client: StdMutex::new(None),
            next_request_id: AtomicU64::new(1),
            next_pending_token: AtomicU64::new(1),
            topology_epoch: AtomicU64::new(0),
            topology_fence: StdMutex::new(()),
            pending_budget: Arc::new(PendingBudget::new(limits.max_pending_responses)),
            pending_votes: Arc::new(StdMutex::new(HashMap::new())),
            pending_callers: Arc::new(StdMutex::new(HashMap::new())),
            pending_syncs: Arc::new(StdMutex::new(HashMap::new())),
            pending_unregisters: Arc::new(StdMutex::new(HashMap::new())),
            name_locks,
        }
    }

    pub(crate) fn attach_client(&self, client: &Client) {
        *lock_unpoisoned(&self.client) = Some(Arc::downgrade(&client.inner));
    }

    fn client(&self) -> Result<Client> {
        let inner = lock_unpoisoned(&self.client)
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| anyhow!("Registry coordinator is not attached to a node client"))?;
        Ok(Client {
            node_id: super::client::NodeId(self.node_id),
            inner,
        })
    }

    fn has_client(&self) -> bool {
        lock_unpoisoned(&self.client)
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some()
    }

    fn members(&self) -> Result<Vec<u64>> {
        let mut members = self.client()?.registry_node_ids();
        if !members.contains(&self.node_id) {
            members.push(self.node_id);
        }
        members.sort_unstable();
        members.dedup();
        if members.len() > self.limits.max_topology_nodes {
            return Err(RegistryCapacityError::new(format!(
                "Registry topology contains {} nodes, exceeding the limit of {}",
                members.len(),
                self.limits.max_topology_nodes
            ))
            .into());
        }
        Ok(members)
    }

    fn leader(&self) -> Result<u64> {
        self.members()?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Registry cluster has no members"))
    }

    fn name_lock_index(&self, name: &str) -> Result<usize> {
        if self.name_locks.is_empty() {
            return Err(RegistryCapacityError::new(
                "Registry name-lock stripe limit is zero or the configured stripes could not be allocated",
            )
            .into());
        }
        Ok((stable_name_hash(name.as_bytes()) as usize) % self.name_locks.len())
    }

    fn next_token(&self) -> u64 {
        self.next_pending_token.fetch_add(1, Ordering::Relaxed)
    }

    async fn send_to(&self, node_id: u64, message: RegistryCoordinationMessage) -> Result<()> {
        self.send_to_with_timeout(node_id, message, NETWORK_SEND_TIMEOUT)
            .await
    }

    async fn send_to_with_timeout(
        &self,
        node_id: u64,
        message: RegistryCoordinationMessage,
        timeout: Duration,
    ) -> Result<()> {
        match tokio::time::timeout(
            timeout,
            self.client()?.send_registry_coordination(node_id, message),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(RegistryTimeoutError(format!(
                "Timed out sending registry coordination message to node {node_id}"
            ))
            .into()),
        }
    }

    fn insert_pending_caller(
        &self,
        request_id: u64,
        coordinator_node_id: u64,
    ) -> Result<(oneshot::Receiver<GlobalRegisterResult>, PendingCallerGuard)> {
        let lease = self.pending_budget.try_acquire()?;
        let token = self.next_token();
        let (sender, receiver) = oneshot::channel();
        let mut pending = lock_unpoisoned(&self.pending_callers);
        if pending.contains_key(&request_id) {
            return Err(RegistryCapacityError::new(format!(
                "Registry request id {request_id} is already pending"
            ))
            .into());
        }
        pending.insert(
            request_id,
            PendingCaller {
                token,
                coordinator_node_id,
                sender,
                _lease: lease,
            },
        );
        drop(pending);
        Ok((
            receiver,
            PendingCallerGuard {
                map: self.pending_callers.clone(),
                request_id,
                token,
            },
        ))
    }

    fn insert_pending_sync(
        &self,
        request_id: u64,
        coordinator_node_id: u64,
    ) -> Result<(oneshot::Receiver<Result<()>>, PendingSyncGuard)> {
        let lease = self.pending_budget.try_acquire()?;
        let token = self.next_token();
        let (sender, receiver) = oneshot::channel();
        let mut pending = lock_unpoisoned(&self.pending_syncs);
        if pending.contains_key(&request_id) {
            return Err(RegistryCapacityError::new(format!(
                "Registry synchronization request id {request_id} is already pending"
            ))
            .into());
        }
        pending.insert(
            request_id,
            PendingSync {
                token,
                coordinator_node_id,
                sender,
                _lease: lease,
            },
        );
        drop(pending);
        Ok((
            receiver,
            PendingSyncGuard {
                map: self.pending_syncs.clone(),
                request_id,
                token,
            },
        ))
    }

    fn insert_pending_unregister(
        &self,
        request_id: u64,
        coordinator_node_id: u64,
    ) -> Result<(
        oneshot::Receiver<GlobalUnregisterResult>,
        PendingUnregisterGuard,
    )> {
        let lease = self.pending_budget.try_acquire()?;
        let token = self.next_token();
        let (sender, receiver) = oneshot::channel();
        let mut pending = lock_unpoisoned(&self.pending_unregisters);
        if pending.contains_key(&request_id) {
            return Err(RegistryCapacityError::new(format!(
                "Registry unregistration request id {request_id} is already pending"
            ))
            .into());
        }
        pending.insert(
            request_id,
            PendingUnregister {
                token,
                coordinator_node_id,
                sender,
                _lease: lease,
            },
        );
        drop(pending);
        Ok((
            receiver,
            PendingUnregisterGuard {
                map: self.pending_unregisters.clone(),
                request_id,
                token,
            },
        ))
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
        let audit = PendingRegistryAudit::new(self.node_id, global_pid);
        let result = self
            .register_global_coordinated_inner(name.into(), global_pid, node_count)
            .await;
        let (audit_result, reason) = match &result {
            Ok(()) => (AuditResult::Succeeded, AuditReason::Completed),
            Err(error) => classify_registration_error(error),
        };
        audit.finish(audit_result, reason);
        result
    }

    async fn register_global_coordinated_inner(
        &self,
        name: ProcessName,
        global_pid: GlobalProcessId,
        node_count: usize,
    ) -> Result<()> {
        let name_string = name.as_str().to_string();
        self.name_lock_index(&name_string)?;

        if global_pid.node_id() != self.node_id {
            return Err(RegistryConflictError(format!(
                "Global registration owner node {} does not match requesting node {}",
                global_pid.node_id(),
                self.node_id
            ))
            .into());
        }

        if node_count <= 1 && !self.has_client() {
            return self.register_single_node(name, global_pid).await;
        }

        let members = self.members()?;
        if members.len() <= 1 {
            return self.register_single_node(name, global_pid).await;
        }
        let leader = members[0];
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);

        let result = if leader == self.node_id {
            self.coordinate_registration(request_id, self.node_id, name_string.clone(), global_pid)
                .await
        } else {
            let (receiver, _pending_guard) = self.insert_pending_caller(request_id, leader)?;
            let request = RegistryCoordinationMessage::GlobalRegisterRequest {
                request_id,
                requesting_node_id: self.node_id,
                name: name_string.clone(),
                global_pid,
            };
            self.send_to(leader, request)
                .await
                .context("Failed to send global registration proposal")?;
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
                    return Err(RegistryTimeoutError(
                        "Global registration timed out waiting for coordinator".into(),
                    )
                    .into())
                }
            }
        };

        result_to_anyhow(&name_string, result)
    }

    async fn register_single_node(
        &self,
        name: ProcessName,
        global_pid: GlobalProcessId,
    ) -> Result<()> {
        let stripe = self.name_lock_index(name.as_str())?;
        let _name_guard = self.name_locks[stripe].lock().await;
        if let Some(existing) = self.registry.lookup_global(name.as_str()) {
            return Err(already_registered_error(name.as_str(), existing.global_pid));
        }
        self.registry
            .can_register_global(name.as_str(), global_pid)?;
        self.registry
            .apply_global(name, global_pid, current_timestamp_ms())
    }

    async fn coordinate_registration(
        &self,
        request_id: u64,
        requesting_node_id: u64,
        name: String,
        global_pid: GlobalProcessId,
    ) -> GlobalRegisterResult {
        let stripe = match self.name_lock_index(&name) {
            Ok(stripe) => stripe,
            Err(_) => return GlobalRegisterResult::ResourceExhausted,
        };
        let _name_guard = self.name_locks[stripe].lock().await;

        if let Some(existing) = self.registry.lookup_global(name.as_str()) {
            return GlobalRegisterResult::AlreadyRegistered {
                existing_gpid: existing.global_pid,
                registered_at: existing.registered_at,
            };
        }
        if let Err(error) = self.registry.can_register_global(name.as_str(), global_pid) {
            return if error.downcast_ref::<RegistryCapacityError>().is_some() {
                GlobalRegisterResult::ResourceExhausted
            } else {
                GlobalRegisterResult::ConflictDetected {
                    conflicting_node_id: self.node_id,
                }
            };
        }

        let members = match self.members() {
            Ok(members) => members,
            Err(error) if error.downcast_ref::<RegistryCapacityError>().is_some() => {
                return GlobalRegisterResult::ResourceExhausted
            }
            Err(_) => {
                return GlobalRegisterResult::ConflictDetected {
                    conflicting_node_id: self.node_id,
                }
            }
        };
        if !members.contains(&requesting_node_id) || global_pid.node_id() != requesting_node_id {
            return GlobalRegisterResult::ConflictDetected {
                conflicting_node_id: self.node_id,
            };
        }

        let required = (members.len() / 2) + 1;
        let vote_key = (requesting_node_id, request_id);
        let entry_lease = match self.pending_budget.try_acquire() {
            Ok(lease) => lease,
            Err(_) => return GlobalRegisterResult::ResourceExhausted,
        };
        let self_vote_lease = match self.pending_budget.try_acquire() {
            Ok(lease) => lease,
            Err(_) => return GlobalRegisterResult::ResourceExhausted,
        };
        let wake = Arc::new(Notify::new());
        let token = self.next_token();
        let topology_epoch = self.topology_epoch.load(Ordering::Acquire);
        let mut responses = HashMap::new();
        responses.insert(
            self.node_id,
            PendingVoteResponse {
                result: GlobalRegisterResult::Success,
                _lease: self_vote_lease,
            },
        );
        {
            let mut pending = lock_unpoisoned(&self.pending_votes);
            if pending.contains_key(&vote_key) {
                return GlobalRegisterResult::ResourceExhausted;
            }
            pending.insert(
                vote_key,
                PendingVotes {
                    token,
                    allowed_members: members.iter().copied().collect(),
                    responses,
                    capacity_exhausted: false,
                    notify: wake.clone(),
                    _lease: entry_lease,
                },
            );
        }
        let _pending_guard = PendingVoteGuard {
            map: self.pending_votes.clone(),
            key: vote_key,
            token,
        };

        let prepare = RegistryCoordinationMessage::GlobalRegisterPrepare {
            request_id,
            requesting_node_id,
            name: name.clone(),
            global_pid,
        };
        let deadline = Instant::now() + REGISTRATION_TIMEOUT;

        loop {
            let (successes, response_count, first_rejection, missing, capacity_exhausted) = {
                let pending = lock_unpoisoned(&self.pending_votes);
                let Some(votes) = pending.get(&vote_key) else {
                    return GlobalRegisterResult::ConflictDetected {
                        conflicting_node_id: self.node_id,
                    };
                };
                let successes = votes
                    .responses
                    .values()
                    .filter(|response| matches!(response.result, GlobalRegisterResult::Success))
                    .count();
                let first_rejection = votes
                    .responses
                    .values()
                    .find(|response| !matches!(response.result, GlobalRegisterResult::Success))
                    .map(|response| response.result.clone());
                let missing = members
                    .iter()
                    .copied()
                    .filter(|node_id| !votes.responses.contains_key(node_id))
                    .collect::<Vec<_>>();
                (
                    successes,
                    votes.responses.len(),
                    first_rejection,
                    missing,
                    votes.capacity_exhausted,
                )
            };

            if capacity_exhausted {
                return GlobalRegisterResult::ResourceExhausted;
            }

            if successes >= required {
                let registered_at = current_timestamp_ms();
                let apply_result = {
                    let _topology_guard = lock_unpoisoned(&self.topology_fence);
                    if self.topology_epoch.load(Ordering::Acquire) != topology_epoch {
                        None
                    } else {
                        Some(
                            self.registry
                                .apply_global(name.clone(), global_pid, registered_at),
                        )
                    }
                };
                match apply_result {
                    None => {
                        return GlobalRegisterResult::ConflictDetected {
                            conflicting_node_id: self.node_id,
                        }
                    }
                    Some(Err(error)) if error.downcast_ref::<RegistryCapacityError>().is_some() => {
                        return GlobalRegisterResult::ResourceExhausted
                    }
                    Some(Err(_)) => {
                        return GlobalRegisterResult::ConflictDetected {
                            conflicting_node_id: self.node_id,
                        }
                    }
                    Some(Ok(())) => {}
                }

                let committed_name = name.clone();
                let notify = RegistryCoordinationMessage::GlobalRegisterNotify {
                    name,
                    global_pid,
                    registered_at,
                };
                for node_id in members.into_iter().filter(|id| *id != self.node_id) {
                    if self.topology_epoch.load(Ordering::Acquire) != topology_epoch {
                        break;
                    }
                    if let Err(error) = self.send_to(node_id, notify.clone()).await {
                        log::debug!("Failed to notify node {node_id} of registry commit: {error}");
                    }
                }
                let topology_is_stable = {
                    let _topology_guard = lock_unpoisoned(&self.topology_fence);
                    self.topology_epoch.load(Ordering::Acquire) == topology_epoch
                };
                if !topology_is_stable {
                    self.registry
                        .remove_global_if_owner(committed_name, global_pid);
                    return GlobalRegisterResult::ConflictDetected {
                        conflicting_node_id: self.node_id,
                    };
                }
                return GlobalRegisterResult::Success;
            }

            if response_count == members.len() || Instant::now() >= deadline {
                return first_rejection.unwrap_or(GlobalRegisterResult::ConflictDetected {
                    conflicting_node_id: self.node_id,
                });
            }

            for node_id in missing {
                if let Err(error) = self
                    .send_to_with_timeout(node_id, prepare.clone(), PREPARE_SEND_TIMEOUT)
                    .await
                {
                    log::trace!("Registry prepare to node {node_id} failed: {error}");
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait = remaining.min(RETRY_INTERVAL);
            let _ = tokio::time::timeout(wait, wake.notified()).await;
        }
    }

    /// Conditionally unregister a cluster-wide name through its coordinator.
    pub async fn unregister_global_coordinated(
        &self,
        name: impl Into<ProcessName>,
        expected_global_pid: GlobalProcessId,
    ) -> Result<GlobalUnregisterResult> {
        let name = name.into();
        self.name_lock_index(name.as_str())?;
        if expected_global_pid.node_id() != self.node_id {
            return Err(RegistryConflictError(format!(
                "Global unregistration owner node {} does not match requesting node {}",
                expected_global_pid.node_id(),
                self.node_id
            ))
            .into());
        }

        if !self.has_client() {
            return Ok(self
                .coordinate_unregistration(
                    name.as_str().to_string(),
                    expected_global_pid,
                    None,
                    true,
                )
                .await);
        }

        let members = self.members()?;
        if members.len() <= 1 {
            return Ok(self
                .coordinate_unregistration(
                    name.as_str().to_string(),
                    expected_global_pid,
                    None,
                    true,
                )
                .await);
        }
        let leader = members[0];
        if leader == self.node_id {
            return Ok(self
                .coordinate_unregistration(
                    name.as_str().to_string(),
                    expected_global_pid,
                    Some(&members),
                    true,
                )
                .await);
        }

        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (receiver, _pending_guard) = self.insert_pending_unregister(request_id, leader)?;
        self.send_to(
            leader,
            RegistryCoordinationMessage::GlobalUnregisterRequest {
                request_id,
                requesting_node_id: self.node_id,
                name: name.as_str().to_string(),
                expected_global_pid,
            },
        )
        .await
        .context("Failed to send global unregistration request")?;

        match tokio::time::timeout(REGISTRATION_TIMEOUT + Duration::from_secs(1), receiver).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(anyhow!(
                "Global unregistration coordinator dropped its response"
            )),
            Err(_) => Err(RegistryTimeoutError(
                "Global unregistration timed out waiting for coordinator".into(),
            )
            .into()),
        }
    }

    async fn coordinate_unregistration(
        &self,
        name: String,
        expected_global_pid: GlobalProcessId,
        known_members: Option<&[u64]>,
        allow_missing_owner: bool,
    ) -> GlobalUnregisterResult {
        let owned_members;
        let members = if let Some(members) = known_members {
            members
        } else if self.has_client() {
            owned_members = match self.members() {
                Ok(members) => members,
                Err(error) if error.downcast_ref::<RegistryCapacityError>().is_some() => {
                    return GlobalUnregisterResult::ResourceExhausted
                }
                Err(_) => return GlobalUnregisterResult::OwnerChanged,
            };
            owned_members.as_slice()
        } else {
            &[]
        };

        let stripe = match self.name_lock_index(&name) {
            Ok(stripe) => stripe,
            Err(_) => return GlobalUnregisterResult::ResourceExhausted,
        };
        let _name_guard = self.name_locks[stripe].lock().await;

        match self.registry.lookup_global(name.as_str()) {
            Some(existing) if existing.global_pid != expected_global_pid => {
                return GlobalUnregisterResult::OwnerChanged
            }
            Some(_) => {
                if !self
                    .registry
                    .remove_global_if_owner(name.as_str(), expected_global_pid)
                {
                    return GlobalUnregisterResult::OwnerChanged;
                }
            }
            None if !allow_missing_owner => return GlobalUnregisterResult::NotFound,
            None => {}
        }

        let notify = RegistryCoordinationMessage::GlobalUnregisterNotify {
            name,
            expected_global_pid,
        };
        for node_id in members.iter().copied().filter(|id| *id != self.node_id) {
            if let Err(error) = self.send_to(node_id, notify.clone()).await {
                log::debug!("Failed to notify node {node_id} of registry removal: {error}");
            }
        }
        GlobalUnregisterResult::Success
    }

    /// Unregister all cluster-wide names still owned by `global_pid`.
    pub async fn unregister_process_registrations(
        &self,
        global_pid: GlobalProcessId,
    ) -> Result<Vec<ProcessName>> {
        if global_pid.node_id() != self.node_id {
            return Err(RegistryConflictError(format!(
                "Process registration cleanup owner node {} does not match requesting node {}",
                global_pid.node_id(),
                self.node_id
            ))
            .into());
        }
        let names = self.registry.global_names_for_process(global_pid);
        let mut removed = Vec::with_capacity(names.len());
        for name in names {
            match self
                .unregister_global_coordinated(name.clone(), global_pid)
                .await?
            {
                GlobalUnregisterResult::Success => removed.push(name),
                GlobalUnregisterResult::NotFound | GlobalUnregisterResult::OwnerChanged => {}
                GlobalUnregisterResult::ResourceExhausted => {
                    return Err(RegistryCapacityError::new(
                        "Registry coordinator could not track process unregistration",
                    )
                    .into())
                }
            }
        }
        Ok(removed)
    }

    /// Dispatch one registry protocol message received from QUIC.
    pub(crate) async fn handle_message(
        &self,
        source_node_id: VerifiedNodeId,
        message: RegistryCoordinationMessage,
    ) -> Result<()> {
        let source_node_id = source_node_id.get();
        match message {
            RegistryCoordinationMessage::GlobalRegisterRequest {
                request_id,
                requesting_node_id,
                name,
                global_pid,
            } => {
                if source_node_id != requesting_node_id
                    || global_pid.node_id() != requesting_node_id
                {
                    self.audit_protocol_denial(source_node_id);
                    return Err(anyhow!(
                        "Registry request source or owner did not match requesting node"
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
                if source_node_id != self.leader()? || global_pid.node_id() != requesting_node_id {
                    self.audit_protocol_denial(source_node_id);
                    return Err(anyhow!(
                        "Registry prepare did not come from the coordinator or named owner"
                    ));
                }
                let response =
                    self.handle_register_request(request_id, requesting_node_id, name, global_pid);
                self.send_to(source_node_id, response).await?;
            }
            RegistryCoordinationMessage::GlobalRegisterResponse {
                request_id,
                requesting_node_id,
                responding_node_id,
                result,
            } => {
                if source_node_id != responding_node_id {
                    self.audit_protocol_denial(source_node_id);
                    return Err(anyhow!(
                        "Registry response source did not match responding node"
                    ));
                }
                if matches!(
                    self.record_vote(requesting_node_id, request_id, responding_node_id, result,),
                    RecordVoteOutcome::NonMember
                ) {
                    self.audit_protocol_denial(source_node_id);
                    return Err(anyhow!("Registry vote came from a nonmember"));
                }
            }
            RegistryCoordinationMessage::GlobalRegisterDecision { request_id, result } => {
                let expected_coordinator = lock_unpoisoned(&self.pending_callers)
                    .get(&request_id)
                    .map(|pending| pending.coordinator_node_id);
                let Some(expected_coordinator) = expected_coordinator else {
                    return Ok(());
                };
                if source_node_id != expected_coordinator || source_node_id != self.leader()? {
                    self.audit_protocol_denial(source_node_id);
                    return Err(anyhow!(
                        "Registry decision did not come from the expected coordinator"
                    ));
                }
                if let Some(pending) = lock_unpoisoned(&self.pending_callers).remove(&request_id) {
                    let _ = pending.sender.send(result);
                }
            }
            RegistryCoordinationMessage::GlobalRegisterNotify {
                name,
                global_pid,
                registered_at,
            } => {
                let members = self.members()?;
                if source_node_id != members[0] || !members.contains(&global_pid.node_id()) {
                    self.audit_protocol_denial(source_node_id);
                    return Err(anyhow!(
                        "Registry commit did not come from the coordinator or names an inactive owner"
                    ));
                }
                self.handle_register_notify(name, global_pid, registered_at)?;
            }
            RegistryCoordinationMessage::GlobalUnregisterRequest {
                request_id,
                requesting_node_id,
                name,
                expected_global_pid,
            } => {
                if source_node_id != requesting_node_id
                    || expected_global_pid.node_id() != requesting_node_id
                    || self.leader()? != self.node_id
                {
                    self.audit_protocol_denial(source_node_id);
                    return Err(anyhow!("Invalid global unregistration request"));
                }
                let response = self
                    .handle_unregister_request(
                        request_id,
                        requesting_node_id,
                        name,
                        expected_global_pid,
                    )
                    .await;
                self.send_to(requesting_node_id, response).await?;
            }
            RegistryCoordinationMessage::GlobalUnregisterResponse { request_id, result } => {
                let expected_coordinator = lock_unpoisoned(&self.pending_unregisters)
                    .get(&request_id)
                    .map(|pending| pending.coordinator_node_id);
                let Some(expected_coordinator) = expected_coordinator else {
                    return Ok(());
                };
                if source_node_id != expected_coordinator || source_node_id != self.leader()? {
                    self.audit_protocol_denial(source_node_id);
                    return Err(anyhow!(
                        "Registry unregistration response did not come from the expected coordinator"
                    ));
                }
                if let Some(pending) =
                    lock_unpoisoned(&self.pending_unregisters).remove(&request_id)
                {
                    let _ = pending.sender.send(result);
                }
            }
            RegistryCoordinationMessage::GlobalUnregisterNotify {
                name,
                expected_global_pid,
            } => {
                if source_node_id != self.leader()? {
                    self.audit_protocol_denial(source_node_id);
                    return Err(anyhow!(
                        "Registry unregistration did not come from the coordinator"
                    ));
                }
                self.handle_unregister_notify(name, expected_global_pid);
            }
            RegistryCoordinationMessage::RegistrySyncRequest {
                request_id,
                requesting_node_id,
            } => {
                if source_node_id != requesting_node_id || self.leader()? != self.node_id {
                    self.audit_protocol_denial(source_node_id);
                    return Err(anyhow!("Invalid registry synchronization request"));
                }
                let response = self.handle_sync_request(request_id, requesting_node_id);
                self.send_to(requesting_node_id, response).await?;
            }
            RegistryCoordinationMessage::RegistrySyncResponse {
                request_id,
                global_entries,
            } => {
                // Serialize topology reconciliation with pending validation, removal, and
                // snapshot application. A duplicate response or cancellation must never apply a
                // snapshot after another task has already removed the corresponding waiter.
                let (pending, apply_result) = {
                    let _topology_guard = lock_unpoisoned(&self.topology_fence);
                    let mut pending_syncs = lock_unpoisoned(&self.pending_syncs);
                    let Some(expected_coordinator) = pending_syncs
                        .get(&request_id)
                        .map(|pending| pending.coordinator_node_id)
                    else {
                        return Ok(());
                    };
                    if source_node_id != expected_coordinator || source_node_id != self.leader()? {
                        self.audit_protocol_denial(source_node_id);
                        return Err(anyhow!(
                            "Registry snapshot did not come from the expected coordinator"
                        ));
                    }
                    let pending = pending_syncs.remove(&request_id).ok_or_else(|| {
                        anyhow!("Registry synchronization waiter disappeared during validation")
                    })?;
                    drop(pending_syncs);
                    let apply_result = self.handle_sync_response(global_entries);
                    (pending, apply_result)
                };
                let waiter_result = match &apply_result {
                    Ok(()) => Ok(()),
                    Err(error) if error.downcast_ref::<RegistryCapacityError>().is_some() => {
                        Err(RegistryCapacityError::new(error.to_string()).into())
                    }
                    Err(error) => Err(anyhow!(error.to_string())),
                };
                let _ = pending.sender.send(waiter_result);
                apply_result?;
            }
            RegistryCoordinationMessage::RegistryHeartbeat { node_id, .. } => {
                if source_node_id != node_id {
                    self.audit_protocol_denial(source_node_id);
                    return Err(anyhow!("Registry heartbeat source did not match node id"));
                }
            }
        }
        Ok(())
    }

    fn record_vote(
        &self,
        requesting_node_id: u64,
        request_id: u64,
        responding_node_id: u64,
        result: GlobalRegisterResult,
    ) -> RecordVoteOutcome {
        let mut pending = lock_unpoisoned(&self.pending_votes);
        let Some(votes) = pending.get_mut(&(requesting_node_id, request_id)) else {
            return RecordVoteOutcome::NotPending;
        };
        if !votes.allowed_members.contains(&responding_node_id) {
            return RecordVoteOutcome::NonMember;
        }
        if votes.responses.contains_key(&responding_node_id) {
            return RecordVoteOutcome::Duplicate;
        }
        let lease = match self.pending_budget.try_acquire() {
            Ok(lease) => lease,
            Err(_) => {
                votes.capacity_exhausted = true;
                votes.notify.notify_one();
                return RecordVoteOutcome::ResourceExhausted;
            }
        };
        votes.responses.insert(
            responding_node_id,
            PendingVoteResponse {
                result,
                _lease: lease,
            },
        );
        votes.notify.notify_one();
        RecordVoteOutcome::Recorded
    }

    /// Pull the coordinator's authoritative snapshot after a join or partition.
    pub async fn synchronize_from_coordinator(&self) -> Result<()> {
        let leader = self.leader()?;
        if leader == self.node_id {
            return Ok(());
        }
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (receiver, _pending_guard) = self.insert_pending_sync(request_id, leader)?;
        let request = RegistryCoordinationMessage::RegistrySyncRequest {
            request_id,
            requesting_node_id: self.node_id,
        };
        self.send_to(leader, request)
            .await
            .context("Failed to request registry synchronization")?;
        match tokio::time::timeout(SYNC_TIMEOUT, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(anyhow!("Registry synchronization response was dropped")),
            Err(_) => Err(RegistryTimeoutError("Registry synchronization timed out".into()).into()),
        }
    }

    pub fn handle_register_request(
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
            _ => match self.registry.can_register_global(name.as_str(), global_pid) {
                Ok(()) => GlobalRegisterResult::Success,
                Err(error) if error.downcast_ref::<RegistryCapacityError>().is_some() => {
                    GlobalRegisterResult::ResourceExhausted
                }
                Err(_) => GlobalRegisterResult::ConflictDetected {
                    conflicting_node_id: self.node_id,
                },
            },
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
        self.record_vote(self.node_id, request_id, self.node_id, result);
        Ok(None)
    }

    pub fn handle_register_notify(
        &self,
        name: String,
        global_pid: GlobalProcessId,
        registered_at: u64,
    ) -> Result<()> {
        self.registry.apply_global(name, global_pid, registered_at)
    }

    pub async fn handle_unregister_request(
        &self,
        request_id: u64,
        _requesting_node_id: u64,
        name: String,
        expected_global_pid: GlobalProcessId,
    ) -> RegistryCoordinationMessage {
        let result = self
            .coordinate_unregistration(name, expected_global_pid, None, false)
            .await;
        RegistryCoordinationMessage::GlobalUnregisterResponse { request_id, result }
    }

    pub fn handle_unregister_notify(
        &self,
        name: String,
        expected_global_pid: GlobalProcessId,
    ) -> bool {
        self.registry
            .remove_global_if_owner(name, expected_global_pid)
    }

    pub fn handle_sync_request(
        &self,
        request_id: u64,
        _requesting_node_id: u64,
    ) -> RegistryCoordinationMessage {
        RegistryCoordinationMessage::RegistrySyncResponse {
            request_id,
            global_entries: self.registry.global_snapshot(),
        }
    }

    pub fn handle_sync_response(&self, global_entries: RegistrySnapshot) -> Result<()> {
        let result = self.registry.replace_global_snapshot(global_entries);
        let (audit_result, reason) = match &result {
            Ok(()) => (AuditResult::Succeeded, AuditReason::Completed),
            Err(error) if error.downcast_ref::<RegistryCapacityError>().is_some() => {
                (AuditResult::Denied, AuditReason::QuotaExceeded)
            }
            Err(_) => (AuditResult::Denied, AuditReason::ProtocolDenied),
        };
        self.audit_registry_event(
            AuditEvent::DistributedSnapshotApply,
            AuditAction::Resync,
            None,
            audit_result,
            reason,
        );
        result
    }

    /// Fence in-flight work and remove registrations owned by removed nodes.
    pub(crate) fn reconcile_topology(
        &self,
        active_nodes: &HashSet<u64>,
        removed_nodes: &[u64],
    ) -> Vec<ProcessName> {
        let _topology_guard = lock_unpoisoned(&self.topology_fence);
        self.topology_epoch.fetch_add(1, Ordering::AcqRel);

        let mut cleaned_names = Vec::new();
        for removed_node_id in removed_nodes.iter().copied().collect::<HashSet<_>>() {
            cleaned_names.extend(self.registry.remove_node_registrations(removed_node_id));
        }

        let drained_votes = {
            let mut pending = lock_unpoisoned(&self.pending_votes);
            let keys = pending
                .iter()
                .filter(|(_, votes)| {
                    !removed_nodes.is_empty()
                        || votes
                            .allowed_members
                            .iter()
                            .any(|node_id| !active_nodes.contains(node_id))
                })
                .map(|(key, _)| *key)
                .collect::<Vec<_>>();
            for key in &keys {
                if let Some(votes) = pending.get(key) {
                    votes.notify.notify_waiters();
                }
            }
            keys.into_iter()
                .filter_map(|key| pending.remove(&key))
                .collect::<Vec<_>>()
        };
        drop(drained_votes);

        let drained_callers = drain_stale_entries(
            &self.pending_callers,
            active_nodes,
            !removed_nodes.is_empty(),
            |pending| pending.coordinator_node_id,
        );
        for pending in drained_callers {
            let _ = pending.sender.send(GlobalRegisterResult::ConflictDetected {
                conflicting_node_id: pending.coordinator_node_id,
            });
        }

        let drained_syncs = drain_stale_entries(
            &self.pending_syncs,
            active_nodes,
            !removed_nodes.is_empty(),
            |pending| pending.coordinator_node_id,
        );
        for pending in drained_syncs {
            let _ = pending.sender.send(Err(RegistryConflictError(format!(
                "Registry topology changed while synchronizing with node {}",
                pending.coordinator_node_id
            ))
            .into()));
        }

        let drained_unregisters = drain_stale_entries(
            &self.pending_unregisters,
            active_nodes,
            !removed_nodes.is_empty(),
            |pending| pending.coordinator_node_id,
        );
        for pending in drained_unregisters {
            let _ = pending.sender.send(GlobalUnregisterResult::OwnerChanged);
        }

        cleaned_names.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        cleaned_names
    }

    /// Compatibility cleanup that only touches global registrations and remains
    /// owner-conditional if a name is concurrently reassigned.
    pub async fn cleanup_node_registrations(&self, failed_node_id: u64) -> Vec<ProcessName> {
        self.registry.remove_node_registrations(failed_node_id)
    }

    fn audit_protocol_denial(&self, verified_remote_node_id: u64) {
        emit_audit_event(protocol_denial_event(verified_remote_node_id));
    }

    fn audit_registry_event(
        &self,
        event: AuditEvent,
        action: AuditAction,
        global_pid: Option<GlobalProcessId>,
        result: AuditResult,
        reason: AuditReason,
    ) {
        let subject = AuditSubject::new().with_node_id(self.node_id);
        let mut target = AuditTarget::new(AuditTargetKind::DistributedRegistry)
            .with_node_id(self.node_id)
            .with_sensitive_data(SensitiveData::Redacted);
        if let Some(global_pid) = global_pid {
            target = target
                .with_node_id(global_pid.node_id())
                .with_environment_id(global_pid.environment_id())
                .with_process_id(global_pid.process_id());
        }
        emit_audit_event(AuditEventV1::new(
            event, action, result, reason, subject, target,
        ));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordVoteOutcome {
    Recorded,
    Duplicate,
    NonMember,
    NotPending,
    ResourceExhausted,
}

fn drain_stale_entries<T, F>(
    map: &Arc<StdMutex<HashMap<u64, T>>>,
    active_nodes: &HashSet<u64>,
    drain_all: bool,
    coordinator_node_id: F,
) -> Vec<T>
where
    F: Fn(&T) -> u64,
{
    let mut pending = lock_unpoisoned(map);
    let request_ids = pending
        .iter()
        .filter(|(_, entry)| drain_all || !active_nodes.contains(&coordinator_node_id(entry)))
        .map(|(request_id, _)| *request_id)
        .collect::<Vec<_>>();
    request_ids
        .into_iter()
        .filter_map(|request_id| pending.remove(&request_id))
        .collect()
}

fn stable_name_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn protocol_denial_event(verified_remote_node_id: u64) -> AuditEventV1 {
    AuditEventV1::new(
        AuditEvent::DistributedRequestAuthorization,
        AuditAction::Validate,
        AuditResult::Denied,
        AuditReason::ProtocolDenied,
        AuditSubject::new().with_node_id(verified_remote_node_id),
        AuditTarget::new(AuditTargetKind::DistributedRequest)
            .with_sensitive_data(SensitiveData::Redacted),
    )
}

fn result_to_anyhow(name: &str, result: GlobalRegisterResult) -> Result<()> {
    match result {
        GlobalRegisterResult::Success => Ok(()),
        GlobalRegisterResult::AlreadyRegistered { existing_gpid, .. } => {
            Err(already_registered_error(name, existing_gpid))
        }
        GlobalRegisterResult::ConflictDetected {
            conflicting_node_id,
        } => Err(RegistryConflictError(format!(
            "Global registration for '{name}' failed without a quorum; conflict reported by node {conflicting_node_id}"
        ))
        .into()),
        GlobalRegisterResult::ResourceExhausted => Err(RegistryCapacityError::new(format!(
            "Global registration for '{name}' exceeded registry coordination capacity"
        ))
        .into()),
    }
}

fn already_registered_error(name: &str, global_pid: GlobalProcessId) -> anyhow::Error {
    RegistryConflictError(format!(
        "Name '{name}' already registered globally with pid {global_pid}"
    ))
    .into()
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

    #[test]
    fn registration_errors_keep_stable_audit_meanings() {
        let conflict = already_registered_error("secret-name", GlobalProcessId::new(1, 2, 3));
        assert_eq!(
            classify_registration_error(&conflict),
            (AuditResult::Denied, AuditReason::Conflict)
        );
        let timeout: anyhow::Error = RegistryTimeoutError("timeout".into()).into();
        assert_eq!(
            classify_registration_error(&timeout),
            (AuditResult::Failed, AuditReason::TimedOut)
        );
        let capacity: anyhow::Error = RegistryCapacityError::new("full").into();
        assert_eq!(
            classify_registration_error(&capacity),
            (AuditResult::Denied, AuditReason::QuotaExceeded)
        );
    }

    #[test]
    fn protocol_denial_event_uses_verified_remote_identity() {
        let event = protocol_denial_event(7);

        assert_eq!(event.result(), AuditResult::Denied);
        assert_eq!(event.reason(), AuditReason::ProtocolDenied);
        assert_eq!(event.subject().node_id(), Some(7));
        assert_eq!(event.target().node_id(), None);
        assert_eq!(event.target().environment_id(), None);
        assert_eq!(event.target().process_id(), None);
        assert_eq!(event.target().sensitive_data(), SensitiveData::Redacted);
    }

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
    async fn zero_name_lock_stripes_return_typed_capacity_error() {
        let registry = Arc::new(DistributedRegistry::with_limits(
            1,
            RegistryLimits {
                max_name_locks: 0,
                ..RegistryLimits::default()
            },
        ));
        let coordinator = RegistryCoordinator::new(registry, 1);

        let error = coordinator
            .register_global_coordinated("service", GlobalProcessId::new(1, 1, 100), 1)
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<RegistryCapacityError>().is_some());
    }

    #[tokio::test]
    async fn missing_local_owner_unregistration_is_idempotent() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry, 1);

        assert_eq!(
            coordinator
                .unregister_global_coordinated("already-removed", GlobalProcessId::new(1, 1, 100),)
                .await
                .unwrap(),
            GlobalUnregisterResult::Success
        );
    }

    #[test]
    fn committed_notification_replaces_stale_partition_state() {
        let registry = Arc::new(DistributedRegistry::new(2));
        let coordinator = RegistryCoordinator::new(registry.clone(), 2);
        let stale = GlobalProcessId::new(2, 1, 100);
        let committed = GlobalProcessId::new(1, 1, 200);
        registry.register_global("service", stale).unwrap();

        coordinator
            .handle_register_notify("service".into(), committed, 42)
            .unwrap();

        let entry = registry.lookup_global("service").unwrap();
        assert_eq!(entry.global_pid, committed);
        assert_eq!(entry.registered_at, 42);
    }

    #[test]
    fn authoritative_sync_removes_stale_entries() {
        let registry = Arc::new(DistributedRegistry::new(2));
        let coordinator = RegistryCoordinator::new(registry.clone(), 2);
        registry
            .register_global("stale", GlobalProcessId::new(2, 1, 100))
            .unwrap();
        let current = GlobalProcessId::new(1, 1, 200);

        coordinator
            .handle_sync_response(vec![("current".into(), current, 7)])
            .unwrap();

        assert!(registry.lookup_global("stale").is_none());
        assert_eq!(
            registry.lookup_global("current").unwrap().global_pid,
            current
        );
    }

    #[test]
    fn rejected_snapshots_preserve_coordinator_registry_state() {
        let registry = Arc::new(DistributedRegistry::with_limits(
            2,
            RegistryLimits {
                max_entries: 2,
                max_retained_bytes: 128,
                ..RegistryLimits::default()
            },
        ));
        let coordinator = RegistryCoordinator::new(registry.clone(), 2);
        let local = GlobalProcessId::new(2, 1, 100);
        let remote = GlobalProcessId::new(1, 1, 200);
        registry.register_local("local", local).unwrap();
        registry.register_global("old", remote).unwrap();
        let baseline = registry.global_snapshot();

        let oversized = coordinator
            .handle_sync_response(vec![("one".into(), remote, 1), ("two".into(), remote, 2)])
            .unwrap_err();
        assert!(oversized.downcast_ref::<RegistryCapacityError>().is_some());
        assert_eq!(registry.global_snapshot(), baseline);

        coordinator
            .handle_sync_response(vec![
                ("duplicate".into(), remote, 1),
                ("duplicate".into(), local, 2),
            ])
            .unwrap_err();
        assert_eq!(registry.global_snapshot(), baseline);
    }

    #[test]
    fn delayed_owner_tombstone_cannot_remove_new_owner() {
        let registry = Arc::new(DistributedRegistry::new(2));
        let coordinator = RegistryCoordinator::new(registry.clone(), 2);
        let old_owner = GlobalProcessId::new(1, 1, 100);
        let new_owner = GlobalProcessId::new(2, 1, 200);
        registry.register_global("service", old_owner).unwrap();
        registry.apply_global("service", new_owner, 2).unwrap();

        assert!(!coordinator.handle_unregister_notify("service".into(), old_owner));
        assert_eq!(
            registry.lookup_global("service").unwrap().global_pid,
            new_owner
        );
    }

    #[test]
    fn name_lock_flood_has_fixed_storage() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry, 1);
        let stripe_count = coordinator.name_locks.len();
        assert!(stripe_count > 0);

        for index in 0..10_000 {
            assert!(coordinator
                .name_lock_index(&format!("attacker-name-{index}"))
                .is_ok());
        }

        assert_eq!(coordinator.name_locks.len(), stripe_count);
    }

    #[test]
    fn aggregate_pending_budget_limits_and_reuses_capacity() {
        let budget = Arc::new(PendingBudget::new(2));
        let first = budget.try_acquire().unwrap();
        let second = budget.try_acquire().unwrap();
        assert!(budget.try_acquire().is_err());
        assert_eq!(budget.used(), 2);

        drop(first);
        let reused = budget.try_acquire().unwrap();
        assert_eq!(budget.used(), 2);
        drop(second);
        drop(reused);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn aggregate_limit_is_shared_by_all_pending_maps() {
        let registry = Arc::new(DistributedRegistry::with_limits(
            1,
            RegistryLimits {
                max_pending_responses: 2,
                ..RegistryLimits::default()
            },
        ));
        let coordinator = RegistryCoordinator::new(registry, 1);
        let (_caller_receiver, caller_guard) = coordinator.insert_pending_caller(1, 2).unwrap();
        let (_sync_receiver, sync_guard) = coordinator.insert_pending_sync(2, 2).unwrap();

        let error = match coordinator.insert_pending_unregister(3, 2) {
            Ok(_) => panic!("aggregate pending limit must reject another entry"),
            Err(error) => error,
        };
        assert!(error.downcast_ref::<RegistryCapacityError>().is_some());
        assert_eq!(coordinator.pending_budget.used(), 2);

        drop(caller_guard);
        let (_unregister_receiver, unregister_guard) =
            coordinator.insert_pending_unregister(3, 2).unwrap();
        assert_eq!(coordinator.pending_budget.used(), 2);
        drop(sync_guard);
        drop(unregister_guard);
        assert_eq!(coordinator.pending_budget.used(), 0);
    }

    #[test]
    fn pending_guards_remove_entries_and_release_budget() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry, 1);

        let (_caller_receiver, caller_guard) = coordinator.insert_pending_caller(1, 2).unwrap();
        let (_sync_receiver, sync_guard) = coordinator.insert_pending_sync(2, 2).unwrap();
        let (_unregister_receiver, unregister_guard) =
            coordinator.insert_pending_unregister(3, 2).unwrap();
        let vote_lease = coordinator.pending_budget.try_acquire().unwrap();
        lock_unpoisoned(&coordinator.pending_votes).insert(
            (1, 4),
            PendingVotes {
                token: 4,
                allowed_members: HashSet::from([1]),
                responses: HashMap::new(),
                capacity_exhausted: false,
                notify: Arc::new(Notify::new()),
                _lease: vote_lease,
            },
        );
        let vote_guard = PendingVoteGuard {
            map: coordinator.pending_votes.clone(),
            key: (1, 4),
            token: 4,
        };
        assert_eq!(coordinator.pending_budget.used(), 4);

        drop(caller_guard);
        drop(sync_guard);
        drop(unregister_guard);
        drop(vote_guard);
        assert!(lock_unpoisoned(&coordinator.pending_callers).is_empty());
        assert!(lock_unpoisoned(&coordinator.pending_syncs).is_empty());
        assert!(lock_unpoisoned(&coordinator.pending_unregisters).is_empty());
        assert!(lock_unpoisoned(&coordinator.pending_votes).is_empty());
        assert_eq!(coordinator.pending_budget.used(), 0);
    }

    #[test]
    fn unknown_voter_does_not_grow_responses() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry, 1);
        let entry_lease = coordinator.pending_budget.try_acquire().unwrap();
        let response_lease = coordinator.pending_budget.try_acquire().unwrap();
        let mut responses = HashMap::new();
        responses.insert(
            1,
            PendingVoteResponse {
                result: GlobalRegisterResult::Success,
                _lease: response_lease,
            },
        );
        lock_unpoisoned(&coordinator.pending_votes).insert(
            (1, 7),
            PendingVotes {
                token: 1,
                allowed_members: HashSet::from([1, 2]),
                responses,
                capacity_exhausted: false,
                notify: Arc::new(Notify::new()),
                _lease: entry_lease,
            },
        );

        assert_eq!(
            coordinator.record_vote(1, 7, 99, GlobalRegisterResult::Success),
            RecordVoteOutcome::NonMember
        );
        let pending = lock_unpoisoned(&coordinator.pending_votes);
        assert_eq!(pending.get(&(1, 7)).unwrap().responses.len(), 1);
        assert_eq!(coordinator.pending_budget.used(), 2);
        drop(pending);
        lock_unpoisoned(&coordinator.pending_votes).clear();
        assert_eq!(coordinator.pending_budget.used(), 0);
    }

    #[test]
    fn topology_cleanup_removes_owned_globals_and_drains_pending_state() {
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = RegistryCoordinator::new(registry.clone(), 1);
        let removed_owner = GlobalProcessId::new(2, 1, 10);
        registry.register_global("removed", removed_owner).unwrap();

        let (_caller_receiver, _caller_guard) = coordinator.insert_pending_caller(1, 2).unwrap();
        let (_sync_receiver, _sync_guard) = coordinator.insert_pending_sync(2, 2).unwrap();
        let (_unregister_receiver, _unregister_guard) =
            coordinator.insert_pending_unregister(3, 2).unwrap();

        let entry_lease = coordinator.pending_budget.try_acquire().unwrap();
        let response_lease = coordinator.pending_budget.try_acquire().unwrap();
        let mut responses = HashMap::new();
        responses.insert(
            1,
            PendingVoteResponse {
                result: GlobalRegisterResult::Success,
                _lease: response_lease,
            },
        );
        lock_unpoisoned(&coordinator.pending_votes).insert(
            (1, 4),
            PendingVotes {
                token: 4,
                allowed_members: HashSet::from([1, 2]),
                responses,
                capacity_exhausted: false,
                notify: Arc::new(Notify::new()),
                _lease: entry_lease,
            },
        );

        let cleaned = coordinator.reconcile_topology(&HashSet::from([1]), &[2]);
        assert_eq!(cleaned, vec![ProcessName::new("removed")]);
        assert!(registry.lookup_global("removed").is_none());
        assert!(lock_unpoisoned(&coordinator.pending_votes).is_empty());
        assert!(lock_unpoisoned(&coordinator.pending_callers).is_empty());
        assert!(lock_unpoisoned(&coordinator.pending_syncs).is_empty());
        assert!(lock_unpoisoned(&coordinator.pending_unregisters).is_empty());
        assert_eq!(coordinator.pending_budget.used(), 0);
    }
}
