use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::sync::RwLock;

use anyhow::Result;
use lunatic_common_api::{
    emit_audit_event, AuditAction, AuditEvent, AuditEventV1, AuditReason, AuditResult,
    AuditSubject, AuditTarget, AuditTargetKind, SensitiveData,
};
use serde::{Deserialize, Serialize};

use super::global_process_id::GlobalProcessId;

/// Finite resource limits for the distributed registry and its coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistryLimits {
    /// Maximum UTF-8 byte length of a process name.
    pub max_name_bytes: usize,
    /// Maximum number of local and global namespace entries combined.
    pub max_entries: usize,
    /// Maximum bytes retained by forward and reverse name copies.
    pub max_retained_bytes: usize,
    /// Maximum number of coordinator name-lock stripes.
    pub max_name_locks: usize,
    /// Maximum number of in-flight coordinator responses.
    pub max_pending_responses: usize,
    /// Maximum number of nodes retained in the coordinator topology.
    pub max_topology_nodes: usize,
}

impl Default for RegistryLimits {
    fn default() -> Self {
        Self {
            max_name_bytes: 1_024,
            max_entries: 16_384,
            max_retained_bytes: 4 * 1_024 * 1_024,
            max_name_locks: 1_024,
            max_pending_responses: 4_096,
            max_topology_nodes: 1_024,
        }
    }
}

/// A registry admission failure caused by a configured capacity limit.
///
/// The concrete error type is preserved through `anyhow::Error`, allowing
/// coordinators to classify quota denials without inspecting error strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryCapacityError {
    message: String,
}

impl RegistryCapacityError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for RegistryCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for RegistryCapacityError {}

/// Aggregate resources currently retained by local and global registrations.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RegistryUsage {
    pub entries: usize,
    pub retained_bytes: usize,
}

/// Registered name for a process in the distributed registry.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
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

/// Registration scope for process names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RegistrationScope {
    /// Local to the current node only.
    Local,
    /// Global across all nodes in the cluster.
    Global,
}

/// Process registry entry with metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryEntry {
    pub global_pid: GlobalProcessId,
    pub scope: RegistrationScope,
    pub registered_at: u64, // Unix timestamp in milliseconds
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ScopedProcessName {
    scope: RegistrationScope,
    name: ProcessName,
}

#[derive(Default)]
struct RegistryState {
    local: HashMap<ProcessName, RegistryEntry>,
    global: HashMap<ProcessName, RegistryEntry>,
    reverse: HashMap<GlobalProcessId, HashSet<ScopedProcessName>>,
    usage: RegistryUsage,
}

/// Distributed process registry providing location transparency.
///
/// Local and global names occupy distinct namespaces. All forward mappings,
/// scoped reverse mappings, and resource accounting share one lock so readers
/// never observe a partially applied registration or cleanup.
pub struct DistributedRegistry {
    node_id: u64,
    limits: RegistryLimits,
    state: RwLock<RegistryState>,
}

impl DistributedRegistry {
    /// Create a registry with finite default limits.
    pub fn new(node_id: u64) -> Self {
        Self::with_limits(node_id, RegistryLimits::default())
    }

    /// Create a registry with explicit limits.
    pub fn with_limits(node_id: u64, limits: RegistryLimits) -> Self {
        Self {
            node_id,
            limits,
            state: RwLock::new(RegistryState::default()),
        }
    }

    /// Return the configured immutable limits.
    pub fn limits(&self) -> RegistryLimits {
        self.limits
    }

    /// Return an atomic snapshot of aggregate registry usage.
    pub fn usage(&self) -> RegistryUsage {
        self.read_state().usage
    }

    /// Register a process with a local name (node-scoped).
    ///
    /// Returns an error if the name is invalid, already registered locally, or
    /// would exceed an aggregate registry limit.
    pub fn register_local(
        &self,
        name: impl Into<ProcessName>,
        global_pid: GlobalProcessId,
    ) -> Result<()> {
        let name = name.into();
        let result = self.insert_local_if_absent(name, global_pid);
        let (audit_result, reason) = registration_audit_outcome(&result);
        self.audit_change(
            AuditAction::Register,
            Some(global_pid),
            audit_result,
            reason,
        );
        result
    }

    /// Insert a process into this node's global registry view.
    ///
    /// Cluster callers should use [`super::Client::register_global`] so success
    /// waits for quorum. This method is a low-level single-node primitive.
    pub fn register_global(
        &self,
        name: impl Into<ProcessName>,
        global_pid: GlobalProcessId,
    ) -> Result<()> {
        let result = self.register_global_untracked(name, global_pid);
        let (audit_result, reason) = registration_audit_outcome(&result);
        self.audit_change(
            AuditAction::Register,
            Some(global_pid),
            audit_result,
            reason,
        );
        result
    }

    /// Store a caller-audited global registration without a replica-level
    /// audit event.
    pub(crate) fn register_global_untracked(
        &self,
        name: impl Into<ProcessName>,
        global_pid: GlobalProcessId,
    ) -> Result<()> {
        let name = name.into();
        self.validate_name(&name)?;

        let mut state = self.write_state();
        if state.global.contains_key(&name) {
            return Err(anyhow::anyhow!("Process name already registered globally"));
        }

        let usage = self.usage_after_add(state.usage, &name)?;
        let entry = RegistryEntry {
            global_pid,
            scope: RegistrationScope::Global,
            registered_at: current_timestamp_ms(),
        };
        state.global.insert(name.clone(), entry);
        add_reverse_mapping(&mut state, global_pid, RegistrationScope::Global, name);
        state.usage = usage;
        Ok(())
    }

    /// Check whether a global registration can be admitted without mutating
    /// registry state.
    ///
    /// An already-applied registration for the same PID is accepted
    /// idempotently; a different owner is a conflict.
    pub fn can_register_global(
        &self,
        name: impl Into<ProcessName>,
        global_pid: GlobalProcessId,
    ) -> Result<()> {
        let name = name.into();
        self.validate_name(&name)?;

        let state = self.read_state();
        if let Some(existing) = state.global.get(&name) {
            return if existing.global_pid == global_pid {
                Ok(())
            } else {
                Err(anyhow::anyhow!("Process name already registered globally"))
            };
        }
        self.usage_after_add(state.usage, &name).map(|_| ())
    }

    /// Apply a committed cluster registration.
    ///
    /// The elected coordinator is authoritative, so a committed value replaces
    /// stale global state. Local state with the same name is unaffected.
    pub fn apply_global(
        &self,
        name: impl Into<ProcessName>,
        global_pid: GlobalProcessId,
        registered_at: u64,
    ) -> Result<()> {
        let name = name.into();
        self.validate_name(&name)?;

        let mut state = self.write_state();
        let previous = state.global.get(&name).cloned();
        let usage = if previous.is_some() {
            state.usage
        } else {
            self.usage_after_add(state.usage, &name)?
        };

        if let Some(previous) = previous {
            remove_reverse_mapping(
                &mut state,
                previous.global_pid,
                RegistrationScope::Global,
                &name,
            );
        }
        state.global.insert(
            name.clone(),
            RegistryEntry {
                global_pid,
                scope: RegistrationScope::Global,
                registered_at,
            },
        );
        add_reverse_mapping(&mut state, global_pid, RegistrationScope::Global, name);
        state.usage = usage;
        Ok(())
    }

    /// Remove a global name only while it is still owned by `expected_pid`.
    ///
    /// This makes delayed cleanup and old-owner notifications harmless after a
    /// name has been reassigned.
    pub fn remove_global_if_owner(
        &self,
        name: impl Into<ProcessName>,
        expected_pid: GlobalProcessId,
    ) -> bool {
        let name = name.into();
        let mut state = self.write_state();
        remove_scoped_registration(
            &mut state,
            RegistrationScope::Global,
            &name,
            Some(expected_pid),
        )
        .is_some()
    }

    /// Replace global state with an authoritative coordinator snapshot.
    ///
    /// The entire snapshot is validated and capacity-checked against current
    /// local state before any visible mutation occurs.
    pub fn replace_global_snapshot(
        &self,
        entries: Vec<(String, GlobalProcessId, u64)>,
    ) -> Result<()> {
        if entries.len() > self.limits.max_entries {
            return Err(RegistryCapacityError::new(format!(
                "Registry entry limit exceeded (maximum {})",
                self.limits.max_entries
            ))
            .into());
        }
        let mut new_global = HashMap::with_capacity(entries.len());
        let mut global_retained_bytes = 0usize;

        for (name, global_pid, registered_at) in entries {
            let name = ProcessName::new(name);
            self.validate_name(&name)?;
            let retained = retained_bytes_for_name(&name)?;
            global_retained_bytes =
                global_retained_bytes.checked_add(retained).ok_or_else(|| {
                    RegistryCapacityError::new("Registry retained-byte accounting overflow")
                })?;
            if global_retained_bytes > self.limits.max_retained_bytes {
                return Err(RegistryCapacityError::new(format!(
                    "Registry retained-byte limit exceeded (maximum {})",
                    self.limits.max_retained_bytes
                ))
                .into());
            }

            let entry = RegistryEntry {
                global_pid,
                scope: RegistrationScope::Global,
                registered_at,
            };
            if new_global.insert(name, entry).is_some() {
                return Err(anyhow::anyhow!(
                    "Registry snapshot contains a duplicate global name"
                ));
            }
        }

        let mut state = self.write_state();
        let entries = state
            .local
            .len()
            .checked_add(new_global.len())
            .ok_or_else(|| RegistryCapacityError::new("Registry entry accounting overflow"))?;
        if entries > self.limits.max_entries {
            return Err(RegistryCapacityError::new(format!(
                "Registry entry limit exceeded (maximum {})",
                self.limits.max_entries
            ))
            .into());
        }

        let local_retained_bytes = retained_bytes_for_names(state.local.keys())?;
        let retained_bytes = local_retained_bytes
            .checked_add(global_retained_bytes)
            .ok_or_else(|| {
                RegistryCapacityError::new("Registry retained-byte accounting overflow")
            })?;
        if retained_bytes > self.limits.max_retained_bytes {
            return Err(RegistryCapacityError::new(format!(
                "Registry retained-byte limit exceeded (maximum {})",
                self.limits.max_retained_bytes
            ))
            .into());
        }

        let mut reverse = HashMap::new();
        for (name, entry) in &state.local {
            add_to_reverse_map(
                &mut reverse,
                entry.global_pid,
                RegistrationScope::Local,
                name.clone(),
            );
        }
        for (name, entry) in &new_global {
            add_to_reverse_map(
                &mut reverse,
                entry.global_pid,
                RegistrationScope::Global,
                name.clone(),
            );
        }

        state.global = new_global;
        state.reverse = reverse;
        state.usage = RegistryUsage {
            entries,
            retained_bytes,
        };
        Ok(())
    }

    /// Return one atomic snapshot of all global entries.
    pub fn global_snapshot(&self) -> Vec<(String, GlobalProcessId, u64)> {
        let state = self.read_state();
        let mut snapshot = state
            .global
            .iter()
            .map(|(name, entry)| {
                (
                    name.as_str().to_string(),
                    entry.global_pid,
                    entry.registered_at,
                )
            })
            .collect::<Vec<_>>();
        snapshot.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        snapshot
    }

    /// Unregister a process name from both namespaces.
    ///
    /// Each scoped mapping is removed independently, so equal local and global
    /// names owned by different processes keep correct reverse indexes and
    /// accounting.
    pub fn unregister(&self, name: impl Into<ProcessName>) -> Result<()> {
        let name = name.into();
        let mut state = self.write_state();
        let removed_local =
            remove_scoped_registration(&mut state, RegistrationScope::Local, &name, None);
        let removed_global =
            remove_scoped_registration(&mut state, RegistrationScope::Global, &name, None);
        drop(state);

        let removed_process = removed_local
            .as_ref()
            .or(removed_global.as_ref())
            .map(|entry| entry.global_pid);
        match removed_process {
            Some(global_pid) => {
                self.audit_change(
                    AuditAction::Unregister,
                    Some(global_pid),
                    AuditResult::Succeeded,
                    AuditReason::Completed,
                );
                Ok(())
            }
            None => {
                self.audit_change(
                    AuditAction::Unregister,
                    None,
                    AuditResult::Failed,
                    AuditReason::NotFound,
                );
                Err(anyhow::anyhow!("Process name not registered"))
            }
        }
    }

    /// Unregister all scoped names still owned by a process.
    ///
    /// The returned entries describe only mappings that were actually removed.
    pub fn unregister_process(
        &self,
        global_pid: GlobalProcessId,
    ) -> Vec<(ProcessName, RegistryEntry)> {
        let mut state = self.write_state();
        let registrations = state.reverse.get(&global_pid).cloned().unwrap_or_default();
        let mut removed = Vec::with_capacity(registrations.len());

        for registration in registrations {
            if let Some(entry) = remove_scoped_registration(
                &mut state,
                registration.scope,
                &registration.name,
                Some(global_pid),
            ) {
                removed.push((registration.name, entry));
            }
        }
        state.reverse.remove(&global_pid);
        drop(state);

        removed.sort_unstable_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| scope_order(left.1.scope).cmp(&scope_order(right.1.scope)))
        });
        if !removed.is_empty() {
            self.audit_change(
                AuditAction::Unregister,
                Some(global_pid),
                AuditResult::Succeeded,
                AuditReason::Completed,
            );
        }
        removed
    }

    /// Remove all local names still owned by a process, leaving its global
    /// names available for owner-conditional coordinator cleanup.
    pub fn remove_local_registrations(&self, global_pid: GlobalProcessId) -> Vec<ProcessName> {
        let mut state = self.write_state();
        let registrations = state
            .reverse
            .get(&global_pid)
            .into_iter()
            .flat_map(|registrations| registrations.iter())
            .filter(|registration| registration.scope == RegistrationScope::Local)
            .cloned()
            .collect::<Vec<_>>();
        let mut removed = Vec::with_capacity(registrations.len());

        for registration in registrations {
            if remove_scoped_registration(
                &mut state,
                RegistrationScope::Local,
                &registration.name,
                Some(global_pid),
            )
            .is_some()
            {
                removed.push(registration.name);
            }
        }
        drop(state);

        removed.sort_unstable();
        if !removed.is_empty() {
            self.audit_change(
                AuditAction::Unregister,
                Some(global_pid),
                AuditResult::Succeeded,
                AuditReason::Completed,
            );
        }
        removed
    }

    /// Remove all global registrations currently owned by a node.
    pub fn remove_node_registrations(&self, node_id: u64) -> Vec<ProcessName> {
        let mut state = self.write_state();
        let registrations = state
            .global
            .iter()
            .filter(|(_, entry)| entry.global_pid.node_id() == node_id)
            .map(|(name, entry)| (name.clone(), entry.global_pid))
            .collect::<Vec<_>>();
        let mut removed = Vec::with_capacity(registrations.len());

        for (name, global_pid) in registrations {
            if remove_scoped_registration(
                &mut state,
                RegistrationScope::Global,
                &name,
                Some(global_pid),
            )
            .is_some()
            {
                removed.push((name, global_pid));
            }
        }
        drop(state);

        removed.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        for (_, global_pid) in &removed {
            self.audit_change(
                AuditAction::Unregister,
                Some(*global_pid),
                AuditResult::Succeeded,
                AuditReason::Completed,
            );
        }
        removed.into_iter().map(|(name, _)| name).collect()
    }

    /// Lookup a process by name, preferring the local namespace.
    pub fn lookup(&self, name: impl Into<ProcessName>) -> Option<RegistryEntry> {
        let name = name.into();
        let state = self.read_state();
        state
            .local
            .get(&name)
            .or_else(|| state.global.get(&name))
            .cloned()
    }

    /// Lookup a process by name in local scope only.
    pub fn lookup_local(&self, name: impl Into<ProcessName>) -> Option<RegistryEntry> {
        let name = name.into();
        self.read_state().local.get(&name).cloned()
    }

    /// Lookup a process by name in global scope only.
    pub fn lookup_global(&self, name: impl Into<ProcessName>) -> Option<RegistryEntry> {
        let name = name.into();
        self.read_state().global.get(&name).cloned()
    }

    /// Get all scoped registered names for a process.
    pub fn get_names(&self, global_pid: GlobalProcessId) -> Vec<ProcessName> {
        let state = self.read_state();
        let mut names = state
            .reverse
            .get(&global_pid)
            .into_iter()
            .flat_map(|registrations| registrations.iter())
            .map(|registration| registration.name.clone())
            .collect::<Vec<_>>();
        names.sort_unstable();
        names
    }

    /// Get global names currently owned by a process.
    pub fn global_names_for_process(&self, global_pid: GlobalProcessId) -> Vec<ProcessName> {
        let state = self.read_state();
        let mut names = state
            .reverse
            .get(&global_pid)
            .into_iter()
            .flat_map(|registrations| registrations.iter())
            .filter(|registration| registration.scope == RegistrationScope::Global)
            .map(|registration| registration.name.clone())
            .collect::<Vec<_>>();
        names.sort_unstable();
        names
    }

    /// Get all locally registered names.
    pub fn local_names(&self) -> Vec<ProcessName> {
        let state = self.read_state();
        let mut names = state.local.keys().cloned().collect::<Vec<_>>();
        names.sort_unstable();
        names
    }

    /// Get all globally registered names.
    pub fn global_names(&self) -> Vec<ProcessName> {
        let state = self.read_state();
        let mut names = state.global.keys().cloned().collect::<Vec<_>>();
        names.sort_unstable();
        names
    }

    /// Get the number of locally registered names.
    pub fn local_count(&self) -> usize {
        self.read_state().local.len()
    }

    /// Get the number of globally registered names.
    pub fn global_count(&self) -> usize {
        self.read_state().global.len()
    }

    /// Clear all registrations and release their accounted resources.
    pub fn clear(&self) {
        *self.write_state() = RegistryState::default();
    }

    fn insert_local_if_absent(&self, name: ProcessName, global_pid: GlobalProcessId) -> Result<()> {
        self.validate_name(&name)?;

        let mut state = self.write_state();
        if state.local.contains_key(&name) {
            return Err(anyhow::anyhow!("Process name already registered locally"));
        }

        let usage = self.usage_after_add(state.usage, &name)?;
        let entry = RegistryEntry {
            global_pid,
            scope: RegistrationScope::Local,
            registered_at: current_timestamp_ms(),
        };
        state.local.insert(name.clone(), entry);
        add_reverse_mapping(&mut state, global_pid, RegistrationScope::Local, name);
        state.usage = usage;
        Ok(())
    }

    fn validate_name(&self, name: &ProcessName) -> Result<()> {
        if name.as_str().is_empty() {
            return Err(InvalidRegistryNameError("Process name must not be empty").into());
        }
        if name.as_str().chars().any(char::is_control) {
            return Err(InvalidRegistryNameError(
                "Process name must not contain control characters",
            )
            .into());
        }
        if name.as_str().len() > self.limits.max_name_bytes {
            return Err(RegistryCapacityError::new(format!(
                "Registry name byte limit exceeded (maximum {})",
                self.limits.max_name_bytes
            ))
            .into());
        }
        Ok(())
    }

    fn usage_after_add(&self, usage: RegistryUsage, name: &ProcessName) -> Result<RegistryUsage> {
        let entries = usage
            .entries
            .checked_add(1)
            .ok_or_else(|| RegistryCapacityError::new("Registry entry accounting overflow"))?;
        if entries > self.limits.max_entries {
            return Err(RegistryCapacityError::new(format!(
                "Registry entry limit exceeded (maximum {})",
                self.limits.max_entries
            ))
            .into());
        }

        let retained_bytes = usage
            .retained_bytes
            .checked_add(retained_bytes_for_name(name)?)
            .ok_or_else(|| {
                RegistryCapacityError::new("Registry retained-byte accounting overflow")
            })?;
        if retained_bytes > self.limits.max_retained_bytes {
            return Err(RegistryCapacityError::new(format!(
                "Registry retained-byte limit exceeded (maximum {})",
                self.limits.max_retained_bytes
            ))
            .into());
        }

        Ok(RegistryUsage {
            entries,
            retained_bytes,
        })
    }

    fn read_state(&self) -> std::sync::RwLockReadGuard<'_, RegistryState> {
        self.state.read().expect("registry state lock poisoned")
    }

    fn write_state(&self) -> std::sync::RwLockWriteGuard<'_, RegistryState> {
        self.state.write().expect("registry state lock poisoned")
    }

    fn audit_change(
        &self,
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
            AuditEvent::DistributedRegistryChange,
            action,
            result,
            reason,
            subject,
            target,
        ));
    }
}

#[derive(Debug)]
struct InvalidRegistryNameError(&'static str);

impl fmt::Display for InvalidRegistryNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for InvalidRegistryNameError {}

fn registration_audit_outcome(result: &Result<()>) -> (AuditResult, AuditReason) {
    match result {
        Ok(()) => (AuditResult::Succeeded, AuditReason::Completed),
        Err(error) if error.downcast_ref::<RegistryCapacityError>().is_some() => {
            (AuditResult::Denied, AuditReason::QuotaExceeded)
        }
        Err(error) if error.downcast_ref::<InvalidRegistryNameError>().is_some() => {
            (AuditResult::Denied, AuditReason::InvalidInput)
        }
        Err(_) => (AuditResult::Denied, AuditReason::Conflict),
    }
}

fn retained_bytes_for_name(name: &ProcessName) -> Result<usize> {
    name.as_str().len().checked_mul(2).ok_or_else(|| {
        RegistryCapacityError::new("Registry retained-byte accounting overflow").into()
    })
}

fn retained_bytes_for_names<'a>(mut names: impl Iterator<Item = &'a ProcessName>) -> Result<usize> {
    names.try_fold(0usize, |retained, name| {
        retained
            .checked_add(retained_bytes_for_name(name)?)
            .ok_or_else(|| {
                RegistryCapacityError::new("Registry retained-byte accounting overflow").into()
            })
    })
}

fn add_reverse_mapping(
    state: &mut RegistryState,
    global_pid: GlobalProcessId,
    scope: RegistrationScope,
    name: ProcessName,
) {
    add_to_reverse_map(&mut state.reverse, global_pid, scope, name);
}

fn add_to_reverse_map(
    reverse: &mut HashMap<GlobalProcessId, HashSet<ScopedProcessName>>,
    global_pid: GlobalProcessId,
    scope: RegistrationScope,
    name: ProcessName,
) {
    reverse
        .entry(global_pid)
        .or_default()
        .insert(ScopedProcessName { scope, name });
}

fn remove_reverse_mapping(
    state: &mut RegistryState,
    global_pid: GlobalProcessId,
    scope: RegistrationScope,
    name: &ProcessName,
) {
    let registration = ScopedProcessName {
        scope,
        name: name.clone(),
    };
    let remove_pid = if let Some(registrations) = state.reverse.get_mut(&global_pid) {
        registrations.remove(&registration);
        registrations.is_empty()
    } else {
        false
    };
    if remove_pid {
        state.reverse.remove(&global_pid);
    }
}

fn remove_scoped_registration(
    state: &mut RegistryState,
    scope: RegistrationScope,
    name: &ProcessName,
    expected_pid: Option<GlobalProcessId>,
) -> Option<RegistryEntry> {
    let entries = match scope {
        RegistrationScope::Local => &mut state.local,
        RegistrationScope::Global => &mut state.global,
    };
    let entry = entries.get(name)?;
    if expected_pid.is_some_and(|expected| entry.global_pid != expected) {
        return None;
    }
    let entry = entries.remove(name)?;

    remove_reverse_mapping(state, entry.global_pid, scope, name);
    let retained_bytes = name
        .as_str()
        .len()
        .checked_mul(2)
        .expect("validated registry name accounting overflow");
    state.usage.entries = state
        .usage
        .entries
        .checked_sub(1)
        .expect("registry entry accounting underflow");
    state.usage.retained_bytes = state
        .usage
        .retained_bytes
        .checked_sub(retained_bytes)
        .expect("registry retained-byte accounting underflow");
    Some(entry)
}

fn scope_order(scope: RegistrationScope) -> u8 {
    match scope {
        RegistrationScope::Local => 0,
        RegistrationScope::Global => 1,
    }
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

    fn limits(max_entries: usize, max_retained_bytes: usize) -> RegistryLimits {
        RegistryLimits {
            max_entries,
            max_retained_bytes,
            ..RegistryLimits::default()
        }
    }

    #[test]
    fn snapshot_rejection_is_atomic() {
        let registry = DistributedRegistry::with_limits(1, limits(2, 20));
        let local = GlobalProcessId::new(1, 1, 1);
        let global = GlobalProcessId::new(2, 1, 1);
        registry.register_local("aa", local).unwrap();
        registry.register_global("bb", global).unwrap();
        let baseline = registry.global_snapshot();
        let usage = registry.usage();

        let error = registry
            .replace_global_snapshot(vec![("cccc".into(), global, 1), ("dddd".into(), global, 2)])
            .unwrap_err();
        assert!(error.downcast_ref::<RegistryCapacityError>().is_some());
        assert_eq!(registry.global_snapshot(), baseline);
        assert_eq!(registry.usage(), usage);

        registry
            .replace_global_snapshot(vec![
                ("duplicate".into(), global, 1),
                ("duplicate".into(), local, 2),
            ])
            .unwrap_err();
        assert_eq!(registry.global_snapshot(), baseline);
        assert_eq!(registry.usage(), usage);
    }

    #[test]
    fn repeated_snapshot_rebuilds_reverse_and_usage() {
        let registry = DistributedRegistry::new(1);
        let local = GlobalProcessId::new(1, 1, 1);
        let old = GlobalProcessId::new(2, 1, 1);
        let new = GlobalProcessId::new(3, 1, 1);
        registry.register_local("same", local).unwrap();
        let local_usage = registry.usage();

        for _ in 0..4 {
            registry
                .replace_global_snapshot(vec![("same".into(), old, 1)])
                .unwrap();
            assert_eq!(registry.global_names_for_process(old).len(), 1);
            registry
                .replace_global_snapshot(vec![("same".into(), new, 2)])
                .unwrap();
            assert!(registry.global_names_for_process(old).is_empty());
            assert_eq!(registry.global_names_for_process(new).len(), 1);
            registry.replace_global_snapshot(Vec::new()).unwrap();
            assert_eq!(registry.usage(), local_usage);
            assert_eq!(registry.lookup_local("same").unwrap().global_pid, local);
        }
    }

    #[test]
    fn capacity_errors_have_quota_audit_semantics() {
        let result = Err(RegistryCapacityError::new("full").into());
        assert_eq!(
            registration_audit_outcome(&result),
            (AuditResult::Denied, AuditReason::QuotaExceeded)
        );
    }
}
