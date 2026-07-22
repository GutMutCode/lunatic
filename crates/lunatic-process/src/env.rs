use std::{
    any::Any,
    fmt,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, RwLock, Weak,
    },
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};

use crate::{state::SignalSendError, Process, Signal};

/// The maximum number of processes admitted to a newly-created environment.
pub const DEFAULT_MAX_PROCESSES: usize = 100_000;
/// The maximum number of simultaneously live environments on a node.
pub const DEFAULT_MAX_ENVIRONMENTS: usize = 1_024;
/// The maximum aggregate number of simultaneously live processes on a node.
pub const DEFAULT_MAX_NODE_PROCESSES: usize = 100_000;

const FIRST_PROCESS_ID: u64 = 1;
static NEXT_STANDALONE_PROCESS_ID: AtomicU64 = AtomicU64::new(FIRST_PROCESS_ID);

enum ProcessIdAllocator {
    Standalone,
    Node(Arc<AtomicU64>),
}

impl ProcessIdAllocator {
    fn next(&self) -> u64 {
        let next_process_id = match self {
            Self::Standalone => &NEXT_STANDALONE_PROCESS_ID,
            Self::Node(next_process_id) => next_process_id,
        };
        next_process_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("process ID allocator exhausted")
    }
}

/// A process could not be admitted because its environment reached its
/// configured process ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessLimitReached {
    environment_id: u64,
    limit: usize,
}

impl ProcessLimitReached {
    pub const fn new(environment_id: u64, limit: usize) -> Self {
        Self {
            environment_id,
            limit,
        }
    }

    pub const fn environment_id(&self) -> u64 {
        self.environment_id
    }

    pub const fn limit(&self) -> usize {
        self.limit
    }
}

impl fmt::Display for ProcessLimitReached {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "environment {} process limit {} reached",
            self.environment_id, self.limit
        )
    }
}

impl std::error::Error for ProcessLimitReached {}

/// A new environment could not be admitted because the node reached its
/// configured environment ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnvironmentLimitReached {
    environment_id: u64,
    limit: usize,
}

impl EnvironmentLimitReached {
    pub const fn new(environment_id: u64, limit: usize) -> Self {
        Self {
            environment_id,
            limit,
        }
    }

    pub const fn environment_id(&self) -> u64 {
        self.environment_id
    }

    pub const fn limit(&self) -> usize {
        self.limit
    }
}

impl fmt::Display for EnvironmentLimitReached {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "environment {} was denied because the node environment limit {} was reached",
            self.environment_id, self.limit
        )
    }
}

impl std::error::Error for EnvironmentLimitReached {}

/// A process could not be admitted because the node reached its aggregate
/// process ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeProcessLimitReached {
    environment_id: u64,
    limit: usize,
}

impl NodeProcessLimitReached {
    pub const fn new(environment_id: u64, limit: usize) -> Self {
        Self {
            environment_id,
            limit,
        }
    }

    pub const fn environment_id(&self) -> u64 {
        self.environment_id
    }

    pub const fn limit(&self) -> usize {
        self.limit
    }
}

impl fmt::Display for NodeProcessLimitReached {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "environment {} process was denied because the node aggregate process limit {} was reached",
            self.environment_id, self.limit
        )
    }
}

impl std::error::Error for NodeProcessLimitReached {}

#[async_trait]
pub trait Environment: Send + Sync {
    fn id(&self) -> u64;

    /// Allocates a process ID for this environment identity.
    ///
    /// Implementations that support recreating an environment ID must not reuse an allocated ID
    /// in a later generation while the allocator remains live.
    ///
    /// # Panics
    ///
    /// Implementations may panic when their process-ID space is exhausted rather than wrapping.
    fn get_next_process_id(&self) -> u64;
    fn get_process(&self, id: u64) -> Option<Arc<dyn Process>>;

    /// Atomically admits and registers a process.
    ///
    /// Implementations must reject duplicate IDs and must not insert the process when admission
    /// fails. Callers that own process lifetime should prefer [`register_process`], which returns
    /// a guard that unregisters the process when dropped.
    fn add_process(&self, id: u64, proc: Arc<dyn Process>) -> Result<()>;

    /// Removes a process, returning whether it was registered.
    fn remove_process(&self, id: u64) -> bool;

    fn process_count(&self) -> usize;

    /// Performs an advisory quota check.
    ///
    /// Admission is enforced by [`Environment::add_process`]; a successful result here does not
    /// reserve capacity.
    async fn can_spawn_next_process(&self) -> Result<Option<()>>;
    /// Sends one signal while preserving ownership on missing, closed, or
    /// backpressured destinations.
    fn send(&self, id: u64, signal: Signal) -> std::result::Result<(), SignalSendError>;

    /// Send a signal to all processes in this environment
    fn send_to_all(&self, signal: Signal);

    /// Get all process IDs for a given module
    /// Default implementation returns empty vector (for environments that don't track modules)
    fn get_processes_for_module(&self, _module_id: u64) -> Vec<u64> {
        Vec::new()
    }

    /// Get the module registry for hot reload operations
    /// Default implementation returns None
    fn get_module_registry(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
}

/// Owns a process's membership in an [`Environment`].
///
/// Dropping this guard unregisters the process. Moving it into a spawned future therefore also
/// releases capacity when that future is aborted or dropped before it is first polled.
#[must_use = "dropping the registration immediately unregisters the process"]
pub struct ProcessRegistration {
    environment: Option<Arc<dyn Environment>>,
    process_id: u64,
    exit_hook: Option<Arc<dyn ProcessExitHook>>,
}

/// A host-owned callback that runs exactly once when process membership ends.
///
/// Implementations must return quickly; asynchronous cleanup should be
/// scheduled onto the runtime instead of blocking the lifecycle path.
pub trait ProcessExitHook: Send + Sync {
    fn process_exited(&self);
}

impl ProcessRegistration {
    /// Atomically registers `process` and returns its lifetime guard.
    pub fn register(
        environment: Arc<dyn Environment>,
        process_id: u64,
        process: Arc<dyn Process>,
    ) -> Result<Self> {
        environment.add_process(process_id, process)?;
        Ok(Self {
            environment: Some(environment),
            process_id,
            exit_hook: None,
        })
    }

    pub fn register_with_exit_hook(
        environment: Arc<dyn Environment>,
        process_id: u64,
        process: Arc<dyn Process>,
        exit_hook: Option<Arc<dyn ProcessExitHook>>,
    ) -> Result<Self> {
        environment.add_process(process_id, process)?;
        Ok(Self {
            environment: Some(environment),
            process_id,
            exit_hook,
        })
    }

    pub fn process_id(&self) -> u64 {
        self.process_id
    }

    /// Unregisters the process now and disarms drop cleanup.
    pub fn unregister(mut self) -> bool {
        self.unregister_inner()
    }

    fn unregister_inner(&mut self) -> bool {
        let removed = self
            .environment
            .take()
            .map(|environment| environment.remove_process(self.process_id))
            .unwrap_or(false);
        if let Some(exit_hook) = self.exit_hook.take() {
            exit_hook.process_exited();
        }
        removed
    }
}

impl fmt::Debug for ProcessRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessRegistration")
            .field(
                "environment_id",
                &self.environment.as_ref().map(|env| env.id()),
            )
            .field("process_id", &self.process_id)
            .field("has_exit_hook", &self.exit_hook.is_some())
            .finish()
    }
}

impl Drop for ProcessRegistration {
    fn drop(&mut self) {
        self.unregister_inner();
    }
}

/// Registers a process and returns a guard that owns its environment membership.
pub fn register_process(
    environment: Arc<dyn Environment>,
    process_id: u64,
    process: Arc<dyn Process>,
) -> Result<ProcessRegistration> {
    ProcessRegistration::register(environment, process_id, process)
}

#[async_trait]
pub trait Environments: Send + Sync {
    type Env: Environment;

    /// Creates the environment or returns its currently live instance.
    ///
    /// Registries may retain environments weakly. Callers must retain the returned [`Arc`] for as
    /// long as the environment should remain live and discoverable through [`Environments::get`].
    async fn create(&self, id: u64) -> Result<Arc<Self::Env>>;

    /// Returns the environment only while some caller still owns it.
    ///
    /// Callers must retain the returned [`Arc`] to keep a weakly registered environment live and
    /// discoverable by later lookups.
    async fn get(&self, id: u64) -> Option<Arc<Self::Env>>;

    /// Creates a registry-backed environment or returns its currently live instance.
    ///
    /// Registries may retain environments weakly. Callers must retain the returned [`Arc`] for as
    /// long as the environment should remain live and discoverable through [`Environments::get`].
    async fn create_with_registry(
        &self,
        id: u64,
        registry: Arc<dyn Any + Send + Sync>,
    ) -> Result<Arc<Self::Env>>;
}

struct NodeProcessQuota {
    count: AtomicUsize,
    limit: usize,
}

impl NodeProcessQuota {
    fn new(limit: usize) -> Self {
        Self {
            count: AtomicUsize::new(0),
            limit,
        }
    }

    fn reserve(self: &Arc<Self>, environment_id: u64) -> Result<NodeProcessLease> {
        self.count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.limit).then(|| count + 1)
            })
            .map_err(|_| NodeProcessLimitReached::new(environment_id, self.limit))?;
        #[cfg(feature = "metrics")]
        metrics::increment_gauge!("lunatic.process.node.process.count", 1.0);
        Ok(NodeProcessLease {
            quota: self.clone(),
        })
    }
}

struct NodeProcessLease {
    quota: Arc<NodeProcessQuota>,
}

impl Drop for NodeProcessLease {
    fn drop(&mut self) {
        self.quota
            .count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_sub(1)
            })
            .expect("admitted node process count cannot underflow");
        #[cfg(feature = "metrics")]
        metrics::decrement_gauge!("lunatic.process.node.process.count", 1.0);
    }
}

struct EnvironmentProcessMetricLease {
    _environment_id: u64,
}

impl EnvironmentProcessMetricLease {
    fn new(environment_id: u64) -> Self {
        #[cfg(all(feature = "metrics", not(feature = "detailed_metrics")))]
        let labels: [(String, String); 0] = [];
        #[cfg(all(feature = "metrics", feature = "detailed_metrics"))]
        let labels = [("environment_id", environment_id.to_string())];
        #[cfg(feature = "metrics")]
        metrics::increment_gauge!("lunatic.process.environment.process.count", 1.0, &labels);
        Self {
            _environment_id: environment_id,
        }
    }
}

impl Drop for EnvironmentProcessMetricLease {
    fn drop(&mut self) {
        #[cfg(all(feature = "metrics", not(feature = "detailed_metrics")))]
        let labels: [(String, String); 0] = [];
        #[cfg(all(feature = "metrics", feature = "detailed_metrics"))]
        let labels = [("environment_id", self._environment_id.to_string())];
        #[cfg(feature = "metrics")]
        metrics::decrement_gauge!("lunatic.process.environment.process.count", 1.0, &labels);
    }
}

struct ProcessEntry {
    process: Arc<dyn Process>,
    // The node-wide capacity follows the map entry, not a spawn call stack. It is therefore
    // returned on every process-exit path, including task cancellation and environment teardown.
    _node_lease: Option<NodeProcessLease>,
    // The per-environment metric follows the map entry too, so dropping an
    // environment with registered processes cannot leave the gauge elevated.
    _environment_metric_lease: EnvironmentProcessMetricLease,
}

#[derive(Clone)]
pub struct LunaticEnvironment {
    inner: Arc<LunaticEnvironmentInner>,
}

struct LunaticEnvironmentInner {
    environment_id: u64,
    process_id_allocator: ProcessIdAllocator,
    processes: Arc<DashMap<u64, ProcessEntry>>,
    process_count: Arc<AtomicUsize>,
    max_processes: Option<usize>,
    node_process_quota: Option<Arc<NodeProcessQuota>>,
    module_registry: RwLock<Option<Arc<dyn Any + Send + Sync>>>,
    // Registry cleanup happens after the final externally-owned Arc disappears.
    _lifecycle: Option<Arc<EnvironmentLifetime>>,
}

impl LunaticEnvironment {
    /// Creates a standalone environment with [`DEFAULT_MAX_PROCESSES`] capacity.
    ///
    /// Use [`LunaticEnvironment::unlimited`] only when the absence of a process limit is explicit.
    pub fn new(id: u64) -> Self {
        Self::with_max_processes(id, DEFAULT_MAX_PROCESSES)
    }

    pub fn with_max_processes(id: u64, max_processes: usize) -> Self {
        Self::with_options(id, Some(max_processes), None)
    }

    /// Explicitly creates a standalone environment without a process limit.
    pub fn unlimited(id: u64) -> Self {
        Self::with_options(id, None, None)
    }

    pub fn with_module_registry(id: u64, registry: Arc<dyn Any + Send + Sync>) -> Self {
        Self::with_options(id, Some(DEFAULT_MAX_PROCESSES), Some(registry))
    }

    pub fn with_module_registry_and_max_processes(
        id: u64,
        registry: Arc<dyn Any + Send + Sync>,
        max_processes: usize,
    ) -> Self {
        Self::with_options(id, Some(max_processes), Some(registry))
    }

    /// Creates a registry-backed environment without a process limit.
    pub fn with_module_registry_unlimited(id: u64, registry: Arc<dyn Any + Send + Sync>) -> Self {
        Self::with_options(id, None, Some(registry))
    }

    fn with_options(
        id: u64,
        max_processes: Option<usize>,
        module_registry: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Self {
        Self::with_node_options(
            id,
            max_processes,
            ProcessIdAllocator::Standalone,
            None,
            module_registry,
            None,
        )
    }

    fn with_node_options(
        id: u64,
        max_processes: Option<usize>,
        process_id_allocator: ProcessIdAllocator,
        node_process_quota: Option<Arc<NodeProcessQuota>>,
        module_registry: Option<Arc<dyn Any + Send + Sync>>,
        lifecycle: Option<Arc<EnvironmentLifetime>>,
    ) -> Self {
        Self {
            inner: Arc::new(LunaticEnvironmentInner {
                environment_id: id,
                processes: Arc::new(DashMap::new()),
                process_count: Arc::new(AtomicUsize::new(0)),
                process_id_allocator,
                max_processes,
                node_process_quota,
                module_registry: RwLock::new(module_registry),
                _lifecycle: lifecycle,
            }),
        }
    }

    pub fn max_processes(&self) -> Option<usize> {
        self.inner.max_processes
    }

    pub fn set_module_registry(&mut self, registry: Arc<dyn Any + Send + Sync>) {
        *self
            .inner
            .module_registry
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(registry);
    }

    pub fn kill_all_processes(&self) {
        for entry in self.inner.processes.iter() {
            let _ = entry.value().process.send(Signal::Kill);
        }
    }

    fn reserve_process_slot(&self) -> Result<()> {
        match self.inner.max_processes {
            Some(limit) => self
                .inner
                .process_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    (count < limit).then(|| count + 1)
                })
                .map(|_| ())
                .map_err(|_| ProcessLimitReached::new(self.inner.environment_id, limit).into()),
            None => self
                .inner
                .process_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_add(1)
                })
                .map(|_| ())
                .map_err(|_| {
                    anyhow!(
                        "environment {} process count overflow",
                        self.inner.environment_id
                    )
                }),
        }
    }

    fn release_process_slot(&self) {
        self.inner
            .process_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_sub(1)
            })
            .expect("registered process count cannot underflow");
    }
}

#[async_trait]
impl Environment for LunaticEnvironment {
    fn get_process(&self, id: u64) -> Option<Arc<dyn Process>> {
        self.inner
            .processes
            .get(&id)
            .map(|entry| entry.process.clone())
    }

    fn add_process(&self, id: u64, proc: Arc<dyn Process>) -> Result<()> {
        match self.inner.processes.entry(id) {
            Entry::Occupied(_) => Err(anyhow!(
                "process {} is already registered in environment {}",
                id,
                self.inner.environment_id
            )),
            Entry::Vacant(entry) => {
                self.reserve_process_slot()?;
                let node_lease = match &self.inner.node_process_quota {
                    Some(quota) => match quota.reserve(self.inner.environment_id) {
                        Ok(lease) => Some(lease),
                        Err(error) => {
                            self.release_process_slot();
                            return Err(error);
                        }
                    },
                    None => None,
                };
                entry.insert(ProcessEntry {
                    process: proc,
                    _node_lease: node_lease,
                    _environment_metric_lease: EnvironmentProcessMetricLease::new(
                        self.inner.environment_id,
                    ),
                });
                Ok(())
            }
        }
    }

    fn remove_process(&self, id: u64) -> bool {
        let removed = match self.inner.processes.remove(&id) {
            Some(removed) => removed,
            None => return false,
        };

        self.release_process_slot();
        // Dropping the entry returns its node-wide lease and environment
        // metric lease.
        drop(removed);
        true
    }

    fn process_count(&self) -> usize {
        self.inner.process_count.load(Ordering::Acquire)
    }

    fn send(&self, id: u64, signal: Signal) -> std::result::Result<(), SignalSendError> {
        if let Some(proc) = self.inner.processes.get(&id) {
            proc.process.send(signal)
        } else {
            Err(SignalSendError::Closed(signal))
        }
    }

    fn send_to_all(&self, signal: Signal) {
        match signal {
            Signal::Kill => {
                for entry in self.inner.processes.iter() {
                    let _ = entry.value().process.send(Signal::Kill);
                }
            }
            Signal::HotReload {
                module_id,
                expected_version,
                new_version,
                acknowledgement,
            } => {
                if acknowledgement.is_some() {
                    log::warn!(
                        "A single acknowledgement channel cannot be broadcast; use ReloadCoordinator"
                    );
                }
                for entry in self.inner.processes.iter() {
                    let _ = entry.value().process.send(Signal::HotReload {
                        module_id,
                        expected_version,
                        new_version,
                        acknowledgement: None,
                    });
                }
            }
            Signal::DieWhenLinkDies(flag) => {
                for entry in self.inner.processes.iter() {
                    let _ = entry.value().process.send(Signal::DieWhenLinkDies(flag));
                }
            }
            _ => {
                // Other signals don't make sense to broadcast
                log::warn!("Attempted to broadcast non-broadcastable signal");
            }
        }
    }

    fn get_next_process_id(&self) -> u64 {
        self.inner.process_id_allocator.next()
    }

    fn id(&self) -> u64 {
        self.inner.environment_id
    }

    async fn can_spawn_next_process(&self) -> Result<Option<()>> {
        if let Some(limit) = self.inner.max_processes {
            if self.inner.process_count.load(Ordering::Acquire) >= limit {
                return Err(ProcessLimitReached::new(self.inner.environment_id, limit).into());
            }
        }
        if let Some(quota) = &self.inner.node_process_quota {
            if quota.count.load(Ordering::Acquire) >= quota.limit {
                return Err(
                    NodeProcessLimitReached::new(self.inner.environment_id, quota.limit).into(),
                );
            }
        }

        Ok(Some(()))
    }

    fn get_module_registry(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.inner
            .module_registry
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
}

struct StoredEnvironment {
    generation: u64,
    canonical: Weak<LunaticEnvironment>,
    inner: Weak<LunaticEnvironmentInner>,
}

struct EnvironmentRegistryInner {
    envs: DashMap<u64, StoredEnvironment>,
    environment_count: AtomicUsize,
    max_environments: usize,
    max_processes_per_environment: usize,
    node_process_quota: Arc<NodeProcessQuota>,
    next_process_id: Arc<AtomicU64>,
    next_generation: AtomicU64,
}

impl EnvironmentRegistryInner {
    fn reserve_environment_slot(&self, environment_id: u64) -> Result<()> {
        self.environment_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.max_environments).then(|| count + 1)
            })
            .map_err(|_| EnvironmentLimitReached::new(environment_id, self.max_environments))?;
        #[cfg(feature = "metrics")]
        metrics::increment_gauge!("lunatic.process.environment.count", 1.0);
        Ok(())
    }

    fn release_environment_slot(&self) {
        self.environment_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_sub(1)
            })
            .expect("admitted environment count cannot underflow");
        #[cfg(feature = "metrics")]
        metrics::decrement_gauge!("lunatic.process.environment.count", 1.0);
    }

    fn next_generation(&self) -> Result<u64> {
        self.next_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                generation.checked_add(1)
            })
            .map_err(|_| anyhow!("environment generation counter exhausted"))
    }

    fn release_generation(&self, environment_id: u64, generation: u64) -> bool {
        if let Entry::Occupied(entry) = self.envs.entry(environment_id) {
            if entry.get().generation == generation {
                // Return capacity before exposing a vacant map entry. A same-ID creator can then
                // either transfer the still-charged occupied entry or reserve the newly vacant
                // slot, without observing a transient false limit failure between those states.
                self.release_environment_slot();
                entry.remove();
                return true;
            }
        }
        false
    }
}

struct EnvironmentLifetime {
    registry: Arc<EnvironmentRegistryInner>,
    environment_id: u64,
    generation: u64,
}

impl Drop for EnvironmentLifetime {
    fn drop(&mut self) {
        self.registry
            .release_generation(self.environment_id, self.generation);
    }
}

/// A bounded node environment registry.
///
/// The registry stores weak references: callers must retain an [`Arc`] returned by
/// [`Environments::create`], [`Environments::create_with_registry`], or [`Environments::get`] to
/// keep that environment live and discoverable. Once its last owner is dropped, the ID may be
/// created again as a new environment generation.
#[derive(Clone)]
pub struct LunaticEnvironments {
    inner: Arc<EnvironmentRegistryInner>,
}

impl Default for LunaticEnvironments {
    fn default() -> Self {
        Self::with_limits(
            DEFAULT_MAX_ENVIRONMENTS,
            DEFAULT_MAX_PROCESSES,
            DEFAULT_MAX_NODE_PROCESSES,
        )
    }
}

impl LunaticEnvironments {
    pub fn with_limits(
        max_environments: usize,
        max_processes_per_environment: usize,
        max_node_processes: usize,
    ) -> Self {
        Self {
            inner: Arc::new(EnvironmentRegistryInner {
                envs: DashMap::new(),
                environment_count: AtomicUsize::new(0),
                max_environments,
                max_processes_per_environment,
                node_process_quota: Arc::new(NodeProcessQuota::new(max_node_processes)),
                next_process_id: Arc::new(AtomicU64::new(FIRST_PROCESS_ID)),
                next_generation: AtomicU64::new(1),
            }),
        }
    }

    pub fn environment_count(&self) -> usize {
        self.inner.environment_count.load(Ordering::Acquire)
    }

    pub fn node_process_count(&self) -> usize {
        self.inner.node_process_quota.count.load(Ordering::Acquire)
    }

    pub fn max_environments(&self) -> usize {
        self.inner.max_environments
    }

    pub fn max_processes_per_environment(&self) -> usize {
        self.inner.max_processes_per_environment
    }

    pub fn max_node_processes(&self) -> usize {
        self.inner.node_process_quota.limit
    }

    fn validate_registry(
        environment: &Arc<LunaticEnvironment>,
        registry: Option<&Arc<dyn Any + Send + Sync>>,
    ) -> Result<()> {
        let Some(requested_registry) = registry else {
            return Ok(());
        };
        match environment.get_module_registry() {
            Some(existing_registry) if Arc::ptr_eq(&existing_registry, requested_registry) => {
                Ok(())
            }
            Some(_) => Err(anyhow!(
                "environment {} already exists with a different module registry",
                environment.id()
            )),
            None => Err(anyhow!(
                "environment {} already exists without a module registry",
                environment.id()
            )),
        }
    }

    fn build_environment(
        &self,
        id: u64,
        generation: u64,
        registry: Option<Arc<dyn Any + Send + Sync>>,
        reserve_slot: bool,
    ) -> Result<Arc<LunaticEnvironment>> {
        if reserve_slot {
            self.inner.reserve_environment_slot(id)?;
        }
        let lifecycle = Arc::new(EnvironmentLifetime {
            registry: self.inner.clone(),
            environment_id: id,
            generation,
        });
        Ok(Arc::new(LunaticEnvironment::with_node_options(
            id,
            Some(self.inner.max_processes_per_environment),
            ProcessIdAllocator::Node(self.inner.next_process_id.clone()),
            Some(self.inner.node_process_quota.clone()),
            registry,
            Some(lifecycle),
        )))
    }

    fn create_atomic(
        &self,
        id: u64,
        registry: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Result<Arc<LunaticEnvironment>> {
        match self.inner.envs.entry(id) {
            Entry::Occupied(mut entry) => {
                if let Some(environment) = entry.get().canonical.upgrade() {
                    // Never drop the final strong reference while holding the DashMap shard: the
                    // environment's lifecycle cleanup needs to lock this same entry.
                    drop(entry);
                    Self::validate_registry(&environment, registry.as_ref())?;
                    return Ok(environment);
                }
                if let Some(inner) = entry.get().inner.upgrade() {
                    // A direct value clone can keep the shared inner alive after the prior outer
                    // Arc disappears. Rebuild one canonical outer without charging a new slot.
                    let environment = Arc::new(LunaticEnvironment { inner });
                    entry.get_mut().canonical = Arc::downgrade(&environment);
                    drop(entry);
                    Self::validate_registry(&environment, registry.as_ref())?;
                    return Ok(environment);
                }

                let generation = self.inner.next_generation()?;
                // The dead generation still owns one charged slot while its destructor waits on
                // this entry lock. Transfer that slot to the replacement; exact-generation drop
                // cleanup prevents the old lifecycle from releasing the replacement's charge.
                let environment = self.build_environment(id, generation, registry, false)?;
                entry.insert(StoredEnvironment {
                    generation,
                    canonical: Arc::downgrade(&environment),
                    inner: Arc::downgrade(&environment.inner),
                });
                Ok(environment)
            }
            Entry::Vacant(entry) => {
                let generation = self.inner.next_generation()?;
                let environment = self.build_environment(id, generation, registry, true)?;
                entry.insert(StoredEnvironment {
                    generation,
                    canonical: Arc::downgrade(&environment),
                    inner: Arc::downgrade(&environment.inner),
                });
                Ok(environment)
            }
        }
    }
}

#[async_trait]
impl Environments for LunaticEnvironments {
    type Env = LunaticEnvironment;

    async fn create(&self, id: u64) -> Result<Arc<Self::Env>> {
        self.create_atomic(id, None)
    }

    async fn create_with_registry(
        &self,
        id: u64,
        registry: Arc<dyn Any + Send + Sync>,
    ) -> Result<Arc<Self::Env>> {
        self.create_atomic(id, Some(registry))
    }

    async fn get(&self, id: u64) -> Option<Arc<Self::Env>> {
        match self.inner.envs.entry(id) {
            Entry::Occupied(mut entry) => {
                if let Some(environment) = entry.get().canonical.upgrade() {
                    return Some(environment);
                }
                let inner = entry.get().inner.upgrade()?;
                let environment = Arc::new(LunaticEnvironment { inner });
                entry.get_mut().canonical = Arc::downgrade(&environment);
                Some(environment)
            }
            Entry::Vacant(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Barrier;

    #[derive(Debug)]
    struct TestProcess(u64);

    impl Process for TestProcess {
        fn id(&self) -> u64 {
            self.0
        }

        fn send(&self, _signal: Signal) -> std::result::Result<(), crate::state::SignalSendError> {
            Ok(())
        }
    }

    fn process(id: u64) -> Arc<dyn Process> {
        Arc::new(TestProcess(id))
    }

    fn environment_with_limit(id: u64, limit: usize) -> Arc<dyn Environment> {
        Arc::new(LunaticEnvironment::with_max_processes(id, limit))
    }

    fn same_environment(left: &Arc<LunaticEnvironment>, right: &Arc<LunaticEnvironment>) -> bool {
        Arc::ptr_eq(&left.inner, &right.inner)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_registration_enforces_the_exact_limit() {
        const ATTEMPTS: usize = 1_000;
        const LIMIT: usize = 37;

        let environment = environment_with_limit(1, LIMIT);
        let barrier = Arc::new(Barrier::new(ATTEMPTS + 1));
        let mut attempts = Vec::with_capacity(ATTEMPTS);

        for id in 0..ATTEMPTS as u64 {
            let environment = environment.clone();
            let barrier = barrier.clone();
            attempts.push(tokio::spawn(async move {
                barrier.wait().await;
                (id, register_process(environment, id, process(id)))
            }));
        }

        barrier.wait().await;
        let mut registrations = Vec::new();
        let mut rejected = Vec::new();
        for attempt in attempts {
            let (id, result) = attempt.await.unwrap();
            match result {
                Ok(registration) => registrations.push(registration),
                Err(_) => rejected.push(id),
            }
        }

        assert_eq!(registrations.len(), LIMIT);
        assert_eq!(rejected.len(), ATTEMPTS - LIMIT);
        assert_eq!(environment.process_count(), LIMIT);
        for id in rejected {
            assert!(environment.get_process(id).is_none());
        }

        drop(registrations);
        assert_eq!(environment.process_count(), 0);
    }

    #[tokio::test]
    async fn rejection_does_not_insert_and_released_capacity_can_be_reused() {
        let environment = environment_with_limit(2, 1);
        let first = register_process(environment.clone(), 10, process(10)).unwrap();

        assert!(register_process(environment.clone(), 11, process(11)).is_err());
        assert!(environment.get_process(11).is_none());
        assert_eq!(environment.process_count(), 1);
        assert!(environment.can_spawn_next_process().await.is_err());

        assert!(first.unregister());
        assert_eq!(environment.process_count(), 0);
        assert!(environment.can_spawn_next_process().await.is_ok());

        let second = register_process(environment.clone(), 11, process(11)).unwrap();
        assert!(environment.get_process(11).is_some());
        drop(second);
        assert_eq!(environment.process_count(), 0);
    }

    #[test]
    fn duplicate_registration_and_repeated_removal_do_not_underflow() {
        let environment = environment_with_limit(3, 1);
        let registration =
            register_process(environment.clone(), 20, process(20)).expect("first registration");

        assert!(register_process(environment.clone(), 20, process(20)).is_err());
        assert_eq!(environment.process_count(), 1);
        assert!(!environment.remove_process(999));
        assert_eq!(environment.process_count(), 1);
        assert!(environment.remove_process(20));
        assert_eq!(environment.process_count(), 0);
        assert!(!environment.remove_process(20));
        assert_eq!(environment.process_count(), 0);

        // The guard observes that explicit external removal already happened and remains harmless.
        drop(registration);
        assert_eq!(environment.process_count(), 0);

        let reused = register_process(environment.clone(), 21, process(21)).unwrap();
        assert_eq!(environment.process_count(), 1);
        drop(reused);
    }

    #[test]
    fn registration_guard_unregisters_on_drop() {
        let environment = environment_with_limit(4, 1);
        let registration = register_process(environment.clone(), 30, process(30)).unwrap();
        assert_eq!(registration.process_id(), 30);
        assert!(environment.get_process(30).is_some());

        drop(registration);

        assert!(environment.get_process(30).is_none());
        assert_eq!(environment.process_count(), 0);
    }

    struct CountingExitHook(AtomicUsize);

    impl ProcessExitHook for CountingExitHook {
        fn process_exited(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn process_exit_hook_runs_exactly_once_on_explicit_or_drop_cleanup() {
        let environment = environment_with_limit(40, 2);
        let explicit_hook = Arc::new(CountingExitHook(AtomicUsize::new(0)));
        let explicit = ProcessRegistration::register_with_exit_hook(
            environment.clone(),
            1,
            process(1),
            Some(explicit_hook.clone()),
        )
        .unwrap();
        assert!(explicit.unregister());
        assert_eq!(explicit_hook.0.load(Ordering::Relaxed), 1);

        let drop_hook = Arc::new(CountingExitHook(AtomicUsize::new(0)));
        let dropped = ProcessRegistration::register_with_exit_hook(
            environment,
            2,
            process(2),
            Some(drop_hook.clone()),
        )
        .unwrap();
        drop(dropped);
        assert_eq!(drop_hook.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn registration_captured_by_an_unpolled_future_is_released() {
        let environment = environment_with_limit(5, 1);
        let registration = register_process(environment.clone(), 31, process(31)).unwrap();
        let unpolled = async move {
            let _registration = registration;
            std::future::pending::<()>().await;
        };

        drop(unpolled);

        assert!(environment.get_process(31).is_none());
        assert_eq!(environment.process_count(), 0);
    }

    #[test]
    fn standalone_environment_constructors_do_not_reuse_process_ids() {
        let first = LunaticEnvironment::new(6);
        let stale_process_id = first.get_next_process_id();
        drop(first);

        let replacement = LunaticEnvironment::unlimited(6);
        let replacement_process_id = replacement.get_next_process_id();

        assert_ne!(stale_process_id, replacement_process_id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_environment_creation_is_idempotent() {
        const ATTEMPTS: usize = 1_000;

        let environments = Arc::new(LunaticEnvironments::default());
        let barrier = Arc::new(Barrier::new(ATTEMPTS + 1));
        let mut attempts = Vec::with_capacity(ATTEMPTS);
        for _ in 0..ATTEMPTS {
            let environments = environments.clone();
            let barrier = barrier.clone();
            attempts.push(tokio::spawn(async move {
                barrier.wait().await;
                environments.create(7).await.unwrap()
            }));
        }

        barrier.wait().await;
        let first = attempts.pop().unwrap().await.unwrap();
        for attempt in attempts {
            let environment = attempt.await.unwrap();
            assert!(Arc::ptr_eq(&first, &environment));
            assert!(same_environment(&first, &environment));
        }
        let lookup = environments.get(7).await.expect("created environment");
        assert!(Arc::ptr_eq(&first, &lookup));
        assert!(same_environment(&first, &lookup));
        assert_eq!(environments.environment_count(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_unique_environment_creation_enforces_the_exact_limit() {
        const ATTEMPTS: usize = 256;
        const LIMIT: usize = 23;

        let environments = Arc::new(LunaticEnvironments::with_limits(LIMIT, 10, 100));
        let barrier = Arc::new(Barrier::new(ATTEMPTS + 1));
        let mut attempts = Vec::with_capacity(ATTEMPTS);
        for id in 0..ATTEMPTS as u64 {
            let environments = environments.clone();
            let barrier = barrier.clone();
            attempts.push(tokio::spawn(async move {
                barrier.wait().await;
                environments.create(id).await
            }));
        }

        barrier.wait().await;
        let mut admitted = Vec::new();
        let mut denied = 0;
        for attempt in attempts {
            match attempt.await.unwrap() {
                Ok(environment) => admitted.push(environment),
                Err(error) => {
                    let denial = error.downcast_ref::<EnvironmentLimitReached>().unwrap();
                    assert_eq!(denial.limit(), LIMIT);
                    assert!(denial.environment_id() < ATTEMPTS as u64);
                    denied += 1;
                }
            }
        }

        assert_eq!(admitted.len(), LIMIT);
        assert_eq!(denied, ATTEMPTS - LIMIT);
        assert_eq!(environments.environment_count(), LIMIT);
        drop(admitted);
        assert_eq!(environments.environment_count(), 0);
        assert!(environments.inner.envs.is_empty());
    }

    #[tokio::test]
    async fn failed_environment_admission_rolls_back_and_capacity_is_reusable() {
        let environments = LunaticEnvironments::with_limits(1, 10, 10);
        let first = environments.create(1).await.unwrap();

        let error = match environments.create(2).await {
            Ok(_) => panic!("second environment must exceed the limit"),
            Err(error) => error,
        };
        assert_eq!(
            error.downcast_ref::<EnvironmentLimitReached>(),
            Some(&EnvironmentLimitReached::new(2, 1))
        );
        assert_eq!(environments.environment_count(), 1);
        assert_eq!(environments.inner.envs.len(), 1);

        drop(first);
        assert_eq!(environments.environment_count(), 0);
        assert!(environments.inner.envs.is_empty());

        let replacement = environments.create(2).await.unwrap();
        assert_eq!(replacement.id(), 2);
        assert_eq!(environments.environment_count(), 1);
    }

    #[tokio::test]
    async fn unpolled_create_and_last_arc_drop_do_not_retain_capacity_or_map_storage() {
        let environments = LunaticEnvironments::with_limits(1, 10, 10);
        let abandoned = environments.create(1);
        drop(abandoned);
        assert_eq!(environments.environment_count(), 0);
        assert!(environments.inner.envs.is_empty());

        let environment = environments.create(1).await.unwrap();
        let clone = environment.clone();
        drop(environment);
        assert_eq!(environments.environment_count(), 1);
        assert!(environments.get(1).await.is_some());

        drop(clone);
        assert_eq!(environments.environment_count(), 0);
        assert!(environments.inner.envs.is_empty());
        assert!(environments.get(1).await.is_none());
    }

    #[tokio::test]
    async fn recreated_environment_does_not_reuse_process_ids() {
        let environments = LunaticEnvironments::with_limits(1, 10, 10);
        let first = environments.create(7).await.unwrap();
        let stale_process_id = first.get_next_process_id();

        drop(first);
        assert_eq!(environments.environment_count(), 0);
        assert!(environments.get(7).await.is_none());

        let replacement = environments.create(7).await.unwrap();
        let replacement_process_id = replacement.get_next_process_id();
        assert_ne!(stale_process_id, replacement_process_id);

        replacement
            .add_process(replacement_process_id, process(replacement_process_id))
            .unwrap();
        assert!(replacement.get_process(stale_process_id).is_none());
        let error = replacement
            .send(stale_process_id, Signal::Kill)
            .unwrap_err();
        assert_eq!(
            error.kind(),
            crate::state::SignalSendErrorKind::Closed,
            "a stale process address must not target the replacement generation"
        );
    }

    #[tokio::test]
    async fn value_clone_keeps_the_managed_environment_visible_and_charged_once() {
        let environments = LunaticEnvironments::with_limits(1, 10, 10);
        let canonical = environments.create(7).await.unwrap();
        let canonical_weak = Arc::downgrade(&canonical);
        let second_wrapper = environments.get(7).await.unwrap();
        assert!(Arc::ptr_eq(&canonical, &second_wrapper));
        drop(canonical);
        assert!(canonical_weak.upgrade().is_some());

        let canonical = second_wrapper;
        let value_clone = canonical.as_ref().clone();
        drop(canonical);

        let lookup = environments
            .get(7)
            .await
            .expect("value clone keeps inner live");
        assert!(Arc::ptr_eq(&value_clone.inner, &lookup.inner));
        let same = environments.create(7).await.unwrap();
        assert!(same_environment(&lookup, &same));
        assert_eq!(environments.environment_count(), 1);

        drop(lookup);
        drop(same);
        drop(value_clone);
        assert_eq!(environments.environment_count(), 0);
        assert!(environments.inner.envs.is_empty());
    }

    #[tokio::test]
    async fn stale_generation_cleanup_cannot_remove_a_recreated_environment() {
        let environments = LunaticEnvironments::with_limits(1, 10, 10);
        let first = environments.create(7).await.unwrap();
        let first_generation = environments.inner.envs.get(&7).unwrap().generation;
        drop(first);

        let second = environments.create(7).await.unwrap();
        let second_generation = environments.inner.envs.get(&7).unwrap().generation;
        assert_ne!(first_generation, second_generation);

        assert!(!environments.inner.release_generation(7, first_generation));
        assert!(same_environment(
            &second,
            &environments.get(7).await.expect("new generation retained")
        ));
        assert_eq!(environments.environment_count(), 1);
    }

    #[tokio::test]
    async fn replacement_transfers_the_slot_while_stale_generation_drop_is_blocked() {
        use std::sync::{mpsc, Mutex};

        struct BlockingDropProcess {
            entered_drop: mpsc::Sender<()>,
            release_drop: Mutex<mpsc::Receiver<()>>,
        }

        impl Process for BlockingDropProcess {
            fn id(&self) -> u64 {
                1
            }

            fn send(
                &self,
                _signal: Signal,
            ) -> std::result::Result<(), crate::state::SignalSendError> {
                Ok(())
            }
        }

        impl Drop for BlockingDropProcess {
            fn drop(&mut self) {
                self.entered_drop.send(()).unwrap();
                self.release_drop.get_mut().unwrap().recv().unwrap();
            }
        }

        let environments = LunaticEnvironments::with_limits(1, 1, 1);
        let stale = environments.create(7).await.unwrap();
        let stale_generation = environments.inner.envs.get(&7).unwrap().generation;
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        stale
            .add_process(
                1,
                Arc::new(BlockingDropProcess {
                    entered_drop: entered_tx,
                    release_drop: Mutex::new(release_rx),
                }),
            )
            .unwrap();

        let dropping = std::thread::spawn(move || drop(stale));
        entered_rx.recv().unwrap();

        // The Arc is already dead, but its lifecycle field cannot run until the blocking process
        // entry finishes dropping. The replacement must reuse the occupied generation's charge.
        let replacement = environments.create(7).await.unwrap();
        let replacement_generation = environments.inner.envs.get(&7).unwrap().generation;
        assert_ne!(stale_generation, replacement_generation);
        assert_eq!(environments.environment_count(), 1);

        release_tx.send(()).unwrap();
        dropping.join().unwrap();
        assert_eq!(environments.environment_count(), 1);
        assert!(same_environment(
            &replacement,
            &environments.get(7).await.expect("replacement retained")
        ));

        drop(replacement);
        assert_eq!(environments.environment_count(), 0);
        assert!(environments.inner.envs.is_empty());
    }

    #[tokio::test]
    async fn environment_churn_reuses_one_slot_without_retaining_weak_entries() {
        const ITERATIONS: u64 = 10_000;
        let environments = LunaticEnvironments::with_limits(1, 1, 1);

        for id in 0..ITERATIONS {
            let environment = environments.create(id % 2).await.unwrap();
            assert_eq!(environments.environment_count(), 1);
            assert_eq!(environments.inner.envs.len(), 1);
            drop(environment);
            assert_eq!(environments.environment_count(), 0);
            assert!(environments.inner.envs.is_empty());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn aggregate_process_limit_is_exact_across_environments() {
        const ATTEMPTS: usize = 256;
        const LIMIT: usize = 29;

        let environments = LunaticEnvironments::with_limits(2, ATTEMPTS, LIMIT);
        let first = environments.create(1).await.unwrap();
        let second = environments.create(2).await.unwrap();
        let barrier = Arc::new(Barrier::new(ATTEMPTS + 1));
        let mut attempts = Vec::with_capacity(ATTEMPTS);
        for process_id in 0..ATTEMPTS as u64 {
            let environment: Arc<dyn Environment> = if process_id % 2 == 0 {
                first.clone()
            } else {
                second.clone()
            };
            let barrier = barrier.clone();
            attempts.push(tokio::spawn(async move {
                barrier.wait().await;
                register_process(environment, process_id, process(process_id))
            }));
        }

        barrier.wait().await;
        let mut admitted = Vec::new();
        let mut denied = 0;
        for attempt in attempts {
            match attempt.await.unwrap() {
                Ok(registration) => admitted.push(registration),
                Err(error) => {
                    let denial = error.downcast_ref::<NodeProcessLimitReached>().unwrap();
                    assert_eq!(denial.limit(), LIMIT);
                    assert!(matches!(denial.environment_id(), 1 | 2));
                    denied += 1;
                }
            }
        }

        assert_eq!(admitted.len(), LIMIT);
        assert_eq!(denied, ATTEMPTS - LIMIT);
        assert_eq!(first.process_count() + second.process_count(), LIMIT);
        assert_eq!(environments.node_process_count(), LIMIT);

        drop(admitted);
        assert_eq!(first.process_count() + second.process_count(), 0);
        assert_eq!(environments.node_process_count(), 0);
    }

    #[tokio::test]
    async fn aggregate_rejection_rolls_back_local_count_and_entry_owns_lease() {
        let environments = LunaticEnvironments::with_limits(2, 10, 1);
        let first = environments.create(1).await.unwrap();
        let second = environments.create(2).await.unwrap();

        first.add_process(1, process(1)).unwrap();
        assert_eq!(environments.node_process_count(), 1);
        let error = second.add_process(2, process(2)).unwrap_err();
        assert!(error.downcast_ref::<NodeProcessLimitReached>().is_some());
        assert_eq!(second.process_count(), 0);
        assert!(second.get_process(2).is_none());

        // Direct registration has no external guard. Dropping the owning environment clears the
        // process entry and returns both environment and aggregate-process capacity.
        drop(first);
        assert_eq!(environments.node_process_count(), 0);
        assert_eq!(environments.environment_count(), 1);

        second.add_process(2, process(2)).unwrap();
        assert_eq!(environments.node_process_count(), 1);
        assert_eq!(second.process_count(), 1);
    }

    #[tokio::test]
    async fn aborted_process_owner_returns_aggregate_capacity() {
        let environments = LunaticEnvironments::with_limits(1, 10, 1);
        let environment = environments.create(1).await.unwrap();
        let registration = register_process(environment.clone(), 1, process(1)).unwrap();
        let task = tokio::spawn(async move {
            let _registration = registration;
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        assert_eq!(environments.node_process_count(), 1);

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(environment.process_count(), 0);
        assert_eq!(environments.node_process_count(), 0);

        let reused = register_process(environment, 2, process(2)).unwrap();
        assert_eq!(environments.node_process_count(), 1);
        drop(reused);
        assert_eq!(environments.node_process_count(), 0);
    }

    #[tokio::test]
    async fn registry_aware_creation_is_idempotent_and_rejects_mismatches() {
        #[derive(Debug)]
        struct Registry {
            _value: u8,
        }

        let environments = LunaticEnvironments::default();
        let registry: Arc<dyn Any + Send + Sync> = Arc::new(Registry { _value: 1 });
        let created = environments
            .create_with_registry(8, registry.clone())
            .await
            .unwrap();
        let same = environments
            .create_with_registry(8, registry)
            .await
            .unwrap();
        assert!(same_environment(&created, &same));
        assert!(same_environment(
            &created,
            &environments.create(8).await.unwrap()
        ));
        assert_eq!(environments.environment_count(), 1);

        let different: Arc<dyn Any + Send + Sync> = Arc::new(Registry { _value: 2 });
        assert!(environments
            .create_with_registry(8, different)
            .await
            .is_err());

        let plain_first = LunaticEnvironments::default();
        let plain_environment = plain_first.create(9).await.unwrap();
        let registry: Arc<dyn Any + Send + Sync> = Arc::new(Registry { _value: 3 });
        assert!(plain_first.create_with_registry(9, registry).await.is_err());
        assert_eq!(plain_first.environment_count(), 1);
        drop(plain_environment);
        assert_eq!(plain_first.environment_count(), 0);
    }
}
