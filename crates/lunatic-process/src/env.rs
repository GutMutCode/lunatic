use std::{
    any::Any,
    fmt,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};

use crate::{state::SignalSendError, Process, Signal};

/// The maximum number of processes admitted to a newly-created environment.
pub const DEFAULT_MAX_PROCESSES: usize = 100_000;

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

#[async_trait]
pub trait Environment: Send + Sync {
    fn id(&self) -> u64;
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
        self.environment
            .take()
            .map(|environment| environment.remove_process(self.process_id))
            .unwrap_or(false)
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

    async fn create(&self, id: u64) -> Result<Arc<Self::Env>>;
    async fn get(&self, id: u64) -> Option<Arc<Self::Env>>;
    async fn create_with_registry(
        &self,
        id: u64,
        registry: Arc<dyn Any + Send + Sync>,
    ) -> Result<Arc<Self::Env>>;
}

#[derive(Clone)]
pub struct LunaticEnvironment {
    environment_id: u64,
    next_process_id: Arc<AtomicU64>,
    processes: Arc<DashMap<u64, Arc<dyn Process>>>,
    process_count: Arc<AtomicUsize>,
    max_processes: Option<usize>,
    module_registry: Option<Arc<dyn Any + Send + Sync>>,
}

impl LunaticEnvironment {
    pub fn new(id: u64) -> Self {
        Self::with_max_processes(id, DEFAULT_MAX_PROCESSES)
    }

    pub fn with_max_processes(id: u64, max_processes: usize) -> Self {
        Self::with_options(id, Some(max_processes), None)
    }

    /// Creates an environment without a process limit.
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
        Self {
            environment_id: id,
            processes: Arc::new(DashMap::new()),
            process_count: Arc::new(AtomicUsize::new(0)),
            next_process_id: Arc::new(AtomicU64::new(1)),
            max_processes,
            module_registry,
        }
    }

    pub fn max_processes(&self) -> Option<usize> {
        self.max_processes
    }

    pub fn set_module_registry(&mut self, registry: Arc<dyn Any + Send + Sync>) {
        self.module_registry = Some(registry);
    }

    pub fn kill_all_processes(&self) {
        for entry in self.processes.iter() {
            let _ = entry.value().send(Signal::Kill);
        }
    }

    fn reserve_process_slot(&self) -> Result<usize> {
        match self.max_processes {
            Some(limit) => self
                .process_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    (count < limit).then(|| count + 1)
                })
                .map(|previous| previous + 1)
                .map_err(|_| ProcessLimitReached::new(self.environment_id, limit).into()),
            None => self
                .process_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_add(1)
                })
                .map(|previous| previous + 1)
                .map_err(|_| anyhow!("environment {} process count overflow", self.environment_id)),
        }
    }

    fn release_process_slot(&self) -> usize {
        self.process_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_sub(1)
            })
            .map(|previous| previous - 1)
            .expect("registered process count cannot underflow")
    }

    fn record_process_count(&self, _count: usize) {
        #[cfg(all(feature = "metrics", not(feature = "detailed_metrics")))]
        let labels: [(String, String); 0] = [];
        #[cfg(all(feature = "metrics", feature = "detailed_metrics"))]
        let labels = [("environment_id", self.id().to_string())];
        #[cfg(feature = "metrics")]
        metrics::gauge!(
            "lunatic.process.environment.process.count",
            _count as f64,
            &labels
        );
    }
}

#[async_trait]
impl Environment for LunaticEnvironment {
    fn get_process(&self, id: u64) -> Option<Arc<dyn Process>> {
        self.processes.get(&id).map(|process| process.clone())
    }

    fn add_process(&self, id: u64, proc: Arc<dyn Process>) -> Result<()> {
        match self.processes.entry(id) {
            Entry::Occupied(_) => Err(anyhow!(
                "process {} is already registered in environment {}",
                id,
                self.environment_id
            )),
            Entry::Vacant(entry) => {
                let count = self.reserve_process_slot()?;
                entry.insert(proc);
                self.record_process_count(count);
                Ok(())
            }
        }
    }

    fn remove_process(&self, id: u64) -> bool {
        let removed = match self.processes.remove(&id) {
            Some(removed) => removed,
            None => return false,
        };

        let count = self.release_process_slot();
        self.record_process_count(count);
        drop(removed);
        true
    }

    fn process_count(&self) -> usize {
        self.process_count.load(Ordering::Acquire)
    }

    fn send(&self, id: u64, signal: Signal) -> std::result::Result<(), SignalSendError> {
        if let Some(proc) = self.processes.get(&id) {
            proc.send(signal)
        } else {
            Err(SignalSendError::Closed(signal))
        }
    }

    fn send_to_all(&self, signal: Signal) {
        match signal {
            Signal::Kill => {
                for entry in self.processes.iter() {
                    let _ = entry.value().send(Signal::Kill);
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
                for entry in self.processes.iter() {
                    let _ = entry.value().send(Signal::HotReload {
                        module_id,
                        expected_version,
                        new_version,
                        acknowledgement: None,
                    });
                }
            }
            Signal::DieWhenLinkDies(flag) => {
                for entry in self.processes.iter() {
                    let _ = entry.value().send(Signal::DieWhenLinkDies(flag));
                }
            }
            _ => {
                // Other signals don't make sense to broadcast
                log::warn!("Attempted to broadcast non-broadcastable signal");
            }
        }
    }

    fn get_next_process_id(&self) -> u64 {
        self.next_process_id.fetch_add(1, Ordering::Relaxed)
    }

    fn id(&self) -> u64 {
        self.environment_id
    }

    async fn can_spawn_next_process(&self) -> Result<Option<()>> {
        if let Some(limit) = self.max_processes {
            if self.process_count.load(Ordering::Acquire) >= limit {
                return Err(ProcessLimitReached::new(self.environment_id, limit).into());
            }
        }

        Ok(Some(()))
    }

    fn get_module_registry(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.module_registry.clone()
    }
}

#[derive(Clone, Default)]
pub struct LunaticEnvironments {
    envs: Arc<DashMap<u64, Arc<LunaticEnvironment>>>,
}

impl LunaticEnvironments {
    fn create_atomic(
        &self,
        id: u64,
        registry: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Result<Arc<LunaticEnvironment>> {
        match self.envs.entry(id) {
            Entry::Occupied(entry) => {
                let environment = entry.get().clone();
                if let Some(requested_registry) = registry {
                    match environment.get_module_registry() {
                        Some(existing_registry)
                            if Arc::ptr_eq(&existing_registry, &requested_registry) => {}
                        Some(_) => {
                            return Err(anyhow!(
                                "environment {} already exists with a different module registry",
                                id
                            ));
                        }
                        None => {
                            return Err(anyhow!(
                                "environment {} already exists without a module registry",
                                id
                            ));
                        }
                    }
                }
                Ok(environment)
            }
            Entry::Vacant(entry) => {
                let environment = Arc::new(match registry {
                    Some(registry) => LunaticEnvironment::with_module_registry(id, registry),
                    None => LunaticEnvironment::new(id),
                });
                entry.insert(environment.clone());
                #[cfg(feature = "metrics")]
                metrics::gauge!("lunatic.process.environment.count", self.envs.len() as f64);
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
        self.envs.get(&id).map(|environment| environment.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        }
        assert!(Arc::ptr_eq(
            &first,
            &environments.get(7).await.expect("created environment")
        ));
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
        assert!(Arc::ptr_eq(&created, &same));
        assert!(Arc::ptr_eq(
            &created,
            &environments.create(8).await.unwrap()
        ));

        let different: Arc<dyn Any + Send + Sync> = Arc::new(Registry { _value: 2 });
        assert!(environments
            .create_with_registry(8, different)
            .await
            .is_err());

        let plain_first = LunaticEnvironments::default();
        plain_first.create(9).await.unwrap();
        let registry: Arc<dyn Any + Send + Sync> = Arc::new(Registry { _value: 3 });
        assert!(plain_first.create_with_registry(9, registry).await.is_err());
    }
}
