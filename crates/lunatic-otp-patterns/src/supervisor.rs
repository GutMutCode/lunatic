//! Supervisor: Process Supervision Pattern
//!
//! Inspired by Erlang's `supervisor`, this module provides fault-tolerance
//! through supervised process trees with configurable restart strategies.
//!
//! ## Example
//!
//! ```ignore
//! use lunatic_otp_patterns::{
//!     ChildSpec, ChildType, RestartPolicy, RestartStrategy, ShutdownPolicy,
//!     Supervisor, SupervisorSpec,
//! };
//! use lunatic_process::{env::Environment, spawn_native, Process};
//! use std::{future, sync::Arc};
//!
//! fn start_worker(environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
//!     let (_join, process) = spawn_native(environment, |_process, _mailbox| async move {
//!         future::pending::<anyhow::Result<()>>().await
//!     })
//!     .map_err(|error| error.to_string())?;
//!     Ok(Arc::new(process))
//! }
//!
//! let spec = SupervisorSpec {
//!     strategy: RestartStrategy::OneForOne,
//!     max_restarts: 3,
//!     max_seconds: 5,
//!     children: vec![
//!         ChildSpec {
//!             id: "worker1".to_string(),
//!             start: start_worker,
//!             restart: RestartPolicy::Permanent,
//!             shutdown: ShutdownPolicy::Timeout(5000),
//!             child_type: ChildType::Worker,
//!         },
//!         ChildSpec {
//!             id: "worker2".to_string(),
//!             start: start_worker,
//!             restart: RestartPolicy::Transient,
//!             shutdown: ShutdownPolicy::Brutal,
//!             child_type: ChildType::Worker,
//!         },
//!     ],
//! };
//!
//! let supervisor = Supervisor::spawn(spec)?;
//! // Runtime ProcessDied events now drive restart strategies automatically.
//! supervisor.shutdown()?;
//! ```

use anyhow::{anyhow, Result};
use lunatic_process::{
    env::{Environment, LunaticEnvironment},
    message::Message,
    spawn_native, DeathReason, NativeProcess, Process, Signal,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Debug},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{mpsc, Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant},
};

const BRUTAL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MONITOR_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(5);

/// Function used to start a child in the supervisor's Lunatic environment.
pub type ChildStart = fn(Arc<dyn Environment>) -> Result<Arc<dyn Process>, String>;
type CleanupTracker = Arc<Mutex<Vec<Arc<dyn Process>>>>;

/// Supervisor specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorSpec {
    /// Restart strategy
    pub strategy: RestartStrategy,

    /// Maximum number of restarts allowed
    pub max_restarts: u32,

    /// Time window (seconds) for max_restarts
    pub max_seconds: u32,

    /// Child process specifications
    pub children: Vec<ChildSpec>,
}

/// Restart strategy determines what happens when a child process dies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestartStrategy {
    /// Restart only the failed child
    ///
    /// When a child process terminates, only that child is restarted.
    /// Other children continue running unaffected.
    OneForOne,

    /// Restart all children when one fails
    ///
    /// When any child process terminates, all children are terminated
    /// and then all are restarted in start order.
    OneForAll,

    /// Restart failed child and all started after it
    ///
    /// When a child process terminates, that child and all children
    /// started after it are terminated and restarted.
    RestForOne,

    /// Simple one-for-one (dynamic children)
    ///
    /// All children are instances of the same child specification.
    /// Children are added dynamically.
    SimpleOneForOne,
}

/// Child process specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildSpec {
    /// Unique child identifier
    pub id: String,

    /// Module and function to start the child
    #[serde(skip)]
    #[serde(default = "default_start_fn")] // Function pointers can't be serialized
    pub start: ChildStart,

    /// Restart policy
    pub restart: RestartPolicy,

    /// Shutdown policy
    pub shutdown: ShutdownPolicy,

    /// Child type
    #[serde(default)]
    pub child_type: ChildType,
}

// Default start function for serialization
fn default_start_fn() -> ChildStart {
    |_| Err("No start function provided".to_string())
}

/// Restart policy for child processes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestartPolicy {
    /// Always restart the child
    ///
    /// Child is restarted regardless of termination reason.
    Permanent,

    /// Restart only on abnormal termination
    ///
    /// Child is restarted if it terminates abnormally (crash).
    /// Normal termination does not trigger restart.
    Transient,

    /// Never restart the child
    ///
    /// Child is not restarted regardless of termination reason.
    Temporary,
}

/// Shutdown policy for child processes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShutdownPolicy {
    /// Brutal kill (immediate termination)
    ///
    /// Child process is killed immediately without graceful shutdown.
    Brutal,

    /// Timeout in milliseconds
    ///
    /// Maximum time to wait for the runtime to acknowledge process termination.
    Timeout(u64),

    /// Infinite timeout (wait forever)
    ///
    /// Supervisor waits indefinitely for the runtime to unregister the child.
    /// Use with caution - can block supervisor shutdown.
    Infinity,
}

/// Child process type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChildType {
    /// Worker process (default)
    #[default]
    Worker,

    /// Supervisor process
    Supervisor,
}

/// Supervisor state
pub struct Supervisor {
    spec: SupervisorSpec,
    environment: Arc<dyn Environment>,
    children: HashMap<String, ChildState>,
    restart_history: Vec<RestartEvent>,
    monitor: Option<Arc<dyn Process>>,
    cleanup_tracker: Option<CleanupTracker>,
}

/// Handle to a process-backed supervisor.
///
/// The supervisor process owns all mutable child state and consumes runtime
/// monitor notifications serially. Cloned handles observe snapshots and can
/// request an orderly shutdown without racing the restart loop.
#[derive(Clone)]
pub struct SupervisorHandle {
    process: NativeProcess,
    environment: Arc<dyn Environment>,
    shutdown: tokio::sync::watch::Sender<bool>,
    shared: Arc<SupervisorShared>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SupervisorExitStatus {
    Normal,
    Failed(String),
}

#[derive(Clone)]
struct SupervisorSnapshot {
    children: Vec<ChildInfo>,
    counts: ChildrenCount,
    restart_history: Vec<(u64, String)>,
}

struct SupervisorShared {
    snapshot: Mutex<SupervisorSnapshot>,
    exit: (Mutex<Option<SupervisorExitStatus>>, Condvar),
}

/// Best-effort orphan prevention if the supervisor process itself is killed.
/// Orderly shutdown still applies each child's configured shutdown policy.
struct SupervisorProcessGuard {
    children: CleanupTracker,
    armed: bool,
}

/// Child process state
#[derive(Clone)]
struct ChildState {
    spec: ChildSpec,
    process: Option<Arc<dyn Process>>,
    restart_count: u32,
}

/// Restart event for tracking restart intensity
#[derive(Debug, Clone)]
struct RestartEvent {
    timestamp: u64,
    child_id: String,
}

impl Debug for ChildState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildState")
            .field("spec", &self.spec)
            .field(
                "process_id",
                &self.process.as_ref().map(|process| process.id()),
            )
            .field("restart_count", &self.restart_count)
            .finish()
    }
}

impl Debug for Supervisor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Supervisor")
            .field("spec", &self.spec)
            .field("environment_id", &self.environment.id())
            .field("children", &self.children)
            .field("restart_history", &self.restart_history)
            .field(
                "monitor_process_id",
                &self.monitor.as_ref().map(|process| process.id()),
            )
            .finish()
    }
}

impl Debug for SupervisorHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SupervisorHandle")
            .field("process_id", &self.process.id())
            .field("environment_id", &self.environment.id())
            .field("alive", &self.is_alive())
            .finish()
    }
}

impl SupervisorShared {
    fn new(supervisor: &Supervisor) -> Self {
        Self {
            snapshot: Mutex::new(SupervisorSnapshot::from_supervisor(supervisor)),
            exit: (Mutex::new(None), Condvar::new()),
        }
    }

    fn publish(&self, supervisor: &Supervisor) {
        *self
            .snapshot
            .lock()
            .expect("Supervisor snapshot mutex poisoned") =
            SupervisorSnapshot::from_supervisor(supervisor);
    }

    fn finish(&self, status: SupervisorExitStatus) {
        let (exit, condvar) = &self.exit;
        let mut exit = exit.lock().expect("Supervisor exit mutex poisoned");
        if exit.is_none() {
            *exit = Some(status);
            condvar.notify_all();
        }
    }

    fn wait_for_exit(&self, timeout: Option<Duration>) -> Result<SupervisorExitStatus, String> {
        let (exit, condvar) = &self.exit;
        let exit = exit.lock().expect("Supervisor exit mutex poisoned");
        let exit = match timeout {
            Some(timeout) => {
                let (exit, wait) = condvar
                    .wait_timeout_while(exit, timeout, |status| status.is_none())
                    .expect("Supervisor exit mutex poisoned");
                if wait.timed_out() && exit.is_none() {
                    return Err("Timed out waiting for Supervisor to exit".to_string());
                }
                exit
            }
            None => condvar
                .wait_while(exit, |status| status.is_none())
                .expect("Supervisor exit mutex poisoned"),
        };

        exit.clone()
            .ok_or_else(|| "Supervisor exit status unavailable".to_string())
    }
}

impl SupervisorSnapshot {
    fn from_supervisor(supervisor: &Supervisor) -> Self {
        Self {
            children: supervisor.which_children(),
            counts: supervisor.count_children(),
            restart_history: supervisor.restart_history(),
        }
    }
}

impl SupervisorProcessGuard {
    fn new(children: CleanupTracker) -> Self {
        Self {
            children,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
        self.children
            .lock()
            .expect("Supervisor cleanup tracker mutex poisoned")
            .clear();
    }
}

impl Drop for SupervisorProcessGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let children = self
            .children
            .lock()
            .expect("Supervisor cleanup tracker mutex poisoned");
        for child in children.iter().rev() {
            let _ = child.send(Signal::Kill);
        }
    }
}

impl Supervisor {
    /// Create a new supervisor with the given specification
    pub fn new(spec: SupervisorSpec) -> Self {
        Self::with_environment(spec, Arc::new(LunaticEnvironment::new(0)))
    }

    /// Create a supervisor that starts and manages children in `environment`.
    pub fn with_environment(spec: SupervisorSpec, environment: Arc<dyn Environment>) -> Self {
        Self {
            spec,
            environment,
            children: HashMap::new(),
            restart_history: Vec::new(),
            monitor: None,
            cleanup_tracker: None,
        }
    }

    /// Spawn a native Lunatic process that owns and runs a supervisor.
    pub fn spawn(spec: SupervisorSpec) -> Result<SupervisorHandle, String> {
        Self::spawn_with_environment(spec, Arc::new(LunaticEnvironment::new(0)))
    }

    /// Spawn a process-backed supervisor in `environment`.
    ///
    /// This is the automatic supervision entry point. It waits until every
    /// initial child has been started and its monitor relation acknowledged.
    pub fn spawn_with_environment(
        spec: SupervisorSpec,
        environment: Arc<dyn Environment>,
    ) -> Result<SupervisorHandle, String> {
        ensure_multi_thread_runtime()?;

        let supervisor = Self::with_environment(spec, environment.clone());
        let shared = Arc::new(SupervisorShared::new(&supervisor));
        let process_shared = shared.clone();
        let (shutdown, mut shutdown_request) = tokio::sync::watch::channel(false);
        let (started_sender, started_receiver) = mpsc::sync_channel(1);

        let (join, process) = spawn_native(environment.clone(), move |process, mailbox| async move {
            let mut supervisor = supervisor;
            let cleanup_tracker = Arc::new(Mutex::new(Vec::new()));
            let mut process_guard = SupervisorProcessGuard::new(cleanup_tracker.clone());
            supervisor.monitor = Some(Arc::new(process));
            supervisor.cleanup_tracker = Some(cleanup_tracker);

            let start_result = supervisor.start_children();
            process_shared.publish(&supervisor);
            let _ = started_sender.send(start_result.clone());
            start_result.map_err(anyhow::Error::msg)?;

            loop {
                tokio::select! {
                    biased;
                    changed = shutdown_request.changed() => {
                        if changed.is_err() || *shutdown_request.borrow() {
                            let result = supervisor.shutdown();
                            process_shared.publish(&supervisor);
                            result.map_err(anyhow::Error::msg)?;
                            process_guard.disarm();
                            return Ok(());
                        }
                    }
                    message = mailbox.pop(None) => {
                        let Message::ProcessDied { process_id, reason } = message else {
                            continue;
                        };
                        if let Err(restart_error) = supervisor.handle_process_exit(process_id, reason) {
                            let shutdown_error = supervisor.shutdown().err();
                            process_shared.publish(&supervisor);
                            let error = match shutdown_error {
                                Some(shutdown_error) => format!(
                                    "{restart_error}; failed to shut down remaining children during escalation: {shutdown_error}"
                                ),
                                None => restart_error,
                            };
                            return Err(anyhow!(error));
                        }
                        process_shared.publish(&supervisor);
                    }
                }
            }
        })
        .map_err(|error| format!("Failed to spawn Supervisor process: {error}"))?;

        let process_id = process.id();
        let join_shared = shared.clone();
        tokio::spawn(async move {
            let status = match join.await {
                Ok(Ok(())) => SupervisorExitStatus::Normal,
                Ok(Err(error)) => SupervisorExitStatus::Failed(error.to_string()),
                Err(error) => {
                    SupervisorExitStatus::Failed(format!("Supervisor process task failed: {error}"))
                }
            };
            join_shared.finish(status);
        });

        let started = run_blocking(|| started_receiver.recv());
        match started {
            Ok(Ok(())) => Ok(SupervisorHandle {
                process,
                environment,
                shutdown,
                shared,
            }),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(format!(
                "Supervisor process {process_id} exited before startup completed"
            )),
        }
    }

    /// Start all child processes
    pub fn start_children(&mut self) -> Result<(), String> {
        let child_ids: Vec<String> = self.spec.children.iter().map(|c| c.id.clone()).collect();
        let mut started: Vec<String> = Vec::with_capacity(child_ids.len());

        for child_id in &child_ids {
            if let Err(error) = self.start_child(child_id) {
                for started_child_id in started.iter().rev() {
                    let _ = self.stop_child(started_child_id);
                }
                return Err(error);
            }
            started.push(child_id.clone());
        }
        Ok(())
    }

    fn refresh_cleanup_tracker(&self) {
        let Some(tracker) = &self.cleanup_tracker else {
            return;
        };
        *tracker
            .lock()
            .expect("Supervisor cleanup tracker mutex poisoned") = self
            .spec
            .children
            .iter()
            .filter_map(|child| {
                self.children
                    .get(&child.id)
                    .and_then(|state| state.process.clone())
            })
            .collect();
    }

    /// Start a specific child process
    pub fn start_child(&mut self, child_id: &str) -> Result<u64, String> {
        self.start_child_internal(child_id, false)
    }

    fn start_child_internal(&mut self, child_id: &str, is_restart: bool) -> Result<u64, String> {
        ensure_multi_thread_runtime()?;

        let child_spec = self
            .spec
            .children
            .iter()
            .find(|c| c.id == child_id)
            .ok_or_else(|| format!("Child '{}' not found", child_id))?
            .clone();

        if let Some(state) = self.children.get_mut(child_id) {
            if let Some(process) = state.process.as_ref() {
                if self.environment.get_process(process.id()).is_some() {
                    return Err(format!(
                        "Child '{}' is already running as process {}",
                        child_id,
                        process.id()
                    ));
                }
            }
            state.process = None;
        }
        self.refresh_cleanup_tracker();

        let process = catch_unwind(AssertUnwindSafe(|| {
            (child_spec.start)(self.environment.clone())
        }))
        .map_err(|payload| {
            format!(
                "Failed to start child '{}': start function panicked: {}",
                child_id,
                panic_payload_message(payload)
            )
        })?
        .map_err(|error| format!("Failed to start child '{}': {}", child_id, error))?;
        let process_id = process.id();

        if let Some(monitor) = self.monitor.clone() {
            let (acknowledgement, acknowledged) = mpsc::sync_channel(1);
            if let Err(error) = process.send(Signal::Monitor {
                process: monitor,
                acknowledgement: Some(acknowledgement),
            }) {
                let _ = process.send(Signal::Kill);
                let _ =
                    self.wait_for_process_exit(child_id, process_id, Some(BRUTAL_SHUTDOWN_TIMEOUT));
                return Err(format!(
                    "Failed to monitor child '{}' process {}: {error}",
                    child_id, process_id
                ));
            }
            let monitor_result =
                run_blocking(|| acknowledged.recv_timeout(MONITOR_REGISTRATION_TIMEOUT));
            if let Err(error) = monitor_result {
                let _ = process.send(Signal::Kill);
                let _ =
                    self.wait_for_process_exit(child_id, process_id, Some(BRUTAL_SHUTDOWN_TIMEOUT));
                return Err(format!(
                    "Timed out waiting for child '{}' process {} monitor registration: {error}",
                    child_id, process_id
                ));
            }
        }

        if self.monitor.is_none()
            && self.environment.get_process(process_id).is_none()
            && process.terminal_reason().is_none()
        {
            let _ = process.send(Signal::Kill);
            return Err(format!(
                "Child '{}' start function returned unregistered process {}",
                child_id, process_id
            ));
        }

        match self.children.get_mut(child_id) {
            Some(state) => {
                state.spec = child_spec;
                state.process = Some(process);
                if is_restart {
                    state.restart_count = state.restart_count.saturating_add(1);
                }
            }
            None => {
                self.children.insert(
                    child_id.to_string(),
                    ChildState {
                        spec: child_spec,
                        process: Some(process),
                        restart_count: u32::from(is_restart),
                    },
                );
            }
        }
        self.refresh_cleanup_tracker();

        Ok(process_id)
    }

    /// Handle child termination
    pub fn handle_child_exit(&mut self, child_id: &str, reason: ExitReason) -> Result<(), String> {
        let child_state = self
            .children
            .get(child_id)
            .ok_or_else(|| format!("Child '{}' not found", child_id))?;

        // Check if restart is needed based on policy and reason
        let should_restart = match child_state.spec.restart {
            RestartPolicy::Permanent => true,
            RestartPolicy::Transient => !matches!(reason, ExitReason::Normal),
            RestartPolicy::Temporary => false,
        };

        if !should_restart {
            return self.stop_child(child_id);
        }

        let child_ids = match self.spec.strategy {
            RestartStrategy::OneForOne | RestartStrategy::SimpleOneForOne => {
                vec![child_id.to_string()]
            }
            RestartStrategy::OneForAll => self
                .spec
                .children
                .iter()
                .filter(|child| {
                    child.id == child_id
                        || self
                            .children
                            .get(&child.id)
                            .and_then(|state| state.process.as_ref())
                            .is_some()
                })
                .map(|child| child.id.clone())
                .collect(),
            RestartStrategy::RestForOne => {
                let failed_index = self
                    .spec
                    .children
                    .iter()
                    .position(|child| child.id == child_id)
                    .ok_or_else(|| format!("Child '{}' not found", child_id))?;
                self.spec.children[failed_index..]
                    .iter()
                    .filter(|child| {
                        child.id == child_id
                            || self
                                .children
                                .get(&child.id)
                                .and_then(|state| state.process.as_ref())
                                .is_some()
                    })
                    .map(|child| child.id.clone())
                    .collect()
            }
        };

        self.restart_children(&child_ids)
    }

    fn handle_process_exit(&mut self, process_id: u64, reason: DeathReason) -> Result<(), String> {
        let child_id = self.children.iter().find_map(|(child_id, state)| {
            state
                .process
                .as_ref()
                .filter(|process| process.id() == process_id)
                .map(|_| child_id.clone())
        });
        let Some(child_id) = child_id else {
            // Strategy restarts and shutdown intentionally terminate children.
            // Their notifications can arrive after a replacement is installed;
            // process identity, rather than child ID alone, makes them stale.
            return Ok(());
        };

        let reason = match reason {
            DeathReason::Normal => ExitReason::Normal,
            DeathReason::Failure | DeathReason::NoProcess => ExitReason::Crash,
        };
        self.handle_child_exit(&child_id, reason)
    }

    fn restart_children(&mut self, child_ids: &[String]) -> Result<(), String> {
        self.reserve_restart_events(child_ids)?;

        for child_id in child_ids.iter().rev() {
            self.stop_child(child_id)?;
        }

        let mut restarted: Vec<String> = Vec::with_capacity(child_ids.len());
        for child_id in child_ids {
            if let Err(error) = self.start_child_internal(child_id, true) {
                for restarted_child_id in restarted.iter().rev() {
                    let _ = self.stop_child(restarted_child_id);
                }
                return Err(error);
            }
            restarted.push(child_id.clone());
        }

        Ok(())
    }

    /// Stop a child process
    fn stop_child(&mut self, child_id: &str) -> Result<(), String> {
        ensure_multi_thread_runtime()?;

        let (process, shutdown) = {
            let state = self
                .children
                .get(child_id)
                .ok_or_else(|| format!("Child '{}' not started", child_id))?;
            (state.process.clone(), state.spec.shutdown)
        };
        let Some(process) = process else {
            return Ok(());
        };
        let process_id = process.id();

        if self.environment.get_process(process_id).is_none() {
            if let Some(state) = self.children.get_mut(child_id) {
                state.process = None;
            }
            self.refresh_cleanup_tracker();
            return Ok(());
        }

        process
            .send(Signal::Kill)
            .map_err(|error| format!("Failed to stop child '{}': {error}", child_id))?;
        let timeout = match shutdown {
            ShutdownPolicy::Brutal => Some(BRUTAL_SHUTDOWN_TIMEOUT),
            ShutdownPolicy::Timeout(milliseconds) => Some(Duration::from_millis(milliseconds)),
            ShutdownPolicy::Infinity => None,
        };
        self.wait_for_process_exit(child_id, process_id, timeout)?;

        if let Some(state) = self.children.get_mut(child_id) {
            state.process = None;
        }
        self.refresh_cleanup_tracker();
        Ok(())
    }

    fn wait_for_process_exit(
        &self,
        child_id: &str,
        process_id: u64,
        timeout: Option<Duration>,
    ) -> Result<(), String> {
        run_blocking(|| {
            let started = Instant::now();
            while self.environment.get_process(process_id).is_some() {
                if timeout.is_some_and(|limit| started.elapsed() >= limit) {
                    return Err(format!(
                        "Timed out stopping child '{}' process {}",
                        child_id, process_id
                    ));
                }
                thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        })
    }

    /// Stop every active child in reverse start order.
    pub fn shutdown(&mut self) -> Result<(), String> {
        let child_ids: Vec<String> = self
            .spec
            .children
            .iter()
            .map(|child| child.id.clone())
            .collect();
        for child_id in child_ids.iter().rev() {
            if self.children.contains_key(child_id) {
                self.stop_child(child_id)?;
            }
        }
        Ok(())
    }

    /// Reserve restart intensity for a complete strategy operation.
    fn reserve_restart_events(&mut self, child_ids: &[String]) -> Result<(), String> {
        let now = current_timestamp_secs();
        let window_start = now.saturating_sub(self.spec.max_seconds as u64);

        self.restart_history
            .retain(|event| event.timestamp >= window_start);

        let attempted_total = self.restart_history.len() + child_ids.len();
        if attempted_total > self.spec.max_restarts as usize {
            return Err(format!(
                "Restart intensity limit exceeded: {} existing + {} requested restarts in {} seconds (max {})",
                self.restart_history.len(),
                child_ids.len(),
                self.spec.max_seconds,
                self.spec.max_restarts
            ));
        }

        self.restart_history
            .extend(child_ids.iter().map(|child_id| RestartEvent {
                timestamp: now,
                child_id: child_id.clone(),
            }));
        Ok(())
    }

    /// Get child process state
    pub fn which_children(&self) -> Vec<ChildInfo> {
        self.spec
            .children
            .iter()
            .filter_map(|spec| self.children.get(&spec.id).map(|state| (spec, state)))
            .map(|(spec, state)| ChildInfo {
                id: spec.id.clone(),
                process_id: state.process.as_ref().map(|process| process.id()),
                child_type: state.spec.child_type,
                restart_count: state.restart_count,
            })
            .collect()
    }

    /// Count children
    pub fn count_children(&self) -> ChildrenCount {
        let mut specs = 0;
        let mut active = 0;
        let mut supervisors = 0;
        let mut workers = 0;

        // Count specs from the specification
        for child_spec in &self.spec.children {
            specs += 1;

            match child_spec.child_type {
                ChildType::Supervisor => supervisors += 1,
                ChildType::Worker => workers += 1,
            }
        }

        // Count active processes from the children map
        for state in self.children.values() {
            if state
                .process
                .as_ref()
                .is_some_and(|process| self.environment.get_process(process.id()).is_some())
            {
                active += 1;
            }
        }

        ChildrenCount {
            specs,
            active,
            supervisors,
            workers,
        }
    }

    /// Return restart events in chronological order for diagnostics.
    pub fn restart_history(&self) -> Vec<(u64, String)> {
        self.restart_history
            .iter()
            .map(|event| (event.timestamp, event.child_id.clone()))
            .collect()
    }
}

impl SupervisorHandle {
    /// Return the native process ID of the supervisor.
    pub fn id(&self) -> u64 {
        self.process.id()
    }

    /// Borrow the native process handle for linking or monitoring the supervisor.
    pub fn process(&self) -> &NativeProcess {
        &self.process
    }

    /// Return whether the supervisor process is still registered and running.
    pub fn is_alive(&self) -> bool {
        if self
            .shared
            .exit
            .0
            .lock()
            .expect("Supervisor exit mutex poisoned")
            .is_some()
        {
            return false;
        }
        self.environment.get_process(self.process.id()).is_some()
    }

    /// Return the most recently published child snapshot.
    pub fn which_children(&self) -> Vec<ChildInfo> {
        self.shared
            .snapshot
            .lock()
            .expect("Supervisor snapshot mutex poisoned")
            .children
            .clone()
    }

    /// Return child counts from the most recently published snapshot.
    pub fn count_children(&self) -> ChildrenCount {
        self.shared
            .snapshot
            .lock()
            .expect("Supervisor snapshot mutex poisoned")
            .counts
            .clone()
    }

    /// Return restart events in chronological order.
    pub fn restart_history(&self) -> Vec<(u64, String)> {
        self.shared
            .snapshot
            .lock()
            .expect("Supervisor snapshot mutex poisoned")
            .restart_history
            .clone()
    }

    /// Request orderly reverse-order child shutdown and wait for completion.
    pub fn shutdown(&self) -> Result<(), String> {
        let _ = self.shutdown.send(true);
        self.wait_for_exit(None)
    }

    /// Wait until the supervisor exits.
    ///
    /// Restart-intensity exhaustion and restart failures are returned as
    /// errors. An orderly shutdown returns `Ok(())`.
    pub fn wait_for_exit(&self, timeout: Option<Duration>) -> Result<(), String> {
        let status = run_blocking(|| self.shared.wait_for_exit(timeout))?;
        match status {
            SupervisorExitStatus::Normal => Ok(()),
            SupervisorExitStatus::Failed(error) => Err(error),
        }
    }
}

/// Exit reason for child process
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExitReason {
    /// Normal termination
    Normal,
    /// Process crashed
    Crash,
    /// Killed by supervisor
    Killed,
    /// Shutdown signal
    Shutdown,
}

/// Child process information
#[derive(Debug, Clone)]
pub struct ChildInfo {
    pub id: String,
    pub process_id: Option<u64>,
    pub child_type: ChildType,
    pub restart_count: u32,
}

/// Children count statistics
#[derive(Debug, Clone)]
pub struct ChildrenCount {
    pub specs: usize,
    pub active: usize,
    pub supervisors: usize,
    pub workers: usize,
}

fn ensure_multi_thread_runtime() -> Result<(), String> {
    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|_| "Supervisor requires a multi-thread Tokio runtime".to_string())?;
    if matches!(
        runtime.runtime_flavor(),
        tokio::runtime::RuntimeFlavor::CurrentThread
    ) {
        return Err("Supervisor requires a multi-thread Tokio runtime".to_string());
    }
    Ok(())
}

fn run_blocking<T>(operation: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(runtime)
            if matches!(
                runtime.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) =>
        {
            tokio::task::block_in_place(operation)
        }
        _ => operation(),
    }
}

fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

fn current_timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_supervisor_creation() {
        let spec = SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 3,
            max_seconds: 5,
            children: vec![],
        };

        let supervisor = Supervisor::new(spec);
        assert_eq!(supervisor.children.len(), 0);
    }

    #[test]
    fn test_restart_strategy_values() {
        assert_eq!(RestartStrategy::OneForOne, RestartStrategy::OneForOne);
        assert_ne!(RestartStrategy::OneForOne, RestartStrategy::OneForAll);
    }

    #[test]
    fn test_restart_policy_should_restart() {
        // Permanent always restarts
        assert!(matches!(RestartPolicy::Permanent, RestartPolicy::Permanent));

        // Transient depends on reason (tested in supervisor logic)
        assert!(matches!(RestartPolicy::Transient, RestartPolicy::Transient));

        // Temporary never restarts
        assert!(matches!(RestartPolicy::Temporary, RestartPolicy::Temporary));
    }

    #[test]
    fn test_shutdown_policy() {
        assert!(matches!(ShutdownPolicy::Brutal, ShutdownPolicy::Brutal));
        assert!(matches!(
            ShutdownPolicy::Timeout(5000),
            ShutdownPolicy::Timeout(_)
        ));
        assert!(matches!(ShutdownPolicy::Infinity, ShutdownPolicy::Infinity));
    }

    #[test]
    fn test_count_children_empty() {
        let spec = SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 3,
            max_seconds: 5,
            children: vec![],
        };

        let supervisor = Supervisor::new(spec);
        let count = supervisor.count_children();

        assert_eq!(count.specs, 0);
        assert_eq!(count.active, 0);
        assert_eq!(count.workers, 0);
        assert_eq!(count.supervisors, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_rejects_current_thread_runtime() {
        let spec = SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 1,
            max_seconds: 5,
            children: vec![ChildSpec {
                id: "worker".to_string(),
                start: |_| Err("should not be called".to_string()),
                restart: RestartPolicy::Permanent,
                shutdown: ShutdownPolicy::Brutal,
                child_type: ChildType::Worker,
            }],
        };
        let mut supervisor = Supervisor::new(spec);

        let error = supervisor.start_children().unwrap_err();
        assert!(error.contains("requires a multi-thread Tokio runtime"));
    }
}
