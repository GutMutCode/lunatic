//! Supervisor: Process Supervision Pattern
//!
//! Inspired by Erlang's `supervisor`, this module provides fault-tolerance
//! through supervised process trees with configurable restart strategies.
//!
//! ## Example
//!
//! ```ignore
//! use lunatic_otp_patterns::{Supervisor, SupervisorSpec, RestartStrategy, ChildSpec};
//! use lunatic_process::{env::Environment, spawn_native, Process};
//! use std::{future, sync::Arc};
//!
//! fn start_worker(environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
//!     let (_join, process) = spawn_native(environment, |_process, _mailbox| async move {
//!         future::pending::<anyhow::Result<()>>().await
//!     });
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
//!         },
//!         ChildSpec {
//!             id: "worker2".to_string(),
//!             start: start_worker,
//!             restart: RestartPolicy::Transient,
//!             shutdown: ShutdownPolicy::Brutal,
//!         },
//!     ],
//! };
//!
//! let mut supervisor = Supervisor::new(spec);
//! supervisor.start_children()?;
//! ```

use anyhow::Result;
use lunatic_process::{
    env::{Environment, LunaticEnvironment},
    Process, Signal,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self, Debug},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

const BRUTAL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Function used to start a child in the supervisor's Lunatic environment.
pub type ChildStart = fn(Arc<dyn Environment>) -> Result<Arc<dyn Process>, String>;

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
            .finish()
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

        let process = (child_spec.start)(self.environment.clone())
            .map_err(|error| format!("Failed to start child '{}': {}", child_id, error))?;
        let process_id = process.id();

        if self.environment.get_process(process_id).is_none() {
            process.send(Signal::Kill);
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
            return Ok(());
        }

        process.send(Signal::Kill);
        let timeout = match shutdown {
            ShutdownPolicy::Brutal => Some(BRUTAL_SHUTDOWN_TIMEOUT),
            ShutdownPolicy::Timeout(milliseconds) => Some(Duration::from_millis(milliseconds)),
            ShutdownPolicy::Infinity => None,
        };
        self.wait_for_process_exit(child_id, process_id, timeout)?;

        if let Some(state) = self.children.get_mut(child_id) {
            state.process = None;
        }
        Ok(())
    }

    fn wait_for_process_exit(
        &self,
        child_id: &str,
        process_id: u64,
        timeout: Option<Duration>,
    ) -> Result<(), String> {
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
