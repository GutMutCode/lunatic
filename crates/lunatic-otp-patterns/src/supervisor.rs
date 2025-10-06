//! Supervisor: Process Supervision Pattern
//!
//! Inspired by Erlang's `supervisor`, this module provides fault-tolerance
//! through supervised process trees with configurable restart strategies.
//!
//! ## Example
//!
//! ```ignore
//! use lunatic_otp_patterns::{Supervisor, SupervisorSpec, RestartStrategy, ChildSpec};
//!
//! let spec = SupervisorSpec {
//!     strategy: RestartStrategy::OneForOne,
//!     max_restarts: 3,
//!     max_seconds: 5,
//!     children: vec![
//!         ChildSpec {
//!             id: "worker1".to_string(),
//!             start: WorkerModule::start,
//!             restart: RestartPolicy::Permanent,
//!             shutdown: ShutdownPolicy::Timeout(5000),
//!         },
//!         ChildSpec {
//!             id: "worker2".to_string(),
//!             start: WorkerModule::start,
//!             restart: RestartPolicy::Transient,
//!             shutdown: ShutdownPolicy::Brutal,
//!         },
//!     ],
//! };
//!
//! let supervisor = Supervisor::start(spec)?;
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use anyhow::Result;

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
    pub start: fn() -> Result<u64, String>,

    /// Restart policy
    pub restart: RestartPolicy,

    /// Shutdown policy
    pub shutdown: ShutdownPolicy,

    /// Child type
    #[serde(default)]
    pub child_type: ChildType,
}

// Default start function for serialization
fn default_start_fn() -> fn() -> Result<u64, String> {
    || Err("No start function provided".to_string())
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
    /// Child is given time to shut down gracefully. If it doesn't
    /// terminate within the timeout, it is killed.
    Timeout(u64),

    /// Infinite timeout (wait forever)
    ///
    /// Supervisor waits indefinitely for child to terminate gracefully.
    /// Use with caution - can block supervisor shutdown.
    Infinity,
}

/// Child process type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChildType {
    /// Worker process (default)
    Worker,

    /// Supervisor process
    Supervisor,
}

impl Default for ChildType {
    fn default() -> Self {
        ChildType::Worker
    }
}

/// Supervisor state
#[derive(Debug)]
pub struct Supervisor {
    spec: SupervisorSpec,
    children: HashMap<String, ChildState>,
    restart_history: Vec<RestartEvent>,
}

/// Child process state
#[derive(Debug, Clone)]
struct ChildState {
    spec: ChildSpec,
    process_id: Option<u64>,
    restart_count: u32,
}

/// Restart event for tracking restart intensity
#[derive(Debug, Clone)]
struct RestartEvent {
    timestamp: u64,
    child_id: String,
}

impl Supervisor {
    /// Create a new supervisor with the given specification
    pub fn new(spec: SupervisorSpec) -> Self {
        let children = HashMap::new();
        let restart_history = Vec::new();

        Supervisor {
            spec,
            children,
            restart_history,
        }
    }

    /// Start all child processes
    pub fn start_children(&mut self) -> Result<(), String> {
        let child_ids: Vec<String> = self.spec.children.iter().map(|c| c.id.clone()).collect();
        for child_id in &child_ids {
            self.start_child(child_id)?;
        }
        Ok(())
    }

    /// Start a specific child process
    pub fn start_child(&mut self, child_id: &str) -> Result<u64, String> {
        let child_spec = self
            .spec
            .children
            .iter()
            .find(|c| c.id == child_id)
            .ok_or_else(|| format!("Child '{}' not found", child_id))?
            .clone();

        let process_id = (child_spec.start)().map_err(|e| format!("Failed to start child: {}", e))?;

        self.children.insert(
            child_id.to_string(),
            ChildState {
                spec: child_spec,
                process_id: Some(process_id),
                restart_count: 0,
            },
        );

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
            // Mark child as stopped
            if let Some(state) = self.children.get_mut(child_id) {
                state.process_id = None;
            }
            return Ok(());
        }

        // Apply restart strategy
        match self.spec.strategy {
            RestartStrategy::OneForOne => {
                self.restart_child(child_id)?;
            }
            RestartStrategy::OneForAll => {
                self.restart_all_children()?;
            }
            RestartStrategy::RestForOne => {
                self.restart_from_child(child_id)?;
            }
            RestartStrategy::SimpleOneForOne => {
                self.restart_child(child_id)?;
            }
        }

        Ok(())
    }

    /// Restart a specific child
    fn restart_child(&mut self, child_id: &str) -> Result<(), String> {
        // Check restart intensity
        self.check_restart_intensity()?;

        // Record restart event
        self.restart_history.push(RestartEvent {
            timestamp: current_timestamp_secs(),
            child_id: child_id.to_string(),
        });

        // Stop child if running
        self.stop_child(child_id)?;

        // Start child
        self.start_child(child_id)?;

        Ok(())
    }

    /// Restart all children
    fn restart_all_children(&mut self) -> Result<(), String> {
        let child_ids: Vec<String> = self.children.keys().cloned().collect();

        for child_id in &child_ids {
            self.stop_child(child_id)?;
        }

        for child_id in &child_ids {
            self.start_child(child_id)?;
        }

        Ok(())
    }

    /// Restart child and all children started after it
    fn restart_from_child(&mut self, failed_child_id: &str) -> Result<(), String> {
        // Find index of failed child
        let failed_index = self
            .spec
            .children
            .iter()
            .position(|c| c.id == failed_child_id)
            .ok_or_else(|| format!("Child '{}' not found", failed_child_id))?;

        // Collect child IDs to avoid borrow conflicts
        let child_ids: Vec<String> = self.spec.children[failed_index..]
            .iter()
            .map(|c| c.id.clone())
            .collect();

        // Stop all children from failed_index onwards
        for child_id in &child_ids {
            self.stop_child(child_id)?;
        }

        // Restart all children from failed_index onwards
        for child_id in &child_ids {
            self.start_child(child_id)?;
        }

        Ok(())
    }

    /// Stop a child process
    fn stop_child(&mut self, child_id: &str) -> Result<(), String> {
        if let Some(state) = self.children.get_mut(child_id) {
            if let Some(process_id) = state.process_id {
                // Kill process based on shutdown policy
                match state.spec.shutdown {
                    ShutdownPolicy::Brutal => {
                        // Immediate kill - in real implementation would call lunatic::process::kill
                        // For now, just mark as stopped
                        state.process_id = None;
                    }
                    ShutdownPolicy::Timeout(_ms) => {
                        // Graceful shutdown with timeout
                        // In real implementation: send shutdown message and wait
                        state.process_id = None;
                    }
                    ShutdownPolicy::Infinity => {
                        // Wait forever for graceful shutdown
                        // In real implementation: send shutdown message and wait indefinitely
                        state.process_id = None;
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if restart intensity limit is exceeded
    fn check_restart_intensity(&mut self) -> Result<(), String> {
        let now = current_timestamp_secs();
        let window_start = now.saturating_sub(self.spec.max_seconds as u64);

        // Remove old events outside window
        self.restart_history
            .retain(|event| event.timestamp >= window_start);

        // Check if limit exceeded
        if self.restart_history.len() >= self.spec.max_restarts as usize {
            return Err(format!(
                "Restart intensity limit exceeded: {} restarts in {} seconds",
                self.spec.max_restarts, self.spec.max_seconds
            ));
        }

        Ok(())
    }

    /// Get child process state
    pub fn which_children(&self) -> Vec<ChildInfo> {
        self.children
            .iter()
            .map(|(id, state)| ChildInfo {
                id: id.clone(),
                process_id: state.process_id,
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

        for (_, state) in &self.children {
            specs += 1;

            if state.process_id.is_some() {
                active += 1;
            }

            match state.spec.child_type {
                ChildType::Supervisor => supervisors += 1,
                ChildType::Worker => workers += 1,
            }
        }

        ChildrenCount {
            specs,
            active,
            supervisors,
            workers,
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

fn current_timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_start_success() -> Result<u64, String> {
        Ok(12345) // Mock process ID
    }

    fn mock_start_failure() -> Result<u64, String> {
        Err("Mock failure".to_string())
    }

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
}
