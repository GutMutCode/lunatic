//! GenEvent - Event handling pattern inspired by Erlang's gen_event
//!
//! GenEvent provides a way to manage multiple event handlers that can process
//! events asynchronously. It's useful for logging, monitoring, and other
//! cross-cutting concerns.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Events that can be sent to handlers
pub trait Event: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static {}

/// Termination reason for event handlers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TerminateReason {
    /// Normal shutdown
    Normal,
    /// Handler crashed
    Shutdown,
    /// Removed by supervisor
    Removed,
    /// Custom reason
    Other(String),
}

/// GenEvent manager
pub struct GenEvent<E: Event> {
    handlers: Arc<RwLock<HashMap<String, Box<dyn Fn(E) + Send + Sync>>>>,
}

impl<E: Event> GenEvent<E> {
    /// Create a new GenEvent manager
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add an event handler
    pub async fn add_handler<F>(&self, id: String, handler: F)
    where
        F: Fn(E) + Send + Sync + 'static,
    {
        let mut handlers = self.handlers.write().await;
        handlers.insert(id, Box::new(handler));
    }

    /// Remove an event handler
    pub async fn remove_handler(&self, id: &str) -> bool {
        let mut handlers = self.handlers.write().await;
        handlers.remove(id).is_some()
    }

    /// Send an event to all handlers
    pub async fn notify(&self, event: E) {
        let handlers = self.handlers.read().await;

        for (_id, handler) in handlers.iter() {
            handler(event.clone());
        }
    }

    /// Send an event to a specific handler
    pub async fn notify_handler(&self, id: &str, event: E) {
        let handlers = self.handlers.read().await;
        if let Some(handler) = handlers.get(id) {
            // Clone handler for async processing
            // Note: In real implementation, handlers would need to be Send
            // This is a simplified version
        }
    }

    /// Get list of active handler IDs
    pub async fn which_handlers(&self) -> Vec<String> {
        let handlers = self.handlers.read().await;
        handlers.keys().cloned().collect()
    }

    /// Get count of active handlers
    pub async fn count_handlers(&self) -> usize {
        let handlers = self.handlers.read().await;
        handlers.len()
    }
}

impl<E: Event> Default for GenEvent<E> {
    fn default() -> Self {
        Self::new()
    }
}

// Example event types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogEvent {
    Info(String),
    Warning(String),
    Error(String),
}

impl Event for LogEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricEvent {
    Counter { name: String, value: i64 },
    Gauge { name: String, value: f64 },
    Histogram { name: String, value: f64 },
}

impl Event for MetricEvent {}

// Example handler functions
pub fn console_logger(event: LogEvent) {
    match event {
        LogEvent::Info(msg) => println!("INFO: {}", msg),
        LogEvent::Warning(msg) => println!("WARN: {}", msg),
        LogEvent::Error(msg) => eprintln!("ERROR: {}", msg),
    }
}

pub fn metrics_collector(event: MetricEvent) {
    match event {
        MetricEvent::Counter { name, value } => {
            println!("Counter {} incremented by {}", name, value);
        }
        MetricEvent::Gauge { name, value } => {
            println!("Gauge {}: {}", name, value);
        }
        MetricEvent::Histogram { name, value } => {
            println!("Histogram {} recorded: {}", name, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_gen_event_basic() {
        let gen_event = GenEvent::<LogEvent>::new();

        // Add handler
        gen_event.add_handler("console".to_string(), console_logger).await;

        // Send event
        gen_event.notify(LogEvent::Info("Test message".to_string())).await;

        // Check handlers
        assert_eq!(gen_event.count_handlers().await, 1);
        assert_eq!(gen_event.which_handlers().await, vec!["console".to_string()]);
    }

    #[tokio::test]
    async fn test_metrics_collector() {
        let gen_event = GenEvent::<MetricEvent>::new();

        gen_event.add_handler("metrics".to_string(), metrics_collector).await;

        // Send counter events
        gen_event.notify(MetricEvent::Counter {
            name: "requests".to_string(),
            value: 5,
        }).await;

        gen_event.notify(MetricEvent::Counter {
            name: "requests".to_string(),
            value: 3,
        }).await;
    }
}