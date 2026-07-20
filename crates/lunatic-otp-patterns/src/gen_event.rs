//! GenEvent - Event handling pattern inspired by Erlang's gen_event
//!
//! GenEvent provides a way to manage multiple event handlers that can process
//! events asynchronously. It's useful for logging, monitoring, and other
//! cross-cutting concerns.

use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    fmt::Display,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
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
    handlers: Arc<RwLock<HashMap<String, Arc<EventHandler<E>>>>>,
}

type EventHandler<E> = dyn Fn(E) -> Result<(), String> + Send + Sync + 'static;

/// Result of delivering an event to one handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerOutcome {
    /// The handler returned normally.
    Succeeded,
    /// The handler returned an error.
    Failed(String),
    /// The handler panicked. Other deliveries continue.
    Panicked(String),
    /// Tokio could not complete the blocking task that ran the handler.
    RuntimeFailed(String),
}

impl HandlerOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

/// Outcomes for the handler snapshot used by one `notify` call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NotifyReport {
    outcomes: BTreeMap<String, HandlerOutcome>,
}

impl NotifyReport {
    pub fn outcomes(&self) -> &BTreeMap<String, HandlerOutcome> {
        &self.outcomes
    }

    pub fn outcome(&self, id: &str) -> Option<&HandlerOutcome> {
        self.outcomes.get(id)
    }

    pub fn len(&self) -> usize {
        self.outcomes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.outcomes.is_empty()
    }

    pub fn all_succeeded(&self) -> bool {
        self.outcomes.values().all(HandlerOutcome::is_success)
    }
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
        let handler: Arc<EventHandler<E>> = Arc::new(move |event| {
            handler(event);
            Ok(())
        });
        let mut handlers = self.handlers.write().await;
        handlers.insert(id, handler);
    }

    /// Add a handler that can report an error without stopping other handlers.
    pub async fn add_fallible_handler<F, Error>(&self, id: String, handler: F)
    where
        F: Fn(E) -> Result<(), Error> + Send + Sync + 'static,
        Error: Display,
    {
        let handler: Arc<EventHandler<E>> =
            Arc::new(move |event| handler(event).map_err(|error| error.to_string()));
        let mut handlers = self.handlers.write().await;
        handlers.insert(id, handler);
    }

    /// Remove an event handler
    pub async fn remove_handler(&self, id: &str) -> bool {
        let mut handlers = self.handlers.write().await;
        handlers.remove(id).is_some()
    }

    /// Send an event concurrently to the current handler snapshot.
    ///
    /// The read lock is released before user code runs. Handlers added after
    /// the snapshot do not receive this event, while removing or replacing a
    /// snapshotted handler does not cancel its in-flight delivery.
    pub async fn notify(&self, event: E) -> NotifyReport {
        let handlers = {
            let handlers = self.handlers.read().await;
            handlers
                .iter()
                .map(|(id, handler)| (id.clone(), handler.clone()))
                .collect::<Vec<_>>()
        };

        let deliveries = handlers
            .into_iter()
            .map(|(id, handler)| {
                let event = event.clone();
                let delivery = tokio::task::spawn_blocking(move || run_handler(handler, event));
                (id, delivery)
            })
            .collect::<Vec<_>>();

        let mut report = NotifyReport::default();
        for (id, delivery) in deliveries {
            report.outcomes.insert(id, delivery_outcome(delivery).await);
        }
        report
    }

    /// Send an event to exactly one handler.
    ///
    /// Returns `None` when no handler with `id` existed at snapshot time.
    pub async fn notify_handler(&self, id: &str, event: E) -> Option<HandlerOutcome> {
        let handler = {
            let handlers = self.handlers.read().await;
            handlers.get(id).cloned()
        }?;

        let delivery = tokio::task::spawn_blocking(move || run_handler(handler, event));
        Some(delivery_outcome(delivery).await)
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

fn run_handler<E: Event>(handler: Arc<EventHandler<E>>, event: E) -> HandlerOutcome {
    match catch_unwind(AssertUnwindSafe(|| handler(event))) {
        Ok(Ok(())) => HandlerOutcome::Succeeded,
        Ok(Err(error)) => HandlerOutcome::Failed(error),
        Err(payload) => HandlerOutcome::Panicked(panic_message(payload)),
    }
}

async fn delivery_outcome(delivery: tokio::task::JoinHandle<HandlerOutcome>) -> HandlerOutcome {
    match delivery.await {
        Ok(outcome) => outcome,
        Err(error) => HandlerOutcome::RuntimeFailed(error.to_string()),
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "handler panicked with a non-string payload".to_string()
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
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc, Arc, Mutex,
        },
        time::Duration,
    };

    use tokio::sync::Notify;

    use super::*;

    #[tokio::test]
    async fn test_gen_event_basic() {
        let gen_event = GenEvent::<LogEvent>::new();

        // Add handler
        gen_event
            .add_handler("console".to_string(), console_logger)
            .await;

        // Send event
        let report = gen_event
            .notify(LogEvent::Info("Test message".to_string()))
            .await;
        assert_eq!(report.outcome("console"), Some(&HandlerOutcome::Succeeded));
        assert!(report.all_succeeded());

        // Check handlers
        assert_eq!(gen_event.count_handlers().await, 1);
        assert_eq!(
            gen_event.which_handlers().await,
            vec!["console".to_string()]
        );
    }

    #[tokio::test]
    async fn test_metrics_collector() {
        let gen_event = GenEvent::<MetricEvent>::new();

        gen_event
            .add_handler("metrics".to_string(), metrics_collector)
            .await;

        // Send counter events
        let first = gen_event
            .notify(MetricEvent::Counter {
                name: "requests".to_string(),
                value: 5,
            })
            .await;
        assert!(first.all_succeeded());

        let second = gen_event
            .notify(MetricEvent::Counter {
                name: "requests".to_string(),
                value: 3,
            })
            .await;
        assert!(second.all_succeeded());
    }

    #[tokio::test]
    async fn notify_handler_calls_exactly_one_handler() {
        let gen_event = GenEvent::<LogEvent>::new();
        let selected_calls = Arc::new(AtomicUsize::new(0));
        let other_calls = Arc::new(AtomicUsize::new(0));

        let selected_counter = selected_calls.clone();
        gen_event
            .add_handler("selected".to_string(), move |_| {
                selected_counter.fetch_add(1, Ordering::SeqCst);
            })
            .await;

        let other_counter = other_calls.clone();
        gen_event
            .add_handler("other".to_string(), move |_| {
                other_counter.fetch_add(1, Ordering::SeqCst);
            })
            .await;

        let outcome = gen_event
            .notify_handler("selected", LogEvent::Info("targeted".to_string()))
            .await;

        assert_eq!(outcome, Some(HandlerOutcome::Succeeded));
        assert_eq!(selected_calls.load(Ordering::SeqCst), 1);
        assert_eq!(other_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            gen_event
                .notify_handler("missing", LogEvent::Info("ignored".to_string()))
                .await,
            None
        );
    }

    #[tokio::test]
    async fn slow_handler_does_not_block_other_delivery_or_handler_changes() {
        let gen_event = Arc::new(GenEvent::<LogEvent>::new());
        let (slow_started_tx, slow_started_rx) = mpsc::channel();
        let (release_slow_tx, release_slow_rx) = mpsc::channel();
        let release_slow_rx = Arc::new(Mutex::new(release_slow_rx));
        let fast_delivered = Arc::new(Notify::new());

        let release = release_slow_rx.clone();
        gen_event
            .add_handler("slow".to_string(), move |_| {
                slow_started_tx.send(()).unwrap();
                release.lock().unwrap().recv().unwrap();
            })
            .await;

        let delivered = fast_delivered.clone();
        gen_event
            .add_handler("fast".to_string(), move |_| {
                delivered.notify_one();
            })
            .await;

        let manager = gen_event.clone();
        let delivery = tokio::spawn(async move {
            manager
                .notify(LogEvent::Info("concurrent".to_string()))
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match slow_started_rx.try_recv() {
                    Ok(()) => break,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        panic!("slow handler exited before starting")
                    }
                }
            }
        })
        .await
        .expect("slow handler starts");

        tokio::time::timeout(Duration::from_secs(1), fast_delivered.notified())
            .await
            .expect("fast handler is not blocked by the slow handler");

        tokio::time::timeout(
            Duration::from_secs(1),
            gen_event.add_handler("late".to_string(), |_| {}),
        )
        .await
        .expect("add_handler is not blocked by user code");
        assert!(
            tokio::time::timeout(Duration::from_secs(1), gen_event.remove_handler("slow"))
                .await
                .expect("remove_handler is not blocked by user code")
        );

        release_slow_tx.send(()).unwrap();
        let report = delivery.await.unwrap();

        assert_eq!(report.len(), 2);
        assert_eq!(report.outcome("slow"), Some(&HandlerOutcome::Succeeded));
        assert_eq!(report.outcome("fast"), Some(&HandlerOutcome::Succeeded));
        assert_eq!(report.outcome("late"), None);
        assert_eq!(gen_event.count_handlers().await, 2);
    }

    #[tokio::test]
    async fn panic_and_error_are_isolated_from_other_handlers() {
        let gen_event = GenEvent::<LogEvent>::new();
        let healthy_calls = Arc::new(AtomicUsize::new(0));

        gen_event
            .add_handler("panic".to_string(), |_| {
                panic!("expected handler panic");
            })
            .await;
        gen_event
            .add_fallible_handler("error".to_string(), |_| -> Result<(), &'static str> {
                Err("expected handler error")
            })
            .await;

        let counter = healthy_calls.clone();
        gen_event
            .add_handler("healthy".to_string(), move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
            })
            .await;

        let report = gen_event
            .notify(LogEvent::Warning("isolation".to_string()))
            .await;

        assert!(matches!(
            report.outcome("panic"),
            Some(HandlerOutcome::Panicked(message)) if message == "expected handler panic"
        ));
        assert_eq!(
            report.outcome("error"),
            Some(&HandlerOutcome::Failed(
                "expected handler error".to_string()
            ))
        );
        assert_eq!(report.outcome("healthy"), Some(&HandlerOutcome::Succeeded));
        assert_eq!(healthy_calls.load(Ordering::SeqCst), 1);
        assert!(!report.all_succeeded());

        assert_eq!(
            gen_event
                .notify_handler("healthy", LogEvent::Info("still alive".to_string()))
                .await,
            Some(HandlerOutcome::Succeeded)
        );
        assert_eq!(healthy_calls.load(Ordering::SeqCst), 2);
    }
}
