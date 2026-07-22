use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
    time::Duration,
};

use anyhow::{anyhow, Result};
use lunatic_distributed::distributed::{DistributedRegistry, GlobalProcessId};
use lunatic_otp_patterns::{GenServer, GenServerConfig, GenServerRuntime, TerminateReason};
use lunatic_process::env::{Environment, LunaticEnvironment};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct Counter {
    value: i64,
}

#[derive(Debug, Serialize, Deserialize)]
enum Call {
    Get,
    Sleep(u64),
    Fail,
}

#[derive(Debug, Serialize, Deserialize)]
enum Cast {
    Add(i64),
    Fail,
    Panic,
}

#[derive(Debug, Serialize, Deserialize)]
struct Reply(i64);

impl GenServer for Counter {
    type State = Self;
    type Call = Call;
    type CallReply = Reply;
    type Cast = Cast;

    fn init() -> Self::State {
        Self { value: 0 }
    }

    fn handle_call(&mut self, request: Self::Call) -> Result<Self::CallReply> {
        match request {
            Call::Get => Ok(Reply(self.value)),
            Call::Sleep(delay_ms) => {
                std::thread::sleep(Duration::from_millis(delay_ms));
                Ok(Reply(self.value))
            }
            Call::Fail => Err(anyhow!("requested call failure")),
        }
    }

    fn handle_cast(&mut self, request: Self::Cast) -> Result<()> {
        match request {
            Cast::Add(amount) => {
                self.value += amount;
                Ok(())
            }
            Cast::Fail => Err(anyhow!("requested cast failure")),
            Cast::Panic => panic!("requested cast panic"),
        }
    }
}

static COUNTING_SERVER_INITS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct CountingServer;

impl GenServer for CountingServer {
    type State = Self;
    type Call = Call;
    type CallReply = Reply;
    type Cast = Cast;

    fn init() -> Self::State {
        COUNTING_SERVER_INITS.fetch_add(1, Ordering::SeqCst);
        Self
    }

    fn handle_call(&mut self, _request: Self::Call) -> Result<Self::CallReply> {
        Ok(Reply(0))
    }

    fn handle_cast(&mut self, request: Self::Cast) -> Result<()> {
        match request {
            Cast::Fail => Err(anyhow!("requested cast failure")),
            Cast::Panic => panic!("requested cast panic"),
            Cast::Add(_) => Ok(()),
        }
    }
}

fn isolated_runtime(
    node_id: u64,
    environment_id: u64,
) -> (
    GenServerRuntime,
    Arc<LunaticEnvironment>,
    Arc<DistributedRegistry>,
) {
    let environment = Arc::new(LunaticEnvironment::new(environment_id));
    let registry = Arc::new(DistributedRegistry::new(node_id));
    let runtime = GenServerRuntime::new(environment.clone(), node_id, registry.clone());
    (runtime, environment, registry)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_lunatic_process_handles_call_cast_and_stop() {
    let handle = Counter::spawn(GenServerConfig::default()).unwrap();
    assert!(handle.is_alive());

    handle.cast(Cast::Add(40)).unwrap();
    handle.cast(Cast::Add(2)).unwrap();
    assert_eq!(handle.call(Call::Get).unwrap().0, 42);

    handle.stop(TerminateReason::Normal).unwrap();
    assert!(!handle.is_alive());
    assert!(handle.call(Call::Get).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_timeout_does_not_poison_the_server() {
    let handle = Counter::spawn(GenServerConfig {
        name: Some("timeout-counter".to_string()),
        // Leave enough room for a fast follow-up call on a loaded CI runner;
        // the deliberately slow call still exceeds this deadline by 3x.
        timeout_ms: Some(250),
    })
    .unwrap();

    let error = handle.call(Call::Sleep(750)).unwrap_err();
    assert!(error.to_string().contains("timed out"));

    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert_eq!(handle.call(Call::Get).unwrap().0, 0);
    handle.stop(TerminateReason::Normal).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_errors_are_returned_and_cast_errors_terminate() {
    let handle = Counter::spawn(GenServerConfig::default()).unwrap();

    let error = handle.call(Call::Fail).unwrap_err();
    assert!(error.to_string().contains("requested call failure"));
    assert!(handle.is_alive());

    handle.cast(Cast::Fail).unwrap();
    let error = handle
        .wait_for_exit(Some(Duration::from_secs(1)))
        .unwrap_err();
    assert!(error.to_string().contains("requested cast failure"));
    assert!(!handle.is_alive());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_removes_the_process_and_rejects_new_messages() {
    let handle = Counter::spawn(GenServerConfig::default()).unwrap();

    handle.kill().unwrap();
    assert!(!handle.is_alive());
    assert!(handle.cast(Cast::Add(1)).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn named_server_registers_in_the_runtime_registry_and_cleans_up_on_stop() {
    let (runtime, _environment, registry) = isolated_runtime(11, 101);
    let handle = Counter::spawn_in(
        runtime.clone(),
        GenServerConfig {
            name: Some("named-counter".to_string()),
            timeout_ms: Some(1_000),
        },
    )
    .unwrap();
    let expected = GlobalProcessId::new(11, 101, handle.id());

    assert_eq!(runtime.whereis("named-counter"), Some(expected));
    assert_eq!(
        registry
            .lookup_local("named-counter")
            .map(|entry| entry.global_pid),
        Some(expected)
    );

    handle.stop(TerminateReason::Normal).unwrap();
    assert_eq!(runtime.whereis("named-counter"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_named_spawn_has_one_winner_and_does_not_initialize_the_loser() {
    COUNTING_SERVER_INITS.store(0, Ordering::SeqCst);
    let (runtime, _environment, _registry) = isolated_runtime(12, 102);
    let barrier = Arc::new(Barrier::new(2));
    let tokio_runtime = tokio::runtime::Handle::current();

    let spawn = |runtime: GenServerRuntime, barrier: Arc<Barrier>| {
        let tokio_runtime = tokio_runtime.clone();
        std::thread::spawn(move || {
            let _runtime_guard = tokio_runtime.enter();
            barrier.wait();
            CountingServer::spawn_in(
                runtime,
                GenServerConfig {
                    name: Some("contended-counter".to_string()),
                    timeout_ms: Some(1_000),
                },
            )
        })
    };

    let first_thread = spawn(runtime.clone(), barrier.clone());
    let second_thread = spawn(runtime.clone(), barrier);
    let first = first_thread.join().unwrap();
    let second = second_thread.join().unwrap();
    let (winner, loser) = match (first, second) {
        (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
        (Ok(_), Ok(_)) => panic!("both GenServers acquired the same local name"),
        (Err(first), Err(second)) => {
            panic!("both named spawns failed: first={first}, second={second}")
        }
    };

    assert!(loser.to_string().contains("already registered locally"));
    assert_eq!(winner.call(Call::Get).unwrap().0, 0);
    assert_eq!(COUNTING_SERVER_INITS.load(Ordering::SeqCst), 1);
    assert_eq!(
        runtime.whereis("contended-counter"),
        Some(GlobalProcessId::new(12, 102, winner.id()))
    );

    winner.stop(TerminateReason::Normal).unwrap();
    assert_eq!(runtime.whereis("contended-counter"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abnormal_exit_and_kill_release_a_name_for_immediate_reuse() {
    let (runtime, _environment, _registry) = isolated_runtime(13, 103);
    let config = || GenServerConfig {
        name: Some("reusable-counter".to_string()),
        timeout_ms: Some(1_000),
    };

    let failed = Counter::spawn_in(runtime.clone(), config()).unwrap();
    failed.cast(Cast::Fail).unwrap();
    assert!(failed.wait_for_exit(Some(Duration::from_secs(1))).is_err());
    assert_eq!(runtime.whereis("reusable-counter"), None);

    let panicked = Counter::spawn_in(runtime.clone(), config()).unwrap();
    panicked.cast(Cast::Panic).unwrap();
    assert!(panicked
        .wait_for_exit(Some(Duration::from_secs(1)))
        .is_err());
    assert_eq!(runtime.whereis("reusable-counter"), None);

    let killed = Counter::spawn_in(runtime.clone(), config()).unwrap();
    killed.kill().unwrap();
    assert_eq!(runtime.whereis("reusable-counter"), None);

    let reused = Counter::spawn_in(runtime.clone(), config()).unwrap();
    reused.stop(TerminateReason::Normal).unwrap();
    assert_eq!(runtime.whereis("reusable-counter"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_name_fails_without_registry_or_process_leaks() {
    let (runtime, environment, registry) = isolated_runtime(14, 104);
    let baseline = registry.usage();

    let error = Counter::spawn_in(
        runtime,
        GenServerConfig {
            name: Some(String::new()),
            timeout_ms: Some(1_000),
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("must not be empty"));
    assert_eq!(registry.usage(), baseline);
    tokio::time::timeout(Duration::from_secs(1), async {
        while environment.process_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed named spawn leaked its provisional process");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn synchronous_handle_waits_do_not_block_a_single_tokio_worker() {
    let handle = Counter::spawn(GenServerConfig {
        name: None,
        timeout_ms: Some(2_000),
    })
    .unwrap();

    let client = tokio::spawn(async move {
        handle.cast(Cast::Add(42)).unwrap();
        let reply = handle.call(Call::Get).unwrap();
        handle.stop(TerminateReason::Normal).unwrap();
        reply.0
    });

    assert_eq!(client.await.unwrap(), 42);
}
