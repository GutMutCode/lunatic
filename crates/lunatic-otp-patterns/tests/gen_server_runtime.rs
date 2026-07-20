use std::time::Duration;

use anyhow::{anyhow, Result};
use lunatic_otp_patterns::{GenServer, GenServerConfig, TerminateReason};
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
        }
    }
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
        timeout_ms: Some(15),
    })
    .unwrap();

    let error = handle.call(Call::Sleep(60)).unwrap_err();
    assert!(error.to_string().contains("timed out"));

    tokio::time::sleep(Duration::from_millis(80)).await;
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
