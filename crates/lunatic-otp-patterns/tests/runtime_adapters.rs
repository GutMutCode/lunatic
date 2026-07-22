use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use lunatic_otp_patterns::{
    Event, GenEvent, GenEventConfig, GenStatem, GenStatemConfig, HandlerOutcome, StateData,
    StopReason,
};
use serde::{Deserialize, Serialize};

static FINAL_STATE: Mutex<Option<(MachineState, i32)>> = Mutex::new(None);
static TRANSITIONS: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum MachineState {
    Idle,
    Active,
}

#[derive(Debug, Serialize, Deserialize)]
enum MachineEvent {
    Activate,
    Set(i32),
}

struct Machine;

impl GenStatem for Machine {
    type State = MachineState;
    type Data = i32;
    type Event = MachineEvent;

    fn init() -> StateData<Self::State, Self::Data> {
        StateData::new(MachineState::Idle, 0)
    }

    fn handle_event(
        &mut self,
        state: Self::State,
        event: Self::Event,
        data: Self::Data,
    ) -> StateData<Self::State, Self::Data> {
        match event {
            MachineEvent::Activate => StateData::new(MachineState::Active, data),
            MachineEvent::Set(value) => StateData::new(state, value),
        }
    }

    fn handle_timeout(
        &mut self,
        state: Self::State,
        data: Self::Data,
    ) -> StateData<Self::State, Self::Data> {
        StateData::new(state, data + 1)
    }

    fn exit_state(&mut self, _old_state: &Self::State, _data: &Self::Data) {
        TRANSITIONS
            .lock()
            .expect("transition mutex poisoned")
            .push("exit");
    }

    fn enter_state(&mut self, _new_state: &Self::State, _data: &Self::Data) {
        TRANSITIONS
            .lock()
            .expect("transition mutex poisoned")
            .push("enter");
    }

    fn terminate(&mut self, state: &Self::State, data: &Self::Data) {
        *FINAL_STATE.lock().expect("final state mutex poisoned") = Some((state.clone(), *data));
    }
}

struct SingleWorkerMachine;

impl GenStatem for SingleWorkerMachine {
    type State = MachineState;
    type Data = i32;
    type Event = MachineEvent;

    fn init() -> StateData<Self::State, Self::Data> {
        StateData::new(MachineState::Idle, 0)
    }

    fn handle_event(
        &mut self,
        state: Self::State,
        event: Self::Event,
        data: Self::Data,
    ) -> StateData<Self::State, Self::Data> {
        match event {
            MachineEvent::Activate => StateData::new(MachineState::Active, data),
            MachineEvent::Set(value) => StateData::new(state, value),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gen_statem_runs_transitions_and_lifecycle_in_a_lunatic_process() {
    *FINAL_STATE.lock().expect("final state mutex poisoned") = None;
    TRANSITIONS
        .lock()
        .expect("transition mutex poisoned")
        .clear();

    let handle = Machine.spawn(GenStatemConfig::default()).unwrap();
    assert!(handle.is_alive());
    handle.send_event(MachineEvent::Activate).unwrap();
    handle.send_event(MachineEvent::Set(41)).unwrap();
    handle.timeout().unwrap();
    handle.stop(StopReason::Normal).unwrap();

    assert!(!handle.is_alive());
    assert_eq!(
        *TRANSITIONS.lock().expect("transition mutex poisoned"),
        ["exit", "enter"]
    );
    assert_eq!(
        *FINAL_STATE.lock().expect("final state mutex poisoned"),
        Some((MachineState::Active, 42))
    );
    assert!(handle.send_event(MachineEvent::Set(0)).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn gen_statem_stop_does_not_block_a_single_tokio_worker() {
    let handle = SingleWorkerMachine
        .spawn(GenStatemConfig {
            timeout_ms: Some(2_000),
        })
        .unwrap();

    let client = tokio::spawn(async move {
        handle.send_event(MachineEvent::Activate).unwrap();
        handle.send_event(MachineEvent::Set(42)).unwrap();
        handle.stop(StopReason::Normal).unwrap();
    });

    client.await.unwrap();
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RuntimeEvent(usize);

impl Event for RuntimeEvent {}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gen_event_dispatches_through_a_lunatic_process_and_stops_cleanly() {
    let delivered = Arc::new(AtomicUsize::new(0));
    let manager = GenEvent::<RuntimeEvent>::new();
    let counter = delivered.clone();
    manager
        .add_handler("counter".to_string(), move |event| {
            counter.fetch_add(event.0, Ordering::SeqCst);
        })
        .await;
    manager
        .add_fallible_handler(
            "failure".to_string(),
            |_event| -> Result<(), &'static str> { Err("isolated failure") },
        )
        .await;

    let handle = manager.spawn(GenEventConfig::default()).unwrap();
    assert!(handle.is_alive());
    let report = handle.notify(RuntimeEvent(7)).unwrap();
    assert_eq!(delivered.load(Ordering::SeqCst), 7);
    assert_eq!(report.outcome("counter"), Some(&HandlerOutcome::Succeeded));
    assert_eq!(
        report.outcome("failure"),
        Some(&HandlerOutcome::Failed("isolated failure".to_string()))
    );

    handle.stop().unwrap();
    assert!(!handle.is_alive());
    assert!(handle
        .wait_for_exit(Some(Duration::from_millis(10)))
        .is_ok());
    assert!(handle.notify(RuntimeEvent(1)).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn gen_event_notify_and_stop_do_not_block_a_single_tokio_worker() {
    let delivered = Arc::new(AtomicUsize::new(0));
    let manager = GenEvent::<RuntimeEvent>::new();
    let counter = delivered.clone();
    manager
        .add_handler("counter".to_string(), move |event| {
            counter.fetch_add(event.0, Ordering::SeqCst);
        })
        .await;
    let handle = manager
        .spawn(GenEventConfig {
            timeout_ms: Some(2_000),
        })
        .unwrap();

    let client = tokio::spawn(async move {
        let report = handle.notify(RuntimeEvent(9)).unwrap();
        handle.stop().unwrap();
        report
    });
    let report = client.await.unwrap();

    assert_eq!(delivered.load(Ordering::SeqCst), 9);
    assert_eq!(report.outcome("counter"), Some(&HandlerOutcome::Succeeded));
}
