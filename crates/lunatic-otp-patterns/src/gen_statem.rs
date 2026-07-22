//! GenStatem: Generic State Machine Pattern
//!
//! Inspired by Erlang's `gen_statem`, this module provides a behavior pattern
//! for implementing finite state machines with event-driven transitions.
//!
//! ## Example
//!
//! ```ignore
//! use lunatic_otp_patterns::{GenStatem, StateData};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
//! enum DoorState {
//!     Locked,
//!     Unlocked,
//! }
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct DoorData {
//!     code: String,
//! }
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! enum DoorEvent {
//!     EnterCode(String),
//!     Lock,
//! }
//!
//! impl GenStatem for DoorStateMachine {
//!     type State = DoorState;
//!     type Data = DoorData;
//!     type Event = DoorEvent;
//!
//!     fn init() -> StateData<Self::State, Self::Data> {
//!         StateData {
//!             state: DoorState::Locked,
//!             data: DoorData { code: "1234".to_string() },
//!         }
//!     }
//!
//!     fn handle_event(
//!         &mut self,
//!         state: Self::State,
//!         event: Self::Event,
//!         data: Self::Data,
//!     ) -> StateData<Self::State, Self::Data> {
//!         match (state, event) {
//!             (DoorState::Locked, DoorEvent::EnterCode(code)) => {
//!                 if code == data.code {
//!                     StateData { state: DoorState::Unlocked, data }
//!                 } else {
//!                     StateData { state: DoorState::Locked, data }
//!                 }
//!             }
//!             (DoorState::Unlocked, DoorEvent::Lock) => {
//!                 StateData { state: DoorState::Locked, data }
//!             }
//!             _ => StateData { state, data },
//!         }
//!     }
//! }
//! ```

use anyhow::{anyhow, Context, Result};
use lunatic_process::{
    env::{Environment, LunaticEnvironment},
    message::{DataMessage, Message},
    spawn_native, Process, Signal,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    fmt::Debug,
    marker::PhantomData,
    sync::{Arc, Condvar, Mutex, OnceLock},
    time::Duration,
};

/// GenStatem behavior trait
///
/// Implement this trait to create a generic state machine following
/// Erlang's gen_statem pattern.
pub trait GenStatem: Sized {
    /// State type (must be comparable for transitions)
    type State: Clone + PartialEq + Serialize + DeserializeOwned + Debug + Send + 'static;

    /// Data type (state machine data)
    type Data: Clone + Serialize + DeserializeOwned + Debug + Send + 'static;

    /// Event type (triggers state transitions)
    type Event: Serialize + DeserializeOwned + Debug + Send + 'static;

    /// Initialize state machine
    ///
    /// Returns initial state and data
    fn init() -> StateData<Self::State, Self::Data>;

    /// Handle event in current state
    ///
    /// Returns new state and data (may be unchanged)
    fn handle_event(
        &mut self,
        state: Self::State,
        event: Self::Event,
        data: Self::Data,
    ) -> StateData<Self::State, Self::Data>;

    /// Callback when entering a new state (optional)
    ///
    /// Called after successful state transition
    fn enter_state(&mut self, _new_state: &Self::State, _data: &Self::Data) {
        // Default: no-op
    }

    /// Callback when exiting a state (optional)
    ///
    /// Called before state transition
    fn exit_state(&mut self, _old_state: &Self::State, _data: &Self::Data) {
        // Default: no-op
    }

    /// Termination callback (optional)
    ///
    /// Called when state machine is about to shut down
    fn terminate(&mut self, _state: &Self::State, _data: &Self::Data) {
        // Default: no-op
    }

    /// Handle a timeout delivered to the state-machine process.
    fn handle_timeout(
        &mut self,
        state: Self::State,
        data: Self::Data,
    ) -> StateData<Self::State, Self::Data> {
        StateData { state, data }
    }

    /// Spawn this state machine as a native Lunatic process.
    fn spawn(self, config: GenStatemConfig) -> Result<GenStatemHandle<Self::Event>>
    where
        Self: Send + 'static,
    {
        ensure_multi_thread_runtime()?;

        let environment = gen_statem_environment();
        let shared = Arc::new(GenStatemShared::new(environment.clone()));
        let (join, process) = spawn_native(environment, move |_process, mailbox| async move {
            let mut machine = self;
            let mut state_data = Self::init();

            loop {
                let data = match mailbox.pop(None).await {
                    Message::Data(data) => data,
                    Message::LinkDied(_) | Message::ProcessDied { .. } => continue,
                };
                let message: StatemMessage<Self::Event> =
                    bincode::deserialize(&data.buffer).context("invalid GenStatem message")?;

                match message {
                    StatemMessage::Event(event) => {
                        state_data =
                            apply_transition(&mut machine, state_data, |machine, state, data| {
                                machine.handle_event(state, event, data)
                            });
                    }
                    StatemMessage::Timeout => {
                        state_data =
                            apply_transition(&mut machine, state_data, |machine, state, data| {
                                machine.handle_timeout(state, data)
                            });
                    }
                    StatemMessage::Stop(_reason) => {
                        machine.terminate(&state_data.state, &state_data.data);
                        return Ok(());
                    }
                }
            }
        })?;

        let join_shared = shared.clone();
        tokio::spawn(async move {
            let result = match join.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(error.to_string()),
                Err(error) => Err(format!("process task failed: {error}")),
            };
            join_shared.finish(result);
        });

        Ok(GenStatemHandle::from_process(
            Arc::new(process),
            shared,
            config,
        ))
    }
}

fn apply_transition<M, F>(
    machine: &mut M,
    current: StateData<M::State, M::Data>,
    transition: F,
) -> StateData<M::State, M::Data>
where
    M: GenStatem,
    F: FnOnce(&mut M, M::State, M::Data) -> StateData<M::State, M::Data>,
{
    let previous_state = current.state.clone();
    let previous_data = current.data.clone();
    let next = transition(machine, current.state, current.data);
    if next.state != previous_state {
        machine.exit_state(&previous_state, &previous_data);
        machine.enter_state(&next.state, &next.data);
    }
    next
}

/// State and data container
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateData<S, D> {
    pub state: S,
    pub data: D,
}

impl<S, D> StateData<S, D> {
    pub fn new(state: S, data: D) -> Self {
        Self { state, data }
    }
}

/// State transition result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransitionResult<S, D> {
    /// Keep current state
    KeepState { data: D },

    /// Transition to new state
    NextState { state: S, data: D },

    /// Stop state machine
    Stop { reason: StopReason },
}

/// Reason for stopping state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StopReason {
    Normal,
    Shutdown,
    Error(String),
}

/// State machine handle for client interactions.
pub struct GenStatemHandle<Event = Vec<u8>> {
    pub process_id: u64,
    process: Arc<dyn Process>,
    shared: Arc<GenStatemShared>,
    config: GenStatemConfig,
    _event: PhantomData<Event>,
}

impl<Event> Clone for GenStatemHandle<Event> {
    fn clone(&self) -> Self {
        Self {
            process_id: self.process_id,
            process: self.process.clone(),
            shared: self.shared.clone(),
            config: self.config.clone(),
            _event: PhantomData,
        }
    }
}

impl<Event> Debug for GenStatemHandle<Event> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GenStatemHandle")
            .field("process_id", &self.process_id)
            .field("alive", &self.is_alive())
            .finish()
    }
}

impl<Event> GenStatemHandle<Event> {
    fn from_process(
        process: Arc<dyn Process>,
        shared: Arc<GenStatemShared>,
        config: GenStatemConfig,
    ) -> Self {
        Self {
            process_id: process.id(),
            process,
            shared,
            config,
            _event: PhantomData,
        }
    }

    pub fn id(&self) -> u64 {
        self.process_id
    }

    pub fn is_alive(&self) -> bool {
        self.shared.result().is_none()
            && self
                .shared
                .environment
                .get_process(self.process_id)
                .is_some()
    }

    pub fn wait_for_exit(&self, timeout: Option<Duration>) -> Result<()> {
        self.shared.wait_for_exit(timeout)
    }
}

impl<Event> GenStatemHandle<Event>
where
    Event: Serialize + DeserializeOwned + Debug,
{
    pub fn send_event(&self, event: Event) -> Result<()> {
        self.send(StatemMessage::Event(event))
    }

    pub fn timeout(&self) -> Result<()> {
        self.send(StatemMessage::Timeout)
    }

    pub fn stop(&self, reason: StopReason) -> Result<()> {
        self.send(StatemMessage::Stop(reason))?;
        self.shared.wait_for_exit(self.config.timeout())
    }

    pub fn kill(&self) -> Result<()> {
        self.ensure_running()?;
        self.process
            .send(Signal::Kill)
            .map_err(|error| anyhow!("Failed to kill GenStatem: {error}"))?;
        self.shared.wait_result(self.config.timeout()).map(|_| ())
    }

    fn send(&self, message: StatemMessage<Event>) -> Result<()> {
        self.ensure_running()?;
        let payload =
            bincode::serialize(&message).context("failed to serialize GenStatem message")?;
        self.process
            .send(Signal::Message(Message::Data(DataMessage::new_from_vec(
                None, payload,
            ))))
            .map_err(|error| anyhow!("Failed to send GenStatem message: {error}"))
    }

    fn ensure_running(&self) -> Result<()> {
        match self.shared.result() {
            None if self.is_alive() => Ok(()),
            Some(Ok(())) => Err(anyhow!("GenStatem process {} has stopped", self.id())),
            Some(Err(error)) => Err(anyhow!(
                "GenStatem process {} terminated: {}",
                self.id(),
                error
            )),
            None => Err(anyhow!(
                "GenStatem process {} is not registered in its environment",
                self.id()
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GenStatemConfig {
    pub timeout_ms: Option<u64>,
}

impl Default for GenStatemConfig {
    fn default() -> Self {
        Self {
            timeout_ms: Some(5_000),
        }
    }
}

impl GenStatemConfig {
    fn timeout(&self) -> Option<Duration> {
        self.timeout_ms.map(Duration::from_millis)
    }
}

struct GenStatemShared {
    environment: Arc<LunaticEnvironment>,
    exit: (Mutex<Option<std::result::Result<(), String>>>, Condvar),
}

impl GenStatemShared {
    fn new(environment: Arc<LunaticEnvironment>) -> Self {
        Self {
            environment,
            exit: (Mutex::new(None), Condvar::new()),
        }
    }

    fn finish(&self, result: std::result::Result<(), String>) {
        let mut exit = self.exit.0.lock().expect("GenStatem exit mutex poisoned");
        if exit.is_none() {
            *exit = Some(result);
            self.exit.1.notify_all();
        }
    }

    fn result(&self) -> Option<std::result::Result<(), String>> {
        self.exit
            .0
            .lock()
            .expect("GenStatem exit mutex poisoned")
            .clone()
    }

    fn wait_for_exit(&self, timeout: Option<Duration>) -> Result<()> {
        self.wait_result(timeout)?.map_err(anyhow::Error::msg)
    }

    fn wait_result(&self, timeout: Option<Duration>) -> Result<std::result::Result<(), String>> {
        blocking_wait(|| {
            let exit = self.exit.0.lock().expect("GenStatem exit mutex poisoned");
            let exit = match timeout {
                Some(timeout) => {
                    let (exit, wait) = self
                        .exit
                        .1
                        .wait_timeout_while(exit, timeout, |result| result.is_none())
                        .expect("GenStatem exit mutex poisoned");
                    if wait.timed_out() && exit.is_none() {
                        return Err(anyhow!("timed out waiting for GenStatem to exit"));
                    }
                    exit
                }
                None => self
                    .exit
                    .1
                    .wait_while(exit, |result| result.is_none())
                    .expect("GenStatem exit mutex poisoned"),
            };
            exit.clone()
                .ok_or_else(|| anyhow!("GenStatem exit status unavailable"))
        })
    }
}

fn gen_statem_environment() -> Arc<LunaticEnvironment> {
    static ENVIRONMENT: OnceLock<Arc<LunaticEnvironment>> = OnceLock::new();
    ENVIRONMENT
        .get_or_init(|| Arc::new(LunaticEnvironment::new(1)))
        .clone()
}

fn ensure_multi_thread_runtime() -> Result<()> {
    let runtime = tokio::runtime::Handle::try_current()
        .context("GenStatem::spawn requires a multi-thread Tokio runtime")?;
    if matches!(
        runtime.runtime_flavor(),
        tokio::runtime::RuntimeFlavor::CurrentThread
    ) {
        return Err(anyhow!(
            "GenStatem::spawn requires a multi-thread Tokio runtime"
        ));
    }
    Ok(())
}

fn blocking_wait<T>(wait: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(runtime)
            if matches!(
                runtime.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) =>
        {
            tokio::task::block_in_place(wait)
        }
        _ => wait(),
    }
}

/// State machine event message envelope
#[derive(Debug, Serialize, Deserialize)]
pub enum StatemMessage<Event> {
    /// External event
    Event(Event),
    /// Timeout event
    Timeout,
    /// Stop signal
    Stop(StopReason),
}

/// Example: Traffic Light State Machine
#[cfg(test)]
mod traffic_light_example {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum TrafficLightState {
        Red,
        Yellow,
        Green,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TrafficLightData {
        cycle_count: u32,
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum TrafficLightEvent {
        Timer,
        Emergency,
    }

    struct TrafficLight;

    impl GenStatem for TrafficLight {
        type State = TrafficLightState;
        type Data = TrafficLightData;
        type Event = TrafficLightEvent;

        fn init() -> StateData<Self::State, Self::Data> {
            StateData {
                state: TrafficLightState::Red,
                data: TrafficLightData { cycle_count: 0 },
            }
        }

        fn handle_event(
            &mut self,
            state: Self::State,
            event: Self::Event,
            mut data: Self::Data,
        ) -> StateData<Self::State, Self::Data> {
            match (state, event) {
                (TrafficLightState::Red, TrafficLightEvent::Timer) => StateData {
                    state: TrafficLightState::Green,
                    data,
                },
                (TrafficLightState::Green, TrafficLightEvent::Timer) => StateData {
                    state: TrafficLightState::Yellow,
                    data,
                },
                (TrafficLightState::Yellow, TrafficLightEvent::Timer) => {
                    data.cycle_count += 1;
                    StateData {
                        state: TrafficLightState::Red,
                        data,
                    }
                }
                (_, TrafficLightEvent::Emergency) => StateData {
                    state: TrafficLightState::Red,
                    data,
                },
            }
        }

        fn enter_state(&mut self, new_state: &Self::State, _data: &Self::Data) {
            println!("Entering state: {:?}", new_state);
        }

        fn exit_state(&mut self, old_state: &Self::State, _data: &Self::Data) {
            println!("Exiting state: {:?}", old_state);
        }
    }

    #[test]
    fn test_traffic_light_transitions() {
        let mut sm = TrafficLight;
        let state_data = TrafficLight::init();

        assert_eq!(state_data.state, TrafficLightState::Red);

        // Red -> Green
        let state_data =
            sm.handle_event(state_data.state, TrafficLightEvent::Timer, state_data.data);
        assert_eq!(state_data.state, TrafficLightState::Green);

        // Green -> Yellow
        let state_data =
            sm.handle_event(state_data.state, TrafficLightEvent::Timer, state_data.data);
        assert_eq!(state_data.state, TrafficLightState::Yellow);

        // Yellow -> Red (cycle completes)
        let state_data =
            sm.handle_event(state_data.state, TrafficLightEvent::Timer, state_data.data);
        assert_eq!(state_data.state, TrafficLightState::Red);
        assert_eq!(state_data.data.cycle_count, 1);
    }

    #[test]
    fn test_emergency_transition() {
        let mut sm = TrafficLight;
        let state_data = StateData {
            state: TrafficLightState::Green,
            data: TrafficLightData { cycle_count: 0 },
        };

        // Emergency from any state -> Red
        let state_data = sm.handle_event(
            state_data.state,
            TrafficLightEvent::Emergency,
            state_data.data,
        );
        assert_eq!(state_data.state, TrafficLightState::Red);
    }
}

/// Example: Door Lock State Machine
#[cfg(test)]
mod door_lock_example {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum DoorState {
        Locked,
        Unlocked,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct DoorData {
        code: String,
        attempts: u32,
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum DoorEvent {
        EnterCode(String),
        Lock,
        Reset,
    }

    struct DoorLock;

    impl GenStatem for DoorLock {
        type State = DoorState;
        type Data = DoorData;
        type Event = DoorEvent;

        fn init() -> StateData<Self::State, Self::Data> {
            StateData {
                state: DoorState::Locked,
                data: DoorData {
                    code: "1234".to_string(),
                    attempts: 0,
                },
            }
        }

        fn handle_event(
            &mut self,
            state: Self::State,
            event: Self::Event,
            mut data: Self::Data,
        ) -> StateData<Self::State, Self::Data> {
            match (state, event) {
                (DoorState::Locked, DoorEvent::EnterCode(code)) => {
                    if code == data.code {
                        data.attempts = 0;
                        StateData {
                            state: DoorState::Unlocked,
                            data,
                        }
                    } else {
                        data.attempts += 1;
                        StateData {
                            state: DoorState::Locked,
                            data,
                        }
                    }
                }
                (DoorState::Unlocked, DoorEvent::Lock) => StateData {
                    state: DoorState::Locked,
                    data,
                },
                (_, DoorEvent::Reset) => {
                    data.attempts = 0;
                    StateData {
                        state: DoorState::Locked,
                        data,
                    }
                }
                (state, _) => StateData { state, data },
            }
        }
    }

    #[test]
    fn test_door_unlock() {
        let mut sm = DoorLock;
        let state_data = DoorLock::init();

        assert_eq!(state_data.state, DoorState::Locked);

        // Wrong code
        let state_data = sm.handle_event(
            state_data.state,
            DoorEvent::EnterCode("0000".to_string()),
            state_data.data,
        );
        assert_eq!(state_data.state, DoorState::Locked);
        assert_eq!(state_data.data.attempts, 1);

        // Correct code
        let state_data = sm.handle_event(
            state_data.state,
            DoorEvent::EnterCode("1234".to_string()),
            state_data.data,
        );
        assert_eq!(state_data.state, DoorState::Unlocked);
        assert_eq!(state_data.data.attempts, 0);

        // Lock again
        let state_data = sm.handle_event(state_data.state, DoorEvent::Lock, state_data.data);
        assert_eq!(state_data.state, DoorState::Locked);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_data_creation() {
        let state_data: StateData<String, i32> = StateData::new("init".to_string(), 0);
        assert_eq!(state_data.state, "init");
        assert_eq!(state_data.data, 0);
    }

    #[test]
    fn test_stop_reason_serialization() {
        let reason = StopReason::Normal;
        let serialized = serde_json::to_string(&reason).unwrap();
        let deserialized: StopReason = serde_json::from_str(&serialized).unwrap();
        assert!(matches!(deserialized, StopReason::Normal));
    }

    #[test]
    fn test_statem_message() {
        let msg: StatemMessage<String> = StatemMessage::Event("test".to_string());
        let serialized = serde_json::to_string(&msg).unwrap();
        let deserialized: StatemMessage<String> = serde_json::from_str(&serialized).unwrap();

        match deserialized {
            StatemMessage::Event(e) => assert_eq!(e, "test"),
            _ => panic!("Expected Event"),
        }
    }
}
