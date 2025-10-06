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

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// GenStatem behavior trait
///
/// Implement this trait to create a generic state machine following
/// Erlang's gen_statem pattern.
pub trait GenStatem: Sized {
    /// State type (must be comparable for transitions)
    type State: Clone + PartialEq + Serialize + for<'de> Deserialize<'de> + Debug;

    /// Data type (state machine data)
    type Data: Clone + Serialize + for<'de> Deserialize<'de> + Debug;

    /// Event type (triggers state transitions)
    type Event: Serialize + for<'de> Deserialize<'de> + Debug;

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

/// State machine handle for client interactions
#[derive(Debug, Clone)]
pub struct GenStatemHandle {
    pub process_id: u64,
}

impl GenStatemHandle {
    pub fn new(process_id: u64) -> Self {
        Self { process_id }
    }

    pub fn id(&self) -> u64 {
        self.process_id
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
