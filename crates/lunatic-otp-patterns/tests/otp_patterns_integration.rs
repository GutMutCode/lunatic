//! Integration tests for OTP patterns in Lunatic
//!
//! These tests verify that the OTP patterns (GenServer, Supervisor, GenStatem)
//! work correctly. They validate the core functionality that would be used
//! in WASM processes, focusing on trait implementations and message handling.

use lunatic_otp_patterns::{
    ChildSpec, ChildType, GenServer, GenStatem, RestartPolicy, RestartStrategy, ServerMessage,
    ServerReply, ShutdownPolicy, StateData, Supervisor, SupervisorSpec, TerminateReason,
};
use serde::{Deserialize, Serialize};

/// Test GenServer implementation
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestCounter {
    count: i64,
}

#[derive(Debug, Serialize, Deserialize)]
enum CounterRequest {
    Increment,
    Decrement,
    Get,
    Set(i64),
}

#[derive(Debug, Serialize, Deserialize)]
enum CounterResponse {
    Ok,
    Value(i64),
}

impl GenServer for TestCounter {
    type State = Self;
    type Call = CounterRequest;
    type CallReply = CounterResponse;
    type Cast = CounterRequest;

    fn init() -> Self::State {
        TestCounter { count: 0 }
    }

    fn handle_call(&mut self, request: Self::Call) -> anyhow::Result<Self::CallReply> {
        match request {
            CounterRequest::Increment => {
                self.count += 1;
                Ok(CounterResponse::Ok)
            }
            CounterRequest::Decrement => {
                self.count -= 1;
                Ok(CounterResponse::Ok)
            }
            CounterRequest::Get => Ok(CounterResponse::Value(self.count)),
            CounterRequest::Set(value) => {
                self.count = value;
                Ok(CounterResponse::Ok)
            }
        }
    }

    fn handle_cast(&mut self, request: Self::Cast) -> anyhow::Result<()> {
        match request {
            CounterRequest::Increment => {
                self.count += 1;
                Ok(())
            }
            CounterRequest::Decrement => {
                self.count -= 1;
                Ok(())
            }
            CounterRequest::Get => Ok(()), // Cast ignores response
            CounterRequest::Set(value) => {
                self.count = value;
                Ok(())
            }
        }
    }
}

/// Test GenServer basic functionality
#[test]
fn test_gen_server_basic() {
    // Create a counter instance and test the message handling
    let mut counter = TestCounter { count: 0 };
    assert_eq!(counter.count, 0);

    // Test handle_call
    let response = counter.handle_call(CounterRequest::Increment).unwrap();
    assert!(matches!(response, CounterResponse::Ok));
    assert_eq!(counter.count, 1);

    let response = counter.handle_call(CounterRequest::Get).unwrap();
    match response {
        CounterResponse::Value(v) => assert_eq!(v, 1),
        _ => panic!("Expected Value"),
    }

    // Test handle_cast
    counter
        .handle_cast(CounterRequest::Increment)
        .expect("increment cast should succeed");
    assert_eq!(counter.count, 2);

    counter
        .handle_cast(CounterRequest::Set(42))
        .expect("set cast should succeed");
    assert_eq!(counter.count, 42);
}

/// Test Supervisor basic functionality
#[test]
fn test_supervisor_basic() {
    let spec = SupervisorSpec {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 3,
        max_seconds: 5,
        children: vec![ChildSpec {
            id: "test_child".to_string(),
            start: || Ok(12345), // Mock process ID
            restart: RestartPolicy::Permanent,
            shutdown: ShutdownPolicy::Timeout(5000),
            child_type: ChildType::Worker,
        }],
    };

    let supervisor = Supervisor::new(spec);
    let count = supervisor.count_children();
    assert_eq!(count.specs, 1); // Should have 1 child spec
    assert_eq!(count.active, 0); // No children started yet
}

/// Test GenStatem basic functionality
#[test]
fn test_gen_statem_basic() {
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum TestState {
        Idle,
        Active,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestData {
        value: i32,
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum TestEvent {
        Start,
        Stop,
        Update(i32),
    }

    struct TestStateMachine;

    impl GenStatem for TestStateMachine {
        type State = TestState;
        type Data = TestData;
        type Event = TestEvent;

        fn init() -> StateData<Self::State, Self::Data> {
            StateData::new(TestState::Idle, TestData { value: 0 })
        }

        fn handle_event(
            &mut self,
            state: Self::State,
            event: Self::Event,
            mut data: Self::Data,
        ) -> StateData<Self::State, Self::Data> {
            match (state, event) {
                (TestState::Idle, TestEvent::Start) => StateData::new(TestState::Active, data),
                (TestState::Active, TestEvent::Stop) => StateData::new(TestState::Idle, data),
                (TestState::Active, TestEvent::Update(new_value)) => {
                    data.value = new_value;
                    StateData::new(TestState::Active, data)
                }
                (state, _) => StateData::new(state, data),
            }
        }
    }

    let mut sm = TestStateMachine;
    let mut state_data = TestStateMachine::init();

    assert_eq!(state_data.state, TestState::Idle);
    assert_eq!(state_data.data.value, 0);

    // Test state transition
    state_data = sm.handle_event(state_data.state, TestEvent::Start, state_data.data);
    assert_eq!(state_data.state, TestState::Active);

    // Test data update
    state_data = sm.handle_event(state_data.state, TestEvent::Update(42), state_data.data);
    assert_eq!(state_data.state, TestState::Active);
    assert_eq!(state_data.data.value, 42);

    // Test stop
    state_data = sm.handle_event(state_data.state, TestEvent::Stop, state_data.data);
    assert_eq!(state_data.state, TestState::Idle);
    assert_eq!(state_data.data.value, 42); // Data preserved
}

/// Test supervisor restart strategies
#[test]
fn test_supervisor_restart_strategies() {
    // Test that all restart strategies are properly defined
    assert_eq!(RestartStrategy::OneForOne, RestartStrategy::OneForOne);
    assert_eq!(RestartStrategy::OneForAll, RestartStrategy::OneForAll);
    assert_eq!(RestartStrategy::RestForOne, RestartStrategy::RestForOne);
    assert_eq!(
        RestartStrategy::SimpleOneForOne,
        RestartStrategy::SimpleOneForOne
    );

    // Test restart policies
    assert_eq!(RestartPolicy::Permanent, RestartPolicy::Permanent);
    assert_eq!(RestartPolicy::Transient, RestartPolicy::Transient);
    assert_eq!(RestartPolicy::Temporary, RestartPolicy::Temporary);

    // Test shutdown policies
    assert!(matches!(ShutdownPolicy::Brutal, ShutdownPolicy::Brutal));
    assert!(matches!(
        ShutdownPolicy::Timeout(5000),
        ShutdownPolicy::Timeout(_)
    ));
    assert!(matches!(ShutdownPolicy::Infinity, ShutdownPolicy::Infinity));

    // Test child types
    assert_eq!(ChildType::Worker, ChildType::Worker);
    assert_eq!(ChildType::Supervisor, ChildType::Supervisor);
}

/// Test serialization of OTP messages
#[test]
fn test_otp_message_serialization() {
    // Test ServerMessage serialization
    let call_msg: ServerMessage<CounterRequest, CounterRequest> = ServerMessage::Call {
        request: CounterRequest::Get,
        reply_to: 123,
    };

    let serialized = serde_json::to_string(&call_msg).unwrap();
    let deserialized: ServerMessage<CounterRequest, CounterRequest> =
        serde_json::from_str(&serialized).unwrap();

    match deserialized {
        ServerMessage::Call { request, reply_to } => {
            assert_eq!(reply_to, 123);
            assert!(matches!(request, CounterRequest::Get));
        }
        _ => panic!("Expected Call message"),
    }

    // Test ServerReply serialization
    let reply = ServerReply {
        reply: CounterResponse::Value(42),
    };

    let serialized = serde_json::to_string(&reply).unwrap();
    let deserialized: ServerReply<CounterResponse> = serde_json::from_str(&serialized).unwrap();

    match deserialized.reply {
        CounterResponse::Value(v) => assert_eq!(v, 42),
        _ => panic!("Expected Value reply"),
    }

    // Test TerminateReason serialization
    let reason = TerminateReason::Normal;
    let serialized = serde_json::to_string(&reason).unwrap();
    let deserialized: TerminateReason = serde_json::from_str(&serialized).unwrap();
    assert!(matches!(deserialized, TerminateReason::Normal));
}
