//! GenServer Example using lunatic-otp-patterns
//!
//! This example demonstrates how to implement a simple counter server
//! using the GenServer pattern from Erlang/OTP.

use lunatic_otp_patterns::{GenServer, GenServerConfig};
use serde::{Deserialize, Serialize};

// Define message types
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Counter {
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

// Implement GenServer for Counter
impl GenServer for Counter {
    type State = Self;
    type Call = CounterRequest;
    type CallReply = CounterResponse;
    type Cast = CounterRequest;

    fn init() -> Self::State {
        Counter { count: 0 }
    }

    fn handle_call(&mut self, request: Self::Call) -> Self::CallReply {
        match request {
            CounterRequest::Increment => {
                self.count += 1;
                CounterResponse::Ok
            }
            CounterRequest::Decrement => {
                self.count -= 1;
                CounterResponse::Ok
            }
            CounterRequest::Get => CounterResponse::Value(self.count),
            CounterRequest::Set(value) => {
                self.count = value;
                CounterResponse::Ok
            }
        }
    }

    fn handle_cast(&mut self, request: Self::Cast) {
        match request {
            CounterRequest::Increment => self.count += 1,
            CounterRequest::Decrement => self.count -= 1,
            CounterRequest::Get => {} // Cast ignores response
            CounterRequest::Set(value) => self.count = value,
        }
    }
}

// Example usage (would be called from _start in real WASM module)
pub fn example_usage() {
    // Note: In real WASM code, this would spawn actual processes
    // For now, this is just a demonstration of the API

    // Create a counter instance for testing
    let mut counter = Counter::init();
    assert_eq!(counter.count, 0);

    // Test handle_call
    let response = counter.handle_call(CounterRequest::Increment);
    assert!(matches!(response, CounterResponse::Ok));
    assert_eq!(counter.count, 1);

    let response = counter.handle_call(CounterRequest::Get);
    match response {
        CounterResponse::Value(v) => assert_eq!(v, 1),
        _ => panic!("Expected Value"),
    }

    // Test handle_cast
    counter.handle_cast(CounterRequest::Increment);
    assert_eq!(counter.count, 2);

    counter.handle_cast(CounterRequest::Set(42));
    assert_eq!(counter.count, 42);

    println!("GenServer example completed successfully!");
}

// Example of how this would be used in real WASM code (pseudo-code)
/*
#[no_mangle]
pub extern "C" fn _start() {
    // Spawn a GenServer process
    let config = GenServerConfig::default();
    let handle = Counter::spawn(config).expect("Failed to spawn GenServer");

    // Send some messages
    handle.call(CounterRequest::Increment).unwrap();
    let CounterResponse::Value(count) = handle.call(CounterRequest::Get).unwrap();
    println!("Counter value: {}", count);

    handle.cast(CounterRequest::Set(100)).unwrap();
}
*/

#[no_mangle]
pub extern "C" fn run_gen_server_example() {
    example_usage();
}

fn main() {
    // Required for compilation but not called in WASM
}