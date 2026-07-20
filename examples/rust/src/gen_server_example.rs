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
            CounterRequest::Increment => self.count += 1,
            CounterRequest::Decrement => self.count -= 1,
            CounterRequest::Get => {} // Cast ignores response
            CounterRequest::Set(value) => self.count = value,
        }
        Ok(())
    }
}

pub fn example_usage() -> anyhow::Result<()> {
    let config = GenServerConfig::default();
    let handle = Counter::spawn(config)?;

    handle.cast(CounterRequest::Increment)?;
    handle.cast(CounterRequest::Set(41))?;
    handle.cast(CounterRequest::Increment)?;

    match handle.call(CounterRequest::Get)? {
        CounterResponse::Value(count) => println!("Counter value: {count}"),
        CounterResponse::Ok => unreachable!("Get always returns a value"),
    }

    handle.stop(lunatic_otp_patterns::TerminateReason::Normal)?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    example_usage()
}
