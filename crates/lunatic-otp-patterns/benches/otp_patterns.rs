//! Benchmarks for OTP patterns (GenServer, Supervisor)
//!
//! These benchmarks measure the performance of OTP pattern operations
//! to ensure they meet the fast performance requirements.

use anyhow::Result;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lunatic_otp_patterns::{
    ChildSpec, GenServer, RestartPolicy, RestartStrategy, ShutdownPolicy, Supervisor,
    SupervisorSpec,
};
use serde::{Deserialize, Serialize};

// Test GenServer implementation for benchmarking
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchCounter {
    count: i64,
}

#[derive(Debug, Serialize, Deserialize)]
enum BenchRequest {
    Increment,
    Get,
    Set(i64),
}

#[derive(Debug, Serialize, Deserialize)]
enum BenchResponse {
    Ok,
    Value(i64),
}

impl GenServer for BenchCounter {
    type State = Self;
    type Call = BenchRequest;
    type CallReply = BenchResponse;
    type Cast = BenchRequest;

    fn init() -> Self::State {
        BenchCounter { count: 0 }
    }

    fn handle_call(&mut self, request: Self::Call) -> Result<Self::CallReply> {
        match request {
            BenchRequest::Increment => {
                self.count += 1;
                Ok(BenchResponse::Ok)
            }
            BenchRequest::Get => Ok(BenchResponse::Value(self.count)),
            BenchRequest::Set(value) => {
                self.count = value;
                Ok(BenchResponse::Ok)
            }
        }
    }

    fn handle_cast(&mut self, request: Self::Cast) -> Result<()> {
        match request {
            BenchRequest::Increment => {
                self.count += 1;
                Ok(())
            }
            BenchRequest::Get => Ok(()), // Cast ignores response
            BenchRequest::Set(value) => {
                self.count = value;
                Ok(())
            }
        }
    }
}

fn bench_otp_patterns(c: &mut Criterion) {
    // GenServer call benchmark
    let mut counter = BenchCounter::init();
    c.bench_function("gen_server_call_increment", |b| {
        b.iter(|| {
            black_box(counter.handle_call(BenchRequest::Increment).unwrap());
        })
    });

    c.bench_function("gen_server_call_get", |b| {
        b.iter(|| {
            black_box(counter.handle_call(BenchRequest::Get).unwrap());
        })
    });

    // GenServer cast benchmark
    let mut counter2 = BenchCounter::init();
    c.bench_function("gen_server_cast_increment", |b| {
        b.iter(|| {
            counter2
                .handle_cast(black_box(BenchRequest::Increment))
                .unwrap();
        })
    });

    // Supervisor benchmark
    let spec = SupervisorSpec {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 3,
        max_seconds: 5,
        children: vec![ChildSpec {
            id: "child1".to_string(),
            start: |_| Err("not started in creation benchmark".to_string()),
            restart: RestartPolicy::Permanent,
            shutdown: ShutdownPolicy::Brutal,
            child_type: Default::default(),
        }],
    };

    c.bench_function("supervisor_creation", |b| {
        b.iter(|| {
            black_box(Supervisor::new(spec.clone()));
        })
    });

    // Message serialization benchmark
    let request = BenchRequest::Set(42);
    c.bench_function("serialize_request", |b| {
        b.iter(|| {
            black_box(serde_json::to_string(&request).unwrap());
        })
    });
}

criterion_group!(benches, bench_otp_patterns);
criterion_main!(benches);
