use std::{collections::HashMap, sync::Arc};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use lunatic_process::{
    env::LunaticEnvironment,
    mailbox::MessageMailbox,
    message::Message,
    runtimes::wasmtime::{default_config, WasmtimeRuntime},
};
use lunatic_runtime::{state::DefaultProcessState, DefaultProcessConfig};
use tokio::sync::RwLock;

fn bench_memory_baseline(c: &mut Criterion) {
    c.bench_function("memory_empty_mailbox_size", |b| {
        b.iter(|| {
            let mailbox = MessageMailbox::default();
            let size = std::mem::size_of_val(&mailbox);
            black_box(size)
        });
    });

    c.bench_function("memory_process_state_size", |b| {
        b.iter(|| {
            let size = std::mem::size_of::<DefaultProcessState>();
            black_box(size)
        });
    });

    c.bench_function("memory_message_size", |b| {
        b.iter(|| {
            let msg = Message::LinkDied(Some(42));
            let size = std::mem::size_of_val(&msg);
            black_box(size)
        });
    });
}

fn bench_memory_mailbox_growth(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_mailbox_growth");
    let rt = tokio::runtime::Runtime::new().unwrap();

    for count in [10, 100, 1000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_messages", count)),
            count,
            |b, &n| {
                b.to_async(&rt).iter(|| async {
                    let mailbox = MessageMailbox::new(n as usize);

                    // Fill mailbox
                    for i in 0..n {
                        mailbox.push(Message::LinkDied(Some(i))).unwrap();
                    }

                    // Measure mailbox state
                    let msg_count = mailbox.len();
                    let base_size = std::mem::size_of_val(&mailbox);

                    // Estimate total size (base + messages)
                    let estimated_total = base_size + (msg_count * std::mem::size_of::<Message>());

                    black_box((msg_count, base_size, estimated_total))
                });
            },
        );
    }

    group.finish();
}

fn bench_memory_process_overhead(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let config = Arc::new(DefaultProcessConfig::default());
    let wasmtime_config = default_config();
    let runtime = rt.block_on(async { WasmtimeRuntime::new(&wasmtime_config).unwrap() });

    c.bench_function("memory_process_minimal_overhead", |b| {
        b.to_async(&rt).iter(|| async {
            let raw_module = wat::parse_file("./wat/hello.wat").unwrap();
            let module = Arc::new(
                runtime
                    .compile_module::<DefaultProcessState>(raw_module.into())
                    .unwrap(),
            );

            let env = Arc::new(LunaticEnvironment::new(0));
            let registry = Arc::new(RwLock::new(HashMap::new()));

            let state = DefaultProcessState::new(
                env.clone(),
                None,
                runtime.clone(),
                module.clone(),
                config.clone(),
                registry,
            )
            .unwrap();

            // Measure various components
            let state_size = std::mem::size_of_val(&state);
            let module_size = std::mem::size_of_val(&*module);
            let config_size = std::mem::size_of_val(&*config);

            black_box((state_size, module_size, config_size))
        });
    });
}

fn bench_memory_wasm_instance(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let wasmtime_config = default_config();
    let runtime = rt.block_on(async { WasmtimeRuntime::new(&wasmtime_config).unwrap() });

    c.bench_function("memory_wasm_instance_creation", |b| {
        b.to_async(&rt).iter(|| async {
            let raw_module = wat::parse_file("./examples/counter_v1.wat").unwrap();
            let module = runtime
                .compile_module::<DefaultProcessState>(raw_module.into())
                .unwrap();

            let config = Arc::new(DefaultProcessConfig::default());
            let env = Arc::new(LunaticEnvironment::new(0));
            let registry = Arc::new(RwLock::new(HashMap::new()));

            let state = DefaultProcessState::new(
                env.clone(),
                None,
                runtime.clone(),
                Arc::new(module.clone()),
                config.clone(),
                registry,
            )
            .unwrap();

            let mut instance = runtime.instantiate(&module, state).await.unwrap();

            // Get memory snapshot to measure actual memory usage
            let snapshot = instance.snapshot_memory().unwrap();
            let memory_bytes = snapshot.memory.len();

            black_box(memory_bytes)
        });
    });
}

fn bench_memory_scalability(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_concurrent_processes");
    group.sample_size(10);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let config = Arc::new(DefaultProcessConfig::default());
    let wasmtime_config = default_config();
    let runtime = rt.block_on(async { WasmtimeRuntime::new(&wasmtime_config).unwrap() });

    let raw_module = wat::parse_file("./wat/hello.wat").unwrap();
    let module = Arc::new(
        runtime
            .compile_module::<DefaultProcessState>(raw_module.into())
            .unwrap(),
    );

    for count in [10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_processes", count)),
            count,
            |b, &n| {
                b.to_async(&rt).iter(|| async {
                    let env = Arc::new(LunaticEnvironment::new(0));
                    let mut states = Vec::new();

                    for _ in 0..n {
                        let registry = Arc::new(RwLock::new(HashMap::new()));
                        let state = DefaultProcessState::new(
                            env.clone(),
                            None,
                            runtime.clone(),
                            module.clone(),
                            config.clone(),
                            registry,
                        )
                        .unwrap();

                        states.push(state);
                    }

                    // Estimate total memory
                    let state_size = std::mem::size_of::<DefaultProcessState>();
                    let total_estimated = state_size * states.len();

                    black_box((states.len(), total_estimated))
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_memory_baseline,
    bench_memory_mailbox_growth,
    bench_memory_process_overhead,
    bench_memory_wasm_instance,
    bench_memory_scalability
);
criterion_main!(benches);
