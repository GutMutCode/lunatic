use std::{collections::HashMap, sync::Arc, time::Instant};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lunatic_process::env::LunaticEnvironment;
use lunatic_process::instance_pool::InstancePool;
use lunatic_process::runtimes::wasmtime::{default_config, WasmtimeRuntime};
use lunatic_runtime::{state::DefaultProcessState, DefaultProcessConfig};
use tokio::sync::RwLock;

const INSTANCE_POOL_WAT: &str =
    r#"(module (memory (export "memory") 1) (func (export "hello") nop))"#;

fn bench_instance_pool_acquire_release(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let config = Arc::new(DefaultProcessConfig::default());
    let wasmtime_config = default_config();
    let runtime = rt.block_on(async { WasmtimeRuntime::new(&wasmtime_config).unwrap() });

    let raw_module = wat::parse_str(INSTANCE_POOL_WAT).unwrap();
    let module = Arc::new(
        runtime
            .compile_module::<DefaultProcessState>(raw_module.into())
            .unwrap(),
    );

    let pool = InstancePool::new(module.clone(), runtime.clone(), 100);
    let env = Arc::new(LunaticEnvironment::new(0));

    rt.block_on(async {
        for _ in 0..100 {
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
            let instance = pool.acquire(state).await.unwrap();
            pool.release(instance);
        }
    });

    c.bench_function("instance_pool_hot_path", |b| {
        b.to_async(&rt).iter(|| async {
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
            let instance = pool.acquire(state).await.unwrap();
            black_box(&instance);
            pool.release(instance);
        });
    });
}

fn bench_instance_pool_cold_path(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let config = Arc::new(DefaultProcessConfig::default());
    let wasmtime_config = default_config();
    let runtime = rt.block_on(async { WasmtimeRuntime::new(&wasmtime_config).unwrap() });

    let raw_module = wat::parse_str(INSTANCE_POOL_WAT).unwrap();
    let module = Arc::new(
        runtime
            .compile_module::<DefaultProcessState>(raw_module.into())
            .unwrap(),
    );

    let pool = InstancePool::new(module.clone(), runtime.clone(), 0);
    let env = Arc::new(LunaticEnvironment::new(0));

    c.bench_function("instance_pool_cold_path", |b| {
        b.to_async(&rt).iter(|| async {
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
            let instance = pool.acquire(state).await.unwrap();
            black_box(&instance);
            pool.release(instance);
        });
    });
}

fn bench_instance_pool_hit_rate(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let config = Arc::new(DefaultProcessConfig::default());
    let wasmtime_config = default_config();
    let runtime = rt.block_on(async { WasmtimeRuntime::new(&wasmtime_config).unwrap() });
    let raw_module = wat::parse_str(INSTANCE_POOL_WAT).unwrap();
    let module = Arc::new(
        runtime
            .compile_module::<DefaultProcessState>(raw_module.into())
            .unwrap(),
    );
    let pool = InstancePool::new(module.clone(), runtime.clone(), 64);
    let env = Arc::new(LunaticEnvironment::new(0));

    // Warm up to seed the pool with one instance
    rt.block_on(async {
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
        let instance = pool.acquire(state).await.unwrap();
        pool.release(instance);
    });

    c.bench_function("instance_pool_hit_rate", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let before = pool.stats();
            rt.block_on(async {
                for _ in 0..iters {
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
                    let instance = pool.acquire(state).await.unwrap();
                    pool.release(instance);
                }
            });
            let after = pool.stats();
            let hits_delta = after.hits.saturating_sub(before.hits);
            let miss_delta = after.misses.saturating_sub(before.misses);
            assert!(
                hits_delta >= iters.saturating_sub(miss_delta),
                "expected pooled hits to dominate: hits_delta={}, miss_delta={}, iters={}",
                hits_delta,
                miss_delta,
                iters
            );
            start.elapsed()
        })
    });
}

criterion_group!(
    benches,
    bench_instance_pool_acquire_release,
    bench_instance_pool_cold_path,
    bench_instance_pool_hit_rate,
);
criterion_main!(benches);
