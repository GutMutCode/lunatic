use std::{collections::HashMap, sync::Arc, time::Instant};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lunatic_process::{
    env::LunaticEnvironment,
    module_registry::ModuleRegistry,
    runtimes::wasmtime::{default_config, WasmtimeRuntime},
};
use lunatic_runtime::{state::DefaultProcessState, DefaultProcessConfig};
use tokio::sync::RwLock;

fn bench_hot_reload_end_to_end(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("hot_reload_module_compilation", |b| {
        b.iter(|| {
            let wasmtime_config = default_config();
            let runtime = rt.block_on(async { WasmtimeRuntime::new(&wasmtime_config).unwrap() });

            let start = Instant::now();
            let raw_module = wat::parse_file("./examples/counter_v1.wat").unwrap();
            let _module = runtime
                .compile_module::<DefaultProcessState>(raw_module.into())
                .unwrap();
            let compile_time = start.elapsed();

            black_box(compile_time)
        });
    });
}

fn bench_hot_reload_registry_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let wasmtime_config = default_config();
    let runtime = rt.block_on(async { WasmtimeRuntime::new(&wasmtime_config).unwrap() });

    c.bench_function("hot_reload_registry_add_version", |b| {
        b.iter(|| {
            let registry = ModuleRegistry::<DefaultProcessState>::new();
            let raw_module = wat::parse_file("./examples/counter_v1.wat").unwrap();
            let module = runtime
                .compile_module::<DefaultProcessState>(raw_module.into())
                .unwrap();

            let start = Instant::now();
            let version = registry.add_version(1, module);
            let registry_time = start.elapsed();

            black_box((version, registry_time))
        });
    });

    c.bench_function("hot_reload_registry_get_latest", |b| {
        let registry = ModuleRegistry::<DefaultProcessState>::new();
        let raw_module = wat::parse_file("./examples/counter_v1.wat").unwrap();
        let module = runtime
            .compile_module::<DefaultProcessState>(raw_module.into())
            .unwrap();
        registry.add_version(1, module);

        b.iter(|| {
            let start = Instant::now();
            let _module = registry.get_latest(1);
            let get_time = start.elapsed();
            black_box(get_time)
        });
    });
}

fn bench_hot_reload_snapshot_restore(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let wasmtime_config = default_config();
    let runtime = rt.block_on(async { WasmtimeRuntime::new(&wasmtime_config).unwrap() });

    c.bench_function("hot_reload_memory_snapshot", |b| {
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
                registry.clone(),
            )
            .unwrap();

            let mut instance = runtime.instantiate(&module, state).await.unwrap();

            let start = Instant::now();
            let snapshot = instance.snapshot_memory().unwrap();
            let snapshot_time = start.elapsed();

            black_box((snapshot, snapshot_time))
        });
    });

    c.bench_function("hot_reload_memory_restore", |b| {
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
                registry.clone(),
            )
            .unwrap();

            let mut instance1 = runtime.instantiate(&module, state).await.unwrap();
            let snapshot = instance1.snapshot_memory().unwrap();

            let state2 = DefaultProcessState::new(
                env.clone(),
                None,
                runtime.clone(),
                Arc::new(module.clone()),
                config.clone(),
                registry.clone(),
            )
            .unwrap();

            let mut instance2 = runtime.instantiate(&module, state2).await.unwrap();

            let start = Instant::now();
            instance2.restore_memory(&snapshot).unwrap();
            let restore_time = start.elapsed();

            black_box(restore_time)
        });
    });
}

fn bench_hot_reload_full_cycle(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let wasmtime_config = default_config();
    let runtime = rt.block_on(async { WasmtimeRuntime::new(&wasmtime_config).unwrap() });

    c.bench_function("hot_reload_FULL_CYCLE", |b| {
        b.to_async(&rt).iter(|| async {
            let total_start = Instant::now();

            // 1. Compile old version
            let raw_v1 = wat::parse_file("./examples/counter_v1.wat").unwrap();
            let module_v1 = runtime
                .compile_module::<DefaultProcessState>(raw_v1.into())
                .unwrap();

            // 2. Create registry and add v1
            let registry = ModuleRegistry::<DefaultProcessState>::new();
            let v1 = registry.add_version(1, module_v1.clone());

            // 3. Create instance with v1
            let config = Arc::new(DefaultProcessConfig::default());
            let env = Arc::new(LunaticEnvironment::new(0));
            let state_registry = Arc::new(RwLock::new(HashMap::new()));

            let state_v1 = DefaultProcessState::new(
                env.clone(),
                None,
                runtime.clone(),
                Arc::new(module_v1.clone()),
                config.clone(),
                state_registry.clone(),
            )
            .unwrap();

            let mut instance_v1 = runtime.instantiate(&module_v1, state_v1).await.unwrap();

            // 4. Snapshot state
            let snapshot = instance_v1.snapshot_memory().unwrap();

            // 5. Compile new version
            let raw_v2 = wat::parse_file("./examples/counter_v2.wat").unwrap();
            let module_v2 = runtime
                .compile_module::<DefaultProcessState>(raw_v2.into())
                .unwrap();

            // 6. Add v2 to registry
            let v2 = registry.add_version(1, module_v2.clone());

            // 7. Create new instance with v2
            let state_v2 = DefaultProcessState::new(
                env.clone(),
                None,
                runtime.clone(),
                Arc::new(module_v2.clone()),
                config.clone(),
                state_registry.clone(),
            )
            .unwrap();

            let mut instance_v2 = runtime.instantiate(&module_v2, state_v2).await.unwrap();

            // 8. Restore state to v2
            instance_v2.restore_memory(&snapshot).unwrap();

            let total_time = total_start.elapsed();

            black_box((v1, v2, total_time))
        });
    });
}

criterion_group!(
    benches,
    bench_hot_reload_end_to_end,
    bench_hot_reload_registry_operations,
    bench_hot_reload_snapshot_restore,
    bench_hot_reload_full_cycle
);
criterion_main!(benches);
