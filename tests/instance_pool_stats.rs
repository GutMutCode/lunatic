use std::collections::HashMap;
use std::sync::Arc;

use lunatic_process::env::LunaticEnvironment;
use lunatic_process::instance_pool::InstancePool;
use lunatic_process::runtimes::wasmtime::{default_config, WasmtimeRuntime};
use lunatic_runtime::state::DefaultProcessState;
use lunatic_runtime::DefaultProcessConfig;
use tokio::sync::RwLock;

fn test_module_source() -> Vec<u8> {
    wat::parse_str(
        r#"
        (module
            (memory 1)
            (export "memory" (memory 0))
            (func (export "main"))
        )
        "#,
    )
    .expect("valid wat")
}

#[tokio::test]
async fn instance_pool_reports_hit_rate() {
    let mut wasmtime_config = default_config();
    wasmtime_config.async_support(true).consume_fuel(true);
    let runtime = WasmtimeRuntime::new(&wasmtime_config).expect("runtime");

    let module_source = test_module_source();
    let module = Arc::new(
        runtime
            .compile_module::<DefaultProcessState>(module_source.into())
            .expect("compile module"),
    );

    let pool = InstancePool::new(module.clone(), runtime.clone(), 8);
    let env = Arc::new(LunaticEnvironment::new(0));
    let config = Arc::new(DefaultProcessConfig::default());

    // First acquire -> miss and create instance
    let registry = Arc::new(RwLock::new(HashMap::new()));
    let state = DefaultProcessState::new(
        env.clone(),
        None,
        runtime.clone(),
        module.clone(),
        config.clone(),
        registry,
    )
    .expect("state");

    let instance = pool.acquire(state).await.expect("acquire first");
    pool.release(instance);

    // Second acquire should hit the pool
    let registry = Arc::new(RwLock::new(HashMap::new()));
    let state = DefaultProcessState::new(
        env.clone(),
        None,
        runtime.clone(),
        module.clone(),
        config.clone(),
        registry,
    )
    .expect("state2");

    let instance = pool.acquire(state).await.expect("acquire second");
    pool.release(instance);

    let stats = pool.stats();
    assert!(
        stats.hits >= 1,
        "expected at least one pool hit, stats={:?}",
        stats
    );
    assert_eq!(
        stats.misses, 1,
        "exactly one miss expected for first acquire"
    );
    assert!(
        stats.pool_size >= 1,
        "instance should remain pooled after release"
    );
}
