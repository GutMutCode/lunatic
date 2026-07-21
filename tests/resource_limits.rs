use lunatic_runtime::state::DefaultProcessState;
use lunatic_runtime::DefaultProcessConfig;

const UDP_ACCOUNTING_GUEST: &str = r#"
(module
    (import "lunatic::networking" "udp_bind"
        (func $udp_bind (param i32 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::networking" "clone_udp_socket"
        (func $clone_udp_socket (param i64) (result i64)))
    (import "lunatic::networking" "drop_udp_socket"
        (func $drop_udp_socket (param i64)))
    (memory (export "memory") 1)
    (data (i32.const 0) "\7f\00\00\01")

    (func (export "clone_drop_stress") (param $iterations i32)
        (local $socket i64)
        (local $clone i64)
        (local $index i32)
        (if (call $udp_bind
                (i32.const 4) (i32.const 0) (i32.const 0)
                (i32.const 0) (i32.const 0) (i32.const 16))
            (then unreachable))
        (local.set $socket (i64.load (i32.const 16)))
        (loop $repeat
            (if (i32.lt_u (local.get $index) (local.get $iterations))
                (then
                    (local.set $clone (call $clone_udp_socket (local.get $socket)))
                    (call $drop_udp_socket (local.get $clone))
                    (local.set $index (i32.add (local.get $index) (i32.const 1)))
                    (br $repeat))))
        (call $drop_udp_socket (local.get $socket)))

    (func (export "overflow")
        (local $socket i64)
        (if (call $udp_bind
                (i32.const 4) (i32.const 0) (i32.const 0)
                (i32.const 0) (i32.const 0) (i32.const 16))
            (then unreachable))
        (local.set $socket (i64.load (i32.const 16)))
        (drop (call $clone_udp_socket (local.get $socket)))
        (drop (call $clone_udp_socket (local.get $socket))))

    (func (export "cleanup_after_overflow")
        (call $drop_udp_socket (i64.const 0))
        (call $drop_udp_socket (i64.const 1)))
)
"#;

#[test]
fn test_default_resource_limits() {
    let config = DefaultProcessConfig::default();

    assert_eq!(config.get_max_table_elements(), 100_000);
    assert_eq!(config.get_max_file_descriptors(), 1024);
    assert_eq!(config.get_max_network_connections(), 1024);
}

#[test]
fn test_custom_resource_limits() {
    let mut config = DefaultProcessConfig::default();

    config.set_max_table_elements(50_000);
    config.set_max_file_descriptors(512);
    config.set_max_network_connections(256);

    assert_eq!(config.get_max_table_elements(), 50_000);
    assert_eq!(config.get_max_file_descriptors(), 512);
    assert_eq!(config.get_max_network_connections(), 256);
}

#[tokio::test]
async fn test_table_growing_limit() {
    use lunatic_process::runtimes::wasmtime::WasmtimeRuntime;
    use lunatic_runtime::state::DefaultProcessState;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use wasmtime::ResourceLimiter;

    let mut config = DefaultProcessConfig::default();
    config.set_max_table_elements(1000);

    let mut wasmtime_config = wasmtime::Config::new();
    wasmtime_config.consume_fuel(true);
    let runtime = WasmtimeRuntime::new(&wasmtime_config).unwrap();

    let env = Arc::new(lunatic_process::env::LunaticEnvironment::new(0));

    let raw_module = wat::parse_str(
        r#"
        (module)
    "#,
    )
    .unwrap();

    let module = Arc::new(runtime.compile_module(raw_module.into()).unwrap());
    let registry = Arc::new(RwLock::new(HashMap::new()));

    let mut state =
        DefaultProcessState::new(env, None, runtime, module, Arc::new(config), registry).unwrap();

    assert!(
        state.table_growing(0, 999, None).unwrap(),
        "Should allow 999 elements"
    );
    assert!(
        state.table_growing(0, 1000, None).unwrap(),
        "Should allow exactly 1000 elements"
    );
    assert!(
        !state.table_growing(0, 1001, None).unwrap(),
        "Should reject 1001 elements"
    );
}

#[tokio::test]
async fn test_network_connection_limit() {
    use lunatic_process::runtimes::wasmtime::WasmtimeRuntime;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    let mut config = DefaultProcessConfig::default();
    config.set_max_network_connections(3);

    let mut wasmtime_config = wasmtime::Config::new();
    wasmtime_config.consume_fuel(true);
    let runtime = WasmtimeRuntime::new(&wasmtime_config).unwrap();

    let env = Arc::new(lunatic_process::env::LunaticEnvironment::new(0));

    let raw_module = wat::parse_str(
        r#"
        (module)
    "#,
    )
    .unwrap();

    let module = Arc::new(runtime.compile_module(raw_module.into()).unwrap());
    let registry = Arc::new(RwLock::new(HashMap::new()));

    let mut state =
        DefaultProcessState::new(env, None, runtime, module, Arc::new(config), registry).unwrap();

    assert!(
        state.can_open_network_connection().is_ok(),
        "Should allow connection 1"
    );
    assert!(
        state.can_open_network_connection().is_ok(),
        "Should allow connection 2"
    );
    assert!(
        state.can_open_network_connection().is_ok(),
        "Should allow connection 3"
    );
    assert!(
        state.can_open_network_connection().is_err(),
        "Should reject connection 4 (limit reached)"
    );

    state.close_network_connection();
    assert!(
        state.can_open_network_connection().is_ok(),
        "Should allow connection after closing one"
    );
}

#[tokio::test]
async fn paired_network_limits_are_atomic_and_recover_without_underflow() {
    use lunatic_process::runtimes::wasmtime::WasmtimeRuntime;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    let mut config = DefaultProcessConfig::default();
    config.set_max_file_descriptors(2);
    config.set_max_network_connections(3);
    let runtime =
        WasmtimeRuntime::new(&lunatic_process::runtimes::wasmtime::default_config()).unwrap();
    let module = Arc::new(
        runtime
            .compile_module(wat::parse_str("(module)").unwrap().into())
            .unwrap(),
    );
    let mut state = DefaultProcessState::new(
        Arc::new(lunatic_process::env::LunaticEnvironment::new(0)),
        None,
        runtime,
        module,
        Arc::new(config),
        Arc::new(RwLock::new(HashMap::new())),
    )
    .unwrap();

    state.reserve_network_handle().unwrap();
    state.reserve_network_handle().unwrap();
    assert_eq!(state.network_resource_counts(), (2, 2));
    assert!(state.reserve_network_handle().is_err());
    assert_eq!(state.network_resource_counts(), (2, 2));

    state.release_network_handle().unwrap();
    state.release_network_handle().unwrap();
    assert_eq!(state.network_resource_counts(), (0, 0));
    assert!(state.release_network_handle().is_err());
    assert_eq!(state.network_resource_counts(), (0, 0));
}

#[tokio::test]
async fn network_ceiling_can_fail_first_without_consuming_a_descriptor() {
    use lunatic_process::runtimes::wasmtime::{default_config, WasmtimeRuntime};
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::RwLock;

    let mut config = DefaultProcessConfig::default();
    config.set_max_file_descriptors(3);
    config.set_max_network_connections(2);
    let runtime = WasmtimeRuntime::new(&default_config()).unwrap();
    let module = Arc::new(
        runtime
            .compile_module(wat::parse_str("(module)").unwrap().into())
            .unwrap(),
    );
    let mut state = DefaultProcessState::new(
        Arc::new(lunatic_process::env::LunaticEnvironment::new(0)),
        None,
        runtime,
        module,
        Arc::new(config),
        Arc::new(RwLock::new(HashMap::new())),
    )
    .unwrap();

    state.reserve_network_handle().unwrap();
    state.reserve_network_handle().unwrap();
    assert_eq!(state.network_resource_counts(), (2, 2));
    assert!(state.reserve_network_handle().is_err());
    assert_eq!(state.network_resource_counts(), (2, 2));

    state.release_network_handle().unwrap();
    state.release_network_handle().unwrap();
    assert_eq!(state.network_resource_counts(), (0, 0));
}

#[tokio::test]
async fn udp_clone_drop_stress_and_quota_failure_keep_counters_exact() {
    use lunatic_networking_api::NetworkingCtx;
    use lunatic_process::{
        env::LunaticEnvironment,
        runtimes::wasmtime::{default_config, WasmtimeRuntime},
    };
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::RwLock;
    use wasmtime::Val;

    async fn instance(
    ) -> anyhow::Result<lunatic_process::runtimes::wasmtime::WasmtimeInstance<DefaultProcessState>>
    {
        let mut config = DefaultProcessConfig::default();
        config.set_max_file_descriptors(2);
        config.set_max_network_connections(2);
        let runtime = WasmtimeRuntime::new(&default_config())?;
        let module =
            Arc::new(runtime.compile_module(wat::parse_str(UDP_ACCOUNTING_GUEST)?.into())?);
        let state = DefaultProcessState::new(
            Arc::new(LunaticEnvironment::new(0)),
            None,
            runtime.clone(),
            module.clone(),
            Arc::new(config),
            Arc::new(RwLock::new(HashMap::new())),
        )?;
        runtime.instantiate(&module, state).await
    }

    let mut stress = instance().await.unwrap();
    stress
        .call_ref("clone_drop_stress", vec![Val::I32(5_000)])
        .await
        .unwrap();
    assert_eq!(stress.state().network_resource_counts(), (0, 0));
    assert!(stress.state().udp_resources().is_empty());

    let mut overflow = instance().await.unwrap();
    assert!(overflow.call_ref("overflow", Vec::new()).await.is_err());
    assert_eq!(overflow.state().network_resource_counts(), (2, 2));
    assert_eq!(overflow.state().udp_resources().len(), 2);
    overflow
        .call_ref("cleanup_after_overflow", Vec::new())
        .await
        .unwrap();
    assert_eq!(overflow.state().network_resource_counts(), (0, 0));
    assert!(overflow.state().udp_resources().is_empty());
}
