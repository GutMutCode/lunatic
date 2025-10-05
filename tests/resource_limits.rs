use lunatic_runtime::DefaultProcessConfig;
use lunatic_runtime::state::DefaultProcessState;
use lunatic_networking_api::NetworkingCtx;

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
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use lunatic_runtime::state::DefaultProcessState;
    use lunatic_process::runtimes::wasmtime::WasmtimeRuntime;
    use wasmtime::ResourceLimiter;

    let mut config = DefaultProcessConfig::default();
    config.set_max_table_elements(1000);

    let mut wasmtime_config = wasmtime::Config::new();
    wasmtime_config.async_support(true).consume_fuel(true);
    let runtime = WasmtimeRuntime::new(&wasmtime_config).unwrap();

    let env = Arc::new(lunatic_process::env::LunaticEnvironment::new(0));
    
    let raw_module = wat::parse_str(r#"
        (module)
    "#).unwrap();
    
    let module = Arc::new(runtime.compile_module(raw_module.into()).unwrap());
    let registry = Arc::new(RwLock::new(HashMap::new()));
    
    let mut state = DefaultProcessState::new(
        env,
        None,
        runtime,
        module,
        Arc::new(config),
        registry,
    ).unwrap();

    assert!(state.table_growing(0, 999, None), "Should allow 999 elements");
    assert!(state.table_growing(0, 1000, None), "Should allow exactly 1000 elements");
    assert!(!state.table_growing(0, 1001, None), "Should reject 1001 elements");
}

#[tokio::test]
async fn test_network_connection_limit() {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use lunatic_process::runtimes::wasmtime::WasmtimeRuntime;

    let mut config = DefaultProcessConfig::default();
    config.set_max_network_connections(3);

    let mut wasmtime_config = wasmtime::Config::new();
    wasmtime_config.async_support(true).consume_fuel(true);
    let runtime = WasmtimeRuntime::new(&wasmtime_config).unwrap();

    let env = Arc::new(lunatic_process::env::LunaticEnvironment::new(0));
    
    let raw_module = wat::parse_str(r#"
        (module)
    "#).unwrap();
    
    let module = Arc::new(runtime.compile_module(raw_module.into()).unwrap());
    let registry = Arc::new(RwLock::new(HashMap::new()));
    
    let mut state = DefaultProcessState::new(
        env,
        None,
        runtime,
        module,
        Arc::new(config),
        registry,
    ).unwrap();

    assert!(state.can_open_network_connection().is_ok(), "Should allow connection 1");
    assert!(state.can_open_network_connection().is_ok(), "Should allow connection 2");
    assert!(state.can_open_network_connection().is_ok(), "Should allow connection 3");
    assert!(state.can_open_network_connection().is_err(), "Should reject connection 4 (limit reached)");
    
    state.close_network_connection();
    assert!(state.can_open_network_connection().is_ok(), "Should allow connection after closing one");
}
