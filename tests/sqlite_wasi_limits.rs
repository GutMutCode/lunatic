use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use lunatic_process::{
    env::LunaticEnvironment,
    runtimes::wasmtime::{default_config, WasmtimeInstance, WasmtimeRuntime},
    state::ProcessState,
};
use lunatic_runtime::{state::DefaultProcessState, DefaultProcessConfig};
use tokio::sync::RwLock;
use wasmtime::Val;

const WASI_FD_GUEST: &str = r#"
(module
    (import "wasi_snapshot_preview1" "path_open"
        (func $path_open (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
    (import "wasi_snapshot_preview1" "fd_close"
        (func $fd_close (param i32) (result i32)))
    (import "wasi_snapshot_preview1" "fd_renumber"
        (func $fd_renumber (param i32 i32) (result i32)))
    (memory (export "memory") 1)
    (data (i32.const 0) "Cargo.toml")
    (global $held_fd (mut i32) (i32.const -1))

    (func $open (param $result_ptr i32) (result i32)
        (call $path_open
            (i32.const 3)
            (i32.const 0)
            (i32.const 0)
            (i32.const 10)
            (i32.const 0)
            (i64.const 2)
            (i64.const 0)
            (i32.const 0)
            (local.get $result_ptr)))

    (func (export "open_close_stress") (param $iterations i32)
        (local $index i32)
        (loop $repeat
            (if (i32.lt_u (local.get $index) (local.get $iterations))
                (then
                    (if (call $open (i32.const 32)) (then unreachable))
                    (if (call $fd_close (i32.load (i32.const 32))) (then unreachable))
                    (local.set $index (i32.add (local.get $index) (i32.const 1)))
                    (br $repeat)))))

    (func (export "limit_then_reuse")
        (if (call $open (i32.const 32)) (then unreachable))
        (if (i32.eqz (call $open (i32.const 36))) (then unreachable))
        (if (call $fd_close (i32.load (i32.const 32))) (then unreachable))
        (if (call $open (i32.const 36)) (then unreachable))
        (if (call $fd_close (i32.load (i32.const 36))) (then unreachable)))

    (func (export "bad_result_pointer")
        (drop (call $open (i32.const 65536))))

    (func (export "renumber_overwrites_destination")
        (if (call $open (i32.const 32)) (then unreachable))
        (if (call $open (i32.const 36)) (then unreachable))
        (if (call $fd_renumber
                (i32.load (i32.const 32))
                (i32.load (i32.const 36)))
            (then unreachable))
        (if (call $open (i32.const 40)) (then unreachable))
        (if (call $fd_close (i32.load (i32.const 36))) (then unreachable))
        (if (call $fd_close (i32.load (i32.const 40))) (then unreachable)))

    (func (export "hold_fd")
        (if (call $open (i32.const 32)) (then unreachable))
        (global.set $held_fd (i32.load (i32.const 32))))

    (func (export "close_held_fd")
        (if (call $fd_close (global.get $held_fd)) (then unreachable))
        (global.set $held_fd (i32.const -1)))

    (func (export "trap_with_held_fd")
        (if (call $open (i32.const 32)) (then unreachable))
        (global.set $held_fd (i32.load (i32.const 32)))
        unreachable)
)
"#;

const WASI_PREVIEW0_FD_GUEST: &str = r#"
(module
    (import "wasi_unstable" "path_open"
        (func $path_open (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
    (import "wasi_unstable" "fd_close"
        (func $fd_close (param i32) (result i32)))
    (memory (export "memory") 1)
    (data (i32.const 0) "Cargo.toml")

    (func $open (param $result_ptr i32) (result i32)
        (call $path_open
            (i32.const 3)
            (i32.const 0)
            (i32.const 0)
            (i32.const 10)
            (i32.const 0)
            (i64.const 2)
            (i64.const 0)
            (i32.const 0)
            (local.get $result_ptr)))

    (func (export "bad_result_pointer")
        (drop (call $open (i32.const 65536))))

    (func (export "open_close")
        (if (call $open (i32.const 32)) (then unreachable))
        (if (call $fd_close (i32.load (i32.const 32))) (then unreachable)))
)
"#;

const SQLITE_GUEST: &str = r#"
(module
    (import "lunatic::error" "drop" (func $drop_error (param i64)))
    (import "lunatic::sqlite" "open"
        (func $open (param i32 i32 i32) (result i64)))
    (import "lunatic::sqlite" "query_prepare_checked"
        (func $prepare (param i64 i32 i32 i32) (result i64)))
    (import "lunatic::sqlite" "sqlite3_finalize" (func $finalize (param i64)))
    (import "lunatic::sqlite" "sqlite3_close" (func $close (param i64)))
    (memory (export "memory") 1)
    (data (i32.const 0) ":memory:")
    (data (i32.const 16) "SELECT 1")
    (global $held_connection (mut i64) (i64.const -1))
    (global $held_statement (mut i64) (i64.const -1))

    (func $must_open (result i64)
        (if (i64.ne
                (call $open (i32.const 0) (i32.const 8) (i32.const 32))
                (i64.const 0))
            (then unreachable))
        (i64.load (i32.const 32)))

    (func $must_prepare (param $connection i64) (result i64)
        (if (i64.ne
                (call $prepare
                    (local.get $connection)
                    (i32.const 16)
                    (i32.const 8)
                    (i32.const 40))
                (i64.const -1))
            (then unreachable))
        (i64.load (i32.const 40)))

    (func (export "open_close_stress") (param $iterations i32)
        (local $index i32)
        (local $connection i64)
        (loop $repeat
            (if (i32.lt_u (local.get $index) (local.get $iterations))
                (then
                    (local.set $connection (call $must_open))
                    (call $close (local.get $connection))
                    (local.set $index (i32.add (local.get $index) (i32.const 1)))
                    (br $repeat)))))

    (func (export "statement_and_connection_limits_reuse")
        (local $connection i64)
        (local $statement i64)
        (local $error i64)
        (local.set $connection (call $must_open))
        (local.set $statement (call $must_prepare (local.get $connection)))

        (local.set $error
            (call $prepare
                (local.get $connection)
                (i32.const 16)
                (i32.const 8)
                (i32.const 48)))
        (if (i64.eq (local.get $error) (i64.const -1)) (then unreachable))
        (call $drop_error (local.get $error))

        (call $close (local.get $connection))
        (if (i64.eqz (call $open (i32.const 0) (i32.const 8) (i32.const 32)))
            (then unreachable))
        (call $drop_error (i64.load (i32.const 32)))

        (call $finalize (local.get $statement))
        (local.set $connection (call $must_open))
        (local.set $statement (call $must_prepare (local.get $connection)))
        (call $finalize (local.get $statement))
        (call $close (local.get $connection)))

    (func (export "bad_open_result_pointer")
        (drop (call $open (i32.const 0) (i32.const 8) (i32.const 65532))))

    (func (export "setup_held_connection")
        (global.set $held_connection (call $must_open)))

    (func (export "bad_prepare_result_pointer")
        (drop (call $prepare
            (global.get $held_connection)
            (i32.const 16)
            (i32.const 8)
            (i32.const 65532))))

    (func (export "prepare_held_statement")
        (global.set $held_statement
            (call $must_prepare (global.get $held_connection))))

    (func (export "cleanup_held_connection")
        (call $close (global.get $held_connection))
        (global.set $held_connection (i64.const -1)))

    (func (export "cleanup_held_resources")
        (call $finalize (global.get $held_statement))
        (global.set $held_statement (i64.const -1))
        (call $close (global.get $held_connection))
        (global.set $held_connection (i64.const -1)))

    (func (export "trap_with_held_resources")
        (global.set $held_connection (call $must_open))
        (global.set $held_statement
            (call $must_prepare (global.get $held_connection)))
        unreachable)
)
"#;

async fn instantiate(
    guest: &str,
    config: DefaultProcessConfig,
) -> Result<WasmtimeInstance<DefaultProcessState>> {
    let runtime = WasmtimeRuntime::new(&default_config())?;
    let module = Arc::new(runtime.compile_module(wat::parse_str(guest)?.into())?);
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

fn wasi_config(max_file_descriptors: u32) -> DefaultProcessConfig {
    let mut config = DefaultProcessConfig::default();
    config.preopen_dir(".");
    config.set_max_file_descriptors(max_file_descriptors);
    config
}

fn sqlite_config() -> DefaultProcessConfig {
    let mut config = DefaultProcessConfig::default();
    config.preopen_dir(".");
    config.set_max_sqlite_connections(1);
    config.set_max_sqlite_statements(1);
    config
}

#[tokio::test]
async fn wasi_fd_limit_recovers_after_close_and_bad_pointer_traps() -> Result<()> {
    let mut instance = instantiate(WASI_FD_GUEST, wasi_config(2)).await?;
    assert_eq!(instance.state().network_resource_counts(), (1, 0));

    instance
        .call_ref("open_close_stress", vec![Val::I32(2_000)])
        .await?;
    assert_eq!(instance.state().network_resource_counts(), (1, 0));
    instance.call_ref("limit_then_reuse", Vec::new()).await?;
    assert_eq!(instance.state().network_resource_counts(), (1, 0));

    for _ in 0..128 {
        assert!(instance
            .call_ref("bad_result_pointer", Vec::new())
            .await
            .is_err());
        assert_eq!(instance.state().network_resource_counts(), (1, 0));
    }
    Ok(())
}

#[tokio::test]
async fn preview0_wasi_fd_limit_recovers_after_bad_pointer_traps() -> Result<()> {
    let mut instance = instantiate(WASI_PREVIEW0_FD_GUEST, wasi_config(2)).await?;
    assert_eq!(instance.state().network_resource_counts(), (1, 0));

    for _ in 0..128 {
        assert!(instance
            .call_ref("bad_result_pointer", Vec::new())
            .await
            .is_err());
        assert_eq!(instance.state().network_resource_counts(), (1, 0));
    }
    instance.call_ref("open_close", Vec::new()).await?;
    assert_eq!(instance.state().network_resource_counts(), (1, 0));
    Ok(())
}

#[tokio::test]
async fn wasi_fd_renumber_releases_the_overwritten_destination_lease() -> Result<()> {
    let mut instance = instantiate(WASI_FD_GUEST, wasi_config(3)).await?;
    instance
        .call_ref("renumber_overwrites_destination", Vec::new())
        .await?;
    assert_eq!(instance.state().network_resource_counts(), (1, 0));
    Ok(())
}

#[tokio::test]
async fn wasi_open_descriptor_recovers_after_a_guest_trap() -> Result<()> {
    let mut instance = instantiate(WASI_FD_GUEST, wasi_config(2)).await?;
    assert!(instance
        .call_ref("trap_with_held_fd", Vec::new())
        .await
        .is_err());
    assert_eq!(instance.state().network_resource_counts(), (2, 0));

    instance.call_ref("close_held_fd", Vec::new()).await?;
    assert_eq!(instance.state().network_resource_counts(), (1, 0));
    instance
        .call_ref("open_close_stress", vec![Val::I32(1)])
        .await?;
    assert_eq!(instance.state().network_resource_counts(), (1, 0));
    Ok(())
}

#[tokio::test]
async fn trapped_process_state_drops_with_open_wasi_and_sqlite_resources() -> Result<()> {
    let wasi_instance = instantiate(WASI_FD_GUEST, wasi_config(2)).await?;
    let wasi_result = wasi_instance.call("trap_with_held_fd", Vec::new()).await;
    assert!(wasi_result.failure().is_some());
    assert_eq!(wasi_result.state().network_resource_counts(), (2, 0));
    drop(wasi_result);

    let sqlite_instance = instantiate(SQLITE_GUEST, sqlite_config()).await?;
    let sqlite_result = sqlite_instance
        .call("trap_with_held_resources", Vec::new())
        .await;
    assert!(sqlite_result.failure().is_some());
    assert_eq!(sqlite_result.state().sqlite_resource_counts(), (1, 1));
    drop(sqlite_result);
    Ok(())
}

#[tokio::test]
async fn sqlite_limits_close_and_bad_pointer_paths_recover_exactly() -> Result<()> {
    let mut instance = instantiate(SQLITE_GUEST, sqlite_config()).await?;
    instance
        .call_ref("open_close_stress", vec![Val::I32(2_000)])
        .await?;
    assert_eq!(instance.state().sqlite_resource_counts(), (0, 0));

    instance
        .call_ref("statement_and_connection_limits_reuse", Vec::new())
        .await?;
    assert_eq!(instance.state().sqlite_resource_counts(), (0, 0));

    for _ in 0..128 {
        assert!(instance
            .call_ref("bad_open_result_pointer", Vec::new())
            .await
            .is_err());
        assert_eq!(instance.state().sqlite_resource_counts(), (0, 0));
    }

    instance
        .call_ref("setup_held_connection", Vec::new())
        .await?;
    assert_eq!(instance.state().sqlite_resource_counts(), (1, 0));
    for _ in 0..128 {
        assert!(instance
            .call_ref("bad_prepare_result_pointer", Vec::new())
            .await
            .is_err());
        assert_eq!(instance.state().sqlite_resource_counts(), (1, 0));
    }
    instance
        .call_ref("cleanup_held_connection", Vec::new())
        .await?;
    assert_eq!(instance.state().sqlite_resource_counts(), (0, 0));

    assert!(instance
        .call_ref("trap_with_held_resources", Vec::new())
        .await
        .is_err());
    assert_eq!(instance.state().sqlite_resource_counts(), (1, 1));
    instance
        .call_ref("cleanup_held_resources", Vec::new())
        .await?;
    assert_eq!(instance.state().sqlite_resource_counts(), (0, 0));
    Ok(())
}

#[tokio::test]
async fn hot_reload_failure_is_atomic_and_success_moves_wasi_descriptors() -> Result<()> {
    let mut instance = instantiate(WASI_FD_GUEST, wasi_config(2)).await?;
    instance.call_ref("hold_fd", Vec::new()).await?;
    assert_eq!(instance.state().network_resource_counts(), (2, 0));

    let module = instance.state().module().clone();
    let mut lower_config = instance.state().config().as_ref().clone();
    lower_config.set_max_file_descriptors(1);
    let lower_config = Arc::new(lower_config);
    let mut rejected = instance
        .state()
        .new_state_for_reload(module.clone(), lower_config)?;
    assert!(instance
        .state_mut()
        .transfer_runtime_resources_to(&mut rejected)
        .is_err());
    assert_eq!(instance.state().network_resource_counts(), (2, 0));
    assert_eq!(rejected.network_resource_counts(), (1, 0));

    let config = instance.state().config().clone();
    let mut replacement = instance.state().new_state_for_reload(module, config)?;
    instance
        .state_mut()
        .transfer_runtime_resources_to(&mut replacement)?;
    assert_eq!(instance.state().network_resource_counts(), (1, 0));
    assert_eq!(replacement.network_resource_counts(), (2, 0));
    Ok(())
}

#[tokio::test]
async fn hot_reload_failure_is_atomic_and_success_moves_sqlite_resources() -> Result<()> {
    let mut instance = instantiate(SQLITE_GUEST, sqlite_config()).await?;
    instance
        .call_ref("setup_held_connection", Vec::new())
        .await?;
    instance
        .call_ref("prepare_held_statement", Vec::new())
        .await?;
    assert_eq!(instance.state().sqlite_resource_counts(), (1, 1));

    let module = instance.state().module().clone();
    let mut lower_config = instance.state().config().as_ref().clone();
    lower_config.set_max_sqlite_connections(0);
    lower_config.set_max_sqlite_statements(0);
    let mut rejected = instance
        .state()
        .new_state_for_reload(module.clone(), Arc::new(lower_config))?;
    assert!(instance
        .state_mut()
        .transfer_runtime_resources_to(&mut rejected)
        .is_err());
    assert_eq!(instance.state().sqlite_resource_counts(), (1, 1));
    assert_eq!(rejected.sqlite_resource_counts(), (0, 0));

    let config = instance.state().config().clone();
    let mut replacement = instance.state().new_state_for_reload(module, config)?;
    instance
        .state_mut()
        .transfer_runtime_resources_to(&mut replacement)?;
    assert_eq!(instance.state().sqlite_resource_counts(), (0, 0));
    assert_eq!(replacement.sqlite_resource_counts(), (1, 1));
    Ok(())
}
