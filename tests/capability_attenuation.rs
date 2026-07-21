use std::{
    collections::HashMap,
    convert::TryInto,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, Once,
    },
    time::Duration,
};

use anyhow::Result;
use lunatic_common_api::{flush_audit, AuditFlushOutcome};
use lunatic_error_api::{ErrorCtx, MAX_ERROR_RESOURCES};
use lunatic_process::{
    config::ProcessConfig,
    env::LunaticEnvironment,
    runtimes::wasmtime::{default_config, WasmtimeInstance, WasmtimeRuntime},
    state::ProcessState,
};
use lunatic_process_api::ProcessConfigCtx;
use lunatic_runtime::{state::DefaultProcessState, DefaultProcessConfig};
use tokio::sync::RwLock;

struct AuditCapture;

static AUDIT_CAPTURE: AuditCapture = AuditCapture;
static AUDIT_EVENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());
static AUDIT_LOGGER_INIT: Once = Once::new();
static NEXT_ENVIRONMENT_ID: AtomicU64 = AtomicU64::new(1);

impl log::Log for AuditCapture {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target() == "audit"
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            AUDIT_EVENTS.lock().unwrap().push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

fn initialize_audit_capture() {
    AUDIT_LOGGER_INIT.call_once(|| {
        log::set_logger(&AUDIT_CAPTURE).expect("the integration test owns its logger");
        log::set_max_level(log::LevelFilter::Info);
    });
}

fn captured_audit_events() -> Vec<serde_json::Value> {
    assert_eq!(
        flush_audit(Duration::from_secs(2)),
        AuditFlushOutcome::Flushed,
        "audit events must reach the capture sink"
    );
    AUDIT_EVENTS
        .lock()
        .unwrap()
        .iter()
        .map(|event| serde_json::from_str(event).expect("audit record must be JSON"))
        .collect()
}

fn events_for_environment(
    environment_id: u64,
    event: &str,
    action: &str,
    result: &str,
) -> Vec<serde_json::Value> {
    captured_audit_events()
        .into_iter()
        .filter(|entry| {
            entry["subject"]["environment_id"] == environment_id
                && entry["event"] == event
                && entry["action"] == action
                && entry["result"] == result
        })
        .collect()
}

const DELEGATION_GUEST: &str = r#"
(module
    (import "lunatic::error" "string_size" (func $error_string_size (param i64) (result i32)))
    (import "lunatic::error" "drop" (func $error_drop (param i64)))
    (import "lunatic::process" "compile_module" (func $compile_module (param i32 i32 i32) (result i32)))
    (import "lunatic::process" "create_config" (func $create_config (result i64)))
    (import "lunatic::process" "config_set_can_compile_modules" (func $set_compile (param i64 i32)))
    (import "lunatic::process" "config_set_can_create_configs" (func $set_create (param i64 i32)))
    (import "lunatic::process" "config_set_can_spawn_processes" (func $set_spawn (param i64 i32)))
    (import "lunatic::process" "config_can_compile_modules" (func $get_compile (param i64) (result i32)))
    (import "lunatic::process" "config_can_create_configs" (func $get_create (param i64) (result i32)))
    (import "lunatic::process" "config_can_spawn_processes" (func $get_spawn (param i64) (result i32)))
    (import "lunatic::process" "config_set_max_memory" (func $set_memory (param i64 i64)))
    (import "lunatic::process" "config_get_max_memory" (func $get_memory (param i64) (result i64)))
    (import "lunatic::process" "config_set_max_fuel" (func $set_fuel (param i64 i64)))
    (import "lunatic::process" "config_get_max_fuel" (func $get_fuel (param i64) (result i64)))
    (import "lunatic::process" "config_set_max_table_elements" (func $set_table (param i64 i32)))
    (import "lunatic::process" "config_get_max_table_elements" (func $get_table (param i64) (result i32)))
    (import "lunatic::process" "config_set_max_file_descriptors" (func $set_fds (param i64 i32)))
    (import "lunatic::process" "config_get_max_file_descriptors" (func $get_fds (param i64) (result i32)))
    (import "lunatic::process" "config_set_max_network_connections" (func $set_network (param i64 i32)))
    (import "lunatic::process" "config_get_max_network_connections" (func $get_network (param i64) (result i32)))
    (import "lunatic::process" "config_set_checked" (func $set_checked (param i64 i32 i64) (result i64)))
    (import "lunatic::process" "spawn" (func $spawn (param i64 i64 i64 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::process" "get_or_spawn" (func $get_or_spawn (param i32 i32 i64 i64 i64 i32 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::distributed" "spawn" (func $distributed_spawn (param i64 i64 i64 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::wasi" "config_preopen_dir" (func $preopen (param i64 i32 i32)))
    (import "lunatic::wasi" "config_preopen_dir_checked" (func $preopen_checked (param i64 i32 i32) (result i64)))
    (import "lunatic::wasi" "config_add_environment_variable" (func $add_env (param i64 i32 i32 i32 i32)))
    (import "lunatic::wasi" "config_add_command_line_argument" (func $add_arg (param i64 i32 i32)))
    (import "lunatic::networking" "udp_bind" (func $udp_bind (param i32 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::networking" "drop_udp_socket" (func $drop_udp_socket (param i64)))
    (import "lunatic::networking" "tcp_bind" (func $tcp_bind (param i32 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::networking" "drop_tcp_listener" (func $drop_tcp_listener (param i64)))
    (import "lunatic::networking" "tcp_accept" (func $tcp_accept (param i64 i32 i32) (result i32)))
    (import "lunatic::networking" "resolve" (func $resolve (param i32 i32 i64 i32) (result i32)))
    (import "lunatic::networking" "drop_dns_iterator" (func $drop_dns_iterator (param i64)))
    (import "lunatic::networking" "tls_bind" (func $tls_bind (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))
    (import "lunatic::registry" "put" (func $registry_put (param i32 i32 i64 i64)))
    (import "lunatic::registry" "remove" (func $registry_remove (param i32 i32)))

    (memory (export "memory") 1)
    (data (i32.const 0) ".")
    (data (i32.const 16) "crates")
    (data (i32.const 32) "child")
    (data (i32.const 48) "name")
    (data (i32.const 96) "SECRET_KEY")
    (data (i32.const 128) "secret-value-sentinel")
    (data (i32.const 176) "secret-argument-sentinel")
    (data (i32.const 224) "\7f\00\00\01")
    (data (i32.const 256) "secret-registry-name")
    (data (i32.const 288) "localhost:80")

    (func $assert_checked_error (param $error i64)
        (if (i64.eq (local.get $error) (i64.const -1)) (then unreachable))
        (if (i32.eq (call $error_string_size (local.get $error)) (i32.const 0))
            (then unreachable)))

    (func (export "deny_by_default")
        (if (i32.ne (call $compile_module (i32.const 0) (i32.const 0) (i32.const 0)) (i32.const -1))
            (then unreachable))
        (if (i64.ne (call $create_config) (i64.const -1))
            (then unreachable)))

    (func (export "deny_spawn_by_default")
        (if
            (i32.ne
                (call $spawn
                    (i64.const 0)
                    (i64.const -1)
                    (i64.const -1)
                    (i32.const 32)
                    (i32.const 5)
                    (i32.const 0)
                    (i32.const 0)
                (i32.const 64))
                (i32.const 1))
            (then unreachable))
        (if (i32.eq (call $error_string_size (i64.load (i32.const 64))) (i32.const 0))
            (then unreachable)))

    (func (export "deny_get_or_spawn_by_default")
        (if
            (i32.ne
                (call $get_or_spawn
                    (i32.const 48)
                    (i32.const 4)
                    (i64.const 0)
                    (i64.const -1)
                    (i64.const -1)
                    (i32.const 32)
                    (i32.const 5)
                    (i32.const 0)
                    (i32.const 0)
                    (i32.const 72)
                (i32.const 80))
                (i32.const 1))
            (then unreachable))
        (if (i32.eq (call $error_string_size (i64.load (i32.const 80))) (i32.const 0))
            (then unreachable)))

    (func (export "get_or_spawn_invalid_output")
        (drop
            (call $get_or_spawn
                (i32.const 48)
                (i32.const 4)
                (i64.const 0)
                (i64.const -1)
                (i64.const -1)
                (i32.const 32)
                (i32.const 5)
                (i32.const 0)
                (i32.const 0)
                (i32.const 65532)
                (i32.const 80))))

    (func (export "preopen_seeded")
        (call $assert_checked_error
            (call $preopen_checked (i64.const 0) (i32.const 0) (i32.const 1))))

    (func (export "legacy_preopen_seeded")
        (call $preopen (i64.const 0) (i32.const 0) (i32.const 1)))

    (func (export "configure_sensitive_values") (local $config i64)
        (local.set $config (call $create_config))
        (call $add_env
            (local.get $config)
            (i32.const 96)
            (i32.const 10)
            (i32.const 128)
            (i32.const 21))
        (call $add_arg (local.get $config) (i32.const 176) (i32.const 24)))

    (func (export "configure_invalid_argument_pointer") (local $config i64)
        (local.set $config (call $create_config))
        (call $add_arg (local.get $config) (i32.const -1) (i32.const 2)))

    (func $escalate_compile (export "escalate_compile") (local $config i64)
        (local.set $config (call $create_config))
        (call $assert_checked_error
            (call $set_checked (local.get $config) (i32.const 5) (i64.const 1))))

    (func $escalate_spawn (export "escalate_spawn") (local $config i64)
        (local.set $config (call $create_config))
        (call $assert_checked_error
            (call $set_checked (local.get $config) (i32.const 7) (i64.const 1))))

    (func (export "legacy_escalate_spawn") (local $config i64)
        (local.set $config (call $create_config))
        (call $set_spawn (local.get $config) (i32.const 1))
        (if (i32.ne (call $get_spawn (local.get $config)) (i32.const 0))
            (then unreachable)))

    (func (export "escalate_create_seeded")
        (call $assert_checked_error
            (call $set_checked (i64.const 0) (i32.const 6) (i64.const 1))))

    (func $escalate_memory (export "escalate_memory") (local $config i64)
        (local.set $config (call $create_config))
        (call $assert_checked_error
            (call $set_checked (local.get $config) (i32.const 0) (i64.const 100001))))

    (func $escalate_fuel (export "escalate_fuel") (local $config i64)
        (local.set $config (call $create_config))
        (call $assert_checked_error
            (call $set_checked (local.get $config) (i32.const 1) (i64.const 0))))

    (func $escalate_table (export "escalate_table") (local $config i64)
        (local.set $config (call $create_config))
        (call $assert_checked_error
            (call $set_checked (local.get $config) (i32.const 2) (i64.const 65))))

    (func $escalate_fds (export "escalate_fds") (local $config i64)
        (local.set $config (call $create_config))
        (call $assert_checked_error
            (call $set_checked (local.get $config) (i32.const 3) (i64.const 9))))

    (func $escalate_network (export "escalate_network") (local $config i64)
        (local.set $config (call $create_config))
        (call $assert_checked_error
            (call $set_checked (local.get $config) (i32.const 4) (i64.const 5))))

    (func $escalate_preopen (export "escalate_preopen") (local $config i64)
        (local.set $config (call $create_config))
        (call $assert_checked_error
            (call $preopen_checked (local.get $config) (i32.const 0) (i32.const 1))))

    (func (export "spawn_selected_config")
        (if
            (i32.ne
                (call $spawn
                    (i64.const 0)
                    (i64.const 0)
                    (i64.const -1)
                    (i32.const 32)
                    (i32.const 5)
                    (i32.const 0)
                    (i32.const 0)
                    (i32.const 64))
                (i32.const 1))
            (then unreachable))
        (if (i32.eq (call $error_string_size (i64.load (i32.const 64))) (i32.const 0))
            (then unreachable)))

    (func (export "get_or_spawn_selected_config")
        (if
            (i32.ne
                (call $get_or_spawn
                    (i32.const 48)
                    (i32.const 4)
                    (i64.const 0)
                    (i64.const 0)
                    (i64.const -1)
                    (i32.const 32)
                    (i32.const 5)
                    (i32.const 0)
                    (i32.const 0)
                    (i32.const 72)
                    (i32.const 80))
                (i32.const 1))
            (then unreachable))
        (if (i32.eq (call $error_string_size (i64.load (i32.const 80))) (i32.const 0))
            (then unreachable)))

    (func (export "bounded_error_denials") (local $index i32) (local $error i64)
        (loop $repeat
            (local.set $error
                (call $set_checked (i64.const 9999) (i32.const 7) (i64.const 1)))
            (local.set $index (i32.add (local.get $index) (i32.const 1)))
            (br_if $repeat (i32.lt_u (local.get $index) (i32.const 1025))))
        (call $error_drop (local.get $error))
        (if (i32.eq (call $error_string_size (local.get $error)) (i32.const 0))
            (then unreachable)))

    (func (export "distributed_preopen_fails_closed") (local $config i64)
        (local.set $config (call $create_config))
        (call $preopen (local.get $config) (i32.const 16) (i32.const 6))
        (if
            (i32.ne
                (call $distributed_spawn
                    (i64.const 0)
                    (local.get $config)
                    (i64.const 0)
                    (i32.const 32)
                    (i32.const 5)
                    (i32.const 0)
                    (i32.const 0)
                (i32.const 64))
                (i32.const 3))
            (then unreachable))
        (if (i32.eq (call $error_string_size (i64.load (i32.const 64))) (i32.const 0))
            (then unreachable)))

    (func $assert_config_and_spawn (param $config i64)
        (if (i32.ne (call $get_compile (local.get $config)) (i32.const 1)) (then unreachable))
        (if (i32.ne (call $get_create (local.get $config)) (i32.const 1)) (then unreachable))
        (if (i32.ne (call $get_spawn (local.get $config)) (i32.const 1)) (then unreachable))
        (if (i64.ne (call $get_memory (local.get $config)) (i64.const 65536)) (then unreachable))
        (if (i64.ne (call $get_fuel (local.get $config)) (i64.const 50)) (then unreachable))
        (if (i32.ne (call $get_table (local.get $config)) (i32.const 32)) (then unreachable))
        (if (i32.ne (call $get_fds (local.get $config)) (i32.const 4)) (then unreachable))
        (if (i32.ne (call $get_network (local.get $config)) (i32.const 2)) (then unreachable))
        (if
            (i32.ne
                (call $spawn
                    (i64.const 0)
                    (local.get $config)
                    (i64.const -1)
                    (i32.const 32)
                    (i32.const 5)
                    (i32.const 0)
                    (i32.const 0)
                    (i32.const 64))
                (i32.const 0))
            (then unreachable)))

    (func (export "delegate_and_spawn") (local $config i64)
        (local.set $config (call $create_config))
        (call $set_compile (local.get $config) (i32.const 1))
        (call $set_create (local.get $config) (i32.const 1))
        (call $set_spawn (local.get $config) (i32.const 1))
        (call $set_memory (local.get $config) (i64.const 65536))
        (call $set_fuel (local.get $config) (i64.const 50))
        (call $set_table (local.get $config) (i32.const 32))
        (call $set_fds (local.get $config) (i32.const 4))
        (call $set_network (local.get $config) (i32.const 2))
        (call $preopen (local.get $config) (i32.const 16) (i32.const 6))
        (call $assert_config_and_spawn (local.get $config)))

    (func (export "checked_delegate_and_spawn") (local $config i64)
        (local.set $config (call $create_config))
        (if (i64.ne (call $set_checked (local.get $config) (i32.const 5) (i64.const 1)) (i64.const -1)) (then unreachable))
        (if (i64.ne (call $set_checked (local.get $config) (i32.const 6) (i64.const 1)) (i64.const -1)) (then unreachable))
        (if (i64.ne (call $set_checked (local.get $config) (i32.const 7) (i64.const 1)) (i64.const -1)) (then unreachable))
        (if (i64.ne (call $set_checked (local.get $config) (i32.const 0) (i64.const 65536)) (i64.const -1)) (then unreachable))
        (if (i64.ne (call $set_checked (local.get $config) (i32.const 1) (i64.const 50)) (i64.const -1)) (then unreachable))
        (if (i64.ne (call $set_checked (local.get $config) (i32.const 2) (i64.const 32)) (i64.const -1)) (then unreachable))
        (if (i64.ne (call $set_checked (local.get $config) (i32.const 3) (i64.const 4)) (i64.const -1)) (then unreachable))
        (if (i64.ne (call $set_checked (local.get $config) (i32.const 4) (i64.const 2)) (i64.const -1)) (then unreachable))
        (if (i64.ne (call $preopen_checked (local.get $config) (i32.const 16) (i32.const 6)) (i64.const -1)) (then unreachable))
        (call $assert_config_and_spawn (local.get $config)))

    (func (export "invalid_checked_setting") (local $config i64)
        (local.set $config (call $create_config))
        (call $assert_checked_error
            (call $set_checked (local.get $config) (i32.const 99) (i64.const 0))))

    (func (export "udp_bind_once") (local $status i32)
        (local.set $status
            (call $udp_bind
                (i32.const 4)
                (i32.const 224)
                (i32.const 0)
                (i32.const 0)
                (i32.const 0)
                (i32.const 240)))
        (if (i32.eqz (local.get $status))
            (then (call $drop_udp_socket (i64.load (i32.const 240))))))

    (func (export "udp_bind_invalid_port")
        (drop
            (call $udp_bind
                (i32.const 4)
                (i32.const 224)
                (i32.const 65536)
                (i32.const 0)
                (i32.const 0)
                (i32.const 240))))

    (func (export "registry_put_remove")
        (call $registry_put
            (i32.const 256) (i32.const 20) (i64.const 77) (i64.const 88))
        (call $registry_remove (i32.const 256) (i32.const 20))
        (call $registry_remove (i32.const 256) (i32.const 20)))

    (func (export "tcp_bind_once") (local $status i32)
        (local.set $status
            (call $tcp_bind
                (i32.const 4) (i32.const 224) (i32.const 0)
                (i32.const 0) (i32.const 0) (i32.const 320)))
        (if (i32.eqz (local.get $status))
            (then (call $drop_tcp_listener (i64.load (i32.const 320))))))

    (func (export "tcp_accept_wait") (local $listener i64)
        (if
            (call $tcp_bind
                (i32.const 4) (i32.const 224) (i32.const 0)
                (i32.const 0) (i32.const 0) (i32.const 320))
            (then unreachable))
        (local.set $listener (i64.load (i32.const 320)))
        (drop (call $tcp_accept (local.get $listener) (i32.const 336) (i32.const 344))))

    (func (export "resolve_once") (local $status i32)
        (local.set $status
            (call $resolve
                (i32.const 288) (i32.const 12) (i64.const -1) (i32.const 328)))
        (if (i32.eqz (local.get $status))
            (then (call $drop_dns_iterator (i64.load (i32.const 328))))))

    (func (export "tls_bind_invalid_material")
        (drop
            (call $tls_bind
                (i32.const 4) (i32.const 224) (i32.const 0)
                (i32.const 0) (i32.const 0) (i32.const 336)
                (i32.const 352) (i32.const 0)
                (i32.const 352) (i32.const 0))))

    (func (export "child"))
)
"#;

async fn instantiate_guest(
    config: DefaultProcessConfig,
) -> Result<WasmtimeInstance<DefaultProcessState>> {
    instantiate_guest_with_seeded_child_and_process_limit(config, None, None).await
}

async fn instantiate_guest_with_seeded_child(
    config: DefaultProcessConfig,
    seeded_child: Option<DefaultProcessConfig>,
) -> Result<WasmtimeInstance<DefaultProcessState>> {
    instantiate_guest_with_seeded_child_and_process_limit(config, seeded_child, None).await
}

async fn instantiate_guest_with_process_limit(
    config: DefaultProcessConfig,
    max_processes: usize,
) -> Result<WasmtimeInstance<DefaultProcessState>> {
    instantiate_guest_with_seeded_child_and_process_limit(config, None, Some(max_processes)).await
}

async fn instantiate_guest_with_seeded_child_and_process_limit(
    config: DefaultProcessConfig,
    seeded_child: Option<DefaultProcessConfig>,
    max_processes: Option<usize>,
) -> Result<WasmtimeInstance<DefaultProcessState>> {
    initialize_audit_capture();
    let runtime = WasmtimeRuntime::new(&default_config())?;
    let raw_module = wat::parse_str(DELEGATION_GUEST)?;
    let module = Arc::new(runtime.compile_module(raw_module.into())?);
    let environment_id = NEXT_ENVIRONMENT_ID.fetch_add(1, Ordering::Relaxed);
    let environment = match max_processes {
        Some(max_processes) => Arc::new(LunaticEnvironment::with_max_processes(
            environment_id,
            max_processes,
        )),
        None => Arc::new(LunaticEnvironment::new(environment_id)),
    };
    let mut state = DefaultProcessState::new(
        environment,
        None,
        runtime.clone(),
        module.clone(),
        Arc::new(config),
        Arc::new(RwLock::new(HashMap::new())),
    )?;
    if let Some(child) = seeded_child {
        let config_id = state.config_resources_mut().add(child);
        assert_eq!(
            config_id, 0,
            "the seeded config must use guest-visible ID 0"
        );
    }
    runtime.instantiate(&module, state).await
}

#[tokio::test]
async fn local_process_quota_denials_are_typed_and_emitted_once() -> Result<()> {
    let mut config = DefaultProcessConfig::default();
    config.set_can_spawn_processes(true);
    let mut instance = instantiate_guest_with_process_limit(config, 0).await?;
    let environment_id = instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");

    instance
        .call_ref("deny_spawn_by_default", Vec::new())
        .await?;
    instance
        .call_ref("deny_get_or_spawn_by_default", Vec::new())
        .await?;

    let spawn = events_for_environment(environment_id, "process_spawn", "spawn", "denied");
    assert_eq!(spawn.len(), 1, "spawn quota denial must be emitted once");
    assert_eq!(spawn[0]["reason"], "resource_limit");

    let get_or_spawn =
        events_for_environment(environment_id, "process_spawn", "lookup_or_spawn", "denied");
    assert_eq!(
        get_or_spawn.len(),
        1,
        "get-or-spawn quota denial must be emitted once"
    );
    assert_eq!(get_or_spawn[0]["reason"], "resource_limit");
    Ok(())
}

#[tokio::test]
async fn udp_bind_host_path_emits_success_and_quota_denial_once() -> Result<()> {
    let mut allowed_config = DefaultProcessConfig::default();
    allowed_config.set_max_network_connections(1);
    let mut allowed = instantiate_guest(allowed_config).await?;
    let allowed_environment_id = allowed
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    allowed.call_ref("udp_bind_once", Vec::new()).await?;

    let allowed_events =
        events_for_environment(allowed_environment_id, "network_bind", "bind", "succeeded");
    assert_eq!(allowed_events.len(), 1, "UDP bind success must emit once");
    assert_eq!(allowed_events[0]["reason"], "completed");
    assert_eq!(allowed_events[0]["target"]["kind"], "udp_socket");
    assert_eq!(allowed_events[0]["target"]["sensitive_data"], "redacted");
    assert!(!allowed_events[0].to_string().contains("127.0.0.1"));

    let mut denied_config = DefaultProcessConfig::default();
    denied_config.set_max_network_connections(0);
    let mut denied = instantiate_guest(denied_config).await?;
    let denied_environment_id = denied
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    denied.call_ref("udp_bind_once", Vec::new()).await?;

    let denied_events =
        events_for_environment(denied_environment_id, "network_bind", "bind", "denied");
    assert_eq!(denied_events.len(), 1, "UDP quota denial must emit once");
    assert_eq!(denied_events[0]["reason"], "resource_limit");
    assert_eq!(denied_events[0]["target"]["sensitive_data"], "redacted");
    Ok(())
}

#[tokio::test]
async fn local_registry_host_path_emits_each_terminal_change_once() -> Result<()> {
    let mut instance = instantiate_guest(DefaultProcessConfig::default()).await?;
    let environment_id = instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    instance.call_ref("registry_put_remove", Vec::new()).await?;

    let registered = events_for_environment(
        environment_id,
        "distributed_registry_change",
        "register",
        "succeeded",
    );
    assert_eq!(registered.len(), 1, "registry put must emit once");
    assert_eq!(registered[0]["target"]["node_id"], 77);
    assert_eq!(registered[0]["target"]["process_id"], 88);

    let removed = events_for_environment(
        environment_id,
        "distributed_registry_change",
        "unregister",
        "succeeded",
    );
    assert_eq!(removed.len(), 1, "registry remove must emit once");

    let missing = events_for_environment(
        environment_id,
        "distributed_registry_change",
        "unregister",
        "failed",
    );
    assert_eq!(missing.len(), 1, "missing registry remove must emit once");
    assert_eq!(missing[0]["reason"], "not_found");

    for event in registered.iter().chain(&removed).chain(&missing) {
        assert_eq!(event["target"]["sensitive_data"], "redacted");
        assert!(!event.to_string().contains("secret-registry-name"));
    }
    Ok(())
}

#[tokio::test]
async fn tcp_dns_and_tls_host_paths_emit_typed_terminal_events_once() -> Result<()> {
    let mut allowed_config = DefaultProcessConfig::default();
    allowed_config.set_max_network_connections(2);
    let mut allowed = instantiate_guest(allowed_config).await?;
    let allowed_environment_id = allowed
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    allowed.call_ref("tcp_bind_once", Vec::new()).await?;
    allowed.call_ref("resolve_once", Vec::new()).await?;

    let tcp_succeeded =
        events_for_environment(allowed_environment_id, "network_bind", "bind", "succeeded");
    assert_eq!(tcp_succeeded.len(), 1, "TCP bind success must emit once");
    assert_eq!(tcp_succeeded[0]["target"]["kind"], "tcp_listener");

    let dns_succeeded = events_for_environment(
        allowed_environment_id,
        "network_connect",
        "resolve",
        "succeeded",
    );
    assert_eq!(dns_succeeded.len(), 1, "DNS success must emit once");
    assert_eq!(dns_succeeded[0]["target"]["kind"], "dns_iterator");

    let mut denied_config = DefaultProcessConfig::default();
    denied_config.set_max_network_connections(0);
    let mut denied = instantiate_guest(denied_config).await?;
    let denied_environment_id = denied
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    denied.call_ref("tcp_bind_once", Vec::new()).await?;
    denied.call_ref("resolve_once", Vec::new()).await?;

    let tcp_denied =
        events_for_environment(denied_environment_id, "network_bind", "bind", "denied");
    assert_eq!(tcp_denied.len(), 1, "TCP quota denial must emit once");
    assert_eq!(tcp_denied[0]["reason"], "resource_limit");

    let dns_denied = events_for_environment(
        denied_environment_id,
        "network_connect",
        "resolve",
        "denied",
    );
    assert_eq!(dns_denied.len(), 1, "DNS quota denial must emit once");
    assert_eq!(dns_denied[0]["reason"], "resource_limit");

    let mut invalid_tls = instantiate_guest(DefaultProcessConfig::default()).await?;
    let tls_environment_id = invalid_tls
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    assert!(invalid_tls
        .call_ref("tls_bind_invalid_material", Vec::new())
        .await
        .is_err());
    let tls_failed = events_for_environment(tls_environment_id, "network_bind", "bind", "failed");
    assert_eq!(tls_failed.len(), 1, "invalid TLS bind must emit once");
    assert_eq!(tls_failed[0]["reason"], "invalid_input");
    assert_eq!(tls_failed[0]["target"]["kind"], "tls_listener");

    let mut invalid_port = instantiate_guest(DefaultProcessConfig::default()).await?;
    let invalid_port_environment_id = invalid_port
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    assert!(invalid_port
        .call_ref("udp_bind_invalid_port", Vec::new())
        .await
        .is_err());
    let invalid_port_events = events_for_environment(
        invalid_port_environment_id,
        "network_bind",
        "bind",
        "failed",
    );
    assert_eq!(
        invalid_port_events.len(),
        1,
        "out-of-range network port must emit once"
    );
    assert_eq!(invalid_port_events[0]["reason"], "invalid_input");
    assert_eq!(
        invalid_port_events[0]["target"]["port"],
        serde_json::Value::Null
    );

    for event in tcp_succeeded
        .iter()
        .chain(&dns_succeeded)
        .chain(&tcp_denied)
        .chain(&dns_denied)
        .chain(&tls_failed)
        .chain(&invalid_port_events)
    {
        assert_eq!(event["target"]["sensitive_data"], "redacted");
        assert!(!event.to_string().contains("127.0.0.1"));
        assert!(!event.to_string().contains("localhost"));
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_tcp_accept_emits_one_cancelled_terminal_event() -> Result<()> {
    let mut config = DefaultProcessConfig::default();
    config.set_max_network_connections(2);
    let mut instance = instantiate_guest(config).await?;
    let environment_id = instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");

    assert!(tokio::time::timeout(
        Duration::from_millis(50),
        instance.call_ref("tcp_accept_wait", Vec::new()),
    )
    .await
    .is_err());

    let cancelled = events_for_environment(environment_id, "network_accept", "accept", "cancelled");
    assert_eq!(cancelled.len(), 1, "cancelled accept must emit once");
    assert_eq!(cancelled[0]["reason"], "cancelled");
    assert_eq!(cancelled[0]["target"]["kind"], "tcp_listener");
    assert_eq!(cancelled[0]["target"]["sensitive_data"], "redacted");
    Ok(())
}

#[tokio::test]
async fn default_guest_cannot_compile_or_create_configs() -> Result<()> {
    let mut instance = instantiate_guest(DefaultProcessConfig::default()).await?;
    let environment_id = instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    instance.call_ref("deny_by_default", Vec::new()).await?;
    assert!(instance.state().config_resources().is_empty());

    let compile = events_for_environment(environment_id, "module_compile", "compile", "denied");
    assert_eq!(compile.len(), 1, "compile denial must be emitted once");
    assert_eq!(compile[0]["schema_version"], 1);
    assert_eq!(compile[0]["reason"], "capability_denied");
    assert_eq!(compile[0]["subject"]["process_id"], instance.state().id());
    assert_eq!(compile[0]["target"]["kind"], "module");

    let create = events_for_environment(environment_id, "config_create", "create", "denied");
    assert_eq!(create.len(), 1, "config denial must be emitted once");
    assert_eq!(create[0]["reason"], "capability_denied");
    assert_eq!(create[0]["target"]["kind"], "configuration");
    Ok(())
}

#[tokio::test]
async fn sensitive_wasi_config_updates_are_redacted_and_emitted_once() -> Result<()> {
    let mut parent = DefaultProcessConfig::default();
    parent.set_can_create_configs(true);
    let mut instance = instantiate_guest(parent).await?;
    let environment_id = instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");

    instance
        .call_ref("configure_sensitive_values", Vec::new())
        .await?;

    let child = instance
        .state()
        .config_resources()
        .get(0)
        .expect("the guest-created configuration must exist");
    assert_eq!(
        child.environment_variables().as_slice(),
        &[("SECRET_KEY".to_owned(), "secret-value-sentinel".to_owned())]
    );
    assert_eq!(
        child.command_line_arguments().as_slice(),
        &["secret-argument-sentinel".to_owned()]
    );

    let events = events_for_environment(environment_id, "config_update", "mutate", "allowed");
    assert_eq!(
        events.len(),
        2,
        "each sensitive config update needs one event"
    );
    for event in events {
        assert_eq!(event["target"]["resource_id"], 0);
        assert_eq!(event["target"]["sensitive_data"], "redacted");
        let encoded = event.to_string();
        assert!(!encoded.contains("SECRET_KEY"));
        assert!(!encoded.contains("secret-value-sentinel"));
        assert!(!encoded.contains("secret-argument-sentinel"));
    }
    Ok(())
}

#[tokio::test]
async fn invalid_wasi_config_pointer_is_audited_once() -> Result<()> {
    let mut parent = DefaultProcessConfig::default();
    parent.set_can_create_configs(true);
    let mut instance = instantiate_guest(parent).await?;
    let environment_id = instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");

    assert!(instance
        .call_ref("configure_invalid_argument_pointer", Vec::new())
        .await
        .is_err());

    let events = events_for_environment(environment_id, "config_update", "mutate", "failed");
    assert_eq!(events.len(), 1, "invalid pointer needs one terminal event");
    assert_eq!(events[0]["reason"], "invalid_input");
    assert_eq!(events[0]["target"]["sensitive_data"], "redacted");
    Ok(())
}

#[tokio::test]
async fn invalid_checked_config_setting_is_audited_once() -> Result<()> {
    let mut parent = DefaultProcessConfig::default();
    parent.set_can_create_configs(true);
    let mut instance = instantiate_guest(parent).await?;
    let environment_id = instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");

    instance
        .call_ref("invalid_checked_setting", Vec::new())
        .await?;

    let events = events_for_environment(environment_id, "config_update", "mutate", "failed");
    assert_eq!(events.len(), 1, "invalid mutation needs one terminal event");
    assert_eq!(events[0]["reason"], "invalid_input");
    assert_eq!(events[0]["target"]["resource_id"], 0);
    Ok(())
}

#[tokio::test]
async fn get_or_spawn_rejects_invalid_output_before_side_effects() -> Result<()> {
    let mut parent = DefaultProcessConfig::default();
    parent.set_can_spawn_processes(true);
    let mut instance = instantiate_guest(parent).await?;
    let environment_id = instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");

    assert!(instance
        .call_ref("get_or_spawn_invalid_output", Vec::new())
        .await
        .is_err());

    let failures =
        events_for_environment(environment_id, "process_spawn", "lookup_or_spawn", "failed");
    assert_eq!(failures.len(), 1, "invalid output needs one failure event");
    assert_eq!(failures[0]["reason"], "invalid_input");
    let successes = events_for_environment(
        environment_id,
        "process_spawn",
        "lookup_or_spawn",
        "succeeded",
    );
    assert!(
        successes.is_empty(),
        "no process may be spawned before validation"
    );
    Ok(())
}

#[tokio::test]
async fn default_guest_host_imports_deny_spawn_and_preopen() -> Result<()> {
    let mut spawn_instance = instantiate_guest(DefaultProcessConfig::default()).await?;
    let spawn_environment_id = spawn_instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    spawn_instance
        .call_ref("deny_spawn_by_default", Vec::new())
        .await?;
    let error = spawn_instance
        .state()
        .error_resources()
        .get(0)
        .expect("spawn denial must create a guest-visible error resource");
    assert!(
        error
            .to_string()
            .contains("doesn't have permissions to spawn"),
        "spawn denial must be explicit: {}",
        error
    );

    let mut lookup_instance = instantiate_guest(DefaultProcessConfig::default()).await?;
    let lookup_environment_id = lookup_instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    lookup_instance
        .call_ref("deny_get_or_spawn_by_default", Vec::new())
        .await?;
    let error = lookup_instance
        .state()
        .error_resources()
        .get(0)
        .expect("get_or_spawn denial must create a guest-visible error resource");
    assert!(error.to_string().contains("get_or_spawn"));
    assert!(error
        .to_string()
        .contains("doesn't have permissions to spawn"));

    let parent = DefaultProcessConfig::default();
    let seeded_child = parent.new_child_config().unwrap();
    let mut preopen_instance =
        instantiate_guest_with_seeded_child(parent.clone(), Some(seeded_child)).await?;
    let preopen_environment_id = preopen_instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    preopen_instance
        .call_ref("preopen_seeded", Vec::new())
        .await?;
    let error = preopen_instance
        .state()
        .error_resources()
        .get(0)
        .expect("checked preopen denial must return an error resource");
    assert!(error
        .to_string()
        .contains("outside parent filesystem authority"));
    preopen_instance
        .call_ref("legacy_preopen_seeded", Vec::new())
        .await?;
    let candidate = preopen_instance.state().config_resources().get(0).unwrap();
    assert!(candidate.preopened_dirs().is_empty());
    parent.validate_child_config(candidate).unwrap();

    let spawn = events_for_environment(spawn_environment_id, "process_spawn", "spawn", "denied");
    assert_eq!(spawn.len(), 1, "spawn denial must be emitted once");
    assert_eq!(spawn[0]["reason"], "capability_denied");

    let lookup = events_for_environment(
        lookup_environment_id,
        "process_spawn",
        "lookup_or_spawn",
        "denied",
    );
    assert_eq!(lookup.len(), 1, "get_or_spawn denial must be emitted once");
    assert_eq!(lookup[0]["reason"], "capability_denied");
    assert_eq!(lookup[0]["target"]["sensitive_data"], "redacted");

    let preopens = events_for_environment(
        preopen_environment_id,
        "filesystem_preopen",
        "preopen",
        "denied",
    );
    assert_eq!(
        preopens.len(),
        2,
        "checked and legacy preopen denials each need one event"
    );
    for event in preopens {
        assert_eq!(event["reason"], "delegation_exceeds_parent");
        assert_eq!(event["target"]["kind"], "filesystem");
        assert_eq!(event["target"]["sensitive_data"], "redacted");
        let serialized = event.to_string();
        assert!(!serialized.contains("crates"));
        assert!(!serialized.contains("outside parent filesystem authority"));
    }
    Ok(())
}

#[tokio::test]
async fn legacy_void_setter_denial_is_a_safe_noop() -> Result<()> {
    let mut parent = DefaultProcessConfig::default();
    parent.set_can_create_configs(true);
    let mut instance = instantiate_guest(parent.clone()).await?;

    instance
        .call_ref("legacy_escalate_spawn", Vec::new())
        .await?;
    let candidate = instance.state().config_resources().get(0).unwrap();
    assert!(!candidate.can_spawn_processes());
    parent.validate_child_config(candidate).unwrap();
    Ok(())
}

#[tokio::test]
async fn local_spawn_boundary_revalidates_a_selected_config() -> Result<()> {
    let mut parent = DefaultProcessConfig::default();
    parent.set_can_spawn_processes(true);
    let mut instance = instantiate_guest(parent.clone()).await?;
    let mut escalated = parent.new_child_config().unwrap();
    escalated.set_can_compile_modules(true);

    let config_id = instance
        .state_mut()
        .config_resources_mut()
        .add(escalated.clone());
    assert_eq!(config_id, 0);
    instance
        .call_ref("spawn_selected_config", Vec::new())
        .await?;
    instance
        .call_ref("get_or_spawn_selected_config", Vec::new())
        .await?;
    let memory = instance.snapshot_memory()?.memory;
    let spawn_error_id = u64::from_le_bytes(memory[64..72].try_into().unwrap());
    let get_or_spawn_error_id = u64::from_le_bytes(memory[80..88].try_into().unwrap());
    for error_id in [spawn_error_id, get_or_spawn_error_id] {
        let error = instance
            .state()
            .error_resources()
            .get(error_id)
            .expect("each spawn boundary must return a guest-readable error");
        assert!(error.to_string().contains("compile-module capability"));
    }

    let result = instance
        .state()
        .new_state(instance.state().module().clone(), Arc::new(escalated));
    let error = match result {
        Ok(_) => panic!("new_state must reject authority above the parent"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("compile-module capability"));
    Ok(())
}

#[tokio::test]
async fn repeated_checked_denials_keep_error_resources_bounded() -> Result<()> {
    let mut instance = instantiate_guest(DefaultProcessConfig::default()).await?;
    instance
        .call_ref("bounded_error_denials", Vec::new())
        .await?;

    assert_eq!(
        instance.state().error_resources().len(),
        MAX_ERROR_RESOURCES
    );
    assert!(
        instance.state().error_resources().get(0).is_some(),
        "the oldest guest-owned error must remain valid at the limit"
    );
    let capacity_error = instance
        .state()
        .error_resources()
        .get((MAX_ERROR_RESOURCES - 1) as u64)
        .expect("the last slot must contain the reusable capacity error");
    assert!(capacity_error
        .to_string()
        .contains("error resource limit reached"));
    assert!(
        instance
            .state()
            .error_resources()
            .get(MAX_ERROR_RESOURCES as u64)
            .is_none(),
        "repeated denials must reuse the capacity error instead of allocating"
    );
    Ok(())
}

#[tokio::test]
async fn restricted_parent_cannot_escalate_child_authority() -> Result<()> {
    let create_parent = DefaultProcessConfig::default();
    let seeded_child = create_parent.new_child_config().unwrap();
    let mut instance =
        instantiate_guest_with_seeded_child(create_parent.clone(), Some(seeded_child)).await?;
    instance
        .call_ref("escalate_create_seeded", Vec::new())
        .await?;
    let error = instance
        .state()
        .error_resources()
        .get(0)
        .expect("checked setter must create an error resource")
        .to_string();
    assert!(
        error.contains("create-config capability"),
        "create-config setter returned the wrong error: {}",
        error
    );
    let candidate = instance
        .state()
        .config_resources()
        .get(0)
        .expect("the rejected mutation must leave the seeded config in place");
    create_parent
        .validate_child_config(candidate)
        .expect("a rejected create-config mutation must not remain stored");

    let mut parent = DefaultProcessConfig::default();
    parent.set_can_create_configs(true);
    parent.set_max_memory(100_000);
    parent.set_max_fuel(Some(100));
    parent.set_max_table_elements(64);
    parent.set_max_file_descriptors(8);
    parent.set_max_network_connections(4);
    parent.preopen_dir("crates");

    let denied = [
        ("escalate_compile", "compile-module capability"),
        ("escalate_spawn", "spawn capability"),
        ("escalate_memory", "max_memory"),
        ("escalate_fuel", "unlimited fuel"),
        ("escalate_table", "max_table_elements"),
        ("escalate_fds", "max_file_descriptors"),
        ("escalate_network", "max_network_connections"),
        ("escalate_preopen", "outside parent filesystem authority"),
    ];

    for &(function, expected) in &denied {
        let mut instance = instantiate_guest(parent.clone()).await?;
        instance.call_ref(function, Vec::new()).await?;
        let error = instance
            .state()
            .error_resources()
            .get(0)
            .expect("checked mutation must create an error resource")
            .to_string();
        assert!(
            error.contains(expected),
            "{} returned the wrong error: {}",
            function,
            error
        );
        let candidate = instance
            .state()
            .config_resources()
            .get(0)
            .expect("create_config stores the candidate before the rejected mutation")
            .clone();

        parent
            .validate_child_config(&candidate)
            .expect("a rejected mutation must not remain in the config resource");
    }

    Ok(())
}

#[tokio::test]
async fn distributed_spawn_rejects_host_local_preopens_before_transport() -> Result<()> {
    let mut parent = DefaultProcessConfig::default();
    parent.set_can_create_configs(true);
    parent.set_can_spawn_processes(true);
    parent.preopen_dir(".");

    let mut instance = instantiate_guest(parent).await?;
    let environment_id = instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    instance
        .call_ref("distributed_preopen_fails_closed", Vec::new())
        .await?;
    let error = instance
        .state()
        .error_resources()
        .get(0)
        .expect("distributed denial must create an error resource")
        .to_string();
    assert!(error.contains("remote config denied"), "{}", error);
    assert!(error.contains("host-local"), "{}", error);
    let events = events_for_environment(
        environment_id,
        "distributed_request_authorization",
        "spawn",
        "denied",
    );
    assert_eq!(events.len(), 1, "distributed denial must be emitted once");
    assert_eq!(events[0]["reason"], "delegation_denied");
    assert_eq!(events[0]["target"]["sensitive_data"], "redacted");
    Ok(())
}

#[tokio::test]
async fn allowed_delegation_can_spawn_a_child() -> Result<()> {
    let mut parent = DefaultProcessConfig::default();
    parent.set_can_compile_modules(true);
    parent.set_can_create_configs(true);
    parent.set_can_spawn_processes(true);
    parent.set_max_fuel(Some(100));
    parent.preopen_dir(".");

    let mut instance = instantiate_guest(parent.clone()).await?;
    let environment_id = instance
        .state()
        .audit_environment_id()
        .expect("default runtime exposes an environment ID");
    instance.call_ref("delegate_and_spawn", Vec::new()).await?;

    let child = instance
        .state()
        .config_resources()
        .get(0)
        .expect("the delegated config remains available");
    parent.validate_child_config(child).unwrap();
    assert!(child.can_compile_modules());
    assert!(child.can_create_configs());
    assert!(child.can_spawn_processes());
    assert_eq!(child.get_max_memory(), 65_536);
    assert_eq!(child.get_max_fuel(), Some(50));
    assert_eq!(child.get_max_table_elements(), 32);
    assert_eq!(child.get_max_file_descriptors(), 4);
    assert_eq!(child.get_max_network_connections(), 2);
    assert_eq!(child.preopened_dirs().len(), 1);

    let creates = events_for_environment(environment_id, "config_create", "create", "allowed");
    assert_eq!(creates.len(), 1, "config creation must be audited once");
    assert_eq!(creates[0]["target"]["resource_id"], 0);

    let mutations = events_for_environment(environment_id, "config_update", "mutate", "allowed");
    assert_eq!(mutations.len(), 8, "each config mutation needs one event");
    let validations =
        events_for_environment(environment_id, "config_update", "validate", "allowed");
    assert_eq!(validations.len(), 1, "spawn validation needs one event");

    let preopens =
        events_for_environment(environment_id, "filesystem_preopen", "preopen", "allowed");
    assert_eq!(preopens.len(), 1, "preopen delegation needs one event");
    assert_eq!(preopens[0]["target"]["sensitive_data"], "redacted");
    assert!(!preopens[0].to_string().contains("crates"));

    let spawns = events_for_environment(environment_id, "process_spawn", "spawn", "succeeded");
    assert_eq!(spawns.len(), 1, "successful spawn must be audited once");
    assert!(spawns[0]["target"]["process_id"].as_u64().is_some());

    Ok(())
}

#[tokio::test]
async fn checked_delegation_can_spawn_a_child() -> Result<()> {
    let mut parent = DefaultProcessConfig::default();
    parent.set_can_compile_modules(true);
    parent.set_can_create_configs(true);
    parent.set_can_spawn_processes(true);
    parent.set_max_fuel(Some(100));
    parent.preopen_dir(".");

    let mut instance = instantiate_guest(parent.clone()).await?;
    instance
        .call_ref("checked_delegate_and_spawn", Vec::new())
        .await?;

    let child = instance
        .state()
        .config_resources()
        .get(0)
        .expect("the checked delegated config remains available");
    parent.validate_child_config(child).unwrap();
    assert!(child.can_compile_modules());
    assert!(child.can_create_configs());
    assert!(child.can_spawn_processes());
    assert_eq!(child.get_max_memory(), 65_536);
    assert_eq!(child.get_max_fuel(), Some(50));
    assert_eq!(child.get_max_table_elements(), 32);
    assert_eq!(child.get_max_file_descriptors(), 4);
    assert_eq!(child.get_max_network_connections(), 2);
    assert_eq!(child.preopened_dirs().len(), 1);
    Ok(())
}
