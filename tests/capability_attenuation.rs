use std::{
    collections::HashMap,
    convert::TryInto,
    sync::{Arc, Mutex, Once},
};

use anyhow::Result;
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

impl log::Log for AuditCapture {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target() == "audit"
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            AUDIT_EVENTS
                .lock()
                .unwrap()
                .push(format!("{} {}", record.target(), record.args()));
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

    (memory (export "memory") 1)
    (data (i32.const 0) ".")
    (data (i32.const 16) "crates")
    (data (i32.const 32) "child")
    (data (i32.const 48) "name")

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

    (func (export "preopen_seeded")
        (call $assert_checked_error
            (call $preopen_checked (i64.const 0) (i32.const 0) (i32.const 1))))

    (func (export "legacy_preopen_seeded")
        (call $preopen (i64.const 0) (i32.const 0) (i32.const 1)))

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

    (func (export "child"))
)
"#;

async fn instantiate_guest(
    config: DefaultProcessConfig,
) -> Result<WasmtimeInstance<DefaultProcessState>> {
    instantiate_guest_with_seeded_child(config, None).await
}

async fn instantiate_guest_with_seeded_child(
    config: DefaultProcessConfig,
    seeded_child: Option<DefaultProcessConfig>,
) -> Result<WasmtimeInstance<DefaultProcessState>> {
    let runtime = WasmtimeRuntime::new(&default_config())?;
    let raw_module = wat::parse_str(DELEGATION_GUEST)?;
    let module = Arc::new(runtime.compile_module(raw_module.into())?);
    let mut state = DefaultProcessState::new(
        Arc::new(LunaticEnvironment::new(0)),
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
async fn default_guest_cannot_compile_or_create_configs() -> Result<()> {
    initialize_audit_capture();
    let mut instance = instantiate_guest(DefaultProcessConfig::default()).await?;
    instance.call_ref("deny_by_default", Vec::new()).await?;
    assert!(instance.state().config_resources().is_empty());
    assert!(AUDIT_EVENTS.lock().unwrap().iter().any(|event| {
        event.contains("capability_delegation")
            && event.contains("operation=create_config")
            && event.contains("outcome=denied")
    }));
    Ok(())
}

#[tokio::test]
async fn default_guest_host_imports_deny_spawn_and_preopen() -> Result<()> {
    let mut spawn_instance = instantiate_guest(DefaultProcessConfig::default()).await?;
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
    initialize_audit_capture();
    let mut parent = DefaultProcessConfig::default();
    parent.set_can_create_configs(true);
    parent.set_can_spawn_processes(true);
    parent.preopen_dir(".");

    let mut instance = instantiate_guest(parent).await?;
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
    assert!(AUDIT_EVENTS.lock().unwrap().iter().any(|event| {
        event.contains("capability_delegation")
            && event.contains("operation=distributed_spawn")
            && event.contains("outcome=denied")
    }));
    Ok(())
}

#[tokio::test]
async fn allowed_delegation_can_spawn_a_child() -> Result<()> {
    initialize_audit_capture();

    let mut parent = DefaultProcessConfig::default();
    parent.set_can_compile_modules(true);
    parent.set_can_create_configs(true);
    parent.set_can_spawn_processes(true);
    parent.set_max_fuel(Some(100));
    parent.preopen_dir(".");

    let mut instance = instantiate_guest(parent.clone()).await?;
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

    let events = AUDIT_EVENTS.lock().unwrap();
    assert!(events.iter().any(|event| {
        event.contains("capability_delegation")
            && event.contains("operation=config_set_can_spawn_processes")
            && event.contains("outcome=allowed")
    }));
    assert!(events
        .iter()
        .any(|event| event.contains("process_spawn") && event.contains("parent=")));

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
