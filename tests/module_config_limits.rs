use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use lunatic_process::{
    env::LunaticEnvironment,
    message::{DataMessage, Message},
    runtimes::{
        wasmtime::{
            default_config, CompiledModuleLimits, CompiledModuleUsage, WasmtimeCompiledModule,
            WasmtimeInstance, WasmtimeRuntime,
        },
        Modules, RawWasm,
    },
    state::ProcessState,
};
use lunatic_process_api::{ProcessConfigCtx, ProcessCtx};
use lunatic_runtime::{state::DefaultProcessState, DefaultProcessConfig};
use tokio::sync::RwLock;

fn wat_data(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("\\{byte:02x}")).collect()
}

async fn instance(
    guest: impl AsRef<[u8]>,
    config: DefaultProcessConfig,
) -> Result<WasmtimeInstance<DefaultProcessState>> {
    let runtime = WasmtimeRuntime::new(&default_config())?;
    let module = Arc::new(runtime.compile_module(guest.as_ref().to_vec().into())?);
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

#[tokio::test]
async fn compiled_module_budget_is_shared_and_released_after_final_clone() -> Result<()> {
    let source = wat::parse_str("(module)")?;
    let limits = CompiledModuleLimits {
        modules: 1,
        source_bytes: source.len(),
        single_module_bytes: source.len(),
    };
    let runtime = WasmtimeRuntime::new_with_module_limits(&default_config(), limits)?;
    let runtime_clone = runtime.clone();

    assert!(runtime
        .compile_module::<DefaultProcessState>(vec![0xff].into())
        .is_err());
    assert_eq!(
        runtime.compiled_module_usage(),
        CompiledModuleUsage::default()
    );

    let module = runtime.compile_module::<DefaultProcessState>(source.clone().into())?;
    let final_clone = module.clone();
    assert_eq!(runtime.compiled_module_usage().modules, 1);
    assert!(runtime_clone
        .compile_module::<DefaultProcessState>(source.clone().into())
        .is_err());

    drop(module);
    assert_eq!(runtime.compiled_module_usage().modules, 1);
    drop(final_clone);
    assert_eq!(
        runtime.compiled_module_usage(),
        CompiledModuleUsage::default()
    );

    let replacement = runtime_clone.compile_module::<DefaultProcessState>(source.into())?;
    assert_eq!(runtime.compiled_module_usage().modules, 1);
    drop(replacement);
    assert_eq!(
        runtime.compiled_module_usage(),
        CompiledModuleUsage::default()
    );
    Ok(())
}

#[tokio::test]
async fn distributed_module_cache_is_bounded_reusable_and_removable() -> Result<()> {
    let runtime = WasmtimeRuntime::new(&default_config())?;
    let cache = Modules::<DefaultProcessState>::with_max_entries(1);
    let source = wat::parse_str("(module)")?;

    let first_compile = cache.compile(runtime.clone(), RawWasm::new(Some(7), source.clone()));
    let concurrent_compile = cache.compile(runtime.clone(), RawWasm::new(Some(7), source.clone()));
    let first = first_compile.await??;
    let reused = concurrent_compile.await??;
    let cached = cache
        .compile(runtime.clone(), RawWasm::new(Some(7), vec![0xff]))
        .await??;
    assert!(Arc::ptr_eq(&first, &reused));
    assert!(Arc::ptr_eq(&first, &cached));
    assert_eq!(cache.len(), 1);
    let detached_inner_clone = first.as_ref().clone();

    let error = match cache
        .compile(runtime.clone(), RawWasm::new(Some(8), source.clone()))
        .await?
    {
        Ok(_) => panic!("cache admission unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("cache limit"));
    assert_eq!(cache.len(), 1);

    assert!(!cache.remove_if_unused(7));
    drop((first, reused, cached));
    assert!(!cache.remove_if_unused(7));
    drop(detached_inner_clone);
    let second = cache
        .compile(runtime, RawWasm::new(Some(8), source))
        .await??;
    assert_eq!(cache.len(), 1);
    assert!(cache.get(7).is_none());
    assert!(cache.get(8).is_some());
    drop(second);
    Ok(())
}

#[tokio::test]
async fn compile_module_enforces_ranges_sizes_handles_and_rolls_back() -> Result<()> {
    let inner = wat::parse_str("(module)")?;
    let invalid_offset = inner.len() + 16;
    let guest = wat::parse_str(format!(
        r#"
        (module
            (import "lunatic::process" "compile_module"
                (func $compile (param i32 i32 i32) (result i32)))
            (import "lunatic::process" "drop_module" (func $drop (param i64)))
            (memory (export "memory") 1)
            (data (i32.const 0) "{}")
            (data (i32.const {}) "\ff")

            (func (export "compile_success")
                (if (i32.ne
                        (call $compile (i32.const 0) (i32.const {}) (i32.const 256))
                        (i32.const 0))
                    (then unreachable)))
            (func (export "compile_limit")
                (if (i32.ne
                        (call $compile (i32.const 0) (i32.const {}) (i32.const 256))
                        (i32.const 1))
                    (then unreachable)))
            (func (export "compile_oversized")
                (if (i32.ne
                        (call $compile (i32.const 0) (i32.const {}) (i32.const 256))
                        (i32.const 1))
                    (then unreachable)))
            (func (export "compile_invalid")
                (if (i32.ne
                        (call $compile (i32.const {}) (i32.const 1) (i32.const 256))
                        (i32.const 1))
                    (then unreachable)))
            (func (export "compile_bad_input")
                (drop (call $compile (i32.const -1) (i32.const 2) (i32.const 256))))
            (func (export "compile_bad_output")
                (drop (call $compile (i32.const 0) (i32.const {}) (i32.const 65532))))
            (func (export "drop_0") (call $drop (i64.const 0)))
            (func (export "drop_1") (call $drop (i64.const 1)))
            (func (export "compile_drop_stress") (param $iterations i32)
                (local $index i32)
                (loop $repeat
                    (if (i32.lt_u (local.get $index) (local.get $iterations))
                        (then
                            (if (i32.ne
                                    (call $compile
                                        (i32.const 0)
                                        (i32.const {})
                                        (i32.const 256))
                                    (i32.const 0))
                                (then unreachable))
                            (call $drop (i64.load (i32.const 256)))
                            (local.set $index
                                (i32.add (local.get $index) (i32.const 1)))
                            (br $repeat)))))
        )
        "#,
        wat_data(&inner),
        invalid_offset,
        inner.len(),
        inner.len(),
        inner.len() + 1,
        invalid_offset,
        inner.len(),
        inner.len(),
    ))?;

    let mut config = DefaultProcessConfig::default();
    config.set_can_compile_modules(true);
    config.set_max_modules(1);
    config.set_max_module_bytes(inner.len() as u64);
    let mut guest = instance(guest, config).await?;
    let baseline = guest.state().runtime().compiled_module_usage();

    guest.call_ref("compile_success", Vec::new()).await?;
    assert_eq!(guest.state().module_resources().len(), 1);
    assert_eq!(
        guest.state().runtime().compiled_module_usage().modules,
        baseline.modules + 1
    );

    guest.call_ref("compile_limit", Vec::new()).await?;
    assert_eq!(guest.state().module_resources().len(), 1);
    guest.call_ref("drop_0", Vec::new()).await?;
    assert!(guest.state().module_resources().is_empty());
    assert_eq!(guest.state().runtime().compiled_module_usage(), baseline);

    guest.call_ref("compile_oversized", Vec::new()).await?;
    guest.call_ref("compile_invalid", Vec::new()).await?;
    assert!(guest.state().module_resources().is_empty());
    assert_eq!(guest.state().runtime().compiled_module_usage(), baseline);

    guest.call_ref("compile_success", Vec::new()).await?;
    guest.call_ref("drop_1", Vec::new()).await?;
    assert_eq!(guest.state().runtime().compiled_module_usage(), baseline);

    assert!(guest
        .call_ref("compile_bad_input", Vec::new())
        .await
        .is_err());
    assert!(guest
        .call_ref("compile_bad_output", Vec::new())
        .await
        .is_err());
    assert!(guest.state().module_resources().is_empty());
    assert_eq!(guest.state().runtime().compiled_module_usage(), baseline);

    guest
        .call_ref("compile_drop_stress", vec![wasmtime::Val::I32(1_000)])
        .await?;
    assert!(guest.state().module_resources().is_empty());
    assert_eq!(guest.state().runtime().compiled_module_usage(), baseline);
    Ok(())
}

#[tokio::test]
async fn config_handle_limit_is_reusable_and_child_ceiling_is_configurable() -> Result<()> {
    let guest = wat::parse_str(
        r#"
        (module
            (import "lunatic::process" "create_config" (func $create (result i64)))
            (import "lunatic::process" "drop_config" (func $drop (param i64)))
            (import "lunatic::process" "config_set_max_modules"
                (func $set_max_modules (param i64 i32)))
            (import "lunatic::process" "config_get_max_modules"
                (func $get_max_modules (param i64) (result i32)))

            (func (export "create_first")
                (if (i64.ne (call $create) (i64.const 0)) (then unreachable))
                (call $set_max_modules (i64.const 0) (i32.const 1))
                (if (i32.ne (call $get_max_modules (i64.const 0)) (i32.const 1))
                    (then unreachable)))
            (func (export "create_at_limit")
                (if (i64.ne (call $create) (i64.const -1)) (then unreachable)))
            (func (export "drop_and_recreate")
                (call $drop (i64.const 0))
                (if (i64.ne (call $create) (i64.const 1)) (then unreachable)))
            (func (export "create_drop_stress") (param $iterations i32)
                (local $config i64)
                (local $index i32)
                (call $drop (i64.const 1))
                (loop $repeat
                    (if (i32.lt_u (local.get $index) (local.get $iterations))
                        (then
                            (local.set $config (call $create))
                            (if (i64.eq (local.get $config) (i64.const -1))
                                (then unreachable))
                            (call $drop (local.get $config))
                            (local.set $index
                                (i32.add (local.get $index) (i32.const 1)))
                            (br $repeat)))))
        )
        "#,
    )?;
    let mut config = DefaultProcessConfig::default();
    config.set_can_create_configs(true);
    config.set_max_configs(1);
    let mut guest = instance(guest, config).await?;

    guest.call_ref("create_first", Vec::new()).await?;
    assert_eq!(guest.state().config_resources().len(), 1);
    guest.call_ref("create_at_limit", Vec::new()).await?;
    assert_eq!(guest.state().config_resources().len(), 1);
    guest.call_ref("drop_and_recreate", Vec::new()).await?;
    assert_eq!(guest.state().config_resources().len(), 1);
    guest
        .call_ref("create_drop_stress", vec![wasmtime::Val::I32(5_000)])
        .await?;
    assert!(guest.state().config_resources().is_empty());
    Ok(())
}

#[tokio::test]
async fn take_module_quota_failure_preserves_message_ownership() -> Result<()> {
    let guest_source = wat::parse_str(
        r#"
        (module
            (import "lunatic::message" "take_module"
                (func $take_module (param i64) (result i64)))
            (func (export "take") (drop (call $take_module (i64.const 0))))
        )
        "#,
    )?;
    let runtime = WasmtimeRuntime::new(&default_config())?;
    let guest_module = Arc::new(runtime.compile_module(guest_source.into())?);
    let transferred: Arc<WasmtimeCompiledModule<DefaultProcessState>> =
        Arc::new(runtime.compile_module(wat::parse_str("(module)")?.into())?);
    let mut config = DefaultProcessConfig::default();
    config.set_max_modules(0);
    let mut state = DefaultProcessState::new(
        Arc::new(LunaticEnvironment::new(0)),
        None,
        runtime.clone(),
        guest_module.clone(),
        Arc::new(config),
        Arc::new(RwLock::new(HashMap::new())),
    )?;
    let mut message = DataMessage::default();
    message.add_resource(transferred);
    *state.message_scratch_area() = Some(Message::Data(message));
    let mut guest = runtime.instantiate(&guest_module, state).await?;

    assert!(guest.call_ref("take", Vec::new()).await.is_err());
    assert!(guest.state().module_resources().is_empty());
    let Message::Data(message) = guest.state_mut().message_scratch_area().as_mut().unwrap() else {
        panic!("data message expected");
    };
    assert!(message.resources[0].is_some());
    Ok(())
}

#[tokio::test]
async fn hot_reload_validates_module_config_handles_and_retained_wasi_config_atomically(
) -> Result<()> {
    let inner = wat::parse_str("(module)")?;
    let guest = wat::parse_str(format!(
        r#"
        (module
            (import "lunatic::process" "compile_module"
                (func $compile (param i32 i32 i32) (result i32)))
            (import "lunatic::process" "create_config" (func $create (result i64)))
            (memory (export "memory") 1)
            (data (i32.const 0) "{}")

            (func (export "create_resources")
                (if (i32.ne
                        (call $compile (i32.const 0) (i32.const {}) (i32.const 256))
                        (i32.const 0))
                    (then unreachable))
                (if (i64.ne (call $create) (i64.const 0)) (then unreachable)))
        )
        "#,
        wat_data(&inner),
        inner.len(),
    ))?;

    let mut config = DefaultProcessConfig::default();
    config.set_can_compile_modules(true);
    config.set_can_create_configs(true);
    config.set_max_modules(1);
    config.set_max_configs(1);
    config.set_max_module_bytes(inner.len() as u64);
    config.set_max_config_entries(4);
    config.set_max_config_bytes(128);
    config.set_command_line_arguments(vec!["retained-argument".into()]);
    let mut guest = instance(guest, config).await?;
    guest.call_ref("create_resources", Vec::new()).await?;
    assert_eq!(guest.state().module_resources().len(), 1);
    assert_eq!(guest.state().config_resources().len(), 1);

    let module = guest.state().module().clone();
    let original_config = guest.state().config().as_ref().clone();

    let mut lower = original_config.clone();
    lower.set_max_modules(0);
    let mut rejected = guest
        .state()
        .new_state_for_reload(module.clone(), Arc::new(lower))?;
    let error = guest
        .state_mut()
        .transfer_runtime_resources_to(&mut rejected)
        .expect_err("live module handles must fit the replacement limit");
    assert!(error.to_string().contains("module handles"));

    let mut lower = original_config.clone();
    lower.set_max_configs(0);
    let mut rejected = guest
        .state()
        .new_state_for_reload(module.clone(), Arc::new(lower))?;
    let error = guest
        .state_mut()
        .transfer_runtime_resources_to(&mut rejected)
        .expect_err("live config handles must fit the replacement limit");
    assert!(error.to_string().contains("configuration handles"));

    let mut lower = original_config.clone();
    lower.set_max_module_bytes((inner.len() - 1) as u64);
    let mut rejected = guest
        .state()
        .new_state_for_reload(module.clone(), Arc::new(lower))?;
    let error = guest
        .state_mut()
        .transfer_runtime_resources_to(&mut rejected)
        .expect_err("retained module sources must fit the replacement byte limit");
    assert!(error.to_string().contains("module 0 source size"));

    let mut lower = original_config.clone();
    lower.set_max_config_entries(1);
    let mut rejected = guest
        .state()
        .new_state_for_reload(module.clone(), Arc::new(lower))?;
    let error = guest
        .state_mut()
        .transfer_runtime_resources_to(&mut rejected)
        .expect_err("stored child configs must remain attenuated under replacement ceilings");
    assert!(error
        .to_string()
        .contains("configuration 0 exceeds replacement authority"));

    let mut lower = original_config.clone();
    lower.set_command_line_arguments(Vec::new());
    lower.set_max_config_bytes(1);
    let mut rejected = guest
        .state()
        .new_state_for_reload(module.clone(), Arc::new(lower))?;
    let error = guest
        .state_mut()
        .transfer_runtime_resources_to(&mut rejected)
        .expect_err("source WASI config retention must fit the replacement byte ceiling");
    assert!(error.to_string().contains("retained config byte count"));

    assert_eq!(guest.state().module_resources().len(), 1);
    assert_eq!(guest.state().config_resources().len(), 1);
    let mut replacement = guest
        .state()
        .new_state_for_reload(module, Arc::new(original_config))?;
    guest
        .state_mut()
        .transfer_runtime_resources_to(&mut replacement)?;
    assert!(guest.state().module_resources().is_empty());
    assert!(guest.state().config_resources().is_empty());
    assert_eq!(replacement.module_resources().len(), 1);
    assert_eq!(replacement.config_resources().len(), 1);
    Ok(())
}
