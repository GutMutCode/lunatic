use std::{
    convert::{TryFrom, TryInto},
    future::Future,
    io::Write,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use hash_map_id::HashMapId;
use lunatic_common_api::{audit_log, get_memory, IntoTrap, LinkerAsyncExt};
use lunatic_distributed::DistributedCtx;
use lunatic_error_api::ErrorCtx;
use lunatic_process::{
    config::ProcessConfig,
    env::Environment,
    mailbox::MessageMailbox,
    message::Message,
    runtimes::{wasmtime::WasmtimeCompiledModule, RawWasm},
    state::{ensure_registry_insert_capacity, ProcessState, MAX_REGISTRY_NAME_BYTES},
    DeathReason, Process, Signal, WasmProcess,
};
use lunatic_wasi_api::LunaticWasiCtx;
use wasmtime::{Caller, Linker, ResourceLimiter, ToWasmtimeResult as _, Val};

pub type ProcessResources = HashMapId<Arc<dyn Process>>;
pub type ModuleResources<S> = HashMapId<Arc<WasmtimeCompiledModule<S>>>;

pub trait ProcessConfigCtx {
    fn can_compile_modules(&self) -> bool;
    fn set_can_compile_modules(&mut self, can: bool);
    fn can_create_configs(&self) -> bool;
    fn set_can_create_configs(&mut self, can: bool);
    fn can_spawn_processes(&self) -> bool;
    fn set_can_spawn_processes(&mut self, can: bool);
    fn get_max_table_elements(&self) -> u32 {
        0
    }
    fn set_max_table_elements(&mut self, _max: u32) {}
    fn get_max_file_descriptors(&self) -> u32 {
        0
    }
    fn set_max_file_descriptors(&mut self, _max: u32) {}
    fn get_max_network_connections(&self) -> u32 {
        0
    }
    fn set_max_network_connections(&mut self, _max: u32) {}
    fn get_max_mailbox_messages(&self) -> u32 {
        lunatic_process::config::DEFAULT_MAX_MAILBOX_MESSAGES
    }
    fn set_max_mailbox_messages(&mut self, _max: u32) {}
    fn get_max_signal_queue(&self) -> u32 {
        lunatic_process::config::DEFAULT_MAX_SIGNAL_QUEUE
    }
    fn set_max_signal_queue(&mut self, _max: u32) {}
    fn get_max_message_size(&self) -> u64 {
        lunatic_process::config::DEFAULT_MAX_MESSAGE_SIZE
    }
    fn set_max_message_size(&mut self, _max: u64) {}
    fn get_max_message_resources(&self) -> u32 {
        lunatic_process::config::DEFAULT_MAX_MESSAGE_RESOURCES
    }
    fn set_max_message_resources(&mut self, _max: u32) {}
    fn can_access_fs_location(&self, path: &Path) -> Result<(), String>;
}

pub trait ProcessCtx<S: ProcessState> {
    fn mailbox(&mut self) -> &mut MessageMailbox;
    fn message_scratch_area(&mut self) -> &mut Option<Message>;
    fn module_resources(&self) -> &ModuleResources<S>;
    fn module_resources_mut(&mut self) -> &mut ModuleResources<S>;
    fn environment(&self) -> Arc<dyn Environment>;
}

// Register the process APIs to the linker
pub fn register<T, E>(linker: &mut Linker<T>) -> Result<()>
where
    T: ProcessState
        + ProcessCtx<T>
        + DistributedCtx<E>
        + ErrorCtx
        + LunaticWasiCtx
        + Send
        + Sync
        + ResourceLimiter
        + 'static,
    for<'a> &'a T: Send,
    T::Config: ProcessConfigCtx,
    E: Environment + 'static,
{
    #[cfg(feature = "metrics")]
    lunatic_process::describe_metrics();

    #[cfg(feature = "metrics")]
    metrics::describe_counter!(
        "lunatic.process.modules.compiled",
        metrics::Unit::Count,
        "number of modules compiled since startup"
    );

    #[cfg(feature = "metrics")]
    metrics::describe_counter!(
        "lunatic.process.modules.dropped",
        metrics::Unit::Count,
        "number of modules dropped since startup"
    );

    #[cfg(feature = "metrics")]
    metrics::describe_gauge!(
        "lunatic.process.modules.active",
        metrics::Unit::Count,
        "number of modules currently in memory"
    );

    #[cfg(feature = "metrics")]
    metrics::describe_histogram!(
        "lunatic.process.modules.compiled.duration",
        metrics::Unit::Seconds,
        "Duration of module compilation"
    );

    linker.func_wrap(
        "lunatic::process",
        "compile_module",
        |caller: Caller<T>, module_data_ptr: u32, module_data_len: u32, id_ptr: u32| {
            compile_module(caller, module_data_ptr, module_data_len, id_ptr).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::process",
        "drop_module",
        |caller: Caller<T>, module_id: u64| drop_module(caller, module_id).to_wasmtime_result(),
    )?;

    #[cfg(feature = "metrics")]
    metrics::describe_counter!(
        "lunatic.process.configs.created",
        metrics::Unit::Count,
        "number of configs created since startup"
    );

    #[cfg(feature = "metrics")]
    metrics::describe_counter!(
        "lunatic.process.configs.dropped",
        metrics::Unit::Count,
        "number of configs dropped since startup"
    );

    #[cfg(feature = "metrics")]
    metrics::describe_gauge!(
        "lunatic.process.configs.active",
        metrics::Unit::Count,
        "number of configs currently in memory"
    );

    linker.func_wrap("lunatic::process", "create_config", create_config)?;
    linker.func_wrap(
        "lunatic::process",
        "drop_config",
        |caller: Caller<T>, config_id: u64| drop_config(caller, config_id).to_wasmtime_result(),
    )?;
    linker.func_wrap2_async(
        "lunatic::process",
        "config_set_max_memory",
        config_set_max_memory,
    )?;
    linker.func_wrap(
        "lunatic::process",
        "config_get_max_memory",
        |caller: Caller<T>, config_id: u64| {
            config_get_max_memory(caller, config_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap2_async(
        "lunatic::process",
        "config_set_max_fuel",
        config_set_max_fuel,
    )?;
    linker.func_wrap(
        "lunatic::process",
        "config_get_max_fuel",
        |caller: Caller<T>, config_id: u64| {
            config_get_max_fuel(caller, config_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap2_async(
        "lunatic::process",
        "config_set_max_table_elements",
        config_set_max_table_elements,
    )?;
    linker.func_wrap(
        "lunatic::process",
        "config_get_max_table_elements",
        |caller: Caller<T>, config_id: u64| {
            config_get_max_table_elements(caller, config_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap2_async(
        "lunatic::process",
        "config_set_max_file_descriptors",
        config_set_max_file_descriptors,
    )?;
    linker.func_wrap(
        "lunatic::process",
        "config_get_max_file_descriptors",
        |caller: Caller<T>, config_id: u64| {
            config_get_max_file_descriptors(caller, config_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap2_async(
        "lunatic::process",
        "config_set_max_network_connections",
        config_set_max_network_connections,
    )?;
    linker.func_wrap(
        "lunatic::process",
        "config_get_max_network_connections",
        |caller: Caller<T>, config_id: u64| {
            config_get_max_network_connections(caller, config_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap2_async(
        "lunatic::process",
        "config_set_max_mailbox_messages",
        config_set_max_mailbox_messages,
    )?;
    linker.func_wrap(
        "lunatic::process",
        "config_get_max_mailbox_messages",
        |caller: Caller<T>, config_id: u64| {
            config_get_max_mailbox_messages(caller, config_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap2_async(
        "lunatic::process",
        "config_set_max_signal_queue",
        config_set_max_signal_queue,
    )?;
    linker.func_wrap(
        "lunatic::process",
        "config_get_max_signal_queue",
        |caller: Caller<T>, config_id: u64| {
            config_get_max_signal_queue(caller, config_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap2_async(
        "lunatic::process",
        "config_set_max_message_size",
        config_set_max_message_size,
    )?;
    linker.func_wrap(
        "lunatic::process",
        "config_get_max_message_size",
        |caller: Caller<T>, config_id: u64| {
            config_get_max_message_size(caller, config_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap2_async(
        "lunatic::process",
        "config_set_max_message_resources",
        config_set_max_message_resources,
    )?;
    linker.func_wrap(
        "lunatic::process",
        "config_get_max_message_resources",
        |caller: Caller<T>, config_id: u64| {
            config_get_max_message_resources(caller, config_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap("lunatic::process", "config_set_checked", config_set_checked)?;
    linker.func_wrap(
        "lunatic::process",
        "config_can_compile_modules",
        |caller: Caller<T>, config_id: u64| {
            config_can_compile_modules(caller, config_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap2_async(
        "lunatic::process",
        "config_set_can_compile_modules",
        config_set_can_compile_modules,
    )?;
    linker.func_wrap(
        "lunatic::process",
        "config_can_create_configs",
        |caller: Caller<T>, config_id: u64| {
            config_can_create_configs(caller, config_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap2_async(
        "lunatic::process",
        "config_set_can_create_configs",
        config_set_can_create_configs,
    )?;
    linker.func_wrap(
        "lunatic::process",
        "config_can_spawn_processes",
        |caller: Caller<T>, config_id: u64| {
            config_can_spawn_processes(caller, config_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap2_async(
        "lunatic::process",
        "config_set_can_spawn_processes",
        config_set_can_spawn_processes,
    )?;

    linker.func_wrap8_async("lunatic::process", "spawn", spawn)?;
    linker.func_wrap11_async("lunatic::process", "get_or_spawn", get_or_spawn)?;
    linker.func_wrap1_async("lunatic::process", "sleep_ms", sleep_ms)?;
    linker.func_wrap(
        "lunatic::process",
        "die_when_link_dies",
        |caller: Caller<T>, trap: u32| die_when_link_dies(caller, trap).to_wasmtime_result(),
    )?;

    linker.func_wrap("lunatic::process", "process_id", process_id)?;
    linker.func_wrap("lunatic::process", "environment_id", environment_id)?;
    linker.func_wrap(
        "lunatic::process",
        "link",
        |caller: Caller<T>, tag: i64, process_id: u64| {
            link(caller, tag, process_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::process",
        "unlink",
        |caller: Caller<T>, process_id: u64| unlink(caller, process_id).to_wasmtime_result(),
    )?;
    linker.func_wrap(
        "lunatic::process",
        "monitor",
        |caller: Caller<T>, process_id: u64| monitor(caller, process_id).to_wasmtime_result(),
    )?;
    linker.func_wrap(
        "lunatic::process",
        "stop_monitoring",
        |caller: Caller<T>, monitor_ref: u64| {
            stop_monitoring(caller, monitor_ref).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::process",
        "kill",
        |caller: Caller<T>, process_id: u64| kill(caller, process_id).to_wasmtime_result(),
    )?;
    linker.func_wrap("lunatic::process", "exists", exists)?;
    Ok(())
}

// Compile a new WebAssembly module.
//
// The `spawn` function can be used to spawn new processes from the module.
// Module compilation can be a CPU intensive task.
//
// Returns:
// *  0 on success - The ID of the newly created module is written to **id_ptr**
// *  1 on error   - The error ID is written to **id_ptr**
// * -1 in case the process doesn't have permission to compile modules.
fn compile_module<T>(
    mut caller: Caller<T>,
    module_data_ptr: u32,
    module_data_len: u32,
    id_ptr: u32,
) -> Result<i32>
where
    T: ProcessState + ProcessCtx<T> + ErrorCtx,
    T::Config: ProcessConfigCtx,
{
    // TODO: Module compilation is CPU intensive and should be done on the blocking task thread pool.
    if !caller.data().config().can_compile_modules() {
        return Ok(-1);
    }

    #[cfg(feature = "metrics")]
    metrics::increment_counter!("lunatic.process.modules.compiled");

    #[cfg(feature = "metrics")]
    metrics::increment_gauge!("lunatic.process.modules.active", 1.0);

    #[cfg(feature = "metrics")]
    let start = Instant::now();

    let mut module = vec![0; module_data_len as usize];
    let memory = get_memory(&mut caller)?;
    memory
        .read(&caller, module_data_ptr as usize, module.as_mut_slice())
        .or_trap("lunatic::process::compile_module")?;

    let module = RawWasm::new(None, module);
    let (mod_or_error_id, result) = match caller.data().runtime().compile_module(module) {
        Ok(module) => (
            caller
                .data_mut()
                .module_resources_mut()
                .add(Arc::new(module)),
            0,
        ),
        Err(error) => (caller.data_mut().add_error_resource(error), 1),
    };

    #[cfg(feature = "metrics")]
    let duration = Instant::now() - start;
    #[cfg(feature = "metrics")]
    metrics::histogram!("lunatic.process.modules.compiled.duration", duration);

    memory
        .write(&mut caller, id_ptr as usize, &mod_or_error_id.to_le_bytes())
        .or_trap("lunatic::process::compile_module")?;
    Ok(result)
}

// Drops the module from resources.
//
// Traps:
// * If the module ID doesn't exist.
fn drop_module<T: ProcessState + ProcessCtx<T>>(
    mut caller: Caller<T>,
    module_id: u64,
) -> Result<()> {
    #[cfg(feature = "metrics")]
    metrics::increment_counter!("lunatic.process.modules.dropped");

    #[cfg(feature = "metrics")]
    metrics::decrement_gauge!("lunatic.process.modules.active", 1.0);

    caller
        .data_mut()
        .module_resources_mut()
        .remove(module_id)
        .or_trap("lunatic::process::drop_module: Module ID doesn't exist")?;
    Ok(())
}

// Create a new configuration with all capabilities denied and resource ceilings
// inherited from the caller.
//
// Returns:
// * ID of newly created configuration in case of success
// * -1 in case the process doesn't have permission to create new configurations
fn create_config<T>(mut caller: Caller<T>) -> i64
where
    T: ProcessState + ProcessCtx<T>,
    T::Config: ProcessConfigCtx,
{
    if !caller.data().config().can_create_configs() {
        audit_log(
            "capability_delegation",
            format!(
                "parent_process={} operation=create_config outcome=denied",
                caller.data().id()
            ),
        );
        return -1;
    }
    let parent_process = caller.data().id();
    let config = match caller.data().config().new_child_config() {
        Ok(config) => config,
        Err(_reason) => {
            audit_log(
                "capability_delegation",
                format!(
                    "parent_process={} operation=create_config outcome=denied",
                    caller.data().id()
                ),
            );
            return -1;
        }
    };
    #[cfg(feature = "metrics")]
    metrics::increment_counter!("lunatic.process.configs.created");
    #[cfg(feature = "metrics")]
    metrics::increment_gauge!("lunatic.process.configs.active", 1.0);
    let config_id = caller.data_mut().config_resources_mut().add(config);
    audit_log(
        "capability_delegation",
        format!(
            "parent_process={parent_process} config_id={config_id} operation=create_config outcome=allowed"
        ),
    );
    config_id as i64
}

// Drops the configuration from resources.
//
// Traps:
// * If the config ID doesn't exist.
fn drop_config<T: ProcessState + ProcessCtx<T>>(
    mut caller: Caller<T>,
    config_id: u64,
) -> Result<()> {
    caller
        .data_mut()
        .config_resources_mut()
        .remove(config_id)
        .or_trap("lunatic::process::drop_config: Config ID doesn't exist")?;
    #[cfg(feature = "metrics")]
    metrics::increment_counter!("lunatic.process.configs.dropped");
    #[cfg(feature = "metrics")]
    metrics::decrement_gauge!("lunatic.process.configs.active", 1.0);
    Ok(())
}

/// Applies a child-config mutation atomically after checking it against the
/// caller's authority. Failed mutations never remain in the resource table.
fn mutate_child_config<T, F>(
    caller: &mut Caller<T>,
    config_id: u64,
    operation: &'static str,
    mutate: F,
) -> Result<()>
where
    T: ProcessState,
    F: FnOnce(&mut T::Config),
{
    let parent_process = caller.data().id();
    let parent_config = caller.data().config().clone();
    let mut candidate = caller
        .data()
        .config_resources()
        .get(config_id)
        .or_trap(format!(
            "lunatic::process::{operation}: Config ID doesn't exist"
        ))?
        .clone();

    mutate(&mut candidate);
    if let Err(reason) = parent_config.validate_child_config(&candidate) {
        audit_log(
            "capability_delegation",
            format!(
                "parent_process={parent_process} config_id={config_id} operation={operation} outcome=denied"
            ),
        );
        return Err(anyhow!(
            "lunatic::process::{operation}: delegation denied: {reason}"
        ));
    }

    *caller
        .data_mut()
        .config_resources_mut()
        .get_mut(config_id)
        .or_trap(format!(
            "lunatic::process::{operation}: Config ID doesn't exist"
        ))? = candidate;
    audit_log(
        "capability_delegation",
        format!(
            "parent_process={parent_process} config_id={config_id} operation={operation} outcome=allowed"
        ),
    );
    Ok(())
}

fn validate_delegated_config<T: ProcessState>(
    state: &T,
    config_id: u64,
    child: &T::Config,
    operation: &'static str,
) -> Result<()> {
    let parent_process = state.id();
    if let Err(reason) = state.config().validate_child_config(child) {
        audit_log(
            "capability_delegation",
            format!(
                "parent_process={parent_process} config_id={config_id} operation={operation} outcome=denied"
            ),
        );
        return Err(anyhow!(
            "lunatic::process::{operation}: delegated config denied: {reason}"
        ));
    }
    audit_log(
        "capability_delegation",
        format!(
            "parent_process={parent_process} config_id={config_id} operation={operation} outcome=allowed"
        ),
    );
    Ok(())
}

fn return_process_error<T: ErrorCtx>(
    caller: &mut Caller<T>,
    id_ptr: u32,
    error: anyhow::Error,
) -> Result<u32> {
    let error_id = caller.data_mut().add_error_resource(error);
    let memory = get_memory(caller)?;
    memory
        .write(caller, id_ptr as usize, &error_id.to_le_bytes())
        .or_trap("lunatic::process::spawn: write error ID")?;
    Ok(1)
}

fn return_process_error_in_state<T: ErrorCtx>(
    state: &mut T,
    memory: &mut [u8],
    id_ptr: u32,
    error: anyhow::Error,
) -> Result<u32> {
    let error_id = state.add_error_resource(error);
    memory
        .get_mut(id_ptr as usize..(id_ptr + 8) as usize)
        .or_trap("lunatic::process::get_or_spawn: write error ID")?
        .write(&error_id.to_le_bytes())
        .or_trap("lunatic::process::get_or_spawn: write error ID")?;
    Ok(1)
}

// Applies a configuration mutation without crossing a host trap on validation
// failure. Returns -1 on success or an error-resource ID on failure.
//
// Setting IDs: 0=memory, 1=fuel, 2=table elements, 3=file descriptors,
// 4=network connections, 5=compile, 6=create-config, 7=spawn,
// 8=mailbox messages, 9=signal queue, 10=message bytes,
// 11=message resources.
fn config_set_checked<T>(mut caller: Caller<T>, config_id: u64, setting: u32, value: u64) -> i64
where
    T: ProcessState + ProcessCtx<T> + ErrorCtx,
    T::Config: ProcessConfigCtx,
{
    let result = match setting {
        0 => usize::try_from(value)
            .map_err(|_| anyhow!("max_memory exceeds platform max"))
            .and_then(|value| {
                mutate_child_config(&mut caller, config_id, "config_set_max_memory", |config| {
                    config.set_max_memory(value)
                })
            }),
        1 => mutate_child_config(&mut caller, config_id, "config_set_max_fuel", |config| {
            config.set_max_fuel((value != 0).then_some(value))
        }),
        2 => u32::try_from(value)
            .map_err(|_| anyhow!("max_table_elements exceeds u32"))
            .and_then(|value| {
                mutate_child_config(
                    &mut caller,
                    config_id,
                    "config_set_max_table_elements",
                    |config| config.set_max_table_elements(value),
                )
            }),
        3 => u32::try_from(value)
            .map_err(|_| anyhow!("max_file_descriptors exceeds u32"))
            .and_then(|value| {
                mutate_child_config(
                    &mut caller,
                    config_id,
                    "config_set_max_file_descriptors",
                    |config| config.set_max_file_descriptors(value),
                )
            }),
        4 => u32::try_from(value)
            .map_err(|_| anyhow!("max_network_connections exceeds u32"))
            .and_then(|value| {
                mutate_child_config(
                    &mut caller,
                    config_id,
                    "config_set_max_network_connections",
                    |config| config.set_max_network_connections(value),
                )
            }),
        5 => mutate_child_config(
            &mut caller,
            config_id,
            "config_set_can_compile_modules",
            |config| config.set_can_compile_modules(value != 0),
        ),
        6 => mutate_child_config(
            &mut caller,
            config_id,
            "config_set_can_create_configs",
            |config| config.set_can_create_configs(value != 0),
        ),
        7 => mutate_child_config(
            &mut caller,
            config_id,
            "config_set_can_spawn_processes",
            |config| config.set_can_spawn_processes(value != 0),
        ),
        8 => u32::try_from(value)
            .map_err(|_| anyhow!("max_mailbox_messages exceeds u32"))
            .and_then(|value| {
                mutate_child_config(
                    &mut caller,
                    config_id,
                    "config_set_max_mailbox_messages",
                    |config| ProcessConfigCtx::set_max_mailbox_messages(config, value),
                )
            }),
        9 => u32::try_from(value)
            .map_err(|_| anyhow!("max_signal_queue exceeds u32"))
            .and_then(|value| {
                mutate_child_config(
                    &mut caller,
                    config_id,
                    "config_set_max_signal_queue",
                    |config| ProcessConfigCtx::set_max_signal_queue(config, value),
                )
            }),
        10 => mutate_child_config(
            &mut caller,
            config_id,
            "config_set_max_message_size",
            |config| ProcessConfigCtx::set_max_message_size(config, value),
        ),
        11 => u32::try_from(value)
            .map_err(|_| anyhow!("max_message_resources exceeds u32"))
            .and_then(|value| {
                mutate_child_config(
                    &mut caller,
                    config_id,
                    "config_set_max_message_resources",
                    |config| ProcessConfigCtx::set_max_message_resources(config, value),
                )
            }),
        _ => Err(anyhow!("unknown config setting ID {setting}")),
    };

    match result {
        Ok(()) => -1,
        Err(error) => caller.data_mut().add_error_resource(error) as i64,
    }
}

// Sets the memory limit on a configuration.
//
// Legacy void ABI: invalid or denied mutations are safe no-ops. Use
// `config_set_checked` to receive a guest-readable error resource.
fn config_set_max_memory<T: ProcessState + ProcessCtx<T> + Send>(
    mut caller: Caller<T>,
    config_id: u64,
    max_memory: u64,
) -> Box<dyn Future<Output = Result<()>> + Send + '_> {
    Box::new(async move {
        if let Ok(max_memory) = usize::try_from(max_memory) {
            let _ =
                mutate_child_config(&mut caller, config_id, "config_set_max_memory", |config| {
                    config.set_max_memory(max_memory)
                });
        }
        Ok(())
    })
}

// Returns the memory limit of a configuration.
//
// Traps:
// * If the config ID doesn't exist.
fn config_get_max_memory<T: ProcessState + ProcessCtx<T>>(
    caller: Caller<T>,
    config_id: u64,
) -> Result<u64> {
    let max_memory = caller
        .data()
        .config_resources()
        .get(config_id)
        .or_trap("lunatic::process::config_get_max_memory: Config ID doesn't exist")?
        .get_max_memory();
    Ok(max_memory as u64)
}

// Sets the fuel limit on a configuration.
//
// A value of 0 indicates no fuel limit.
//
// Legacy void ABI: invalid or denied mutations are safe no-ops. Use
// `config_set_checked` to receive a guest-readable error resource.
fn config_set_max_fuel<T: ProcessState + ProcessCtx<T> + Send>(
    mut caller: Caller<T>,
    config_id: u64,
    max_fuel: u64,
) -> Box<dyn Future<Output = Result<()>> + Send + '_> {
    Box::new(async move {
        let max_fuel = match max_fuel {
            0 => None,
            max_fuel => Some(max_fuel),
        };

        let _ = mutate_child_config(&mut caller, config_id, "config_set_max_fuel", |config| {
            config.set_max_fuel(max_fuel)
        });
        Ok(())
    })
}

// Returns the fuel limit of a configuration.
//
// A value of 0 indicates no fuel limit.
//
// Traps:
// * If the config ID doesn't exist.
fn config_get_max_fuel<T: ProcessState + ProcessCtx<T>>(
    caller: Caller<T>,
    config_id: u64,
) -> Result<u64> {
    let max_fuel = caller
        .data()
        .config_resources()
        .get(config_id)
        .or_trap("lunatic::process::config_get_max_fuel: Config ID doesn't exist")?
        .get_max_fuel();
    match max_fuel {
        None => Ok(0),
        Some(max_fuel) => Ok(max_fuel),
    }
}

// Sets the maximum number of table elements on a child configuration.
// Invalid or denied legacy mutations are safe no-ops.
fn config_set_max_table_elements<T>(
    mut caller: Caller<T>,
    config_id: u64,
    max: u32,
) -> Box<dyn Future<Output = Result<()>> + Send + '_>
where
    T: ProcessState + ProcessCtx<T> + Send,
    T::Config: ProcessConfigCtx,
{
    Box::new(async move {
        let _ = mutate_child_config(
            &mut caller,
            config_id,
            "config_set_max_table_elements",
            |config| config.set_max_table_elements(max),
        );
        Ok(())
    })
}

// Returns the maximum number of table elements on a child configuration.
fn config_get_max_table_elements<T>(caller: Caller<T>, config_id: u64) -> Result<u32>
where
    T: ProcessState + ProcessCtx<T>,
    T::Config: ProcessConfigCtx,
{
    Ok(caller
        .data()
        .config_resources()
        .get(config_id)
        .or_trap("lunatic::process::config_get_max_table_elements: Config ID doesn't exist")?
        .get_max_table_elements())
}

// Sets the maximum number of file descriptors on a child configuration.
// Invalid or denied legacy mutations are safe no-ops.
fn config_set_max_file_descriptors<T>(
    mut caller: Caller<T>,
    config_id: u64,
    max: u32,
) -> Box<dyn Future<Output = Result<()>> + Send + '_>
where
    T: ProcessState + ProcessCtx<T> + Send,
    T::Config: ProcessConfigCtx,
{
    Box::new(async move {
        let _ = mutate_child_config(
            &mut caller,
            config_id,
            "config_set_max_file_descriptors",
            |config| config.set_max_file_descriptors(max),
        );
        Ok(())
    })
}

// Returns the maximum number of file descriptors on a child configuration.
fn config_get_max_file_descriptors<T>(caller: Caller<T>, config_id: u64) -> Result<u32>
where
    T: ProcessState + ProcessCtx<T>,
    T::Config: ProcessConfigCtx,
{
    Ok(caller
        .data()
        .config_resources()
        .get(config_id)
        .or_trap("lunatic::process::config_get_max_file_descriptors: Config ID doesn't exist")?
        .get_max_file_descriptors())
}

// Sets the maximum number of network connections on a child configuration.
// Invalid or denied legacy mutations are safe no-ops.
fn config_set_max_network_connections<T>(
    mut caller: Caller<T>,
    config_id: u64,
    max: u32,
) -> Box<dyn Future<Output = Result<()>> + Send + '_>
where
    T: ProcessState + ProcessCtx<T> + Send,
    T::Config: ProcessConfigCtx,
{
    Box::new(async move {
        let _ = mutate_child_config(
            &mut caller,
            config_id,
            "config_set_max_network_connections",
            |config| config.set_max_network_connections(max),
        );
        Ok(())
    })
}

// Returns the maximum number of network connections on a child configuration.
fn config_get_max_network_connections<T>(caller: Caller<T>, config_id: u64) -> Result<u32>
where
    T: ProcessState + ProcessCtx<T>,
    T::Config: ProcessConfigCtx,
{
    Ok(caller
        .data()
        .config_resources()
        .get(config_id)
        .or_trap("lunatic::process::config_get_max_network_connections: Config ID doesn't exist")?
        .get_max_network_connections())
}

fn config_set_max_mailbox_messages<T>(
    mut caller: Caller<T>,
    config_id: u64,
    max: u32,
) -> Box<dyn Future<Output = Result<()>> + Send + '_>
where
    T: ProcessState + ProcessCtx<T> + Send,
    T::Config: ProcessConfigCtx,
{
    Box::new(async move {
        let _ = mutate_child_config(
            &mut caller,
            config_id,
            "config_set_max_mailbox_messages",
            |config| ProcessConfigCtx::set_max_mailbox_messages(config, max),
        );
        Ok(())
    })
}

fn config_get_max_mailbox_messages<T>(caller: Caller<T>, config_id: u64) -> Result<u32>
where
    T: ProcessState + ProcessCtx<T>,
    T::Config: ProcessConfigCtx,
{
    let config =
        caller.data().config_resources().get(config_id).or_trap(
            "lunatic::process::config_get_max_mailbox_messages: Config ID doesn't exist",
        )?;
    Ok(ProcessConfigCtx::get_max_mailbox_messages(config))
}

fn config_set_max_signal_queue<T>(
    mut caller: Caller<T>,
    config_id: u64,
    max: u32,
) -> Box<dyn Future<Output = Result<()>> + Send + '_>
where
    T: ProcessState + ProcessCtx<T> + Send,
    T::Config: ProcessConfigCtx,
{
    Box::new(async move {
        let _ = mutate_child_config(
            &mut caller,
            config_id,
            "config_set_max_signal_queue",
            |config| ProcessConfigCtx::set_max_signal_queue(config, max),
        );
        Ok(())
    })
}

fn config_get_max_signal_queue<T>(caller: Caller<T>, config_id: u64) -> Result<u32>
where
    T: ProcessState + ProcessCtx<T>,
    T::Config: ProcessConfigCtx,
{
    let config = caller
        .data()
        .config_resources()
        .get(config_id)
        .or_trap("lunatic::process::config_get_max_signal_queue: Config ID doesn't exist")?;
    Ok(ProcessConfigCtx::get_max_signal_queue(config))
}

fn config_set_max_message_size<T>(
    mut caller: Caller<T>,
    config_id: u64,
    max: u64,
) -> Box<dyn Future<Output = Result<()>> + Send + '_>
where
    T: ProcessState + ProcessCtx<T> + Send,
    T::Config: ProcessConfigCtx,
{
    Box::new(async move {
        let _ = mutate_child_config(
            &mut caller,
            config_id,
            "config_set_max_message_size",
            |config| ProcessConfigCtx::set_max_message_size(config, max),
        );
        Ok(())
    })
}

fn config_get_max_message_size<T>(caller: Caller<T>, config_id: u64) -> Result<u64>
where
    T: ProcessState + ProcessCtx<T>,
    T::Config: ProcessConfigCtx,
{
    let config = caller
        .data()
        .config_resources()
        .get(config_id)
        .or_trap("lunatic::process::config_get_max_message_size: Config ID doesn't exist")?;
    Ok(ProcessConfigCtx::get_max_message_size(config))
}

fn config_set_max_message_resources<T>(
    mut caller: Caller<T>,
    config_id: u64,
    max: u32,
) -> Box<dyn Future<Output = Result<()>> + Send + '_>
where
    T: ProcessState + ProcessCtx<T> + Send,
    T::Config: ProcessConfigCtx,
{
    Box::new(async move {
        let _ = mutate_child_config(
            &mut caller,
            config_id,
            "config_set_max_message_resources",
            |config| ProcessConfigCtx::set_max_message_resources(config, max),
        );
        Ok(())
    })
}

fn config_get_max_message_resources<T>(caller: Caller<T>, config_id: u64) -> Result<u32>
where
    T: ProcessState + ProcessCtx<T>,
    T::Config: ProcessConfigCtx,
{
    let config =
        caller.data().config_resources().get(config_id).or_trap(
            "lunatic::process::config_get_max_message_resources: Config ID doesn't exist",
        )?;
    Ok(ProcessConfigCtx::get_max_message_resources(config))
}

// Returns 1 if processes spawned from this configuration can compile Wasm modules, otherwise 0.
//
// Traps:
// * If the config ID doesn't exist.
fn config_can_compile_modules<T>(caller: Caller<T>, config_id: u64) -> Result<u32>
where
    T: ProcessState + ProcessCtx<T>,
    T::Config: ProcessConfigCtx,
{
    let can = caller
        .data()
        .config_resources()
        .get(config_id)
        .or_trap("lunatic::process::config_can_compile_modules: Config ID doesn't exist")?
        .can_compile_modules();
    Ok(can as u32)
}

// If set to a value >0 (true), processes spawned from this configuration will be able to compile
// Wasm modules.
//
// Invalid or denied legacy mutations are safe no-ops.
fn config_set_can_compile_modules<T>(
    mut caller: Caller<T>,
    config_id: u64,
    can: u32,
) -> Box<dyn Future<Output = Result<()>> + Send + '_>
where
    T: ProcessState + ProcessCtx<T> + Send,
    T::Config: ProcessConfigCtx,
{
    Box::new(async move {
        let _ = mutate_child_config(
            &mut caller,
            config_id,
            "config_set_can_compile_modules",
            |config| config.set_can_compile_modules(can != 0),
        );
        Ok(())
    })
}

// Returns 1 if processes spawned from this configuration can create other configurations,
// otherwise 0.
//
// Traps:
// * If the config ID doesn't exist.
fn config_can_create_configs<T>(caller: Caller<T>, config_id: u64) -> Result<u32>
where
    T: ProcessState + ProcessCtx<T>,
    T::Config: ProcessConfigCtx,
{
    let can = caller
        .data()
        .config_resources()
        .get(config_id)
        .or_trap("lunatic::process::config_can_create_configs: Config ID doesn't exist")?
        .can_create_configs();
    Ok(can as u32)
}

// If set to a value >0 (true), processes spawned from this configuration will be able to create
// other configuration.
//
// Invalid or denied legacy mutations are safe no-ops.
fn config_set_can_create_configs<T>(
    mut caller: Caller<T>,
    config_id: u64,
    can: u32,
) -> Box<dyn Future<Output = Result<()>> + Send + '_>
where
    T: ProcessState + ProcessCtx<T> + Send,
    T::Config: ProcessConfigCtx,
{
    Box::new(async move {
        let _ = mutate_child_config(
            &mut caller,
            config_id,
            "config_set_can_create_configs",
            |config| config.set_can_create_configs(can != 0),
        );
        Ok(())
    })
}

// Returns 1 if processes spawned from this configuration can spawn sub-processes, otherwise 0.
//
// Traps:
// * If the config ID doesn't exist.
fn config_can_spawn_processes<T>(caller: Caller<T>, config_id: u64) -> Result<u32>
where
    T: ProcessState + ProcessCtx<T>,
    T::Config: ProcessConfigCtx,
{
    let can = caller
        .data()
        .config_resources()
        .get(config_id)
        .or_trap("lunatic::process::config_can_spawn_processes: Config ID doesn't exist")?
        .can_spawn_processes();
    Ok(can as u32)
}

// If set to a value >0 (true), processes spawned from this configuration will be able to spawn
// sub-processes.
//
// Invalid or denied legacy mutations are safe no-ops.
fn config_set_can_spawn_processes<T>(
    mut caller: Caller<T>,
    config_id: u64,
    can: u32,
) -> Box<dyn Future<Output = Result<()>> + Send + '_>
where
    T: ProcessState + ProcessCtx<T> + Send,
    T::Config: ProcessConfigCtx,
{
    Box::new(async move {
        let _ = mutate_child_config(
            &mut caller,
            config_id,
            "config_set_can_spawn_processes",
            |config| config.set_can_spawn_processes(can != 0),
        );
        Ok(())
    })
}

// Spawns a new process using the passed in function inside a module as the entry point.
//
// If **link** is not 0, it will link the child and parent processes. The value of the **link**
// argument will be used as the link-tag for the child. This means, if the child traps the parent
// is going to get a signal back with the value used as the tag.
//
// If *config_id* or *module_id* have the value -1, the same module/config is used as in the
// process calling this function.
//
// The function arguments are passed as an array with the following structure:
// [0 byte = type ID; 1..17 bytes = value as u128, ...]
// The type ID follows the WebAssembly binary convention:
//  - 0x7F => i32
//  - 0x7E => i64
//  - 0x7B => v128
// If any other value is used as type ID, this function will trap.
//
// Returns:
// * 0 on success - The ID of the newly created process is written to **id_ptr**
// * 1 on error   - The error ID is written to **id_ptr**
//
// Traps:
// * If the module ID doesn't exist.
// * If the function string is not a valid utf8 string.
// * If the params array is in a wrong format.
// * If any memory outside the guest heap space is referenced.
#[allow(clippy::too_many_arguments)]
fn spawn<T>(
    mut caller: Caller<T>,
    link: i64,
    config_id: i64,
    module_id: i64,
    func_str_ptr: u32,
    func_str_len: u32,
    params_ptr: u32,
    params_len: u32,
    id_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_>
where
    T: ProcessState
        + ProcessCtx<T>
        + ErrorCtx
        + LunaticWasiCtx
        + ResourceLimiter
        + Send
        + Sync
        + 'static,
    for<'a> &'a T: Send,
    T::Config: ProcessConfigCtx,
{
    Box::new(async move {
        if !caller.data().config().can_spawn_processes() {
            return return_process_error(
                &mut caller,
                id_ptr,
                anyhow!("Process doesn't have permissions to spawn sub-processes"),
            );
        }

        if !caller.data().is_initialized() {
            return return_process_error(
                &mut caller,
                id_ptr,
                anyhow!("Cannot spawn process during module initialization"),
            );
        }

        let config = match config_id {
            -1 => caller.data().config().clone(),
            config_id => {
                let config = caller
                    .data()
                    .config_resources()
                    .get(config_id as u64)
                    .or_trap("lunatic::process::spawn: Config ID doesn't exist")?
                    .clone();
                if let Err(error) =
                    validate_delegated_config(caller.data(), config_id as u64, &config, "spawn")
                {
                    return return_process_error(&mut caller, id_ptr, error);
                }
                Arc::new(config)
            }
        };

        let module = match module_id {
            -1 => caller.data().module().clone(),
            module_id => caller
                .data()
                .module_resources()
                .get(module_id as u64)
                .or_trap("lunatic::process::spawn: Module ID doesn't exist")?
                .clone(),
        };

        let mut new_state = match caller.data().new_state(module.clone(), config) {
            Ok(state) => state,
            Err(error) => return return_process_error(&mut caller, id_ptr, error),
        };

        let memory = get_memory(&mut caller)?;
        let func_str = memory
            .data(&caller)
            .get(func_str_ptr as usize..(func_str_ptr + func_str_len) as usize)
            .or_trap("lunatic::process::spawn")?;
        let function = std::str::from_utf8(func_str).or_trap("lunatic::process::spawn")?;
        let params = memory
            .data(&caller)
            .get(params_ptr as usize..(params_ptr + params_len) as usize)
            .or_trap("lunatic::process::spawn")?;
        let params_chunks = &mut params.chunks_exact(17);
        let params = params_chunks
            .map(|chunk| {
                let value = u128::from_le_bytes(chunk[1..].try_into()?);
                let result = match chunk[0] {
                    0x7F => Val::I32(value as i32),
                    0x7E => Val::I64(value as i64),
                    0x7B => Val::V128(value.into()),
                    _ => return Err(anyhow!("Unsupported type ID")),
                };
                Ok(result)
            })
            .collect::<Result<Vec<_>>>()?;
        if !params_chunks.remainder().is_empty() {
            return Err(anyhow!(
                "Params array must be in chunks of 17 bytes, but {} bytes remained",
                params_chunks.remainder().len()
            ));
        }
        // Should processes be linked together?
        let link: Option<(Option<i64>, Arc<dyn Process>)> = match link {
            0 => None,
            tag => {
                let id = caller.data().id();
                let signal_mailbox = caller.data().signal_mailbox().clone();
                let process = WasmProcess::new(id, signal_mailbox.0);
                Some((Some(tag), Arc::new(process)))
            }
        };

        let runtime = caller.data().runtime().clone();

        // Inherit stdout and stderr streams if they are redirected by the parent.
        let stdout = if let Some(stdout) = caller.data().get_stdout() {
            let next_stream = stdout.next();
            new_state.set_stdout(next_stream.clone());
            Some((stdout.clone(), next_stream))
        } else {
            None
        };
        if let Some(stderr) = caller.data().get_stderr() {
            // If stderr is same as stdout, use same `next_stream`.
            if let Some((stdout, next_stream)) = stdout {
                if &stdout == stderr {
                    new_state.set_stderr(next_stream);
                } else {
                    new_state.set_stderr(stderr.next());
                }
            } else {
                new_state.set_stderr(stderr.next());
            }
        }

        let parent_id = caller.data().id();

        // set state instead of config TODO
        let env = caller.data().environment();
        let (proc_or_error_id, result) = match lunatic_process::wasm::spawn_wasm(
            env, runtime, &module, new_state, function, params, link,
        )
        .await
        {
            Ok((_, process)) => {
                audit_log(
                    "process_spawn",
                    format!("parent={} child={}", parent_id, process.id()),
                );
                (process.id(), 0)
            }
            Err(error) => (caller.data_mut().add_error_resource(error), 1),
        };

        memory
            .write(caller, id_ptr as usize, &proc_or_error_id.to_le_bytes())
            .or_trap("lunatic::process::spawn")?;
        Ok(result)
    })
}

// Looks up or spawns a new process.
//
// This function has a similar signature as `spawn`, but it first tries to look up a process in the registry
// under `name`. If it exists returns it, if not spawns a new one and registers it under this name. This
// operation is atomic. While a new process is being looked up and spawned, no other process can be inserted
// into the registry under the same name.
//
// Different than spawn, the lookup can result in a process running on a different node. This means that the
// node_id also needs to be returned through a pointer.
//
// Returns:
// * 0 on success        - The ID of the newly created process is written to **id_ptr**
// * 1 on error          - The error ID is written to **id_ptr**
// * 2 on lookup success - The lookup found a process and the id is written to **id_ptr**
//
// Traps:
// * If the name lookup string is not a valid utf8 string.
// * If the module ID doesn't exist.
// * If the function string is not a valid utf8 string.
// * If the params array is in a wrong format.
// * If any memory outside the guest heap space is referenced.
#[allow(clippy::too_many_arguments)]
fn get_or_spawn<T, E>(
    mut caller: Caller<T>,
    name_str_ptr: u32,
    name_str_len: u32,
    link: i64,
    config_id: i64,
    module_id: i64,
    func_str_ptr: u32,
    func_str_len: u32,
    params_ptr: u32,
    params_len: u32,
    node_id_ptr: u32,
    id_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_>
where
    T: ProcessState
        + ProcessCtx<T>
        + DistributedCtx<E>
        + ErrorCtx
        + LunaticWasiCtx
        + ResourceLimiter
        + Send
        + Sync
        + 'static,
    for<'a> &'a T: Send,
    T::Config: ProcessConfigCtx,
    E: Environment,
{
    Box::new(async move {
        let memory = get_memory(&mut caller)?;
        let (memory_slice, state) = memory.data_and_store_mut(&mut caller);
        let name = memory_slice
            .get(name_str_ptr as usize..(name_str_ptr + name_str_len) as usize)
            .or_trap("lunatic::process::get_or_spawn")?;
        let name = std::str::from_utf8(name).or_trap("lunatic::process::get_or_spawn")?;

        // Lock the registry for every other process before lookup.
        let registry = state.registry().clone();
        let mut registry = registry.write().await;
        let node_id = state
            .distributed()
            .as_ref()
            .map(|distributed| distributed.node_id())
            .unwrap_or(0);
        let environment = state.environment();
        let process = match registry.get(name).copied() {
            Some((entry_node_id, process_id))
                if entry_node_id == node_id && environment.get_process(process_id).is_none() =>
            {
                // Local process registration is the liveness authority. Remove
                // a stale name atomically so get-or-spawn can reuse it.
                registry.remove(name);
                None
            }
            process => process,
        };

        if let Some((node_id, process_id)) = process {
            // Return the process from the registry.
            memory_slice
                .get_mut(node_id_ptr as usize..(node_id_ptr + 8) as usize)
                .or_trap("lunatic::process::get_or_spawn")?
                .write(&node_id.to_le_bytes())
                .or_trap("lunatic::process::get_or_spawn")?;

            memory_slice
                .get_mut(id_ptr as usize..(id_ptr + 8) as usize)
                .or_trap("lunatic::process::get_or_spawn")?
                .write(&process_id.to_le_bytes())
                .or_trap("lunatic::process::get_or_spawn")?;
            Ok(2)
        } else {
            if name.len() <= MAX_REGISTRY_NAME_BYTES
                && ensure_registry_insert_capacity(&registry, name).is_err()
            {
                // Capacity may consist entirely of dead local registrations.
                // Sweep only on saturation so normal lookup remains O(1), and
                // never discard remote entries whose liveness is not locally
                // authoritative.
                registry.retain(|_, (entry_node_id, process_id)| {
                    *entry_node_id != node_id || environment.get_process(*process_id).is_some()
                });
            }
            if let Err(error) = ensure_registry_insert_capacity(&registry, name) {
                return return_process_error_in_state(state, memory_slice, id_ptr, error);
            }
            let name = name.to_owned();
            // Spawn a new process. This is copy of the code in `spawn` because host functions can't call
            // each other.
            if !state.config().can_spawn_processes() {
                return return_process_error_in_state(
                    state,
                    memory_slice,
                    id_ptr,
                    anyhow!(
                        "lunatic::process:get_or_spawn: Process doesn't have permissions to spawn sub-processes"
                    ),
                );
            }

            if !state.is_initialized() {
                return return_process_error_in_state(
                    state,
                    memory_slice,
                    id_ptr,
                    anyhow!(
                        "lunatic::process:get_or_spawn: Cannot spawn process during module initialization"
                    ),
                );
            }

            let config = match config_id {
                -1 => state.config().clone(),
                config_id => {
                    let config = state
                        .config_resources()
                        .get(config_id as u64)
                        .or_trap("lunatic::process::get_or_spawn: Config ID doesn't exist")?
                        .clone();
                    if let Err(error) =
                        validate_delegated_config(state, config_id as u64, &config, "get_or_spawn")
                    {
                        return return_process_error_in_state(state, memory_slice, id_ptr, error);
                    }
                    Arc::new(config)
                }
            };

            let module = match module_id {
                -1 => state.module().clone(),
                module_id => state
                    .module_resources()
                    .get(module_id as u64)
                    .or_trap("lunatic::process::get_or_spawn: Module ID doesn't exist")?
                    .clone(),
            };

            let mut new_state = match state.new_state(module.clone(), config) {
                Ok(new_state) => new_state,
                Err(error) => {
                    return return_process_error_in_state(state, memory_slice, id_ptr, error)
                }
            };

            let func_str = memory_slice
                .get(func_str_ptr as usize..(func_str_ptr + func_str_len) as usize)
                .or_trap("lunatic::process::get_or_spawn")?;
            let function =
                std::str::from_utf8(func_str).or_trap("lunatic::process::get_or_spawn")?;
            let params = memory_slice
                .get(params_ptr as usize..(params_ptr + params_len) as usize)
                .or_trap("lunatic::process::get_or_spawn")?;
            let params_chunks = &mut params.chunks_exact(17);
            let params = params_chunks
                .map(|chunk| {
                    let value = u128::from_le_bytes(chunk[1..].try_into()?);
                    let result = match chunk[0] {
                        0x7F => Val::I32(value as i32),
                        0x7E => Val::I64(value as i64),
                        0x7B => Val::V128(value.into()),
                        _ => return Err(anyhow!("Unsupported type ID")),
                    };
                    Ok(result)
                })
                .collect::<Result<Vec<_>>>()?;
            if !params_chunks.remainder().is_empty() {
                return Err(anyhow!(
                    "Params array must be in chunks of 17 bytes, but {} bytes remained",
                    params_chunks.remainder().len()
                ));
            }
            // Should processes be linked together?
            let link: Option<(Option<i64>, Arc<dyn Process>)> = match link {
                0 => None,
                tag => {
                    let id = state.id();
                    let signal_mailbox = state.signal_mailbox().clone();
                    let process = WasmProcess::new(id, signal_mailbox.0);
                    Some((Some(tag), Arc::new(process)))
                }
            };

            let runtime = state.runtime().clone();

            // Inherit stdout and stderr streams if they are redirected by the parent.
            let stdout = if let Some(stdout) = state.get_stdout() {
                let next_stream = stdout.next();
                new_state.set_stdout(next_stream.clone());
                Some((stdout.clone(), next_stream))
            } else {
                None
            };
            if let Some(stderr) = state.get_stderr() {
                // If stderr is same as stdout, use same `next_stream`.
                if let Some((stdout, next_stream)) = stdout {
                    if &stdout == stderr {
                        new_state.set_stderr(next_stream);
                    } else {
                        new_state.set_stderr(stderr.next());
                    }
                } else {
                    new_state.set_stderr(stderr.next());
                }
            }

            // set state instead of config TODO
            let env = state.environment();
            let (proc_or_error_id, result) = match lunatic_process::wasm::spawn_wasm(
                env, runtime, &module, new_state, function, params, link,
            )
            .await
            {
                Ok((_, process)) => (process.id(), 0),
                Err(error) => (state.add_error_resource(error), 1),
            };

            memory_slice
                .get_mut(node_id_ptr as usize..(node_id_ptr + 8) as usize)
                .or_trap("lunatic::process::get_or_spawn")?
                .write(&node_id.to_le_bytes())
                .or_trap("lunatic::process::get_or_spawn")?;

            memory_slice
                .get_mut(id_ptr as usize..(id_ptr + 8) as usize)
                .or_trap("lunatic::process::get_or_spawn")?
                .write(&proc_or_error_id.to_le_bytes())
                .or_trap("lunatic::process::get_or_spawn")?;

            // Error-resource IDs are local diagnostic handles, not process IDs.
            // Only publish a registry entry after central spawn admission and
            // process creation both succeeded.
            if result == 0 {
                registry.insert(name, (node_id, proc_or_error_id));
            }

            Ok(result)
        }
    })
}

// lunatic::process::sleep_ms(millis: u64)
//
// Suspend process for `millis`.
fn sleep_ms<T: ProcessState + ProcessCtx<T>>(
    _: Caller<T>,
    millis: u64,
) -> Box<dyn Future<Output = Result<()>> + Send + '_> {
    Box::new(async move {
        tokio::time::sleep(Duration::from_millis(millis)).await;
        Ok(())
    })
}

// Defines what happens to this process if one of the linked processes notifies us that it died.
//
// There are 2 options:
// 1. `trap == 0` the received signal will be turned into a signal message and put into the mailbox.
// 2. `trap != 0` the process will die and notify all linked processes of its death.
//
// The default behaviour for a newly spawned process is 2.
fn die_when_link_dies<T: ProcessState + ProcessCtx<T>>(
    mut caller: Caller<T>,
    trap: u32,
) -> Result<()> {
    caller
        .data_mut()
        .signal_mailbox()
        .0
        .send(Signal::DieWhenLinkDies(trap != 0))?;
    Ok(())
}

// Returns ID of the process currently running
fn process_id<T: ProcessState + ProcessCtx<T>>(caller: Caller<T>) -> u64 {
    caller.data().id()
}

// Returns ID of the environment in which the process is currently running
fn environment_id<T: ProcessState + ProcessCtx<T>>(caller: Caller<T>) -> u64 {
    caller.data().environment().id()
}

// Link current process to **process_id**. This is not an atomic operation, any of the 2 processes
// could fail before processing the `Link` signal and may not notify the other. If the target no
// longer exists, a `LinkDied` signal with `DeathReason::NoProcess` is sent to the caller.
fn link<T: ProcessState + ProcessCtx<T>>(
    mut caller: Caller<T>,
    tag: i64,
    process_id: u64,
) -> Result<()> {
    let tag = match tag {
        0 => None,
        tag => Some(tag),
    };
    // Create handle to itself
    let id = caller.data().id();
    let signal_mailbox = caller.data().signal_mailbox().clone();
    let this_process = WasmProcess::new(id, signal_mailbox.0);

    // Send link signal to other process
    let process = caller.data().environment().get_process(process_id);

    if let Some(process) = process {
        process.send(Signal::Link(tag, Arc::new(this_process)))?;

        // Send link signal to itself
        caller
            .data_mut()
            .signal_mailbox()
            .0
            .send(Signal::Link(tag, process))?;
    } else {
        caller.data_mut().signal_mailbox().0.send(Signal::LinkDied(
            process_id,
            tag,
            DeathReason::NoProcess,
        ))?;
    }
    Ok(())
}

// Unlink current process from **process_id**. This is not an atomic operation. Missing targets are
// ignored because there is no remaining link to remove.
fn unlink<T: ProcessState + ProcessCtx<T>>(mut caller: Caller<T>, process_id: u64) -> Result<()> {
    // Create handle to itself
    let this_process_id = caller.data().id();

    // Send unlink signal to other process
    let process = caller.data().environment().get_process(process_id);

    if let Some(process) = process {
        process.send(Signal::UnLink {
            process_id: this_process_id,
        })?;
    }

    // Send unlink signal to itself
    caller
        .data_mut()
        .signal_mailbox()
        .0
        .send(Signal::UnLink { process_id })?;

    Ok(())
}

// Start monitoring **process_id**. This is not an atomic operation. If the target no longer
// exists, enqueue one `ProcessDied` notification for the caller immediately.
fn monitor<T: ProcessState + ProcessCtx<T>>(mut caller: Caller<T>, process_id: u64) -> Result<()> {
    let process = caller.data().environment().get_process(process_id);

    if let Some(process) = process {
        let id = caller.data().id();
        let signal_mailbox = caller.data().signal_mailbox().clone();
        let this_process = WasmProcess::new(id, signal_mailbox.0);
        process.send(Signal::Monitor(Arc::new(this_process)))?;
    } else {
        caller
            .data_mut()
            .signal_mailbox()
            .0
            .send(Signal::ProcessDied(process_id))?;
    }

    Ok(())
}

// Stop monitoring **process_id**. This is not an atomic operation. Missing targets are ignored
// because there is no live monitor registration to remove.
fn stop_monitoring<T: ProcessState + ProcessCtx<T>>(
    caller: Caller<T>,
    process_id: u64,
) -> Result<()> {
    // Create handle to itself
    let this_process_id = caller.data().id();

    // Send unlink signal to other process
    let process = caller.data().environment().get_process(process_id);

    if let Some(process) = process {
        process.send(Signal::StopMonitoring {
            process_id: this_process_id,
        })?;
    }

    Ok(())
}

// Send a Kill signal to **process_id**. Missing targets are ignored; process IDs are inherently
// racy and the target may exit between lookup and delivery.
fn kill<T: ProcessState + ProcessCtx<T>>(caller: Caller<T>, process_id: u64) -> Result<()> {
    // Send kill signal to process
    if let Some(process) = caller.data().environment().get_process(process_id) {
        process.send(Signal::Kill)?;
    }
    Ok(())
}

// Checks to see if a process exists
fn exists<T: ProcessState + ProcessCtx<T>>(caller: Caller<T>, process_id: u64) -> i32 {
    caller
        .data()
        .environment()
        .get_process(process_id)
        .is_some() as i32
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use lunatic_process::config::{
        DEFAULT_MAX_MAILBOX_MESSAGES, DEFAULT_MAX_MESSAGE_RESOURCES, DEFAULT_MAX_MESSAGE_SIZE,
        DEFAULT_MAX_SIGNAL_QUEUE,
    };

    use super::ProcessConfigCtx;

    #[derive(Default)]
    struct PreResourceCeilingConfig {
        compile: bool,
        create: bool,
        spawn: bool,
    }

    // This intentionally implements only the methods required before the
    // resource-ceiling imports were added.
    impl ProcessConfigCtx for PreResourceCeilingConfig {
        fn can_compile_modules(&self) -> bool {
            self.compile
        }

        fn set_can_compile_modules(&mut self, can: bool) {
            self.compile = can;
        }

        fn can_create_configs(&self) -> bool {
            self.create
        }

        fn set_can_create_configs(&mut self, can: bool) {
            self.create = can;
        }

        fn can_spawn_processes(&self) -> bool {
            self.spawn
        }

        fn set_can_spawn_processes(&mut self, can: bool) {
            self.spawn = can;
        }

        fn can_access_fs_location(&self, _path: &Path) -> Result<(), String> {
            Err("denied".into())
        }
    }

    #[test]
    fn existing_config_contexts_compile_with_fail_closed_resource_defaults() {
        let mut config = PreResourceCeilingConfig::default();
        config.set_max_table_elements(100);
        config.set_max_file_descriptors(100);
        config.set_max_network_connections(100);
        config.set_max_mailbox_messages(100);
        config.set_max_signal_queue(100);
        config.set_max_message_size(100);
        config.set_max_message_resources(100);
        assert_eq!(config.get_max_table_elements(), 0);
        assert_eq!(config.get_max_file_descriptors(), 0);
        assert_eq!(config.get_max_network_connections(), 0);
        assert_eq!(
            config.get_max_mailbox_messages(),
            DEFAULT_MAX_MAILBOX_MESSAGES
        );
        assert_eq!(config.get_max_signal_queue(), DEFAULT_MAX_SIGNAL_QUEUE);
        assert_eq!(config.get_max_message_size(), DEFAULT_MAX_MESSAGE_SIZE);
        assert_eq!(
            config.get_max_message_resources(),
            DEFAULT_MAX_MESSAGE_RESOURCES
        );
    }
}
