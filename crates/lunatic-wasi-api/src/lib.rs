use anyhow::{anyhow, Result};
use lunatic_common_api::{audit_log, get_memory, IntoTrap};
use lunatic_error_api::ErrorCtx;
use lunatic_process::{config::ProcessConfig, state::ProcessState};
use lunatic_stdout_capture::StdoutCapture;
use wasi_common::{
    sync::{ambient_authority, Dir, WasiCtxBuilder},
    WasiCtx,
};
use wasmtime::{Caller, Linker, ToWasmtimeResult as _};

/// Create a `WasiCtx` from configuration settings.
pub fn build_wasi(
    args: Option<&Vec<String>>,
    envs: Option<&Vec<(String, String)>>,
    dirs: &[(String, String)],
) -> Result<WasiCtx> {
    let mut wasi = WasiCtxBuilder::new();
    wasi.inherit_stdio();
    if let Some(envs) = envs {
        wasi.envs(envs)?;
    }
    if let Some(args) = args {
        wasi.args(args)?;
    }
    for (preopen_dir_path, resolved_path) in dirs {
        let preopen_dir = Dir::open_ambient_dir(resolved_path, ambient_authority())?;
        wasi.preopened_dir(preopen_dir, preopen_dir_path)?;
    }
    Ok(wasi.build())
}

pub trait LunaticWasiConfigCtx {
    fn add_environment_variable(&mut self, key: String, value: String);
    fn add_command_line_argument(&mut self, argument: String);
    fn preopen_dir(&mut self, dir: String);
}

pub trait LunaticWasiCtx {
    fn wasi(&self) -> &WasiCtx;
    fn wasi_mut(&mut self) -> &mut WasiCtx;
    fn set_stdout(&mut self, stdout: StdoutCapture);
    fn get_stdout(&self) -> Option<&StdoutCapture>;
    fn set_stderr(&mut self, stderr: StdoutCapture);
    fn get_stderr(&self) -> Option<&StdoutCapture>;
}

// Register WASI APIs to the linker
pub fn register<T>(linker: &mut Linker<T>) -> Result<()>
where
    T: ProcessState + LunaticWasiCtx + Send + 'static,
    T::Config: LunaticWasiConfigCtx,
{
    // Register all wasi host functions
    wasi_common::sync::snapshots::preview_1::add_wasi_snapshot_preview1_to_linker(linker, |ctx| {
        ctx.wasi_mut()
    })?;

    // Register host functions to configure wasi
    linker.func_wrap(
        "lunatic::wasi",
        "config_add_environment_variable",
        |caller: Caller<'_, T>,
         config_id: u64,
         key_ptr: u32,
         key_len: u32,
         value_ptr: u32,
         value_len: u32| {
            add_environment_variable(caller, config_id, key_ptr, key_len, value_ptr, value_len)
                .to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::wasi",
        "config_add_command_line_argument",
        |caller: Caller<'_, T>, config_id: u64, argument_ptr: u32, argument_len: u32| {
            add_command_line_argument(caller, config_id, argument_ptr, argument_len)
                .to_wasmtime_result()
        },
    )?;
    linker.func_wrap("lunatic::wasi", "config_preopen_dir", preopen_dir)?;
    Ok(())
}

/// Registers the checked WASI configuration imports that return guest-visible
/// error resources. Kept separate so existing embedders using [`register`]
/// are not required to add [`ErrorCtx`] merely for the legacy WASI surface.
pub fn register_checked<T>(linker: &mut Linker<T>) -> Result<()>
where
    T: ProcessState + ErrorCtx + 'static,
    T::Config: LunaticWasiConfigCtx,
{
    linker.func_wrap(
        "lunatic::wasi",
        "config_preopen_dir_checked",
        preopen_dir_checked,
    )?;
    Ok(())
}

// Adds environment variable to a configuration.
//
// Traps:
// * If the config ID doesn't exist.
// * If the key or value string is not a valid utf8 string.
// * If any of the memory slices falls outside the memory.
fn add_environment_variable<T>(
    mut caller: Caller<T>,
    config_id: u64,
    key_ptr: u32,
    key_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> Result<()>
where
    T: ProcessState,
    T::Config: LunaticWasiConfigCtx,
{
    let memory = get_memory(&mut caller)?;
    let key_str = memory
        .data(&caller)
        .get(key_ptr as usize..(key_ptr + key_len) as usize)
        .or_trap("lunatic::wasi::config_add_environment_variable")?;
    let key = std::str::from_utf8(key_str)
        .or_trap("lunatic::wasi::config_add_environment_variable")?
        .to_string();
    let value_str = memory
        .data(&caller)
        .get(value_ptr as usize..(value_ptr + value_len) as usize)
        .or_trap("lunatic::wasi::config_add_environment_variable")?;
    let value = std::str::from_utf8(value_str)
        .or_trap("lunatic::wasi::config_add_environment_variable")?
        .to_string();

    caller
        .data_mut()
        .config_resources_mut()
        .get_mut(config_id)
        .or_trap("lunatic::wasi::config_set_max_memory: Config ID doesn't exist")?
        .add_environment_variable(key, value);
    Ok(())
}

// Adds command line argument to a configuration.
//
// Traps:
// * If the config ID doesn't exist.
// * If the argument string is not a valid utf8 string.
// * If any of the memory slices falls outside the memory.
fn add_command_line_argument<T>(
    mut caller: Caller<T>,
    config_id: u64,
    argument_ptr: u32,
    argument_len: u32,
) -> Result<()>
where
    T: ProcessState,
    T::Config: LunaticWasiConfigCtx,
{
    let memory = get_memory(&mut caller)?;
    let argument_str = memory
        .data(&caller)
        .get(argument_ptr as usize..(argument_ptr + argument_len) as usize)
        .or_trap("lunatic::wasi::add_command_line_argument")?;
    let argument = std::str::from_utf8(argument_str)
        .or_trap("lunatic::wasi::add_command_line_argument")?
        .to_string();

    caller
        .data_mut()
        .config_resources_mut()
        .get_mut(config_id)
        .or_trap("lunatic::wasi::add_command_line_argument: Config ID doesn't exist")?
        .add_command_line_argument(argument);
    Ok(())
}

// Mark a directory as preopened in the configuration.
//
// The legacy void ABI keeps denied mutations as audited no-ops. New guests
// should call `config_preopen_dir_checked`, which returns -1 on success or a
// guest-readable error-resource ID without crossing a host trap.
fn preopen_dir<T>(mut caller: Caller<T>, config_id: u64, dir_ptr: u32, dir_len: u32)
where
    T: ProcessState,
    T::Config: LunaticWasiConfigCtx,
{
    let _ = preopen_dir_impl(&mut caller, config_id, dir_ptr, dir_len);
}

fn preopen_dir_checked<T>(mut caller: Caller<T>, config_id: u64, dir_ptr: u32, dir_len: u32) -> i64
where
    T: ProcessState + ErrorCtx,
    T::Config: LunaticWasiConfigCtx,
{
    match preopen_dir_impl(&mut caller, config_id, dir_ptr, dir_len) {
        Ok(()) => -1,
        Err(error) => caller.data_mut().add_error_resource(error) as i64,
    }
}

fn preopen_dir_impl<T>(
    caller: &mut Caller<T>,
    config_id: u64,
    dir_ptr: u32,
    dir_len: u32,
) -> Result<()>
where
    T: ProcessState,
    T::Config: LunaticWasiConfigCtx,
{
    let memory = get_memory(caller)?;
    let dir_str = memory
        .data(&*caller)
        .get(dir_ptr as usize..(dir_ptr + dir_len) as usize)
        .or_trap("lunatic::wasi::preopen_dir")?;
    let dir = std::str::from_utf8(dir_str)
        .or_trap("lunatic::wasi::preopen_dir")?
        .to_string();

    let parent_process = caller.data().id();
    let parent_config = caller.data().config().clone();
    let mut candidate = caller
        .data()
        .config_resources()
        .get(config_id)
        .or_trap("lunatic::wasi::preopen_dir: Config ID doesn't exist")?
        .clone();
    candidate.preopen_dir(dir);

    if let Err(reason) = parent_config.validate_child_config(&candidate) {
        audit_log(
            "capability_delegation",
            format!(
                "parent_process={parent_process} config_id={config_id} operation=config_preopen_dir outcome=denied"
            ),
        );
        return Err(anyhow!(
            "lunatic::wasi::config_preopen_dir: delegation denied: {reason}"
        ));
    }

    *caller
        .data_mut()
        .config_resources_mut()
        .get_mut(config_id)
        .or_trap("lunatic::wasi::preopen_dir: Config ID doesn't exist")? = candidate;
    audit_log(
        "capability_delegation",
        format!(
            "parent_process={parent_process} config_id={config_id} operation=config_preopen_dir outcome=allowed"
        ),
    );
    Ok(())
}
