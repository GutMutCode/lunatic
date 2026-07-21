use std::{any::Any, ops::Range, path::PathBuf};

use anyhow::{anyhow, Result};
use lunatic_common_api::{
    emit_audit_event, get_memory, AuditAction, AuditEvent, AuditEventV1, AuditReason, AuditResult,
    AuditSubject, AuditTarget, AuditTargetKind, IntoTrap, SensitiveData,
};
use lunatic_error_api::ErrorCtx;
use lunatic_process::{config::ProcessConfig, state::ProcessState};
use lunatic_stdout_capture::StdoutCapture;
use wasi_common::{
    dir::{OpenResult, ReaddirCursor, ReaddirEntity, WasiDir},
    file::{FdFlags, Filestat, OFlags},
    sync::{ambient_authority, dir::Dir as SyncDir, Dir, WasiCtxBuilder},
    Error, SystemTimeSpec, WasiCtx,
};
use wasmtime::{Caller, Linker, ToWasmtimeResult as _};

/// Create a `WasiCtx` from configuration settings.
pub fn build_wasi(
    args: Option<&Vec<String>>,
    envs: Option<&Vec<(String, String)>>,
    dirs: &[(String, String)],
) -> Result<WasiCtx> {
    build_wasi_with_audit(args, envs, dirs, AuditSubject::new())
}

/// Creates a WASI context whose preopened directories emit typed audit
/// events. Host and guest paths are used only for the delegated operation and
/// can never enter the event schema.
pub fn build_wasi_with_audit(
    args: Option<&Vec<String>>,
    envs: Option<&Vec<(String, String)>>,
    dirs: &[(String, String)],
    subject: AuditSubject,
) -> Result<WasiCtx> {
    let mut wasi = WasiCtxBuilder::new();
    wasi.inherit_stdio();
    if let Some(envs) = envs {
        wasi.envs(envs)?;
    }
    if let Some(args) = args {
        wasi.args(args)?;
    }
    let wasi = wasi.build();
    for (preopen_dir_path, resolved_path) in dirs {
        let preopen_dir = Dir::open_ambient_dir(resolved_path, ambient_authority())?;
        let preopen_dir = SyncDir::from_cap_std(preopen_dir);
        wasi.push_preopened_dir(
            Box::new(AuditedWasiDir::new(Box::new(preopen_dir), subject)),
            preopen_dir_path,
        )?;
    }
    Ok(wasi)
}

struct AuditedWasiDir {
    inner: Box<dyn WasiDir>,
    subject: AuditSubject,
}

impl AuditedWasiDir {
    fn new(inner: Box<dyn WasiDir>, subject: AuditSubject) -> Self {
        Self { inner, subject }
    }

    fn target() -> AuditTarget {
        AuditTarget::new(AuditTargetKind::Filesystem).with_sensitive_data(SensitiveData::Redacted)
    }

    fn unwrap(dir: &dyn WasiDir) -> &dyn WasiDir {
        dir.as_any()
            .downcast_ref::<Self>()
            .map_or(dir, |audited| audited.inner.as_ref())
    }

    fn wrap_open_result(&self, result: OpenResult) -> OpenResult {
        match result {
            OpenResult::File(file) => OpenResult::File(file),
            OpenResult::Dir(dir) => OpenResult::Dir(Box::new(Self::new(dir, self.subject))),
        }
    }
}

struct PendingWasiAudit {
    subject: AuditSubject,
    action: AuditAction,
    emitted: bool,
}

impl PendingWasiAudit {
    fn new(subject: AuditSubject, action: AuditAction) -> Self {
        Self {
            subject,
            action,
            emitted: false,
        }
    }

    fn finish<T>(mut self, result: &std::result::Result<T, Error>) {
        let (audit_result, reason) = if result.is_ok() {
            (AuditResult::Succeeded, AuditReason::Completed)
        } else {
            (AuditResult::Failed, AuditReason::IoError)
        };
        self.emit(audit_result, reason);
    }

    fn emit(&mut self, result: AuditResult, reason: AuditReason) {
        let _ = emit_audit_event(AuditEventV1::new(
            AuditEvent::FilesystemAccess,
            self.action,
            result,
            reason,
            self.subject,
            AuditedWasiDir::target(),
        ));
        self.emitted = true;
    }
}

impl Drop for PendingWasiAudit {
    fn drop(&mut self) {
        if !self.emitted {
            self.emit(AuditResult::Cancelled, AuditReason::Cancelled);
        }
    }
}

#[async_trait::async_trait]
impl WasiDir for AuditedWasiDir {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn open_file(
        &self,
        symlink_follow: bool,
        path: &str,
        oflags: OFlags,
        read: bool,
        write: bool,
        fdflags: FdFlags,
    ) -> std::result::Result<OpenResult, Error> {
        let audit = PendingWasiAudit::new(self.subject, AuditAction::Open);
        let result = self
            .inner
            .open_file(symlink_follow, path, oflags, read, write, fdflags)
            .await
            .map(|opened| self.wrap_open_result(opened));
        audit.finish(&result);
        result
    }

    async fn create_dir(&self, path: &str) -> std::result::Result<(), Error> {
        let audit = PendingWasiAudit::new(self.subject, AuditAction::Create);
        let result = self.inner.create_dir(path).await;
        audit.finish(&result);
        result
    }

    async fn readdir(
        &self,
        cursor: ReaddirCursor,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = std::result::Result<ReaddirEntity, Error>> + Send>,
        Error,
    > {
        let audit = PendingWasiAudit::new(self.subject, AuditAction::Read);
        let result = self.inner.readdir(cursor).await;
        audit.finish(&result);
        result
    }

    async fn symlink(&self, old_path: &str, new_path: &str) -> std::result::Result<(), Error> {
        let audit = PendingWasiAudit::new(self.subject, AuditAction::Link);
        let result = self.inner.symlink(old_path, new_path).await;
        audit.finish(&result);
        result
    }

    async fn remove_dir(&self, path: &str) -> std::result::Result<(), Error> {
        let audit = PendingWasiAudit::new(self.subject, AuditAction::Remove);
        let result = self.inner.remove_dir(path).await;
        audit.finish(&result);
        result
    }

    async fn unlink_file(&self, path: &str) -> std::result::Result<(), Error> {
        let audit = PendingWasiAudit::new(self.subject, AuditAction::Remove);
        let result = self.inner.unlink_file(path).await;
        audit.finish(&result);
        result
    }

    async fn read_link(&self, path: &str) -> std::result::Result<PathBuf, Error> {
        let audit = PendingWasiAudit::new(self.subject, AuditAction::Read);
        let result = self.inner.read_link(path).await;
        audit.finish(&result);
        result
    }

    async fn get_filestat(&self) -> std::result::Result<Filestat, Error> {
        let audit = PendingWasiAudit::new(self.subject, AuditAction::Read);
        let result = self.inner.get_filestat().await;
        audit.finish(&result);
        result
    }

    async fn get_path_filestat(
        &self,
        path: &str,
        follow_symlinks: bool,
    ) -> std::result::Result<Filestat, Error> {
        let audit = PendingWasiAudit::new(self.subject, AuditAction::Read);
        let result = self.inner.get_path_filestat(path, follow_symlinks).await;
        audit.finish(&result);
        result
    }

    async fn rename(
        &self,
        path: &str,
        dest_dir: &dyn WasiDir,
        dest_path: &str,
    ) -> std::result::Result<(), Error> {
        let audit = PendingWasiAudit::new(self.subject, AuditAction::Rename);
        let result = self
            .inner
            .rename(path, Self::unwrap(dest_dir), dest_path)
            .await;
        audit.finish(&result);
        result
    }

    async fn hard_link(
        &self,
        path: &str,
        target_dir: &dyn WasiDir,
        target_path: &str,
    ) -> std::result::Result<(), Error> {
        let audit = PendingWasiAudit::new(self.subject, AuditAction::Link);
        let result = self
            .inner
            .hard_link(path, Self::unwrap(target_dir), target_path)
            .await;
        audit.finish(&result);
        result
    }

    async fn set_times(
        &self,
        path: &str,
        atime: Option<SystemTimeSpec>,
        mtime: Option<SystemTimeSpec>,
        follow_symlinks: bool,
    ) -> std::result::Result<(), Error> {
        let audit = PendingWasiAudit::new(self.subject, AuditAction::SetTimes);
        let result = self
            .inner
            .set_times(path, atime, mtime, follow_symlinks)
            .await;
        audit.finish(&result);
        result
    }
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use std::{
        fs, io,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use lunatic_common_api::{
        flush_audit, install_global_audit_dispatcher, AuditConfig, AuditDispatcher,
        AuditFlushOutcome, AuditSink,
    };

    use super::*;

    struct RecordingSink(Arc<Mutex<Vec<String>>>);

    impl AuditSink for RecordingSink {
        fn write(&mut self, event: &AuditEventV1) -> io::Result<()> {
            self.0
                .lock()
                .unwrap()
                .push(serde_json::to_string(event).map_err(io::Error::other)?);
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn audited_directory_records_identity_without_collecting_paths() -> Result<()> {
        let records = Arc::new(Mutex::new(Vec::new()));
        let dispatcher =
            AuditDispatcher::new(AuditConfig::default(), RecordingSink(Arc::clone(&records)));
        assert!(install_global_audit_dispatcher(dispatcher).is_ok());

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "lunatic-wasi-audit-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&root)?;
        let cap_dir = Dir::open_ambient_dir(&root, ambient_authority())?;
        let audited = AuditedWasiDir::new(
            Box::new(SyncDir::from_cap_std(cap_dir)),
            AuditSubject::new()
                .with_environment_id(41)
                .with_process_id(99),
        );
        let sentinel = "secret-customer-path.txt";
        let opened = audited
            .open_file(
                false,
                sentinel,
                OFlags::CREATE,
                true,
                true,
                FdFlags::empty(),
            )
            .await?;
        drop(opened);

        assert_eq!(
            flush_audit(Duration::from_secs(2)),
            AuditFlushOutcome::Flushed
        );
        let events = records.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(!events[0].contains(sentinel));
        let event: serde_json::Value = serde_json::from_str(&events[0])?;
        assert_eq!(event["event"], "filesystem_access");
        assert_eq!(event["action"], "open");
        assert_eq!(event["result"], "succeeded");
        assert_eq!(event["subject"]["environment_id"], 41);
        assert_eq!(event["subject"]["process_id"], 99);
        assert_eq!(event["target"]["sensitive_data"], "redacted");
        drop(events);

        drop(PendingWasiAudit::new(
            AuditSubject::new()
                .with_environment_id(41)
                .with_process_id(99),
            AuditAction::Remove,
        ));
        assert_eq!(
            flush_audit(Duration::from_secs(2)),
            AuditFlushOutcome::Flushed
        );
        let events = records.lock().unwrap();
        assert_eq!(events.len(), 2);
        let cancelled: serde_json::Value = serde_json::from_str(&events[1])?;
        assert_eq!(cancelled["result"], "cancelled");
        assert_eq!(cancelled["reason"], "cancelled");
        drop(events);
        fs::remove_dir_all(root)?;
        Ok(())
    }
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

fn process_audit_subject<T: ProcessState>(state: &T) -> AuditSubject {
    let mut subject = AuditSubject::new().with_process_id(state.id());
    if let Some(node_id) = state.audit_node_id() {
        subject = subject.with_node_id(node_id);
    }
    if let Some(environment_id) = state.audit_environment_id() {
        subject = subject.with_environment_id(environment_id);
    }
    subject
}

fn audit_config_update<T: ProcessState>(state: &T, config_id: u64, operation: &Result<()>) {
    let (result, reason) = if operation.is_ok() {
        (AuditResult::Allowed, AuditReason::PolicyAllowed)
    } else {
        (AuditResult::Failed, AuditReason::InvalidInput)
    };
    let _ = emit_audit_event(AuditEventV1::new(
        AuditEvent::ConfigUpdate,
        AuditAction::Mutate,
        result,
        reason,
        process_audit_subject(state),
        AuditTarget::new(AuditTargetKind::Configuration)
            .with_resource_id(config_id)
            .with_sensitive_data(SensitiveData::Redacted),
    ));
}

fn checked_guest_range(pointer: u32, length: u32, operation: &'static str) -> Result<Range<usize>> {
    let end = pointer
        .checked_add(length)
        .ok_or_else(|| anyhow!("{operation}: guest pointer overflow"))?;
    Ok(pointer as usize..end as usize)
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
    let operation = (|| -> Result<()> {
        let memory = get_memory(&mut caller)?;
        let key_str = memory
            .data(&caller)
            .get(checked_guest_range(
                key_ptr,
                key_len,
                "lunatic::wasi::config_add_environment_variable",
            )?)
            .or_trap("lunatic::wasi::config_add_environment_variable")?;
        let key = std::str::from_utf8(key_str)
            .or_trap("lunatic::wasi::config_add_environment_variable")?
            .to_string();
        let value_str = memory
            .data(&caller)
            .get(checked_guest_range(
                value_ptr,
                value_len,
                "lunatic::wasi::config_add_environment_variable",
            )?)
            .or_trap("lunatic::wasi::config_add_environment_variable")?;
        let value = std::str::from_utf8(value_str)
            .or_trap("lunatic::wasi::config_add_environment_variable")?
            .to_string();

        caller
            .data_mut()
            .config_resources_mut()
            .get_mut(config_id)
            .or_trap("lunatic::wasi::config_add_environment_variable: Config ID doesn't exist")?
            .add_environment_variable(key, value);
        Ok(())
    })();
    audit_config_update(caller.data(), config_id, &operation);
    operation
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
    let operation = (|| -> Result<()> {
        let memory = get_memory(&mut caller)?;
        let argument_str = memory
            .data(&caller)
            .get(checked_guest_range(
                argument_ptr,
                argument_len,
                "lunatic::wasi::add_command_line_argument",
            )?)
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
    })();
    audit_config_update(caller.data(), config_id, &operation);
    operation
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
    enum Decision {
        Allowed,
        Denied(anyhow::Error),
    }

    let subject = process_audit_subject(caller.data());
    let target = AuditTarget::new(AuditTargetKind::Filesystem)
        .with_resource_id(config_id)
        .with_sensitive_data(SensitiveData::Redacted);

    let operation = (|| -> Result<Decision> {
        let memory = get_memory(caller)?;
        let dir_str = memory
            .data(&*caller)
            .get(checked_guest_range(
                dir_ptr,
                dir_len,
                "lunatic::wasi::preopen_dir",
            )?)
            .or_trap("lunatic::wasi::preopen_dir")?;
        let dir = std::str::from_utf8(dir_str)
            .or_trap("lunatic::wasi::preopen_dir")?
            .to_string();

        let parent_config = caller.data().config().clone();
        let mut candidate = caller
            .data()
            .config_resources()
            .get(config_id)
            .or_trap("lunatic::wasi::preopen_dir: Config ID doesn't exist")?
            .clone();
        candidate.preopen_dir(dir);

        if let Err(reason) = parent_config.validate_child_config(&candidate) {
            return Ok(Decision::Denied(anyhow!(
                "lunatic::wasi::config_preopen_dir: delegation denied: {reason}"
            )));
        }

        *caller
            .data_mut()
            .config_resources_mut()
            .get_mut(config_id)
            .or_trap("lunatic::wasi::preopen_dir: Config ID doesn't exist")? = candidate;
        Ok(Decision::Allowed)
    })();

    let (result, reason, return_value) = match operation {
        Ok(Decision::Allowed) => (AuditResult::Allowed, AuditReason::PolicyAllowed, Ok(())),
        Ok(Decision::Denied(error)) => (
            AuditResult::Denied,
            AuditReason::DelegationExceedsParent,
            Err(error),
        ),
        Err(error) => (AuditResult::Failed, AuditReason::InvalidInput, Err(error)),
    };
    let _ = emit_audit_event(AuditEventV1::new(
        AuditEvent::FilesystemPreopen,
        AuditAction::Preopen,
        result,
        reason,
        subject,
        target,
    ));
    return_value
}
