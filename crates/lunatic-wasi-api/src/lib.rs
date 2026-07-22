use std::{
    any::Any,
    io::{IoSlice, IoSliceMut, SeekFrom},
    ops::Range,
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

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
    file::{Advice, FdFlags, FileType, Filestat, OFlags, RiFlags, RoFlags, SdFlags, SiFlags},
    snapshots::preview_1::types::Errno,
    sync::{ambient_authority, dir::Dir as SyncDir, Dir, WasiCtxBuilder},
    Error, SystemTimeSpec, WasiCtx, WasiFile,
};
use wasmtime::{Caller, Extern, Linker, ToWasmtimeResult as _};

pub const DEFAULT_WASI_FILE_DESCRIPTOR_LIMIT: u32 = 1024;

/// Shared accounting boundary for every non-stdio descriptor retained by a
/// WASI context. Implementations must reserve and release exactly one unit for
/// each successful lease.
pub trait WasiFileDescriptorQuota: Send + Sync {
    fn reserve(&self) -> Result<()>;
    fn release(&self) -> Result<()>;
}

#[derive(Debug)]
struct LocalWasiFileDescriptorQuota {
    open: AtomicU32,
    max: u32,
}

impl LocalWasiFileDescriptorQuota {
    fn new(max: u32) -> Self {
        Self {
            open: AtomicU32::new(0),
            max,
        }
    }
}

impl WasiFileDescriptorQuota for LocalWasiFileDescriptorQuota {
    fn reserve(&self) -> Result<()> {
        self.open
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |open| {
                (open < self.max).then_some(open + 1)
            })
            .map(|_| ())
            .map_err(|_| anyhow!("Max WASI file descriptors ({}) reached", self.max))
    }

    fn release(&self) -> Result<()> {
        self.open
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |open| {
                open.checked_sub(1)
            })
            .map(|_| ())
            .map_err(|_| anyhow!("WASI file descriptor accounting underflow"))
    }
}

struct WasiFileDescriptorLease {
    quota: Arc<dyn WasiFileDescriptorQuota>,
}

impl WasiFileDescriptorLease {
    fn acquire(quota: Arc<dyn WasiFileDescriptorQuota>) -> Result<Self> {
        quota.reserve()?;
        Ok(Self { quota })
    }

    fn acquire_for_guest(
        quota: Arc<dyn WasiFileDescriptorQuota>,
    ) -> std::result::Result<Self, Error> {
        quota.reserve().map_err(|_| Error::from(Errno::Mfile))?;
        Ok(Self { quota })
    }
}

impl Drop for WasiFileDescriptorLease {
    fn drop(&mut self) {
        let result = self.quota.release();
        debug_assert!(result.is_ok());
    }
}

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
    build_wasi_with_audit_and_quota(
        args,
        envs,
        dirs,
        subject,
        Arc::new(LocalWasiFileDescriptorQuota::new(
            DEFAULT_WASI_FILE_DESCRIPTOR_LIMIT,
        )),
    )
}

/// Creates a WASI context tied to the process-wide descriptor accounting
/// shared with Lunatic networking handles.
pub fn build_wasi_with_audit_and_quota(
    args: Option<&Vec<String>>,
    envs: Option<&Vec<(String, String)>>,
    dirs: &[(String, String)],
    subject: AuditSubject,
    quota: Arc<dyn WasiFileDescriptorQuota>,
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
        let lease = WasiFileDescriptorLease::acquire(Arc::clone(&quota))?;
        let preopen_dir = Dir::open_ambient_dir(resolved_path, ambient_authority())?;
        let preopen_dir = SyncDir::from_cap_std(preopen_dir);
        wasi.push_preopened_dir(
            Box::new(AuditedWasiDir::with_lease(
                Box::new(preopen_dir),
                subject,
                Arc::clone(&quota),
                lease,
            )),
            preopen_dir_path,
        )?;
    }
    Ok(wasi)
}

struct AuditedWasiDir {
    inner: Box<dyn WasiDir>,
    subject: AuditSubject,
    quota: Arc<dyn WasiFileDescriptorQuota>,
    _lease: WasiFileDescriptorLease,
}

impl AuditedWasiDir {
    #[cfg(test)]
    fn new(inner: Box<dyn WasiDir>, subject: AuditSubject) -> Self {
        let quota: Arc<dyn WasiFileDescriptorQuota> = Arc::new(LocalWasiFileDescriptorQuota::new(
            DEFAULT_WASI_FILE_DESCRIPTOR_LIMIT,
        ));
        let lease = WasiFileDescriptorLease::acquire(Arc::clone(&quota))
            .expect("default WASI descriptor quota must have capacity");
        Self::with_lease(inner, subject, quota, lease)
    }

    fn with_lease(
        inner: Box<dyn WasiDir>,
        subject: AuditSubject,
        quota: Arc<dyn WasiFileDescriptorQuota>,
        lease: WasiFileDescriptorLease,
    ) -> Self {
        Self {
            inner,
            subject,
            quota,
            _lease: lease,
        }
    }

    fn target() -> AuditTarget {
        AuditTarget::new(AuditTargetKind::Filesystem).with_sensitive_data(SensitiveData::Redacted)
    }

    fn unwrap(dir: &dyn WasiDir) -> &dyn WasiDir {
        dir.as_any()
            .downcast_ref::<Self>()
            .map_or(dir, |audited| audited.inner.as_ref())
    }

    fn wrap_open_result(&self, result: OpenResult, lease: WasiFileDescriptorLease) -> OpenResult {
        match result {
            OpenResult::File(file) => OpenResult::File(Box::new(QuotaWasiFile::new(
                file,
                Arc::clone(&self.quota),
                lease,
            ))),
            OpenResult::Dir(dir) => OpenResult::Dir(Box::new(Self::with_lease(
                dir,
                self.subject,
                Arc::clone(&self.quota),
                lease,
            ))),
        }
    }
}

struct QuotaWasiFile {
    inner: Box<dyn WasiFile>,
    quota: Arc<dyn WasiFileDescriptorQuota>,
    _lease: WasiFileDescriptorLease,
}

impl QuotaWasiFile {
    fn new(
        inner: Box<dyn WasiFile>,
        quota: Arc<dyn WasiFileDescriptorQuota>,
        lease: WasiFileDescriptorLease,
    ) -> Self {
        Self {
            inner,
            quota,
            _lease: lease,
        }
    }
}

#[async_trait::async_trait]
impl WasiFile for QuotaWasiFile {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn get_filetype(&self) -> std::result::Result<FileType, Error> {
        self.inner.get_filetype().await
    }

    #[cfg(unix)]
    fn pollable(&self) -> Option<rustix::fd::BorrowedFd<'_>> {
        self.inner.pollable()
    }

    #[cfg(windows)]
    fn pollable(&self) -> Option<io_extras::os::windows::RawHandleOrSocket> {
        self.inner.pollable()
    }

    fn isatty(&self) -> bool {
        self.inner.isatty()
    }

    async fn sock_accept(&self, fdflags: FdFlags) -> std::result::Result<Box<dyn WasiFile>, Error> {
        let lease = WasiFileDescriptorLease::acquire_for_guest(Arc::clone(&self.quota))?;
        let accepted = self.inner.sock_accept(fdflags).await?;
        Ok(Box::new(Self::new(
            accepted,
            Arc::clone(&self.quota),
            lease,
        )))
    }

    async fn sock_recv<'a>(
        &self,
        data: &mut [IoSliceMut<'a>],
        flags: RiFlags,
    ) -> std::result::Result<(u64, RoFlags), Error> {
        self.inner.sock_recv(data, flags).await
    }

    async fn sock_send<'a>(
        &self,
        data: &[IoSlice<'a>],
        flags: SiFlags,
    ) -> std::result::Result<u64, Error> {
        self.inner.sock_send(data, flags).await
    }

    async fn sock_shutdown(&self, how: SdFlags) -> std::result::Result<(), Error> {
        self.inner.sock_shutdown(how).await
    }

    async fn datasync(&self) -> std::result::Result<(), Error> {
        self.inner.datasync().await
    }

    async fn sync(&self) -> std::result::Result<(), Error> {
        self.inner.sync().await
    }

    async fn get_fdflags(&self) -> std::result::Result<FdFlags, Error> {
        self.inner.get_fdflags().await
    }

    async fn set_fdflags(&mut self, flags: FdFlags) -> std::result::Result<(), Error> {
        self.inner.set_fdflags(flags).await
    }

    async fn get_filestat(&self) -> std::result::Result<Filestat, Error> {
        self.inner.get_filestat().await
    }

    async fn set_filestat_size(&self, size: u64) -> std::result::Result<(), Error> {
        self.inner.set_filestat_size(size).await
    }

    async fn advise(
        &self,
        offset: u64,
        len: u64,
        advice: Advice,
    ) -> std::result::Result<(), Error> {
        self.inner.advise(offset, len, advice).await
    }

    async fn set_times(
        &self,
        atime: Option<SystemTimeSpec>,
        mtime: Option<SystemTimeSpec>,
    ) -> std::result::Result<(), Error> {
        self.inner.set_times(atime, mtime).await
    }

    async fn read_vectored<'a>(
        &self,
        bufs: &mut [IoSliceMut<'a>],
    ) -> std::result::Result<u64, Error> {
        self.inner.read_vectored(bufs).await
    }

    async fn read_vectored_at<'a>(
        &self,
        bufs: &mut [IoSliceMut<'a>],
        offset: u64,
    ) -> std::result::Result<u64, Error> {
        self.inner.read_vectored_at(bufs, offset).await
    }

    async fn write_vectored<'a>(&self, bufs: &[IoSlice<'a>]) -> std::result::Result<u64, Error> {
        self.inner.write_vectored(bufs).await
    }

    async fn write_vectored_at<'a>(
        &self,
        bufs: &[IoSlice<'a>],
        offset: u64,
    ) -> std::result::Result<u64, Error> {
        self.inner.write_vectored_at(bufs, offset).await
    }

    async fn seek(&self, pos: SeekFrom) -> std::result::Result<u64, Error> {
        self.inner.seek(pos).await
    }

    async fn peek(&self, buf: &mut [u8]) -> std::result::Result<u64, Error> {
        self.inner.peek(buf).await
    }

    fn num_ready_bytes(&self) -> std::result::Result<u64, Error> {
        self.inner.num_ready_bytes()
    }

    async fn readable(&self) -> std::result::Result<(), Error> {
        self.inner.readable().await
    }

    async fn writable(&self) -> std::result::Result<(), Error> {
        self.inner.writable().await
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
        let lease = match WasiFileDescriptorLease::acquire_for_guest(Arc::clone(&self.quota)) {
            Ok(lease) => lease,
            Err(error) => {
                let result = Err(error);
                audit.finish(&result);
                return result;
            }
        };
        let result = self
            .inner
            .open_file(symlink_follow, path, oflags, read, write, fdflags)
            .await
            .map(|opened| self.wrap_open_result(opened, lease));
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
        sync::{Arc, Mutex, OnceLock},
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

    fn shared_audit_records() -> Arc<Mutex<Vec<String>>> {
        static RECORDS: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();
        RECORDS
            .get_or_init(|| {
                let records = Arc::new(Mutex::new(Vec::new()));
                let dispatcher = AuditDispatcher::new(
                    AuditConfig::default(),
                    RecordingSink(Arc::clone(&records)),
                );
                install_global_audit_dispatcher(dispatcher)
                    .expect("test audit dispatcher is installed exactly once");
                records
            })
            .clone()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn audited_directory_records_identity_without_collecting_paths() -> Result<()> {
        let records = shared_audit_records();

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
        let matching = events
            .iter()
            .map(|event| serde_json::from_str::<serde_json::Value>(event))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|event| {
                event["subject"]["environment_id"] == 41 && event["subject"]["process_id"] == 99
            })
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        assert!(!events.iter().any(|event| event.contains(sentinel)));
        let event = &matching[0];
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
        let matching = events
            .iter()
            .map(|event| serde_json::from_str::<serde_json::Value>(event))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|event| {
                event["subject"]["environment_id"] == 41 && event["subject"]["process_id"] == 99
            })
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 2);
        let cancelled = &matching[1];
        assert_eq!(cancelled["result"], "cancelled");
        assert_eq!(cancelled["reason"], "cancelled");
        drop(events);
        // Windows does not allow removing a directory while the capability
        // directory still owns an open handle to it.
        drop(audited);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn descriptor_leases_reject_at_boundary_and_reuse_released_slots() -> Result<()> {
        let quota: Arc<dyn WasiFileDescriptorQuota> =
            Arc::new(LocalWasiFileDescriptorQuota::new(1));
        let lease = WasiFileDescriptorLease::acquire(Arc::clone(&quota))?;
        assert!(WasiFileDescriptorLease::acquire(Arc::clone(&quota)).is_err());
        drop(lease);
        let replacement = WasiFileDescriptorLease::acquire(quota)?;
        drop(replacement);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_and_closed_opens_release_descriptor_quota() -> Result<()> {
        let _records = shared_audit_records();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "lunatic-wasi-quota-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&root)?;

        let quota = Arc::new(LocalWasiFileDescriptorQuota::new(2));
        let root_lease = WasiFileDescriptorLease::acquire(quota.clone())?;
        let cap_dir = Dir::open_ambient_dir(&root, ambient_authority())?;
        let audited = AuditedWasiDir::with_lease(
            Box::new(SyncDir::from_cap_std(cap_dir)),
            AuditSubject::new(),
            quota.clone(),
            root_lease,
        );

        assert!(audited
            .open_file(
                false,
                "missing",
                OFlags::empty(),
                true,
                false,
                FdFlags::empty(),
            )
            .await
            .is_err());
        let first = audited
            .open_file(false, "first", OFlags::CREATE, true, true, FdFlags::empty())
            .await?;
        assert!(audited
            .open_file(
                false,
                "second",
                OFlags::CREATE,
                true,
                true,
                FdFlags::empty(),
            )
            .await
            .is_err());
        drop(first);
        let replacement = audited
            .open_file(
                false,
                "second",
                OFlags::CREATE,
                true,
                true,
                FdFlags::empty(),
            )
            .await?;
        drop(replacement);
        drop(audited);
        assert_eq!(quota.open.load(Ordering::Acquire), 0);

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn partial_preopen_build_rolls_back_all_descriptor_leases() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "lunatic-wasi-preopen-quota-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&root)?;
        let dirs = vec![
            ("/one".to_string(), root.to_string_lossy().into_owned()),
            ("/two".to_string(), root.to_string_lossy().into_owned()),
        ];
        let quota = Arc::new(LocalWasiFileDescriptorQuota::new(1));

        assert!(build_wasi_with_audit_and_quota(
            None,
            None,
            &dirs,
            AuditSubject::new(),
            quota.clone(),
        )
        .is_err());
        assert_eq!(quota.open.load(Ordering::Acquire), 0);

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn wasi_descriptor_result_pointer_is_preflighted() {
        assert!(validate_wasi_fd_result_pointer(16, 4, "test").is_ok());
        assert!(validate_wasi_fd_result_pointer(16, 3, "test").is_err());
        assert!(validate_wasi_fd_result_pointer(16, 16, "test").is_err());
        assert!(validate_wasi_fd_result_pointer(16, -1, "test").is_err());
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

fn validate_wasi_fd_result_pointer(
    memory_len: usize,
    pointer: i32,
    operation: &'static str,
) -> Result<()> {
    let pointer = pointer as u32;
    anyhow::ensure!(
        pointer.is_multiple_of(std::mem::align_of::<u32>() as u32),
        "{operation}: unaligned guest result pointer"
    );
    let range = checked_guest_range(pointer, std::mem::size_of::<u32>() as u32, operation)?;
    anyhow::ensure!(
        range.end <= memory_len,
        "{operation}: guest result pointer is out of bounds"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn wasi_path_open_preflight<T>(
    mut caller: Caller<'_, T>,
    fd: i32,
    dirflags: i32,
    path_ptr: i32,
    path_len: i32,
    oflags: i32,
    rights_base: i64,
    rights_inheriting: i64,
    fdflags: i32,
    result_ptr: i32,
) -> Result<i32>
where
    T: LunaticWasiCtx,
{
    let export = caller.get_export("memory");
    match &export {
        Some(Extern::Memory(memory)) => {
            validate_wasi_fd_result_pointer(
                memory.data_size(&caller),
                result_ptr,
                "wasi_snapshot_preview1::path_open",
            )?;
            let (memory, state) = memory.data_and_store_mut(&mut caller);
            let mut memory = wiggle::GuestMemory::Unshared(memory);
            let result = async {
                Ok(
                    wasi_common::snapshots::preview_1::wasi_snapshot_preview1::path_open(
                        state.wasi_mut(),
                        &mut memory,
                        fd,
                        dirflags,
                        path_ptr,
                        path_len,
                        oflags,
                        rights_base,
                        rights_inheriting,
                        fdflags,
                        result_ptr,
                    )
                    .await?,
                )
            };
            wiggle::run_in_dummy_executor(result)?
        }
        Some(Extern::SharedMemory(memory)) => {
            validate_wasi_fd_result_pointer(
                memory.data().len(),
                result_ptr,
                "wasi_snapshot_preview1::path_open",
            )?;
            let mut memory = wiggle::GuestMemory::Shared(memory.data());
            let state = caller.data_mut();
            let result = async {
                Ok(
                    wasi_common::snapshots::preview_1::wasi_snapshot_preview1::path_open(
                        state.wasi_mut(),
                        &mut memory,
                        fd,
                        dirflags,
                        path_ptr,
                        path_len,
                        oflags,
                        rights_base,
                        rights_inheriting,
                        fdflags,
                        result_ptr,
                    )
                    .await?,
                )
            };
            wiggle::run_in_dummy_executor(result)?
        }
        _ => Err(anyhow!("missing required memory export")),
    }
}

#[allow(clippy::too_many_arguments)]
fn wasi_unstable_path_open_preflight<T>(
    mut caller: Caller<'_, T>,
    fd: i32,
    dirflags: i32,
    path_ptr: i32,
    path_len: i32,
    oflags: i32,
    rights_base: i64,
    rights_inheriting: i64,
    fdflags: i32,
    result_ptr: i32,
) -> Result<i32>
where
    T: LunaticWasiCtx,
{
    let export = caller.get_export("memory");
    match &export {
        Some(Extern::Memory(memory)) => {
            validate_wasi_fd_result_pointer(
                memory.data_size(&caller),
                result_ptr,
                "wasi_unstable::path_open",
            )?;
            let (memory, state) = memory.data_and_store_mut(&mut caller);
            let mut memory = wiggle::GuestMemory::Unshared(memory);
            let result = async {
                Ok(wasi_common::snapshots::preview_0::wasi_unstable::path_open(
                    state.wasi_mut(),
                    &mut memory,
                    fd,
                    dirflags,
                    path_ptr,
                    path_len,
                    oflags,
                    rights_base,
                    rights_inheriting,
                    fdflags,
                    result_ptr,
                )
                .await?)
            };
            wiggle::run_in_dummy_executor(result)?
        }
        Some(Extern::SharedMemory(memory)) => {
            validate_wasi_fd_result_pointer(
                memory.data().len(),
                result_ptr,
                "wasi_unstable::path_open",
            )?;
            let mut memory = wiggle::GuestMemory::Shared(memory.data());
            let state = caller.data_mut();
            let result = async {
                Ok(wasi_common::snapshots::preview_0::wasi_unstable::path_open(
                    state.wasi_mut(),
                    &mut memory,
                    fd,
                    dirflags,
                    path_ptr,
                    path_len,
                    oflags,
                    rights_base,
                    rights_inheriting,
                    fdflags,
                    result_ptr,
                )
                .await?)
            };
            wiggle::run_in_dummy_executor(result)?
        }
        _ => Err(anyhow!("missing required memory export")),
    }
}

fn wasi_sock_accept_preflight<T>(
    mut caller: Caller<'_, T>,
    fd: i32,
    flags: i32,
    result_ptr: i32,
) -> Result<i32>
where
    T: LunaticWasiCtx,
{
    let export = caller.get_export("memory");
    match &export {
        Some(Extern::Memory(memory)) => {
            validate_wasi_fd_result_pointer(
                memory.data_size(&caller),
                result_ptr,
                "wasi_snapshot_preview1::sock_accept",
            )?;
            let (memory, state) = memory.data_and_store_mut(&mut caller);
            let mut memory = wiggle::GuestMemory::Unshared(memory);
            let result = async {
                Ok(
                    wasi_common::snapshots::preview_1::wasi_snapshot_preview1::sock_accept(
                        state.wasi_mut(),
                        &mut memory,
                        fd,
                        flags,
                        result_ptr,
                    )
                    .await?,
                )
            };
            wiggle::run_in_dummy_executor(result)?
        }
        Some(Extern::SharedMemory(memory)) => {
            validate_wasi_fd_result_pointer(
                memory.data().len(),
                result_ptr,
                "wasi_snapshot_preview1::sock_accept",
            )?;
            let mut memory = wiggle::GuestMemory::Shared(memory.data());
            let state = caller.data_mut();
            let result = async {
                Ok(
                    wasi_common::snapshots::preview_1::wasi_snapshot_preview1::sock_accept(
                        state.wasi_mut(),
                        &mut memory,
                        fd,
                        flags,
                        result_ptr,
                    )
                    .await?,
                )
            };
            wiggle::run_in_dummy_executor(result)?
        }
        _ => Err(anyhow!("missing required memory export")),
    }
}

/// Registers preview0, preview1, and Lunatic WASI APIs with descriptor-safe wrappers.
///
/// The generated WASI functions must be shadowed for `path_open` and `sock_accept`, so this
/// function temporarily enables [`Linker`] shadowing and leaves it disabled on return. Embedders
/// that intentionally allow later registrations to shadow existing definitions must re-enable
/// that policy after this call.
pub fn register<T>(linker: &mut Linker<T>) -> Result<()>
where
    T: ProcessState + LunaticWasiCtx + Send + 'static,
    T::Config: LunaticWasiConfigCtx,
{
    // Register all wasi host functions
    wasi_common::sync::snapshots::preview_1::add_wasi_snapshot_preview1_to_linker(linker, |ctx| {
        ctx.wasi_mut()
    })?;
    wasi_common::sync::snapshots::preview_0::add_wasi_unstable_to_linker(linker, |ctx| {
        ctx.wasi_mut()
    })?;

    // Wiggle writes result pointers only after the WASI trait call has inserted
    // the new descriptor. Preflight the descriptor-producing imports so a
    // catchable bad-pointer trap cannot retain an unreachable table entry.
    linker.allow_shadowing(true);
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "path_open",
        |caller: Caller<'_, T>,
         fd: i32,
         dirflags: i32,
         path_ptr: i32,
         path_len: i32,
         oflags: i32,
         rights_base: i64,
         rights_inheriting: i64,
         fdflags: i32,
         result_ptr: i32| {
            wasi_path_open_preflight(
                caller,
                fd,
                dirflags,
                path_ptr,
                path_len,
                oflags,
                rights_base,
                rights_inheriting,
                fdflags,
                result_ptr,
            )
            .to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "wasi_unstable",
        "path_open",
        |caller: Caller<'_, T>,
         fd: i32,
         dirflags: i32,
         path_ptr: i32,
         path_len: i32,
         oflags: i32,
         rights_base: i64,
         rights_inheriting: i64,
         fdflags: i32,
         result_ptr: i32| {
            wasi_unstable_path_open_preflight(
                caller,
                fd,
                dirflags,
                path_ptr,
                path_len,
                oflags,
                rights_base,
                rights_inheriting,
                fdflags,
                result_ptr,
            )
            .to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "sock_accept",
        |caller: Caller<'_, T>, fd: i32, flags: i32, result_ptr: i32| {
            wasi_sock_accept_preflight(caller, fd, flags, result_ptr).to_wasmtime_result()
        },
    )?;
    linker.allow_shadowing(false);

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

        let parent_config = caller.data().config().clone();
        let mut candidate = caller
            .data()
            .config_resources()
            .get(config_id)
            .or_trap("lunatic::wasi::config_add_environment_variable: Config ID doesn't exist")?
            .clone();
        candidate.add_environment_variable(key, value);
        parent_config
            .validate_child_config(&candidate)
            .map_err(|reason| {
                anyhow!(
                    "lunatic::wasi::config_add_environment_variable: delegation denied: {reason}"
                )
            })?;
        *caller
            .data_mut()
            .config_resources_mut()
            .get_mut(config_id)
            .or_trap("lunatic::wasi::config_add_environment_variable: Config ID doesn't exist")? =
            candidate;
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

        let parent_config = caller.data().config().clone();
        let mut candidate = caller
            .data()
            .config_resources()
            .get(config_id)
            .or_trap("lunatic::wasi::add_command_line_argument: Config ID doesn't exist")?
            .clone();
        candidate.add_command_line_argument(argument);
        parent_config
            .validate_child_config(&candidate)
            .map_err(|reason| {
                anyhow!("lunatic::wasi::add_command_line_argument: delegation denied: {reason}")
            })?;
        *caller
            .data_mut()
            .config_resources_mut()
            .get_mut(config_id)
            .or_trap("lunatic::wasi::add_command_line_argument: Config ID doesn't exist")? =
            candidate;
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
