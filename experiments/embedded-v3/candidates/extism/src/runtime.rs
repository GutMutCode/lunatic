use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};

use crate::authority;
use crate::guest::{
    sha256_hex, validate_activations, ArtifactIdentity, GuestCatalog, GuestInstance,
};
use crate::protocol::*;

const MAILBOX_CAPACITY: usize = 64;
const PAYLOAD_LIMIT: usize = 1_024;
const CPU_DEADLINE: Duration = Duration::from_millis(50);
const CONTROL_WAIT: Duration = Duration::from_millis(1);
const INTERNAL_DEADLINE: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct Emitter {
    inner: Arc<Mutex<EmissionState>>,
}

struct EmissionState {
    next_event_seq: u64,
    writer: Box<dyn Write + Send>,
}

impl Emitter {
    pub fn stdout() -> Self {
        Self::new(Box::new(BufWriter::new(io::stdout())))
    }

    pub fn new(writer: Box<dyn Write + Send>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(EmissionState {
                next_event_seq: 1,
                writer,
            })),
        }
    }

    pub fn emit(&self, request_id: RequestId, message: EventMessage) -> Result<()> {
        let mut state = self.inner.lock().unwrap();
        let event = EventEnvelope::new(state.next_event_seq, request_id.0, message);
        let line = encode_event_line(&event)?;
        state.next_event_seq = state
            .next_event_seq
            .checked_add(1)
            .ok_or_else(|| anyhow!("event sequence overflow"))?;
        state.writer.write_all(line.as_bytes())?;
        state.writer.write_all(b"\n")?;
        state.writer.flush()?;
        Ok(())
    }

    pub fn fatal(&self, _originating_request: RequestId, code: &str, message: impl Into<String>) {
        let _ = self.emit(
            RequestId(0),
            EventMessage::Fatal(FatalEvent {
                code: code.to_owned(),
                message: message.into(),
            }),
        );
    }
}

pub struct Runtime {
    emitter: Emitter,
    artifact_root: PathBuf,
    catalog: Mutex<Option<Arc<GuestCatalog>>>,
    tenants: Mutex<BTreeMap<u32, TenantSlot>>,
    background: AtomicUsize,
    shutting_down: AtomicBool,
}

enum TenantSlot {
    Starting,
    Ready(Arc<TenantHandle>),
}

struct TenantHandle {
    commands: SyncSender<CommandJob>,
    controls: mpsc::Sender<WorkerControl>,
    gate_closed: AtomicBool,
    pending: AtomicUsize,
    book: Mutex<CommandBook>,
    metadata: Mutex<TenantMetadata>,
    join: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
struct TenantMetadata {
    tenant_id: TenantId,
    incarnation: IncarnationToken,
    generation: u64,
    counter: i64,
    identity: ArtifactIdentity,
}

#[derive(Default)]
struct CommandBook {
    completed: HashMap<u64, CachedCommand>,
    inflight: HashMap<u64, CommandFingerprint>,
}

#[derive(Clone)]
struct CachedCommand {
    fingerprint: CommandFingerprint,
    result: CommandResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommandFingerprint {
    operation: CommandOperation,
    payload_sha256: String,
}

struct CommandJob {
    request_id: RequestId,
    control: CommandControl,
    fingerprint: CommandFingerprint,
    admission: Arc<(Mutex<bool>, Condvar)>,
}

enum WorkerControl {
    SetGate {
        request_id: RequestId,
        control: SetDequeueGateControl,
    },
    Snapshot {
        request_id: RequestId,
        expected: Option<IncarnationToken>,
    },
    Fault {
        request_id: RequestId,
        control: InjectFaultControl,
        catalog: Arc<GuestCatalog>,
    },
    Freeze {
        reply: SyncSender<Result<FrozenTenant>>,
    },
    Swap {
        instance: Box<GuestInstance>,
        identity: ArtifactIdentity,
        counter: i64,
        reply: SyncSender<Result<()>>,
    },
    Resume {
        reply: SyncSender<()>,
    },
    Stop {
        reply: SyncSender<()>,
    },
}

#[derive(Clone)]
struct FrozenTenant {
    counter: i64,
    metadata: TenantMetadata,
}

impl Runtime {
    pub fn new(emitter: Emitter, artifact_root: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            emitter,
            artifact_root,
            catalog: Mutex::new(None),
            tenants: Mutex::new(BTreeMap::new()),
            background: AtomicUsize::new(0),
            shutting_down: AtomicBool::new(false),
        })
    }

    pub fn initialize(&self, request_id: RequestId, control: InitControl) -> Result<()> {
        let catalog = Arc::new(GuestCatalog::load(&self.artifact_root)?);
        let mut slot = self.catalog.lock().unwrap();
        if slot.is_some() {
            bail!("runtime initialized twice");
        }
        *slot = Some(catalog);
        self.emitter.emit(
            request_id,
            EventMessage::Initialized(InitializedEvent {
                run_id: control.run_id,
            }),
        )
    }

    pub fn create_tenant(self: &Arc<Self>, request_id: RequestId, control: CreateTenantControl) {
        let catalog = match self.catalog() {
            Ok(catalog) => catalog,
            Err(error) => {
                self.emitter
                    .fatal(request_id, "not_initialized", format!("{error:#}"));
                return;
            }
        };
        let spec = match catalog.spec(&control.artifact_sha256) {
            Some(spec) => spec,
            None => {
                self.emitter.fatal(
                    request_id,
                    "unknown_artifact",
                    format!("unknown artifact {}", control.artifact_sha256),
                );
                return;
            }
        };
        {
            let mut tenants = self.tenants.lock().unwrap();
            if tenants.contains_key(&control.tenant_id.0) {
                self.emitter.fatal(
                    request_id,
                    "duplicate_tenant",
                    "tenant is already active or starting",
                );
                return;
            }
            tenants.insert(control.tenant_id.0, TenantSlot::Starting);
        }
        if let Err(error) = self.emitter.emit(
            request_id,
            EventMessage::TenantCreated(TenantCreatedEvent {
                tenant_id: control.tenant_id,
                incarnation: control.incarnation,
            }),
        ) {
            self.emitter
                .fatal(request_id, "stdout_failure", format!("{error:#}"));
            return;
        }
        self.background.fetch_add(1, Ordering::SeqCst);
        let runtime = self.clone();
        thread::spawn(move || {
            let result = (|| -> Result<()> {
                let mut instance = catalog
                    .instantiate(&control.artifact_sha256, control.tenant_id.0 as i32, 0)
                    .map_err(|failure| anyhow!(failure.message))?;
                let activations = instance.take_activations();
                validate_activations(&activations, control.tenant_id.0 as i32, spec)?;
                let identity = ArtifactIdentity {
                    logical_version: control.logical_version.clone(),
                    build_id: control.build_id.clone(),
                    artifact_sha256: control.artifact_sha256.clone(),
                };
                let handle = spawn_worker(
                    runtime.emitter.clone(),
                    instance,
                    TenantMetadata {
                        tenant_id: control.tenant_id,
                        incarnation: control.incarnation,
                        generation: control.incarnation.value(),
                        counter: 0,
                        identity: identity.clone(),
                    },
                );
                runtime
                    .tenants
                    .lock()
                    .unwrap()
                    .insert(control.tenant_id.0, TenantSlot::Ready(handle));
                emit_activation(
                    &runtime.emitter,
                    request_id,
                    control.tenant_id,
                    control.incarnation,
                    &identity,
                )?;
                runtime.emitter.emit(
                    request_id,
                    EventMessage::TenantReady(TenantReadyEvent {
                        tenant_id: control.tenant_id,
                        incarnation: control.incarnation,
                    }),
                )?;
                Ok(())
            })();
            if let Err(error) = result {
                runtime.tenants.lock().unwrap().remove(&control.tenant_id.0);
                runtime
                    .emitter
                    .fatal(request_id, "create_failed", format!("{error:#}"));
            }
            runtime.background.fetch_sub(1, Ordering::SeqCst);
        });
    }

    pub fn command(&self, request_id: RequestId, control: CommandControl) -> Result<()> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return self.reject_command(request_id, &control, RejectionReason::ShuttingDown);
        }
        let Some(handle) = self.ready_tenant(control.tenant_id) else {
            return self.reject_command(request_id, &control, RejectionReason::TenantMissing);
        };
        if handle.metadata.lock().unwrap().incarnation != control.incarnation {
            return self.reject_command(request_id, &control, RejectionReason::StaleIncarnation);
        }
        if control.payload.len() > PAYLOAD_LIMIT {
            return self.reject_command(request_id, &control, RejectionReason::PayloadTooLarge);
        }
        let fingerprint = CommandFingerprint {
            operation: control.operation.clone(),
            payload_sha256: control.payload_sha256.clone(),
        };
        {
            let book = handle.book.lock().unwrap();
            let conflict = book
                .completed
                .get(&control.command_id.0)
                .map(|cached| cached.fingerprint != fingerprint)
                .unwrap_or(false)
                || book
                    .inflight
                    .get(&control.command_id.0)
                    .map(|pending| pending != &fingerprint)
                    .unwrap_or(false);
            if conflict {
                drop(book);
                return self.reject_command(
                    request_id,
                    &control,
                    RejectionReason::CommandIdConflict,
                );
            }
        }
        let admission = Arc::new((Mutex::new(false), Condvar::new()));
        let job = CommandJob {
            request_id,
            control: control.clone(),
            fingerprint: fingerprint.clone(),
            admission: admission.clone(),
        };
        handle.pending.fetch_add(1, Ordering::SeqCst);
        match handle.commands.try_send(job) {
            Ok(()) => {
                handle
                    .book
                    .lock()
                    .unwrap()
                    .inflight
                    .insert(control.command_id.0, fingerprint);
                self.emitter.emit(
                    request_id,
                    EventMessage::CommandAccepted(CommandAcceptedEvent {
                        tenant_id: control.tenant_id,
                        incarnation: control.incarnation,
                        command_id: control.command_id,
                    }),
                )?;
                let (lock, condition) = &*admission;
                *lock.lock().unwrap() = true;
                condition.notify_one();
                Ok(())
            }
            Err(TrySendError::Full(_)) => {
                handle.pending.fetch_sub(1, Ordering::SeqCst);
                self.reject_command(request_id, &control, RejectionReason::Backpressure)
            }
            Err(TrySendError::Disconnected(_)) => {
                handle.pending.fetch_sub(1, Ordering::SeqCst);
                self.reject_command(request_id, &control, RejectionReason::TenantMissing)
            }
        }
    }

    pub fn set_gate(&self, request_id: RequestId, control: SetDequeueGateControl) -> Result<()> {
        let handle = self
            .ready_tenant(control.tenant_id)
            .ok_or_else(|| anyhow!("gate target is missing"))?;
        if handle.metadata.lock().unwrap().incarnation != control.incarnation {
            bail!("gate control used a stale incarnation");
        }
        handle
            .controls
            .send(WorkerControl::SetGate {
                request_id,
                control,
            })
            .map_err(|_| anyhow!("gate worker disconnected"))
    }

    pub fn snapshot(&self, request_id: RequestId, control: SnapshotControl) -> Result<()> {
        let Some(handle) = self.ready_tenant(control.tenant_id) else {
            return self.emitter.emit(
                request_id,
                EventMessage::SnapshotMissing(SnapshotMissingEvent {
                    tenant_id: control.tenant_id,
                }),
            );
        };
        let actual = handle.metadata.lock().unwrap().incarnation;
        if let Some(expected) = control.expected_incarnation {
            if expected != actual {
                return self.emitter.emit(
                    request_id,
                    EventMessage::SnapshotStale(SnapshotStaleEvent {
                        tenant_id: control.tenant_id,
                        expected_incarnation: expected,
                        actual_incarnation: actual,
                    }),
                );
            }
        }
        handle
            .controls
            .send(WorkerControl::Snapshot {
                request_id,
                expected: control.expected_incarnation,
            })
            .map_err(|_| anyhow!("snapshot worker disconnected"))
    }

    pub fn inject_fault(&self, request_id: RequestId, control: InjectFaultControl) -> Result<()> {
        let handle = self
            .ready_tenant(control.tenant_id)
            .ok_or_else(|| anyhow!("fault target is missing"))?;
        let metadata = handle.metadata.lock().unwrap().clone();
        if metadata.incarnation != control.incarnation {
            bail!("fault control used a stale incarnation");
        }
        if control.expected_replacement_incarnation != control.incarnation.checked_next().unwrap() {
            bail!("fault replacement incarnation is not the oracle-owned successor");
        }
        let catalog = self.catalog()?;
        if catalog.spec(&control.replacement_artifact_sha256).is_none() {
            bail!("fault replacement artifact is unknown");
        }
        handle
            .controls
            .send(WorkerControl::Fault {
                request_id,
                control,
                catalog,
            })
            .map_err(|_| anyhow!("fault worker disconnected"))
    }

    pub fn rollout(self: &Arc<Self>, request_id: RequestId, control: RolloutControl) -> Result<()> {
        let catalog = self.catalog()?;
        let spec = match catalog.spec(&control.artifact_sha256) {
            Some(spec) => spec,
            None => {
                return self.emitter.emit(
                    request_id,
                    EventMessage::RolloutRejected(RolloutRejectedEvent {
                        rollout_id: control.rollout_id,
                        reason: "unknown frozen core artifact".into(),
                    }),
                );
            }
        };
        if Path::new(&control.artifact_ref)
            .file_name()
            .and_then(|name| name.to_str())
            != Some(spec.file_name)
        {
            return self.emitter.emit(
                request_id,
                EventMessage::RolloutRejected(RolloutRejectedEvent {
                    rollout_id: control.rollout_id,
                    reason: "artifact_ref does not name the hashed frozen artifact".into(),
                }),
            );
        }
        let handles = {
            let tenants = self.tenants.lock().unwrap();
            let mut handles = Vec::with_capacity(control.targets.len());
            for tenant in &control.targets {
                let Some(TenantSlot::Ready(handle)) = tenants.get(&tenant.0) else {
                    return self.emitter.emit(
                        request_id,
                        EventMessage::RolloutRejected(RolloutRejectedEvent {
                            rollout_id: control.rollout_id,
                            reason: format!("target {} is not ready", tenant.0),
                        }),
                    );
                };
                if handle.metadata.lock().unwrap().identity.logical_version != control.from_version
                {
                    return self.emitter.emit(
                        request_id,
                        EventMessage::RolloutRejected(RolloutRejectedEvent {
                            rollout_id: control.rollout_id,
                            reason: format!("target {} is not on from_version", tenant.0),
                        }),
                    );
                }
                handles.push((*tenant, handle.clone()));
            }
            handles
        };
        self.emitter.emit(
            request_id,
            EventMessage::RolloutStarted(RolloutStartedEvent {
                rollout_id: control.rollout_id.clone(),
            }),
        )?;
        self.background.fetch_add(1, Ordering::SeqCst);
        let runtime = self.clone();
        thread::spawn(move || {
            let result = runtime.execute_rollout(request_id, control, catalog, handles);
            if let Err(error) = result {
                runtime
                    .emitter
                    .fatal(request_id, "rollout_failed", format!("{error:#}"));
            }
            runtime.background.fetch_sub(1, Ordering::SeqCst);
        });
        Ok(())
    }

    pub fn authority_probe(
        &self,
        request_id: RequestId,
        control: AuthorityProbeControl,
    ) -> Result<()> {
        self.emitter.emit(
            request_id,
            EventMessage::AuthorityProbeAccepted(AuthorityProbeAcceptedEvent {
                attempt_id: control.attempt_id,
                probe: control.probe,
                artifact_sha256: control.artifact_sha256.clone(),
                parameter_sha256: control.parameter_sha256.clone(),
                production_policy_sha256: control.production_policy_sha256.clone(),
            }),
        )?;
        let outcome = authority::run_probe(control.probe, control.artifact_bytes)?;
        self.emitter.emit(
            request_id,
            EventMessage::AuthorityProbeTerminal(AuthorityProbeTerminalEvent {
                attempt_id: control.attempt_id,
                probe: control.probe,
                artifact_sha256: control.artifact_sha256,
                parameter_sha256: control.parameter_sha256,
                production_policy_sha256: control.production_policy_sha256,
                stage: outcome.stage,
                result: outcome.result,
                error_class: outcome.error_class,
                guest_return_sha256: EMPTY_SHA256.into(),
            }),
        )
    }

    pub fn quiesce(&self, request_id: RequestId) -> Result<()> {
        let deadline = Instant::now() + INTERNAL_DEADLINE;
        loop {
            let idle_tenants = self
                .tenants
                .lock()
                .unwrap()
                .values()
                .all(|slot| match slot {
                    TenantSlot::Starting => false,
                    TenantSlot::Ready(handle) => handle.pending.load(Ordering::SeqCst) == 0,
                });
            if idle_tenants && self.background.load(Ordering::SeqCst) == 0 {
                break;
            }
            if Instant::now() >= deadline {
                bail!("quiesce timed out with active candidate work");
            }
            thread::yield_now();
        }
        self.emitter
            .emit(request_id, EventMessage::Quiesced(QuiescedEvent {}))
    }

    pub fn teardown(&self, request_id: RequestId, control: TeardownTenantControl) -> Result<()> {
        let handle = {
            let mut tenants = self.tenants.lock().unwrap();
            let Some(TenantSlot::Ready(handle)) = tenants.get(&control.tenant_id.0) else {
                bail!("teardown target is not ready");
            };
            if handle.metadata.lock().unwrap().incarnation != control.incarnation {
                bail!("teardown control used a stale incarnation");
            }
            if handle.pending.load(Ordering::SeqCst) != 0 {
                bail!("teardown attempted with accepted commands outstanding");
            }
            let handle = handle.clone();
            tenants.remove(&control.tenant_id.0);
            handle
        };
        stop_worker(&handle)?;
        self.emitter.emit(
            request_id,
            EventMessage::TenantTornDown(TenantTornDownEvent {
                tenant_id: control.tenant_id,
                incarnation: control.incarnation,
            }),
        )
    }

    pub fn shutdown(&self, request_id: RequestId) -> Result<()> {
        self.shutting_down.store(true, Ordering::SeqCst);
        let handles = {
            let mut tenants = self.tenants.lock().unwrap();
            let handles = tenants
                .values()
                .filter_map(|slot| match slot {
                    TenantSlot::Starting => None,
                    TenantSlot::Ready(handle) => Some(handle.clone()),
                })
                .collect::<Vec<_>>();
            tenants.clear();
            handles
        };
        for handle in handles {
            stop_worker(&handle)?;
        }
        self.emitter.emit(
            request_id,
            EventMessage::ShutdownComplete(ShutdownCompleteEvent {}),
        )
    }

    fn execute_rollout(
        &self,
        request_id: RequestId,
        control: RolloutControl,
        catalog: Arc<GuestCatalog>,
        handles: Vec<(TenantId, Arc<TenantHandle>)>,
    ) -> Result<()> {
        let spec = catalog
            .spec(&control.artifact_sha256)
            .ok_or_else(|| anyhow!("rollout artifact disappeared"))?;
        for (tenant, _) in &handles {
            match catalog.instantiate(&control.artifact_sha256, tenant.0 as i32, 0) {
                Ok(mut preflight) => {
                    let activations = preflight.take_activations();
                    validate_activations(&activations, tenant.0 as i32, spec)?;
                    emit_activation(
                        &self.emitter,
                        request_id,
                        *tenant,
                        self.incarnation(*tenant)?,
                        &rollout_identity(&control),
                    )?;
                }
                Err(failure) => {
                    if !failure.activations.is_empty() {
                        validate_activations(&failure.activations, tenant.0 as i32, spec)?;
                        emit_activation(
                            &self.emitter,
                            request_id,
                            *tenant,
                            self.incarnation(*tenant)?,
                            &rollout_identity(&control),
                        )?;
                    }
                    return self.emitter.emit(
                        request_id,
                        EventMessage::RolloutRolledBack(RolloutRolledBackEvent {
                            rollout_id: control.rollout_id,
                            restored_version: control.from_version,
                            reason: failure.message,
                        }),
                    );
                }
            }
        }
        let mut frozen = Vec::with_capacity(handles.len());
        for (tenant, handle) in &handles {
            let (send, receive) = mpsc::sync_channel(1);
            handle
                .controls
                .send(WorkerControl::Freeze { reply: send })?;
            let snapshot = receive.recv_timeout(INTERNAL_DEADLINE)??;
            if snapshot.metadata.tenant_id != *tenant {
                bail!("worker froze the wrong tenant");
            }
            frozen.push(snapshot);
        }
        let mut replacements = Vec::with_capacity(frozen.len());
        for snapshot in &frozen {
            match catalog.instantiate(
                &control.artifact_sha256,
                snapshot.metadata.tenant_id.0 as i32,
                snapshot.counter,
            ) {
                Ok(mut instance) => {
                    let activations = instance.take_activations();
                    validate_activations(&activations, snapshot.metadata.tenant_id.0 as i32, spec)?;
                    emit_activation(
                        &self.emitter,
                        request_id,
                        snapshot.metadata.tenant_id,
                        snapshot.metadata.incarnation,
                        &rollout_identity(&control),
                    )?;
                    replacements.push(instance);
                }
                Err(failure) => {
                    self.resume_all(&handles)?;
                    return self.emitter.emit(
                        request_id,
                        EventMessage::RolloutRolledBack(RolloutRolledBackEvent {
                            rollout_id: control.rollout_id,
                            restored_version: control.from_version,
                            reason: failure.message,
                        }),
                    );
                }
            }
        }
        let identity = rollout_identity(&control);
        for (((tenant, handle), snapshot), instance) in
            handles.iter().zip(frozen.iter()).zip(replacements)
        {
            let (send, receive) = mpsc::sync_channel(1);
            handle.controls.send(WorkerControl::Swap {
                instance: Box::new(instance),
                identity: identity.clone(),
                counter: snapshot.counter,
                reply: send,
            })?;
            if let Err(error) = receive.recv_timeout(INTERNAL_DEADLINE)? {
                self.emitter.emit(
                    request_id,
                    EventMessage::RolloutInDoubt(RolloutInDoubtEvent {
                        rollout_id: control.rollout_id,
                        reason: format!("target {} swap failed: {error:#}", tenant.0),
                    }),
                )?;
                return Ok(());
            }
            self.emitter.emit(
                request_id,
                EventMessage::RolloutTargetReady(RolloutTargetReadyEvent {
                    rollout_id: control.rollout_id.clone(),
                    tenant_id: *tenant,
                    incarnation: snapshot.metadata.incarnation,
                    logical_version: control.to_version.clone(),
                    build_id: control.to_build_id.clone(),
                    artifact_sha256: control.artifact_sha256.clone(),
                }),
            )?;
        }
        self.emitter.emit(
            request_id,
            EventMessage::RolloutCommitted(RolloutCommittedEvent {
                rollout_id: control.rollout_id,
                version: control.to_version,
            }),
        )
    }

    fn resume_all(&self, handles: &[(TenantId, Arc<TenantHandle>)]) -> Result<()> {
        for (_, handle) in handles {
            let (send, receive) = mpsc::sync_channel(1);
            handle
                .controls
                .send(WorkerControl::Resume { reply: send })?;
            receive.recv_timeout(INTERNAL_DEADLINE)?;
        }
        Ok(())
    }

    fn incarnation(&self, tenant: TenantId) -> Result<IncarnationToken> {
        self.ready_tenant(tenant)
            .map(|handle| handle.metadata.lock().unwrap().incarnation)
            .ok_or_else(|| anyhow!("tenant {} disappeared during rollout", tenant.0))
    }

    fn catalog(&self) -> Result<Arc<GuestCatalog>> {
        self.catalog
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow!("runtime is not initialized"))
    }

    fn ready_tenant(&self, tenant: TenantId) -> Option<Arc<TenantHandle>> {
        match self.tenants.lock().unwrap().get(&tenant.0) {
            Some(TenantSlot::Ready(handle)) => Some(handle.clone()),
            _ => None,
        }
    }

    fn reject_command(
        &self,
        request_id: RequestId,
        control: &CommandControl,
        reason: RejectionReason,
    ) -> Result<()> {
        self.emitter.emit(
            request_id,
            EventMessage::CommandRejected(CommandRejectedEvent {
                tenant_id: control.tenant_id,
                incarnation: control.incarnation,
                command_id: control.command_id,
                reason,
            }),
        )
    }
}

fn rollout_identity(control: &RolloutControl) -> ArtifactIdentity {
    ArtifactIdentity {
        logical_version: control.to_version.clone(),
        build_id: control.to_build_id.clone(),
        artifact_sha256: control.artifact_sha256.clone(),
    }
}

fn spawn_worker(
    emitter: Emitter,
    instance: GuestInstance,
    metadata: TenantMetadata,
) -> Arc<TenantHandle> {
    let (command_send, command_receive) = mpsc::sync_channel(MAILBOX_CAPACITY);
    let (control_send, control_receive) = mpsc::channel();
    let handle = Arc::new(TenantHandle {
        commands: command_send,
        controls: control_send,
        gate_closed: AtomicBool::new(false),
        pending: AtomicUsize::new(0),
        book: Mutex::new(CommandBook::default()),
        metadata: Mutex::new(metadata),
        join: Mutex::new(None),
    });
    let worker_handle = handle.clone();
    let join = thread::spawn(move || {
        if let Err(error) = worker_loop(
            emitter.clone(),
            worker_handle,
            instance,
            command_receive,
            control_receive,
        ) {
            emitter.fatal(RequestId(0), "worker_failed", format!("{error:#}"));
        }
    });
    *handle.join.lock().unwrap() = Some(join);
    handle
}

fn worker_loop(
    emitter: Emitter,
    handle: Arc<TenantHandle>,
    mut instance: GuestInstance,
    commands: Receiver<CommandJob>,
    controls: Receiver<WorkerControl>,
) -> Result<()> {
    let mut frozen = false;
    loop {
        while let Ok(control) = controls.try_recv() {
            if handle_worker_control(&emitter, &handle, &mut instance, &mut frozen, control)? {
                return Ok(());
            }
        }
        if frozen || handle.gate_closed.load(Ordering::SeqCst) {
            match controls.recv_timeout(CONTROL_WAIT) {
                Ok(control) => {
                    if handle_worker_control(
                        &emitter,
                        &handle,
                        &mut instance,
                        &mut frozen,
                        control,
                    )? {
                        return Ok(());
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Ok(()),
            }
            continue;
        }
        match commands.try_recv() {
            Ok(job) => execute_command(&emitter, &handle, &mut instance, job)?,
            Err(mpsc::TryRecvError::Empty) => match controls.recv_timeout(CONTROL_WAIT) {
                Ok(control) => {
                    if handle_worker_control(
                        &emitter,
                        &handle,
                        &mut instance,
                        &mut frozen,
                        control,
                    )? {
                        return Ok(());
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Ok(()),
            },
            Err(mpsc::TryRecvError::Disconnected) => return Ok(()),
        }
    }
}

fn handle_worker_control(
    emitter: &Emitter,
    handle: &Arc<TenantHandle>,
    instance: &mut GuestInstance,
    frozen: &mut bool,
    control: WorkerControl,
) -> Result<bool> {
    match control {
        WorkerControl::SetGate {
            request_id,
            control,
        } => {
            handle.gate_closed.store(control.closed, Ordering::SeqCst);
            emitter.emit(
                request_id,
                EventMessage::DequeueGateSet(DequeueGateSetEvent {
                    tenant_id: control.tenant_id,
                    incarnation: control.incarnation,
                    closed: control.closed,
                }),
            )?;
            Ok(false)
        }
        WorkerControl::Snapshot {
            request_id,
            expected,
        } => {
            let metadata = handle.metadata.lock().unwrap().clone();
            if let Some(expected) = expected {
                if expected != metadata.incarnation {
                    emitter.emit(
                        request_id,
                        EventMessage::SnapshotStale(SnapshotStaleEvent {
                            tenant_id: metadata.tenant_id,
                            expected_incarnation: expected,
                            actual_incarnation: metadata.incarnation,
                        }),
                    )?;
                    return Ok(false);
                }
            }
            let observed = instance.call_business("snapshot", 0)?;
            if observed.counter != metadata.counter {
                bail!("guest snapshot disagreed with candidate state");
            }
            emitter.emit(
                request_id,
                EventMessage::SnapshotPresent(SnapshotPresentEvent {
                    tenant_id: metadata.tenant_id,
                    incarnation: metadata.incarnation,
                    state: result_for(&metadata, observed.counter, false),
                }),
            )?;
            Ok(false)
        }
        WorkerControl::Fault {
            request_id,
            control,
            catalog,
        } => {
            execute_fault(emitter, handle, instance, request_id, control, catalog)?;
            Ok(false)
        }
        WorkerControl::Freeze { reply } => {
            if *frozen {
                let _ = reply.send(Err(anyhow!("tenant is already frozen")));
                return Ok(false);
            }
            let metadata = handle.metadata.lock().unwrap().clone();
            let snapshot = instance.call_business("snapshot", 0);
            match snapshot {
                Ok(snapshot) if snapshot.counter == metadata.counter => {
                    *frozen = true;
                    let _ = reply.send(Ok(FrozenTenant {
                        counter: snapshot.counter,
                        metadata,
                    }));
                }
                Ok(_) => {
                    let _ = reply.send(Err(anyhow!("freeze snapshot counter mismatch")));
                }
                Err(error) => {
                    let _ = reply.send(Err(error));
                }
            }
            Ok(false)
        }
        WorkerControl::Swap {
            instance: replacement,
            identity,
            counter,
            reply,
        } => {
            if !*frozen {
                let _ = reply.send(Err(anyhow!("swap requires a frozen tenant")));
                return Ok(false);
            }
            *instance = *replacement;
            {
                let mut metadata = handle.metadata.lock().unwrap();
                metadata.identity = identity;
                metadata.counter = counter;
            }
            *frozen = false;
            let _ = reply.send(Ok(()));
            Ok(false)
        }
        WorkerControl::Resume { reply } => {
            *frozen = false;
            let _ = reply.send(());
            Ok(false)
        }
        WorkerControl::Stop { reply } => {
            let _ = reply.send(());
            Ok(true)
        }
    }
}

fn execute_command(
    emitter: &Emitter,
    handle: &Arc<TenantHandle>,
    instance: &mut GuestInstance,
    job: CommandJob,
) -> Result<()> {
    wait_for_admission(&job.admission);
    let terminal = (|| -> Result<CommandResult> {
        if let Some(cached) = handle
            .book
            .lock()
            .unwrap()
            .completed
            .get(&job.control.command_id.0)
            .cloned()
        {
            if cached.fingerprint != job.fingerprint {
                bail!("accepted command conflicts with a completed command");
            }
            let mut result = cached.result;
            result.deduplicated = true;
            return Ok(result);
        }
        let export = match &job.control.operation {
            CommandOperation::Increment(operation) if operation.delta == 1 => "increment",
            CommandOperation::Increment(_) => bail!("core guest supports only delta one"),
            CommandOperation::Read(_) => "probe",
        };
        let observed = instance.call_business(export, job.control.command_id.0)?;
        let mut metadata = handle.metadata.lock().unwrap();
        let expected = match job.control.operation {
            CommandOperation::Increment(_) => metadata
                .counter
                .checked_add(1)
                .ok_or_else(|| anyhow!("counter overflow"))?,
            CommandOperation::Read(_) => metadata.counter,
        };
        if observed.counter != expected {
            bail!("guest counter transition did not match the command");
        }
        metadata.counter = observed.counter;
        let result = result_for(&metadata, observed.counter, false);
        handle.book.lock().unwrap().completed.insert(
            job.control.command_id.0,
            CachedCommand {
                fingerprint: job.fingerprint.clone(),
                result: result.clone(),
            },
        );
        Ok(result)
    })();
    handle
        .book
        .lock()
        .unwrap()
        .inflight
        .remove(&job.control.command_id.0);
    handle.pending.fetch_sub(1, Ordering::SeqCst);
    match terminal {
        Ok(result) => emitter.emit(
            job.request_id,
            EventMessage::CommandCompleted(CommandCompletedEvent {
                tenant_id: job.control.tenant_id,
                incarnation: job.control.incarnation,
                command_id: job.control.command_id,
                result,
            }),
        ),
        Err(error) => emitter.emit(
            job.request_id,
            EventMessage::CommandFailed(CommandFailedEvent {
                tenant_id: job.control.tenant_id,
                incarnation: job.control.incarnation,
                command_id: job.control.command_id,
                error: format!("{error:#}"),
            }),
        ),
    }
}

fn execute_fault(
    emitter: &Emitter,
    handle: &Arc<TenantHandle>,
    instance: &mut GuestInstance,
    request_id: RequestId,
    control: InjectFaultControl,
    catalog: Arc<GuestCatalog>,
) -> Result<()> {
    if handle.pending.load(Ordering::SeqCst) != 0 {
        bail!("fault began with accepted commands outstanding");
    }
    let metadata = handle.metadata.lock().unwrap().clone();
    match control.fault {
        FaultKind::Trap(_) => {
            if instance.call_trap(control.fault_id.0).is_ok() {
                bail!("trap fault returned successfully");
            }
            emitter.emit(
                request_id,
                EventMessage::ExecutionFailed(ExecutionFailedEvent {
                    fault_id: control.fault_id,
                    tenant_id: metadata.tenant_id,
                    incarnation: metadata.incarnation,
                    reason: FaultFailureReason::GuestTrap,
                }),
            )?;
        }
        FaultKind::CpuHog(_) => {
            execute_cpu_deadline(emitter, instance, request_id, &control, &metadata)?;
        }
    }
    emitter.emit(
        request_id,
        EventMessage::FailureObserved(FailureObservedEvent {
            fault_id: control.fault_id,
            tenant_id: metadata.tenant_id,
            failed_incarnation: metadata.incarnation,
        }),
    )?;
    let spec = catalog
        .spec(&control.replacement_artifact_sha256)
        .ok_or_else(|| anyhow!("unknown fault replacement artifact"))?;
    let mut replacement = catalog
        .instantiate(
            &control.replacement_artifact_sha256,
            metadata.tenant_id.0 as i32,
            0,
        )
        .map_err(|failure| anyhow!(failure.message))?;
    let activations = replacement.take_activations();
    validate_activations(&activations, metadata.tenant_id.0 as i32, spec)?;
    let identity = ArtifactIdentity {
        logical_version: control.replacement_logical_version,
        build_id: control.replacement_build_id,
        artifact_sha256: control.replacement_artifact_sha256,
    };
    *instance = replacement;
    {
        let mut current = handle.metadata.lock().unwrap();
        current.incarnation = control.expected_replacement_incarnation;
        current.generation = control.expected_replacement_incarnation.value();
        current.counter = 0;
        current.identity = identity.clone();
    }
    *handle.book.lock().unwrap() = CommandBook::default();
    handle.gate_closed.store(false, Ordering::SeqCst);
    emit_activation(
        emitter,
        request_id,
        metadata.tenant_id,
        control.expected_replacement_incarnation,
        &identity,
    )?;
    emitter.emit(
        request_id,
        EventMessage::TenantReady(TenantReadyEvent {
            tenant_id: metadata.tenant_id,
            incarnation: control.expected_replacement_incarnation,
        }),
    )
}

fn execute_cpu_deadline(
    emitter: &Emitter,
    instance: &mut GuestInstance,
    request_id: RequestId,
    control: &InjectFaultControl,
    metadata: &TenantMetadata,
) -> Result<()> {
    let cancel = instance.cancel_handle();
    let (started_send, started_receive) = mpsc::sync_channel(1);
    let emitted = Arc::new(AtomicBool::new(false));
    let observer_emitted = emitted.clone();
    let observer_emitter = emitter.clone();
    let fault_id = control.fault_id;
    let tenant_id = metadata.tenant_id;
    let incarnation = metadata.incarnation;
    let expected_handle = i64::try_from(fault_id.0).unwrap_or(i64::MIN);
    let observer = Arc::new(move |handle: i64| {
        if handle != expected_handle || observer_emitted.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = observer_emitter.emit(
            request_id,
            EventMessage::ExecutionStarted(ExecutionStartedEvent {
                fault_id,
                tenant_id,
                incarnation,
                origin: ExecutionStartOrigin::GuestFirstActionObserver,
            }),
        );
        let _ = started_send.send(());
    });
    let timer = thread::spawn(move || -> Result<()> {
        let marker = started_receive
            .recv_timeout(Duration::from_millis(20))
            .context("guest did not reach execution_started within 20ms");
        if marker.is_ok() {
            thread::sleep(CPU_DEADLINE);
        }
        cancel.cancel().context("Extism cancel failed")?;
        marker
    });
    let call = instance.call_cpu(control.fault_id.0, observer);
    let timer_result = timer.join().map_err(|_| anyhow!("CPU timer panicked"))?;
    timer_result?;
    if call.is_ok() {
        bail!("cpu_loop returned successfully after cancellation");
    }
    if !emitted.load(Ordering::SeqCst) {
        bail!("CPU cancellation occurred without a guest execution marker");
    }
    emitter.emit(
        request_id,
        EventMessage::ExecutionFailed(ExecutionFailedEvent {
            fault_id: control.fault_id,
            tenant_id: metadata.tenant_id,
            incarnation: metadata.incarnation,
            reason: FaultFailureReason::CpuDeadline,
        }),
    )
}

fn wait_for_admission(admission: &Arc<(Mutex<bool>, Condvar)>) {
    let (lock, condition) = &**admission;
    let mut ready = lock.lock().unwrap();
    while !*ready {
        ready = condition.wait(ready).unwrap();
    }
}

fn result_for(metadata: &TenantMetadata, counter: i64, deduplicated: bool) -> CommandResult {
    CommandResult {
        incarnation: metadata.incarnation,
        generation: DecimalU64(metadata.generation),
        counter,
        logical_version: metadata.identity.logical_version.clone(),
        build_id: metadata.identity.build_id.clone(),
        deduplicated,
        business_result_sha256: business_result_sha256(metadata.generation, counter),
    }
}

fn business_result_sha256(generation: u64, counter: i64) -> String {
    sha256_hex(format!(r#"{{"counter":{counter},"generation":{generation}}}"#).as_bytes())
}

fn emit_activation(
    emitter: &Emitter,
    request_id: RequestId,
    tenant_id: TenantId,
    incarnation: IncarnationToken,
    identity: &ArtifactIdentity,
) -> Result<()> {
    emitter.emit(
        request_id,
        EventMessage::ActivationStarted(ActivationStartedEvent {
            tenant_id,
            incarnation,
            logical_version: identity.logical_version.clone(),
            build_id: identity.build_id.clone(),
            artifact_sha256: identity.artifact_sha256.clone(),
        }),
    )
}

fn stop_worker(handle: &Arc<TenantHandle>) -> Result<()> {
    let (send, receive) = mpsc::sync_channel(1);
    handle.controls.send(WorkerControl::Stop { reply: send })?;
    receive.recv_timeout(INTERNAL_DEADLINE)?;
    if let Some(join) = handle.join.lock().unwrap().take() {
        join.join().map_err(|_| anyhow!("tenant worker panicked"))?;
    }
    Ok(())
}
