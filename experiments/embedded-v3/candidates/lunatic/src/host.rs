use std::collections::HashMap;
use std::convert::TryFrom;
use std::env;
use std::future::Future;
use std::io::{self, BufRead, BufReader};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use tokio::runtime::Builder;
use tokio::sync::{Notify, RwLock};

use crate::events::EventSink;
use crate::protocol::*;
use crate::runtime::{activation_identity, Admission, RuntimeSet, Tenant, TenantRuntime};

type TenantMap = Arc<RwLock<HashMap<u32, Arc<Tenant>>>>;

pub fn run() -> Result<()> {
    Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()?
        .block_on(run_session())
}

async fn run_session() -> Result<()> {
    let sink = EventSink::stdout();
    let mut host = Host::new(sink.clone());
    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            return fail(&sink, anyhow!("control stream closed before shutdown"));
        }
        if line.ends_with('\n') {
            line.pop();
            if line.ends_with('\r') {
                line.pop();
            }
        }
        let control = match decode_control_line(&line) {
            Ok(control) => control,
            Err(error) => return fail(&sink, error.into()),
        };
        match host.dispatch(control).await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => return fail(&sink, error),
        }
    }
}

fn fail(sink: &EventSink, error: anyhow::Error) -> Result<()> {
    let message = error.to_string();
    let _ = sink.emit(
        RequestId(0),
        EventMessage::Fatal(FatalEvent {
            code: "candidate_failure".into(),
            message,
        }),
    );
    Err(error)
}

struct TaskTracker {
    active: AtomicUsize,
    changed: Notify,
}

struct ActiveTask(Arc<TaskTracker>);

impl Drop for ActiveTask {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_waiters();
    }
}

impl TaskTracker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            active: AtomicUsize::new(0),
            changed: Notify::new(),
        })
    }

    fn spawn<F>(self: &Arc<Self>, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.active.fetch_add(1, Ordering::SeqCst);
        let tracker = Arc::clone(self);
        tokio::spawn(async move {
            let _active = ActiveTask(tracker);
            future.await;
        });
    }

    async fn wait_idle(&self) {
        loop {
            let changed = self.changed.notified();
            if self.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            changed.await;
        }
    }
}

struct Host {
    sink: EventSink,
    runtime: Option<Arc<RuntimeSet>>,
    tenants: TenantMap,
    tasks: Arc<TaskTracker>,
    greeted: bool,
    initialized: bool,
}

impl Host {
    fn new(sink: EventSink) -> Self {
        Self {
            sink,
            runtime: None,
            tenants: Arc::new(RwLock::new(HashMap::new())),
            tasks: TaskTracker::new(),
            greeted: false,
            initialized: false,
        }
    }

    async fn dispatch(&mut self, envelope: ControlEnvelope) -> Result<bool> {
        let request_id = envelope.request_id;
        match envelope.message {
            ControlMessage::Hello(control) => self.hello(request_id, control)?,
            ControlMessage::Init(control) => self.init(request_id, control)?,
            ControlMessage::CreateTenant(control) => {
                self.require_running()?;
                self.create(request_id, control).await?;
            }
            ControlMessage::Command(control) => {
                self.require_running()?;
                self.command(request_id, control).await?;
            }
            ControlMessage::InjectFault(control) => {
                self.require_running()?;
                self.inject_fault(request_id, control).await?;
            }
            ControlMessage::Rollout(control) => {
                self.require_running()?;
                self.rollout(request_id, control).await?;
            }
            ControlMessage::Snapshot(control) => {
                self.require_running()?;
                self.snapshot(request_id, control).await?;
            }
            ControlMessage::SetDequeueGate(control) => {
                self.require_running()?;
                self.set_gate(request_id, control).await?;
            }
            ControlMessage::AuthorityProbe(control) => {
                self.require_running()?;
                self.authority(request_id, control)?;
            }
            ControlMessage::Quiesce(_) => {
                self.require_running()?;
                self.quiesce(request_id).await?;
            }
            ControlMessage::TeardownTenant(control) => {
                self.require_running()?;
                self.teardown(request_id, control).await?;
            }
            ControlMessage::Shutdown(_) => {
                self.require_running()?;
                self.shutdown(request_id).await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn hello(&mut self, request_id: RequestId, control: HelloControl) -> Result<()> {
        ensure!(!self.greeted, "duplicate hello");
        ensure!(
            control.expected_candidate == CandidateKind::Lunatic,
            "hello requested another candidate"
        );
        self.sink.emit(
            request_id,
            EventMessage::Hello(HelloEvent {
                candidate: CandidateKind::Lunatic,
                implementation_version: "embedded-v3-lunatic-1".into(),
            }),
        )?;
        self.greeted = true;
        Ok(())
    }

    fn init(&mut self, request_id: RequestId, control: InitControl) -> Result<()> {
        ensure!(
            self.greeted && !self.initialized,
            "init outside greeted state"
        );
        let artifact_root = env::current_dir()
            .context("resolve candidate process current directory")?
            .join("guest-artifacts");
        self.runtime = Some(RuntimeSet::load(&artifact_root)?);
        self.initialized = true;
        self.sink.emit(
            request_id,
            EventMessage::Initialized(InitializedEvent {
                run_id: control.run_id,
            }),
        )
    }

    fn require_running(&self) -> Result<()> {
        ensure!(self.initialized, "control before shared initialization");
        Ok(())
    }

    fn runtime(&self) -> Result<Arc<RuntimeSet>> {
        self.runtime
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("runtime is not initialized"))
    }

    async fn create(&self, request_id: RequestId, control: CreateTenantControl) -> Result<()> {
        let runtime = self.runtime()?;
        let artifact = runtime
            .artifact(&control.artifact_sha256)
            .ok_or_else(|| anyhow!("create references an unknown core artifact"))?;
        {
            let tenants = self.tenants.read().await;
            ensure!(
                !tenants.contains_key(&control.tenant_id.0),
                "tenant already exists"
            );
        }
        self.sink.emit(
            request_id,
            EventMessage::TenantCreated(TenantCreatedEvent {
                tenant_id: control.tenant_id,
                incarnation: control.incarnation,
            }),
        )?;
        let identity = activation_identity(
            control.tenant_id,
            control.incarnation,
            control.logical_version,
            control.build_id,
            &artifact,
        );
        let instance = runtime
            .instantiate(
                Arc::clone(&artifact),
                identity.clone(),
                request_id,
                true,
                0,
                self.sink.clone(),
            )
            .await?;
        let tenant = Arc::new(Tenant::new(TenantRuntime::new(
            identity, artifact, instance,
        )));
        let mut tenants = self.tenants.write().await;
        ensure!(
            tenants.insert(control.tenant_id.0, tenant).is_none(),
            "concurrent duplicate tenant creation"
        );
        drop(tenants);
        self.sink.emit(
            request_id,
            EventMessage::TenantReady(TenantReadyEvent {
                tenant_id: control.tenant_id,
                incarnation: control.incarnation,
            }),
        )
    }

    async fn command(&self, request_id: RequestId, control: CommandControl) -> Result<()> {
        let tenant = self.tenants.read().await.get(&control.tenant_id.0).cloned();
        let Some(tenant) = tenant else {
            return self.reject(request_id, &control, RejectionReason::TenantMissing);
        };
        let ticket = match tenant.admit(&control)? {
            Admission::Accepted(ticket) => ticket,
            Admission::StaleIncarnation => {
                return self.reject(request_id, &control, RejectionReason::StaleIncarnation)
            }
            Admission::PayloadTooLarge => {
                return self.reject(request_id, &control, RejectionReason::PayloadTooLarge)
            }
            Admission::CommandIdConflict => {
                return self.reject(request_id, &control, RejectionReason::CommandIdConflict)
            }
            Admission::Backpressure => {
                return self.reject(request_id, &control, RejectionReason::Backpressure)
            }
        };
        self.sink.emit(
            request_id,
            EventMessage::CommandAccepted(CommandAcceptedEvent {
                tenant_id: control.tenant_id,
                incarnation: control.incarnation,
                command_id: control.command_id,
            }),
        )?;
        let sink = self.sink.clone();
        self.tasks.spawn(async move {
            let execution = tenant.execute(ticket, request_id, &control).await;
            let message = match execution {
                Ok(result) => EventMessage::CommandCompleted(CommandCompletedEvent {
                    tenant_id: control.tenant_id,
                    incarnation: control.incarnation,
                    command_id: control.command_id,
                    result,
                }),
                Err(error) => EventMessage::CommandFailed(CommandFailedEvent {
                    tenant_id: control.tenant_id,
                    incarnation: control.incarnation,
                    command_id: control.command_id,
                    error: error.to_string(),
                }),
            };
            let _ = sink.emit(request_id, message);
            let _ = tenant.finish_command(ticket);
        });
        Ok(())
    }

    fn reject(
        &self,
        request_id: RequestId,
        control: &CommandControl,
        reason: RejectionReason,
    ) -> Result<()> {
        self.sink.emit(
            request_id,
            EventMessage::CommandRejected(CommandRejectedEvent {
                tenant_id: control.tenant_id,
                incarnation: control.incarnation,
                command_id: control.command_id,
                reason,
            }),
        )
    }

    async fn inject_fault(&self, request_id: RequestId, control: InjectFaultControl) -> Result<()> {
        let tenant = self
            .tenants
            .read()
            .await
            .get(&control.tenant_id.0)
            .cloned()
            .ok_or_else(|| anyhow!("fault target is missing"))?;
        ensure!(
            tenant.incarnation()? == control.incarnation,
            "stale fault incarnation"
        );
        ensure!(
            tenant.outstanding()? == 0,
            "fault target has outstanding commands"
        );
        let runtime = self.runtime()?;
        let artifact = runtime
            .artifact(&control.replacement_artifact_sha256)
            .ok_or_else(|| anyhow!("fault replacement artifact is unknown"))?;
        let sink = self.sink.clone();
        self.tasks.spawn(async move {
            if let Err(error) =
                fault_task(request_id, control, tenant, runtime, artifact, sink.clone()).await
            {
                let _ = sink.emit(
                    RequestId(0),
                    EventMessage::Fatal(FatalEvent {
                        code: "fault_recovery_failure".into(),
                        message: error.to_string(),
                    }),
                );
            }
        });
        Ok(())
    }

    async fn rollout(&self, request_id: RequestId, control: RolloutControl) -> Result<()> {
        let runtime = self.runtime()?;
        let artifact = match runtime.artifact(&control.artifact_sha256) {
            Some(artifact) if runtime.artifact_matches_ref(&artifact, &control.artifact_ref) => {
                artifact
            }
            _ => {
                return self.sink.emit(
                    request_id,
                    EventMessage::RolloutRejected(RolloutRejectedEvent {
                        rollout_id: control.rollout_id,
                        reason: "unknown_or_mismatched_artifact".into(),
                    }),
                )
            }
        };
        let mut targets = Vec::with_capacity(control.targets.len());
        let tenants = self.tenants.read().await;
        for target in &control.targets {
            let Some(tenant) = tenants.get(&target.0).cloned() else {
                drop(tenants);
                return self.sink.emit(
                    request_id,
                    EventMessage::RolloutRejected(RolloutRejectedEvent {
                        rollout_id: control.rollout_id,
                        reason: "target_missing".into(),
                    }),
                );
            };
            targets.push((target.to_owned(), tenant));
        }
        drop(tenants);
        self.sink.emit(
            request_id,
            EventMessage::RolloutStarted(RolloutStartedEvent {
                rollout_id: control.rollout_id.clone(),
            }),
        )?;
        let sink = self.sink.clone();
        self.tasks.spawn(async move {
            if let Err(error) = rollout_task(
                request_id,
                control,
                targets,
                runtime,
                artifact,
                sink.clone(),
            )
            .await
            {
                let _ = sink.emit(
                    RequestId(0),
                    EventMessage::Fatal(FatalEvent {
                        code: "rollout_driver_failure".into(),
                        message: error.to_string(),
                    }),
                );
            }
        });
        Ok(())
    }

    async fn snapshot(&self, request_id: RequestId, control: SnapshotControl) -> Result<()> {
        let tenant = self.tenants.read().await.get(&control.tenant_id.0).cloned();
        let Some(tenant) = tenant else {
            return self.sink.emit(
                request_id,
                EventMessage::SnapshotMissing(SnapshotMissingEvent {
                    tenant_id: control.tenant_id,
                }),
            );
        };
        let actual = tenant.incarnation()?;
        if control
            .expected_incarnation
            .is_some_and(|expected| expected != actual)
        {
            return self.sink.emit(
                request_id,
                EventMessage::SnapshotStale(SnapshotStaleEvent {
                    tenant_id: control.tenant_id,
                    expected_incarnation: control.expected_incarnation.unwrap(),
                    actual_incarnation: actual,
                }),
            );
        }
        let state = tenant.snapshot(request_id).await?;
        self.sink.emit(
            request_id,
            EventMessage::SnapshotPresent(SnapshotPresentEvent {
                tenant_id: control.tenant_id,
                incarnation: actual,
                state,
            }),
        )
    }

    async fn set_gate(&self, request_id: RequestId, control: SetDequeueGateControl) -> Result<()> {
        let tenant = self
            .tenants
            .read()
            .await
            .get(&control.tenant_id.0)
            .cloned()
            .ok_or_else(|| anyhow!("gate target missing"))?;
        tenant.set_gate(control.incarnation, control.closed)?;
        self.sink.emit(
            request_id,
            EventMessage::DequeueGateSet(DequeueGateSetEvent {
                tenant_id: control.tenant_id,
                incarnation: control.incarnation,
                closed: control.closed,
            }),
        )?;
        if !control.closed {
            tenant.release_gate_waiters();
        }
        Ok(())
    }

    fn authority(&self, request_id: RequestId, control: AuthorityProbeControl) -> Result<()> {
        self.sink.emit(
            request_id,
            EventMessage::AuthorityProbeAccepted(AuthorityProbeAcceptedEvent {
                attempt_id: control.attempt_id,
                probe: control.probe,
                artifact_sha256: control.artifact_sha256.clone(),
                parameter_sha256: control.parameter_sha256.clone(),
                production_policy_sha256: control.production_policy_sha256.clone(),
            }),
        )?;
        let error = match self.runtime()?.compile_authority(control.artifact_bytes) {
            Ok(()) => return Err(anyhow!("an authority import unexpectedly linked")),
            Err(error) => error,
        };
        let detail = format!("{error:#}");
        ensure!(
            detail.contains("unknown import") || detail.contains("has not been defined"),
            "authority module failed before genuine import linking: {detail}"
        );
        self.sink.emit(
            request_id,
            EventMessage::AuthorityProbeTerminal(AuthorityProbeTerminalEvent {
                attempt_id: control.attempt_id,
                probe: control.probe,
                artifact_sha256: control.artifact_sha256,
                parameter_sha256: control.parameter_sha256,
                production_policy_sha256: control.production_policy_sha256,
                stage: AuthorityProbeStage::Compile,
                result: AuthorityProbeResult::AbsentAtLink,
                error_class: AuthorityProbeErrorClass::UnknownImport,
                guest_return_sha256: EMPTY_SHA256.into(),
            }),
        )
    }

    async fn quiesce(&self, request_id: RequestId) -> Result<()> {
        self.tasks.wait_idle().await;
        for tenant in self.tenants.read().await.values() {
            ensure!(
                tenant.outstanding()? == 0,
                "quiesce found accepted commands"
            );
        }
        self.sink
            .emit(request_id, EventMessage::Quiesced(QuiescedEvent {}))
    }

    async fn teardown(&self, request_id: RequestId, control: TeardownTenantControl) -> Result<()> {
        let mut tenants = self.tenants.write().await;
        let tenant = tenants
            .get(&control.tenant_id.0)
            .ok_or_else(|| anyhow!("teardown target missing"))?;
        ensure!(
            tenant.incarnation()? == control.incarnation,
            "stale teardown incarnation"
        );
        ensure!(
            tenant.outstanding()? == 0,
            "teardown target has outstanding commands"
        );
        tenants.remove(&control.tenant_id.0);
        drop(tenants);
        self.sink.emit(
            request_id,
            EventMessage::TenantTornDown(TenantTornDownEvent {
                tenant_id: control.tenant_id,
                incarnation: control.incarnation,
            }),
        )
    }

    async fn shutdown(&self, request_id: RequestId) -> Result<()> {
        self.tasks.wait_idle().await;
        ensure!(
            self.tenants.read().await.is_empty(),
            "shutdown with active tenants"
        );
        self.sink.emit(
            request_id,
            EventMessage::ShutdownComplete(ShutdownCompleteEvent {}),
        )
    }
}

async fn fault_task(
    request_id: RequestId,
    control: InjectFaultControl,
    tenant: Arc<Tenant>,
    runtime_set: Arc<RuntimeSet>,
    artifact: Arc<crate::runtime::Artifact>,
    sink: EventSink,
) -> Result<()> {
    let mut runtime = tenant.lock_runtime().await;
    let handle = i64::try_from(control.fault_id.0).context("fault_id exceeds guest handle ABI")?;
    runtime.instance.state_mut().begin_fault(
        handle,
        request_id,
        control.fault_id,
        control.incarnation,
    );
    let reason = match control.fault {
        FaultKind::Trap(_) => {
            ensure!(
                runtime.instance.call_ref("trap", Vec::new()).await.is_err(),
                "trap export unexpectedly returned"
            );
            FaultFailureReason::GuestTrap
        }
        FaultKind::CpuHog(_) => {
            let call = runtime.instance.call_ref("cpu_loop", Vec::new());
            ensure!(
                tokio::time::timeout(Duration::from_millis(50), call)
                    .await
                    .is_err(),
                "CPU loop returned before its deadline"
            );
            FaultFailureReason::CpuDeadline
        }
    };
    sink.emit(
        request_id,
        EventMessage::ExecutionFailed(ExecutionFailedEvent {
            fault_id: control.fault_id,
            tenant_id: control.tenant_id,
            incarnation: control.incarnation,
            reason,
        }),
    )?;
    sink.emit(
        request_id,
        EventMessage::FailureObserved(FailureObservedEvent {
            fault_id: control.fault_id,
            tenant_id: control.tenant_id,
            failed_incarnation: control.incarnation,
        }),
    )?;
    let identity = activation_identity(
        control.tenant_id,
        control.expected_replacement_incarnation,
        control.replacement_logical_version,
        control.replacement_build_id,
        &artifact,
    );
    let instance = runtime_set
        .instantiate(
            Arc::clone(&artifact),
            identity.clone(),
            request_id,
            true,
            0,
            sink.clone(),
        )
        .await?;
    runtime.identity = identity;
    runtime.artifact = artifact;
    runtime.instance = instance;
    runtime.clear_completed();
    tenant.reset_incarnation(control.expected_replacement_incarnation)?;
    drop(runtime);
    sink.emit(
        request_id,
        EventMessage::TenantReady(TenantReadyEvent {
            tenant_id: control.tenant_id,
            incarnation: control.expected_replacement_incarnation,
        }),
    )
}

struct RollbackRecord {
    tenant_id: TenantId,
    tenant: Arc<Tenant>,
    identity: crate::state::ActivationIdentity,
    artifact: Arc<crate::runtime::Artifact>,
}

async fn rollout_task(
    request_id: RequestId,
    control: RolloutControl,
    targets: Vec<(TenantId, Arc<Tenant>)>,
    runtime_set: Arc<RuntimeSet>,
    artifact: Arc<crate::runtime::Artifact>,
    sink: EventSink,
) -> Result<()> {
    let mut migrated = Vec::with_capacity(targets.len());
    for (tenant_id, tenant) in targets {
        let mut current = tenant.lock_runtime().await;
        ensure!(
            current.identity.logical_version == control.from_version,
            "rollout from_version does not match running tenant"
        );
        let old_identity = current.identity.clone();
        let old_artifact = Arc::clone(&current.artifact);
        let memory = current.instance.snapshot_memory()?;
        let new_identity = activation_identity(
            tenant_id,
            old_identity.incarnation,
            control.to_version.clone(),
            control.to_build_id.clone(),
            &artifact,
        );
        let replacement = runtime_set
            .instantiate(
                Arc::clone(&artifact),
                new_identity.clone(),
                request_id,
                true,
                0,
                sink.clone(),
            )
            .await;
        let mut replacement = match replacement {
            Ok(replacement) => replacement,
            Err(error) => {
                drop(current);
                if let Err(rollback_error) =
                    rollback(request_id, migrated, Arc::clone(&runtime_set), sink.clone()).await
                {
                    return sink.emit(
                        request_id,
                        EventMessage::RolloutInDoubt(RolloutInDoubtEvent {
                            rollout_id: control.rollout_id,
                            reason: format!(
                                "activation failed: {error}; rollback failed: {rollback_error}"
                            ),
                        }),
                    );
                }
                return sink.emit(
                    request_id,
                    EventMessage::RolloutRolledBack(RolloutRolledBackEvent {
                        rollout_id: control.rollout_id,
                        restored_version: control.from_version,
                        reason: format!("activation_failed: {error}"),
                    }),
                );
            }
        };
        replacement.restore_memory(&memory)?;
        current.identity = new_identity.clone();
        current.artifact = Arc::clone(&artifact);
        current.instance = replacement;
        migrated.push(RollbackRecord {
            tenant_id,
            tenant: Arc::clone(&tenant),
            identity: old_identity,
            artifact: old_artifact,
        });
        drop(current);
        sink.emit(
            request_id,
            EventMessage::RolloutTargetReady(RolloutTargetReadyEvent {
                rollout_id: control.rollout_id.clone(),
                tenant_id,
                incarnation: new_identity.incarnation,
                logical_version: new_identity.logical_version,
                build_id: new_identity.build_id,
                artifact_sha256: new_identity.artifact_sha256,
            }),
        )?;
    }
    sink.emit(
        request_id,
        EventMessage::RolloutCommitted(RolloutCommittedEvent {
            rollout_id: control.rollout_id,
            version: control.to_version,
        }),
    )
}

async fn rollback(
    request_id: RequestId,
    mut records: Vec<RollbackRecord>,
    runtime_set: Arc<RuntimeSet>,
    sink: EventSink,
) -> Result<()> {
    while let Some(record) = records.pop() {
        let mut current = record.tenant.lock_runtime().await;
        let memory = current.instance.snapshot_memory()?;
        let mut replacement = runtime_set
            .instantiate(
                Arc::clone(&record.artifact),
                record.identity.clone(),
                request_id,
                false,
                0,
                sink.clone(),
            )
            .await?;
        replacement.restore_memory(&memory)?;
        current.identity = record.identity;
        current.artifact = record.artifact;
        current.instance = replacement;
        ensure!(
            current.identity.tenant_id == record.tenant_id,
            "rollback restored another tenant identity"
        );
    }
    Ok(())
}
