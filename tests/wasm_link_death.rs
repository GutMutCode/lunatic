use std::{
    collections::HashMap,
    future::pending,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use lunatic_otp_patterns::{
    ChildSpec, ChildType, RestartPolicy, RestartStrategy, ShutdownPolicy, Supervisor,
    SupervisorSpec,
};
use lunatic_process::{
    env::{Environment, LunaticEnvironment},
    link_processes,
    message::{DataMessage, Message},
    runtimes::{
        wasmtime::{default_config, WasmtimeCompiledModule, WasmtimeRuntime},
        RawWasm,
    },
    spawn_native,
    state::{mailboxes_with_capacity, ProcessState},
    unlink_process,
    wasm::spawn_wasm,
    DeathReason, Process, Signal, WasmProcess,
};
use lunatic_runtime::{state::DefaultProcessState, DefaultProcessConfig};
use tokio::{
    sync::{mpsc, oneshot, Notify, RwLock},
    task::JoinHandle,
    time::timeout,
};
use wasmtime::{Linker, Val};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const MISSING_PROCESS_ID: u64 = 9_999_999;

const LIFECYCLE_GUEST: &str = r#"
(module
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::message" "send" (func $send (param i64) (result i32)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))
    (import "lunatic::message" "get_tag" (func $get_tag (result i64)))
    (import "lunatic::message" "get_process_id" (func $get_process_id (result i64)))
    (import "lunatic::process" "die_when_link_dies" (func $die_when_link_dies (param i32)))
    (import "lunatic::process" "link" (func $link (param i64 i64)))
    (import "lunatic::process" "monitor" (func $monitor (param i64)))
    (import "lunatic::process" "sleep_ms" (func $sleep_ms (param i64)))
    (import "test" "panic" (func $host_panic))

    (func $ready (param $observer i64)
        (call $create_data (i64.const 0) (i64.const 0))
        (drop (call $send (local.get $observer))))

    (func $wait (result i32)
        (call $receive (i32.const 0) (i32.const 0) (i64.const -1)))

    (func (export "normal") (param $observer i64)
        (call $ready (local.get $observer))
        (drop (call $wait)))

    (func (export "guest_trap") (param $observer i64)
        (call $ready (local.get $observer))
        (drop (call $wait))
        unreachable)

    (func (export "host_panic") (param $observer i64)
        (call $ready (local.get $observer))
        (drop (call $wait))
        (call $host_panic))

    (func (export "wait_for_kill") (param $observer i64)
        (call $ready (local.get $observer))
        (drop (call $wait)))

    (func (export "sleep_for_kill") (param $observer i64)
        (call $ready (local.get $observer))
        (call $sleep_ms (i64.const 60000)))

    (func (export "no_process") (param $observer i64) (param $missing i64)
        (call $ready (local.get $observer))
        (drop (call $wait))
        (call $link (i64.const 404) (local.get $missing))
        (drop (call $wait))
        unreachable)

    (func (export "trap_no_process") (param $observer i64) (param $missing i64)
        (local $kind i32)
        (call $die_when_link_dies (i32.const 0))
        (call $ready (local.get $observer))
        (drop (call $wait))
        (call $link (i64.const 404) (local.get $missing))
        (local.set $kind (call $wait))
        (if (i32.ne (local.get $kind) (i32.const 1)) (then unreachable))
        (if (i64.ne (call $get_tag) (i64.const 404)) (then unreachable)))

    (func (export "missing_monitor") (param $observer i64) (param $missing i64)
        (local $kind i32)
        (call $ready (local.get $observer))
        (drop (call $wait))
        (call $monitor (local.get $missing))
        (local.set $kind (call $wait))
        (if (i32.ne (local.get $kind) (i32.const 2)) (then unreachable))
        (if (i64.ne (call $get_process_id) (local.get $missing)) (then unreachable)))

    (func (export "default_peer") (param $observer i64)
        (call $ready (local.get $observer))
        (drop (call $wait)))

    (func (export "trap_peer") (param $observer i64) (param $expected_tag i64)
        (local $kind i32)
        (call $die_when_link_dies (i32.const 0))
        (call $ready (local.get $observer))
        (local.set $kind (call $wait))
        (if (i32.ne (local.get $kind) (i32.const 1)) (then unreachable))
        (if (i64.ne (call $get_tag) (local.get $expected_tag)) (then unreachable)))

    (func (export "fail_now")
        unreachable)

    (func (export "supervised_wait")
        (call $sleep_ms (i64.const 60000)))
)
"#;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Observed {
    LinkDied {
        process_id: u64,
        tag: Option<i64>,
        removed_before_notification: bool,
    },
    ProcessDied {
        process_id: u64,
        reason: DeathReason,
        removed_before_notification: bool,
    },
}

struct RecordingProcess {
    id: u64,
    ready: AtomicBool,
    notify: Notify,
}

impl RecordingProcess {
    fn new(id: u64) -> Self {
        Self {
            id,
            ready: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    async fn wait_until_ready(&self) -> Result<()> {
        timeout(TEST_TIMEOUT, async {
            loop {
                let notified = self.notify.notified();
                if self.ready.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        })
        .await
        .context("Wasm guest did not reach the ready barrier")?;
        Ok(())
    }
}

impl Process for RecordingProcess {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(
        &self,
        signal: Signal,
    ) -> std::result::Result<(), lunatic_process::state::SignalSendError> {
        if matches!(signal, Signal::Message(Message::Data(_))) {
            self.ready.store(true, Ordering::Release);
            self.notify.notify_one();
        }
        Ok(())
    }
}

struct BoundedLifecycleObserver {
    process: Arc<dyn Process>,
    join: JoinHandle<Result<()>>,
    target_id: Arc<AtomicU64>,
    events: mpsc::Receiver<Observed>,
}

impl BoundedLifecycleObserver {
    async fn spawn(environment: Arc<LunaticEnvironment>) -> Result<Self> {
        let target_id = Arc::new(AtomicU64::new(0));
        let task_target_id = target_id.clone();
        let task_environment = environment.clone();
        let (events_sender, events) = mpsc::channel(4);
        let (initialized_sender, initialized) = oneshot::channel();
        let (join, process) = spawn_native(environment, move |_, mailbox| async move {
            let initial = mailbox.pop(None).await;
            if !matches!(initial, Message::Data(_)) {
                bail!("lifecycle observer received an unexpected initialization message");
            }
            initialized_sender
                .send(())
                .map_err(|_| anyhow!("lifecycle observer initialization receiver disappeared"))?;

            loop {
                let event = match mailbox.pop(None).await {
                    Message::LinkDied(tag) => {
                        let process_id = task_target_id.load(Ordering::Acquire);
                        if process_id == 0 {
                            bail!("link notification arrived before target identity was installed");
                        }
                        Observed::LinkDied {
                            process_id,
                            tag,
                            removed_before_notification: task_environment
                                .get_process(process_id)
                                .is_none(),
                        }
                    }
                    Message::ProcessDied { process_id, reason } => Observed::ProcessDied {
                        process_id,
                        reason,
                        removed_before_notification: task_environment
                            .get_process(process_id)
                            .is_none(),
                    },
                    _ => continue,
                };
                events_sender
                    .send(event)
                    .await
                    .map_err(|_| anyhow!("lifecycle event receiver disappeared"))?;
            }
        })?;
        let process: Arc<dyn Process> = Arc::new(process);

        process
            .send(Signal::DieWhenLinkDies(false))
            .map_err(|error| anyhow!(error.to_string()))?;
        process
            .send(Signal::Message(Message::Data(DataMessage::default())))
            .map_err(|error| anyhow!(error.to_string()))?;
        timeout(TEST_TIMEOUT, initialized)
            .await
            .context("lifecycle observer did not initialize")?
            .context("lifecycle observer stopped during initialization")?;

        Ok(Self {
            process,
            join,
            target_id,
            events,
        })
    }

    fn set_target(&self, process_id: u64) {
        self.target_id
            .compare_exchange(0, process_id, Ordering::AcqRel, Ordering::Acquire)
            .expect("a lifecycle observer must be assigned exactly one target");
    }

    async fn assert_lifecycle(
        &mut self,
        process_id: u64,
        expected_tag: Option<i64>,
        expected_reason: DeathReason,
    ) -> Result<()> {
        let expected_link_count = usize::from(expected_reason != DeathReason::Normal);
        let expected_event_count = expected_link_count + 1;
        let events = timeout(TEST_TIMEOUT, async {
            let mut events = Vec::with_capacity(expected_event_count);
            while events.len() < expected_event_count {
                events.push(
                    self.events
                        .recv()
                        .await
                        .context("lifecycle observer stopped before reporting target exit")?,
                );
            }
            Ok::<_, anyhow::Error>(events)
        })
        .await
        .context("lifecycle observer did not report target exit")??;

        let link_deaths: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Observed::LinkDied {
                    process_id: observed_id,
                    tag,
                    removed_before_notification,
                } if *observed_id == process_id => Some((*tag, *removed_before_notification)),
                _ => None,
            })
            .collect();
        let expected_link_deaths = if expected_reason == DeathReason::Normal {
            Vec::new()
        } else {
            vec![(expected_tag, true)]
        };
        assert_eq!(
            link_deaths, expected_link_deaths,
            "link notification must preserve the tag and be sent once after removal"
        );

        let monitor_deaths: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Observed::ProcessDied {
                    process_id: observed_id,
                    reason,
                    removed_before_notification,
                } if *observed_id == process_id => Some((*reason, *removed_before_notification)),
                _ => None,
            })
            .collect();
        assert_eq!(
            monitor_deaths,
            vec![(expected_reason, true)],
            "duplicate monitor registration must preserve the reason and notify exactly once after removal"
        );
        assert!(
            !unlink_process(self.process.as_ref(), Some(self.process.id()), process_id)?,
            "target exit must remove the observer's reciprocal link exactly once"
        );

        assert!(
            timeout(Duration::from_millis(50), self.events.recv())
                .await
                .is_err(),
            "target exit must not emit duplicate lifecycle events"
        );
        Ok(())
    }

    async fn shutdown(self) -> Result<()> {
        let _ = self.process.send(Signal::Kill);
        let _ = wait_for_join(self.join).await?;
        Ok(())
    }
}

/// Registers the same monitor twice and waits until the second registration
/// replaces the first in the target's relation map. The acknowledgement is an
/// actual signal-processing barrier, which is required before requesting an
/// out-of-band Kill that intentionally preempts queued signals.
async fn register_monitor_and_wait(
    process: &dyn Process,
    observer: Arc<dyn Process>,
) -> Result<()> {
    let (acknowledgement, registered) = std::sync::mpsc::sync_channel(1);

    process
        .send(Signal::Monitor {
            process: observer.clone(),
            acknowledgement: None,
        })
        .map_err(|error| anyhow!(error.to_string()))?;
    process
        .send(Signal::Monitor {
            process: observer,
            acknowledgement: Some(acknowledgement),
        })
        .map_err(|error| anyhow!(error.to_string()))?;
    registered
        .recv_timeout(TEST_TIMEOUT)
        .context("target did not process the monitor registration barrier")?;
    Ok(())
}

struct WasmHarness {
    environment: Arc<LunaticEnvironment>,
    runtime: WasmtimeRuntime,
    module: Arc<WasmtimeCompiledModule<DefaultProcessState>>,
    registry: Arc<RwLock<HashMap<String, (u64, u64)>>>,
}

static SUPERVISED_WASM_HARNESS: OnceLock<WasmHarness> = OnceLock::new();
static SUPERVISED_WASM_STARTS: AtomicUsize = AtomicUsize::new(0);
const SUPERVISED_WASM_FAILURES_BEFORE_STABLE: usize = 3;

impl WasmHarness {
    fn new() -> Result<Self> {
        let runtime = WasmtimeRuntime::new(&default_config())?;
        let raw = RawWasm::new(None, wat::parse_str(LIFECYCLE_GUEST)?);
        let module = wasmtime::Module::new(runtime.engine(), raw.as_slice())?;
        let mut linker = Linker::<DefaultProcessState>::new(runtime.engine());
        <DefaultProcessState as ProcessState>::register(&mut linker)?;
        linker.func_wrap::<_, ()>("test", "panic", || {
            panic!("intentional host panic for lifecycle verification")
        })?;
        let instance_pre = linker.instantiate_pre(&module)?;
        let module = Arc::new(WasmtimeCompiledModule::new(raw, module, instance_pre));

        Ok(Self {
            environment: Arc::new(LunaticEnvironment::new(7)),
            runtime,
            module,
            registry: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn recorder(&self) -> Arc<RecordingProcess> {
        let id = self.environment.get_next_process_id();
        let recorder = Arc::new(RecordingProcess::new(id));
        self.environment.add_process(id, recorder.clone()).unwrap();
        recorder
    }

    async fn spawn(
        &self,
        function: &str,
        params: Vec<Val>,
        link: Option<(Option<i64>, Arc<dyn Process>)>,
    ) -> Result<(JoinHandle<Result<DefaultProcessState>>, Arc<dyn Process>)> {
        let state = DefaultProcessState::new(
            self.environment.clone(),
            None,
            self.runtime.clone(),
            self.module.clone(),
            Arc::new(DefaultProcessConfig::default()),
            self.registry.clone(),
        )?;
        spawn_wasm(
            self.environment.clone(),
            self.runtime.clone(),
            &self.module,
            state,
            function,
            params,
            link,
        )
        .await
    }
}

fn start_supervised_wasm(
    environment: Arc<dyn Environment>,
) -> std::result::Result<Arc<dyn Process>, String> {
    let harness = SUPERVISED_WASM_HARNESS
        .get()
        .ok_or_else(|| "supervised Wasm harness was not initialized".to_string())?;
    let function = if SUPERVISED_WASM_STARTS.fetch_add(1, Ordering::SeqCst)
        < SUPERVISED_WASM_FAILURES_BEFORE_STABLE
    {
        "fail_now"
    } else {
        "supervised_wait"
    };
    if environment.id() != harness.environment.id() {
        return Err("Supervisor used an unexpected Wasm environment".to_string());
    }
    let state = DefaultProcessState::new(
        harness.environment.clone(),
        None,
        harness.runtime.clone(),
        harness.module.clone(),
        Arc::new(DefaultProcessConfig::default()),
        harness.registry.clone(),
    )
    .map_err(|error| error.to_string())?;

    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            let (join, process) = spawn_wasm(
                environment,
                harness.runtime.clone(),
                &harness.module,
                state,
                function,
                Vec::new(),
                None,
            )
            .await
            .map_err(|error| error.to_string())?;
            if function == "fail_now" {
                let result = join.await.map_err(|error| {
                    format!("trapped Wasm task failed outside its runner: {error}")
                })?;
                if result.is_ok() {
                    return Err("the supervised Wasm trap exited normally".to_string());
                }
            }
            Ok(process)
        })
    })
}

#[derive(Clone, Copy)]
enum Trigger {
    Release,
    Kill,
}

#[derive(Clone, Copy)]
enum ExpectedOutcome {
    Success,
    Failure(&'static str),
}

async fn wait_for_join<T>(join: JoinHandle<Result<T>>) -> Result<Result<T>> {
    timeout(TEST_TIMEOUT, join)
        .await
        .context("process did not terminate before the lifecycle timeout")?
        .context("process task panicked outside the lifecycle runner")
}

async fn run_wasm_case(
    harness: &WasmHarness,
    function: &str,
    extra_params: Vec<Val>,
    trigger: Trigger,
    expected_outcome: ExpectedOutcome,
    expected_reason: DeathReason,
    link_tag: i64,
) -> Result<Option<DefaultProcessState>> {
    let recorder = harness.recorder();
    let mut lifecycle = BoundedLifecycleObserver::spawn(harness.environment.clone()).await?;
    let mut params = vec![Val::I64(recorder.id() as i64)];
    params.extend(extra_params);
    let (join, process) = harness
        .spawn(
            function,
            params,
            Some((Some(link_tag), lifecycle.process.clone())),
        )
        .await?;
    let process_id = process.id();
    lifecycle.set_target(process_id);

    recorder.wait_until_ready().await?;
    register_monitor_and_wait(process.as_ref(), lifecycle.process.clone()).await?;

    match trigger {
        Trigger::Release => process.send(Signal::Message(Message::Data(DataMessage::default()))),
        Trigger::Kill => process.send(Signal::Kill),
    }
    .map_err(|error| anyhow!(error.to_string()))?;

    let outcome = wait_for_join(join).await?;
    let state = match (expected_outcome, outcome) {
        (ExpectedOutcome::Success, Ok(state)) => Some(state),
        (ExpectedOutcome::Success, Err(error)) => {
            bail!("{function} should succeed, but failed: {error:#}")
        }
        (ExpectedOutcome::Failure(expected), Err(error)) => {
            let rendered = format!("{error:#}");
            assert!(
                rendered.contains(expected),
                "{} error `{}` did not contain `{}`",
                function,
                rendered,
                expected
            );
            None
        }
        (ExpectedOutcome::Failure(_), Ok(_)) => {
            bail!("{function} should fail, but returned successfully")
        }
    };

    lifecycle
        .assert_lifecycle(process_id, Some(link_tag), expected_reason)
        .await?;
    assert!(
        harness.environment.get_process(process_id).is_none(),
        "terminated process must be removed from the environment"
    );
    lifecycle.shutdown().await?;
    Ok(state)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_wasm_exit_matrix_preserves_link_and_monitor_semantics() -> Result<()> {
    let harness = WasmHarness::new()?;

    run_wasm_case(
        &harness,
        "normal",
        Vec::new(),
        Trigger::Release,
        ExpectedOutcome::Success,
        DeathReason::Normal,
        101,
    )
    .await?;
    run_wasm_case(
        &harness,
        "guest_trap",
        Vec::new(),
        Trigger::Release,
        ExpectedOutcome::Failure("wasm backtrace"),
        DeathReason::Failure,
        102,
    )
    .await?;
    run_wasm_case(
        &harness,
        "host_panic",
        Vec::new(),
        Trigger::Release,
        ExpectedOutcome::Failure("Process panicked"),
        DeathReason::Failure,
        103,
    )
    .await?;
    run_wasm_case(
        &harness,
        "wait_for_kill",
        Vec::new(),
        Trigger::Kill,
        ExpectedOutcome::Failure("Process killed"),
        DeathReason::Failure,
        104,
    )
    .await?;
    run_wasm_case(
        &harness,
        "sleep_for_kill",
        Vec::new(),
        Trigger::Kill,
        ExpectedOutcome::Failure("Process killed"),
        DeathReason::Failure,
        105,
    )
    .await?;
    run_wasm_case(
        &harness,
        "no_process",
        vec![Val::I64(MISSING_PROCESS_ID as i64)],
        Trigger::Release,
        ExpectedOutcome::Failure("Process killed"),
        DeathReason::Failure,
        106,
    )
    .await?;

    let trap_state = run_wasm_case(
        &harness,
        "trap_no_process",
        vec![Val::I64(MISSING_PROCESS_ID as i64)],
        Trigger::Release,
        ExpectedOutcome::Success,
        DeathReason::Normal,
        107,
    )
    .await?
    .context("trap-exit guest should return its state")?;
    assert!(
        trap_state.message_mailbox().is_empty(),
        "the tagged LinkDied message must be consumed exactly once"
    );

    let monitor_state = run_wasm_case(
        &harness,
        "missing_monitor",
        vec![Val::I64(MISSING_PROCESS_ID as i64)],
        Trigger::Release,
        ExpectedOutcome::Success,
        DeathReason::Normal,
        108,
    )
    .await?
    .context("missing-monitor guest should return its state")?;
    assert!(
        monitor_state.message_mailbox().is_empty(),
        "monitoring a missing process must enqueue exactly one ProcessDied message"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_link_capacity_failure_is_atomic_and_reusable() -> Result<()> {
    let harness = WasmHarness::new()?;
    let ((parent_sender, parent_receiver), _parent_mailbox) = mailboxes_with_capacity(1, 1);
    let ((filler_sender, _filler_receiver), _filler_mailbox) = mailboxes_with_capacity(1, 1);
    let parent: Arc<dyn Process> = Arc::new(WasmProcess::new(70_000, parent_sender));
    let filler: Arc<dyn Process> = Arc::new(WasmProcess::new(70_001, filler_sender));
    link_processes(
        parent.as_ref(),
        Some(parent.id()),
        None,
        filler.as_ref(),
        Some(filler.id()),
        None,
    )?;

    let error = match harness
        .spawn("fail_now", Vec::new(), Some((Some(777), parent.clone())))
        .await
    {
        Ok(_) => bail!("spawn-link should fail when the parent link capacity is exhausted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("link capacity is exhausted"));
    assert_eq!(
        harness.environment.process_count(),
        0,
        "failed spawn-link must remove its provisional child registration"
    );
    assert!(
        unlink_process(parent.as_ref(), Some(parent.id()), filler.id())?,
        "failed spawn-link must preserve the parent's pre-existing relation"
    );

    let (join, child) = harness
        .spawn("fail_now", Vec::new(), Some((Some(777), parent.clone())))
        .await?;
    let child_id = child.id();
    assert!(wait_for_join(join).await?.is_err());
    assert!(matches!(
        timeout(TEST_TIMEOUT, parent_receiver.recv())
            .await
            .context("successful spawn-link did not report child failure")?
            .context("spawn-link parent receiver closed")?
            .into_signal(),
        Signal::LinkDied(id, Some(777), DeathReason::Failure) if id == child_id
    ));
    assert!(
        !unlink_process(parent.as_ref(), Some(parent.id()), child_id)?,
        "child exit must already have removed the reciprocal relation"
    );
    assert_eq!(harness.environment.process_count(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_wasm_peer_honors_default_and_trap_exit_link_modes() -> Result<()> {
    let harness = WasmHarness::new()?;

    let default_observer = harness.recorder();
    let mut default_lifecycle =
        BoundedLifecycleObserver::spawn(harness.environment.clone()).await?;
    let (default_join, default_peer) = harness
        .spawn(
            "default_peer",
            vec![Val::I64(default_observer.id() as i64)],
            Some((Some(201), default_lifecycle.process.clone())),
        )
        .await?;
    let default_peer_id = default_peer.id();
    default_lifecycle.set_target(default_peer_id);
    default_observer.wait_until_ready().await?;
    register_monitor_and_wait(default_peer.as_ref(), default_lifecycle.process.clone()).await?;

    let (failing_join, _) = harness
        .spawn(
            "fail_now",
            Vec::new(),
            Some((Some(9001), default_peer.clone())),
        )
        .await?;
    assert!(wait_for_join(failing_join).await?.is_err());
    let default_result = wait_for_join(default_join).await?;
    let default_error = match default_result {
        Ok(_) => bail!("default linked Wasm peer should terminate after child failure"),
        Err(error) => error,
    };
    assert!(format!("{default_error:#}").contains("Process killed"));
    default_lifecycle
        .assert_lifecycle(default_peer_id, Some(201), DeathReason::Failure)
        .await?;
    default_lifecycle.shutdown().await?;

    let trap_observer = harness.recorder();
    let mut trap_lifecycle = BoundedLifecycleObserver::spawn(harness.environment.clone()).await?;
    let (trap_join, trap_peer) = harness
        .spawn(
            "trap_peer",
            vec![Val::I64(trap_observer.id() as i64), Val::I64(9002)],
            Some((Some(202), trap_lifecycle.process.clone())),
        )
        .await?;
    let trap_peer_id = trap_peer.id();
    trap_lifecycle.set_target(trap_peer_id);
    trap_observer.wait_until_ready().await?;
    register_monitor_and_wait(trap_peer.as_ref(), trap_lifecycle.process.clone()).await?;

    let (failing_join, _) = harness
        .spawn(
            "fail_now",
            Vec::new(),
            Some((Some(9002), trap_peer.clone())),
        )
        .await?;
    assert!(wait_for_join(failing_join).await?.is_err());
    let trap_state = wait_for_join(trap_join).await??;
    assert!(
        trap_state.message_mailbox().is_empty(),
        "trap-exit peer must consume the tagged LinkDied message"
    );
    trap_lifecycle
        .assert_lifecycle(trap_peer_id, Some(202), DeathReason::Normal)
        .await?;
    trap_lifecycle.shutdown().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_wasm_trap_drives_supervisor_restart_policy() -> Result<()> {
    let harness = WasmHarness::new()?;
    let environment = harness.environment.clone();
    SUPERVISED_WASM_HARNESS
        .set(harness)
        .map_err(|_| anyhow!("supervised Wasm harness was initialized more than once"))?;
    SUPERVISED_WASM_STARTS.store(0, Ordering::SeqCst);

    let supervisor = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: (SUPERVISED_WASM_FAILURES_BEFORE_STABLE + 1) as u32,
            max_seconds: 60,
            children: vec![ChildSpec {
                id: "wasm-worker".to_string(),
                start: start_supervised_wasm,
                restart: RestartPolicy::Permanent,
                shutdown: ShutdownPolicy::Brutal,
                child_type: ChildType::Worker,
            }],
        },
        environment.clone(),
    )
    .map_err(anyhow::Error::msg)?;

    let replacement = timeout(TEST_TIMEOUT, async {
        loop {
            if let Some(child) = supervisor.which_children().into_iter().find(|child| {
                child.id == "wasm-worker"
                    && child.restart_count == SUPERVISED_WASM_FAILURES_BEFORE_STABLE as u32
            }) {
                if let Some(process_id) = child.process_id {
                    break process_id;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let replacement_id = match replacement {
        Ok(process_id) => process_id,
        Err(error) => {
            let _ = supervisor.shutdown();
            return Err(error).context("Supervisor did not survive repeated trapped Wasm children");
        }
    };
    let replacement_was_active = environment.get_process(replacement_id).is_some();
    let start_count = SUPERVISED_WASM_STARTS.load(Ordering::SeqCst);
    let restart_history = supervisor.restart_history();

    supervisor.shutdown().map_err(anyhow::Error::msg)?;
    timeout(TEST_TIMEOUT, async {
        while environment.process_count() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| {
        format!(
            "Supervisor teardown retained {} registered processes after shutdown",
            environment.process_count()
        )
    })?;
    let registered_after_shutdown = environment.process_count();

    assert_eq!(
        start_count,
        SUPERVISED_WASM_FAILURES_BEFORE_STABLE + 1,
        "each immediate trap should cause one replacement before the stable child"
    );
    assert!(
        replacement_was_active,
        "the replacement Wasm child should remain active until supervisor shutdown"
    );
    assert_eq!(
        restart_history.len(),
        SUPERVISED_WASM_FAILURES_BEFORE_STABLE
    );
    assert!(restart_history
        .iter()
        .all(|(_, child_id)| child_id == "wasm-worker"));
    println!(
        "LUNATIC_RESILIENCE_EVIDENCE {{\"kind\":\"repeated_wasm_crash_restart\",\"failures\":{},\"starts\":{},\"stable_replacement_active\":true,\"registered_after_shutdown\":{registered_after_shutdown}}}",
        SUPERVISED_WASM_FAILURES_BEFORE_STABLE,
        start_count
    );
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum NativeOutcome {
    Normal,
    Error,
    Panic,
    Kill,
    NoProcess,
}

async fn run_native_case(outcome: NativeOutcome, link_tag: i64) -> Result<()> {
    let environment = Arc::new(LunaticEnvironment::new(8));
    let mut lifecycle = BoundedLifecycleObserver::spawn(environment.clone()).await?;
    let (release_sender, release_receiver) = oneshot::channel::<()>();

    let (join, process) =
        lunatic_process::spawn_native(environment.clone(), move |_, _| async move {
            match outcome {
                NativeOutcome::Normal => {
                    release_receiver
                        .await
                        .map_err(|_| anyhow!("native release sender disappeared"))?;
                    Ok(())
                }
                NativeOutcome::Error => {
                    release_receiver
                        .await
                        .map_err(|_| anyhow!("native release sender disappeared"))?;
                    Err(anyhow!("intentional native failure"))
                }
                NativeOutcome::Panic => {
                    let _ = release_receiver.await;
                    panic!("intentional native panic")
                }
                NativeOutcome::Kill | NativeOutcome::NoProcess => pending::<Result<()>>().await,
            }
        })?;
    let process_id = process.id();
    lifecycle.set_target(process_id);
    link_processes(
        &process,
        Some(process_id),
        Some(link_tag),
        lifecycle.process.as_ref(),
        Some(lifecycle.process.id()),
        None,
    )?;
    register_monitor_and_wait(&process, lifecycle.process.clone()).await?;

    match outcome {
        NativeOutcome::Normal | NativeOutcome::Error | NativeOutcome::Panic => release_sender
            .send(())
            .map_err(|_| anyhow!("native process exited before release"))?,
        NativeOutcome::Kill => process
            .send(Signal::Kill)
            .map_err(|error| anyhow!(error.to_string()))?,
        NativeOutcome::NoProcess => process
            .send(Signal::LinkDied(
                MISSING_PROCESS_ID,
                Some(404),
                DeathReason::NoProcess,
            ))
            .map_err(|error| anyhow!(error.to_string()))?,
    }

    let result = wait_for_join(join).await?;
    let expected_reason = match outcome {
        NativeOutcome::Normal => {
            if let Err(error) = result {
                bail!("native normal process failed: {error:#}");
            }
            DeathReason::Normal
        }
        NativeOutcome::Error => {
            let error = result.err().context("native error process should fail")?;
            assert!(format!("{error:#}").contains("intentional native failure"));
            DeathReason::Failure
        }
        NativeOutcome::Panic => {
            let error = result.err().context("native panic should be captured")?;
            assert!(format!("{error:#}").contains("Process panicked"));
            DeathReason::Failure
        }
        NativeOutcome::Kill | NativeOutcome::NoProcess => {
            let error = result
                .err()
                .context("native terminated process should fail")?;
            assert!(format!("{error:#}").contains("Process killed"));
            DeathReason::Failure
        }
    };

    lifecycle
        .assert_lifecycle(process_id, Some(link_tag), expected_reason)
        .await?;
    assert!(environment.get_process(process_id).is_none());
    lifecycle.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_and_wasm_runners_share_the_same_exit_contract() -> Result<()> {
    for (index, outcome) in [
        NativeOutcome::Normal,
        NativeOutcome::Error,
        NativeOutcome::Panic,
        NativeOutcome::Kill,
        NativeOutcome::NoProcess,
    ]
    .iter()
    .copied()
    .enumerate()
    {
        run_native_case(outcome, 300 + index as i64).await?;
    }
    Ok(())
}
