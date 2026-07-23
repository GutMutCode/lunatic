use std::{
    collections::HashMap,
    convert::TryInto,
    sync::Arc,
    time::{Duration, Instant as StdInstant},
};

use anyhow::{bail, Context, Result};
use lunatic_process::{
    env::{Environment, Environments, LunaticEnvironment, LunaticEnvironments},
    hot_reload::{ReloadCoordinator, ReloadStatus},
    message::{DataMessage, Message},
    module_registry::ModuleRegistry,
    runtimes::wasmtime::{default_config, WasmtimeCompiledModule, WasmtimeRuntime},
    state::SignalSendErrorKind,
    wasm::{spawn_wasm_with_options, WasmSpawnOptions},
    Process, ProcessReloadStatus, ReloadAcknowledgement, Signal,
};
use lunatic_runtime::{
    hot_reload::register_module_update, DefaultProcessConfig, DefaultProcessState,
};
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
    time::{timeout, timeout_at, Instant as TokioInstant},
};
use wasmtime::Val;

const MODULE_ID: u64 = 0;
const PROCESS_COUNT: usize = 16;
const ROUND_COUNT: usize = 10;
const MAILBOX_CAPACITY: usize = 8;
const SIGNAL_QUEUE_CAPACITY: u32 = 12;
const MESSAGE_ALLOCATION: usize = 1_024;

const PAUSE_TAG: i64 = -7_777;
const PROCESS_ID_TAG_BASE: i64 = 10_000_000;
const PAUSE_ACK_TAG_BASE: i64 = 20_000_000;
const MESSAGE_TAG_BASE: i64 = 30_000_000;
const BARRIER_TAG_BASE: i64 = 40_000_000;

const PAUSE_MS: u64 = 60_000;
const RESUME_DELAY_MS: u64 = 200;
const OPERATION_WATCHDOG: Duration = Duration::from_secs(2);
const REPORT_WATCHDOG: Duration = Duration::from_secs(2);
const LIFECYCLE_WATCHDOG: Duration = Duration::from_secs(10);
const PRODUCT_OBJECTIVE_WARNING: Duration = Duration::from_millis(100);

// These are deliberately conservative same-runner CI runaway guards. They are
// not a portable latency SLA and do not, by themselves, prove the product's
// sub-100 ms live-reload objective.
const COMMIT_CI_WATCHDOG: Duration = Duration::from_millis(250);
const ROLLBACK_CI_WATCHDOG: Duration = Duration::from_millis(500);

const GUEST_TEMPLATE: &str = r#"
(module
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::message" "send" (func $send (param i64) (result i32)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))
    (import "lunatic::message" "get_tag" (func $get_tag (result i64)))
    (import "lunatic::message" "write_data" (func $write_data (param i32 i32) (result i32)))
    (import "lunatic::process" "process_id" (func $process_id (result i64)))
    (import "lunatic::process" "sleep_ms" (func $sleep_ms (param i64)))

    (memory (export "memory") 1)

    __START_FUNCTION__

    (func $report (param $observer i64) (param $tag i64)
        (i64.store (i32.const 16) (call $process_id))
        (call $create_data (local.get $tag) (i64.const 8))
        (if
            (i32.ne (call $write_data (i32.const 16) (i32.const 8)) (i32.const 8))
            (then unreachable))
        (drop (call $send (local.get $observer))))

    (func (export "run") (param $observer i64)
        (local $tag i64)
        (if (i32.eqz (i32.load (i32.const 0)))
            (then
                (i32.store (i32.const 0) (i32.const 1))
                (i64.store (i32.const 8) (call $process_id))
                (call $report
                    (local.get $observer)
                    (i64.add
                        (i64.const __PROCESS_ID_TAG_BASE__)
                        (call $process_id)))))
        (if (i64.ne (i64.load (i32.const 8)) (call $process_id))
            (then unreachable))

        ;; A failed candidate remains suspended until rollback. Committed code
        ;; waits briefly before clearing the pause flag, giving the coordinator
        ;; a deterministic window to interrupt a PID-local apply failure before
        ;; any queued application message is consumed.
        (if (i32.load (i32.const 4))
            (then __RESUME_ACTION__))

        (loop $messages
            (drop (call $receive (i32.const 0) (i32.const 0) (i64.const -1)))
            (local.set $tag (call $get_tag))
            (if (i64.eq (local.get $tag) (i64.const __PAUSE_TAG__))
                (then
                    (i32.store (i32.const 4) (i32.const 1))
                    (call $report
                        (local.get $observer)
                        (i64.add
                            (i64.const __PAUSE_ACK_TAG_BASE__)
                            (call $process_id)))
                    (call $sleep_ms (i64.const __PAUSE_MS__)))
                (else
                    (call $report (local.get $observer) (local.get $tag))))
            (br $messages)))
)
"#;

struct RecordingProcess {
    id: u64,
    reports: mpsc::UnboundedSender<GuestReport>,
}

#[derive(Debug)]
struct GuestReport {
    tag: i64,
    process_id: u64,
}

impl Process for RecordingProcess {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(
        &self,
        signal: Signal,
    ) -> std::result::Result<(), lunatic_process::state::SignalSendError> {
        if let Signal::Message(Message::Data(message)) = signal {
            if let Some(tag) = message.tag {
                let process_id = message
                    .buffer
                    .as_slice()
                    .try_into()
                    .map(u64::from_le_bytes)
                    .unwrap_or(u64::MAX);
                let _ = self.reports.send(GuestReport { tag, process_id });
            }
        }
        Ok(())
    }
}

struct LiveProcess {
    join: JoinHandle<Result<DefaultProcessState>>,
    process: Arc<dyn Process>,
}

fn guest_module(trapping_process_id: Option<u64>, hold_until_rollback: bool) -> String {
    let start_function = match trapping_process_id {
        Some(process_id) => format!(
            r#"
            (func $reload_start
                (if (i64.eq (call $process_id) (i64.const {process_id}))
                    (then unreachable)))
            (start $reload_start)
            "#
        ),
        None => "(func $reload_start) (start $reload_start)".to_owned(),
    };
    let resume_action = if hold_until_rollback {
        format!("(call $sleep_ms (i64.const {PAUSE_MS}))")
    } else {
        format!(
            "(call $sleep_ms (i64.const {RESUME_DELAY_MS})) \
             (i32.store (i32.const 4) (i32.const 0))"
        )
    };

    GUEST_TEMPLATE
        .replace("__START_FUNCTION__", &start_function)
        .replace("__RESUME_ACTION__", &resume_action)
        .replace("__PROCESS_ID_TAG_BASE__", &PROCESS_ID_TAG_BASE.to_string())
        .replace("__PAUSE_TAG__", &PAUSE_TAG.to_string())
        .replace("__PAUSE_ACK_TAG_BASE__", &PAUSE_ACK_TAG_BASE.to_string())
        .replace("__PAUSE_MS__", &PAUSE_MS.to_string())
}

async fn spawn_live_process(
    environment: Arc<LunaticEnvironment>,
    runtime: &WasmtimeRuntime,
    module: &Arc<WasmtimeCompiledModule<DefaultProcessState>>,
    config: &Arc<DefaultProcessConfig>,
    observer_id: u64,
    version: u32,
) -> Result<LiveProcess> {
    let state = DefaultProcessState::new(
        environment.clone(),
        None,
        runtime.clone(),
        module.clone(),
        config.clone(),
        Arc::new(RwLock::new(HashMap::new())),
    )?;
    let (join, process) = spawn_wasm_with_options(
        environment,
        runtime.clone(),
        module,
        state,
        "run",
        vec![Val::I64(observer_id as i64)],
        WasmSpawnOptions {
            link: None,
            initial_module_version: Some((MODULE_ID, version)),
        },
    )
    .await?;
    Ok(LiveProcess { join, process })
}

fn tagged_message(tag: i64, allocation: usize) -> Signal {
    Signal::Message(Message::Data(DataMessage::new(Some(tag), allocation)))
}

fn message_tag(round: usize, process_index: usize, sequence: usize) -> i64 {
    MESSAGE_TAG_BASE + (round as i64 * 10_000) + (process_index as i64 * 100) + sequence as i64
}

fn barrier_tag(round: usize, process_index: usize) -> i64 {
    BARRIER_TAG_BASE + (round as i64 * 100) + process_index as i64
}

async fn expect_exact_report_set(
    receiver: &mut mpsc::UnboundedReceiver<GuestReport>,
    expected: impl IntoIterator<Item = (i64, u64)>,
    context: &str,
) -> Result<()> {
    let mut remaining = expected.into_iter().collect::<HashMap<_, _>>();
    let deadline = TokioInstant::now() + REPORT_WATCHDOG;
    while !remaining.is_empty() {
        let report = timeout_at(deadline, receiver.recv())
            .await
            .with_context(|| format!("timed out waiting for {context}; remaining={remaining:?}"))?
            .with_context(|| format!("observer closed while waiting for {context}"))?;
        let expected_process_id = remaining.remove(&report.tag).with_context(|| {
            format!(
                "duplicate or unexpected observer tag {} from process {} while waiting for {context}",
                report.tag, report.process_id
            )
        })?;
        if report.process_id != expected_process_id {
            bail!(
                "observer tag {} came from process {}, expected process {} while waiting for {context}",
                report.tag,
                report.process_id,
                expected_process_id
            );
        }
    }
    Ok(())
}

async fn send_barrier_with_backpressure(
    process: &Arc<dyn Process>,
    tag: i64,
    deadline: TokioInstant,
) -> Result<()> {
    loop {
        match process.send(tagged_message(tag, 0)) {
            Ok(()) => return Ok(()),
            Err(error)
                if matches!(
                    error.kind(),
                    SignalSendErrorKind::MailboxFull | SignalSendErrorKind::QueueFull
                ) =>
            {
                if TokioInstant::now() >= deadline {
                    bail!(
                        "timed out admitting barrier tag {tag} to process {}",
                        process.id()
                    );
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(error) => bail!(
                "barrier tag {tag} failed for process {}: {error}",
                process.id()
            ),
        }
    }
}

async fn expect_round_fifo(
    receiver: &mut mpsc::UnboundedReceiver<GuestReport>,
    round: usize,
    process_ids: &[u64],
) -> Result<()> {
    if process_ids.len() != PROCESS_COUNT {
        bail!(
            "round {round} expected {PROCESS_COUNT} process identities, got {}",
            process_ids.len()
        );
    }
    let mut messages = HashMap::new();
    let mut barriers = HashMap::new();
    for (process_index, expected_process_id) in process_ids.iter().copied().enumerate() {
        for sequence in 0..MAILBOX_CAPACITY {
            messages.insert(
                message_tag(round, process_index, sequence),
                (process_index, sequence, expected_process_id),
            );
        }
        barriers.insert(
            barrier_tag(round, process_index),
            (process_index, expected_process_id),
        );
    }

    let mut next_sequence = [0; PROCESS_COUNT];
    let deadline = TokioInstant::now() + REPORT_WATCHDOG;
    while !barriers.is_empty() {
        let report = timeout_at(deadline, receiver.recv())
            .await
            .with_context(|| {
                format!(
                    "round {round} timed out draining messages; remaining_messages={}, remaining_barriers={}",
                    messages.len(),
                    barriers.len()
                )
            })?
            .context("observer closed while draining a reload round")?;

        if let Some((process_index, sequence, expected_process_id)) = messages.remove(&report.tag) {
            if report.process_id != expected_process_id {
                bail!(
                    "round {round} message tag {} came from process {}, expected process {expected_process_id}",
                    report.tag,
                    report.process_id
                );
            }
            if sequence != next_sequence[process_index] {
                bail!(
                    "round {round} process {process_index} violated FIFO: got sequence {sequence}, expected {}",
                    next_sequence[process_index]
                );
            }
            next_sequence[process_index] += 1;
            continue;
        }

        if let Some((process_index, expected_process_id)) = barriers.remove(&report.tag) {
            if report.process_id != expected_process_id {
                bail!(
                    "round {round} barrier tag {} came from process {}, expected process {expected_process_id}",
                    report.tag,
                    report.process_id
                );
            }
            if next_sequence[process_index] != MAILBOX_CAPACITY {
                bail!(
                    "round {round} process {process_index} reached its barrier after {} of {MAILBOX_CAPACITY} messages",
                    next_sequence[process_index]
                );
            }
            continue;
        }

        bail!(
            "round {round} observed duplicate or unexpected tag {} from process {}",
            report.tag,
            report.process_id
        );
    }

    if !messages.is_empty() {
        bail!(
            "round {round} reached every process barrier with {} messages missing",
            messages.len()
        );
    }
    Ok(())
}

fn acknowledgement_ids(acknowledgements: &[ReloadAcknowledgement]) -> Vec<u64> {
    acknowledgements
        .iter()
        .map(|acknowledgement| acknowledgement.process_id)
        .collect()
}

fn assert_committed_status(
    status: &ReloadStatus,
    expected_process_ids: &[u64],
    previous_version: u32,
    current_version: u32,
) -> Result<()> {
    let ReloadStatus::Committed { acknowledgements } = status else {
        bail!("successful update ended in unexpected status: {status:?}");
    };
    assert_eq!(acknowledgement_ids(acknowledgements), expected_process_ids);
    for acknowledgement in acknowledgements {
        assert_eq!(acknowledgement.module_id, MODULE_ID);
        assert_eq!(acknowledgement.previous_version, previous_version);
        assert_eq!(acknowledgement.current_version, current_version);
        assert_eq!(acknowledgement.status, ProcessReloadStatus::Applied);
    }
    Ok(())
}

fn assert_rolled_back_status(
    status: &ReloadStatus,
    expected_process_ids: &[u64],
    trapped_process_id: u64,
    previous_version: u32,
    candidate_version: u32,
) -> Result<()> {
    let ReloadStatus::RolledBack {
        apply_errors,
        acknowledgements,
    } = status
    else {
        bail!("failed update ended in unexpected status: {status:?}");
    };
    assert_eq!(apply_errors.len(), 1);
    assert_eq!(apply_errors[0].process_id, trapped_process_id);
    assert_eq!(apply_errors[0].known_version, Some(previous_version));
    assert_eq!(acknowledgement_ids(acknowledgements), expected_process_ids);

    for acknowledgement in acknowledgements {
        assert_eq!(acknowledgement.module_id, MODULE_ID);
        assert_eq!(acknowledgement.current_version, previous_version);
        if acknowledgement.process_id == trapped_process_id {
            assert_eq!(acknowledgement.previous_version, previous_version);
            assert_eq!(acknowledgement.status, ProcessReloadStatus::AlreadyAtTarget);
        } else {
            assert_eq!(acknowledgement.previous_version, candidate_version);
            assert_eq!(acknowledgement.status, ProcessReloadStatus::Applied);
        }
    }
    Ok(())
}

fn assert_stable_processes(
    environment: &LunaticEnvironment,
    live_processes: &[LiveProcess],
    expected_process_ids: &[u64],
) -> Result<()> {
    let actual_ids = live_processes
        .iter()
        .map(|live| live.process.id())
        .collect::<Vec<_>>();
    assert_eq!(actual_ids, expected_process_ids);

    for live in live_processes {
        let registered = environment
            .get_process(live.process.id())
            .with_context(|| format!("process {} disappeared after reload", live.process.id()))?;
        assert!(
            Arc::ptr_eq(&registered, &live.process),
            "process {} changed its environment handle during reload",
            live.process.id()
        );
    }
    Ok(())
}

fn assert_registry_ownership(
    registry: &ModuleRegistry<DefaultProcessState>,
    committed_version: u32,
    latest_version: u32,
    expected_process_ids: &[u64],
) {
    assert_eq!(
        registry.get_committed_version_number(MODULE_ID),
        Some(committed_version)
    );
    assert_eq!(
        registry.processes_for_module(MODULE_ID, 7),
        expected_process_ids
    );
    assert_eq!(
        registry.version_count(MODULE_ID),
        latest_version as usize + 1
    );
    for version in 0..=latest_version {
        let expected = usize::from(version == committed_version) * PROCESS_COUNT;
        assert_eq!(
            registry.process_count(MODULE_ID, version),
            Some(expected),
            "unexpected process ownership for module version {version}"
        );
    }
}

fn percentile(samples: &[Duration], percent: usize) -> Duration {
    assert!(!samples.is_empty());
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = (percent * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn report_percentiles(label: &str, samples: &[Duration]) -> Duration {
    let p50 = percentile(samples, 50);
    let p95 = percentile(samples, 95);
    let p99 = percentile(samples, 99);
    eprintln!(
        "live_hot_reload_scale {label}: samples={} p50={:.3}ms p95={:.3}ms p99={:.3}ms",
        samples.len(),
        millis(p50),
        millis(p95),
        millis(p99)
    );
    if p99 > PRODUCT_OBJECTIVE_WARNING {
        eprintln!(
            "warning: live_hot_reload_scale {label} p99 {:.3}ms exceeds the 100ms product objective",
            millis(p99)
        );
    }
    p99
}

#[cfg(debug_assertions)]
fn require_release_build() -> Result<()> {
    bail!("the ignored production scale gate must be run with --release")
}

#[cfg(not(debug_assertions))]
fn require_release_build() -> Result<()> {
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release-only production scale gate; run with cargo test --release --test live_hot_reload_scale -- --ignored --exact"]
async fn production_hot_reload_scale_soak_preserves_full_mailboxes_and_rolls_back() -> Result<()> {
    require_release_build()?;

    let runtime = WasmtimeRuntime::new(&default_config())?;
    let module_registry = Arc::new(ModuleRegistry::<DefaultProcessState>::with_max_versions(
        ROUND_COUNT + 1,
    ));
    let initial_module =
        runtime.compile_module(wat::parse_str(guest_module(None, false))?.into())?;
    let initial_version = module_registry.add_version(MODULE_ID, initial_module)?;
    assert_eq!(initial_version, 0);
    let initial_module = module_registry
        .get_version(MODULE_ID, initial_version)
        .context("initial module version was not registered")?;

    let environments = Arc::new(LunaticEnvironments::default());
    let environment = environments
        .create_with_registry(
            7,
            module_registry.clone() as Arc<dyn std::any::Any + Send + Sync>,
        )
        .await?;
    let environment_for_reload: Arc<dyn Environment> = environment.clone();

    let (tag_sender, mut tag_receiver) = mpsc::unbounded_channel();
    let observer = Arc::new(RecordingProcess {
        id: environment.get_next_process_id(),
        reports: tag_sender,
    });
    environment
        .add_process(observer.id(), observer.clone())
        .context("failed to register the scale-test observer")?;

    let mut process_config = DefaultProcessConfig::default();
    process_config.set_max_mailbox_messages(MAILBOX_CAPACITY as u32);
    process_config.set_max_signal_queue(SIGNAL_QUEUE_CAPACITY);
    process_config.set_max_message_size(MESSAGE_ALLOCATION as u64);
    let process_config = Arc::new(process_config);

    let live_processes = timeout(LIFECYCLE_WATCHDOG, async {
        let mut processes = Vec::with_capacity(PROCESS_COUNT);
        for _ in 0..PROCESS_COUNT {
            processes.push(
                spawn_live_process(
                    environment.clone(),
                    &runtime,
                    &initial_module,
                    &process_config,
                    observer.id(),
                    initial_version,
                )
                .await?,
            );
        }
        Ok::<_, anyhow::Error>(processes)
    })
    .await
    .context("initial live-process population exceeded the lifecycle watchdog")??;
    let process_ids = live_processes
        .iter()
        .map(|live| live.process.id())
        .collect::<Vec<_>>();
    assert!(process_ids.windows(2).all(|ids| ids[0] < ids[1]));
    expect_exact_report_set(
        &mut tag_receiver,
        process_ids
            .iter()
            .map(|process_id| (PROCESS_ID_TAG_BASE + *process_id as i64, *process_id)),
        "initial live process identities",
    )
    .await?;
    assert_registry_ownership(
        module_registry.as_ref(),
        initial_version,
        initial_version,
        &process_ids,
    );

    let coordinator = ReloadCoordinator::new();
    let mut committed_version = initial_version;
    let mut commit_latencies = Vec::with_capacity(ROUND_COUNT / 2);
    let mut rollback_latencies = Vec::with_capacity(ROUND_COUNT / 2);
    let mut ceiling_violations = Vec::new();
    let mut mailbox_full_denials = 0usize;

    for round in 0..ROUND_COUNT {
        for live in &live_processes {
            live.process
                .send(tagged_message(PAUSE_TAG, 0))
                .with_context(|| {
                    format!(
                        "round {round} failed to pause process {}",
                        live.process.id()
                    )
                })?;
        }
        expect_exact_report_set(
            &mut tag_receiver,
            process_ids
                .iter()
                .map(|process_id| (PAUSE_ACK_TAG_BASE + *process_id as i64, *process_id)),
            &format!("round {round} pause acknowledgements"),
        )
        .await?;

        for (process_index, live) in live_processes.iter().enumerate() {
            for sequence in 0..MAILBOX_CAPACITY {
                live.process
                    .send(tagged_message(
                        message_tag(round, process_index, sequence),
                        MESSAGE_ALLOCATION,
                    ))
                    .with_context(|| {
                        format!(
                            "round {round} process {process_index} rejected admitted message {sequence}"
                        )
                    })?;
            }
            let overflow = live
                .process
                .send(tagged_message(
                    i64::MAX - process_index as i64,
                    MESSAGE_ALLOCATION,
                ))
                .expect_err("the ninth message must not exceed the configured mailbox bound");
            assert_eq!(overflow.kind(), SignalSendErrorKind::MailboxFull);
            mailbox_full_denials += 1;
        }

        let should_commit = round % 2 == 0;
        let trapped_process_id =
            (!should_commit).then(|| process_ids[(round / 2) % process_ids.len()]);
        let latest_before = module_registry
            .get_latest_version_number(MODULE_ID)
            .context("module history disappeared before a reload round")?;
        let candidate_version = latest_before + 1;
        let candidate = wat::parse_str(guest_module(
            trapped_process_id,
            trapped_process_id.is_some(),
        ))?;

        let started = StdInstant::now();
        let update_result = timeout(
            OPERATION_WATCHDOG,
            register_module_update(
                &runtime,
                module_registry.as_ref(),
                &environment_for_reload,
                &coordinator,
                MODULE_ID,
                candidate,
            ),
        )
        .await
        .with_context(|| {
            format!(
                "round {round} exceeded the {:?} per-operation watchdog",
                OPERATION_WATCHDOG
            )
        })?;
        let elapsed = started.elapsed();

        let status = coordinator
            .status(MODULE_ID)
            .await
            .with_context(|| format!("round {round} retained no coordinator status"))?;
        if should_commit {
            let returned_version =
                update_result.with_context(|| format!("round {round} was expected to commit"))?;
            assert_eq!(returned_version, candidate_version);
            assert_committed_status(&status, &process_ids, committed_version, candidate_version)?;
            assert_eq!(
                module_registry.process_count(MODULE_ID, committed_version),
                Some(0)
            );
            committed_version = candidate_version;
            commit_latencies.push(elapsed);
            if elapsed > COMMIT_CI_WATCHDOG {
                ceiling_violations.push(format!(
                    "commit round {round}: {:.3}ms > {:.3}ms",
                    millis(elapsed),
                    millis(COMMIT_CI_WATCHDOG)
                ));
            }
        } else {
            assert!(
                update_result.is_err(),
                "round {} unexpectedly committed its trapping candidate",
                round
            );
            assert_rolled_back_status(
                &status,
                &process_ids,
                trapped_process_id.expect("rollback round must select a trapped process"),
                committed_version,
                candidate_version,
            )?;
            assert_eq!(
                module_registry.process_count(MODULE_ID, candidate_version),
                Some(0)
            );
            rollback_latencies.push(elapsed);
            if elapsed > ROLLBACK_CI_WATCHDOG {
                ceiling_violations.push(format!(
                    "rollback round {round}: {:.3}ms > {:.3}ms",
                    millis(elapsed),
                    millis(ROLLBACK_CI_WATCHDOG)
                ));
            }
        }

        assert_stable_processes(&environment, &live_processes, &process_ids)?;
        assert_registry_ownership(
            module_registry.as_ref(),
            committed_version,
            candidate_version,
            &process_ids,
        );

        let barrier_deadline = TokioInstant::now() + REPORT_WATCHDOG;
        for (process_index, live) in live_processes.iter().enumerate() {
            send_barrier_with_backpressure(
                &live.process,
                barrier_tag(round, process_index),
                barrier_deadline,
            )
            .await?;
        }
        expect_round_fifo(&mut tag_receiver, round, &process_ids).await?;
    }

    assert_eq!(
        mailbox_full_denials,
        PROCESS_COUNT * ROUND_COUNT,
        "each full mailbox must reject exactly one deliberate overflow attempt"
    );
    assert_eq!(
        PROCESS_COUNT * ROUND_COUNT * MAILBOX_CAPACITY,
        1_280,
        "the gate's exact-once workload changed unexpectedly"
    );

    let final_latest_version = module_registry
        .get_latest_version_number(MODULE_ID)
        .context("module history disappeared before teardown")?;
    for live in &live_processes {
        live.process
            .send(Signal::Kill)
            .with_context(|| format!("failed to stop live process {}", live.process.id()))?;
    }
    for live in live_processes {
        let process_id = live.process.id();
        let result = timeout(LIFECYCLE_WATCHDOG, live.join)
            .await
            .with_context(|| format!("process {process_id} did not stop during teardown"))?
            .with_context(|| format!("process {process_id} task panicked during teardown"))?;
        assert!(
            result.is_err(),
            "kill must fail live process {}",
            process_id
        );
        assert!(environment.get_process(process_id).is_none());
    }
    assert!(module_registry
        .processes_for_module(MODULE_ID, environment.id())
        .is_empty());
    for version in 0..=final_latest_version {
        assert_eq!(module_registry.process_count(MODULE_ID, version), Some(0));
    }
    environment.remove_process(observer.id());
    assert!(environment.get_process(observer.id()).is_none());

    let commit_p99 = report_percentiles("commit", &commit_latencies);
    let rollback_p99 = report_percentiles("rollback", &rollback_latencies);
    assert!(
        commit_p99 <= COMMIT_CI_WATCHDOG,
        "commit p99 {:.3}ms exceeded the {:.3}ms same-runner CI watchdog; this is a regression guard, not portable product proof",
        millis(commit_p99),
        millis(COMMIT_CI_WATCHDOG)
    );
    assert!(
        rollback_p99 <= ROLLBACK_CI_WATCHDOG,
        "rollback p99 {:.3}ms exceeded the {:.3}ms same-runner CI watchdog; this is a regression guard, not portable product proof",
        millis(rollback_p99),
        millis(ROLLBACK_CI_WATCHDOG)
    );
    assert!(
        ceiling_violations.is_empty(),
        "individual operation CI watchdog violations: {:?}",
        ceiling_violations
    );
    eprintln!(
        "LUNATIC_LIVE_RELOAD_EVIDENCE {{\"processes\":{PROCESS_COUNT},\"rounds\":{ROUND_COUNT},\"commit_samples\":{},\"rollback_samples\":{},\"fifo_messages\":{},\"mailbox_full_denials\":{},\"registered_after_shutdown\":0,\"commit_watchdog_ms\":{},\"rollback_watchdog_ms\":{}}}",
        commit_latencies.len(),
        rollback_latencies.len(),
        PROCESS_COUNT * ROUND_COUNT * MAILBOX_CAPACITY,
        mailbox_full_denials,
        COMMIT_CI_WATCHDOG.as_millis(),
        ROLLBACK_CI_WATCHDOG.as_millis()
    );
    Ok(())
}
