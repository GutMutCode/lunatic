use std::{
    collections::{HashMap, HashSet, VecDeque},
    convert::TryInto,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, ensure, Context, Result};
use lunatic_process::{
    config::ProcessConfig,
    env::{Environment, LunaticEnvironment, ProcessLimitReached},
    hot_reload::ReloadCoordinator,
    message::{DataMessage, Message},
    module_registry::ModuleRegistry,
    runtimes::wasmtime::{default_config, WasmtimeCompiledModule, WasmtimeRuntime},
    spawn_native,
    state::SignalSendErrorKind,
    wasm::{spawn_wasm, spawn_wasm_with_options, WasmSpawnOptions},
    Process, Signal,
};
use lunatic_runtime::{
    hot_reload::register_module_update, state::DefaultProcessState, DefaultProcessConfig,
};
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
    time::timeout,
};
use wasmtime::Val;

const OVERALL_WATCHDOG: Duration = Duration::from_secs(90);
const PHASE_WATCHDOG: Duration = Duration::from_secs(30);
const JOIN_WATCHDOG: Duration = Duration::from_secs(10);
const LOCAL_PROGRESS_WATCHDOG: Duration = Duration::from_secs(5);

const SCALE_POPULATIONS: &[usize] = &[1, 8, 32];
const SCALE_MEASURED_BATCHES: usize = 5;
const SCALE_ECHO_ROUNDS: usize = 16;
const SOAK_POPULATION: usize = 8;
const SOAK_CYCLES: usize = 10;
const MAILBOX_CAPACITY: usize = 8;
const SIGNAL_QUEUE_CAPACITY: u32 = 32;
const WASM_PAGE_BYTES: usize = 64 * 1024;
const RESOURCE_PRESSURE_MODULE_ID: u64 = 1_667_100;

const EXTENDED_SOAK_DURATION_ENV: &str = "LUNATIC_EXTENDED_SOAK_DURATION_SECS";
const EXTENDED_SOAK_POPULATION_ENV: &str = "LUNATIC_EXTENDED_SOAK_POPULATION";
const EXTENDED_SOAK_ECHO_ROUNDS_ENV: &str = "LUNATIC_EXTENDED_SOAK_ECHO_ROUNDS";
const EXTENDED_SOAK_MAX_RSS_GROWTH_ENV: &str = "LUNATIC_EXTENDED_SOAK_MAX_RSS_GROWTH_BYTES";
const DEFAULT_EXTENDED_SOAK_DURATION_SECS: u64 = 2 * 60 * 60;
const DEFAULT_EXTENDED_SOAK_POPULATION: usize = 32;
const DEFAULT_EXTENDED_SOAK_ECHO_ROUNDS: usize = 4;
const DEFAULT_EXTENDED_SOAK_MAX_RSS_GROWTH_BYTES: u64 = 512 * 1024 * 1024;
const MAX_EXTENDED_SOAK_POPULATION: usize = 256;
const MAX_EXTENDED_SOAK_ECHO_ROUNDS: usize = 64;

const READY_TAG: i64 = -100;
const GROW_COMMAND_TAG: i64 = -102;
const SATURATION_COMMAND_TAG: i64 = -103;
const GROW_RESULT_TAG: i64 = -202;
const SATURATION_ACK_TAG: i64 = -203;

// This guest keeps the exercised path intentionally small: every observation still crosses a
// live Wasm Store, Lunatic's bounded signal ingress, the process mailbox and a native observer.
const SCALE_GUEST_TEMPLATE: &str = r#"
(module
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::message" "data_size" (func $data_size (result i64)))
    (import "lunatic::message" "get_tag" (func $get_tag (result i64)))
    (import "lunatic::message" "read_data" (func $read_data (param i32 i32) (result i32)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))
    (import "lunatic::message" "send" (func $send (param i64) (result i32)))
    (import "lunatic::message" "write_data" (func $write_data (param i32 i32) (result i32)))
    (import "lunatic::process" "sleep_ms" (func $sleep_ms (param i64)))

    (memory (export "memory") 1)

    (func $assert_i32 (param $actual i32) (param $expected i32)
        (if (i32.ne (local.get $actual) (local.get $expected)) (then unreachable)))

    (func $assert_i64 (param $actual i64) (param $expected i64)
        (if (i64.ne (local.get $actual) (local.get $expected)) (then unreachable)))

    (func $notify (param $observer i64) (param $tag i64) (param $index i64) (param $value i64)
        (i64.store (i32.const 0) (local.get $index))
        (i64.store (i32.const 8) (local.get $value))
        (call $create_data (local.get $tag) (i64.const 16))
        (call $assert_i32 (call $write_data (i32.const 0) (i32.const 16)) (i32.const 16))
        (call $assert_i32 (call $send (local.get $observer)) (i32.const 0)))

    (func (export "run") (param $observer i64) (param $index i64) (param $staged i64)
        (local $tag i64)
        (local $value i64)

        ;; Touch the initial page so the scale evidence does not count only an untouched mapping.
        (i32.store8 (i32.const 4096) (i32.wrap_i64 (local.get $index)))
        (call $notify
            (local.get $observer)
            (i64.const -100)
            (local.get $index)
            (i64.extend_i32_u (memory.size)))

        __STAGING_GATE__

        (loop $serve
            (call $assert_i32
                (call $receive (i32.const 0) (i32.const 0) (i64.const -1))
                (i32.const 0))
            (local.set $tag (call $get_tag))

            (if (i64.eq (local.get $tag) (i64.const -101))
                (then (return)))

            (if (i64.eq (local.get $tag) (i64.const -102))
                (then
                    (local.set $value
                        (i64.extend_i32_s (memory.grow (i32.const 1))))
                    ;; Commit part of an admitted second page instead of reporting a lazy mapping.
                    (if (i64.ge_s (local.get $value) (i64.const 0))
                        (then (i32.store8 (i32.const 65536) (i32.const 1))))
                    (call $notify
                        (local.get $observer)
                        (i64.const -202)
                        (local.get $index)
                        (local.get $value))
                    (br $serve)))

            (call $assert_i64 (call $data_size) (i64.const 16))
            (call $assert_i32
                (call $read_data (i32.const 0) (i32.const 16))
                (i32.const 16))
            (call $assert_i64 (i64.load (i32.const 0)) (local.get $index))
            (local.set $value (i64.load (i32.const 8)))

            (if (i64.eq (local.get $tag) (i64.const -103))
                (then
                    (call $notify
                        (local.get $observer)
                        (i64.const -203)
                        (local.get $index)
                        (local.get $value)))
                (else
                    (call $notify
                        (local.get $observer)
                        (local.get $tag)
                        (local.get $index)
                        (local.get $value))))
            (br $serve)))
)
"#;

fn scale_guest_source(with_staging_gate: bool) -> String {
    let staging_gate = if with_staging_gate {
        r#"
        ;; A staged consumer remains outside receive forever. The host releases it by applying
        ;; the gate-free module through the production hot-reload path, so mailbox saturation
        ;; does not depend on scheduler timing or a fixed sleep deadline.
        (if (i64.gt_u (local.get $staged) (i64.const 0))
            (then
                (loop $staging_gate
                    (call $sleep_ms (i64.const 60000))
                    (br $staging_gate))))
        "#
    } else {
        ""
    };
    SCALE_GUEST_TEMPLATE.replace("__STAGING_GATE__", staging_gate)
}

#[derive(Debug)]
struct Observation {
    tag: i64,
    index: u64,
    value: i64,
    observed_at: Instant,
}

struct ObservationInbox {
    receiver: mpsc::Receiver<Observation>,
    deferred: VecDeque<Observation>,
}

impl ObservationInbox {
    async fn matching<F>(
        &mut self,
        watchdog: Duration,
        description: &str,
        predicate: F,
    ) -> Result<Observation>
    where
        F: Fn(&Observation) -> bool,
    {
        let deadline = Instant::now() + watchdog;
        loop {
            if let Some(position) = self.deferred.iter().position(&predicate) {
                return Ok(self
                    .deferred
                    .remove(position)
                    .expect("deferred observation index must remain valid"));
            }

            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| anyhow!("timed out waiting for {description}"))?;
            let observation = timeout(remaining, self.receiver.recv())
                .await
                .with_context(|| format!("timed out waiting for {description}"))?
                .ok_or_else(|| anyhow!("observer stopped while waiting for {description}"))?;
            if predicate(&observation) {
                return Ok(observation);
            }
            self.deferred.push_back(observation);
        }
    }
}

struct ObserverHandle {
    process: Arc<dyn Process>,
    join: JoinHandle<Result<()>>,
    inbox: ObservationInbox,
}

impl ObserverHandle {
    fn spawn(environment: Arc<LunaticEnvironment>) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(4_096);
        let (join, process) = spawn_native(environment, move |_, mailbox| async move {
            loop {
                let Message::Data(message) = mailbox.pop(None).await else {
                    bail!("scale observer received a non-data message");
                };
                ensure!(
                    message.buffer.len() == 16,
                    "scale observer received {} bytes instead of 16",
                    message.buffer.len()
                );
                let index = u64::from_le_bytes(
                    message.buffer[0..8]
                        .try_into()
                        .expect("checked observation index slice"),
                );
                let value = i64::from_le_bytes(
                    message.buffer[8..16]
                        .try_into()
                        .expect("checked observation value slice"),
                );
                sender
                    .send(Observation {
                        tag: message.tag.unwrap_or(0),
                        index,
                        value,
                        observed_at: Instant::now(),
                    })
                    .await
                    .map_err(|_| anyhow!("scale observation receiver disappeared"))?;
            }
        })?;

        Ok(Self {
            process: Arc::new(process),
            join,
            inbox: ObservationInbox {
                receiver,
                deferred: VecDeque::new(),
            },
        })
    }

    async fn stop(self) -> Result<()> {
        self.process
            .send(Signal::Kill)
            .map_err(|error| anyhow!("failed to kill native observer: {error}"))?;
        let result = timeout(JOIN_WATCHDOG, self.join)
            .await
            .context("native observer did not join")?
            .context("native observer task panicked")?;
        match result {
            Ok(()) => bail!("killed native observer exited normally"),
            Err(error) => ensure!(
                error.to_string().contains("Process killed"),
                "native observer failed before kill: {error}"
            ),
        }
        Ok(())
    }
}

struct GuestHandle {
    index: u64,
    process: Arc<dyn Process>,
    join: JoinHandle<Result<DefaultProcessState>>,
    spawn_started_at: Instant,
    spawn_elapsed: Duration,
}

struct Harness {
    runtime: WasmtimeRuntime,
    module: Arc<WasmtimeCompiledModule<DefaultProcessState>>,
    config: Arc<DefaultProcessConfig>,
    registry: Arc<RwLock<HashMap<String, (u64, u64)>>>,
}

impl Harness {
    fn new() -> Result<Self> {
        let runtime = WasmtimeRuntime::new(&default_config())?;
        let module =
            Arc::new(runtime.compile_module(wat::parse_str(scale_guest_source(true))?.into())?);
        let mut config = DefaultProcessConfig::default();
        config.set_max_memory(2 * WASM_PAGE_BYTES);
        config.set_max_mailbox_messages(MAILBOX_CAPACITY as u32);
        config.set_max_signal_queue(SIGNAL_QUEUE_CAPACITY);
        Ok(Self {
            runtime,
            module,
            config: Arc::new(config),
            registry: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn state_for_module(
        &self,
        environment: Arc<LunaticEnvironment>,
        module: Arc<WasmtimeCompiledModule<DefaultProcessState>>,
    ) -> Result<DefaultProcessState> {
        DefaultProcessState::new(
            environment,
            None,
            self.runtime.clone(),
            module,
            self.config.clone(),
            self.registry.clone(),
        )
    }

    fn state(&self, environment: Arc<LunaticEnvironment>) -> Result<DefaultProcessState> {
        self.state_for_module(environment, self.module.clone())
    }

    async fn spawn_guest(
        &self,
        environment: Arc<LunaticEnvironment>,
        observer_id: u64,
        index: u64,
        staged: bool,
    ) -> Result<GuestHandle> {
        self.spawn_guest_from_module(
            environment,
            observer_id,
            index,
            staged,
            self.module.clone(),
            None,
        )
        .await
    }

    async fn spawn_guest_from_module(
        &self,
        environment: Arc<LunaticEnvironment>,
        observer_id: u64,
        index: u64,
        staged: bool,
        module: Arc<WasmtimeCompiledModule<DefaultProcessState>>,
        initial_module_version: Option<(u64, u32)>,
    ) -> Result<GuestHandle> {
        let spawn_started_at = Instant::now();
        let state = self.state_for_module(environment.clone(), module.clone())?;
        let (join, process) = spawn_wasm_with_options(
            environment,
            self.runtime.clone(),
            &module,
            state,
            "run",
            vec![
                Val::I64(observer_id as i64),
                Val::I64(index as i64),
                Val::I64(i64::from(staged)),
            ],
            WasmSpawnOptions {
                link: None,
                initial_module_version,
            },
        )
        .await?;
        Ok(GuestHandle {
            index,
            process,
            join,
            spawn_started_at,
            spawn_elapsed: spawn_started_at.elapsed(),
        })
    }

    async fn assert_next_spawn_rejected(
        &self,
        environment: Arc<LunaticEnvironment>,
        observer_id: u64,
        index: u64,
    ) -> Result<()> {
        let state = self.state(environment.clone())?;
        match spawn_wasm(
            environment.clone(),
            self.runtime.clone(),
            &self.module,
            state,
            "run",
            vec![
                Val::I64(observer_id as i64),
                Val::I64(index as i64),
                Val::I64(0),
            ],
            None,
        )
        .await
        {
            Err(error) => {
                let denial = error.downcast_ref::<ProcessLimitReached>().ok_or_else(|| {
                    anyhow!("full environment returned an untyped admission error: {error}")
                })?;
                ensure!(
                    denial.environment_id() == environment.id(),
                    "admission denial named environment {} instead of {}",
                    denial.environment_id(),
                    environment.id()
                );
                ensure!(
                    Some(denial.limit()) == environment.max_processes(),
                    "admission denial reported limit {} instead of {:?}",
                    denial.limit(),
                    environment.max_processes()
                );
            }
            Ok((join, process)) => {
                let _ = process.send(Signal::Kill);
                let _ = join.await;
                bail!("environment admitted a process beyond its configured limit");
            }
        }
        Ok(())
    }
}

fn data_signal(tag: i64, index: u64, value: i64) -> Signal {
    let mut buffer = Vec::with_capacity(16);
    buffer.extend_from_slice(&index.to_le_bytes());
    buffer.extend_from_slice(&value.to_le_bytes());
    Signal::Message(Message::Data(DataMessage::new_from_vec(Some(tag), buffer)))
}

fn command_signal(tag: i64) -> Signal {
    Signal::Message(Message::Data(DataMessage::new(Some(tag), 0)))
}

fn send_data(process: &Arc<dyn Process>, tag: i64, index: u64, value: i64) -> Result<()> {
    process
        .send(data_signal(tag, index, value))
        .map_err(|error| {
            anyhow!(
                "failed to send tag {tag} to process {}: {error}",
                process.id()
            )
        })
}

async fn spawn_population(
    harness: &Harness,
    environment: Arc<LunaticEnvironment>,
    observer_id: u64,
    population: usize,
) -> Result<Vec<GuestHandle>> {
    spawn_population_range(harness, environment, observer_id, 0, population).await
}

async fn spawn_population_range(
    harness: &Harness,
    environment: Arc<LunaticEnvironment>,
    observer_id: u64,
    first_index: usize,
    count: usize,
) -> Result<Vec<GuestHandle>> {
    timeout(PHASE_WATCHDOG, async {
        let mut guests = Vec::with_capacity(count);
        for index in first_index..first_index + count {
            guests.push(
                harness
                    .spawn_guest(environment.clone(), observer_id, index as u64, false)
                    .await?,
            );
        }
        Ok(guests)
    })
    .await
    .with_context(|| format!("timed out spawning {count} Wasm guests from index {first_index}"))?
}

async fn await_ready(
    inbox: &mut ObservationInbox,
    guests: &[GuestHandle],
) -> Result<Vec<Duration>> {
    let starts: HashMap<u64, Instant> = guests
        .iter()
        .map(|guest| (guest.index, guest.spawn_started_at))
        .collect();
    let mut seen = HashSet::with_capacity(guests.len());
    let mut latencies = Vec::with_capacity(guests.len());
    for _ in guests {
        let observation = inbox
            .matching(PHASE_WATCHDOG, "guest readiness", |event| {
                event.tag == READY_TAG
            })
            .await?;
        ensure!(
            starts.contains_key(&observation.index),
            "unknown guest {} reported ready",
            observation.index
        );
        ensure!(
            seen.insert(observation.index),
            "guest {} reported ready twice",
            observation.index
        );
        ensure!(
            observation.value == 1,
            "guest {} reported {} initial Wasm pages instead of 1",
            observation.index,
            observation.value
        );
        let latency = observation
            .observed_at
            .checked_duration_since(starts[&observation.index])
            .ok_or_else(|| anyhow!("guest readiness timestamp preceded its spawn"))?;
        ensure!(
            latency <= PHASE_WATCHDOG,
            "guest {} readiness exceeded the watchdog: {latency:?}",
            observation.index
        );
        latencies.push(latency);
    }
    Ok(latencies)
}

async fn ping_population(
    inbox: &mut ObservationInbox,
    guests: &[GuestHandle],
    sequence_base: i64,
) -> Result<Vec<Duration>> {
    let mut pending = HashMap::with_capacity(guests.len());
    for guest in guests {
        let sequence = sequence_base + guest.index as i64 + 1;
        let sent_at = Instant::now();
        send_data(&guest.process, sequence, guest.index, sequence)?;
        pending.insert(sequence, (guest.index, sent_at));
    }

    let mut latencies = Vec::with_capacity(guests.len());
    while !pending.is_empty() {
        let observation = inbox
            .matching(PHASE_WATCHDOG, "sequence-tag echo", |event| {
                pending.contains_key(&event.tag)
            })
            .await?;
        let (expected_index, sent_at) = pending
            .remove(&observation.tag)
            .expect("matching observation must have a pending sequence");
        ensure!(
            observation.index == expected_index,
            "sequence {} came from guest {} instead of {}",
            observation.tag,
            observation.index,
            expected_index
        );
        ensure!(
            observation.value == observation.tag,
            "sequence {} returned payload {}",
            observation.tag,
            observation.value
        );
        let latency = observation
            .observed_at
            .checked_duration_since(sent_at)
            .ok_or_else(|| anyhow!("echo timestamp preceded its send"))?;
        ensure!(
            latency <= LOCAL_PROGRESS_WATCHDOG,
            "local guest echo exceeded the deadlock watchdog: {latency:?}"
        );
        latencies.push(latency);
    }
    Ok(latencies)
}

async fn kill_and_join(guests: Vec<GuestHandle>) -> Result<()> {
    for guest in &guests {
        guest
            .process
            .send(Signal::Kill)
            .map_err(|error| anyhow!("failed to kill guest {}: {error}", guest.index))?;
    }
    for guest in guests {
        let index = guest.index;
        let result = timeout(JOIN_WATCHDOG, guest.join)
            .await
            .with_context(|| format!("guest {index} did not join after kill"))?
            .with_context(|| format!("guest {index} task panicked"))?;
        match result {
            Ok(_) => bail!("killed guest {index} exited normally"),
            Err(error) => ensure!(
                error.to_string().contains("Process killed"),
                "guest {} failed before kill: {error}",
                index
            ),
        }
    }
    Ok(())
}

async fn wait_for_process_count(
    environment: &LunaticEnvironment,
    expected: usize,
    description: &str,
) -> Result<()> {
    timeout(PHASE_WATCHDOG, async {
        while environment.process_count() != expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| {
        format!(
            "{description}: expected {expected} registered processes, found {}",
            environment.process_count()
        )
    })?;
    Ok(())
}

#[derive(Clone, Copy)]
struct Percentiles {
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
}

fn percentiles(samples: &[Duration]) -> Percentiles {
    assert!(!samples.is_empty());
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let pick = |percent: usize| {
        let rank = (sorted.len() * percent).div_ceil(100).max(1);
        sorted[rank - 1].as_micros()
    };
    Percentiles {
        p50_us: pick(50),
        p95_us: pick(95),
        p99_us: pick(99),
    }
}

#[cfg(target_os = "linux")]
fn rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kibibytes = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    kibibytes.checked_mul(1024)
}

#[cfg(not(target_os = "linux"))]
fn rss_bytes() -> Option<u64> {
    None
}

fn rss_delta(before: Option<u64>, after: Option<u64>) -> Option<i128> {
    before
        .zip(after)
        .map(|(before, after)| i128::from(after) - i128::from(before))
}

fn json_optional<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

fn environment_u64(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .with_context(|| format!("{name} must be an unsigned integer, found {value:?}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => bail!("{name} is not valid Unicode"),
    }
}

#[derive(Clone, Copy)]
struct ExtendedSoakConfig {
    requested_duration_seconds: u64,
    duration: Duration,
    population: usize,
    echo_rounds: usize,
    max_rss_growth_bytes: u64,
}

impl ExtendedSoakConfig {
    fn from_environment() -> Result<Self> {
        let requested_duration_seconds = environment_u64(
            EXTENDED_SOAK_DURATION_ENV,
            DEFAULT_EXTENDED_SOAK_DURATION_SECS,
        )?;
        ensure!(
            requested_duration_seconds > 0,
            "{EXTENDED_SOAK_DURATION_ENV} must be at least one second"
        );

        let population: usize = environment_u64(
            EXTENDED_SOAK_POPULATION_ENV,
            DEFAULT_EXTENDED_SOAK_POPULATION as u64,
        )?
        .try_into()
        .with_context(|| format!("{EXTENDED_SOAK_POPULATION_ENV} does not fit usize"))?;
        ensure!(
            (1..=MAX_EXTENDED_SOAK_POPULATION).contains(&population),
            "{EXTENDED_SOAK_POPULATION_ENV} must be between 1 and {MAX_EXTENDED_SOAK_POPULATION}"
        );

        let echo_rounds: usize = environment_u64(
            EXTENDED_SOAK_ECHO_ROUNDS_ENV,
            DEFAULT_EXTENDED_SOAK_ECHO_ROUNDS as u64,
        )?
        .try_into()
        .with_context(|| format!("{EXTENDED_SOAK_ECHO_ROUNDS_ENV} does not fit usize"))?;
        ensure!(
            (1..=MAX_EXTENDED_SOAK_ECHO_ROUNDS).contains(&echo_rounds),
            "{EXTENDED_SOAK_ECHO_ROUNDS_ENV} must be between 1 and {MAX_EXTENDED_SOAK_ECHO_ROUNDS}"
        );

        let max_rss_growth_bytes = environment_u64(
            EXTENDED_SOAK_MAX_RSS_GROWTH_ENV,
            DEFAULT_EXTENDED_SOAK_MAX_RSS_GROWTH_BYTES,
        )?;

        Ok(Self {
            requested_duration_seconds,
            duration: Duration::from_secs(requested_duration_seconds),
            population,
            echo_rounds,
            max_rss_growth_bytes,
        })
    }
}

fn emit_scale_evidence(
    population: usize,
    spawn: &[Duration],
    spawn_elapsed: Duration,
    ready: &[Duration],
    echo: &[Duration],
    echo_elapsed: Duration,
    rss: (Option<u64>, Option<u64>),
) {
    let (rss_before, rss_after) = rss;
    let echo_samples = echo.len();
    let spawn_samples = spawn.len();
    let ready_samples = ready.len();
    let spawn = percentiles(spawn);
    let ready = percentiles(ready);
    let echo = percentiles(echo);
    let echo_elapsed_us = echo_elapsed.as_micros().max(1);
    let spawn_elapsed_us = spawn_elapsed.as_micros().max(1);
    let spawn_rate_per_sec = spawn_samples as f64 / spawn_elapsed.as_secs_f64().max(f64::EPSILON);
    let echo_rate_per_sec = echo_samples as f64 / echo_elapsed.as_secs_f64().max(f64::EPSILON);
    let observed_process_rss_delta = rss_delta(rss_before, rss_after);
    let observed_process_rss_delta_per_guest =
        observed_process_rss_delta.map(|delta| delta / population as i128);
    println!(
        "LUNATIC_SCALE_EVIDENCE {{\"kind\":\"scale\",\"population\":{population},\"warmup_batches\":1,\"measured_batches\":{SCALE_MEASURED_BATCHES},\"spawn_samples\":{spawn_samples},\"spawn_batch_elapsed_us\":{spawn_elapsed_us},\"spawn_rate_per_sec\":{spawn_rate_per_sec:.3},\"spawn_p50_us\":{},\"spawn_p95_us\":{},\"spawn_p99_us\":{},\"ready_samples\":{},\"ready_p50_us\":{},\"ready_p95_us\":{},\"ready_p99_us\":{},\"mailbox_echo_rounds_per_batch\":{SCALE_ECHO_ROUNDS},\"mailbox_echo_samples\":{},\"mailbox_echo_elapsed_us\":{echo_elapsed_us},\"mailbox_echo_rate_per_sec\":{echo_rate_per_sec:.3},\"mailbox_echo_p50_us\":{},\"mailbox_echo_p95_us\":{},\"mailbox_echo_p99_us\":{},\"wasm_committed_pages_per_guest\":1,\"wasm_committed_bytes_per_guest\":{WASM_PAGE_BYTES},\"observed_process_rss_before_bytes\":{},\"observed_process_rss_after_bytes\":{},\"observed_process_rss_delta_bytes\":{},\"observed_process_rss_delta_per_guest_bytes\":{},\"observed_process_rss_delta_per_guest_method\":\"process_wide_delta_divided_by_live_guest_count\",\"rss_curve_method\":\"single_baseline_incremental_live_populations_1_8_32\",\"rss_attribution\":\"observed_process_wide_not_allocator_attributable\"}}",
        spawn.p50_us,
        spawn.p95_us,
        spawn.p99_us,
        ready_samples,
        ready.p50_us,
        ready.p95_us,
        ready.p99_us,
        echo_samples,
        echo.p50_us,
        echo.p95_us,
        echo.p99_us,
        json_optional(rss_before),
        json_optional(rss_after),
        json_optional(observed_process_rss_delta),
        json_optional(observed_process_rss_delta_per_guest),
    );
}

async fn measure_live_population_rss_curve(
    harness: &Harness,
    environment_id: u64,
) -> Result<HashMap<usize, (Option<u64>, Option<u64>)>> {
    let max_population = SCALE_POPULATIONS
        .iter()
        .copied()
        .max()
        .context("scale population curve is empty")?;
    let environment = Arc::new(LunaticEnvironment::with_max_processes(
        environment_id,
        max_population + 1,
    ));
    let mut observer = ObserverHandle::spawn(environment.clone())?;
    let baseline_rss = rss_bytes();
    let mut guests = Vec::with_capacity(max_population);
    let mut curve = HashMap::with_capacity(SCALE_POPULATIONS.len());

    for population in SCALE_POPULATIONS.iter().copied() {
        let first_index = guests.len();
        let mut added = spawn_population_range(
            harness,
            environment.clone(),
            observer.process.id(),
            first_index,
            population - first_index,
        )
        .await?;
        await_ready(&mut observer.inbox, &added).await?;
        guests.append(&mut added);
        ensure!(
            environment.process_count() == population + 1,
            "RSS curve registered {} processes instead of {}",
            environment.process_count(),
            population + 1
        );
        ping_population(&mut observer.inbox, &guests, 2_000_000 + population as i64).await?;
        curve.insert(population, (baseline_rss, rss_bytes()));
    }

    kill_and_join(guests).await?;
    wait_for_process_count(&environment, 1, "RSS curve guest cleanup").await?;
    observer.stop().await?;
    wait_for_process_count(&environment, 0, "RSS curve observer cleanup").await?;
    Ok(curve)
}

async fn run_scale_point(
    harness: &Harness,
    environment_id: u64,
    population: usize,
    rss: (Option<u64>, Option<u64>),
) -> Result<()> {
    let environment = Arc::new(LunaticEnvironment::with_max_processes(
        environment_id,
        population + 1,
    ));
    let mut observer = ObserverHandle::spawn(environment.clone())?;
    ensure!(
        environment.process_count() == 1,
        "observer was not registered"
    );
    let warmup_guests = spawn_population(
        harness,
        environment.clone(),
        observer.process.id(),
        population,
    )
    .await?;
    await_ready(&mut observer.inbox, &warmup_guests).await?;
    ping_population(&mut observer.inbox, &warmup_guests, 1_000).await?;
    kill_and_join(warmup_guests).await?;
    wait_for_process_count(&environment, 1, "scale point warm-up cleanup").await?;

    let mut spawn = Vec::with_capacity(population * SCALE_MEASURED_BATCHES);
    let mut ready = Vec::with_capacity(population * SCALE_MEASURED_BATCHES);
    let mut echo = Vec::with_capacity(population * SCALE_ECHO_ROUNDS * SCALE_MEASURED_BATCHES);
    let mut spawn_elapsed = Duration::ZERO;
    let mut echo_elapsed = Duration::ZERO;

    for batch in 0..SCALE_MEASURED_BATCHES {
        let spawn_started_at = Instant::now();
        let guests = spawn_population(
            harness,
            environment.clone(),
            observer.process.id(),
            population,
        )
        .await?;
        spawn_elapsed += spawn_started_at.elapsed();
        ready.extend(await_ready(&mut observer.inbox, &guests).await?);
        ensure!(
            environment.process_count() == population + 1,
            "scale batch {batch} registered {} processes instead of {}",
            environment.process_count(),
            population + 1
        );
        harness
            .assert_next_spawn_rejected(
                environment.clone(),
                observer.process.id(),
                population as u64,
            )
            .await?;
        ensure!(
            environment.process_count() == population + 1,
            "scale batch {batch} rejected spawn changed the registered process count"
        );

        let echo_started_at = Instant::now();
        for round in 0..SCALE_ECHO_ROUNDS {
            echo.extend(
                ping_population(
                    &mut observer.inbox,
                    &guests,
                    10_000 + ((batch * SCALE_ECHO_ROUNDS + round) * population) as i64,
                )
                .await?,
            );
        }
        echo_elapsed += echo_started_at.elapsed();
        spawn.extend(guests.iter().map(|guest| guest.spawn_elapsed));

        kill_and_join(guests).await?;
        wait_for_process_count(
            &environment,
            1,
            &format!("scale batch {batch} guest cleanup"),
        )
        .await?;
    }

    let expected_echoes = population * SCALE_ECHO_ROUNDS * SCALE_MEASURED_BATCHES;
    ensure!(
        echo.len() == expected_echoes,
        "scale point collected {} mailbox echoes instead of {expected_echoes}",
        echo.len()
    );
    emit_scale_evidence(
        population,
        &spawn,
        spawn_elapsed,
        &ready,
        &echo,
        echo_elapsed,
        rss,
    );
    observer.stop().await?;
    wait_for_process_count(&environment, 0, "scale point observer cleanup").await
}

async fn run_resource_pressure(harness: &Harness, environment_id: u64) -> Result<()> {
    const SIBLING_SEQUENCE: i64 = 90_001;
    const RECOVERY_SEQUENCE: i64 = 90_002;

    let module_registry = Arc::new(ModuleRegistry::<DefaultProcessState>::with_max_versions(2));
    let staged_module = harness
        .runtime
        .compile_module(wat::parse_str(scale_guest_source(true))?.into())?;
    let staged_version = module_registry.add_version(RESOURCE_PRESSURE_MODULE_ID, staged_module)?;
    ensure!(staged_version == 0, "staged module was not version zero");
    let staged_module = module_registry
        .get_version(RESOURCE_PRESSURE_MODULE_ID, staged_version)
        .context("staged resource-pressure module was not registered")?;
    let environment = Arc::new(LunaticEnvironment::with_module_registry_and_max_processes(
        environment_id,
        module_registry.clone() as Arc<dyn std::any::Any + Send + Sync>,
        3,
    ));
    let mut observer = ObserverHandle::spawn(environment.clone())?;
    let stalled = harness
        .spawn_guest_from_module(
            environment.clone(),
            observer.process.id(),
            0,
            true,
            staged_module,
            Some((RESOURCE_PRESSURE_MODULE_ID, staged_version)),
        )
        .await?;
    // The staged module cannot enter receive until the explicit gate-free hot reload below.
    for sequence in 0..MAILBOX_CAPACITY {
        stalled
            .process
            .send(data_signal(
                SATURATION_COMMAND_TAG,
                stalled.index,
                sequence as i64,
            ))
            .map_err(|error| anyhow!("mailbox filled before capacity: {error}"))?;
    }
    let rejection = stalled
        .process
        .send(data_signal(
            SATURATION_COMMAND_TAG,
            stalled.index,
            MAILBOX_CAPACITY as i64,
        ))
        .expect_err("ninth message must exceed mailbox capacity eight");
    ensure!(
        rejection.kind() == SignalSendErrorKind::MailboxFull,
        "mailbox saturation returned {:?} instead of MailboxFull",
        rejection.kind()
    );
    let rejected_signal = rejection.into_signal();

    let sibling = harness
        .spawn_guest(environment.clone(), observer.process.id(), 1, false)
        .await?;
    let guests = vec![stalled, sibling];
    await_ready(&mut observer.inbox, &guests).await?;
    harness
        .assert_next_spawn_rejected(environment.clone(), observer.process.id(), 2)
        .await?;

    let stalled = &guests[0];
    let sibling = &guests[1];
    let sibling_sent_at = Instant::now();
    send_data(
        &sibling.process,
        SIBLING_SEQUENCE,
        sibling.index,
        SIBLING_SEQUENCE,
    )?;
    let sibling_reply = observer
        .inbox
        .matching(
            LOCAL_PROGRESS_WATCHDOG,
            "unrelated sibling progress during saturation",
            |event| event.tag == SIBLING_SEQUENCE,
        )
        .await?;
    ensure!(
        sibling_reply.index == sibling.index && sibling_reply.value == SIBLING_SEQUENCE,
        "sibling returned a mismatched saturation probe"
    );
    let sibling_latency = sibling_reply
        .observed_at
        .checked_duration_since(sibling_sent_at)
        .ok_or_else(|| anyhow!("sibling reply preceded its send"))?;
    ensure!(
        sibling_latency <= LOCAL_PROGRESS_WATCHDOG,
        "saturated peer blocked unrelated sibling for {sibling_latency:?}"
    );

    let environment_for_reload: Arc<dyn Environment> = environment.clone();
    let coordinator = ReloadCoordinator::new();
    let released_version = timeout(
        PHASE_WATCHDOG,
        register_module_update(
            &harness.runtime,
            module_registry.as_ref(),
            &environment_for_reload,
            &coordinator,
            RESOURCE_PRESSURE_MODULE_ID,
            wat::parse_str(scale_guest_source(false))?,
        ),
    )
    .await
    .context("resource-pressure staging gate hot reload timed out")??;
    ensure!(
        released_version == staged_version + 1,
        "resource-pressure staging gate committed version {released_version} instead of {}",
        staged_version + 1
    );

    let first_ack = observer
        .inbox
        .matching(PHASE_WATCHDOG, "stalled mailbox drain", |event| {
            event.tag == SATURATION_ACK_TAG
        })
        .await?;
    ensure!(
        first_ack.index == stalled.index,
        "wrong process drained mailbox"
    );
    let mut acknowledgements = HashSet::from([first_ack.value]);
    stalled
        .process
        .send(rejected_signal)
        .map_err(|error| anyhow!("released mailbox slot was not reusable: {error}"))?;
    while acknowledgements.len() < MAILBOX_CAPACITY + 1 {
        let observation = observer
            .inbox
            .matching(PHASE_WATCHDOG, "saturation acknowledgement", |event| {
                event.tag == SATURATION_ACK_TAG
            })
            .await?;
        ensure!(
            observation.index == stalled.index,
            "saturation acknowledgement came from guest {}",
            observation.index
        );
        ensure!(
            acknowledgements.insert(observation.value),
            "duplicate saturation acknowledgement {}",
            observation.value
        );
    }
    ensure!(
        acknowledgements == (0..=MAILBOX_CAPACITY as i64).collect(),
        "mailbox reuse did not preserve all admitted messages: {acknowledgements:?}"
    );

    let rss_before_growth = rss_bytes();
    sibling
        .process
        .send(command_signal(GROW_COMMAND_TAG))
        .map_err(|error| anyhow!("failed to request admitted memory growth: {error}"))?;
    let first_growth = observer
        .inbox
        .matching(PHASE_WATCHDOG, "admitted memory.grow result", |event| {
            event.tag == GROW_RESULT_TAG && event.index == sibling.index
        })
        .await?;
    ensure!(
        first_growth.value == 1,
        "first memory.grow returned {} instead of previous size 1",
        first_growth.value
    );
    sibling
        .process
        .send(command_signal(GROW_COMMAND_TAG))
        .map_err(|error| anyhow!("failed to request denied memory growth: {error}"))?;
    let denied_growth = observer
        .inbox
        .matching(PHASE_WATCHDOG, "denied memory.grow result", |event| {
            event.tag == GROW_RESULT_TAG && event.index == sibling.index
        })
        .await?;
    ensure!(
        denied_growth.value == -1,
        "memory.grow beyond 2-page ceiling returned {} instead of -1",
        denied_growth.value
    );

    let recovery_sent_at = Instant::now();
    send_data(
        &sibling.process,
        RECOVERY_SEQUENCE,
        sibling.index,
        RECOVERY_SEQUENCE,
    )?;
    let recovery = observer
        .inbox
        .matching(PHASE_WATCHDOG, "post-denial guest progress", |event| {
            event.tag == RECOVERY_SEQUENCE
        })
        .await?;
    ensure!(
        recovery.index == sibling.index && recovery.value == RECOVERY_SEQUENCE,
        "guest did not preserve state after denied memory growth"
    );
    let recovery_latency = recovery
        .observed_at
        .checked_duration_since(recovery_sent_at)
        .ok_or_else(|| anyhow!("recovery reply preceded its send"))?;
    ensure!(
        recovery_latency <= LOCAL_PROGRESS_WATCHDOG,
        "post-denial guest progress exceeded watchdog: {recovery_latency:?}"
    );
    let rss_after_growth = rss_bytes();

    println!(
        "LUNATIC_SCALE_EVIDENCE {{\"kind\":\"resource_pressure\",\"population\":2,\"mailbox_capacity\":{MAILBOX_CAPACITY},\"accepted_before_full\":{MAILBOX_CAPACITY},\"rejection\":\"mailbox_full\",\"capacity_reused\":true,\"sibling_progress_us\":{},\"memory_grow_results\":[1,-1],\"post_denial_progress_us\":{},\"rss_growth_delta_bytes\":{}}}",
        sibling_latency.as_micros(),
        recovery_latency.as_micros(),
        json_optional(rss_delta(rss_before_growth, rss_after_growth)),
    );

    kill_and_join(guests).await?;
    wait_for_process_count(&environment, 1, "resource-pressure guest cleanup").await?;
    observer.stop().await?;
    wait_for_process_count(&environment, 0, "resource-pressure observer cleanup").await
}

async fn run_soak(harness: &Harness, environment_id: u64) -> Result<()> {
    let environment = Arc::new(LunaticEnvironment::with_max_processes(
        environment_id,
        SOAK_POPULATION + 1,
    ));
    let mut observer = ObserverHandle::spawn(environment.clone())?;
    let baseline_rss = rss_bytes();
    let mut peak_rss = baseline_rss;
    let mut all_spawn = Vec::with_capacity(SOAK_POPULATION * SOAK_CYCLES);
    let mut all_ready = Vec::with_capacity(SOAK_POPULATION * SOAK_CYCLES);
    let mut all_echo = Vec::with_capacity(SOAK_POPULATION * SOAK_CYCLES);

    for cycle in 0..SOAK_CYCLES {
        let guests = spawn_population(
            harness,
            environment.clone(),
            observer.process.id(),
            SOAK_POPULATION,
        )
        .await?;
        all_spawn.extend(guests.iter().map(|guest| guest.spawn_elapsed));
        all_ready.extend(await_ready(&mut observer.inbox, &guests).await?);
        ensure!(
            environment.process_count() == SOAK_POPULATION + 1,
            "soak cycle {cycle} did not fill its process quota"
        );
        harness
            .assert_next_spawn_rejected(
                environment.clone(),
                observer.process.id(),
                SOAK_POPULATION as u64,
            )
            .await?;
        all_echo.extend(
            ping_population(
                &mut observer.inbox,
                &guests,
                1_000_000 + (cycle as i64 * 1_000),
            )
            .await?,
        );
        if let Some(current) = rss_bytes() {
            peak_rss = Some(peak_rss.map_or(current, |peak| peak.max(current)));
        }

        kill_and_join(guests).await?;
        wait_for_process_count(
            &environment,
            1,
            &format!("soak cycle {cycle} guest cleanup"),
        )
        .await?;
    }

    let post_cleanup_rss = rss_bytes();
    let spawn = percentiles(&all_spawn);
    let ready = percentiles(&all_ready);
    let echo = percentiles(&all_echo);
    println!(
        "LUNATIC_SCALE_EVIDENCE {{\"kind\":\"soak\",\"population\":{SOAK_POPULATION},\"cycles\":{SOAK_CYCLES},\"process_lifecycles\":{},\"spawn_p99_us\":{},\"ready_p99_us\":{},\"echo_p99_us\":{},\"rss_peak_delta_bytes\":{},\"rss_post_cleanup_delta_bytes\":{},\"registered_after_each_cycle\":1}}",
        SOAK_POPULATION * SOAK_CYCLES,
        spawn.p99_us,
        ready.p99_us,
        echo.p99_us,
        json_optional(rss_delta(baseline_rss, peak_rss)),
        json_optional(rss_delta(baseline_rss, post_cleanup_rss)),
    );

    observer.stop().await?;
    wait_for_process_count(&environment, 0, "soak observer cleanup").await
}

async fn run_extended_soak(
    harness: &Harness,
    environment_id: u64,
    config: ExtendedSoakConfig,
) -> Result<()> {
    let environment = Arc::new(LunaticEnvironment::with_max_processes(
        environment_id,
        config.population + 1,
    ));
    let mut observer = ObserverHandle::spawn(environment.clone())?;
    ensure!(
        environment.process_count() == 1,
        "extended-soak observer was not registered"
    );

    let baseline_rss = rss_bytes();
    let mut peak_rss = baseline_rss;
    let mut rss_samples = u64::from(baseline_rss.is_some());
    let started_at = Instant::now();
    let deadline = started_at
        .checked_add(config.duration)
        .context("extended-soak deadline overflowed Instant")?;
    let mut cycles = 0_u64;
    let mut mailbox_echo_samples = 0_u64;

    loop {
        if cycles > 0 && Instant::now() >= deadline {
            break;
        }

        let guests = spawn_population(
            harness,
            environment.clone(),
            observer.process.id(),
            config.population,
        )
        .await?;
        await_ready(&mut observer.inbox, &guests).await?;
        ensure!(
            environment.process_count() == config.population + 1,
            "extended-soak cycle {cycles} registered {} processes instead of {}",
            environment.process_count(),
            config.population + 1
        );

        for round in 0..config.echo_rounds {
            let echoes = ping_population(
                &mut observer.inbox,
                &guests,
                5_000_000 + (round * (config.population + 1)) as i64,
            )
            .await?;
            ensure!(
                echoes.len() == config.population,
                "extended-soak cycle {cycles} round {round} collected {} echoes instead of {}",
                echoes.len(),
                config.population
            );
            mailbox_echo_samples = mailbox_echo_samples
                .checked_add(echoes.len() as u64)
                .context("extended-soak mailbox sample count overflowed")?;
        }

        if let Some(current) = rss_bytes() {
            rss_samples += 1;
            peak_rss = Some(peak_rss.map_or(current, |peak| peak.max(current)));
        }

        kill_and_join(guests).await?;
        wait_for_process_count(
            &environment,
            1,
            &format!("extended-soak cycle {cycles} guest cleanup"),
        )
        .await?;
        if let Some(current) = rss_bytes() {
            rss_samples += 1;
            peak_rss = Some(peak_rss.map_or(current, |peak| peak.max(current)));
        }
        cycles += 1;

        let rss_growth = baseline_rss
            .zip(peak_rss)
            .map(|(baseline, peak)| peak.saturating_sub(baseline));
        if rss_growth.is_some_and(|growth| growth > config.max_rss_growth_bytes) {
            break;
        }
    }

    ensure!(cycles > 0, "extended soak completed no process cycles");
    observer.stop().await?;
    wait_for_process_count(&environment, 0, "extended-soak observer cleanup").await?;
    let final_rss = rss_bytes();
    if let Some(current) = final_rss {
        rss_samples += 1;
        peak_rss = Some(peak_rss.map_or(current, |peak| peak.max(current)));
    }

    let elapsed = started_at.elapsed();
    let process_lifecycles = cycles
        .checked_mul(config.population as u64)
        .context("extended-soak process lifecycle count overflowed")?;
    let rss_growth = baseline_rss
        .zip(peak_rss)
        .map(|(baseline, peak)| peak.saturating_sub(baseline));
    let rss_guard_passed = rss_growth.map(|growth| growth <= config.max_rss_growth_bytes);
    let registered_after_shutdown = environment.process_count();
    println!(
        "LUNATIC_SCALE_EVIDENCE {{\"kind\":\"extended_soak\",\"requested_duration_seconds\":{},\"elapsed_seconds\":{},\"population\":{},\"echo_rounds_per_cycle\":{},\"cycles\":{cycles},\"process_lifecycles\":{process_lifecycles},\"mailbox_echo_samples\":{mailbox_echo_samples},\"registered_after_each_cycle\":1,\"registered_after_shutdown\":{registered_after_shutdown},\"wasm_committed_bytes_per_guest\":{WASM_PAGE_BYTES},\"rss_baseline_bytes\":{},\"rss_peak_bytes\":{},\"rss_final_bytes\":{},\"rss_samples\":{rss_samples},\"rss_growth_bytes\":{},\"rss_growth_limit_bytes\":{},\"rss_guard_passed\":{},\"rss_attribution\":\"observed_process_wide_not_allocator_attributable\"}}",
        config.requested_duration_seconds,
        elapsed.as_secs(),
        config.population,
        config.echo_rounds,
        json_optional(baseline_rss),
        json_optional(peak_rss),
        json_optional(final_rss),
        json_optional(rss_growth),
        config.max_rss_growth_bytes,
        json_optional(rss_guard_passed),
    );

    ensure!(
        registered_after_shutdown == 0,
        "extended soak retained {registered_after_shutdown} registered processes after shutdown"
    );
    if let Some(growth) = rss_growth {
        ensure!(
            growth <= config.max_rss_growth_bytes,
            "observed process-wide RSS growth {growth} bytes exceeded the configured {}-byte guard",
            config.max_rss_growth_bytes
        );
    }
    ensure!(
        elapsed >= config.duration,
        "extended soak stopped after {elapsed:?}, before requested {:?}",
        config.duration
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_wasm_scale_soak_and_resource_pressure_are_bounded() -> Result<()> {
    timeout(OVERALL_WATCHDOG, async {
        let harness = Harness::new()?;
        let rss_curve = measure_live_population_rss_curve(&harness, 1_666_999).await?;
        for (offset, population) in SCALE_POPULATIONS.iter().copied().enumerate() {
            let rss = rss_curve
                .get(&population)
                .copied()
                .with_context(|| format!("RSS curve omitted population {population}"))?;
            run_scale_point(&harness, 1_667_000 + offset as u64, population, rss).await?;
        }
        run_resource_pressure(&harness, 1_667_100).await?;
        run_soak(&harness, 1_667_200).await
    })
    .await
    .context("actual-Wasm scale/soak gate exceeded its 90-second watchdog")?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "two-hour production soak; override LUNATIC_EXTENDED_SOAK_DURATION_SECS for a local smoke run"]
async fn extended_actual_wasm_soak_is_bounded() -> Result<()> {
    let config = ExtendedSoakConfig::from_environment()?;
    let watchdog = config
        .duration
        .checked_add(Duration::from_secs(5 * 60))
        .context("extended-soak watchdog duration overflowed")?;
    timeout(watchdog, async {
        let harness = Harness::new()?;
        run_extended_soak(&harness, 1_667_300, config).await
    })
    .await
    .with_context(|| {
        format!(
            "extended actual-Wasm soak exceeded its {:?} duration plus five-minute watchdog grace",
            config.duration
        )
    })?
}
