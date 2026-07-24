use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use sha2::{Digest, Sha256};
use wasmtime::{
    Caller, Config, Engine, Instance, Linker, Module, Store, StoreLimits, StoreLimitsBuilder,
    TypedFunc,
};

use crate::protocol::{
    encode_event_line, ActivationStartedEvent, AuthorityProbeKind, CandidateKind, EventEnvelope,
    EventMessage, ExecutionStartOrigin, ExecutionStartedEvent, IncarnationToken, RequestId,
    TenantId,
};

const NORMAL_DEADLINE_TICKS: u64 = 1_000_000;
pub const CPU_DEADLINE_TICKS: u64 = 50;
const TENANT_MEMORY_LIMIT: usize = 10 * 1024 * 1024;

#[derive(Clone)]
pub struct EventSink {
    inner: Arc<Mutex<OutputState>>,
}

struct OutputState {
    next_sequence: u64,
    writer: BufWriter<io::Stdout>,
}

impl EventSink {
    pub fn stdout() -> Self {
        Self {
            inner: Arc::new(Mutex::new(OutputState {
                next_sequence: 1,
                writer: BufWriter::new(io::stdout()),
            })),
        }
    }

    pub fn emit(&self, request_id: RequestId, message: EventMessage) -> Result<()> {
        let mut output = self
            .inner
            .lock()
            .map_err(|_| anyhow!("stdout lock poisoned"))?;
        let envelope = EventEnvelope::new(output.next_sequence, request_id.0, message);
        let line = encode_event_line(&envelope).context("encode candidate event")?;
        output
            .writer
            .write_all(line.as_bytes())
            .context("write candidate event")?;
        output.writer.write_all(b"\n").context("write newline")?;
        output.writer.flush().context("flush candidate event")?;
        output.next_sequence = output
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("event sequence overflow"))?;
        Ok(())
    }
}

pub struct SharedRuntime {
    pub engine: Engine,
    pub catalog: ArtifactCatalog,
    ticker_stop: Arc<AtomicBool>,
    ticker: Mutex<Option<JoinHandle<()>>>,
}

impl SharedRuntime {
    pub fn initialize(artifact_root: &Path) -> Result<Arc<Self>> {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config)
            .map_err(wasmtime_error)
            .context("create shared Wasmtime engine")?;
        let catalog = ArtifactCatalog::load(&engine, artifact_root)?;
        let ticker_stop = Arc::new(AtomicBool::new(false));
        let ticker_engine = engine.clone();
        let stop = ticker_stop.clone();
        let ticker = thread::Builder::new()
            .name("embedded-v3-wasmtime-epoch".into())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(1));
                    ticker_engine.increment_epoch();
                }
            })
            .context("spawn Wasmtime epoch ticker")?;
        Ok(Arc::new(Self {
            engine,
            catalog,
            ticker_stop,
            ticker: Mutex::new(Some(ticker)),
        }))
    }

    pub fn stop_ticker(&self) -> Result<()> {
        self.ticker_stop.store(true, Ordering::Release);
        if let Some(ticker) = self
            .ticker
            .lock()
            .map_err(|_| anyhow!("ticker lock poisoned"))?
            .take()
        {
            ticker
                .join()
                .map_err(|_| anyhow!("epoch ticker panicked"))?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Artifact {
    pub module: Module,
    pub file_name: &'static str,
    pub sha256: &'static str,
    pub version_marker: i32,
    pub build_marker: i64,
}

#[derive(Clone)]
pub struct ArtifactCatalog {
    by_sha: HashMap<&'static str, Artifact>,
}

struct ArtifactSpec {
    file_name: &'static str,
    sha256: &'static str,
    version_marker: i32,
    build_marker: i64,
}

const ARTIFACT_SPECS: [ArtifactSpec; 4] = [
    ArtifactSpec {
        file_name: "tenant-a.wasm",
        sha256: "9bce3d46eceef79abe98d88d16c2a8ac75ec1ffd68a7f328cc34a9c1a1fae0d1",
        version_marker: 1,
        build_marker: 0x4133_5633_544e_5401,
    },
    ArtifactSpec {
        file_name: "tenant-b.wasm",
        sha256: "5c918a4ea3730ac7109188ec94108a135fb4a537a713bcbf27fae325a5ea511e",
        version_marker: 2,
        build_marker: 0x4233_5633_544e_5402,
    },
    ArtifactSpec {
        file_name: "tenant-bad-a.wasm",
        sha256: "d3675abac308b7a20513f2ff65042c9b0e9e84195c16ec5256f8472a15653a2d",
        version_marker: 1,
        build_marker: 0x4133_5633_4241_4401,
    },
    ArtifactSpec {
        file_name: "tenant-bad-b.wasm",
        sha256: "ab3274086121b6ff8af75b331c5c101b5216c187b86f8ea12cd06673e72dc557",
        version_marker: 2,
        build_marker: 0x4233_5633_4241_4402,
    },
];

impl ArtifactCatalog {
    fn load(engine: &Engine, artifact_root: &Path) -> Result<Self> {
        let mut by_sha = HashMap::new();
        for spec in ARTIFACT_SPECS {
            let path = artifact_root.join(spec.file_name);
            let bytes = fs::read(&path)
                .with_context(|| format!("read frozen core artifact {}", path.display()))?;
            let digest = sha256_hex(&bytes);
            if digest != spec.sha256 {
                return Err(anyhow!(
                    "artifact {} digest {digest} differs from frozen {}",
                    spec.file_name,
                    spec.sha256
                ));
            }
            let module = Module::new(engine, &bytes)
                .map_err(wasmtime_error)
                .with_context(|| format!("compile frozen artifact {}", spec.file_name))?;
            by_sha.insert(
                spec.sha256,
                Artifact {
                    module,
                    file_name: spec.file_name,
                    sha256: spec.sha256,
                    version_marker: spec.version_marker,
                    build_marker: spec.build_marker,
                },
            );
        }
        Ok(Self { by_sha })
    }

    pub fn by_sha(&self, sha256: &str) -> Result<Artifact> {
        self.by_sha
            .get(sha256)
            .cloned()
            .ok_or_else(|| anyhow!("unrecognized frozen core artifact digest {sha256}"))
    }

    pub fn rollout(&self, sha256: &str, artifact_ref: &str) -> Result<Artifact> {
        let artifact = self.by_sha(sha256)?;
        let expected = PathBuf::from("guest-artifacts").join(artifact.file_name);
        if Path::new(artifact_ref) != expected {
            return Err(anyhow!(
                "artifact_ref {artifact_ref:?} does not name {}",
                expected.display()
            ));
        }
        Ok(artifact)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub logical_version: String,
    pub build_id: String,
    pub artifact_sha256: String,
    pub version_marker: i32,
    pub build_marker: i64,
}

impl Identity {
    pub fn new(
        logical_version: String,
        build_id: String,
        artifact_sha256: String,
        artifact: &Artifact,
    ) -> Self {
        debug_assert_eq!(artifact_sha256, artifact.sha256);
        Self {
            logical_version,
            build_id,
            artifact_sha256,
            version_marker: artifact.version_marker,
            build_marker: artifact.build_marker,
        }
    }

    pub fn activation(&self, tenant: TenantId, incarnation: IncarnationToken) -> EventMessage {
        EventMessage::ActivationStarted(ActivationStartedEvent {
            tenant_id: tenant,
            incarnation,
            logical_version: self.logical_version.clone(),
            build_id: self.build_id.clone(),
            artifact_sha256: self.artifact_sha256.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GuestActivation {
    tenant: i32,
    version: i32,
    build: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GuestResult {
    handle: i64,
    counter: i64,
    kind: i32,
    version: i32,
    build: i64,
}

#[derive(Clone)]
pub(crate) struct FaultObserver {
    pub(crate) sink: EventSink,
    pub(crate) request_id: RequestId,
    pub(crate) fault_id: u64,
    pub(crate) tenant_id: TenantId,
    pub(crate) incarnation: IncarnationToken,
}

struct HostState {
    tenant: i32,
    restore_counter: i64,
    command_handle: i64,
    activations: Vec<GuestActivation>,
    results: Vec<GuestResult>,
    execution_observer: Option<FaultObserver>,
    observer_error: Option<String>,
    limits: StoreLimits,
}

impl HostState {
    fn new(tenant: u32, restore_counter: i64) -> Self {
        Self {
            tenant: tenant as i32,
            restore_counter,
            command_handle: 0,
            activations: Vec::new(),
            results: Vec::new(),
            execution_observer: None,
            observer_error: None,
            limits: StoreLimitsBuilder::new()
                .memory_size(TENANT_MEMORY_LIMIT)
                .instances(1)
                .memories(1)
                .build(),
        }
    }
}

fn production_linker(engine: &Engine) -> Result<Linker<HostState>> {
    let mut linker = Linker::new(engine);
    linker
        .func_wrap(
            "comparison",
            "tenant_id",
            |caller: Caller<'_, HostState>| caller.data().tenant,
        )
        .map_err(wasmtime_error)?;
    linker
        .func_wrap(
            "comparison",
            "restore_counter",
            |caller: Caller<'_, HostState>| caller.data().restore_counter,
        )
        .map_err(wasmtime_error)?;
    linker
        .func_wrap(
            "comparison",
            "command_handle",
            |caller: Caller<'_, HostState>| caller.data().command_handle,
        )
        .map_err(wasmtime_error)?;
    linker
        .func_wrap(
            "comparison",
            "activation_started",
            |mut caller: Caller<'_, HostState>, tenant: i32, version: i32, build: i64| {
                caller.data_mut().activations.push(GuestActivation {
                    tenant,
                    version,
                    build,
                });
            },
        )
        .map_err(wasmtime_error)?;
    linker
        .func_wrap(
            "comparison",
            "emit_result",
            |mut caller: Caller<'_, HostState>,
             handle: i64,
             counter: i64,
             kind: i32,
             version: i32,
             build: i64| {
                caller.data_mut().results.push(GuestResult {
                    handle,
                    counter,
                    kind,
                    version,
                    build,
                });
            },
        )
        .map_err(wasmtime_error)?;
    linker
        .func_wrap(
            "comparison",
            "execution_started",
            |mut caller: Caller<'_, HostState>, handle: i64| {
                let observer = caller.data().execution_observer.clone();
                if let Some(observer) = observer {
                    if handle != observer.fault_id as i64 {
                        caller.data_mut().observer_error = Some(format!(
                            "execution marker handle {handle} differs from fault {}",
                            observer.fault_id
                        ));
                        return;
                    }
                    let message = EventMessage::ExecutionStarted(ExecutionStartedEvent {
                        fault_id: observer.fault_id.into(),
                        tenant_id: observer.tenant_id,
                        incarnation: observer.incarnation,
                        origin: ExecutionStartOrigin::GuestFirstActionObserver,
                    });
                    if let Err(error) = observer.sink.emit(observer.request_id, message) {
                        caller.data_mut().observer_error = Some(error.to_string());
                    }
                } else {
                    caller.data_mut().observer_error =
                        Some("execution_started without an armed CPU observer".into());
                }
            },
        )
        .map_err(wasmtime_error)?;
    linker
        .func_wrap(
            "comparison",
            "bind_observer",
            |_caller: Caller<'_, HostState>, _observer: i64| {},
        )
        .map_err(wasmtime_error)?;
    linker
        .func_wrap(
            "comparison",
            "next_command",
            |_caller: Caller<'_, HostState>| 0_i32,
        )
        .map_err(wasmtime_error)?;
    Ok(linker)
}

pub struct Guest {
    store: Store<HostState>,
    _instance: Instance,
    increment: TypedFunc<(), ()>,
    probe: TypedFunc<(), ()>,
    snapshot: TypedFunc<(), ()>,
    trap: TypedFunc<(), ()>,
    cpu_loop: TypedFunc<(), ()>,
}

pub struct InstantiationFailure {
    pub activation_observed: bool,
    pub detail: String,
}

impl Guest {
    pub fn instantiate(
        runtime: &SharedRuntime,
        artifact: &Artifact,
        identity: &Identity,
        tenant_id: TenantId,
        restore_counter: i64,
    ) -> std::result::Result<Self, InstantiationFailure> {
        let mut store = Store::new(
            &runtime.engine,
            HostState::new(tenant_id.0, restore_counter),
        );
        store.limiter(|state| &mut state.limits);
        store.epoch_deadline_trap();
        store.set_epoch_deadline(NORMAL_DEADLINE_TICKS);
        let linker = production_linker(&runtime.engine).map_err(|error| InstantiationFailure {
            activation_observed: false,
            detail: error.to_string(),
        })?;
        let instance = match linker.instantiate(&mut store, &artifact.module) {
            Ok(instance) => instance,
            Err(error) => {
                return Err(InstantiationFailure {
                    activation_observed: activation_matches(
                        &store.data().activations,
                        identity,
                        tenant_id,
                    ),
                    detail: error.to_string(),
                });
            }
        };
        if !activation_matches(&store.data().activations, identity, tenant_id) {
            return Err(InstantiationFailure {
                activation_observed: false,
                detail: "guest activation marker did not match tenant/version/build".into(),
            });
        }
        let activation_check = typed(&instance, &mut store, "activation_check")?;
        let increment = typed(&instance, &mut store, "increment")?;
        let probe = typed(&instance, &mut store, "probe")?;
        let snapshot = typed(&instance, &mut store, "snapshot")?;
        let trap = typed(&instance, &mut store, "trap")?;
        let cpu_loop = typed(&instance, &mut store, "cpu_loop")?;
        activation_check
            .call(&mut store, ())
            .map_err(|error| InstantiationFailure {
                activation_observed: true,
                detail: error.to_string(),
            })?;
        Ok(Self {
            store,
            _instance: instance,
            increment,
            probe,
            snapshot,
            trap,
            cpu_loop,
        })
    }

    pub fn call_command(&mut self, handle: u64, increment: bool) -> Result<(i64, i32)> {
        self.arm_call(handle)?;
        if increment {
            self.increment
                .call(&mut self.store, ())
                .map_err(wasmtime_error)?;
        } else {
            self.probe
                .call(&mut self.store, ())
                .map_err(wasmtime_error)?;
        }
        let result = self.take_result(handle, if increment { 1 } else { 2 })?;
        Ok((result.counter, result.kind))
    }

    pub fn call_snapshot(&mut self, handle: u64) -> Result<i64> {
        self.arm_call(handle)?;
        self.snapshot
            .call(&mut self.store, ())
            .map_err(wasmtime_error)?;
        Ok(self.take_result(handle, 3)?.counter)
    }

    pub fn call_trap(&mut self) -> Result<()> {
        self.store.set_epoch_deadline(NORMAL_DEADLINE_TICKS);
        match self.trap.call(&mut self.store, ()) {
            Err(error)
                if error.downcast_ref::<wasmtime::Trap>()
                    == Some(&wasmtime::Trap::UnreachableCodeReached) =>
            {
                Ok(())
            }
            Err(error) => Err(anyhow!("trap export failed with another cause: {error:#}")),
            Ok(()) => Err(anyhow!("trap export returned successfully")),
        }
    }

    pub(crate) fn call_cpu_loop(&mut self, observer: FaultObserver) -> Result<()> {
        self.store.data_mut().command_handle = observer.fault_id as i64;
        self.store.data_mut().execution_observer = Some(observer);
        self.store.data_mut().observer_error = None;
        self.store.set_epoch_deadline(CPU_DEADLINE_TICKS);
        let call_result = self.cpu_loop.call(&mut self.store, ());
        self.store.data_mut().execution_observer = None;
        if let Some(error) = self.store.data_mut().observer_error.take() {
            return Err(anyhow!(error));
        }
        match call_result {
            Ok(()) => Err(anyhow!("cpu_loop returned successfully")),
            Err(error)
                if error.downcast_ref::<wasmtime::Trap>() == Some(&wasmtime::Trap::Interrupt) =>
            {
                Ok(())
            }
            Err(error) => Err(anyhow!(
                "cpu_loop failed without epoch interruption: {error:#}"
            )),
        }
    }

    fn arm_call(&mut self, handle: u64) -> Result<()> {
        let handle = i64::try_from(handle).context("command handle exceeds signed guest ABI")?;
        self.store.data_mut().command_handle = handle;
        self.store.data_mut().results.clear();
        self.store.set_epoch_deadline(NORMAL_DEADLINE_TICKS);
        Ok(())
    }

    fn take_result(&mut self, handle: u64, expected_kind: i32) -> Result<GuestResult> {
        if self.store.data().results.len() != 1 {
            return Err(anyhow!(
                "guest emitted {} results",
                self.store.data().results.len()
            ));
        }
        let result = self.store.data().results[0].clone();
        if result.handle != handle as i64 || result.kind != expected_kind {
            return Err(anyhow!("guest result handle or kind mismatch"));
        }
        Ok(result)
    }

    pub fn verify_identity(&self, identity: &Identity) -> Result<()> {
        let result = self
            .store
            .data()
            .results
            .last()
            .ok_or_else(|| anyhow!("guest did not emit a result"))?;
        if result.version != identity.version_marker || result.build != identity.build_marker {
            return Err(anyhow!("guest result version/build marker mismatch"));
        }
        Ok(())
    }
}

fn typed(
    instance: &Instance,
    store: &mut Store<HostState>,
    name: &str,
) -> std::result::Result<TypedFunc<(), ()>, InstantiationFailure> {
    instance
        .get_typed_func(store, name)
        .map_err(|error| InstantiationFailure {
            activation_observed: true,
            detail: format!("missing or mistyped export {name}: {error}"),
        })
}

fn activation_matches(
    activations: &[GuestActivation],
    identity: &Identity,
    tenant_id: TenantId,
) -> bool {
    activations
        == [GuestActivation {
            tenant: tenant_id.0 as i32,
            version: identity.version_marker,
            build: identity.build_marker,
        }]
}

pub fn authority_absent_at_link(
    runtime: &SharedRuntime,
    probe: AuthorityProbeKind,
    bytes: &[u8],
) -> Result<()> {
    let module = Module::new(&runtime.engine, bytes)
        .map_err(wasmtime_error)
        .context("compile authority artifact")?;
    let expected = expected_first_unresolved_import(probe);
    let first = module
        .imports()
        .next()
        .map(|import| (import.module().to_owned(), import.name().to_owned()))
        .ok_or_else(|| anyhow!("authority artifact has no capability import"))?;
    if first != (expected.0.to_owned(), expected.1.to_owned()) {
        return Err(anyhow!(
            "authority first import {:?}::{:?} differs from {:?}::{:?}",
            first.0,
            first.1,
            expected.0,
            expected.1
        ));
    }
    let mut store = Store::new(&runtime.engine, HostState::new(0, 0));
    store.limiter(|state| &mut state.limits);
    store.epoch_deadline_trap();
    store.set_epoch_deadline(NORMAL_DEADLINE_TICKS);
    let linker = production_linker(&runtime.engine)?;
    match linker.instantiate(&mut store, &module) {
        Ok(_) => Err(anyhow!(
            "authority artifact unexpectedly linked through production linker"
        )),
        Err(error) => {
            let chain = format!("{error:#}");
            if chain.contains("unknown import")
                && chain.contains(expected.0)
                && chain.contains(expected.1)
            {
                Ok(())
            } else {
                Err(anyhow!(
                    "authority instantiation was not the exact unknown-import failure: {chain}"
                ))
            }
        }
    }
}

fn expected_first_unresolved_import(probe: AuthorityProbeKind) -> (&'static str, &'static str) {
    match probe {
        AuthorityProbeKind::WasiP1FsRead | AuthorityProbeKind::WasiP1FsMutate => {
            ("wasi_snapshot_preview1", "path_open")
        }
        AuthorityProbeKind::LunaticTcp => ("lunatic::networking", "tcp_connect"),
        AuthorityProbeKind::LunaticUdp => ("lunatic::networking", "udp_bind"),
        AuthorityProbeKind::LunaticSqliteCreate => ("lunatic::sqlite", "open"),
        AuthorityProbeKind::ExtismHttp => ("extism:host/env", "alloc"),
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn wasmtime_error(error: wasmtime::Error) -> anyhow::Error {
    anyhow!("{error:#}")
}

pub fn implementation_hello() -> EventMessage {
    EventMessage::Hello(crate::protocol::HelloEvent {
        candidate: CandidateKind::RawWasmtime,
        implementation_version: "embedded-v3-direct-wasmtime-1".into(),
    })
}
