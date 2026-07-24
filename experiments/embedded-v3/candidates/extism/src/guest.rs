use std::cell::RefCell;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Context, Result};
use extism::{
    CompiledPlugin, Function, Manifest, Plugin, PluginBuilder, UserData, Val, ValType, Wasm,
};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactIdentity {
    pub logical_version: String,
    pub build_id: String,
    pub artifact_sha256: String,
}

#[derive(Debug, Clone, Copy)]
pub struct ArtifactSpec {
    pub file_name: &'static str,
    pub sha256: &'static str,
    pub guest_version: i32,
    pub build_marker: i64,
}

pub const ARTIFACTS: [ArtifactSpec; 4] = [
    ArtifactSpec {
        file_name: "tenant-a.wasm",
        sha256: "9bce3d46eceef79abe98d88d16c2a8ac75ec1ffd68a7f328cc34a9c1a1fae0d1",
        guest_version: 1,
        build_marker: 0x4133_5633_544e_5401,
    },
    ArtifactSpec {
        file_name: "tenant-b.wasm",
        sha256: "5c918a4ea3730ac7109188ec94108a135fb4a537a713bcbf27fae325a5ea511e",
        guest_version: 2,
        build_marker: 0x4233_5633_544e_5402,
    },
    ArtifactSpec {
        file_name: "tenant-bad-a.wasm",
        sha256: "d3675abac308b7a20513f2ff65042c9b0e9e84195c16ec5256f8472a15653a2d",
        guest_version: 1,
        build_marker: 0x4133_5633_4241_4401,
    },
    ArtifactSpec {
        file_name: "tenant-bad-b.wasm",
        sha256: "ab3274086121b6ff8af75b331c5c101b5216c187b86f8ea12cd06673e72dc557",
        guest_version: 2,
        build_marker: 0x4233_5633_4241_4402,
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationObservation {
    pub tenant_id: i32,
    pub guest_version: i32,
    pub build_marker: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultObservation {
    pub handle: i64,
    pub counter: i64,
    pub kind: i32,
    pub guest_version: i32,
    pub build_marker: i64,
}

type ExecutionObserver = Arc<dyn Fn(i64) + Send + Sync>;

struct HostState {
    tenant_id: i32,
    restore_counter: i64,
    command_handle: i64,
    activations: Vec<ActivationObservation>,
    results: Vec<ResultObservation>,
    execution_observer: Option<ExecutionObserver>,
}

impl HostState {
    fn new(tenant_id: i32, restore_counter: i64) -> Self {
        Self {
            tenant_id,
            restore_counter,
            command_handle: 0,
            activations: Vec::new(),
            results: Vec::new(),
            execution_observer: None,
        }
    }
}

thread_local! {
    static ACTIVE_HOST: RefCell<Option<Arc<Mutex<HostState>>>> = const { RefCell::new(None) };
}

fn with_active_host<T>(host: Arc<Mutex<HostState>>, f: impl FnOnce() -> T) -> T {
    ACTIVE_HOST.with(|slot| {
        let previous = slot.replace(Some(host));
        let result = f();
        slot.replace(previous);
        result
    })
}

fn active_host() -> Result<Arc<Mutex<HostState>>, extism::Error> {
    ACTIVE_HOST
        .with(|slot| slot.borrow().clone())
        .ok_or_else(|| anyhow!("comparison host callback ran without an active tenant"))
}

fn host_function(
    name: &str,
    params: impl IntoIterator<Item = ValType>,
    results: impl IntoIterator<Item = ValType>,
    callback: impl 'static
        + Fn(
            &mut extism::CurrentPlugin,
            &[Val],
            &mut [Val],
            UserData<()>,
        ) -> Result<(), extism::Error>
        + Send
        + Sync,
) -> Function {
    Function::new(name, params, results, UserData::new(()), callback).with_namespace("comparison")
}

pub fn production_host_functions() -> Vec<Function> {
    vec![
        host_function("tenant_id", [], [ValType::I32], |_, _, output, _| {
            let host = active_host()?;
            output[0] = Val::I32(host.lock().unwrap().tenant_id);
            Ok(())
        }),
        host_function("restore_counter", [], [ValType::I64], |_, _, output, _| {
            let host = active_host()?;
            output[0] = Val::I64(host.lock().unwrap().restore_counter);
            Ok(())
        }),
        host_function("command_handle", [], [ValType::I64], |_, _, output, _| {
            let host = active_host()?;
            output[0] = Val::I64(host.lock().unwrap().command_handle);
            Ok(())
        }),
        host_function(
            "activation_started",
            [ValType::I32, ValType::I32, ValType::I64],
            [],
            |_, input, _, _| {
                let host = active_host()?;
                host.lock()
                    .unwrap()
                    .activations
                    .push(ActivationObservation {
                        tenant_id: input[0].i32().unwrap(),
                        guest_version: input[1].i32().unwrap(),
                        build_marker: input[2].i64().unwrap(),
                    });
                Ok(())
            },
        ),
        host_function(
            "emit_result",
            [
                ValType::I64,
                ValType::I64,
                ValType::I32,
                ValType::I32,
                ValType::I64,
            ],
            [],
            |_, input, _, _| {
                let host = active_host()?;
                host.lock().unwrap().results.push(ResultObservation {
                    handle: input[0].i64().unwrap(),
                    counter: input[1].i64().unwrap(),
                    kind: input[2].i32().unwrap(),
                    guest_version: input[3].i32().unwrap(),
                    build_marker: input[4].i64().unwrap(),
                });
                Ok(())
            },
        ),
        host_function("execution_started", [ValType::I64], [], |_, input, _, _| {
            let host = active_host()?;
            let observer = host.lock().unwrap().execution_observer.clone();
            if let Some(observer) = observer {
                observer(input[0].i64().unwrap());
            }
            Ok(())
        }),
        host_function("bind_observer", [ValType::I64], [], |_, _, _, _| Ok(())),
        host_function("next_command", [], [ValType::I32], |_, _, output, _| {
            output[0] = Val::I32(0);
            Ok(())
        }),
    ]
}

#[derive(Clone)]
pub struct GuestCatalog {
    compiled: BTreeMap<String, CompiledPlugin>,
}

impl GuestCatalog {
    pub fn load(root: &Path) -> Result<Self> {
        let mut compiled = BTreeMap::new();
        for spec in ARTIFACTS {
            let path = root.join(spec.file_name);
            let bytes = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
            let actual = sha256_hex(&bytes);
            if actual != spec.sha256 {
                bail!(
                    "{} digest mismatch: expected {}, got {}",
                    spec.file_name,
                    spec.sha256,
                    actual
                );
            }
            let manifest = Manifest::new([Wasm::data(bytes)]);
            let plugin = PluginBuilder::new(manifest)
                .with_wasi(false)
                .with_functions(production_host_functions())
                .compile()
                .with_context(|| format!("compile {}", spec.file_name))?;
            compiled.insert(spec.sha256.to_owned(), plugin);
        }
        Ok(Self { compiled })
    }

    pub fn spec(&self, sha256: &str) -> Option<&'static ArtifactSpec> {
        ARTIFACTS.iter().find(|spec| spec.sha256 == sha256)
    }

    pub fn instantiate(
        &self,
        sha256: &str,
        tenant_id: i32,
        restore_counter: i64,
    ) -> std::result::Result<GuestInstance, InstantiationFailure> {
        let Some(compiled) = self.compiled.get(sha256) else {
            return Err(InstantiationFailure {
                message: format!("unknown core artifact {sha256}"),
                activations: Vec::new(),
            });
        };
        let Some(spec) = self.spec(sha256) else {
            return Err(InstantiationFailure {
                message: format!("artifact {sha256} has no frozen specification"),
                activations: Vec::new(),
            });
        };
        let host = Arc::new(Mutex::new(HostState::new(tenant_id, restore_counter)));
        let plugin = with_active_host(host.clone(), || Plugin::new_from_compiled(compiled));
        match plugin {
            Ok(mut plugin) => {
                let activation_check = with_active_host(host.clone(), || {
                    plugin.call::<&[u8], Vec<u8>>("activation_check", &[])
                });
                if let Err(error) = activation_check {
                    return Err(InstantiationFailure {
                        message: format!("activation_check failed: {error:#}"),
                        activations: host.lock().unwrap().activations.clone(),
                    });
                }
                Ok(GuestInstance { plugin, host, spec })
            }
            Err(error) => Err(InstantiationFailure {
                message: format!("plugin instantiation failed: {error:#}"),
                activations: host.lock().unwrap().activations.clone(),
            }),
        }
    }
}

#[derive(Debug)]
pub struct InstantiationFailure {
    pub message: String,
    pub activations: Vec<ActivationObservation>,
}

pub struct GuestInstance {
    plugin: Plugin,
    host: Arc<Mutex<HostState>>,
    spec: &'static ArtifactSpec,
}

impl GuestInstance {
    pub fn take_activations(&mut self) -> Vec<ActivationObservation> {
        std::mem::take(&mut self.host.lock().unwrap().activations)
    }

    pub fn call_business(&mut self, export: &str, handle: u64) -> Result<ResultObservation> {
        let handle = i64::try_from(handle).context("command handle exceeds i64")?;
        {
            let mut host = self.host.lock().unwrap();
            host.command_handle = handle;
            host.results.clear();
            host.execution_observer = None;
        }
        with_active_host(self.host.clone(), || {
            self.plugin.call::<&[u8], Vec<u8>>(export, &[])
        })
        .with_context(|| format!("guest export {export} failed"))?;
        let results = std::mem::take(&mut self.host.lock().unwrap().results);
        if results.len() != 1 {
            bail!("guest export {export} emitted {} results", results.len());
        }
        let result = results.into_iter().next().unwrap();
        self.validate_result(export, handle, &result)?;
        Ok(result)
    }

    pub fn call_trap(&mut self, handle: u64) -> Result<()> {
        self.prepare_nonbusiness_call(handle, None)?;
        with_active_host(self.host.clone(), || {
            self.plugin.call::<&[u8], Vec<u8>>("trap", &[])
        })?;
        bail!("trap export returned successfully")
    }

    pub fn call_cpu(&mut self, handle: u64, observer: ExecutionObserver) -> Result<()> {
        self.prepare_nonbusiness_call(handle, Some(observer))?;
        with_active_host(self.host.clone(), || {
            self.plugin.call::<&[u8], Vec<u8>>("cpu_loop", &[])
        })?;
        bail!("cpu_loop export returned successfully")
    }

    pub fn cancel_handle(&self) -> extism::CancelHandle {
        self.plugin.cancel_handle()
    }

    fn prepare_nonbusiness_call(
        &mut self,
        handle: u64,
        observer: Option<ExecutionObserver>,
    ) -> Result<()> {
        let handle = i64::try_from(handle).context("fault handle exceeds i64")?;
        let mut host = self.host.lock().unwrap();
        host.command_handle = handle;
        host.results.clear();
        host.execution_observer = observer;
        Ok(())
    }

    fn validate_result(&self, export: &str, handle: i64, result: &ResultObservation) -> Result<()> {
        let expected_kind = match export {
            "increment" => 1,
            "probe" => 2,
            "snapshot" => 3,
            other => bail!("unsupported business export {other}"),
        };
        if result.handle != handle
            || result.kind != expected_kind
            || result.guest_version != self.spec.guest_version
            || result.build_marker != self.spec.build_marker
        {
            bail!("guest result did not match handle/export/artifact identity");
        }
        Ok(())
    }
}

pub fn validate_activations(
    activations: &[ActivationObservation],
    tenant_id: i32,
    spec: &ArtifactSpec,
) -> Result<()> {
    // Extism registers the main module with its linker before lazily instantiating
    // the callable main instance, so a successful start is observed twice. A start
    // that traps during linker registration is observed once.
    if activations.is_empty() || activations.len() > 2 {
        bail!(
            "activation emitted an invalid number of markers: {}",
            activations.len()
        );
    }
    if activations.iter().any(|marker| {
        marker.tenant_id != tenant_id
            || marker.guest_version != spec.guest_version
            || marker.build_marker != spec.build_marker
    }) {
        bail!("activation marker did not match tenant and frozen artifact");
    }
    Ok(())
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
