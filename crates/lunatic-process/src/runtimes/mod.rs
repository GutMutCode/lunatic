//! WebAssembly runtimes powering lunatic.
//!
//! Currently only Wasmtime is supported, but it should be "easy" to add any runtime that has a
//! `Linker` abstraction and supports `async` host functions.
//!
//! NOTE: This traits are not used at all. Until rust supports async-traits all functions working
//!       with a runtime will directly take `wasmtime::WasmtimeRuntime` instead of a generic.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use dashmap::DashMap;
use tokio::task::JoinHandle;

use crate::state::ProcessState;

use self::wasmtime::{WasmtimeCompiledModule, WasmtimeRuntime};

pub mod wasmtime;

/// Maximum number of compiled modules retained in the distributed module cache.
pub const DEFAULT_MAX_CACHED_MODULES: usize = 256;

pub struct RawWasm {
    // Id returned by control and used when spawning modules on other nodes
    pub id: Option<u64>,
    pub bytes: Vec<u8>,
}

impl RawWasm {
    pub fn new(id: Option<u64>, bytes: Vec<u8>) -> Self {
        Self { id, bytes }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

impl From<Vec<u8>> for RawWasm {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(None, bytes)
    }
}

/// A `WasmRuntime` is a compiler that can generate runnable code from raw .wasm files.
///
/// It also provides a mechanism to register host functions that are accessible to the wasm guest
/// code through the generic type `T`. The type `T` must implement the [`ProcessState`] trait and
/// expose a `register` function for host functions.
pub trait WasmRuntime<T>: Clone
where
    T: crate::state::ProcessState + Default + Send,
{
    type WasmInstance: WasmInstance;

    /// Takes a raw binary WebAssembly module and returns the index of a compiled module.
    fn compile_module(&mut self, data: RawWasm) -> anyhow::Result<usize>;

    /// Returns a reference to the raw binary WebAssembly module if the index exists.
    fn wasm_module(&self, index: usize) -> Option<&RawWasm>;

    // Creates a wasm instance from compiled module if the index exists.
    /* async fn instantiate(
        &self,
        index: usize,
        state: T,
        config: ProcessConfig,
    ) -> Result<WasmtimeInstance<T>>; */
}

pub trait WasmInstance {
    type Param;

    // Calls a wasm function by name with the specified arguments. Ignores the returned values.
    /* async fn call(&mut self, function: &str, params: Vec<Self::Param>) -> Result<()>; */
}

pub struct Modules<T> {
    modules: Arc<DashMap<u64, Arc<WasmtimeCompiledModule<T>>>>,
    max_entries: usize,
    compile_lock: Arc<Mutex<()>>,
}

impl<T> Clone for Modules<T> {
    fn clone(&self) -> Self {
        Self {
            modules: self.modules.clone(),
            max_entries: self.max_entries,
            compile_lock: self.compile_lock.clone(),
        }
    }
}

impl<T> Default for Modules<T> {
    fn default() -> Self {
        Self::with_max_entries(DEFAULT_MAX_CACHED_MODULES)
    }
}

impl<T> Modules<T> {
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            modules: Arc::new(DashMap::new()),
            max_entries,
            compile_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn len(&self) -> usize {
        self.modules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    /// Removes a cache entry only when no process or caller retains either the
    /// cached `Arc` or a compiled-module clone. This avoids creating two live
    /// compiled identities for one distributed module ID.
    pub fn remove_if_unused(&self, module_id: u64) -> bool {
        self.modules
            .remove_if(&module_id, |_, module| {
                Arc::strong_count(module) == 1 && module.has_unique_inner()
            })
            .is_some()
    }
}

impl<T: ProcessState + 'static> Modules<T> {
    pub fn get(&self, module_id: u64) -> Option<Arc<WasmtimeCompiledModule<T>>> {
        self.modules.get(&module_id).map(|m| m.clone())
    }

    pub fn compile(
        &self,
        runtime: WasmtimeRuntime,
        wasm: RawWasm,
    ) -> JoinHandle<Result<Arc<WasmtimeCompiledModule<T>>>> {
        let modules = self.modules.clone();
        let max_entries = self.max_entries;
        let compile_lock = self.compile_lock.clone();
        tokio::task::spawn_blocking(move || {
            let id = wasm.id;

            // Serialize cache misses so concurrent requests for the same ID
            // reuse one compiled module and cache capacity cannot be raced.
            let _compile_guard = compile_lock
                .lock()
                .map_err(|_| anyhow::anyhow!("distributed module cache lock poisoned"))?;
            if let Some(id) = id {
                if let Some(module) = modules.get(&id) {
                    return Ok(module.clone());
                }
                if modules.len() >= max_entries {
                    // Cache entries are only ownership roots, not active-use
                    // declarations. Reclaim entries whose wrapper and compiled
                    // inner module are otherwise unreferenced before rejecting
                    // a new distributed module ID.
                    let unused_ids: Vec<u64> = modules
                        .iter()
                        .filter(|entry| {
                            Arc::strong_count(entry.value()) == 1
                                && entry.value().has_unique_inner()
                        })
                        .map(|entry| *entry.key())
                        .collect();
                    for unused_id in unused_ids {
                        modules.remove_if(&unused_id, |_, module| {
                            Arc::strong_count(module) == 1 && module.has_unique_inner()
                        });
                        if modules.len() < max_entries {
                            break;
                        }
                    }
                }
                if modules.len() >= max_entries {
                    anyhow::bail!(
                        "distributed module cache limit ({max_entries}) reached; all cached modules are still in use"
                    );
                }
            }

            match runtime.compile_module(wasm) {
                Ok(m) => {
                    let module = Arc::new(m);
                    if let Some(id) = id {
                        modules.insert(id, Arc::clone(&module));
                    }
                    Ok(module)
                }
                Err(e) => Err(e),
            }
        })
    }
}
