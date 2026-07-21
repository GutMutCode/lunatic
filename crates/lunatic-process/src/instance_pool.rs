use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use wasmtime::ResourceLimiter;

use crate::runtimes::wasmtime::{
    MemorySnapshot, WasmtimeCompiledModule, WasmtimeInstance, WasmtimeRuntime,
};
use crate::state::ProcessState;

struct PooledInstance<S>
where
    S: Send + 'static,
{
    instance: WasmtimeInstance<S>,
    last_used: Instant,
}

const INSTANCE_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstancePoolStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub created: usize,
    pub pool_size: usize,
}

pub struct PoolMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub created: AtomicUsize,
}

impl PoolMetrics {
    fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            created: AtomicUsize::new(0),
        }
    }
}

#[cfg(feature = "metrics")]
fn increment_counter(name: &'static str) {
    metrics::increment_counter!(name);
}

#[cfg(not(feature = "metrics"))]
fn increment_counter(_name: &'static str) {}

#[cfg(feature = "metrics")]
fn record_gauge(name: &'static str, value: f64) {
    metrics::gauge!(name, value);
}

#[cfg(not(feature = "metrics"))]
fn record_gauge(_name: &'static str, _value: f64) {}

pub struct InstancePool<S>
where
    S: ProcessState + Send + 'static,
{
    pool: Arc<Mutex<VecDeque<PooledInstance<S>>>>,
    module: Arc<WasmtimeCompiledModule<S>>,
    runtime: WasmtimeRuntime,
    max_size: usize,
    metrics: Arc<PoolMetrics>,
    initial_snapshot: Arc<Mutex<Option<MemorySnapshot>>>,
}

impl<S> InstancePool<S>
where
    S: ProcessState + Send + ResourceLimiter + 'static,
{
    pub fn new(
        module: Arc<WasmtimeCompiledModule<S>>,
        runtime: WasmtimeRuntime,
        max_size: usize,
    ) -> Self {
        Self {
            pool: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            module,
            runtime,
            max_size,
            metrics: Arc::new(PoolMetrics::new()),
            initial_snapshot: Arc::new(Mutex::new(None)),
        }
    }

    #[allow(clippy::await_holding_lock)]
    pub async fn acquire(&self, state: S) -> Result<WasmtimeInstance<S>> {
        let mut pool = self.pool.lock().unwrap();
        while let Some(pooled) = pool.pop_front() {
            if pooled.last_used.elapsed() > INSTANCE_TTL {
                self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
                increment_counter("lunatic.instance_pool.evicted_total");
                log::trace!("Discarded stale instance (>{:?})", INSTANCE_TTL);
                continue;
            }

            drop(pool);
            self.metrics.hits.fetch_add(1, Ordering::Relaxed);
            increment_counter("lunatic.instance_pool.hit_total");
            log::trace!("Instance pool hit (reusing existing instance)");

            let instance = self.reset_instance(pooled.instance, state).await?;
            self.record_pool_size();
            return Ok(instance);
        }
        drop(pool);

        self.metrics.misses.fetch_add(1, Ordering::Relaxed);
        self.metrics.created.fetch_add(1, Ordering::Relaxed);
        increment_counter("lunatic.instance_pool.miss_total");
        increment_counter("lunatic.instance_pool.created_total");
        log::trace!("Instance pool miss (creating new instance)");

        let instance = self.create_instance(state).await?;
        self.record_pool_size();
        Ok(instance)
    }

    pub fn release(&self, instance: WasmtimeInstance<S>) {
        let len_after = {
            let mut pool = self.pool.lock().unwrap();
            if pool.len() < self.max_size {
                pool.push_back(PooledInstance {
                    instance,
                    last_used: Instant::now(),
                });
                let len = pool.len();
                log::trace!("Released instance to pool (pool size: {})", len);
                len
            } else {
                self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
                increment_counter("lunatic.instance_pool.evicted_total");
                log::trace!("Pool full, dropping instance (max: {})", self.max_size);
                pool.len()
            }
        };

        self.record_pool_size_with(len_after);
    }

    fn record_pool_size(&self) {
        let size = self.pool_size();
        self.record_pool_size_with(size);
    }

    fn record_pool_size_with(&self, size: usize) {
        record_gauge("lunatic.instance_pool.size", size as f64);
    }

    pub fn stats(&self) -> InstancePoolStats {
        InstancePoolStats {
            hits: self.metrics.hits.load(Ordering::Relaxed),
            misses: self.metrics.misses.load(Ordering::Relaxed),
            evictions: self.metrics.evictions.load(Ordering::Relaxed),
            created: self.metrics.created.load(Ordering::Relaxed),
            pool_size: self.pool_size(),
        }
    }

    async fn create_instance(&self, state: S) -> Result<WasmtimeInstance<S>> {
        let mut instance = self.runtime.instantiate(&self.module, state).await?;
        let snapshot = instance.snapshot_memory()?;
        let mut guard = self.initial_snapshot.lock().unwrap();
        guard.get_or_insert(snapshot);
        Ok(instance)
    }

    async fn reset_instance(
        &self,
        instance: WasmtimeInstance<S>,
        new_state: S,
    ) -> Result<WasmtimeInstance<S>> {
        let mut instance = instance;

        if let Some(snapshot) = self.initial_snapshot.lock().unwrap().as_ref() {
            instance.restore_memory(snapshot)?;
        } else {
            let snapshot = instance.snapshot_memory()?;
            *self.initial_snapshot.lock().unwrap() = Some(snapshot);
        }

        {
            let slot = instance.state_mut();
            let _old_state = std::mem::replace(slot, new_state);
            // dropping _old_state releases resources from previous run
        }

        instance.state_mut().initialize();

        Ok(instance)
    }

    pub fn metrics(&self) -> &PoolMetrics {
        &self.metrics
    }

    pub fn pool_size(&self) -> usize {
        self.pool.lock().unwrap().len()
    }
}

impl<S> Clone for InstancePool<S>
where
    S: ProcessState + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
            module: Arc::clone(&self.module),
            runtime: self.runtime.clone(),
            max_size: self.max_size,
            metrics: Arc::clone(&self.metrics),
            initial_snapshot: Arc::clone(&self.initial_snapshot),
        }
    }
}
