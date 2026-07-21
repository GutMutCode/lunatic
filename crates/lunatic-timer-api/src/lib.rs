use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use hash_map_id::HashMapId;
use lunatic_common_api::{IntoTrap, LinkerAsyncExt};
use lunatic_process::{state::ProcessState, Signal};
use lunatic_process_api::ProcessCtx;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};
use wasmtime::{Caller, Linker, ToWasmtimeResult as _};

#[derive(Debug)]
struct HeapValue {
    instant: Instant,
    key: u64,
}

impl PartialOrd for HeapValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.instant.cmp(&other.instant).reverse()
    }
}

impl PartialEq for HeapValue {
    fn eq(&self, other: &Self) -> bool {
        self.instant.eq(&other.instant)
    }
}

impl Eq for HeapValue {}

/// Finite default for host-side delayed-message tasks owned by one process.
pub const DEFAULT_MAX_TIMERS: usize = 1_024;

#[derive(Debug)]
struct TimerEntry {
    handle: JoinHandle<()>,
    // The permit is released when a timer completes and is cleaned up, is
    // canceled, or its owning process state is dropped.
    _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub struct TimerResources {
    hash_map: HashMapId<TimerEntry>,
    heap: BinaryHeap<HeapValue>,
    admission: Arc<Semaphore>,
    capacity: usize,
}

impl Default for TimerResources {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_MAX_TIMERS)
    }
}

impl TimerResources {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            hash_map: HashMapId::new(),
            heap: BinaryHeap::new(),
            admission: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }

    fn try_reserve(&mut self) -> Result<OwnedSemaphorePermit> {
        self.cleanup_expired_timers();
        Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| anyhow!("timer limit ({}) reached", self.capacity))
    }

    fn add(
        &mut self,
        handle: JoinHandle<()>,
        target_time: Instant,
        permit: OwnedSemaphorePermit,
    ) -> u64 {
        let id = self.hash_map.add(TimerEntry {
            handle,
            _permit: permit,
        });
        self.heap.push(HeapValue {
            instant: target_time,
            key: id,
        });
        id
    }

    fn cleanup_expired_timers(&mut self) {
        let deadline = Instant::now();
        while let Some(HeapValue { instant, key }) = self.heap.peek() {
            if *instant > deadline {
                // instant is after the deadline so stop
                return;
            }

            // A deadline only makes a timer runnable; it does not prove the
            // spawned future has completed. Releasing admission for an
            // unfinished zero-delay task would detach its JoinHandle and let
            // a tight guest loop retain unbounded messages outside the Store.
            if self
                .hash_map
                .get(*key)
                .is_some_and(|entry| !entry.handle.is_finished())
            {
                return;
            }

            let key = self
                .heap
                .pop()
                .expect("not empty because we matched on peek")
                .key;
            self.hash_map.remove(key);
        }
    }

    pub fn remove(&mut self, id: u64) -> Option<JoinHandle<()>> {
        let entry = self.hash_map.remove(id)?;
        // Canceled far-future timers must not leave unbounded stale deadline
        // metadata in the heap while their admission permits are reused.
        self.heap.retain(|deadline| deadline.key != id);
        Some(entry.handle)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.hash_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hash_map.is_empty()
    }
}

impl Drop for TimerResources {
    fn drop(&mut self) {
        for (_, entry) in self.hash_map.iter() {
            entry.handle.abort();
        }
    }
}

pub trait TimerCtx {
    fn timer_resources(&self) -> &TimerResources;
    fn timer_resources_mut(&mut self) -> &mut TimerResources;
}

pub fn register<T: ProcessState + ProcessCtx<T> + TimerCtx + Send + 'static>(
    linker: &mut Linker<T>,
) -> Result<()> {
    linker.func_wrap(
        "lunatic::timer",
        "send_after",
        |caller: Caller<'_, T>, process_id: u64, duration: u64| {
            send_after(caller, process_id, duration).to_wasmtime_result()
        },
    )?;
    linker.func_wrap1_async("lunatic::timer", "cancel_timer", cancel_timer)?;

    #[cfg(feature = "metrics")]
    metrics::describe_counter!(
        "lunatic.timers.started",
        metrics::Unit::Count,
        "number of timers set since startup, will usually be completed + canceled + active"
    );
    #[cfg(feature = "metrics")]
    metrics::describe_counter!(
        "lunatic.timers.completed",
        metrics::Unit::Count,
        "number of timers completed since startup"
    );
    #[cfg(feature = "metrics")]
    metrics::describe_counter!(
        "lunatic.timers.canceled",
        metrics::Unit::Count,
        "number of timers canceled since startup"
    );
    #[cfg(feature = "metrics")]
    metrics::describe_gauge!(
        "lunatic.timers.active",
        metrics::Unit::Count,
        "number of timers currently active"
    );

    Ok(())
}

// Sends the message to a process after a delay.
//
// There are no guarantees that the message will be received.
//
// Traps:
// * If the process ID doesn't exist.
// * If it's called before creating the next message.
fn send_after<T: ProcessState + ProcessCtx<T> + TimerCtx>(
    mut caller: Caller<T>,
    process_id: u64,
    delay: u64,
) -> Result<u64> {
    let process = caller
        .data()
        .environment()
        .get_process(process_id)
        .or_trap("lunatic::timer::send_after: process does not exist")?;
    let target_time = Instant::now()
        .checked_add(Duration::from_millis(delay))
        .ok_or_else(|| anyhow!("timer deadline overflow"))?;
    // Reserve before taking the scratch message. A quota failure is therefore
    // explicit and ownership preserving, and no detached task can bypass the
    // per-process timer ceiling.
    let timer_permit = caller.data_mut().timer_resources_mut().try_reserve()?;
    let message = caller
        .data_mut()
        .message_scratch_area()
        .take()
        .or_trap("lunatic::message::send_after")?;

    let timer_handle = tokio::task::spawn(async move {
        #[cfg(feature = "metrics")]
        metrics::increment_counter!("lunatic.timers.started");
        #[cfg(feature = "metrics")]
        metrics::increment_gauge!("lunatic.timers.active", 1.0);
        let duration_remaining = target_time.saturating_duration_since(Instant::now());
        if duration_remaining != Duration::ZERO {
            tokio::time::sleep(duration_remaining).await;
        }
        #[cfg(feature = "metrics")]
        metrics::increment_counter!("lunatic.timers.completed");
        #[cfg(feature = "metrics")]
        metrics::decrement_gauge!("lunatic.timers.active", 1.0);
        if let Err(error) = process.send(Signal::Message(message)) {
            log::warn!(
                "Delayed message to process {} was rejected: {}",
                process.id(),
                error
            );
        }
    });

    let id = caller
        .data_mut()
        .timer_resources_mut()
        .add(timer_handle, target_time, timer_permit);
    Ok(id)
}

// Cancels the specified timer.
//
// Returns:
// * 1 if a timer with the timer_id was found
// * 0 if no timer was found, this can be either because:
//     - timer had expired
//     - timer already had been canceled
//     - timer_id never corresponded to a timer
fn cancel_timer<T: ProcessState + TimerCtx + Send>(
    mut caller: Caller<T>,
    timer_id: u64,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        let timer_handle = caller.data_mut().timer_resources_mut().remove(timer_id);
        match timer_handle {
            Some(timer_handle) => {
                timer_handle.abort();
                #[cfg(feature = "metrics")]
                metrics::increment_counter!("lunatic.timers.canceled");
                #[cfg(feature = "metrics")]
                metrics::decrement_gauge!("lunatic.timers.active", 1.0);
                Ok(1)
            }
            None => Ok(0),
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{future::pending, time::Duration};

    use super::TimerResources;

    #[tokio::test]
    async fn active_timer_limit_is_explicit_and_cancel_recovers_capacity() {
        let mut timers = TimerResources::with_capacity(1);
        let permit = timers.try_reserve().unwrap();
        let handle = tokio::spawn(pending());
        let timer_id = timers.add(
            handle,
            std::time::Instant::now() + Duration::from_secs(60),
            permit,
        );

        assert_eq!(timers.capacity(), 1);
        assert_eq!(timers.len(), 1);
        assert!(timers.try_reserve().is_err());

        timers.remove(timer_id).unwrap().abort();
        assert!(timers.is_empty());
        assert!(timers.heap.is_empty());
        assert!(timers.try_reserve().is_ok());
    }

    #[tokio::test]
    async fn repeated_far_future_cancellation_does_not_grow_deadline_heap() {
        let mut timers = TimerResources::with_capacity(1);
        for _ in 0..10_000 {
            let permit = timers.try_reserve().unwrap();
            let handle = tokio::spawn(pending());
            let timer_id = timers.add(
                handle,
                std::time::Instant::now() + Duration::from_secs(86_400),
                permit,
            );
            timers.remove(timer_id).unwrap().abort();
        }
        assert!(timers.is_empty());
        assert!(timers.heap.is_empty());
    }

    #[tokio::test]
    async fn expired_timer_cleanup_recovers_capacity() {
        let mut timers = TimerResources::with_capacity(1);
        let permit = timers.try_reserve().unwrap();
        let handle = tokio::spawn(async {});
        timers.add(handle, std::time::Instant::now(), permit);
        tokio::task::yield_now().await;

        assert!(timers.try_reserve().is_ok());
        assert!(timers.is_empty());
    }

    #[tokio::test]
    async fn expired_but_unfinished_timer_keeps_its_admission() {
        let mut timers = TimerResources::with_capacity(1);
        let permit = timers.try_reserve().unwrap();
        let handle = tokio::spawn(pending());
        timers.add(handle, std::time::Instant::now(), permit);

        for _ in 0..1_000 {
            assert!(timers.try_reserve().is_err());
        }
        assert_eq!(timers.len(), 1);
    }
}
