use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use std::vec::IntoIter;

use anyhow::Result;
use tokio::time::timeout;
use wasmtime::{Caller, Linker, ToWasmtimeResult as _};

use lunatic_common_api::{
    get_memory, AuditAction, AuditEvent, AuditReason, AuditResult, AuditTargetKind, IntoTrap,
    LinkerAsyncExt,
};
use lunatic_error_api::ErrorCtx;

use crate::{
    redacted_network_target, validate_memory_range, DnsIteratorLease, NetworkingCtx,
    PendingNetworkAudit,
};

pub struct DnsIterator {
    iter: IntoIter<SocketAddr>,
    // Runtime-created iterators keep their quota reservation for as long as
    // the table entry exists. Legacy callers can still construct an unleased
    // iterator with `new`, but host functions only use `with_lease`.
    _lease: Option<DnsIteratorLease>,
}

impl DnsIterator {
    /// Creates an iterator without an embedded quota lease.
    ///
    /// This preserves the original constructor for non-runtime uses. Values
    /// inserted into a guest-visible [`crate::DnsResources`] table should use
    /// [`Self::with_lease`] instead.
    pub fn new(iter: IntoIter<SocketAddr>) -> Self {
        Self { iter, _lease: None }
    }

    /// Creates an iterator that owns one previously reserved quota lease.
    pub fn with_lease(iter: IntoIter<SocketAddr>, lease: DnsIteratorLease) -> Self {
        Self {
            iter,
            _lease: Some(lease),
        }
    }
}

impl Iterator for DnsIterator {
    type Item = SocketAddr;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

// Register DNS networking APIs to the linker
pub fn register<T: NetworkingCtx + ErrorCtx + Send + 'static>(
    linker: &mut Linker<T>,
) -> Result<()> {
    linker.func_wrap4_async("lunatic::networking", "resolve", resolve)?;
    linker.func_wrap(
        "lunatic::networking",
        "drop_dns_iterator",
        |caller: Caller<'_, T>, dns_iter_id: u64| {
            drop_dns_iterator(caller, dns_iter_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::networking",
        "resolve_next",
        |caller: Caller<'_, T>,
         dns_iter_id: u64,
         addr_type_u32_ptr: u32,
         addr_u8_ptr: u32,
         port_u16_ptr: u32,
         flow_info_u32_ptr: u32,
         scope_id_u32_ptr: u32| {
            resolve_next(
                caller,
                dns_iter_id,
                addr_type_u32_ptr,
                addr_u8_ptr,
                port_u16_ptr,
                flow_info_u32_ptr,
                scope_id_u32_ptr,
            )
            .to_wasmtime_result()
        },
    )?;
    Ok(())
}

// Performs a DNS resolution. The returned iterator may not actually yield any values
// depending on the outcome of any resolution performed.
//
// If timeout is specified (value different from `u64::MAX`), the function will return on timeout
// expiration with value 9027.
//
// Returns:
// * 0 on success - The ID of the newly created DNS iterator is written to **id_u64_ptr**
// * 1 on error   - The error ID is written to **id_u64_ptr**
// * 9027 if the operation timed out
//
// Traps:
// * If the name is not a valid utf8 string.
// * If any memory outside the guest heap space is referenced.
fn resolve<T: NetworkingCtx + ErrorCtx + Send>(
    mut caller: Caller<T>,
    name_str_ptr: u32,
    name_str_len: u32,
    timeout_duration: u64,
    id_u64_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        let mut audit = PendingNetworkAudit::new(
            caller.data(),
            AuditEvent::NetworkConnect,
            AuditAction::Resolve,
            redacted_network_target(AuditTargetKind::DnsIterator, None, None),
        );
        let memory = get_memory(&mut caller)?;
        validate_memory_range(
            &caller,
            &memory,
            id_u64_ptr,
            std::mem::size_of::<u64>(),
            "lunatic::networking::resolve",
        )?;
        let (memory_slice, state) = memory.data_and_store_mut(&mut caller);

        let buffer = memory_slice
            .get(name_str_ptr as usize..(name_str_ptr + name_str_len) as usize)
            .or_trap("lunatic::network::resolve")?;
        let name = std::str::from_utf8(buffer)
            .or_trap("lunatic::network::resolve::not_valid_utf8_string")?;

        let lease = state.reserve_dns_iterator_lease();
        let (iter_or_error_id, result, audit_result, audit_reason) = match lease {
            Ok(lease) => {
                audit.set_fallback_reason(AuditReason::RuntimeFailure);
                // Check for timeout during lookup.
                let lookup_host = tokio::net::lookup_host(name);
                audit.mark_async();
                match match timeout_duration {
                    // Without timeout
                    u64::MAX => Ok(lookup_host.await),
                    // With timeout
                    t => timeout(Duration::from_millis(t), lookup_host).await,
                } {
                    Ok(Ok(sockets)) => {
                        // This is a bug in clippy, this collect is not needless.
                        #[allow(clippy::needless_collect)]
                        let iterator = DnsIterator::with_lease(
                            sockets.collect::<Vec<SocketAddr>>().into_iter(),
                            lease,
                        );
                        let id = state.dns_resources_mut().add(iterator);
                        audit.set_target(redacted_network_target(
                            AuditTargetKind::DnsIterator,
                            Some(id),
                            None,
                        ));
                        (id, 0, AuditResult::Succeeded, AuditReason::Completed)
                    }
                    Ok(Err(error)) => (
                        state.add_error_resource(error.into()),
                        1,
                        AuditResult::Failed,
                        AuditReason::RuntimeFailure,
                    ),
                    Err(_) => (0, 9027, AuditResult::Failed, AuditReason::TimedOut),
                }
            }
            Err(error) => (
                state.add_error_resource(error),
                1,
                AuditResult::Denied,
                AuditReason::ResourceLimit,
            ),
        };
        let memory = get_memory(&mut caller)?;
        memory
            .write(
                &mut caller,
                id_u64_ptr as usize,
                &iter_or_error_id.to_le_bytes(),
            )
            .or_trap("lunatic::networking::resolve")?;
        audit.finish(audit_result, audit_reason);
        Ok(result)
    })
}

// Drops the DNS iterator resource..
//
// Traps:
// * If the DNS iterator ID doesn't exist.
fn drop_dns_iterator<T: NetworkingCtx>(mut caller: Caller<T>, dns_iter_id: u64) -> Result<()> {
    let iterator = caller
        .data_mut()
        .dns_resources_mut()
        .remove(dns_iter_id)
        .or_trap("lunatic::networking::drop_dns_iterator")?;
    // Dropping the table value drops its embedded lease and releases exactly
    // one quota unit. Invalid IDs never reach this path.
    drop(iterator);
    Ok(())
}

// Takes the next socket address from DNS iterator and writes it to the passed in pointers.
//
// Addresses type is going to be a value of `4` or `6`, representing v4 or v6 addresses. The
// caller needs to reserve enough space at `addr_u8_ptr` for both values to fit in (16 bytes).
// `flow_info_u32_ptr` & `scope_id_u32_ptr` are only going to be used with version v6.
//
// Returns:
// * 0 on success
// * 1 on error   - There are no more addresses in this iterator
//
// Traps:
// * If the DNS iterator ID doesn't exist.
// * If any memory outside the guest heap space is referenced.
fn resolve_next<T: NetworkingCtx>(
    mut caller: Caller<T>,
    dns_iter_id: u64,
    addr_type_u32_ptr: u32,
    addr_u8_ptr: u32,
    port_u16_ptr: u32,
    flow_info_u32_ptr: u32,
    scope_id_u32_ptr: u32,
) -> Result<u32> {
    let memory = get_memory(&mut caller)?;
    let dns_iter = caller
        .data_mut()
        .dns_resources_mut()
        .get_mut(dns_iter_id)
        .or_trap("lunatic::networking::resolve_next")?;

    match dns_iter.next() {
        Some(socket_addr) => {
            match socket_addr {
                SocketAddr::V4(v4) => {
                    memory
                        .write(&mut caller, addr_type_u32_ptr as usize, &4u32.to_le_bytes())
                        .or_trap("lunatic::networking::resolve_next")?;
                    memory
                        .write(&mut caller, addr_u8_ptr as usize, &v4.ip().octets())
                        .or_trap("lunatic::networking::resolve_next")?;
                    memory
                        .write(&mut caller, port_u16_ptr as usize, &v4.port().to_le_bytes())
                        .or_trap("lunatic::networking::resolve_next")?;
                }
                SocketAddr::V6(v6) => {
                    memory
                        .write(&mut caller, addr_type_u32_ptr as usize, &6u32.to_le_bytes())
                        .or_trap("lunatic::networking::resolve_next")?;
                    memory
                        .write(&mut caller, addr_u8_ptr as usize, &v6.ip().octets())
                        .or_trap("lunatic::networking::resolve_next")?;
                    memory
                        .write(&mut caller, port_u16_ptr as usize, &v6.port().to_le_bytes())
                        .or_trap("lunatic::networking::resolve_next")?;
                    memory
                        .write(
                            &mut caller,
                            flow_info_u32_ptr as usize,
                            &v6.flowinfo().to_le_bytes(),
                        )
                        .or_trap("lunatic::networking::resolve_next")?;
                    memory
                        .write(
                            &mut caller,
                            scope_id_u32_ptr as usize,
                            &v6.scope_id().to_le_bytes(),
                        )
                        .or_trap("lunatic::networking::resolve_next")?;
                }
            }
            Ok(0)
        }
        None => Ok(1),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use anyhow::anyhow;

    use crate::{DnsIteratorLease, DnsIteratorQuota, DnsResources};

    use super::DnsIterator;

    #[derive(Debug)]
    struct TestQuota {
        max: usize,
        current: AtomicUsize,
        reserve_attempts: AtomicUsize,
        releases: AtomicUsize,
    }

    impl TestQuota {
        fn new(max: usize) -> Arc<Self> {
            Arc::new(Self {
                max,
                current: AtomicUsize::new(0),
                reserve_attempts: AtomicUsize::new(0),
                releases: AtomicUsize::new(0),
            })
        }

        fn current(&self) -> usize {
            self.current.load(Ordering::SeqCst)
        }
    }

    impl DnsIteratorQuota for TestQuota {
        fn reserve(&self) -> anyhow::Result<()> {
            self.reserve_attempts.fetch_add(1, Ordering::SeqCst);
            self.current
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    (current < self.max).then_some(current + 1)
                })
                .map(|_| ())
                .map_err(|_| anyhow!("DNS iterator quota reached"))
        }

        fn release(&self) -> anyhow::Result<()> {
            self.current
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current.checked_sub(1)
                })
                .map(|_| {
                    self.releases.fetch_add(1, Ordering::SeqCst);
                })
                .map_err(|_| anyhow!("DNS iterator quota underflow"))
        }
    }

    fn reserve(quota: &Arc<TestQuota>) -> anyhow::Result<DnsIteratorLease> {
        let owner: Arc<dyn DnsIteratorQuota> = quota.clone();
        DnsIteratorLease::reserve_new(owner)
    }

    #[test]
    fn cloned_quota_owners_share_one_finite_ceiling() {
        let quota = TestQuota::new(1);
        let owner: Arc<dyn DnsIteratorQuota> = quota.clone();
        let cloned_owner = owner.clone();

        let lease = DnsIteratorLease::reserve_new(owner).unwrap();
        for _ in 0..64 {
            assert!(DnsIteratorLease::reserve_new(cloned_owner.clone()).is_err());
        }
        assert_eq!(quota.current(), 1);
        assert_eq!(quota.reserve_attempts.load(Ordering::SeqCst), 65);
        assert_eq!(quota.releases.load(Ordering::SeqCst), 0);

        drop(lease);
        assert_eq!(quota.current(), 0);
        assert_eq!(quota.releases.load(Ordering::SeqCst), 1);

        drop(DnsIteratorLease::reserve_new(cloned_owner).unwrap());
        assert_eq!(quota.current(), 0);
        assert_eq!(quota.releases.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn table_removal_releases_each_iterator_exactly_once() {
        let quota = TestQuota::new(1);
        let mut resources = DnsResources::default();

        for _ in 0..64 {
            let iterator =
                DnsIterator::with_lease(Vec::new().into_iter(), reserve(&quota).unwrap());
            let id = resources.add(iterator);
            assert_eq!(quota.current(), 1);

            drop(resources.remove(id).unwrap());
            assert_eq!(quota.current(), 0);
        }

        assert_eq!(quota.releases.load(Ordering::SeqCst), 64);
    }

    #[test]
    fn dropping_the_whole_table_releases_all_iterators() {
        let quota = TestQuota::new(4);
        let mut resources = DnsResources::default();

        for _ in 0..4 {
            let iterator =
                DnsIterator::with_lease(Vec::new().into_iter(), reserve(&quota).unwrap());
            resources.add(iterator);
        }
        assert_eq!(quota.current(), 4);

        drop(resources);
        assert_eq!(quota.current(), 0);
        assert_eq!(quota.releases.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn repeated_pre_reservation_failures_roll_back_without_leaking() {
        let quota = TestQuota::new(1);

        for _ in 0..64 {
            // Simulate an I/O failure or cancellation after admission but
            // before the iterator is inserted into the table.
            drop(reserve(&quota).unwrap());
            assert_eq!(quota.current(), 0);
        }

        assert_eq!(quota.reserve_attempts.load(Ordering::SeqCst), 64);
        assert_eq!(quota.releases.load(Ordering::SeqCst), 64);
    }
}
