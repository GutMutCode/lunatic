use std::future::Future;

use anyhow::Result;
use lunatic_common_api::{
    emit_audit_event, get_memory, AuditAction, AuditEvent, AuditEventV1, AuditReason, AuditResult,
    AuditSubject, AuditTarget, AuditTargetKind, IntoTrap, LinkerAsyncExt, SensitiveData,
};
use lunatic_process::state::{
    ensure_registry_insert_capacity, ProcessState, MAX_REGISTRY_NAME_BYTES,
};
use lunatic_process_api::ProcessCtx;
use wasmtime::{Caller, Linker};

struct PendingRegistryAudit {
    action: AuditAction,
    subject: AuditSubject,
    target: AuditTarget,
    fallback_result: AuditResult,
    fallback_reason: AuditReason,
    emitted: bool,
}

impl PendingRegistryAudit {
    fn new<T: ProcessState>(state: &T, action: AuditAction, target: AuditTarget) -> Self {
        let mut subject = AuditSubject::new().with_process_id(state.id());
        if let Some(node_id) = state.audit_node_id() {
            subject = subject.with_node_id(node_id);
        }
        if let Some(environment_id) = state.audit_environment_id() {
            subject = subject.with_environment_id(environment_id);
        }
        Self {
            action,
            subject,
            target,
            fallback_result: AuditResult::Failed,
            fallback_reason: AuditReason::InvalidInput,
            emitted: false,
        }
    }

    fn set_fallback(&mut self, result: AuditResult, reason: AuditReason) {
        self.fallback_result = result;
        self.fallback_reason = reason;
    }

    fn finish(mut self, result: AuditResult, reason: AuditReason) {
        self.emit(result, reason, None);
    }

    fn finish_with_target(mut self, result: AuditResult, reason: AuditReason, target: AuditTarget) {
        self.emit(result, reason, Some(target));
    }

    fn emit(&mut self, result: AuditResult, reason: AuditReason, target: Option<AuditTarget>) {
        emit_audit_event(AuditEventV1::new(
            AuditEvent::DistributedRegistryChange,
            self.action,
            result,
            reason,
            self.subject,
            target.unwrap_or(self.target),
        ));
        self.emitted = true;
    }
}

impl Drop for PendingRegistryAudit {
    fn drop(&mut self) {
        if !self.emitted {
            self.emit(self.fallback_result, self.fallback_reason, None);
        }
    }
}

fn registry_target(node_id: Option<u64>, process_id: Option<u64>) -> AuditTarget {
    let mut target = AuditTarget::new(AuditTargetKind::DistributedRegistry)
        .with_sensitive_data(SensitiveData::Redacted);
    if let Some(node_id) = node_id {
        target = target.with_node_id(node_id);
    }
    if let Some(process_id) = process_id {
        target = target.with_process_id(process_id);
    }
    target
}

// Register the registry APIs to the linker
pub fn register<T: ProcessState + ProcessCtx<T> + Send + Sync + 'static>(
    linker: &mut Linker<T>,
) -> Result<()> {
    linker.func_wrap4_async("lunatic::registry", "put", put)?;
    linker.func_wrap4_async("lunatic::registry", "get", get)?;
    linker.func_wrap2_async("lunatic::registry", "remove", remove)?;

    #[cfg(feature = "metrics")]
    metrics::describe_counter!(
        "lunatic.registry.write",
        metrics::Unit::Count,
        "number of new entries written to the registry"
    );
    #[cfg(feature = "metrics")]
    metrics::describe_counter!(
        "lunatic.timers.read",
        metrics::Unit::Count,
        "number of entries read from the registry"
    );
    #[cfg(feature = "metrics")]
    metrics::describe_counter!(
        "lunatic.timers.deletion",
        metrics::Unit::Count,
        "number of entries deleted from the registry"
    );
    #[cfg(feature = "metrics")]
    metrics::describe_gauge!(
        "lunatic.timers.registered",
        metrics::Unit::Count,
        "number of processes currently registered"
    );

    Ok(())
}

// Registers process with ID under `name`.
//
// Traps:
// * If the process ID doesn't exist.
// * If any memory outside the guest heap space is referenced.
fn put<T: ProcessState + ProcessCtx<T> + Send + Sync>(
    mut caller: Caller<T>,
    name_str_ptr: u32,
    name_str_len: u32,
    node_id: u64,
    process_id: u64,
) -> Box<dyn Future<Output = Result<()>> + Send + '_> {
    Box::new(async move {
        let mut audit = PendingRegistryAudit::new(
            caller.data(),
            AuditAction::Register,
            registry_target(Some(node_id), Some(process_id)),
        );
        let memory = get_memory(&mut caller)?;
        let (memory_slice, state) = memory.data_and_store_mut(&mut caller);
        let name = memory_slice
            .get(name_str_ptr as usize..(name_str_ptr + name_str_len) as usize)
            .or_trap("lunatic::registry::put")?;
        let name = std::str::from_utf8(name).or_trap("lunatic::registry::put")?;

        audit.set_fallback(AuditResult::Cancelled, AuditReason::Cancelled);
        let mut registry = state.registry().write().await;
        audit.set_fallback(AuditResult::Failed, AuditReason::InternalError);
        if let Err(error) = ensure_registry_insert_capacity(&registry, name) {
            let reason =
                if name.len() > MAX_REGISTRY_NAME_BYTES || name.chars().any(char::is_control) {
                    AuditReason::InvalidInput
                } else {
                    AuditReason::ResourceLimit
                };
            let result = if reason == AuditReason::ResourceLimit {
                AuditResult::Denied
            } else {
                AuditResult::Failed
            };
            audit.finish(result, reason);
            return Err(error);
        }
        registry.insert(name.to_owned(), (node_id, process_id));
        audit.finish(AuditResult::Succeeded, AuditReason::Completed);

        #[cfg(feature = "metrics")]
        metrics::increment_counter!("lunatic.registry.write");

        #[cfg(feature = "metrics")]
        metrics::increment_gauge!("lunatic.registry.registered", 1.0);

        Ok(())
    })
}

// Looks up process under `name` and returns 0 if it was found or 1 if not found.
//
// Traps:
// * If any memory outside the guest heap space is referenced.
fn get<T: ProcessState + ProcessCtx<T> + Send + Sync>(
    mut caller: Caller<T>,
    name_str_ptr: u32,
    name_str_len: u32,
    node_id_ptr: u32,
    process_id_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        let memory = get_memory(&mut caller)?;
        let (memory_slice, state) = memory.data_and_store_mut(&mut caller);
        let name = memory_slice
            .get(name_str_ptr as usize..(name_str_ptr + name_str_len) as usize)
            .or_trap("lunatic::registry::get")?;
        let name = std::str::from_utf8(name).or_trap("lunatic::registry::get")?;

        #[cfg(feature = "metrics")]
        metrics::increment_counter!("lunatic.registry.read");

        let (node_id, process_id) = if let Some(process) = state.registry().read().await.get(name) {
            *process
        } else {
            return Ok(1);
        };

        memory
            .write(&mut caller, node_id_ptr as usize, &node_id.to_le_bytes())
            .or_trap("lunatic::registry::get")?;

        memory
            .write(
                &mut caller,
                process_id_ptr as usize,
                &process_id.to_le_bytes(),
            )
            .or_trap("lunatic::registry::get")?;
        Ok(0)
    })
}

// Removes process under `name` if it exists.
//
// Traps:
// * If any memory outside the guest heap space is referenced.
fn remove<T: ProcessState + ProcessCtx<T> + Send + Sync>(
    mut caller: Caller<T>,
    name_str_ptr: u32,
    name_str_len: u32,
) -> Box<dyn Future<Output = Result<()>> + Send + '_> {
    Box::new(async move {
        let mut audit = PendingRegistryAudit::new(
            caller.data(),
            AuditAction::Unregister,
            registry_target(None, None),
        );
        let memory = get_memory(&mut caller)?;
        let (memory_slice, state) = memory.data_and_store_mut(&mut caller);
        let name = memory_slice
            .get(name_str_ptr as usize..(name_str_ptr + name_str_len) as usize)
            .or_trap("lunatic::registry::get")?;
        let name = std::str::from_utf8(name).or_trap("lunatic::registry::get")?;

        audit.set_fallback(AuditResult::Cancelled, AuditReason::Cancelled);
        let removed = state.registry().write().await.remove(name);
        match removed {
            Some((node_id, process_id)) => audit.finish_with_target(
                AuditResult::Succeeded,
                AuditReason::Completed,
                registry_target(Some(node_id), Some(process_id)),
            ),
            None => audit.finish(AuditResult::Failed, AuditReason::NotFound),
        }

        #[cfg(feature = "metrics")]
        metrics::increment_counter!("lunatic.registry.deletion");

        #[cfg(feature = "metrics")]
        if removed.is_some() {
            metrics::decrement_gauge!("lunatic.registry.registered", 1.0);
        }

        Ok(())
    })
}
