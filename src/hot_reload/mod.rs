pub mod watcher;

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use lunatic_common_api::{
    emit_audit_event, AuditAction, AuditEvent, AuditEventV1, AuditReason, AuditResult,
    AuditSubject, AuditTarget, AuditTargetKind, SensitiveData,
};
use lunatic_process::{
    env::Environment,
    hot_reload::{ReloadCoordinator, ReloadStatus},
    module_registry::ModuleRegistry,
    runtimes::wasmtime::WasmtimeRuntime,
    state::ProcessState,
};

pub use watcher::{FileChangeEvent, FileWatcher};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuditStage {
    Preparing,
    Blocked,
    Applying,
    Compensating,
}

fn audit_outcome(
    succeeded: bool,
    stage: AuditStage,
    status: Option<&ReloadStatus>,
) -> (AuditAction, AuditResult, AuditReason) {
    if succeeded {
        return (
            AuditAction::Commit,
            AuditResult::Succeeded,
            AuditReason::Completed,
        );
    }
    match stage {
        AuditStage::Blocked => (
            AuditAction::Commit,
            AuditResult::Denied,
            AuditReason::Conflict,
        ),
        AuditStage::Preparing => (
            AuditAction::Commit,
            AuditResult::Failed,
            AuditReason::RuntimeFailure,
        ),
        AuditStage::Applying | AuditStage::Compensating => match status {
            Some(ReloadStatus::RolledBack { .. }) => (
                AuditAction::Rollback,
                AuditResult::Failed,
                AuditReason::ReloadNack,
            ),
            Some(ReloadStatus::InDoubt { .. }) => (
                AuditAction::InDoubt,
                AuditResult::Failed,
                AuditReason::RollbackFailed,
            ),
            Some(ReloadStatus::Applying | ReloadStatus::RollingBack { .. }) => (
                AuditAction::InDoubt,
                AuditResult::Failed,
                AuditReason::RuntimeFailure,
            ),
            Some(ReloadStatus::Committed { .. }) if stage == AuditStage::Compensating => (
                AuditAction::Rollback,
                AuditResult::Failed,
                AuditReason::RuntimeFailure,
            ),
            Some(ReloadStatus::Committed { .. }) | None => (
                AuditAction::Commit,
                AuditResult::Failed,
                AuditReason::RuntimeFailure,
            ),
        },
    }
}

struct PendingHotReloadAudit {
    environment_id: u64,
    module_id: u64,
    stage: AuditStage,
    emitted: bool,
}

impl PendingHotReloadAudit {
    fn new(environment_id: u64, module_id: u64) -> Self {
        Self {
            environment_id,
            module_id,
            stage: AuditStage::Preparing,
            emitted: false,
        }
    }

    fn finish(mut self, succeeded: bool, status: Option<&ReloadStatus>) {
        let (action, result, reason) = audit_outcome(succeeded, self.stage, status);
        self.emit(action, result, reason);
    }

    fn emit(&mut self, action: AuditAction, result: AuditResult, reason: AuditReason) {
        emit_audit_event(AuditEventV1::new(
            AuditEvent::HotReloadUpdate,
            action,
            result,
            reason,
            AuditSubject::new().with_environment_id(self.environment_id),
            AuditTarget::new(AuditTargetKind::HotReload)
                .with_resource_id(self.module_id)
                .with_sensitive_data(SensitiveData::Redacted),
        ));
        self.emitted = true;
    }
}

impl Drop for PendingHotReloadAudit {
    fn drop(&mut self) {
        if self.emitted {
            return;
        }
        let action = match self.stage {
            AuditStage::Preparing | AuditStage::Blocked => AuditAction::Commit,
            AuditStage::Applying | AuditStage::Compensating => AuditAction::InDoubt,
        };
        self.emit(action, AuditResult::Cancelled, AuditReason::Cancelled);
    }
}

/// Compile, register and atomically apply a module update through the same boundary
/// used by `lunatic run --watch`.
pub async fn register_module_update<S>(
    runtime: &WasmtimeRuntime,
    registry: &ModuleRegistry<S>,
    env: &Arc<dyn Environment>,
    coordinator: &ReloadCoordinator<S>,
    module_id: u64,
    bytes: Vec<u8>,
) -> Result<u32>
where
    S: ProcessState + Send + Sync + 'static,
{
    let mut audit = PendingHotReloadAudit::new(env.id(), module_id);
    let result = register_module_update_inner(
        runtime,
        registry,
        env,
        coordinator,
        module_id,
        bytes,
        &mut audit.stage,
    )
    .await;
    let status = if result.is_err() {
        coordinator.status(module_id).await
    } else {
        None
    };
    audit.finish(result.is_ok(), status.as_ref());

    result
}

async fn register_module_update_inner<S>(
    runtime: &WasmtimeRuntime,
    registry: &ModuleRegistry<S>,
    env: &Arc<dyn Environment>,
    coordinator: &ReloadCoordinator<S>,
    module_id: u64,
    bytes: Vec<u8>,
    audit_stage: &mut AuditStage,
) -> Result<u32>
where
    S: ProcessState + Send + Sync + 'static,
{
    let _transaction = registry.lock_transaction(module_id).await;
    if coordinator.is_reload_in_progress(module_id).await {
        *audit_stage = AuditStage::Blocked;
        return Err(anyhow!(
            "Module {} cannot accept another reload while its previous operation is active or in doubt",
            module_id
        ));
    }
    let old_version = registry
        .get_committed_version_number(module_id)
        .ok_or_else(|| anyhow!("Module {} has no committed version", module_id))?;
    let module = runtime.compile_module(bytes.into())?;
    let version = registry.add_version(module_id, module)?;
    let affected_processes = registry.processes_for_module(module_id, env.id());

    *audit_stage = AuditStage::Applying;
    coordinator
        .perform_atomic_reload(
            env.as_ref(),
            module_id,
            old_version,
            version,
            affected_processes.clone(),
        )
        .await
        .with_context(|| {
            format!(
                "Module {} reload {} -> {} did not commit",
                module_id, old_version, version
            )
        })?;

    if let Err(commit_error) = registry.mark_committed(module_id, version) {
        // A registry commit is synchronous and should only fail if lifecycle
        // invariants were violated. Compensate anyway so processes and the
        // committed pointer cannot silently diverge.
        *audit_stage = AuditStage::Compensating;
        let rollback_result = coordinator
            .perform_atomic_reload(
                env.as_ref(),
                module_id,
                version,
                old_version,
                affected_processes,
            )
            .await;
        return match rollback_result {
            Ok(()) => Err(commit_error).context(format!(
                "Registry commit failed; all processes were restored to version {}",
                old_version
            )),
            Err(rollback_error) => Err(anyhow!(
                "Registry commit failed ({commit_error}); compensating rollback also failed ({rollback_error})"
            )),
        };
    }

    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_classification_distinguishes_preparation_conflicts_and_rollbacks() {
        assert_eq!(
            audit_outcome(false, AuditStage::Blocked, None),
            (
                AuditAction::Commit,
                AuditResult::Denied,
                AuditReason::Conflict
            )
        );
        assert_eq!(
            audit_outcome(
                false,
                AuditStage::Preparing,
                Some(&ReloadStatus::RolledBack {
                    apply_errors: Vec::new(),
                    acknowledgements: Vec::new(),
                })
            ),
            (
                AuditAction::Commit,
                AuditResult::Failed,
                AuditReason::RuntimeFailure
            )
        );
        assert_eq!(
            audit_outcome(
                false,
                AuditStage::Applying,
                Some(&ReloadStatus::RolledBack {
                    apply_errors: Vec::new(),
                    acknowledgements: Vec::new(),
                })
            ),
            (
                AuditAction::Rollback,
                AuditResult::Failed,
                AuditReason::ReloadNack
            )
        );
        assert_eq!(
            audit_outcome(
                false,
                AuditStage::Compensating,
                Some(&ReloadStatus::Committed {
                    acknowledgements: Vec::new(),
                })
            ),
            (
                AuditAction::Rollback,
                AuditResult::Failed,
                AuditReason::RuntimeFailure
            )
        );
    }
}
