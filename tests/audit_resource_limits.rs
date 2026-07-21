use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use lunatic_common_api::{
    install_global_audit_dispatcher, AuditConfig, AuditDispatcher, AuditEventV1, AuditFlushOutcome,
    AuditSink,
};
use lunatic_process::{
    config::ProcessConfig,
    env::LunaticEnvironment,
    runtimes::wasmtime::{default_config, WasmtimeRuntime},
    state::ProcessState,
};
use lunatic_runtime::{state::DefaultProcessState, DefaultProcessConfig};
use tokio::sync::RwLock;
use wasmtime::ResourceLimiter;

struct RecordingSink(Arc<Mutex<Vec<AuditEventV1>>>);

impl AuditSink for RecordingSink {
    fn write(&mut self, event: &AuditEventV1) -> io::Result<()> {
        self.0.lock().unwrap().push(event.clone());
        Ok(())
    }
}

#[tokio::test]
async fn memory_and_table_limit_denials_have_typed_identity() -> anyhow::Result<()> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let dispatcher =
        AuditDispatcher::new(AuditConfig::default(), RecordingSink(Arc::clone(&events)));
    install_global_audit_dispatcher(dispatcher)
        .map_err(|_| anyhow::anyhow!("audit dispatcher was already initialized"))?;

    let mut config = DefaultProcessConfig::default();
    config.set_max_memory(64 * 1024);
    config.set_max_table_elements(10);
    let runtime = WasmtimeRuntime::new(&default_config())?;
    let module = Arc::new(runtime.compile_module(wat::parse_str("(module)")?.into())?);
    let environment = Arc::new(LunaticEnvironment::new(73));
    let mut state = DefaultProcessState::new(
        environment,
        None,
        runtime,
        module,
        Arc::new(config),
        Arc::new(RwLock::new(HashMap::new())),
    )?;

    assert!(!state.memory_growing(0, 64 * 1024 + 1, None)?);
    assert!(!state.table_growing(0, 11, None)?);
    assert_eq!(
        lunatic_common_api::flush_audit(Duration::from_secs(2)),
        AuditFlushOutcome::Flushed
    );

    let events = events.lock().unwrap();
    assert_eq!(events.len(), 2);
    for event in events.iter() {
        assert_eq!(
            event.event(),
            lunatic_common_api::AuditEvent::ResourceLimitDenied
        );
        assert_eq!(event.result(), lunatic_common_api::AuditResult::Denied);
        assert_eq!(
            event.reason(),
            lunatic_common_api::AuditReason::ResourceLimit
        );
        assert_eq!(event.subject().environment_id(), Some(73));
        assert_eq!(event.subject().process_id(), Some(state.id()));
    }
    assert_eq!(
        events[0].target().kind(),
        lunatic_common_api::AuditTargetKind::Memory
    );
    assert_eq!(
        events[1].target().kind(),
        lunatic_common_api::AuditTargetKind::Table
    );
    Ok(())
}
