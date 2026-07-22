use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, Once,
    },
    time::Duration,
};

use anyhow::Result;
use lunatic_common_api::{flush_audit, AuditFlushOutcome};
use lunatic_error_api::ErrorCtx;
use lunatic_networking_api::NetworkingCtx;
use lunatic_process::{
    env::LunaticEnvironment,
    resource_migration::TlsCredentialHandle,
    runtimes::wasmtime::{
        default_config, WasmtimeCompiledModule, WasmtimeInstance, WasmtimeRuntime,
    },
    state::ProcessState,
};
use lunatic_process_api::ProcessConfigCtx;
use lunatic_runtime::{
    state::DefaultProcessState,
    tls_credentials::{
        EphemeralTlsCredentialProvider, TlsCredentialMaterial, TlsCredentialProvider,
        TlsCredentialProviderError, TlsCredentialScope,
    },
    DefaultProcessConfig,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::RwLock,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use wasmtime::Val;

const OUTCOME_OFFSET: usize = 16;
const STATUS_OFFSET: usize = 24;

const TLS_PROVIDER_GUEST: &str = r#"
(module
    (import "lunatic::networking" "tls_bind_with_credential"
        (func $tls_bind_with_credential
            (param i32 i32 i32 i32 i32 i32 i64 i64)
            (result i32)))
    (import "lunatic::networking" "tls_accept"
        (func $tls_accept (param i64 i32 i32) (result i32)))
    (import "lunatic::networking" "tls_read"
        (func $tls_read (param i64 i32 i32 i32) (result i32)))
    (import "lunatic::networking" "tls_write_vectored"
        (func $tls_write_vectored (param i64 i32 i32 i32) (result i32)))
    (import "lunatic::networking" "tls_flush"
        (func $tls_flush (param i64 i32) (result i32)))
    (import "lunatic::networking" "drop_dns_iterator"
        (func $drop_dns_iterator (param i64)))
    (import "lunatic::networking" "drop_tls_listener"
        (func $drop_tls_listener (param i64)))
    (import "lunatic::networking" "drop_tls_stream"
        (func $drop_tls_stream (param i64)))

    (memory (export "memory") 1)
    (data (i32.const 0) "\7f\00\00\01")
    (data (i32.const 96) "pong")

    (func (export "bind") (param $handle_low i64) (param $handle_high i64)
        (i32.store
            (i32.const 24)
            (call $tls_bind_with_credential
                (i32.const 4)
                (i32.const 0)
                (i32.const 0)
                (i32.const 0)
                (i32.const 0)
                (i32.const 16)
                (local.get $handle_low)
                (local.get $handle_high))))

    (func (export "accept_and_echo")
        (local $stream i64)
        (local $peer i64)
        (local $read i32)
        (local $written i32)
        (local $chunk i32)

        (if
            (call $tls_accept
                (i64.load (i32.const 16))
                (i32.const 32)
                (i32.const 40))
            (then unreachable))
        (local.set $stream (i64.load (i32.const 32)))
        (local.set $peer (i64.load (i32.const 40)))

        (block $read_done
            (loop $read_more
                (br_if $read_done (i32.eq (local.get $read) (i32.const 4)))
                (if
                    (call $tls_read
                        (local.get $stream)
                        (i32.add (i32.const 80) (local.get $read))
                        (i32.sub (i32.const 4) (local.get $read))
                        (i32.const 48))
                    (then unreachable))
                (local.set $chunk (i32.wrap_i64 (i64.load (i32.const 48))))
                (if (i32.eqz (local.get $chunk)) (then unreachable))
                (local.set $read (i32.add (local.get $read) (local.get $chunk)))
                (br $read_more)))

        (if
            (i32.ne (i32.load (i32.const 80)) (i32.const 0x676e6970))
            (then unreachable))

        (block $write_done
            (loop $write_more
                (br_if $write_done (i32.eq (local.get $written) (i32.const 4)))
                (i32.store
                    (i32.const 104)
                    (i32.add (i32.const 96) (local.get $written)))
                (i32.store
                    (i32.const 108)
                    (i32.sub (i32.const 4) (local.get $written)))
                (if
                    (call $tls_write_vectored
                        (local.get $stream)
                        (i32.const 104)
                        (i32.const 1)
                        (i32.const 56))
                    (then unreachable))
                (local.set $chunk (i32.wrap_i64 (i64.load (i32.const 56))))
                (if (i32.eqz (local.get $chunk)) (then unreachable))
                (local.set $written (i32.add (local.get $written) (local.get $chunk)))
                (br $write_more)))

        (if
            (call $tls_flush (local.get $stream) (i32.const 64))
            (then unreachable))
        (call $drop_dns_iterator (local.get $peer)))

    (func (export "cleanup")
        (call $drop_tls_stream (i64.load (i32.const 32)))
        (call $drop_tls_listener (i64.load (i32.const 16))))
)
"#;

#[derive(Clone)]
struct CapturedLog {
    target: String,
    message: String,
}

struct LogCapture;

static LOG_CAPTURE: LogCapture = LogCapture;
static LOG_RECORDS: Mutex<Vec<CapturedLog>> = Mutex::new(Vec::new());
static LOG_INIT: Once = Once::new();
static NEXT_ENVIRONMENT_ID: AtomicU64 = AtomicU64::new(20_000);

impl log::Log for LogCapture {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            LOG_RECORDS.lock().unwrap().push(CapturedLog {
                target: record.target().to_owned(),
                message: record.args().to_string(),
            });
        }
    }

    fn flush(&self) {}
}

fn initialize_log_capture() {
    LOG_INIT.call_once(|| {
        log::set_logger(&LOG_CAPTURE).expect("the integration test owns its logger");
        log::set_max_level(log::LevelFilter::Info);
    });
}

fn audit_events_for_environment(environment_id: u64) -> Vec<serde_json::Value> {
    assert_eq!(
        flush_audit(Duration::from_secs(2)),
        AuditFlushOutcome::Flushed,
        "audit events must reach the capture sink"
    );
    LOG_RECORDS
        .lock()
        .unwrap()
        .iter()
        .filter(|record| record.target == "audit")
        .filter_map(|record| serde_json::from_str::<serde_json::Value>(&record.message).ok())
        .filter(|event| event["subject"]["environment_id"] == environment_id)
        .collect()
}

fn assert_only_network_bind_event(
    events: &[serde_json::Value],
    expected_result: &str,
    expected_reason: &str,
    context: &str,
) {
    let bind_events = events
        .iter()
        .filter(|event| event["event"] == "network_bind")
        .collect::<Vec<_>>();
    assert_eq!(
        bind_events.len(),
        1,
        "{} must emit exactly one network_bind event: {:?}",
        context,
        events
    );
    let event = bind_events[0];
    assert_eq!(event["action"], "bind", "{} action", context);
    assert_eq!(event["result"], expected_result, "{} result", context);
    assert_eq!(event["reason"], expected_reason, "{} reason", context);
}

fn runtime_and_module() -> Result<(
    WasmtimeRuntime,
    Arc<WasmtimeCompiledModule<DefaultProcessState>>,
)> {
    let runtime = WasmtimeRuntime::new(&default_config())?;
    let module = Arc::new(runtime.compile_module(wat::parse_str(TLS_PROVIDER_GUEST)?.into())?);
    Ok((runtime, module))
}

fn process_config(credential_capability: bool) -> Arc<DefaultProcessConfig> {
    process_config_with_network_limit(credential_capability, 8)
}

fn process_config_with_network_limit(
    credential_capability: bool,
    max_network_connections: u32,
) -> Arc<DefaultProcessConfig> {
    let mut config = DefaultProcessConfig::default();
    config.set_max_file_descriptors(8);
    config.set_max_network_connections(max_network_connections);
    config.set_can_use_tls_credential_handles(credential_capability);
    Arc::new(config)
}

fn new_state(
    environment: Arc<LunaticEnvironment>,
    runtime: WasmtimeRuntime,
    module: Arc<WasmtimeCompiledModule<DefaultProcessState>>,
    config: Arc<DefaultProcessConfig>,
    provider: Arc<dyn TlsCredentialProvider>,
) -> Result<DefaultProcessState> {
    DefaultProcessState::new_with_tls_credential_provider(
        environment,
        None,
        runtime,
        module,
        config,
        Arc::new(RwLock::new(HashMap::new())),
        provider,
    )
}

fn tls_identity() -> Result<(TlsAcceptor, TlsConnector, Vec<u8>, Vec<u8>)> {
    use lunatic_distributed::{control::cert, distributed::server::gen_node_cert};
    use tokio_rustls::rustls::{
        pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
        ClientConfig, RootCertStore, ServerConfig,
    };

    let root = cert::test_root_cert()?;
    let server_cert = gen_node_cert("localhost")?;
    let server_cert_pem = server_cert.serialize_pem_with_signer(&root)?;
    let server_key_pem = server_cert.serialize_private_key_pem();
    let server_cert_der = CertificateDer::from_pem_slice(server_cert_pem.as_bytes())?;
    let server_key_der = PrivateKeyDer::from_pem_slice(server_key_pem.as_bytes())?;
    let private_key_der = server_key_der.secret_der().to_vec();
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![server_cert_der], server_key_der)?;

    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from_pem_slice(
        root.certificate_pem().as_bytes(),
    )?)?;
    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok((
        TlsAcceptor::from(Arc::new(server_config)),
        TlsConnector::from(Arc::new(client_config)),
        private_key_der,
        server_key_pem.into_bytes(),
    ))
}

fn abi_params(handle: TlsCredentialHandle) -> Vec<Val> {
    let (low, high) = handle.to_abi_parts();
    vec![Val::I64(low as i64), Val::I64(high as i64)]
}

fn bind_outcome(instance: &mut WasmtimeInstance<DefaultProcessState>) -> Result<(u32, u64)> {
    let memory = instance.snapshot_memory()?.memory;
    let status = u32::from_le_bytes(memory[STATUS_OFFSET..STATUS_OFFSET + 4].try_into()?);
    let output = u64::from_le_bytes(memory[OUTCOME_OFFSET..OUTCOME_OFFSET + 8].try_into()?);
    Ok((status, output))
}

fn error_text(instance: &WasmtimeInstance<DefaultProcessState>, error_id: u64) -> String {
    instance
        .state()
        .error_resources()
        .get(error_id)
        .expect("a denied bind must return a live error resource")
        .to_string()
}

fn assert_slice_absent(haystack: &[u8], marker: &[u8], context: &str) {
    assert!(!marker.is_empty());
    assert!(
        !haystack
            .windows(marker.len())
            .any(|window| window == marker),
        "{} retained the private-key marker",
        context
    );
}

fn pem_body_marker(pem: &[u8]) -> Vec<u8> {
    pem.split(|byte| *byte == b'\n')
        .find(|line| line.len() >= 32 && !line.starts_with(b"-----"))
        .expect("generated private key must contain a PEM body line")[..32]
        .to_vec()
}

fn assert_bind_error(
    instance: &mut WasmtimeInstance<DefaultProcessState>,
    expected: &str,
) -> Result<String> {
    let (status, error_id) = bind_outcome(instance)?;
    assert_eq!(status, 1, "a denied credential bind must return an error");
    let error = error_text(instance, error_id);
    assert_eq!(error, expected);
    Ok(error)
}

#[tokio::test]
async fn guest_provider_handle_bind_accepts_and_round_trips_without_key_bytes() -> Result<()> {
    initialize_log_capture();
    let provider = Arc::new(EphemeralTlsCredentialProvider::default());
    let (runtime, module) = runtime_and_module()?;
    let environment_id = NEXT_ENVIRONMENT_ID.fetch_add(1, Ordering::Relaxed);
    let environment = Arc::new(LunaticEnvironment::new(environment_id));
    let (acceptor, connector, private_key_der, private_key_pem) = tls_identity()?;
    assert!(private_key_der.len() >= 40);
    let private_key_marker = private_key_der[8..40].to_vec();
    let private_key_text_marker = pem_body_marker(&private_key_pem);

    let state = new_state(
        environment,
        runtime.clone(),
        module.clone(),
        process_config(true),
        provider,
    )?;
    let handle = state.provision_tls_listener_credential(TlsCredentialMaterial::new(acceptor))?;
    assert!(format!("{handle:?}").contains("[REDACTED]"));
    let mut guest = runtime.instantiate(&module, state).await?;

    guest.call_ref("bind", abi_params(handle)).await?;
    let (status, listener_id) = bind_outcome(&mut guest)?;
    assert_eq!(status, 0, "provider-backed guest bind must succeed");
    let local_addr = guest
        .state()
        .tls_listener_resources()
        .get(listener_id)
        .expect("returned listener ID must resolve")
        .listener
        .local_addr()?;

    let client = async move {
        use tokio_rustls::rustls::pki_types::ServerName;

        let socket = TcpStream::connect(local_addr).await?;
        let mut stream = connector
            .connect(ServerName::try_from("localhost".to_owned())?, socket)
            .await?;
        stream.write_all(b"ping").await?;
        let mut response = [0_u8; 4];
        stream.read_exact(&mut response).await?;
        anyhow::ensure!(&response == b"pong", "unexpected guest TLS response");
        stream.shutdown().await?;
        Result::<()>::Ok(())
    };
    let (guest_result, client_result) =
        tokio::join!(guest.call_ref("accept_and_echo", Vec::new()), client);
    guest_result?;
    client_result?;

    let wasm_memory = guest.snapshot_memory()?.memory;
    assert_slice_absent(&wasm_memory, &private_key_marker, "Wasm MemorySnapshot");
    assert_slice_absent(
        &wasm_memory,
        &private_key_text_marker,
        "Wasm MemorySnapshot",
    );
    assert_slice_absent(&wasm_memory, &private_key_pem, "Wasm MemorySnapshot");

    let resource_snapshot = guest
        .state()
        .capture_resource_snapshot()?
        .expect("a live TLS listener must produce a resource snapshot");
    let snapshot_bytes = resource_snapshot.to_bytes()?;
    assert_slice_absent(
        &snapshot_bytes,
        &private_key_marker,
        "ResourceMigrationSnapshot",
    );
    assert_slice_absent(
        &snapshot_bytes,
        &private_key_text_marker,
        "ResourceMigrationSnapshot",
    );
    assert_slice_absent(
        &snapshot_bytes,
        &private_key_pem,
        "ResourceMigrationSnapshot",
    );
    let snapshot_debug = format!("{resource_snapshot:?}");
    assert_slice_absent(
        snapshot_debug.as_bytes(),
        &private_key_text_marker,
        "ResourceMigrationSnapshot Debug",
    );
    assert_slice_absent(
        snapshot_debug.as_bytes(),
        &private_key_pem,
        "ResourceMigrationSnapshot Debug",
    );
    assert!(
        !snapshot_debug.contains(&format!("{private_key_marker:?}")),
        "ResourceMigrationSnapshot Debug retained the private-key marker"
    );

    guest.call_ref("cleanup", Vec::new()).await?;
    guest.call_ref("bind", abi_params(handle)).await?;
    let consumed_error = assert_bind_error(&mut guest, "TLS listener credential is unavailable")?;
    assert_slice_absent(
        consumed_error.as_bytes(),
        &private_key_marker,
        "consumed-handle error",
    );
    assert_slice_absent(
        consumed_error.as_bytes(),
        &private_key_text_marker,
        "consumed-handle error",
    );
    assert_slice_absent(
        consumed_error.as_bytes(),
        &private_key_pem,
        "consumed-handle error",
    );

    let events = audit_events_for_environment(environment_id);
    let bind_events = events
        .iter()
        .filter(|event| event["event"] == "network_bind")
        .collect::<Vec<_>>();
    assert_eq!(bind_events.len(), 2, "success plus consumed-handle bind");
    assert!(bind_events.iter().all(|event| event["action"] == "bind"));
    assert!(bind_events
        .iter()
        .any(|event| { event["result"] == "succeeded" && event["reason"] == "completed" }));
    assert!(bind_events
        .iter()
        .any(|event| { event["result"] == "denied" && event["reason"] == "policy_denied" }));
    let audit_json = serde_json::to_vec(&events)?;
    assert_slice_absent(&audit_json, &private_key_marker, "audit output");
    assert_slice_absent(&audit_json, &private_key_text_marker, "audit output");
    assert_slice_absent(&audit_json, &private_key_pem, "audit output");
    for record in LOG_RECORDS.lock().unwrap().iter() {
        assert_slice_absent(record.message.as_bytes(), &private_key_marker, "log output");
        assert_slice_absent(
            record.message.as_bytes(),
            &private_key_text_marker,
            "log output",
        );
        assert_slice_absent(record.message.as_bytes(), &private_key_pem, "log output");
    }

    Ok(())
}

struct FailingTlsCredentialProvider;

impl TlsCredentialProvider for FailingTlsCredentialProvider {
    fn provision(
        &self,
        _scope: TlsCredentialScope,
        _material: TlsCredentialMaterial,
    ) -> std::result::Result<TlsCredentialHandle, TlsCredentialProviderError> {
        Err(TlsCredentialProviderError::ProviderFailure)
    }

    fn take(
        &self,
        _scope: TlsCredentialScope,
        _handle: &TlsCredentialHandle,
    ) -> std::result::Result<TlsCredentialMaterial, TlsCredentialProviderError> {
        Err(TlsCredentialProviderError::ProviderFailure)
    }
}

#[tokio::test]
async fn guest_provider_handle_denials_are_scoped_stable_and_secret_free() -> Result<()> {
    initialize_log_capture();

    let provider = Arc::new(EphemeralTlsCredentialProvider::default());
    let (runtime, module) = runtime_and_module()?;
    let environment = Arc::new(LunaticEnvironment::new(
        NEXT_ENVIRONMENT_ID.fetch_add(1, Ordering::Relaxed),
    ));
    let (acceptor, _connector, private_key_der, private_key_pem) = tls_identity()?;
    assert!(private_key_der.len() >= 40);
    let private_key_marker = private_key_der[8..40].to_vec();
    let private_key_text_marker = pem_body_marker(&private_key_pem);
    let owner_state = new_state(
        environment.clone(),
        runtime.clone(),
        module.clone(),
        process_config(true),
        provider.clone(),
    )?;
    let owner_handle = owner_state
        .provision_tls_listener_credential(TlsCredentialMaterial::new(acceptor.clone()))?;

    let wrong_process_state = new_state(
        environment,
        runtime.clone(),
        module.clone(),
        process_config(true),
        provider.clone(),
    )?;
    let wrong_process_environment_id = NetworkingCtx::audit_environment_id(&wrong_process_state)
        .expect("default state has an environment ID");
    let mut wrong_process = runtime.instantiate(&module, wrong_process_state).await?;
    wrong_process
        .call_ref("bind", abi_params(owner_handle))
        .await?;
    let wrong_process_error =
        assert_bind_error(&mut wrong_process, "TLS listener credential is unavailable")?;

    let wrong_environment_id = NEXT_ENVIRONMENT_ID.fetch_add(1, Ordering::Relaxed);
    let wrong_environment_state = new_state(
        Arc::new(LunaticEnvironment::new(wrong_environment_id)),
        runtime.clone(),
        module.clone(),
        process_config(true),
        provider.clone(),
    )?;
    let mut wrong_environment = runtime
        .instantiate(&module, wrong_environment_state)
        .await?;
    wrong_environment
        .call_ref("bind", abi_params(owner_handle))
        .await?;
    let wrong_environment_error = assert_bind_error(
        &mut wrong_environment,
        "TLS listener credential is unavailable",
    )?;

    let missing_environment_id = NEXT_ENVIRONMENT_ID.fetch_add(1, Ordering::Relaxed);
    let missing_state = new_state(
        Arc::new(LunaticEnvironment::new(missing_environment_id)),
        runtime.clone(),
        module.clone(),
        process_config(true),
        provider,
    )?;
    let mut missing = runtime.instantiate(&module, missing_state).await?;
    let missing_handle = TlsCredentialHandle::from_bytes([0x5a; 16]);
    missing.call_ref("bind", abi_params(missing_handle)).await?;
    let missing_error = assert_bind_error(&mut missing, "TLS listener credential is unavailable")?;

    let expired_provider = Arc::new(EphemeralTlsCredentialProvider::new(Duration::ZERO));
    let expired_environment_id = NEXT_ENVIRONMENT_ID.fetch_add(1, Ordering::Relaxed);
    let expired_state = new_state(
        Arc::new(LunaticEnvironment::new(expired_environment_id)),
        runtime.clone(),
        module.clone(),
        process_config(true),
        expired_provider,
    )?;
    let expired_handle = expired_state
        .provision_tls_listener_credential(TlsCredentialMaterial::new(acceptor.clone()))?;
    let mut expired = runtime.instantiate(&module, expired_state).await?;
    expired.call_ref("bind", abi_params(expired_handle)).await?;
    let expired_error = assert_bind_error(&mut expired, "TLS listener credential has expired")?;

    let revoked_provider = Arc::new(EphemeralTlsCredentialProvider::default());
    let revoked_environment_id = NEXT_ENVIRONMENT_ID.fetch_add(1, Ordering::Relaxed);
    let revoked_state = new_state(
        Arc::new(LunaticEnvironment::new(revoked_environment_id)),
        runtime.clone(),
        module.clone(),
        process_config(true),
        revoked_provider,
    )?;
    let revoked_handle = revoked_state
        .provision_tls_listener_credential(TlsCredentialMaterial::new(acceptor.clone()))?;
    revoked_state.revoke_tls_listener_credential(&revoked_handle)?;
    let mut revoked = runtime.instantiate(&module, revoked_state).await?;
    revoked.call_ref("bind", abi_params(revoked_handle)).await?;
    let revoked_error = assert_bind_error(&mut revoked, "TLS listener credential is unavailable")?;

    let denied_provider = Arc::new(EphemeralTlsCredentialProvider::default());
    let denied_environment_id = NEXT_ENVIRONMENT_ID.fetch_add(1, Ordering::Relaxed);
    let denied_state = new_state(
        Arc::new(LunaticEnvironment::new(denied_environment_id)),
        runtime.clone(),
        module.clone(),
        process_config_with_network_limit(false, 0),
        denied_provider,
    )?;
    let denied_handle =
        denied_state.provision_tls_listener_credential(TlsCredentialMaterial::new(acceptor))?;
    let mut denied = runtime.instantiate(&module, denied_state).await?;
    denied.call_ref("bind", abi_params(denied_handle)).await?;
    let capability_error =
        assert_bind_error(&mut denied, "TLS credential handle capability is denied")?;
    denied
        .state()
        .revoke_tls_listener_credential(&denied_handle)?;

    let failing_environment_id = NEXT_ENVIRONMENT_ID.fetch_add(1, Ordering::Relaxed);
    let failing_state = new_state(
        Arc::new(LunaticEnvironment::new(failing_environment_id)),
        runtime.clone(),
        module.clone(),
        process_config(true),
        Arc::new(FailingTlsCredentialProvider),
    )?;
    let mut failing = runtime.instantiate(&module, failing_state).await?;
    let unknown_handle = TlsCredentialHandle::from_bytes([0xa5; 16]);
    failing.call_ref("bind", abi_params(unknown_handle)).await?;
    let provider_error = assert_bind_error(&mut failing, "TLS credential provider failed")?;

    for instance in [
        &mut wrong_process,
        &mut wrong_environment,
        &mut missing,
        &mut expired,
        &mut revoked,
        &mut denied,
        &mut failing,
    ] {
        let memory = instance.snapshot_memory()?.memory;
        assert_slice_absent(&memory, &private_key_marker, "denied guest memory");
        assert_slice_absent(&memory, &private_key_text_marker, "denied guest memory");
        assert_slice_absent(&memory, &private_key_pem, "denied guest memory");
    }

    let wrong_process_events = audit_events_for_environment(wrong_process_environment_id);
    let wrong_environment_events = audit_events_for_environment(wrong_environment_id);
    let missing_events = audit_events_for_environment(missing_environment_id);
    let expired_events = audit_events_for_environment(expired_environment_id);
    let revoked_events = audit_events_for_environment(revoked_environment_id);
    let capability_events = audit_events_for_environment(denied_environment_id);
    let provider_failure_events = audit_events_for_environment(failing_environment_id);

    for (events, context) in [
        (&wrong_process_events, "wrong-process"),
        (&wrong_environment_events, "wrong-environment"),
        (&missing_events, "missing"),
        (&expired_events, "expired"),
        (&revoked_events, "revoked"),
    ] {
        assert_only_network_bind_event(events, "denied", "policy_denied", context);
    }
    assert_only_network_bind_event(
        &capability_events,
        "denied",
        "capability_denied",
        "capability-zero-quota",
    );
    assert_only_network_bind_event(
        &provider_failure_events,
        "failed",
        "runtime_failure",
        "provider-failure",
    );

    let events = [
        wrong_process_events,
        wrong_environment_events,
        missing_events,
        expired_events,
        revoked_events,
        capability_events,
        provider_failure_events,
    ]
    .concat();
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"] == "network_bind")
            .count(),
        7
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["result"] == "denied")
            .count(),
        6
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["result"] == "failed")
            .count(),
        1
    );
    let observable = format!(
        "{events:?} {wrong_process_error} {wrong_environment_error} {missing_error} \
         {expired_error} {revoked_error} {capability_error} {provider_error}",
    );
    assert!(!observable.contains("a5a5a5a5"));
    assert_slice_absent(observable.as_bytes(), &private_key_marker, "denial output");
    assert_slice_absent(
        observable.as_bytes(),
        &private_key_text_marker,
        "denial output",
    );
    assert_slice_absent(observable.as_bytes(), &private_key_pem, "denial output");
    for record in LOG_RECORDS.lock().unwrap().iter() {
        assert_slice_absent(record.message.as_bytes(), &private_key_marker, "log output");
        assert_slice_absent(
            record.message.as_bytes(),
            &private_key_text_marker,
            "log output",
        );
        assert_slice_absent(record.message.as_bytes(), &private_key_pem, "log output");
    }
    Ok(())
}
