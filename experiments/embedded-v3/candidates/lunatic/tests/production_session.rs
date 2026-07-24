use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use embedded_v3_lunatic::protocol::*;
use sha2::{Digest, Sha256};

const A_SHA: &str = "9bce3d46eceef79abe98d88d16c2a8ac75ec1ffd68a7f328cc34a9c1a1fae0d1";
const B_SHA: &str = "5c918a4ea3730ac7109188ec94108a135fb4a537a713bcbf27fae325a5ea511e";
const BAD_A_SHA: &str = "d3675abac308b7a20513f2ff65042c9b0e9e84195c16ec5256f8472a15653a2d";

struct Session {
    child: Child,
    input: BufWriter<ChildStdin>,
    output: BufReader<ChildStdout>,
    request: u64,
    sequence: u64,
}

impl Session {
    fn start() -> Self {
        Self::start_at(Path::new(env!("CARGO_MANIFEST_DIR")))
    }

    fn start_at(run_root: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_embedded-v3-lunatic"))
            .current_dir(run_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let input = BufWriter::new(child.stdin.take().unwrap());
        let output = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            input,
            output,
            request: 1,
            sequence: 1,
        }
    }

    fn send(&mut self, message: ControlMessage) -> RequestId {
        let request = RequestId(self.request);
        self.request += 1;
        let line = encode_control_line(&ControlEnvelope::new(request, message)).unwrap();
        writeln!(self.input, "{line}").unwrap();
        self.input.flush().unwrap();
        request
    }

    fn receive(&mut self, request: RequestId) -> EventMessage {
        let event = self.receive_next();
        assert_eq!(event.request_id, request);
        event.message
    }

    fn receive_next(&mut self) -> EventEnvelope {
        let mut line = String::new();
        self.output.read_line(&mut line).unwrap();
        assert!(
            !line.is_empty(),
            "candidate exited before an expected event"
        );
        let event = decode_event_line(line.trim_end_matches(['\r', '\n'])).unwrap();
        assert_eq!(event.event_seq, EventSeq(self.sequence));
        self.sequence += 1;
        event
    }

    fn finish(mut self) {
        drop(self.input);
        assert!(self.child.wait().unwrap().success());
    }

    fn expect_failure(mut self) {
        drop(self.input);
        assert!(!self.child.wait().unwrap().success());
    }
}

struct EmptyRunBundle(PathBuf);

impl EmptyRunBundle {
    fn create() -> Self {
        let root = std::env::temp_dir().join(format!(
            "embedded-v3-lunatic-empty-run-bundle-{}",
            std::process::id()
        ));
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        fs::create_dir_all(root.join("guest-artifacts")).unwrap();
        Self(root)
    }
}

impl Drop for EmptyRunBundle {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn create(tenant: u32, incarnation: u64, version: &str, build: &str, sha: &str) -> ControlMessage {
    ControlMessage::CreateTenant(CreateTenantControl {
        tenant_id: TenantId(tenant),
        incarnation: IncarnationToken::new(incarnation),
        logical_version: version.into(),
        build_id: build.into(),
        artifact_sha256: sha.into(),
    })
}

fn command(tenant: u32, incarnation: u64, id: u64, payload: Vec<u8>) -> ControlMessage {
    ControlMessage::Command(CommandControl {
        tenant_id: TenantId(tenant),
        incarnation: IncarnationToken::new(incarnation),
        command_id: CommandId(id),
        operation: CommandOperation::Increment(IncrementOperation { delta: 1 }),
        payload_sha256: digest(&payload),
        payload,
    })
}

#[test]
fn startup_resolves_guest_artifacts_from_process_current_directory() {
    let run_bundle = EmptyRunBundle::create();
    let mut session = Session::start_at(&run_bundle.0);
    let hello = session.send(ControlMessage::Hello(HelloControl {
        oracle_name: "run-bundle-path-test".into(),
        expected_candidate: CandidateKind::Lunatic,
    }));
    assert!(matches!(session.receive(hello), EventMessage::Hello(_)));
    session.send(ControlMessage::Init(InitControl {
        run_id: "run-bundle-path-test".into(),
        tenant_capacity: 32,
    }));
    let fatal = session.receive_next();
    assert_eq!(fatal.request_id, RequestId(0));
    match fatal.message {
        EventMessage::Fatal(FatalEvent { code, message }) => {
            assert_eq!(code, "candidate_failure");
            assert!(message.contains(&run_bundle.0.join("guest-artifacts").display().to_string()));
        }
        event => panic!("expected run-bundle artifact failure, got {:?}", event),
    }
    session.expect_failure();
}

#[test]
fn accepted_event_precedes_terminal_for_each_request_in_32_command_burst() {
    let mut session = Session::start();
    let hello = session.send(ControlMessage::Hello(HelloControl {
        oracle_name: "accept-order-test".into(),
        expected_candidate: CandidateKind::Lunatic,
    }));
    assert!(matches!(session.receive(hello), EventMessage::Hello(_)));
    let init = session.send(ControlMessage::Init(InitControl {
        run_id: "accept-order-test-1".into(),
        tenant_capacity: 32,
    }));
    assert!(matches!(
        session.receive(init),
        EventMessage::Initialized(_)
    ));

    for tenant in 0..32 {
        let request = session.send(create(tenant, 0, "A", "build-a", A_SHA));
        assert!(matches!(
            session.receive(request),
            EventMessage::TenantCreated(_)
        ));
        assert!(matches!(
            session.receive(request),
            EventMessage::ActivationStarted(_)
        ));
        assert!(matches!(
            session.receive(request),
            EventMessage::TenantReady(_)
        ));
    }

    let mut requests = std::collections::HashMap::new();
    for tenant in 0..32 {
        let request = session.send(command(tenant, 0, 10_000 + u64::from(tenant), Vec::new()));
        requests.insert(request, (tenant, false, false));
    }

    let mut terminal_count = 0;
    while terminal_count < 32 {
        let event = session.receive_next();
        let (tenant, accepted, completed) = requests
            .get_mut(&event.request_id)
            .expect("burst emitted an event for an unknown request");
        match event.message {
            EventMessage::CommandAccepted(value) => {
                assert!(!*accepted, "request was accepted more than once");
                assert_eq!(value.tenant_id, TenantId(*tenant));
                *accepted = true;
            }
            EventMessage::CommandCompleted(value) => {
                assert!(
                    *accepted,
                    "command terminal raced ahead of its accepted event"
                );
                assert!(!*completed, "request completed more than once");
                assert_eq!(value.tenant_id, TenantId(*tenant));
                *completed = true;
                terminal_count += 1;
            }
            other => panic!("unexpected burst event: {:?}", other),
        }
    }
    assert!(requests
        .values()
        .all(|(_, accepted, completed)| *accepted && *completed));

    for tenant in 0..32 {
        let teardown = session.send(ControlMessage::TeardownTenant(TeardownTenantControl {
            tenant_id: TenantId(tenant),
            incarnation: IncarnationToken::new(0),
        }));
        assert!(matches!(
            session.receive(teardown),
            EventMessage::TenantTornDown(_)
        ));
    }
    let shutdown = session.send(ControlMessage::Shutdown(ShutdownControl {}));
    assert!(matches!(
        session.receive(shutdown),
        EventMessage::ShutdownComplete(_)
    ));
    session.finish();
}

#[test]
fn production_candidate_executes_frozen_recovery_pressure_rollout_and_authority_paths() {
    let mut session = Session::start();
    let hello = session.send(ControlMessage::Hello(HelloControl {
        oracle_name: "candidate-test".into(),
        expected_candidate: CandidateKind::Lunatic,
    }));
    assert!(matches!(session.receive(hello), EventMessage::Hello(_)));
    let init = session.send(ControlMessage::Init(InitControl {
        run_id: "candidate-test-1".into(),
        tenant_capacity: 32,
    }));
    assert!(matches!(
        session.receive(init),
        EventMessage::Initialized(_)
    ));

    let first = session.send(create(0, 0, "A", "build-a", A_SHA));
    assert!(matches!(
        session.receive(first),
        EventMessage::TenantCreated(_)
    ));
    assert!(matches!(
        session.receive(first),
        EventMessage::ActivationStarted(_)
    ));
    assert!(matches!(
        session.receive(first),
        EventMessage::TenantReady(_)
    ));

    let close = session.send(ControlMessage::SetDequeueGate(SetDequeueGateControl {
        tenant_id: TenantId(0),
        incarnation: IncarnationToken::new(0),
        closed: true,
    }));
    assert!(matches!(
        session.receive(close),
        EventMessage::DequeueGateSet(_)
    ));
    let mut admitted = Vec::new();
    for id in 1_000..1_064 {
        let request = session.send(command(0, 0, id, Vec::new()));
        assert!(matches!(
            session.receive(request),
            EventMessage::CommandAccepted(_)
        ));
        admitted.push(request);
    }
    let overflow = session.send(command(0, 0, 1_064, Vec::new()));
    assert!(matches!(
        session.receive(overflow),
        EventMessage::CommandRejected(CommandRejectedEvent {
            reason: RejectionReason::Backpressure,
            ..
        })
    ));
    let open = session.send(ControlMessage::SetDequeueGate(SetDequeueGateControl {
        tenant_id: TenantId(0),
        incarnation: IncarnationToken::new(0),
        closed: false,
    }));
    assert!(matches!(
        session.receive(open),
        EventMessage::DequeueGateSet(_)
    ));
    for (offset, request) in admitted.into_iter().enumerate() {
        match session.receive(request) {
            EventMessage::CommandCompleted(event) => {
                assert_eq!(event.command_id, CommandId(1_000 + offset as u64));
                assert_eq!(event.result.counter, offset as i64 + 1);
            }
            other => panic!("unexpected pressure terminal: {:?}", other),
        }
    }

    let oversize = session.send(command(0, 0, 2_000, vec![1; 1_025]));
    assert!(matches!(
        session.receive(oversize),
        EventMessage::CommandRejected(CommandRejectedEvent {
            reason: RejectionReason::PayloadTooLarge,
            ..
        })
    ));
    let limit = session.send(command(0, 0, 2_001, vec![2; 1_024]));
    assert!(matches!(
        session.receive(limit),
        EventMessage::CommandAccepted(_)
    ));
    assert!(matches!(
        session.receive(limit),
        EventMessage::CommandCompleted(_)
    ));

    let trap = session.send(ControlMessage::InjectFault(InjectFaultControl {
        fault_id: DecimalU64(1),
        tenant_id: TenantId(0),
        incarnation: IncarnationToken::new(0),
        expected_replacement_incarnation: IncarnationToken::new(1),
        replacement_logical_version: "A".into(),
        replacement_build_id: "build-a".into(),
        replacement_artifact_sha256: A_SHA.into(),
        fault: FaultKind::Trap(TrapFault {}),
    }));
    assert!(matches!(
        session.receive(trap),
        EventMessage::ExecutionFailed(_)
    ));
    assert!(matches!(
        session.receive(trap),
        EventMessage::FailureObserved(_)
    ));
    assert!(matches!(
        session.receive(trap),
        EventMessage::ActivationStarted(_)
    ));
    assert!(matches!(
        session.receive(trap),
        EventMessage::TenantReady(_)
    ));

    let cpu = session.send(ControlMessage::InjectFault(InjectFaultControl {
        fault_id: DecimalU64(2),
        tenant_id: TenantId(0),
        incarnation: IncarnationToken::new(1),
        expected_replacement_incarnation: IncarnationToken::new(2),
        replacement_logical_version: "A".into(),
        replacement_build_id: "build-a".into(),
        replacement_artifact_sha256: A_SHA.into(),
        fault: FaultKind::CpuHog(CpuHogFault {}),
    }));
    assert!(matches!(
        session.receive(cpu),
        EventMessage::ExecutionStarted(_)
    ));
    assert!(matches!(
        session.receive(cpu),
        EventMessage::ExecutionFailed(_)
    ));
    assert!(matches!(
        session.receive(cpu),
        EventMessage::FailureObserved(_)
    ));
    assert!(matches!(
        session.receive(cpu),
        EventMessage::ActivationStarted(_)
    ));
    assert!(matches!(session.receive(cpu), EventMessage::TenantReady(_)));

    let valid = session.send(ControlMessage::Rollout(RolloutControl {
        rollout_id: "valid-1".into(),
        targets: vec![TenantId(0)],
        from_version: "A".into(),
        to_version: "B".into(),
        to_build_id: "build-b".into(),
        artifact_ref: "guest-artifacts/tenant-b.wasm".into(),
        artifact_sha256: B_SHA.into(),
    }));
    assert!(matches!(
        session.receive(valid),
        EventMessage::RolloutStarted(_)
    ));
    assert!(matches!(
        session.receive(valid),
        EventMessage::ActivationStarted(_)
    ));
    assert!(matches!(
        session.receive(valid),
        EventMessage::RolloutTargetReady(_)
    ));
    assert!(matches!(
        session.receive(valid),
        EventMessage::RolloutCommitted(_)
    ));

    for tenant in 1..8 {
        let request = session.send(create(tenant, 0, "B", "build-b", B_SHA));
        assert!(matches!(
            session.receive(request),
            EventMessage::TenantCreated(_)
        ));
        assert!(matches!(
            session.receive(request),
            EventMessage::ActivationStarted(_)
        ));
        assert!(matches!(
            session.receive(request),
            EventMessage::TenantReady(_)
        ));
    }
    let failed = session.send(ControlMessage::Rollout(RolloutControl {
        rollout_id: "failed-1".into(),
        targets: (0..8).map(TenantId).collect(),
        from_version: "B".into(),
        to_version: "A".into(),
        to_build_id: "build-bad-a".into(),
        artifact_ref: "guest-artifacts/tenant-bad-a.wasm".into(),
        artifact_sha256: BAD_A_SHA.into(),
    }));
    assert!(matches!(
        session.receive(failed),
        EventMessage::RolloutStarted(_)
    ));
    for _ in 0..7 {
        assert!(matches!(
            session.receive(failed),
            EventMessage::ActivationStarted(_)
        ));
        assert!(matches!(
            session.receive(failed),
            EventMessage::RolloutTargetReady(_)
        ));
    }
    assert!(matches!(
        session.receive(failed),
        EventMessage::ActivationStarted(_)
    ));
    assert!(matches!(
        session.receive(failed),
        EventMessage::RolloutRolledBack(_)
    ));

    let authority_root =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../guests/authority/artifacts");
    for (attempt, probe, file) in [
        (1, AuthorityProbeKind::WasiP1FsRead, "wasi_p1_fs_read.wasm"),
        (
            2,
            AuthorityProbeKind::WasiP1FsMutate,
            "wasi_p1_fs_mutate.wasm",
        ),
        (3, AuthorityProbeKind::LunaticTcp, "lunatic_tcp.wasm"),
        (4, AuthorityProbeKind::LunaticUdp, "lunatic_udp.wasm"),
        (
            5,
            AuthorityProbeKind::LunaticSqliteCreate,
            "lunatic_sqlite_create.wasm",
        ),
        (6, AuthorityProbeKind::ExtismHttp, "extism_http.wasm"),
    ] {
        let authority_bytes = fs::read(authority_root.join(file)).unwrap();
        let authority = session.send(ControlMessage::AuthorityProbe(AuthorityProbeControl {
            attempt_id: DecimalU64(attempt),
            probe,
            artifact_sha256: digest(&authority_bytes),
            artifact_bytes: authority_bytes,
            parameter_sha256: "b".repeat(64),
            production_policy_sha256: "c".repeat(64),
        }));
        assert!(matches!(
            session.receive(authority),
            EventMessage::AuthorityProbeAccepted(_)
        ));
        assert!(matches!(
            session.receive(authority),
            EventMessage::AuthorityProbeTerminal(AuthorityProbeTerminalEvent {
                result: AuthorityProbeResult::AbsentAtLink,
                error_class: AuthorityProbeErrorClass::UnknownImport,
                ..
            })
        ));
    }

    for tenant in 0..8 {
        let teardown = session.send(ControlMessage::TeardownTenant(TeardownTenantControl {
            tenant_id: TenantId(tenant),
            incarnation: IncarnationToken::new(if tenant == 0 { 2 } else { 0 }),
        }));
        assert!(matches!(
            session.receive(teardown),
            EventMessage::TenantTornDown(_)
        ));
    }
    let shutdown = session.send(ControlMessage::Shutdown(ShutdownControl {}));
    assert!(matches!(
        session.receive(shutdown),
        EventMessage::ShutdownComplete(_)
    ));
    session.finish();
}

#[test]
fn production_source_uses_lunatic_runtime_without_direct_wasmtime_construction() {
    let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut source = String::new();
    for file in ["host.rs", "runtime.rs", "state.rs"] {
        source.push_str(&fs::read_to_string(source_root.join(file)).unwrap());
    }
    for forbidden in [
        "wasmtime::Engine::new",
        "wasmtime::Module::new",
        "wasmtime::Store::new",
        "wasmtime::Instance::new",
    ] {
        assert!(
            !source.contains(forbidden),
            "direct bypass found: {}",
            forbidden
        );
    }
    for required in [
        "WasmtimeRuntime::new",
        ".compile_module::<CandidateState>",
        ".instantiate(",
        ".call_ref(",
        ".snapshot_memory(",
        ".restore_memory(",
    ] {
        assert!(
            source.contains(required),
            "missing product path: {}",
            required
        );
    }
}
