use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use embedded_v3_direct_wasmtime::protocol::*;
use embedded_v3_direct_wasmtime::runtime::sha256_hex;

const A_SHA: &str = "9bce3d46eceef79abe98d88d16c2a8ac75ec1ffd68a7f328cc34a9c1a1fae0d1";

struct Harness {
    child: Child,
    input: BufWriter<ChildStdin>,
    output: BufReader<ChildStdout>,
}

impl Harness {
    fn spawn() -> Self {
        Self::spawn_at(Path::new(env!("CARGO_MANIFEST_DIR")))
    }

    fn spawn_at(run_root: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_embedded-v3-direct-wasmtime"))
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
        }
    }

    fn send(&mut self, request_id: u64, message: ControlMessage) {
        let line = encode_control_line(&ControlEnvelope::new(request_id, message)).unwrap();
        writeln!(self.input, "{line}").unwrap();
        self.input.flush().unwrap();
    }

    fn receive(&mut self, request_id: u64) -> EventMessage {
        let mut line = String::new();
        assert_ne!(self.output.read_line(&mut line).unwrap(), 0);
        let event = decode_event_line(line.trim_end()).unwrap();
        assert_eq!(event.request_id, RequestId(request_id));
        event.message
    }

    fn shutdown(mut self, request_id: u64) {
        self.send(request_id, ControlMessage::Shutdown(ShutdownControl {}));
        assert!(matches!(
            self.receive(request_id),
            EventMessage::ShutdownComplete(_)
        ));
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
            "embedded-v3-direct-empty-run-bundle-{}",
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

fn command(incarnation: u64, command_id: u64, operation: CommandOperation) -> CommandControl {
    let payload = vec![7; 64];
    CommandControl {
        tenant_id: TenantId(0),
        incarnation: IncarnationToken::new(incarnation),
        command_id: CommandId(command_id),
        operation,
        payload_sha256: sha256_hex(&payload),
        payload,
    }
}

#[test]
fn startup_resolves_guest_artifacts_from_process_current_directory() {
    let run_bundle = EmptyRunBundle::create();
    let mut harness = Harness::spawn_at(&run_bundle.0);
    harness.send(
        1,
        ControlMessage::Hello(HelloControl {
            oracle_name: "run-bundle-path-test".into(),
            expected_candidate: CandidateKind::RawWasmtime,
        }),
    );
    assert!(matches!(harness.receive(1), EventMessage::Hello(_)));
    harness.send(
        2,
        ControlMessage::Init(InitControl {
            run_id: "run-bundle-path-test".into(),
            tenant_capacity: 32,
        }),
    );
    match harness.receive(2) {
        EventMessage::Fatal(FatalEvent { code, message }) => {
            assert_eq!(code, "candidate_contract_error");
            assert!(message.contains(&run_bundle.0.join("guest-artifacts").display().to_string()));
        }
        event => panic!("expected run-bundle artifact failure, got {:?}", event),
    }
    harness.expect_failure();
}

fn create(harness: &mut Harness) {
    harness.send(
        1,
        ControlMessage::Hello(HelloControl {
            oracle_name: "direct-test".into(),
            expected_candidate: CandidateKind::RawWasmtime,
        }),
    );
    assert!(matches!(harness.receive(1), EventMessage::Hello(_)));
    harness.send(
        2,
        ControlMessage::Init(InitControl {
            run_id: "direct-test".into(),
            tenant_capacity: 32,
        }),
    );
    assert!(matches!(harness.receive(2), EventMessage::Initialized(_)));
    harness.send(
        3,
        ControlMessage::CreateTenant(CreateTenantControl {
            tenant_id: TenantId(0),
            incarnation: IncarnationToken::new(0),
            logical_version: "A".into(),
            build_id: "build-a".into(),
            artifact_sha256: A_SHA.into(),
        }),
    );
    assert!(matches!(harness.receive(3), EventMessage::TenantCreated(_)));
    assert!(matches!(
        harness.receive(3),
        EventMessage::ActivationStarted(_)
    ));
    assert!(matches!(harness.receive(3), EventMessage::TenantReady(_)));
}

fn complete_increment(harness: &mut Harness, request: u64, incarnation: u64, command_id: u64) {
    harness.send(
        request,
        ControlMessage::Command(command(
            incarnation,
            command_id,
            CommandOperation::Increment(IncrementOperation { delta: 1 }),
        )),
    );
    assert!(matches!(
        harness.receive(request),
        EventMessage::CommandAccepted(_)
    ));
    let terminal = harness.receive(request);
    assert!(
        matches!(terminal, EventMessage::CommandCompleted(_)),
        "{:?}",
        terminal
    );
}

#[test]
fn trap_cpu_recovery_dedup_stale_and_cleanup_use_real_guest_execution() {
    let mut harness = Harness::spawn();
    create(&mut harness);
    complete_increment(&mut harness, 4, 0, 100);

    harness.send(
        5,
        ControlMessage::Command(command(
            0,
            100,
            CommandOperation::Increment(IncrementOperation { delta: 1 }),
        )),
    );
    assert!(matches!(
        harness.receive(5),
        EventMessage::CommandAccepted(_)
    ));
    let EventMessage::CommandCompleted(duplicate) = harness.receive(5) else {
        panic!("duplicate did not complete");
    };
    assert!(duplicate.result.deduplicated);
    assert_eq!(duplicate.result.counter, 1);

    harness.send(
        6,
        ControlMessage::InjectFault(InjectFaultControl {
            fault_id: DecimalU64(1),
            tenant_id: TenantId(0),
            incarnation: IncarnationToken::new(0),
            expected_replacement_incarnation: IncarnationToken::new(1),
            replacement_logical_version: "A".into(),
            replacement_build_id: "build-a".into(),
            replacement_artifact_sha256: A_SHA.into(),
            fault: FaultKind::Trap(TrapFault {}),
        }),
    );
    assert!(matches!(
        harness.receive(6),
        EventMessage::ExecutionFailed(ExecutionFailedEvent {
            reason: FaultFailureReason::GuestTrap,
            ..
        })
    ));
    assert!(matches!(
        harness.receive(6),
        EventMessage::FailureObserved(_)
    ));
    assert!(matches!(
        harness.receive(6),
        EventMessage::ActivationStarted(_)
    ));
    assert!(matches!(harness.receive(6), EventMessage::TenantReady(_)));

    harness.send(
        7,
        ControlMessage::Command(command(
            0,
            100,
            CommandOperation::Increment(IncrementOperation { delta: 1 }),
        )),
    );
    assert!(matches!(
        harness.receive(7),
        EventMessage::CommandRejected(CommandRejectedEvent {
            reason: RejectionReason::StaleIncarnation,
            ..
        })
    ));
    complete_increment(&mut harness, 8, 1, 100);
    complete_increment(&mut harness, 9, 1, 101);

    let admitted = Instant::now();
    harness.send(
        10,
        ControlMessage::InjectFault(InjectFaultControl {
            fault_id: DecimalU64(2),
            tenant_id: TenantId(0),
            incarnation: IncarnationToken::new(1),
            expected_replacement_incarnation: IncarnationToken::new(2),
            replacement_logical_version: "A".into(),
            replacement_build_id: "build-a".into(),
            replacement_artifact_sha256: A_SHA.into(),
            fault: FaultKind::CpuHog(CpuHogFault {}),
        }),
    );
    assert!(matches!(
        harness.receive(10),
        EventMessage::ExecutionStarted(_)
    ));
    let started = Instant::now();
    assert!(started.duration_since(admitted) <= Duration::from_millis(20));
    assert!(matches!(
        harness.receive(10),
        EventMessage::ExecutionFailed(ExecutionFailedEvent {
            reason: FaultFailureReason::CpuDeadline,
            ..
        })
    ));
    let failed = Instant::now();
    assert!(failed.duration_since(started) >= Duration::from_millis(40));
    assert!(failed.duration_since(started) <= Duration::from_millis(100));
    assert!(matches!(
        harness.receive(10),
        EventMessage::FailureObserved(_)
    ));
    assert!(matches!(
        harness.receive(10),
        EventMessage::ActivationStarted(_)
    ));
    assert!(matches!(harness.receive(10), EventMessage::TenantReady(_)));
    assert!(Instant::now().duration_since(admitted) <= Duration::from_millis(500));

    harness.send(11, ControlMessage::Quiesce(QuiesceControl {}));
    assert!(matches!(harness.receive(11), EventMessage::Quiesced(_)));
    harness.send(
        12,
        ControlMessage::TeardownTenant(TeardownTenantControl {
            tenant_id: TenantId(0),
            incarnation: IncarnationToken::new(2),
        }),
    );
    assert!(matches!(
        harness.receive(12),
        EventMessage::TenantTornDown(_)
    ));
    harness.send(
        13,
        ControlMessage::Command(command(2, 999, CommandOperation::Read(ReadOperation {}))),
    );
    assert!(matches!(
        harness.receive(13),
        EventMessage::CommandRejected(CommandRejectedEvent {
            reason: RejectionReason::TenantMissing,
            ..
        })
    ));
    harness.shutdown(14);
}
