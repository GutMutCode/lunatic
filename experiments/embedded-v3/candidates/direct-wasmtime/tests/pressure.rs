use std::io::{BufRead, BufReader, BufWriter, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

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
        let mut child = Command::new(env!("CARGO_BIN_EXE_embedded-v3-direct-wasmtime"))
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        Self {
            input: BufWriter::new(child.stdin.take().unwrap()),
            output: BufReader::new(child.stdout.take().unwrap()),
            child,
        }
    }

    fn send(&mut self, request: u64, message: ControlMessage) {
        let line = encode_control_line(&ControlEnvelope::new(request, message)).unwrap();
        writeln!(self.input, "{line}").unwrap();
        self.input.flush().unwrap();
    }

    fn receive(&mut self, request: u64) -> EventMessage {
        let mut line = String::new();
        assert_ne!(self.output.read_line(&mut line).unwrap(), 0);
        let event = decode_event_line(line.trim_end()).unwrap();
        assert_eq!(event.request_id, RequestId(request));
        event.message
    }
}

fn command(command_id: u64, payload_bytes: usize) -> CommandControl {
    let payload = vec![(command_id & 0xff) as u8; payload_bytes];
    CommandControl {
        tenant_id: TenantId(0),
        incarnation: IncarnationToken::new(0),
        command_id: CommandId(command_id),
        operation: CommandOperation::Increment(IncrementOperation { delta: 1 }),
        payload_sha256: sha256_hex(&payload),
        payload,
    }
}

fn start(harness: &mut Harness) {
    harness.send(
        1,
        ControlMessage::Hello(HelloControl {
            oracle_name: "pressure-test".into(),
            expected_candidate: CandidateKind::RawWasmtime,
        }),
    );
    assert!(matches!(harness.receive(1), EventMessage::Hello(_)));
    harness.send(
        2,
        ControlMessage::Init(InitControl {
            run_id: "pressure-test".into(),
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

#[test]
fn gate_preserves_exact_capacity_fifo_retry_and_payload_boundary() {
    let mut harness = Harness::spawn();
    start(&mut harness);
    harness.send(
        4,
        ControlMessage::SetDequeueGate(SetDequeueGateControl {
            tenant_id: TenantId(0),
            incarnation: IncarnationToken::new(0),
            closed: true,
        }),
    );
    assert!(matches!(
        harness.receive(4),
        EventMessage::DequeueGateSet(DequeueGateSetEvent { closed: true, .. })
    ));

    for index in 0..64_u64 {
        let request = 100 + index;
        harness.send(request, ControlMessage::Command(command(1_000 + index, 64)));
        assert!(matches!(
            harness.receive(request),
            EventMessage::CommandAccepted(_)
        ));
    }
    harness.send(164, ControlMessage::Command(command(1_064, 64)));
    assert!(matches!(
        harness.receive(164),
        EventMessage::CommandRejected(CommandRejectedEvent {
            reason: RejectionReason::Backpressure,
            ..
        })
    ));

    harness.send(
        5,
        ControlMessage::SetDequeueGate(SetDequeueGateControl {
            tenant_id: TenantId(0),
            incarnation: IncarnationToken::new(0),
            closed: false,
        }),
    );
    assert!(matches!(
        harness.receive(5),
        EventMessage::DequeueGateSet(DequeueGateSetEvent { closed: false, .. })
    ));
    for index in 0..64_u64 {
        let request = 100 + index;
        let EventMessage::CommandCompleted(completed) = harness.receive(request) else {
            panic!("accepted command did not complete");
        };
        assert_eq!(completed.command_id, CommandId(1_000 + index));
        assert_eq!(completed.result.counter, (index + 1) as i64);
    }

    harness.send(165, ControlMessage::Command(command(1_064, 64)));
    assert!(matches!(
        harness.receive(165),
        EventMessage::CommandAccepted(_)
    ));
    let EventMessage::CommandCompleted(retried) = harness.receive(165) else {
        panic!("retried command did not complete");
    };
    assert_eq!(retried.result.counter, 65);
    assert!(!retried.result.deduplicated);

    harness.send(166, ControlMessage::Command(command(2_000, 1_025)));
    assert!(matches!(
        harness.receive(166),
        EventMessage::CommandRejected(CommandRejectedEvent {
            reason: RejectionReason::PayloadTooLarge,
            ..
        })
    ));
    harness.send(167, ControlMessage::Command(command(2_001, 1_024)));
    assert!(matches!(
        harness.receive(167),
        EventMessage::CommandAccepted(_)
    ));
    let EventMessage::CommandCompleted(limit) = harness.receive(167) else {
        panic!("limit-sized command did not complete");
    };
    assert_eq!(limit.result.counter, 66);

    let mut conflict = command(2_001, 1_024);
    conflict.payload = vec![99; 1_024];
    conflict.payload_sha256 = sha256_hex(&conflict.payload);
    harness.send(168, ControlMessage::Command(conflict));
    assert!(matches!(
        harness.receive(168),
        EventMessage::CommandRejected(CommandRejectedEvent {
            reason: RejectionReason::CommandIdConflict,
            ..
        })
    ));

    harness.send(
        200,
        ControlMessage::TeardownTenant(TeardownTenantControl {
            tenant_id: TenantId(0),
            incarnation: IncarnationToken::new(0),
        }),
    );
    assert!(matches!(
        harness.receive(200),
        EventMessage::TenantTornDown(_)
    ));
    harness.send(201, ControlMessage::Shutdown(ShutdownControl {}));
    assert!(matches!(
        harness.receive(201),
        EventMessage::ShutdownComplete(_)
    ));
    drop(harness.input);
    assert!(harness.child.wait().unwrap().success());
}
