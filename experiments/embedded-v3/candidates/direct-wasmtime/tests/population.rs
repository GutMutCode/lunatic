use std::collections::{BTreeMap, BTreeSet};
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

    fn receive(&mut self) -> EventEnvelope {
        let mut line = String::new();
        assert_ne!(self.output.read_line(&mut line).unwrap(), 0);
        decode_event_line(line.trim_end()).unwrap()
    }

    fn receive_request(&mut self, request: u64) -> EventMessage {
        let event = self.receive();
        assert_eq!(event.request_id, RequestId(request));
        event.message
    }
}

#[test]
fn full_population_uses_32_independent_instances_and_cleans_up() {
    let mut harness = Harness::spawn();
    harness.send(
        1,
        ControlMessage::Hello(HelloControl {
            oracle_name: "population-test".into(),
            expected_candidate: CandidateKind::RawWasmtime,
        }),
    );
    assert!(matches!(harness.receive_request(1), EventMessage::Hello(_)));
    harness.send(
        2,
        ControlMessage::Init(InitControl {
            run_id: "population-test".into(),
            tenant_capacity: 32,
        }),
    );
    assert!(matches!(
        harness.receive_request(2),
        EventMessage::Initialized(_)
    ));

    for tenant in 0..32_u32 {
        let request = 10 + u64::from(tenant);
        harness.send(
            request,
            ControlMessage::CreateTenant(CreateTenantControl {
                tenant_id: TenantId(tenant),
                incarnation: IncarnationToken::new(0),
                logical_version: "A".into(),
                build_id: "build-a".into(),
                artifact_sha256: A_SHA.into(),
            }),
        );
        assert!(matches!(
            harness.receive_request(request),
            EventMessage::TenantCreated(_)
        ));
        assert!(matches!(
            harness.receive_request(request),
            EventMessage::ActivationStarted(_)
        ));
        assert!(matches!(
            harness.receive_request(request),
            EventMessage::TenantReady(_)
        ));
    }

    for tenant in 0..32_u32 {
        let payload = vec![tenant as u8; 64];
        harness.send(
            100 + u64::from(tenant),
            ControlMessage::Command(CommandControl {
                tenant_id: TenantId(tenant),
                incarnation: IncarnationToken::new(0),
                command_id: CommandId(1_000 + u64::from(tenant)),
                operation: CommandOperation::Increment(IncrementOperation { delta: 1 }),
                payload_sha256: sha256_hex(&payload),
                payload,
            }),
        );
    }
    let mut phases: BTreeMap<u64, u8> = BTreeMap::new();
    let mut counters = BTreeSet::new();
    for _ in 0..64 {
        let event = harness.receive();
        let phase = phases.entry(event.request_id.0).or_default();
        match event.message {
            EventMessage::CommandAccepted(_) => {
                assert_eq!(*phase, 0);
                *phase = 1;
            }
            EventMessage::CommandCompleted(completed) => {
                assert_eq!(*phase, 1);
                assert_eq!(completed.result.counter, 1);
                counters.insert(completed.tenant_id.0);
                *phase = 2;
            }
            other => panic!("unexpected population event: {:?}", other),
        }
    }
    assert_eq!(phases.len(), 32);
    assert!(phases.values().all(|phase| *phase == 2));
    assert_eq!(counters.len(), 32);

    for tenant in 0..32_u32 {
        let request = 200 + u64::from(tenant);
        harness.send(
            request,
            ControlMessage::TeardownTenant(TeardownTenantControl {
                tenant_id: TenantId(tenant),
                incarnation: IncarnationToken::new(0),
            }),
        );
        assert!(matches!(
            harness.receive_request(request),
            EventMessage::TenantTornDown(_)
        ));
    }
    harness.send(300, ControlMessage::Shutdown(ShutdownControl {}));
    assert!(matches!(
        harness.receive_request(300),
        EventMessage::ShutdownComplete(_)
    ));
    drop(harness.input);
    assert!(harness.child.wait().unwrap().success());
}
