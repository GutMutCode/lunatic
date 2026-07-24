use std::io::{BufRead, BufReader, BufWriter, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use embedded_v3_direct_wasmtime::protocol::*;
use embedded_v3_direct_wasmtime::runtime::sha256_hex;

const A_SHA: &str = "9bce3d46eceef79abe98d88d16c2a8ac75ec1ffd68a7f328cc34a9c1a1fae0d1";
const B_SHA: &str = "5c918a4ea3730ac7109188ec94108a135fb4a537a713bcbf27fae325a5ea511e";
const BAD_B_SHA: &str = "ab3274086121b6ff8af75b331c5c101b5216c187b86f8ea12cd06673e72dc557";

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

fn start(harness: &mut Harness) {
    harness.send(
        1,
        ControlMessage::Hello(HelloControl {
            oracle_name: "rollout-test".into(),
            expected_candidate: CandidateKind::RawWasmtime,
        }),
    );
    assert!(matches!(harness.receive(1), EventMessage::Hello(_)));
    harness.send(
        2,
        ControlMessage::Init(InitControl {
            run_id: "rollout-test".into(),
            tenant_capacity: 32,
        }),
    );
    assert!(matches!(harness.receive(2), EventMessage::Initialized(_)));
    for tenant in 0..8_u32 {
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
            harness.receive(request),
            EventMessage::TenantCreated(_)
        ));
        assert!(matches!(
            harness.receive(request),
            EventMessage::ActivationStarted(_)
        ));
        assert!(matches!(
            harness.receive(request),
            EventMessage::TenantReady(_)
        ));
    }
}

fn increment_each(harness: &mut Harness) {
    for tenant in 0..8_u32 {
        let request = 30 + u64::from(tenant);
        let payload = vec![tenant as u8; 64];
        harness.send(
            request,
            ControlMessage::Command(CommandControl {
                tenant_id: TenantId(tenant),
                incarnation: IncarnationToken::new(0),
                command_id: CommandId(1_000 + u64::from(tenant)),
                operation: CommandOperation::Increment(IncrementOperation { delta: 1 }),
                payload_sha256: sha256_hex(&payload),
                payload,
            }),
        );
        assert!(matches!(
            harness.receive(request),
            EventMessage::CommandAccepted(_)
        ));
        assert!(matches!(
            harness.receive(request),
            EventMessage::CommandCompleted(_)
        ));
    }
}

struct RolloutSpec<'a> {
    request: u64,
    rollout_id: &'a str,
    from: &'a str,
    to: &'a str,
    build: &'a str,
    file: &'a str,
    sha: &'a str,
}

fn rollout(harness: &mut Harness, spec: RolloutSpec<'_>) -> (usize, usize, EventMessage) {
    let RolloutSpec {
        request,
        rollout_id,
        from,
        to,
        build,
        file,
        sha,
    } = spec;
    harness.send(
        request,
        ControlMessage::Rollout(RolloutControl {
            rollout_id: rollout_id.into(),
            targets: (0..8).map(TenantId).collect(),
            from_version: from.into(),
            to_version: to.into(),
            to_build_id: build.into(),
            artifact_ref: format!("guest-artifacts/{file}"),
            artifact_sha256: sha.into(),
        }),
    );
    assert!(matches!(
        harness.receive(request),
        EventMessage::RolloutStarted(_)
    ));
    let mut activations = 0;
    let mut ready = 0;
    loop {
        let event = harness.receive(request);
        match event {
            EventMessage::ActivationStarted(_) => activations += 1,
            EventMessage::RolloutTargetReady(_) => ready += 1,
            EventMessage::RolloutCommitted(_)
            | EventMessage::RolloutRolledBack(_)
            | EventMessage::RolloutInDoubt(_) => return (activations, ready, event),
            other => panic!("unexpected rollout event: {:?}", other),
        }
    }
}

fn assert_snapshots(harness: &mut Harness, version: &str, build: &str) {
    for tenant in 0..8_u32 {
        let request = 60 + u64::from(tenant);
        harness.send(
            request,
            ControlMessage::Snapshot(SnapshotControl {
                tenant_id: TenantId(tenant),
                expected_incarnation: Some(IncarnationToken::new(0)),
            }),
        );
        let EventMessage::SnapshotPresent(snapshot) = harness.receive(request) else {
            panic!("snapshot was not present");
        };
        assert_eq!(snapshot.state.counter, 1);
        assert_eq!(snapshot.state.logical_version, version);
        assert_eq!(snapshot.state.build_id, build);
        assert_eq!(snapshot.state.incarnation, IncarnationToken::new(0));
    }
}

#[test]
fn failed_activation_rolls_every_target_back_and_valid_rollout_preserves_state() {
    let mut harness = Harness::spawn();
    start(&mut harness);
    increment_each(&mut harness);

    let (activations, ready, failed) = rollout(
        &mut harness,
        RolloutSpec {
            request: 40,
            rollout_id: "failed-b",
            from: "A",
            to: "B",
            build: "build-bad-b",
            file: "tenant-bad-b.wasm",
            sha: BAD_B_SHA,
        },
    );
    assert_eq!(activations, 8);
    assert_eq!(ready, 7);
    assert!(matches!(failed, EventMessage::RolloutRolledBack(_)));
    assert_snapshots(&mut harness, "A", "build-a");

    let (activations, ready, committed) = rollout(
        &mut harness,
        RolloutSpec {
            request: 50,
            rollout_id: "valid-b",
            from: "A",
            to: "B",
            build: "build-b",
            file: "tenant-b.wasm",
            sha: B_SHA,
        },
    );
    assert_eq!(activations, 8);
    assert_eq!(ready, 8);
    assert!(matches!(committed, EventMessage::RolloutCommitted(_)));
    assert_snapshots(&mut harness, "B", "build-b");

    for tenant in 0..8_u32 {
        let request = 80 + u64::from(tenant);
        harness.send(
            request,
            ControlMessage::TeardownTenant(TeardownTenantControl {
                tenant_id: TenantId(tenant),
                incarnation: IncarnationToken::new(0),
            }),
        );
        assert!(matches!(
            harness.receive(request),
            EventMessage::TenantTornDown(_)
        ));
    }
    harness.send(100, ControlMessage::Shutdown(ShutdownControl {}));
    assert!(matches!(
        harness.receive(100),
        EventMessage::ShutdownComplete(_)
    ));
    drop(harness.input);
    assert!(harness.child.wait().unwrap().success());
}
