#![allow(dead_code)]

#[path = "../src/protocol.rs"]
mod protocol;

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use protocol::*;
use sha2::{Digest, Sha256};

const TENANT_A: &str = "9bce3d46eceef79abe98d88d16c2a8ac75ec1ffd68a7f328cc34a9c1a1fae0d1";
const TENANT_B: &str = "5c918a4ea3730ac7109188ec94108a135fb4a537a713bcbf27fae325a5ea511e";
const TENANT_BAD_A: &str = "d3675abac308b7a20513f2ff65042c9b0e9e84195c16ec5256f8472a15653a2d";

struct Harness {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_request: u64,
    next_event: u64,
    buffered: BTreeMap<u64, Vec<EventEnvelope>>,
}

impl Harness {
    fn spawn() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_embedded-v3-extism"))
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn Extism candidate");
        let stdin = BufWriter::new(child.stdin.take().expect("candidate stdin"));
        let stdout = BufReader::new(child.stdout.take().expect("candidate stdout"));
        Self {
            child,
            stdin,
            stdout,
            next_request: 1,
            next_event: 1,
            buffered: BTreeMap::new(),
        }
    }

    fn send(&mut self, message: ControlMessage) -> u64 {
        let request = self.next_request;
        self.next_request += 1;
        let envelope = ControlEnvelope::new(RequestId(request), message);
        serde_json::to_writer(&mut self.stdin, &envelope).expect("serialize control");
        self.stdin.write_all(b"\n").expect("write newline");
        self.stdin.flush().expect("flush control");
        request
    }

    fn round_trip(&mut self, message: ControlMessage) -> Vec<EventEnvelope> {
        let request = self.send(message);
        self.wait(request)
    }

    fn wait(&mut self, request: u64) -> Vec<EventEnvelope> {
        loop {
            if self
                .buffered
                .get(&request)
                .and_then(|events| events.last())
                .is_some_and(|event| is_terminal(&event.message))
            {
                return self.buffered.remove(&request).unwrap();
            }
            let mut line = String::new();
            let read = self
                .stdout
                .read_line(&mut line)
                .expect("read candidate event");
            assert_ne!(read, 0, "candidate stdout closed before terminal event");
            let envelope: EventEnvelope = serde_json::from_str(line.trim_end())
                .unwrap_or_else(|error| panic!("invalid event {:?}: {}", line, error));
            validate_event_envelope(&envelope).expect("valid event envelope");
            assert_eq!(envelope.event_seq.0, self.next_event, "event sequence");
            self.next_event += 1;
            if let EventMessage::Fatal(fatal) = &envelope.message {
                panic!("candidate fatal {}: {}", fatal.code, fatal.message);
            }
            self.buffered
                .entry(envelope.request_id.0)
                .or_default()
                .push(envelope);
        }
    }

    fn initialize(&mut self) {
        let hello = self.round_trip(ControlMessage::Hello(HelloControl {
            oracle_name: "extism-integration-test".into(),
            expected_candidate: CandidateKind::Extism,
        }));
        assert!(matches!(
            hello.last().unwrap().message,
            EventMessage::Hello(_)
        ));
        let initialized = self.round_trip(ControlMessage::Init(InitControl {
            run_id: "extism-integration-test".into(),
            tenant_capacity: 32,
        }));
        assert!(matches!(
            initialized.last().unwrap().message,
            EventMessage::Initialized(_)
        ));
    }

    fn create_a(&mut self, tenant_id: u32) {
        let events = self.round_trip(ControlMessage::CreateTenant(CreateTenantControl {
            tenant_id: TenantId(tenant_id),
            incarnation: IncarnationToken::new(0),
            logical_version: "A".into(),
            build_id: "A".into(),
            artifact_sha256: TENANT_A.into(),
        }));
        assert!(events
            .iter()
            .any(|event| matches!(event.message, EventMessage::TenantCreated(_))));
        assert!(matches!(
            events.last().unwrap().message,
            EventMessage::TenantReady(_)
        ));
    }

    fn shutdown(mut self) {
        let events = self.round_trip(ControlMessage::Shutdown(ShutdownControl::default()));
        assert!(matches!(
            events.last().unwrap().message,
            EventMessage::ShutdownComplete(_)
        ));
        assert!(self.child.wait().expect("wait candidate").success());
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn is_terminal(message: &EventMessage) -> bool {
    !matches!(
        message,
        EventMessage::TenantCreated(_)
            | EventMessage::CommandAccepted(_)
            | EventMessage::ExecutionStarted(_)
            | EventMessage::ExecutionFailed(_)
            | EventMessage::FailureObserved(_)
            | EventMessage::RolloutStarted(_)
            | EventMessage::ActivationStarted(_)
            | EventMessage::RolloutTargetReady(_)
            | EventMessage::AuthorityProbeAccepted(_)
    )
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn command(tenant: u32, incarnation: u64, id: u64, payload: Vec<u8>) -> ControlMessage {
    ControlMessage::Command(CommandControl {
        tenant_id: TenantId(tenant),
        incarnation: IncarnationToken::new(incarnation),
        command_id: CommandId(id),
        operation: CommandOperation::Increment(IncrementOperation { delta: 1 }),
        payload_sha256: sha256(&payload),
        payload,
    })
}

fn snapshot(harness: &mut Harness, incarnation: u64) -> CommandResult {
    let events = harness.round_trip(ControlMessage::Snapshot(SnapshotControl {
        tenant_id: TenantId(0),
        expected_incarnation: Some(IncarnationToken::new(incarnation)),
    }));
    match &events.last().unwrap().message {
        EventMessage::SnapshotPresent(event) => event.state.clone(),
        other => panic!("unexpected snapshot terminal: {:?}", other),
    }
}

fn artifacts_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../guests/authority/artifacts")
}

fn verify_authority(harness: &mut Harness) {
    let probes = [
        (
            AuthorityProbeKind::WasiP1FsRead,
            "wasi_p1_fs_read.wasm",
            "54036193974010613797dbe7eb0c38707a543cacc527c4629170f2dec9d3d94e",
        ),
        (
            AuthorityProbeKind::WasiP1FsMutate,
            "wasi_p1_fs_mutate.wasm",
            "3957cafa04f03032968b384d163720cd7dd0d1b1fcd2d3a4711b4c90c11213b9",
        ),
        (
            AuthorityProbeKind::LunaticTcp,
            "lunatic_tcp.wasm",
            "89bd340abc693b78722386c3a358c3604d2dc9d900157d61fbbe941ce44bc1ec",
        ),
        (
            AuthorityProbeKind::LunaticUdp,
            "lunatic_udp.wasm",
            "0978966bd17ed2efe9461d1ce6f342254e0c14095fb73043a01d795daeb70987",
        ),
        (
            AuthorityProbeKind::LunaticSqliteCreate,
            "lunatic_sqlite_create.wasm",
            "9e8a8ea6c53c46f436b62c5263cdb8f6dbfc039ec23cf2a42c3623c13f3a2900",
        ),
        (
            AuthorityProbeKind::ExtismHttp,
            "extism_http.wasm",
            "66c9c8415da5fe32257eae848617d3d296ce6a075df91979897c129fbb7d58e2",
        ),
    ];
    for (index, (probe, file, parameter_sha256)) in probes.iter().copied().enumerate() {
        let artifact = fs::read(artifacts_root().join(file)).expect("read authority artifact");
        let artifact_sha256 = sha256(&artifact);
        let events = harness.round_trip(ControlMessage::AuthorityProbe(AuthorityProbeControl {
            attempt_id: DecimalU64(80_000 + index as u64),
            probe,
            artifact_sha256: artifact_sha256.clone(),
            artifact_bytes: artifact,
            parameter_sha256: parameter_sha256.into(),
            production_policy_sha256: "f".repeat(64),
        }));
        assert!(matches!(
            events.first().unwrap().message,
            EventMessage::AuthorityProbeAccepted(_)
        ));
        let EventMessage::AuthorityProbeTerminal(terminal) = &events.last().unwrap().message else {
            panic!("unexpected authority terminal for {:?}", probe);
        };
        assert_eq!(terminal.artifact_sha256, artifact_sha256);
        assert_eq!(
            terminal.guest_return_sha256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        if probe == AuthorityProbeKind::ExtismHttp {
            assert_eq!(terminal.stage, AuthorityProbeStage::Invoke);
            assert_eq!(terminal.result, AuthorityProbeResult::PolicyDenied);
            assert_eq!(
                terminal.error_class,
                AuthorityProbeErrorClass::HttpCapabilityDenied
            );
        } else {
            assert!(matches!(
                terminal.stage,
                AuthorityProbeStage::Compile | AuthorityProbeStage::Instantiate
            ));
            assert_eq!(terminal.result, AuthorityProbeResult::AbsentAtLink);
            assert_eq!(
                terminal.error_class,
                AuthorityProbeErrorClass::UnknownImport
            );
        }
    }
}

#[test]
fn full_population_runs_in_one_candidate_process_and_cleans_up() {
    let mut harness = Harness::spawn();
    harness.initialize();
    let creates: Vec<_> = (0..32)
        .map(|tenant| {
            harness.send(ControlMessage::CreateTenant(CreateTenantControl {
                tenant_id: TenantId(tenant),
                incarnation: IncarnationToken::new(0),
                logical_version: "A".into(),
                build_id: "A".into(),
                artifact_sha256: TENANT_A.into(),
            }))
        })
        .collect();
    for request in creates {
        assert!(matches!(
            harness.wait(request).last().unwrap().message,
            EventMessage::TenantReady(_)
        ));
    }
    let commands: Vec<_> = (0..32)
        .map(|tenant| {
            harness.send(command(
                tenant,
                0,
                90_000 + u64::from(tenant),
                vec![tenant as u8; 32],
            ))
        })
        .collect();
    for (tenant, request) in commands.into_iter().enumerate() {
        let events = harness.wait(request);
        assert_eq!(events.len(), 2, "population command event count");
        assert!(matches!(
            events[0].message,
            EventMessage::CommandAccepted(CommandAcceptedEvent {
                tenant_id,
                command_id,
                ..
            }) if tenant_id.0 == tenant as u32 && command_id.0 == 90_000 + tenant as u64
        ));
        match &events[1].message {
            EventMessage::CommandCompleted(event) => assert_eq!(event.result.counter, 1),
            other => panic!("unexpected population command terminal: {:?}", other),
        }
    }
    assert!(matches!(
        harness
            .round_trip(ControlMessage::Quiesce(QuiesceControl::default()))
            .last()
            .unwrap()
            .message,
        EventMessage::Quiesced(_)
    ));
    for tenant in 0..32 {
        assert!(matches!(
            harness
                .round_trip(ControlMessage::TeardownTenant(TeardownTenantControl {
                    tenant_id: TenantId(tenant),
                    incarnation: IncarnationToken::new(0),
                }))
                .last()
                .unwrap()
                .message,
            EventMessage::TenantTornDown(_)
        ));
    }
    harness.shutdown();
}

#[test]
fn pressure_cpu_rollout_and_authority_use_real_extism_paths() {
    let mut harness = Harness::spawn();
    harness.initialize();
    harness.create_a(0);
    harness.create_a(7);

    let oversized = vec![0x5a; 1_025];
    let events = harness.round_trip(command(0, 0, 10, oversized));
    assert!(matches!(
        events.last().unwrap().message,
        EventMessage::CommandRejected(CommandRejectedEvent {
            reason: RejectionReason::PayloadTooLarge,
            ..
        })
    ));

    let events = harness.round_trip(ControlMessage::SetDequeueGate(SetDequeueGateControl {
        tenant_id: TenantId(0),
        incarnation: IncarnationToken::new(0),
        closed: true,
    }));
    assert!(matches!(
        events.last().unwrap().message,
        EventMessage::DequeueGateSet(DequeueGateSetEvent { closed: true, .. })
    ));

    let queued: Vec<_> = (0..64)
        .map(|index| harness.send(command(0, 0, 1_000 + index, vec![index as u8; 32])))
        .collect();
    let overflow = harness.send(command(0, 0, 2_000, vec![0x77; 32]));
    let overflow_events = harness.wait(overflow);
    assert!(matches!(
        overflow_events.last().unwrap().message,
        EventMessage::CommandRejected(CommandRejectedEvent {
            reason: RejectionReason::Backpressure,
            ..
        })
    ));

    let events = harness.round_trip(ControlMessage::SetDequeueGate(SetDequeueGateControl {
        tenant_id: TenantId(0),
        incarnation: IncarnationToken::new(0),
        closed: false,
    }));
    assert!(matches!(
        events.last().unwrap().message,
        EventMessage::DequeueGateSet(DequeueGateSetEvent { closed: false, .. })
    ));
    for request in queued {
        let events = harness.wait(request);
        assert!(events
            .iter()
            .any(|event| matches!(event.message, EventMessage::CommandAccepted(_))));
        assert!(matches!(
            events.last().unwrap().message,
            EventMessage::CommandCompleted(_)
        ));
    }
    let state = snapshot(&mut harness, 0);
    assert_eq!(state.counter, 64);
    assert_eq!(state.logical_version, "A");

    let events = harness.round_trip(ControlMessage::InjectFault(InjectFaultControl {
        fault_id: DecimalU64(50_001),
        tenant_id: TenantId(0),
        incarnation: IncarnationToken::new(0),
        expected_replacement_incarnation: IncarnationToken::new(1),
        replacement_logical_version: "A".into(),
        replacement_build_id: "A".into(),
        replacement_artifact_sha256: TENANT_A.into(),
        fault: FaultKind::CpuHog(CpuHogFault::default()),
    }));
    assert!(events.iter().any(|event| matches!(
        event.message,
        EventMessage::ExecutionStarted(ExecutionStartedEvent {
            origin: ExecutionStartOrigin::GuestFirstActionObserver,
            ..
        })
    )));
    assert!(events.iter().any(|event| matches!(
        event.message,
        EventMessage::ExecutionFailed(ExecutionFailedEvent {
            reason: FaultFailureReason::CpuDeadline,
            ..
        })
    )));
    assert!(events
        .iter()
        .any(|event| matches!(event.message, EventMessage::FailureObserved(_))));
    assert!(matches!(
        events.last().unwrap().message,
        EventMessage::TenantReady(TenantReadyEvent {
            incarnation,
            ..
        }) if incarnation.value() == 1
    ));
    let recovered = snapshot(&mut harness, 1);
    assert_eq!(recovered.counter, 0);

    let events = harness.round_trip(ControlMessage::Rollout(RolloutControl {
        rollout_id: "extism-good-b".into(),
        targets: vec![TenantId(0), TenantId(7)],
        from_version: "A".into(),
        to_version: "B".into(),
        to_build_id: "B".into(),
        artifact_ref: "tenant-b.wasm".into(),
        artifact_sha256: TENANT_B.into(),
    }));
    assert!(
        events
            .iter()
            .any(|event| matches!(event.message, EventMessage::RolloutStarted(_))),
        "rollout events: {:?}",
        events
    );
    assert!(events
        .iter()
        .any(|event| matches!(event.message, EventMessage::ActivationStarted(_))));
    assert!(matches!(
        events.last().unwrap().message,
        EventMessage::RolloutCommitted(RolloutCommittedEvent { ref version, .. }) if version == "B"
    ));
    let on_b = harness.round_trip(command(0, 1, 3_000, vec![0x42; 32]));
    match &on_b.last().unwrap().message {
        EventMessage::CommandCompleted(event) => {
            assert_eq!(event.result.counter, 1);
            assert_eq!(event.result.logical_version, "B");
            assert_eq!(event.result.build_id, "B");
        }
        other => panic!("unexpected B command terminal: {:?}", other),
    }

    let events = harness.round_trip(ControlMessage::Rollout(RolloutControl {
        rollout_id: "extism-bad-a".into(),
        targets: vec![TenantId(7)],
        from_version: "B".into(),
        to_version: "A".into(),
        to_build_id: "A-bad".into(),
        artifact_ref: "tenant-bad-a.wasm".into(),
        artifact_sha256: TENANT_BAD_A.into(),
    }));
    assert!(
        matches!(
            events.last().unwrap().message,
            EventMessage::RolloutRolledBack(RolloutRolledBackEvent {
                ref restored_version,
                ..
            }) if restored_version == "B"
        ),
        "bad rollout events: {:?}",
        events
    );
    let after_rollback = snapshot(&mut harness, 1);
    assert_eq!(after_rollback.counter, 1);
    assert_eq!(after_rollback.logical_version, "B");

    verify_authority(&mut harness);

    let events = harness.round_trip(ControlMessage::Quiesce(QuiesceControl::default()));
    assert!(matches!(
        events.last().unwrap().message,
        EventMessage::Quiesced(_)
    ));
    let events = harness.round_trip(ControlMessage::TeardownTenant(TeardownTenantControl {
        tenant_id: TenantId(0),
        incarnation: IncarnationToken::new(1),
    }));
    assert!(matches!(
        events.last().unwrap().message,
        EventMessage::TenantTornDown(_)
    ));
    let events = harness.round_trip(ControlMessage::TeardownTenant(TeardownTenantControl {
        tenant_id: TenantId(7),
        incarnation: IncarnationToken::new(0),
    }));
    assert!(matches!(
        events.last().unwrap().message,
        EventMessage::TenantTornDown(_)
    ));
    harness.shutdown();
}
