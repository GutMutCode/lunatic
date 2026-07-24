use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use embedded_v3_oracle::protocol::*;
use embedded_v3_oracle::sampling::{ProcessSampler, SamplerStatus};
use embedded_v3_oracle::{CandidateProcess, OracleSession, TimeoutTable};

const ARTIFACT_A_SHA256: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn trace_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "embedded-v3-oracle-{}-{nonce}.ndjson",
        std::process::id()
    ))
}

fn command_message(incarnation: IncarnationToken) -> ControlMessage {
    ControlMessage::Command(CommandControl {
        tenant_id: TenantId(2),
        incarnation,
        command_id: CommandId(77),
        operation: CommandOperation::Increment(IncrementOperation { delta: 3 }),
        payload: Vec::new(),
        payload_sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
    })
}

fn completed_result(events: &[embedded_v3_oracle::transport::ReceivedEvent]) -> CommandResult {
    events
        .iter()
        .find_map(|event| match &event.envelope.message {
            EventMessage::CommandCompleted(completed) => Some(completed.result.clone()),
            _ => None,
        })
        .expect("command emitted its terminal result")
}

#[test]
fn fake_candidate_exercises_transport_incarnations_and_deduplication() {
    let trace_path = trace_path();
    let command = Command::new(env!("CARGO_BIN_EXE_fake-candidate"));
    let process = CandidateProcess::spawn(command, &trace_path).unwrap();
    let sampler = ProcessSampler::start(&process);
    let mut session = OracleSession::new(process, TimeoutTable::default());

    session
        .round_trip(&ControlEnvelope::new(
            1,
            ControlMessage::Hello(HelloControl {
                oracle_name: "smoke".into(),
                expected_candidate: CandidateIdentity::fake_test_double(),
            }),
        ))
        .unwrap();
    session
        .round_trip(&ControlEnvelope::new(
            2,
            ControlMessage::Init(InitControl {
                run_id: "fake-smoke".into(),
                tenant_capacity: 32,
            }),
        ))
        .unwrap();
    session
        .round_trip(&ControlEnvelope::new(
            3,
            ControlMessage::CreateTenant(CreateTenantControl {
                tenant_id: TenantId(2),
                incarnation: IncarnationToken::new(0),
                logical_version: "A".into(),
                build_id: "A".into(),
                artifact_sha256: ARTIFACT_A_SHA256.into(),
            }),
        ))
        .unwrap();
    let first_incarnation = session
        .oracle()
        .tenant_incarnation(TenantId(2))
        .copied()
        .unwrap();

    let first = session
        .round_trip(&ControlEnvelope::new(4, command_message(first_incarnation)))
        .unwrap();
    let duplicate = session
        .round_trip(&ControlEnvelope::new(5, command_message(first_incarnation)))
        .unwrap();
    let first_result = completed_result(&first);
    let mut duplicate_result = completed_result(&duplicate);
    assert!(!first_result.deduplicated);
    assert!(duplicate_result.deduplicated);
    duplicate_result.deduplicated = false;
    assert_eq!(first_result, duplicate_result);
    assert_eq!(duplicate_result.counter, 3);
    let accepted_at = duplicate
        .iter()
        .find(|event| matches!(event.envelope.message, EventMessage::CommandAccepted(_)))
        .unwrap()
        .received_at_ns;
    let completed_at = duplicate
        .iter()
        .find(|event| matches!(event.envelope.message, EventMessage::CommandCompleted(_)))
        .unwrap()
        .received_at_ns;
    assert!(completed_at > accepted_at);

    session
        .round_trip(&ControlEnvelope::new(
            6,
            ControlMessage::Snapshot(SnapshotControl {
                tenant_id: TenantId(2),
                expected_incarnation: Some(first_incarnation),
            }),
        ))
        .unwrap();
    session
        .round_trip(&ControlEnvelope::new(
            7,
            ControlMessage::TeardownTenant(TeardownTenantControl {
                tenant_id: TenantId(2),
                incarnation: first_incarnation,
            }),
        ))
        .unwrap();
    session
        .round_trip(&ControlEnvelope::new(
            8,
            ControlMessage::CreateTenant(CreateTenantControl {
                tenant_id: TenantId(2),
                incarnation: IncarnationToken::new(1),
                logical_version: "A".into(),
                build_id: "A".into(),
                artifact_sha256: ARTIFACT_A_SHA256.into(),
            }),
        ))
        .unwrap();
    let second_incarnation = session
        .oracle()
        .tenant_incarnation(TenantId(2))
        .copied()
        .unwrap();
    assert_ne!(first_incarnation, second_incarnation);

    let stale = session
        .round_trip(&ControlEnvelope::new(9, command_message(first_incarnation)))
        .unwrap();
    assert!(matches!(
        &stale[0].envelope.message,
        EventMessage::CommandRejected(CommandRejectedEvent {
            reason: RejectionReason::StaleIncarnation,
            ..
        })
    ));
    assert_eq!(stale.len(), 1, "rejection must not have a later terminal");

    session
        .round_trip(&ControlEnvelope::new(
            10,
            ControlMessage::TeardownTenant(TeardownTenantControl {
                tenant_id: TenantId(2),
                incarnation: second_incarnation,
            }),
        ))
        .unwrap();
    session
        .round_trip(&ControlEnvelope::new(
            11,
            ControlMessage::Shutdown(ShutdownControl::default()),
        ))
        .unwrap();
    let sampler_status = sampler.stop();
    assert!(matches!(sampler_status, SamplerStatus::Stopped));
    session.process_mut().close_stdin();
    let status = session
        .process_mut()
        .wait_for_exit(Duration::from_secs(2))
        .unwrap();
    assert!(status.success());
    assert!(session
        .process()
        .stderr_lines()
        .iter()
        .any(|line| line.contains("strict stdin/stdout")));

    let raw_trace = fs::read_to_string(&trace_path).unwrap();
    let records: Vec<serde_json::Value> = raw_trace
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    for record in &records {
        assert!(record["monotonic_ns"].as_u64().is_some());
    }
    for required in [
        "stdin",
        "stdout",
        "stderr",
        "resource_sample",
        "process_exit",
    ] {
        assert!(
            records.iter().any(|record| record["kind"] == required),
            "trace did not contain {}",
            required
        );
    }
    fs::remove_file(trace_path).unwrap();
}
