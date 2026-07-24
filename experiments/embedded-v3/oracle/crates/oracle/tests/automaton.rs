use embedded_v3_oracle::protocol::*;
use embedded_v3_oracle::{Oracle, OracleError, RequestPhase};
use sha2::{Digest, Sha256};

const ARTIFACT_A_SHA256: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ARTIFACT_B_SHA256: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn token(value: &str) -> IncarnationToken {
    IncarnationToken::new(match value {
        "inc-1" | "old" => 0,
        "inc-2" | "inc-2-v2" | "new" => 1,
        "stale" => 99,
        other => other.parse().expect("test incarnation is numeric"),
    })
}

fn event(seq: u64, request: u64, message: EventMessage) -> EventEnvelope {
    EventEnvelope::new(seq, request, message)
}

fn bootstrap() -> Oracle {
    let mut oracle = Oracle::new();
    oracle
        .register_control(&ControlEnvelope::new(
            1,
            ControlMessage::Hello(HelloControl {
                oracle_name: "test".into(),
                expected_candidate: CandidateIdentity::fake_test_double(),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            1,
            1,
            EventMessage::Hello(HelloEvent {
                candidate: CandidateIdentity::fake_test_double(),
                implementation_version: "test".into(),
            }),
        ))
        .unwrap();
    oracle
        .register_control(&ControlEnvelope::new(
            2,
            ControlMessage::Init(InitControl {
                run_id: "run".into(),
                tenant_capacity: 32,
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            2,
            2,
            EventMessage::Initialized(InitializedEvent {
                run_id: "run".into(),
            }),
        ))
        .unwrap();
    oracle
}

fn register_command(oracle: &mut Oracle, request_id: u64, incarnation: &str) {
    oracle
        .register_control(&ControlEnvelope::new(
            request_id,
            ControlMessage::Command(CommandControl {
                tenant_id: TenantId(2),
                incarnation: token(incarnation),
                command_id: CommandId(9),
                operation: CommandOperation::Increment(IncrementOperation { delta: 1 }),
                payload: Vec::new(),
                payload_sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .into(),
            }),
        ))
        .unwrap();
}

fn accepted(incarnation: &str) -> EventMessage {
    EventMessage::CommandAccepted(CommandAcceptedEvent {
        tenant_id: TenantId(2),
        incarnation: token(incarnation),
        command_id: CommandId(9),
    })
}

fn completed(incarnation: &str) -> EventMessage {
    EventMessage::CommandCompleted(CommandCompletedEvent {
        tenant_id: TenantId(2),
        incarnation: token(incarnation),
        command_id: CommandId(9),
        result: CommandResult {
            incarnation: token(incarnation),
            generation: DecimalU64(token(incarnation).value()),
            counter: 1,
            logical_version: "A".into(),
            build_id: "A".into(),
            deduplicated: false,
            business_result_sha256: embedded_v3_oracle::workload::business_result_digest(
                token(incarnation).value(),
                1,
            ),
        },
    })
}

fn oracle_with_tenant() -> Oracle {
    let mut oracle = bootstrap();
    oracle
        .register_control(&ControlEnvelope::new(
            3,
            ControlMessage::CreateTenant(CreateTenantControl {
                tenant_id: TenantId(2),
                incarnation: token("inc-1"),
                logical_version: "A".into(),
                build_id: "A".into(),
                artifact_sha256: ARTIFACT_A_SHA256.into(),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            3,
            3,
            EventMessage::TenantCreated(TenantCreatedEvent {
                tenant_id: TenantId(2),
                incarnation: token("inc-1"),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            4,
            3,
            rollout_activation("A", "A", ARTIFACT_A_SHA256),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            5,
            3,
            EventMessage::TenantReady(TenantReadyEvent {
                tenant_id: TenantId(2),
                incarnation: token("inc-1"),
            }),
        ))
        .unwrap();
    oracle
}

fn register_rollout(oracle: &mut Oracle) {
    oracle
        .register_control(&ControlEnvelope::new(
            4,
            ControlMessage::Rollout(RolloutControl {
                rollout_id: "rollout".into(),
                targets: vec![TenantId(2)],
                from_version: "A".into(),
                to_version: "B".into(),
                to_build_id: "B".into(),
                artifact_ref: "guest-artifacts/tenant-b.wasm".into(),
                artifact_sha256: ARTIFACT_B_SHA256.into(),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            6,
            4,
            EventMessage::RolloutStarted(RolloutStartedEvent {
                rollout_id: "rollout".into(),
            }),
        ))
        .unwrap();
}

fn rollout_activation(version: &str, build_id: &str, sha256: &str) -> EventMessage {
    EventMessage::ActivationStarted(ActivationStartedEvent {
        tenant_id: TenantId(2),
        incarnation: token("inc-1"),
        logical_version: version.into(),
        build_id: build_id.into(),
        artifact_sha256: sha256.into(),
    })
}

fn rollout_ready() -> EventMessage {
    EventMessage::RolloutTargetReady(RolloutTargetReadyEvent {
        rollout_id: "rollout".into(),
        tenant_id: TenantId(2),
        incarnation: token("inc-1"),
        logical_version: "B".into(),
        build_id: "B".into(),
        artifact_sha256: ARTIFACT_B_SHA256.into(),
    })
}

fn authority_probe() -> AuthorityProbeControl {
    let artifact_bytes = b"\0asm\x01\0\0\0".to_vec();
    AuthorityProbeControl {
        attempt_id: DecimalU64(9),
        probe: AuthorityProbeKind::WasiP1FsRead,
        artifact_sha256: format!("{:x}", Sha256::digest(&artifact_bytes)),
        artifact_bytes,
        parameter_sha256: ARTIFACT_A_SHA256.into(),
        production_policy_sha256: ARTIFACT_B_SHA256.into(),
    }
}

#[test]
fn event_sequences_must_be_exactly_contiguous() {
    let mut oracle = bootstrap();
    register_command(&mut oracle, 3, "inc-1");
    let error = oracle
        .observe_event(&event(4, 3, accepted("inc-1")))
        .unwrap_err();
    assert_eq!(
        error,
        OracleError::EventSequence {
            expected: 3,
            actual: 4
        }
    );
}

#[test]
fn request_ids_are_nonzero_and_never_reused() {
    let mut oracle = Oracle::new();
    let hello = ControlMessage::Hello(HelloControl {
        oracle_name: "test".into(),
        expected_candidate: CandidateIdentity::fake_test_double(),
    });
    assert_eq!(
        oracle
            .register_control(&ControlEnvelope::new(0, hello.clone()))
            .unwrap_err(),
        OracleError::ZeroRequestId
    );

    let mut oracle = Oracle::new();
    oracle
        .register_control(&ControlEnvelope::new(1, hello.clone()))
        .unwrap();
    let error = oracle
        .register_control(&ControlEnvelope::new(1, hello))
        .unwrap_err();
    assert_eq!(error, OracleError::DuplicateRequest(RequestId(1)));
}

#[test]
fn command_acceptance_has_exactly_one_terminal_event() {
    let mut oracle = bootstrap();
    register_command(&mut oracle, 3, "inc-1");
    oracle
        .observe_event(&event(3, 3, accepted("inc-1")))
        .unwrap();
    assert_eq!(
        oracle.phase(RequestId(3)),
        Some(RequestPhase::CommandAccepted)
    );
    oracle
        .observe_event(&event(4, 3, completed("inc-1")))
        .unwrap();
    assert!(oracle.is_terminal(RequestId(3)));
    assert_eq!(
        oracle
            .observe_event(&event(5, 3, completed("inc-1")))
            .unwrap_err(),
        OracleError::DuplicateTerminal(RequestId(3))
    );
}

#[test]
fn rejection_is_terminal_and_cannot_be_followed_by_a_result() {
    let mut oracle = bootstrap();
    register_command(&mut oracle, 3, "stale");
    oracle
        .observe_event(&event(
            3,
            3,
            EventMessage::CommandRejected(CommandRejectedEvent {
                tenant_id: TenantId(2),
                incarnation: token("stale"),
                command_id: CommandId(9),
                reason: RejectionReason::StaleIncarnation,
            }),
        ))
        .unwrap();
    assert_eq!(
        oracle
            .observe_event(&event(4, 3, completed("stale")))
            .unwrap_err(),
        OracleError::DuplicateTerminal(RequestId(3))
    );
}

#[test]
fn terminal_event_after_external_timeout_is_rejected_as_late() {
    let mut oracle = bootstrap();
    register_command(&mut oracle, 3, "inc-1");
    oracle
        .observe_event(&event(3, 3, accepted("inc-1")))
        .unwrap();
    oracle.expire_request(RequestId(3)).unwrap();
    assert_eq!(
        oracle
            .observe_event(&event(4, 3, completed("inc-1")))
            .unwrap_err(),
        OracleError::LateEvent(RequestId(3))
    );
}

#[test]
fn create_fault_snapshot_rollout_teardown_and_shutdown_follow_their_automata() {
    let mut oracle = bootstrap();
    oracle
        .register_control(&ControlEnvelope::new(
            3,
            ControlMessage::CreateTenant(CreateTenantControl {
                tenant_id: TenantId(2),
                incarnation: token("inc-1"),
                logical_version: "A".into(),
                build_id: "A".into(),
                artifact_sha256: ARTIFACT_A_SHA256.into(),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            3,
            3,
            EventMessage::TenantCreated(TenantCreatedEvent {
                tenant_id: TenantId(2),
                incarnation: token("inc-1"),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            4,
            3,
            EventMessage::ActivationStarted(ActivationStartedEvent {
                tenant_id: TenantId(2),
                incarnation: token("inc-1"),
                logical_version: "A".into(),
                build_id: "A".into(),
                artifact_sha256: ARTIFACT_A_SHA256.into(),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            5,
            3,
            EventMessage::TenantReady(TenantReadyEvent {
                tenant_id: TenantId(2),
                incarnation: token("inc-1"),
            }),
        ))
        .unwrap();

    oracle
        .register_control(&ControlEnvelope::new(
            4,
            ControlMessage::InjectFault(InjectFaultControl {
                fault_id: DecimalU64(7),
                tenant_id: TenantId(2),
                incarnation: token("inc-1"),
                expected_replacement_incarnation: token("inc-2"),
                replacement_logical_version: "A".into(),
                replacement_build_id: "A".into(),
                replacement_artifact_sha256: ARTIFACT_A_SHA256.into(),
                fault: FaultKind::CpuHog(CpuHogFault::default()),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            6,
            4,
            EventMessage::ExecutionStarted(ExecutionStartedEvent {
                fault_id: DecimalU64(7),
                tenant_id: TenantId(2),
                incarnation: token("inc-1"),
                origin: ExecutionStartOrigin::GuestFirstActionObserver,
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            7,
            4,
            EventMessage::ExecutionFailed(ExecutionFailedEvent {
                fault_id: DecimalU64(7),
                tenant_id: TenantId(2),
                incarnation: token("inc-1"),
                reason: FaultFailureReason::CpuDeadline,
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            8,
            4,
            EventMessage::FailureObserved(FailureObservedEvent {
                fault_id: DecimalU64(7),
                tenant_id: TenantId(2),
                failed_incarnation: token("inc-1"),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            9,
            4,
            EventMessage::ActivationStarted(ActivationStartedEvent {
                tenant_id: TenantId(2),
                incarnation: token("inc-2"),
                logical_version: "A".into(),
                build_id: "A".into(),
                artifact_sha256: ARTIFACT_A_SHA256.into(),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            10,
            4,
            EventMessage::TenantReady(TenantReadyEvent {
                tenant_id: TenantId(2),
                incarnation: token("inc-2"),
            }),
        ))
        .unwrap();
    assert_eq!(
        oracle.tenant_incarnation(TenantId(2)),
        Some(&token("inc-2"))
    );

    oracle
        .register_control(&ControlEnvelope::new(
            5,
            ControlMessage::Snapshot(SnapshotControl {
                tenant_id: TenantId(2),
                expected_incarnation: Some(token("inc-2")),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            11,
            5,
            EventMessage::SnapshotPresent(SnapshotPresentEvent {
                tenant_id: TenantId(2),
                incarnation: token("inc-2"),
                state: CommandResult {
                    incarnation: token("inc-2"),
                    generation: DecimalU64(1),
                    counter: 0,
                    logical_version: "A".into(),
                    build_id: "A".into(),
                    deduplicated: false,
                    business_result_sha256: embedded_v3_oracle::workload::business_result_digest(
                        1, 0,
                    ),
                },
            }),
        ))
        .unwrap();

    oracle
        .register_control(&ControlEnvelope::new(
            6,
            ControlMessage::Rollout(RolloutControl {
                rollout_id: "r-1".into(),
                targets: vec![TenantId(2)],
                from_version: "v1".into(),
                to_version: "v2".into(),
                to_build_id: "B".into(),
                artifact_ref: "guest-artifacts/tenant-b.wasm".into(),
                artifact_sha256: ARTIFACT_B_SHA256.into(),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            12,
            6,
            EventMessage::RolloutStarted(RolloutStartedEvent {
                rollout_id: "r-1".into(),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            13,
            6,
            EventMessage::ActivationStarted(ActivationStartedEvent {
                tenant_id: TenantId(2),
                incarnation: token("inc-2-v2"),
                logical_version: "v2".into(),
                build_id: "B".into(),
                artifact_sha256: ARTIFACT_B_SHA256.into(),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            14,
            6,
            EventMessage::RolloutTargetReady(RolloutTargetReadyEvent {
                rollout_id: "r-1".into(),
                tenant_id: TenantId(2),
                incarnation: token("inc-2-v2"),
                logical_version: "v2".into(),
                build_id: "B".into(),
                artifact_sha256: ARTIFACT_B_SHA256.into(),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            15,
            6,
            EventMessage::RolloutCommitted(RolloutCommittedEvent {
                rollout_id: "r-1".into(),
                version: "v2".into(),
            }),
        ))
        .unwrap();

    oracle
        .register_control(&ControlEnvelope::new(
            7,
            ControlMessage::TeardownTenant(TeardownTenantControl {
                tenant_id: TenantId(2),
                incarnation: token("inc-2-v2"),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            16,
            7,
            EventMessage::TenantTornDown(TenantTornDownEvent {
                tenant_id: TenantId(2),
                incarnation: token("inc-2-v2"),
            }),
        ))
        .unwrap();
    assert!(oracle.tenant_incarnation(TenantId(2)).is_none());

    oracle
        .register_control(&ControlEnvelope::new(
            8,
            ControlMessage::Shutdown(ShutdownControl::default()),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            17,
            8,
            EventMessage::ShutdownComplete(ShutdownCompleteEvent::default()),
        ))
        .unwrap();
}

#[test]
fn snapshot_missing_and_stale_are_distinct_terminal_outcomes() {
    let mut oracle = bootstrap();
    oracle
        .register_control(&ControlEnvelope::new(
            3,
            ControlMessage::Snapshot(SnapshotControl {
                tenant_id: TenantId(3),
                expected_incarnation: None,
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            3,
            3,
            EventMessage::SnapshotMissing(SnapshotMissingEvent {
                tenant_id: TenantId(3),
            }),
        ))
        .unwrap();

    oracle
        .register_control(&ControlEnvelope::new(
            4,
            ControlMessage::Snapshot(SnapshotControl {
                tenant_id: TenantId(2),
                expected_incarnation: Some(token("old")),
            }),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            4,
            4,
            EventMessage::SnapshotStale(SnapshotStaleEvent {
                tenant_id: TenantId(2),
                expected_incarnation: token("old"),
                actual_incarnation: token("new"),
            }),
        ))
        .unwrap();
}

#[test]
fn one_through_four_identical_activation_markers_are_allowed() {
    let mut oracle = oracle_with_tenant();
    register_rollout(&mut oracle);
    for sequence in 7..=10 {
        oracle
            .observe_event(&event(
                sequence,
                4,
                rollout_activation("B", "B", ARTIFACT_B_SHA256),
            ))
            .unwrap();
    }
    oracle
        .observe_event(&event(11, 4, rollout_ready()))
        .unwrap();
}

#[test]
fn fifth_or_conflicting_activation_marker_is_rejected() {
    let mut fifth = oracle_with_tenant();
    register_rollout(&mut fifth);
    for sequence in 7..=10 {
        fifth
            .observe_event(&event(
                sequence,
                4,
                rollout_activation("B", "B", ARTIFACT_B_SHA256),
            ))
            .unwrap();
    }
    assert!(matches!(
        fifth
            .observe_event(&event(
                11,
                4,
                rollout_activation("B", "B", ARTIFACT_B_SHA256),
            ))
            .unwrap_err(),
        OracleError::Transition { .. }
    ));

    let mut conflicting = oracle_with_tenant();
    register_rollout(&mut conflicting);
    conflicting
        .observe_event(&event(
            7,
            4,
            rollout_activation("B", "B", ARTIFACT_B_SHA256),
        ))
        .unwrap();
    assert!(matches!(
        conflicting
            .observe_event(&event(
                8,
                4,
                rollout_activation("B", "wrong-build", ARTIFACT_B_SHA256),
            ))
            .unwrap_err(),
        OracleError::Transition { .. }
    ));
}

#[test]
fn ready_requires_activation_and_no_marker_may_follow_ready_or_terminal() {
    let mut missing = oracle_with_tenant();
    register_rollout(&mut missing);
    assert!(matches!(
        missing
            .observe_event(&event(7, 4, rollout_ready()))
            .unwrap_err(),
        OracleError::Transition { .. }
    ));

    let mut after_ready = oracle_with_tenant();
    register_rollout(&mut after_ready);
    after_ready
        .observe_event(&event(
            7,
            4,
            rollout_activation("B", "B", ARTIFACT_B_SHA256),
        ))
        .unwrap();
    after_ready
        .observe_event(&event(8, 4, rollout_ready()))
        .unwrap();
    assert!(matches!(
        after_ready
            .observe_event(&event(
                9,
                4,
                rollout_activation("B", "B", ARTIFACT_B_SHA256),
            ))
            .unwrap_err(),
        OracleError::Transition { .. }
    ));

    let mut after_terminal = oracle_with_tenant();
    register_rollout(&mut after_terminal);
    after_terminal
        .observe_event(&event(
            7,
            4,
            rollout_activation("B", "B", ARTIFACT_B_SHA256),
        ))
        .unwrap();
    after_terminal
        .observe_event(&event(8, 4, rollout_ready()))
        .unwrap();
    after_terminal
        .observe_event(&event(
            9,
            4,
            EventMessage::RolloutCommitted(RolloutCommittedEvent {
                rollout_id: "rollout".into(),
                version: "B".into(),
            }),
        ))
        .unwrap();
    assert_eq!(
        after_terminal
            .observe_event(&event(
                10,
                4,
                rollout_activation("B", "B", ARTIFACT_B_SHA256),
            ))
            .unwrap_err(),
        OracleError::DuplicateTerminal(RequestId(4))
    );
}

#[test]
fn eager_rollout_failure_needs_no_marker_and_marker_never_changes_incarnation() {
    let mut oracle = oracle_with_tenant();
    register_rollout(&mut oracle);
    oracle
        .observe_event(&event(
            7,
            4,
            EventMessage::RolloutRolledBack(RolloutRolledBackEvent {
                rollout_id: "rollout".into(),
                restored_version: "A".into(),
                reason: "eager activation rejected".into(),
            }),
        ))
        .unwrap();
    assert_eq!(
        oracle.tenant_incarnation(TenantId(2)),
        Some(&token("inc-1"))
    );
}

#[test]
fn authority_probe_requires_accepted_then_exact_terminal() {
    let mut oracle = bootstrap();
    let probe = authority_probe();
    oracle
        .register_control(&ControlEnvelope::new(
            3,
            ControlMessage::AuthorityProbe(probe.clone()),
        ))
        .unwrap();
    oracle
        .observe_event(&event(
            3,
            3,
            EventMessage::AuthorityProbeAccepted(AuthorityProbeAcceptedEvent {
                attempt_id: probe.attempt_id,
                probe: probe.probe,
                artifact_sha256: probe.artifact_sha256.clone(),
                parameter_sha256: probe.parameter_sha256.clone(),
                production_policy_sha256: probe.production_policy_sha256.clone(),
            }),
        ))
        .unwrap();
    assert_eq!(
        oracle.phase(RequestId(3)),
        Some(RequestPhase::AuthorityProbeAccepted)
    );
    oracle
        .observe_event(&event(
            4,
            3,
            EventMessage::AuthorityProbeTerminal(AuthorityProbeTerminalEvent {
                attempt_id: probe.attempt_id,
                probe: probe.probe,
                artifact_sha256: probe.artifact_sha256,
                parameter_sha256: probe.parameter_sha256,
                production_policy_sha256: probe.production_policy_sha256,
                stage: AuthorityProbeStage::Instantiate,
                result: AuthorityProbeResult::AbsentAtLink,
                error_class: AuthorityProbeErrorClass::UnknownImport,
                guest_return_sha256: EMPTY_SHA256.into(),
            }),
        ))
        .unwrap();

    let mut direct_terminal = bootstrap();
    let probe = authority_probe();
    direct_terminal
        .register_control(&ControlEnvelope::new(
            3,
            ControlMessage::AuthorityProbe(probe.clone()),
        ))
        .unwrap();
    assert!(matches!(
        direct_terminal
            .observe_event(&event(
                3,
                3,
                EventMessage::AuthorityProbeTerminal(AuthorityProbeTerminalEvent {
                    attempt_id: probe.attempt_id,
                    probe: probe.probe,
                    artifact_sha256: probe.artifact_sha256,
                    parameter_sha256: probe.parameter_sha256,
                    production_policy_sha256: probe.production_policy_sha256,
                    stage: AuthorityProbeStage::Instantiate,
                    result: AuthorityProbeResult::AbsentAtLink,
                    error_class: AuthorityProbeErrorClass::UnknownImport,
                    guest_return_sha256: EMPTY_SHA256.into(),
                }),
            ))
            .unwrap_err(),
        OracleError::Transition { .. }
    ));
}

#[test]
fn unsolicited_fatal_requires_request_zero() {
    let mut oracle = bootstrap();
    oracle
        .observe_event(&event(
            3,
            0,
            EventMessage::Fatal(FatalEvent {
                code: "host_failed".into(),
                message: "boom".into(),
            }),
        ))
        .unwrap();
    assert_eq!(oracle.fatal().unwrap().code, "host_failed");

    let mut oracle = bootstrap();
    let error = oracle
        .observe_event(&event(
            3,
            0,
            EventMessage::SnapshotMissing(SnapshotMissingEvent {
                tenant_id: TenantId(1),
            }),
        ))
        .unwrap_err();
    assert_eq!(error, OracleError::InvalidUnsolicitedEvent);
}
