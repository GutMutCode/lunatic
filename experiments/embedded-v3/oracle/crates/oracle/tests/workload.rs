use embedded_v3_oracle::protocol::{
    AuthorityProbeErrorClass, AuthorityProbeKind, AuthorityProbeResult, AuthorityProbeStage,
    AuthorityProbeTerminalEvent, DecimalU64, TenantId, EMPTY_SHA256,
};
use embedded_v3_oracle::workload::*;
use sha2::{Digest, Sha256};

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn command(request_id: u64, tenant: u32, incarnation: u64, id: u64) -> ModelCommand {
    let payload = vec![0x5a; 64];
    ModelCommand {
        request_id,
        tenant_id: TenantId(tenant),
        incarnation,
        command_id: id,
        operation: ModelOperation::Increment(1),
        payload_sha256: sha256_hex(&payload),
        payload,
    }
}

#[test]
fn full_shape_is_deterministic_and_complete() {
    let scenario = ScenarioDocument::embedded().unwrap();
    for block in 0..BLOCK_COUNT {
        let plan = build_full_plan(&scenario, block).unwrap();
        let summary = verify_full_shape(&plan).unwrap();
        assert_eq!(summary.total_actions, EXPECTED_STATIC_ACTIONS_PER_BLOCK);
        assert_eq!(summary.normal_warmup_commands, 1_024);
        assert_eq!(summary.normal_commands, 10_240);
        assert_eq!(summary.pressure_accepted_before_overflow, 64);
        assert_eq!(summary.fault_seeds, 60);
        assert_eq!(summary.failed_rollouts, 30);
        assert_eq!(summary.valid_rollouts, 30);
        assert_eq!(summary.update_anchor_reads, 960);
        assert_eq!(summary.update_proof_snapshots, 2_880);
        assert_eq!(summary.authority_positive_controls, 6);
        assert_eq!(summary.authority_canaries, 6);
        assert_eq!(
            summary.retained_actions,
            if block == 0 {
                0
            } else {
                EXPECTED_RETAINED_ACTIONS
            }
        );
        assert_eq!(summary.block, block_order(block).unwrap());
    }
}

#[test]
fn canonical_plan_bundle_hash_is_frozen() {
    let scenario: ScenarioDocument = serde_json::from_str(EMBEDDED_SCENARIO_JSON).unwrap();
    assert_eq!(
        plan_bundle_sha256(&scenario).unwrap(),
        EMBEDDED_PLAN_BUNDLE_SHA256
    );
}

#[test]
fn frozen_scenario_is_closed_and_exactly_typed() {
    let unknown = EMBEDDED_SCENARIO_JSON.replacen('{', "{\"unexpected\":true,", 1);
    assert!(serde_json::from_str::<ScenarioDocument>(&unknown).is_err());

    let changed = EMBEDDED_SCENARIO_JSON.replacen("\"count\": 32", "\"count\": 31", 1);
    let changed: ScenarioDocument = serde_json::from_str(&changed).unwrap();
    assert!(changed.validate().is_err());
}

#[test]
fn failed_and_valid_mixed_window_slos_are_both_frozen_at_two_seconds() {
    let scenario: serde_json::Value = serde_json::from_str(EMBEDDED_SCENARIO_JSON).unwrap();
    let slos = &scenario["verdict"]["absolute_slos"];
    assert_eq!(slos["failed_mixed_window_max_ms"], 2_000);
    assert_eq!(slos["valid_mixed_window_max_ms"], 2_000);

    let schema: serde_json::Value =
        serde_json::from_str(include_str!("../../../schemas/scenario-final.schema.json")).unwrap();
    let absolute_slos = &schema["$defs"]["absolute_slos"];
    let required = absolute_slos["required"].as_array().unwrap();
    for name in ["failed_mixed_window_max_ms", "valid_mixed_window_max_ms"] {
        assert!(required.iter().any(|value| value == name));
        assert_eq!(absolute_slos["properties"][name]["const"], 2_000);
    }
}

#[test]
fn structured_command_ids_are_collision_free_and_exactly_bounded() {
    assert_eq!(
        worker_command_id(59, 15, WORKER_MAX_SEQUENCE).unwrap(),
        UPDATE_WORKER_PREFIX
            | (59_u64 << STRUCTURED_ATTEMPT_SHIFT)
            | (15_u64 << STRUCTURED_TENANT_SHIFT)
            | WORKER_MAX_SEQUENCE
    );
    assert!(worker_command_id(60, 0, 0).is_err());
    assert!(worker_command_id(0, 16, 0).is_err());
    assert!(worker_command_id(0, 0, WORKER_MAX_SEQUENCE + 1).is_err());

    assert_eq!(
        proof_command_id(59, 15, 2).unwrap(),
        UPDATE_PROOF_PREFIX
            | (59_u64 << STRUCTURED_ATTEMPT_SHIFT)
            | (15_u64 << STRUCTURED_TENANT_SHIFT)
            | 2
    );
    assert!(proof_command_id(60, 0, 0).is_err());
    assert!(proof_command_id(0, 16, 0).is_err());
    assert!(proof_command_id(0, 0, 3).is_err());

    assert_eq!(
        cleanup_command_id(34, 31).unwrap(),
        CLEANUP_PREFIX | (34_u64 << STRUCTURED_ATTEMPT_SHIFT) | (31_u64 << STRUCTURED_TENANT_SHIFT)
    );
    assert!(cleanup_command_id(35, 0).is_err());
    assert!(cleanup_command_id(0, 32).is_err());
    assert_ne!(
        worker_command_id(0, 0, 0).unwrap(),
        proof_command_id(0, 0, 0).unwrap()
    );
    assert_ne!(
        proof_command_id(0, 0, 0).unwrap(),
        cleanup_command_id(0, 0).unwrap()
    );
}

fn worker_record(
    block: u32,
    attempt: u32,
    tenant: u32,
    sequence: u64,
    transports: &[(u64, u64, WorkerTransportOutcome)],
) -> WorkerLogicalTrace {
    let command_id = worker_command_id(attempt, tenant, sequence).unwrap();
    let payload_bytes = 64;
    let payload_sha256 =
        sha256_hex(&deterministic_payload(block, command_id, payload_bytes).unwrap());
    WorkerLogicalTrace {
        sequence,
        operation: if sequence == 0 || sequence % 2 == 0 {
            WorkerOperation::Read
        } else {
            WorkerOperation::IncrementOne
        },
        command_id,
        payload_bytes,
        payload_sha256: payload_sha256.clone(),
        rollout_flush_ns: 1_000,
        candidate_terminal_ns: 2_000,
        drain_completed_ns: 2_500,
        proof_started_ns: 2_600,
        transports: transports
            .iter()
            .map(|(request_id, issued_ns, outcome)| WorkerTransportTrace {
                request_id: *request_id,
                command_id,
                payload_sha256: payload_sha256.clone(),
                issued_ns: *issued_ns,
                outcome: *outcome,
            })
            .collect(),
    }
}

fn valid_worker_trace() -> Vec<WorkerLogicalTrace> {
    vec![
        worker_record(
            3,
            7,
            5,
            0,
            &[(
                10,
                100,
                WorkerTransportOutcome::AcceptedTerminal { terminal_ns: 200 },
            )],
        ),
        worker_record(
            3,
            7,
            5,
            1,
            &[
                (
                    11,
                    1_100,
                    WorkerTransportOutcome::RetryableRejection {
                        reason: WorkerRetryReason::Backpressure,
                        rejected_ns: 1_200,
                    },
                ),
                (
                    12,
                    1_300,
                    WorkerTransportOutcome::AcceptedTerminal { terminal_ns: 1_400 },
                ),
            ],
        ),
        worker_record(
            3,
            7,
            5,
            2,
            &[(
                13,
                1_500,
                WorkerTransportOutcome::AcceptedTerminal { terminal_ns: 1_600 },
            )],
        ),
    ]
}

#[test]
fn worker_cardinality_is_derived_from_typed_transport_evidence() {
    let trace = valid_worker_trace();
    let summary = validate_worker_trace_cardinality(3, 7, 5, &trace).unwrap();
    assert_eq!(summary.logical_operations, 3);
    assert_eq!(summary.continuous_logical_operations, 2);
    assert_eq!(summary.transport_attempts, 4);
    assert_eq!(summary.retries, 1);

    let anchor_only = &trace[..1];
    let summary = validate_worker_trace_cardinality(3, 7, 5, anchor_only).unwrap();
    assert_eq!(summary.logical_operations, 1);
    assert_eq!(summary.continuous_logical_operations, 0);
}

#[test]
fn worker_trace_rejects_self_reported_or_out_of_window_evidence() {
    let baseline = valid_worker_trace();

    let mut changed = baseline.clone();
    changed[1].sequence = 2;
    assert!(validate_worker_trace_cardinality(3, 7, 5, &changed).is_err());

    let mut changed = baseline.clone();
    changed[1].operation = WorkerOperation::Read;
    assert!(validate_worker_trace_cardinality(3, 7, 5, &changed).is_err());

    let mut changed = baseline.clone();
    changed[1].transports[1].command_id += 1;
    assert!(validate_worker_trace_cardinality(3, 7, 5, &changed).is_err());

    let mut changed = baseline.clone();
    changed[1].transports[0].issued_ns = changed[1].candidate_terminal_ns;
    assert!(validate_worker_trace_cardinality(3, 7, 5, &changed).is_err());

    let mut changed = baseline.clone();
    changed[1].transports[0].outcome =
        WorkerTransportOutcome::AcceptedTerminal { terminal_ns: 1_200 };
    assert!(validate_worker_trace_cardinality(3, 7, 5, &changed).is_err());

    let mut changed = baseline.clone();
    changed[2].transports[0].issued_ns = 1_350;
    assert!(validate_worker_trace_cardinality(3, 7, 5, &changed).is_err());

    let mut changed = baseline;
    changed[2].transports[0].outcome =
        WorkerTransportOutcome::AcceptedTerminal { terminal_ns: 2_501 };
    assert!(validate_worker_trace_cardinality(3, 7, 5, &changed).is_err());
}

#[test]
fn canonical_plan_freezes_pressure_fault_update_authority_and_cleanup_order() {
    let scenario: ScenarioDocument = serde_json::from_str(EMBEDDED_SCENARIO_JSON).unwrap();
    let block = 4;
    let plan = build_full_plan(&scenario, block).unwrap();

    let pressure: Vec<_> = plan
        .iter()
        .filter(|action| {
            matches!(
                action.kind,
                PlanKind::Oversize1025Rejected
                    | PlanKind::Boundary1024Accepted
                    | PlanKind::PressureGateClose
                    | PlanKind::PressureAccepted
                    | PlanKind::PressureOverflowRejected
                    | PlanKind::PressureGateOpen
                    | PlanKind::PressureDrain
                    | PlanKind::PressureRetry
            )
        })
        .collect();
    assert_eq!(pressure.len(), 71);
    assert_eq!(pressure[0].kind, PlanKind::Oversize1025Rejected);
    assert_eq!(pressure[0].payload_bytes, Some(1_025));
    assert_eq!(pressure[1].kind, PlanKind::Boundary1024Accepted);
    assert_eq!(pressure[1].payload_bytes, Some(1_024));
    assert_eq!(pressure[2].kind, PlanKind::PressureGateClose);
    assert!(pressure[3..67]
        .iter()
        .all(|action| action.kind == PlanKind::PressureAccepted));
    assert_eq!(pressure[67].kind, PlanKind::PressureOverflowRejected);
    assert_eq!(pressure[68].kind, PlanKind::PressureGateOpen);
    assert_eq!(pressure[69].kind, PlanKind::PressureDrain);
    assert_eq!(pressure[70].kind, PlanKind::PressureRetry);
    assert_eq!(pressure[67].command_id, pressure[70].command_id);
    assert_eq!(pressure[67].payload_sha256, pressure[70].payload_sha256);

    for fault_kind in [PlanKind::TrapFault, PlanKind::CpuFault] {
        let fault_index = plan
            .iter()
            .position(|action| action.kind == fault_kind)
            .unwrap();
        assert_eq!(plan[fault_index - 1].kind, PlanKind::FaultSeed);
        assert_eq!(plan[fault_index + 1].kind, PlanKind::SiblingProbe);
        assert_eq!(plan[fault_index + 2].kind, PlanKind::OldIncarnationStale);
        assert_eq!(plan[fault_index + 3].kind, PlanKind::NewIncarnationMutation);
        assert_eq!(
            plan[fault_index + 4].kind,
            PlanKind::NewIncarnationDuplicate
        );
        for action in &plan[fault_index + 2..=fault_index + 4] {
            assert_eq!(action.command_id, plan[fault_index - 1].command_id);
            assert_eq!(action.payload_sha256, plan[fault_index - 1].payload_sha256);
        }
    }

    let failed_attempt: Vec<_> = plan
        .iter()
        .filter(|action| action.rollout_index == Some(0))
        .collect();
    assert_eq!(failed_attempt.len(), 116);
    for tenant in 0..UPDATE_TARGETS {
        let offset = (tenant * 3) as usize;
        assert_eq!(
            failed_attempt[offset].kind,
            PlanKind::UpdateBaselineSnapshot
        );
        assert_eq!(failed_attempt[offset + 1].kind, PlanKind::UpdateWorkerStart);
        assert_eq!(
            failed_attempt[offset + 2].kind,
            PlanKind::UpdateWorkerAnchorRead
        );
        assert_eq!(failed_attempt[offset + 2].tenant_id, Some(tenant));
        assert_eq!(
            failed_attempt[offset + 2].command_id.as_deref(),
            Some(
                worker_command_id(0, tenant, 0)
                    .unwrap()
                    .to_string()
                    .as_str()
            )
        );
    }
    assert_eq!(failed_attempt[48].kind, PlanKind::RolloutRequest);
    assert_eq!(failed_attempt[49].kind, PlanKind::UpdateWorkerContinuous);
    assert_eq!(failed_attempt[50].kind, PlanKind::RolloutTerminal);
    assert_eq!(
        failed_attempt[50].label.as_deref(),
        Some("failed_rolled_back")
    );
    assert_eq!(failed_attempt[51].kind, PlanKind::UpdateWorkerDrain);
    for tenant in 0..UPDATE_TARGETS {
        let replay = failed_attempt[52 + tenant as usize];
        let anchor = failed_attempt[(tenant * 3 + 2) as usize];
        assert_eq!(replay.kind, PlanKind::UpdateCompletedReplay);
        assert_eq!(replay.tenant_id, Some(tenant));
        assert_eq!(replay.command_id, anchor.command_id);
        assert_eq!(replay.payload_sha256, anchor.payload_sha256);
    }
    for wave in 0..PROOF_WAVES {
        for tenant in 0..UPDATE_TARGETS {
            let action = failed_attempt[68 + (wave * UPDATE_TARGETS + tenant) as usize];
            assert_eq!(action.kind, PlanKind::UpdateProofSnapshot);
            assert_eq!(action.proof_wave, Some(wave));
            assert_eq!(action.tenant_id, Some(tenant));
            assert_eq!(action.label.as_deref(), Some("A"));
            assert_eq!(
                action.command_id.as_deref(),
                Some(
                    proof_command_id(0, tenant, wave)
                        .unwrap()
                        .to_string()
                        .as_str()
                )
            );
        }
    }

    let cleanup: Vec<_> = plan
        .iter()
        .filter(|action| {
            action.cycle == Some(0)
                && matches!(
                    action.kind,
                    PlanKind::CleanupCreate
                        | PlanKind::CleanupIncrement
                        | PlanKind::CleanupQuiesce
                        | PlanKind::CleanupSnapshot
                        | PlanKind::CleanupTeardown
                        | PlanKind::CleanupStaleProbe
                        | PlanKind::CleanupActiveZero
                        | PlanKind::CleanupPostRss
                )
        })
        .collect();
    assert_eq!(cleanup.len(), 165);
    assert!(cleanup[..32]
        .iter()
        .all(|action| action.kind == PlanKind::CleanupCreate));
    assert!(cleanup[32..64]
        .iter()
        .all(|action| action.kind == PlanKind::CleanupIncrement));
    assert_eq!(cleanup[64].kind, PlanKind::CleanupQuiesce);
    assert!(cleanup[65..97]
        .iter()
        .all(|action| action.kind == PlanKind::CleanupSnapshot));
    assert!(cleanup[97..129]
        .iter()
        .all(|action| action.kind == PlanKind::CleanupTeardown));
    assert!(cleanup[129..161]
        .iter()
        .all(|action| action.kind == PlanKind::CleanupStaleProbe));
    for stale in &cleanup[129..161] {
        let increment = cleanup[32..64]
            .iter()
            .find(|action| action.tenant_id == stale.tenant_id)
            .unwrap();
        assert_eq!(stale.command_id, increment.command_id);
        assert_eq!(stale.payload_sha256, increment.payload_sha256);
    }
    assert_eq!(cleanup[161].kind, PlanKind::CleanupActiveZero);
    assert_eq!(
        cleanup[162..]
            .iter()
            .map(|action| action.sample_offset_ms)
            .collect::<Vec<_>>(),
        vec![Some(1_000), Some(1_100), Some(1_200)]
    );
}

#[test]
fn every_candidate_runs_all_six_authority_probes_without_applicability_skips() {
    let scenario: ScenarioDocument = serde_json::from_str(EMBEDDED_SCENARIO_JSON).unwrap();
    for block in 0..BLOCK_COUNT {
        let plan = build_full_plan(&scenario, block).unwrap();
        let canaries: Vec<_> = plan
            .iter()
            .filter(|action| action.kind == PlanKind::AuthorityCanary)
            .map(|action| action.label.as_deref().unwrap())
            .collect();
        let expected: Vec<_> = (0..AUTHORITY_CANARY_IDS.len())
            .map(|offset| {
                AUTHORITY_CANARY_IDS[(block as usize + offset) % AUTHORITY_CANARY_IDS.len()]
            })
            .collect();
        assert_eq!(canaries, expected);
        assert_eq!(block_order(block).unwrap().candidates.len(), 3);
    }
}

#[test]
fn canonical_bundle_detects_kind_order_retention_and_payload_digest_mutations() {
    let scenario: ScenarioDocument = serde_json::from_str(EMBEDDED_SCENARIO_JSON).unwrap();
    let mut plans = (0..BLOCK_COUNT)
        .map(|block| build_full_plan(&scenario, block))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let baseline = hash_canonical_plan_bundle(&plans).unwrap();
    assert_eq!(baseline, EMBEDDED_PLAN_BUNDLE_SHA256);
    let action_index = plans[1]
        .iter()
        .position(|action| action.kind == PlanKind::NormalIncrement)
        .unwrap();

    let original_kind = plans[1][action_index].kind;
    plans[1][action_index].kind = PlanKind::CrossTenantFirst;
    assert_ne!(hash_canonical_plan_bundle(&plans).unwrap(), baseline);
    assert!(verify_full_shape(&plans[1]).is_err());
    plans[1][action_index].kind = original_kind;

    plans[1].swap(action_index, action_index + 1);
    assert_ne!(hash_canonical_plan_bundle(&plans).unwrap(), baseline);
    assert!(verify_full_shape(&plans[1]).is_err());
    plans[1].swap(action_index, action_index + 1);

    plans[1][action_index].retained = false;
    assert_ne!(hash_canonical_plan_bundle(&plans).unwrap(), baseline);
    assert!(verify_full_shape(&plans[1]).is_err());
    plans[1][action_index].retained = true;

    let original_digest = plans[1][action_index].payload_sha256.clone();
    plans[1][action_index].payload_sha256 = Some("0".repeat(64));
    assert_ne!(hash_canonical_plan_bundle(&plans).unwrap(), baseline);
    assert!(verify_full_shape(&plans[1]).is_err());
    plans[1][action_index].payload_sha256 = original_digest;

    assert_eq!(hash_canonical_plan_bundle(&plans).unwrap(), baseline);
    assert!(verify_full_shape(&plans[1]).is_ok());
}

#[test]
fn state_oracle_rejects_reject_all_forgery_and_cross_tenant_cache() {
    let mut oracle = StateOracle::new();
    oracle.create(TenantId(0), 0, "A", "build-a").unwrap();
    oracle.create(TenantId(1), 0, "A", "build-a").unwrap();
    let first = command(1, 0, 0, 7);
    oracle.begin_command(first.clone()).unwrap();
    assert!(oracle
        .observe_admission(1, AdmissionOutcome::RetryableRejection)
        .is_err());

    let mut oracle = StateOracle::new();
    oracle.create(TenantId(0), 0, "A", "build-a").unwrap();
    oracle.create(TenantId(1), 0, "A", "build-a").unwrap();
    oracle.begin_command(first.clone()).unwrap();
    oracle
        .observe_admission(1, AdmissionOutcome::Accepted)
        .unwrap();
    let expected = oracle.expected_result(&first).unwrap();
    let mut forged = expected.clone();
    forged.counter += 1;
    assert!(oracle.observe_completed(1, &forged).is_err());

    let mut oracle = StateOracle::new();
    oracle.create(TenantId(0), 0, "A", "build-a").unwrap();
    oracle.create(TenantId(1), 0, "A", "build-a").unwrap();
    oracle.begin_command(first.clone()).unwrap();
    oracle
        .observe_admission(1, AdmissionOutcome::Accepted)
        .unwrap();
    let result = oracle.expected_result(&first).unwrap();
    oracle.observe_completed(1, &result).unwrap();
    let other = command(2, 1, 0, 7);
    oracle.begin_command(other.clone()).unwrap();
    oracle
        .observe_admission(2, AdmissionOutcome::Accepted)
        .unwrap();
    assert!(!oracle.expected_result(&other).unwrap().deduplicated);
}

#[test]
fn command_id_fingerprint_conflicts_while_pending_and_after_cache() {
    let mut oracle = StateOracle::new();
    oracle.create(TenantId(0), 0, "A", "build-a").unwrap();
    let first = command(1, 0, 0, 42);
    oracle.begin_command(first.clone()).unwrap();
    oracle
        .observe_admission(1, AdmissionOutcome::Accepted)
        .unwrap();

    let mut pending_conflict = command(2, 0, 0, 42);
    pending_conflict.operation = ModelOperation::Read;
    oracle.begin_command(pending_conflict).unwrap();
    oracle
        .observe_admission(2, AdmissionOutcome::CommandIdConflict)
        .unwrap();

    let expected = oracle.expected_result(&first).unwrap();
    oracle.observe_completed(1, &expected).unwrap();
    let mut cached_conflict = command(3, 0, 0, 42);
    cached_conflict.payload[0] = 0;
    cached_conflict.payload_sha256 = sha256_hex(&cached_conflict.payload);
    oracle.begin_command(cached_conflict).unwrap();
    oracle
        .observe_admission(3, AdmissionOutcome::CommandIdConflict)
        .unwrap();

    let duplicate = command(4, 0, 0, 42);
    oracle.begin_command(duplicate.clone()).unwrap();
    oracle
        .observe_admission(4, AdmissionOutcome::Accepted)
        .unwrap();
    let duplicate_result = oracle.expected_result(&duplicate).unwrap();
    assert!(duplicate_result.deduplicated);
    oracle.observe_completed(4, &duplicate_result).unwrap();
}

#[test]
fn stale_cache_and_candidate_owned_incarnations_fail() {
    let mut oracle = StateOracle::new();
    assert!(oracle.create(TenantId(2), 9, "A", "a").is_err());
    oracle.create(TenantId(2), 0, "A", "a").unwrap();
    let old = command(1, 2, 0, 11);
    oracle.begin_command(old.clone()).unwrap();
    oracle
        .observe_admission(1, AdmissionOutcome::Accepted)
        .unwrap();
    let result = oracle.expected_result(&old).unwrap();
    oracle.observe_completed(1, &result).unwrap();
    oracle.recover(TenantId(2), 1).unwrap();
    let stale = command(2, 2, 0, 11);
    oracle.begin_command(stale).unwrap();
    assert!(oracle
        .observe_admission(2, AdmissionOutcome::Accepted)
        .is_err());
    oracle.teardown(TenantId(2), 1).unwrap();
    let missing = command(3, 2, 0, 11);
    oracle.begin_command(missing).unwrap();
    oracle
        .observe_admission(3, AdmissionOutcome::TenantMissing)
        .unwrap();
    assert!(oracle.recover(TenantId(2), 2).is_err());
}

#[test]
fn pressure_and_oversize_have_only_the_frozen_rejections() {
    let mut oracle = StateOracle::new();
    oracle.create(TenantId(0), 0, "A", "a").unwrap();
    oracle.set_dequeue_gate(TenantId(0), 0, true).unwrap();
    let mut accepted = Vec::new();
    for request_id in 1..=64 {
        let command = command(request_id, 0, 0, request_id);
        oracle.begin_command(command.clone()).unwrap();
        oracle
            .observe_admission(request_id, AdmissionOutcome::Accepted)
            .unwrap();
        accepted.push(command);
    }
    assert_eq!(oracle.accepted_outstanding(TenantId(0)), 64);

    let overflow = command(65, 0, 0, 65);
    oracle.begin_command(overflow).unwrap();
    oracle
        .observe_admission(65, AdmissionOutcome::RetryableRejection)
        .unwrap();

    oracle.set_dequeue_gate(TenantId(0), 0, false).unwrap();
    for command in accepted {
        let expected = oracle.expected_result(&command).unwrap();
        oracle
            .observe_completed(command.request_id, &expected)
            .unwrap();
    }
    let mut oversize = command(66, 0, 0, 66);
    oversize.payload = vec![0x5a; 1_025];
    oversize.payload_sha256 = sha256_hex(&oversize.payload);
    oracle.begin_command(oversize).unwrap();
    oracle
        .observe_admission(66, AdmissionOutcome::Oversize)
        .unwrap();
}

#[test]
fn version_intersection_catches_rollout_liar_and_rollback_no_activation() {
    let mut oracle = StateOracle::new();
    oracle.create(TenantId(0), 0, "A", "a").unwrap();
    let before = command(1, 0, 0, 9);
    oracle.begin_command(before.clone()).unwrap();
    oracle
        .observe_admission(1, AdmissionOutcome::Accepted)
        .unwrap();
    oracle
        .begin_rollout(&[TenantId(0)], "B", "b", false)
        .unwrap();
    let mut lie = oracle.expected_result(&before).unwrap();
    lie.logical_version = "B".into();
    lie.build_id = "b".into();
    assert!(oracle.observe_completed(1, &lie).is_err());
    // A failed rollout returns the receive set to A; a request sent during the
    // attempt may therefore only complete as A after rollback.
    oracle.finish_rollout(&[TenantId(0)], "A", "a").unwrap();
}

#[test]
fn deadline_heap_expires_other_outstanding_requests_and_rejects_late_terminal() {
    let mut deadlines = DeadlineTracker::default();
    deadlines.register(1, 0, 100).unwrap();
    deadlines.register(2, 0, 50).unwrap();
    deadlines.accepted(1, 10, 200).unwrap();
    assert_eq!(deadlines.expire(60), vec![(2, DeadlinePhase::Admission)]);
    assert!(deadlines.terminal(2).is_err());
    assert_eq!(deadlines.expire(210), vec![(1, DeadlinePhase::Terminal)]);
}

#[test]
fn instant_or_host_synthesized_cpu_proofs_fail() {
    assert!(validate_cpu_observation(
        0,
        1,
        2,
        42_000_002,
        100_000_000,
        CpuStartOrigin::GuestFirstActionObserver
    )
    .is_ok());
    assert!(
        validate_cpu_observation(0, 1, 2, 3, 4, CpuStartOrigin::GuestFirstActionObserver).is_err()
    );
    assert!(validate_cpu_observation(
        0,
        1,
        2,
        42_000_002,
        100_000_000,
        CpuStartOrigin::HostSynthesized
    )
    .is_err());
}

#[test]
fn authority_canary_requires_production_denial_evidence_and_zero_effect_tail() {
    let absent = AuthorityCanaryEvidence {
        terminal: AuthorityProbeTerminalEvent {
            attempt_id: DecimalU64::from(1),
            probe: AuthorityProbeKind::LunaticTcp,
            artifact_sha256: "a".repeat(64),
            parameter_sha256: "b".repeat(64),
            production_policy_sha256: "c".repeat(64),
            stage: AuthorityProbeStage::Instantiate,
            result: AuthorityProbeResult::AbsentAtLink,
            error_class: AuthorityProbeErrorClass::UnknownImport,
            guest_return_sha256: EMPTY_SHA256.into(),
        },
        positive_control_detected: true,
        tail_complete: true,
        tail_elapsed_ms: 500,
        secret_or_digest_observed: false,
        external_effect_observed: false,
        sibling_probe_succeeded: true,
        production_linker_inventory_contains_exact_import: false,
        actual_link_failure_observed: true,
        typed_production_policy_denial_observed: false,
    };
    assert!(validate_authority_canary(&absent).is_ok());

    let mut forged = absent.clone();
    forged.production_linker_inventory_contains_exact_import = true;
    assert!(validate_authority_canary(&forged).is_err());
    let mut forged = absent.clone();
    forged.tail_elapsed_ms = 499;
    assert!(validate_authority_canary(&forged).is_err());
    let mut forged = absent.clone();
    forged.external_effect_observed = true;
    assert!(validate_authority_canary(&forged).is_err());

    let mut denied = absent;
    denied.terminal.probe = AuthorityProbeKind::ExtismHttp;
    denied.terminal.stage = AuthorityProbeStage::Invoke;
    denied.terminal.result = AuthorityProbeResult::PolicyDenied;
    denied.terminal.error_class = AuthorityProbeErrorClass::HttpCapabilityDenied;
    denied.production_linker_inventory_contains_exact_import = true;
    denied.actual_link_failure_observed = false;
    denied.typed_production_policy_denial_observed = true;
    assert!(validate_authority_canary(&denied).is_ok());
    denied.terminal.error_class = AuthorityProbeErrorClass::NetworkCapabilityDenied;
    assert!(validate_authority_canary(&denied).is_err());
}
