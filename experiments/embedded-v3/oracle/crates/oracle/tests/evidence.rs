use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use embedded_v3_oracle::analysis::KeyMetrics;
use embedded_v3_oracle::evidence::{
    verify_file, ArtifactEvidence, CandidateBuildEvidence, CompletedCounts, CompletedMetrics,
    CompletedOutcome, CompletedStatus, EffortEvidence, EmbeddedWasmEngineEvidence,
    EnvironmentEvidence, EvidenceError, ExecutionTopologyEvidence, FailurePhase, GateResult,
    GuestArtifactsEvidence, ImmutableEvidenceDirectory, IncompleteOutcome, IncompleteStatus,
    MandatoryGates, MetricEvidence, NormalMetricEvidence, OracleEvidence, OrderedTraceSink,
    PartialCounts, ProcessExitEvidence, ProtocolAudit, RawEvidenceFiles, RawRunSummary,
    RelativeEvidencePath, RetryAudit, RssEvidence, RunFailure, RunManifest, ScenarioEvidence,
    Sha256Digest, TenantExecutionModel, EVIDENCE_SCHEMA_VERSION, EXPERIMENT_ID,
};
use embedded_v3_oracle::protocol::CandidateKind;
use embedded_v3_oracle::workload::{
    block_order, build_full_plan, verify_full_shape, PlanKind, ScenarioDocument,
    EMBEDDED_PLAN_BUNDLE_SHA256, EMBEDDED_SCENARIO_SHA256,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(label: &str) -> Self {
        let unique = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "embedded-v3-evidence-{label}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn digest(byte: char) -> Sha256Digest {
    Sha256Digest::parse(byte.to_string().repeat(64)).unwrap()
}

fn path(value: &str) -> RelativeEvidencePath {
    RelativeEvidencePath::parse(value).unwrap()
}

fn artifact(value: &str, _byte: char, bytes: u64) -> ArtifactEvidence {
    let content = vec![b'x'; bytes as usize];
    ArtifactEvidence {
        path: path(value),
        sha256: Sha256Digest::parse(
            Sha256::digest(content)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        )
        .unwrap(),
        bytes,
    }
}

fn sample_manifest(run_id: &str) -> RunManifest {
    let authority_policy = artifact("authority-policy.json", '1', 30);
    let source_review = artifact("source-review.json", '2', 40);
    let oracle_source_snapshot = artifact("oracle/source-snapshot.tar", '4', 60);
    let source_snapshot = artifact("candidate/source-snapshot.tar", '9', 50);
    RunManifest {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        experiment_id: EXPERIMENT_ID.into(),
        run_id: run_id.into(),
        block_index: 1,
        retained: true,
        candidate: CandidateKind::Lunatic,
        candidate_order_position: 2,
        freeze_utc: "2026-07-23T00:00:00Z".into(),
        plan_bundle_sha256: Sha256Digest::parse(EMBEDDED_PLAN_BUNDLE_SHA256).unwrap(),
        authority_policy_sha256: authority_policy.sha256.clone(),
        authority_policy,
        source_review_sha256: source_review.sha256.clone(),
        source_review,
        scenario: ScenarioEvidence {
            path: path("scenario-final.json"),
            sha256: Sha256Digest::parse(EMBEDDED_SCENARIO_SHA256).unwrap(),
            schema_path: path("schemas/scenario-final.schema.json"),
            schema_sha256: digest('3'),
        },
        oracle: OracleEvidence {
            revision: oracle_source_snapshot.sha256.as_str().to_owned(),
            source_tree_sha256: oracle_source_snapshot.sha256.clone(),
            source_snapshot: oracle_source_snapshot,
            executable: artifact("oracle.exe", '5', 100),
            protocol_source_sha256: digest('6'),
            raw_summary_schema_sha256: digest('7'),
            run_manifest_schema_sha256: digest('8'),
        },
        candidate_build: CandidateBuildEvidence {
            source_revision: source_snapshot.sha256.as_str().to_owned(),
            source_tree_sha256: source_snapshot.sha256.clone(),
            source_snapshot,
            executable: artifact("candidate.exe", 'b', 200),
            argv: vec!["candidate.exe".into()],
            cargo_lock: artifact("Cargo.lock", 'c', 10),
            sloc_manifest: artifact("sloc-manifest.json", 'd', 10),
            sloc_result: artifact("sloc-result.json", 'e', 10),
            production_sloc: 500,
            direct_dependencies: vec!["serde@1.0.229".into()],
            transitive_package_count: 12,
            binary_bytes: 200,
            execution_topology: ExecutionTopologyEvidence {
                os_processes: 1,
                long_lived_helper_processes: 0,
                tenant_execution_model: TenantExecutionModel::TokioTasks,
                tokio_worker_threads: 4,
                dedicated_tenant_threads: 0,
            },
            embedded_wasm_engine: EmbeddedWasmEngineEvidence {
                implementation: "wasmtime".into(),
                version: "46.0.1".into(),
            },
            build_command: "cargo build --release --locked".into(),
        },
        guest_artifacts: GuestArtifactsEvidence {
            a: artifact("A.wasm", '1', 10),
            b: artifact("B.wasm", '2', 10),
            bad_a: artifact("bad-A.wasm", '3', 10),
            bad_b: artifact("bad-B.wasm", '4', 10),
            authority: artifact("authority.json", '5', 10),
        },
        environment: EnvironmentEvidence {
            os: "Windows 11 Home 10.0.26200 build 26200".into(),
            cpu: "Intel Core i7-14700K".into(),
            logical_processors: 28,
            physical_memory_bytes: 68_475_179_008,
            rustc: "1.95.0".into(),
            cargo: "1.95.0".into(),
            oracle_wasmtime: "46.0.1".into(),
            tokio: "1.53.1".into(),
            node: "24.4.1".into(),
            machine_fingerprint_sha256: digest('6'),
        },
        effort: EffortEvidence {
            checkpoint_elapsed_minutes: 30,
            completion_elapsed_minutes: 60,
            tuning_elapsed_minutes: 10,
            tuning_revisions: 1,
            architecture_review_sha256: digest('7'),
        },
    }
}

fn set_candidate(manifest: &mut RunManifest, candidate: CandidateKind) {
    manifest.candidate = candidate;
    manifest.candidate_order_position = block_order(manifest.block_index)
        .unwrap()
        .candidates
        .iter()
        .position(|ordered| *ordered == candidate)
        .unwrap() as u32;
    match candidate {
        CandidateKind::Lunatic | CandidateKind::RawWasmtime => {
            manifest.candidate_build.execution_topology = ExecutionTopologyEvidence {
                os_processes: 1,
                long_lived_helper_processes: 0,
                tenant_execution_model: TenantExecutionModel::TokioTasks,
                tokio_worker_threads: 4,
                dedicated_tenant_threads: 0,
            };
            manifest.candidate_build.embedded_wasm_engine.version = "46.0.1".into();
        }
        CandidateKind::Extism => {
            manifest.candidate_build.execution_topology = ExecutionTopologyEvidence {
                os_processes: 1,
                long_lived_helper_processes: 0,
                tenant_execution_model: TenantExecutionModel::DedicatedThreads,
                tokio_worker_threads: 0,
                dedicated_tenant_threads: 32,
            };
            manifest.candidate_build.embedded_wasm_engine.version = "43.0.2".into();
        }
    }
}

fn key_set(object: &serde_json::Map<String, Value>) -> BTreeSet<String> {
    object.keys().cloned().collect()
}

fn string_set(values: &Value) -> BTreeSet<String> {
    values
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_owned())
        .collect()
}

fn assert_required_and_only_properties(value: &Value, schema: &Value) {
    let object = value.as_object().unwrap();
    let required = string_set(&schema["required"]);
    let properties = key_set(schema["properties"].as_object().unwrap());
    assert_eq!(required, properties);
    assert_eq!(key_set(object), required);
}

fn create_run(root: &Path, run_id: &str) -> ImmutableEvidenceDirectory {
    ImmutableEvidenceDirectory::create(root, sample_manifest(run_id)).unwrap()
}

fn populate_bound_artifacts(run: &ImmutableEvidenceDirectory) {
    for artifact in run.manifest().bound_artifacts() {
        run.write_bound_artifact(artifact, &vec![b'x'; artifact.bytes as usize])
            .unwrap();
    }
    run.verify_bound_artifacts().unwrap();
}

fn metric(p99: u64) -> MetricEvidence {
    MetricEvidence {
        sample_count: 30,
        minimum_ns: p99 - 1,
        nearest_rank_p99_ns: p99,
        maximum_ns: p99 + 1,
        slo_min_ns: None,
        slo_max_ns: p99 + 10,
        pass: true,
    }
}

fn gate(label: &str) -> GateResult {
    GateResult {
        pass: true,
        evidence_refs: vec![path(label)],
    }
}

fn completed_outcome() -> CompletedOutcome {
    CompletedOutcome {
        status: CompletedStatus::Completed,
        overall_pass: true,
        counts: CompletedCounts {
            warmup_commands: 1_024,
            normal_original_commands: 10_240,
            delayed_duplicate_cases: 80,
            delayed_duplicate_requests: 240,
            pressure_submissions: 68,
            pressure_initial_accepted: 64,
            pressure_retryable_rejected: 1,
            pressure_retries: 1,
            fault_seed_commands: 60,
            trap_faults: 30,
            cpu_faults: 30,
            fault_post_recovery_cases: 60,
            fault_post_recovery_requests: 180,
            sibling_probes: 60,
            update_failed_attempts: 30,
            update_valid_attempts: 30,
            update_proof_probes: 2_880,
            update_worker_attempts: 960,
            authority_canaries: 6,
            cleanup_warmup_cycles: 5,
            cleanup_retained_cycles: 30,
            cleanup_old_endpoint_probes: 1_120,
            accepted_commands: 1,
            terminal_accepted_commands: 1,
        },
        gates: MandatoryGates {
            isolation: gate("isolation.ndjson"),
            authority: gate("authority.ndjson"),
            admission: gate("admission.ndjson"),
            recovery: gate("recovery.ndjson"),
            update_safety: gate("update-safety.ndjson"),
            state_semantics: gate("state-semantics.ndjson"),
            cleanup: gate("cleanup.ndjson"),
        },
        metrics_ns: CompletedMetrics {
            shared_init_ns: 101,
            warm_create: MetricEvidence {
                sample_count: 32,
                ..metric(102)
            },
            normal: NormalMetricEvidence {
                sample_count: 10_240,
                minimum_ns: 102,
                nearest_rank_p99_ns: 103,
                maximum_ns: 104,
                total_ns: 104_000,
                slo_min_ns: None,
                slo_max_ns: 200,
                pass: true,
            },
            pressure_admission: MetricEvidence {
                sample_count: 64,
                ..metric(105)
            },
            sibling: MetricEvidence {
                sample_count: 60,
                ..metric(106)
            },
            trap_replacement_ready: metric(107),
            cpu_replacement_ready: metric(108),
            cpu_failed_terminal: metric(109),
            failed_update_unavailability: MetricEvidence {
                sample_count: 480,
                ..metric(110)
            },
            valid_update_unavailability: MetricEvidence {
                sample_count: 480,
                ..metric(111)
            },
            failed_rollout: metric(112),
            valid_rollout: metric(113),
            failed_mixed_window: metric(114),
            valid_mixed_window: metric(115),
        },
        rss: RssEvidence {
            units: "bytes".into(),
            process_tree_complete: true,
            minimal_host_stable_median_bytes: 1_000,
            shared_init_stable_median_bytes: 1_100,
            ready_32_stable_median_bytes: 1_200,
            ready_delta_bytes: 200,
            phase_peak_bytes: 1_300,
            peak_delta_bytes: 300,
            cleanup_delta_bytes: vec![0; 30],
            maximum_post_cleanup_delta_bytes: 0,
            cleanup_slope_bytes_per_cycle: 0.0,
            pass: true,
        },
        retry_audit: RetryAudit {
            delayed_duplicate_cases: 80,
            delayed_duplicate_requests: 240,
            pressure_retries: 1,
            update_worker_retries: 0,
            oracle_transport_retries: 0,
            candidate_hidden_retries: 0,
            unauthorized_retries: 0,
        },
        protocol_audit: ProtocolAudit {
            violations: 0,
            invalid_json_lines: 0,
            late_events: 0,
            unexpected_rejections: 0,
            timeouts: 0,
            nonpositive_durations: 0,
        },
    }
}

#[test]
fn manifest_serialization_has_every_schema_required_field_and_binding() {
    let manifest = sample_manifest("schema-run");
    manifest.validate().unwrap();
    let value = serde_json::to_value(&manifest).unwrap();
    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/run-manifest.schema.json")).unwrap();

    assert_required_and_only_properties(&value, &schema);
    for field in [
        "scenario",
        "oracle",
        "candidate_build",
        "guest_artifacts",
        "environment",
        "effort",
    ] {
        assert_required_and_only_properties(&value[field], &schema["properties"][field]);
    }
    assert_required_and_only_properties(
        &value["candidate_build"]["execution_topology"],
        &schema["$defs"]["execution_topology"],
    );
    assert_required_and_only_properties(
        &value["candidate_build"]["embedded_wasm_engine"],
        &schema["$defs"]["embedded_wasm_engine"],
    );
    assert_eq!(
        value["plan_bundle_sha256"],
        Value::from(EMBEDDED_PLAN_BUNDLE_SHA256)
    );
    assert_eq!(
        value["authority_policy_sha256"],
        value["authority_policy"]["sha256"]
    );
    assert_eq!(
        value["source_review_sha256"],
        value["source_review"]["sha256"]
    );

    let artifact_keys =
        BTreeSet::from(["bytes".to_owned(), "path".to_owned(), "sha256".to_owned()]);
    assert_eq!(
        key_set(value["candidate_build"]["executable"].as_object().unwrap()),
        artifact_keys
    );
    assert_eq!(
        key_set(value["oracle"]["source_snapshot"].as_object().unwrap()),
        artifact_keys
    );
    assert_eq!(
        key_set(
            value["candidate_build"]["source_snapshot"]
                .as_object()
                .unwrap()
        ),
        artifact_keys
    );
}

#[test]
fn run_directory_and_each_evidence_name_are_create_new_and_tamper_evident() {
    let temporary = TestDirectory::new("immutable");
    let run = create_run(&temporary.path, "immutable-run");
    run.verify_manifest().unwrap();
    populate_bound_artifacts(&run);

    let second =
        ImmutableEvidenceDirectory::create(&temporary.path, sample_manifest("immutable-run"));
    assert!(matches!(second, Err(EvidenceError::Io(_))));

    let first = run
        .write_evidence(path("stream-trace.ndjson"), b"{\"trace_seq\":1}\n")
        .unwrap();
    assert!(matches!(
        run.write_evidence(path("stream-trace.ndjson"), b"replacement"),
        Err(EvidenceError::Io(_))
    ));
    verify_file(run.path(), &first).unwrap();

    let mut policy = OpenOptions::new()
        .append(true)
        .open(run.path().join("authority-policy.json"))
        .unwrap();
    policy.write_all(b"tamper").unwrap();
    policy.flush().unwrap();
    assert!(matches!(
        run.verify_bound_artifacts(),
        Err(EvidenceError::Tampered(_))
    ));

    let mut manifest = OpenOptions::new()
        .append(true)
        .open(run.path().join("run-manifest.json"))
        .unwrap();
    manifest.write_all(b"tamper").unwrap();
    manifest.flush().unwrap();
    assert!(matches!(
        run.verify_manifest(),
        Err(EvidenceError::Tampered(_))
    ));
}

#[test]
fn ordered_trace_is_contiguous_under_concurrency_and_finalization_is_terminal() {
    let temporary = TestDirectory::new("trace");
    let trace_path = temporary.path.join("trace.ndjson");
    let sink = OrderedTraceSink::create(&trace_path).unwrap();
    assert!(matches!(
        OrderedTraceSink::create(&trace_path),
        Err(EvidenceError::Io(_))
    ));

    let mut workers = Vec::new();
    for producer in 0..8_u64 {
        let sink = sink.clone();
        workers.push(thread::spawn(move || {
            for local in 0..50_u64 {
                sink.record(&json!({"kind": "test", "producer": producer, "local": local}))
                    .unwrap();
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }

    let finalization = sink.finalize().unwrap();
    assert_eq!(finalization.records, 400);
    assert!(matches!(
        sink.finalize(),
        Err(EvidenceError::TraceFinalized)
    ));
    assert!(matches!(
        sink.record(&json!({"kind": "late"})),
        Err(EvidenceError::TraceFinalized)
    ));

    let bytes = fs::read(&trace_path).unwrap();
    assert_eq!(bytes.len() as u64, finalization.bytes);
    let records: Vec<Value> = String::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 400);
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record["trace_seq"], Value::from(index as u64 + 1));
    }
}

#[test]
fn raw_incomplete_summary_binds_manifest_and_all_evidence_then_finalizes_once() {
    let temporary = TestDirectory::new("summary");
    let run = create_run(&temporary.path, "summary-run");
    populate_bound_artifacts(&run);
    let stream_trace = run
        .write_evidence(path("stream-trace.ndjson"), b"{\"trace_seq\":1}\n")
        .unwrap();
    let rss_trace = run.write_evidence(path("rss-trace.ndjson"), b"").unwrap();
    let filesystem_observer_trace = run
        .write_evidence(path("filesystem-observer.ndjson"), b"")
        .unwrap();
    let network_observer_trace = run
        .write_evidence(path("network-observer.ndjson"), b"")
        .unwrap();
    let stderr_trace = run.write_evidence(path("stderr.ndjson"), b"").unwrap();
    let summary = RawRunSummary {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        experiment_id: EXPERIMENT_ID.into(),
        run_id: "summary-run".into(),
        block_index: 1,
        retained: true,
        candidate: CandidateKind::Lunatic,
        scenario_sha256: Sha256Digest::parse(EMBEDDED_SCENARIO_SHA256).unwrap(),
        manifest_sha256: run.manifest_evidence().sha256.clone(),
        started_monotonic_ns: 1,
        finished_monotonic_ns: 3,
        process_exit: ProcessExitEvidence {
            observed_monotonic_ns: 2,
            code: Some(1),
            success: false,
        },
        outcome: IncompleteOutcome {
            status: IncompleteStatus::Failed,
            failure: RunFailure {
                phase: FailurePhase::Normal,
                code: "candidate_failed".into(),
                message: "candidate terminated".into(),
                request_id: Some(10),
            },
            partial_counts: PartialCounts {
                controls_sent: 10,
                events_received: 9,
            },
            protocol_violations: 0,
            hidden_retry_count: 0,
        },
        evidence: RawEvidenceFiles {
            run_manifest: run.manifest_evidence().clone(),
            stream_trace,
            rss_trace,
            filesystem_observer_trace,
            network_observer_trace,
            stderr_trace,
        },
    };
    summary.validate(run.manifest()).unwrap();

    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/raw-summary.schema.json")).unwrap();
    let value = serde_json::to_value(&summary).unwrap();
    assert_required_and_only_properties(&value, &schema);
    assert_required_and_only_properties(&value["evidence"], &schema["properties"]["evidence"]);
    assert_required_and_only_properties(&value["outcome"], &schema["$defs"]["incomplete_outcome"]);

    let summary_evidence = run.write_incomplete_summary(&summary).unwrap();
    verify_file(run.path(), &summary_evidence).unwrap();
    assert!(matches!(
        run.write_incomplete_summary(&summary),
        Err(EvidenceError::Io(_))
    ));

    let mut stream = OpenOptions::new()
        .append(true)
        .open(run.path().join("stream-trace.ndjson"))
        .unwrap();
    stream.write_all(b"tampered\n").unwrap();
    stream.flush().unwrap();
    assert!(matches!(
        run.write_incomplete_summary(&RawRunSummary {
            run_id: "summary-run".into(),
            ..summary
        }),
        Err(EvidenceError::Tampered(_))
    ));
}

#[test]
fn completed_gate_refs_resolve_to_verified_immutable_raw_files() {
    let temporary = TestDirectory::new("completed-gates");
    let run = create_run(&temporary.path, "completed-gates-run");
    populate_bound_artifacts(&run);
    let stream_trace = run
        .write_evidence(path("stream-trace.ndjson"), b"{\"trace_seq\":1}\n")
        .unwrap();
    let rss_trace = run
        .write_evidence(path("rss-trace.ndjson"), b"{}\n")
        .unwrap();
    let filesystem_observer_trace = run
        .write_evidence(path("filesystem-observer.ndjson"), b"{}\n")
        .unwrap();
    let network_observer_trace = run
        .write_evidence(path("network-observer.ndjson"), b"{}\n")
        .unwrap();
    let stderr_trace = run.write_evidence(path("stderr.ndjson"), b"\n").unwrap();
    let mut outcome = completed_outcome();
    for gate in [
        &mut outcome.gates.isolation,
        &mut outcome.gates.authority,
        &mut outcome.gates.admission,
        &mut outcome.gates.recovery,
        &mut outcome.gates.update_safety,
        &mut outcome.gates.state_semantics,
        &mut outcome.gates.cleanup,
    ] {
        gate.evidence_refs = vec![path("stream-trace.ndjson")];
    }
    let summary = RawRunSummary {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        experiment_id: EXPERIMENT_ID.into(),
        run_id: "completed-gates-run".into(),
        block_index: 1,
        retained: true,
        candidate: CandidateKind::Lunatic,
        scenario_sha256: Sha256Digest::parse(EMBEDDED_SCENARIO_SHA256).unwrap(),
        manifest_sha256: run.manifest_evidence().sha256.clone(),
        started_monotonic_ns: 1,
        finished_monotonic_ns: 3,
        process_exit: ProcessExitEvidence {
            observed_monotonic_ns: 2,
            code: Some(0),
            success: true,
        },
        outcome,
        evidence: RawEvidenceFiles {
            run_manifest: run.manifest_evidence().clone(),
            stream_trace,
            rss_trace,
            filesystem_observer_trace,
            network_observer_trace,
            stderr_trace,
        },
    };
    summary.validate(run.manifest()).unwrap();

    let mut unresolved = summary.clone();
    unresolved.outcome.gates.cleanup.evidence_refs = vec![path("unbound.ndjson")];
    assert!(matches!(
        unresolved.validate(run.manifest()),
        Err(EvidenceError::Invariant(_))
    ));

    let mut forged = summary.clone();
    forged.evidence.stream_trace.sha256 = digest('f');
    assert!(matches!(
        run.write_completed_summary(&forged),
        Err(EvidenceError::Tampered(_))
    ));

    let summary_evidence = run.write_completed_summary(&summary).unwrap();
    verify_file(run.path(), &summary_evidence).unwrap();
}

#[test]
fn every_completed_count_const_matches_the_executable_plan() {
    let scenario = ScenarioDocument::embedded().unwrap();
    let plan = build_full_plan(&scenario, 0).unwrap();
    let shape = verify_full_shape(&plan).unwrap();
    let count = |kind: PlanKind| plan.iter().filter(|action| action.kind == kind).count() as u64;
    let expected = BTreeMap::from([
        (
            "warmup_commands".to_owned(),
            count(PlanKind::NormalWarmupIncrement),
        ),
        (
            "normal_original_commands".to_owned(),
            count(PlanKind::NormalIncrement),
        ),
        (
            "delayed_duplicate_cases".to_owned(),
            count(PlanKind::DelayedOriginalDuplicate),
        ),
        (
            "delayed_duplicate_requests".to_owned(),
            count(PlanKind::DelayedOriginalDuplicate)
                + count(PlanKind::CrossTenantFirst)
                + count(PlanKind::CrossTenantDuplicate),
        ),
        (
            "pressure_submissions".to_owned(),
            count(PlanKind::Oversize1025Rejected)
                + count(PlanKind::Boundary1024Accepted)
                + count(PlanKind::PressureAccepted)
                + count(PlanKind::PressureOverflowRejected)
                + count(PlanKind::PressureRetry),
        ),
        (
            "pressure_initial_accepted".to_owned(),
            count(PlanKind::PressureAccepted),
        ),
        (
            "pressure_retryable_rejected".to_owned(),
            count(PlanKind::PressureOverflowRejected),
        ),
        (
            "pressure_retries".to_owned(),
            count(PlanKind::PressureRetry),
        ),
        ("fault_seed_commands".to_owned(), count(PlanKind::FaultSeed)),
        ("trap_faults".to_owned(), count(PlanKind::TrapFault)),
        ("cpu_faults".to_owned(), count(PlanKind::CpuFault)),
        (
            "fault_post_recovery_cases".to_owned(),
            count(PlanKind::OldIncarnationStale),
        ),
        (
            "fault_post_recovery_requests".to_owned(),
            count(PlanKind::OldIncarnationStale)
                + count(PlanKind::NewIncarnationMutation)
                + count(PlanKind::NewIncarnationDuplicate),
        ),
        ("sibling_probes".to_owned(), count(PlanKind::SiblingProbe)),
        ("update_failed_attempts".to_owned(), shape.failed_rollouts),
        ("update_valid_attempts".to_owned(), shape.valid_rollouts),
        (
            "update_proof_probes".to_owned(),
            count(PlanKind::UpdateProofSnapshot),
        ),
        (
            "authority_canaries".to_owned(),
            count(PlanKind::AuthorityCanary),
        ),
        (
            "cleanup_warmup_cycles".to_owned(),
            shape.cleanup_warmup_cycles,
        ),
        (
            "cleanup_retained_cycles".to_owned(),
            shape.cleanup_retained_cycles,
        ),
        (
            "cleanup_old_endpoint_probes".to_owned(),
            count(PlanKind::CleanupStaleProbe),
        ),
    ]);
    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/raw-summary.schema.json")).unwrap();
    let properties = schema["$defs"]["completed_counts"]["properties"]
        .as_object()
        .unwrap();
    let actual: BTreeMap<String, u64> = properties
        .iter()
        .filter_map(|(name, property)| {
            property["const"]
                .as_u64()
                .map(|value| (name.clone(), value))
        })
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(
        properties["update_worker_attempts"]["minimum"].as_u64(),
        Some(count(PlanKind::UpdateWorkerAnchorRead))
    );
}

#[test]
fn completed_summary_preserves_the_scenario_and_analysis_key_metric_registry() {
    let scenario = ScenarioDocument::embedded().unwrap();
    let scenario_names: Vec<String> = scenario
        .verdict
        .key_metrics
        .iter()
        .map(|name| {
            serde_json::to_value(name)
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(scenario_names, KeyMetrics::NAMES);

    let outcome = completed_outcome();
    outcome.validate().unwrap();
    let key_metrics = outcome.key_metrics();
    assert_eq!(
        key_set(
            serde_json::to_value(key_metrics)
                .unwrap()
                .as_object()
                .unwrap()
        ),
        KeyMetrics::NAMES
            .iter()
            .map(|name| (*name).to_owned())
            .collect()
    );
    assert_eq!(key_metrics.shared_init_ns, 101);
    assert_eq!(key_metrics.normal_total_ns, 104_000);
    assert_eq!(key_metrics.ready_32_total_rss_bytes, 1_200);
    assert_eq!(key_metrics.peak_total_rss_bytes, 1_300);

    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/raw-summary.schema.json")).unwrap();
    let metrics = &schema["$defs"]["metrics"];
    let metric_properties = metrics["properties"].as_object().unwrap();
    assert!(metric_properties.contains_key("shared_init_ns"));
    assert_eq!(
        metric_properties["normal"]["allOf"][0]["$ref"],
        Value::from("#/$defs/normal_metric")
    );
    assert!(string_set(&schema["$defs"]["normal_metric"]["required"]).contains("total_ns"));
    for diagnostic in [
        "cpu_failed_terminal",
        "failed_mixed_window",
        "valid_mixed_window",
    ] {
        assert!(metric_properties.contains_key(diagnostic));
    }
    assert_eq!(
        metric_properties["failed_mixed_window"]["allOf"][0]["$ref"],
        Value::from("#/$defs/nonnegative_metric")
    );
    assert_eq!(
        metric_properties["valid_mixed_window"]["allOf"][0]["$ref"],
        Value::from("#/$defs/nonnegative_metric")
    );
    assert_eq!(
        schema["$defs"]["nonnegative_metric"]["properties"]["minimum_ns"]["minimum"],
        Value::from(0)
    );
    for distribution in [
        "warm_create",
        "normal",
        "pressure_admission",
        "sibling",
        "trap_replacement_ready",
        "cpu_replacement_ready",
        "failed_update_unavailability",
        "valid_update_unavailability",
        "failed_rollout",
        "valid_rollout",
        "failed_mixed_window",
        "valid_mixed_window",
    ] {
        assert!(metric_properties.contains_key(distribution));
    }
    let expected_sample_counts = [
        ("warm_create", 32),
        ("normal", 10_240),
        ("pressure_admission", 64),
        ("sibling", 60),
        ("trap_replacement_ready", 30),
        ("cpu_replacement_ready", 30),
        ("cpu_failed_terminal", 30),
        ("failed_update_unavailability", 480),
        ("valid_update_unavailability", 480),
        ("failed_rollout", 30),
        ("valid_rollout", 30),
        ("failed_mixed_window", 30),
        ("valid_mixed_window", 30),
    ];
    for (name, expected) in expected_sample_counts {
        assert_eq!(
            metric_properties[name]["allOf"][1]["properties"]["sample_count"]["const"],
            Value::from(expected)
        );
    }
    let rss = schema["$defs"]["rss_result"]["properties"]
        .as_object()
        .unwrap();
    assert!(rss.contains_key("ready_32_stable_median_bytes"));
    assert!(rss.contains_key("phase_peak_bytes"));
}

#[test]
fn frozen_binding_mutations_are_rejected() {
    let mut manifest = sample_manifest("mutation-run");
    manifest.plan_bundle_sha256 = digest('f');
    assert!(matches!(
        manifest.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut manifest = sample_manifest("mutation-run");
    manifest.scenario.sha256 = digest('f');
    assert!(matches!(
        manifest.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut manifest = sample_manifest("mutation-run");
    manifest.candidate_build.source_tree_sha256 = digest('f');
    assert!(matches!(
        manifest.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut manifest = sample_manifest("mutation-run");
    manifest.candidate_order_position = 0;
    assert!(matches!(
        manifest.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut manifest = sample_manifest("mutation-run");
    manifest.authority_policy_sha256 = digest('f');
    assert!(matches!(
        manifest.validate(),
        Err(EvidenceError::Invariant(_))
    ));
}

#[test]
fn every_candidate_requires_its_exact_execution_topology() {
    for candidate in [
        CandidateKind::Lunatic,
        CandidateKind::Extism,
        CandidateKind::RawWasmtime,
    ] {
        let mut manifest = sample_manifest("candidate-topology");
        set_candidate(&mut manifest, candidate);
        assert!(manifest.validate().is_ok(), "{:?}", candidate);
    }

    let mut manifest = sample_manifest("extism-tokio-mutation");
    set_candidate(&mut manifest, CandidateKind::Extism);
    manifest
        .candidate_build
        .execution_topology
        .tokio_worker_threads = 4;
    assert!(matches!(
        manifest.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut manifest = sample_manifest("extism-thread-mutation");
    set_candidate(&mut manifest, CandidateKind::Extism);
    manifest
        .candidate_build
        .execution_topology
        .dedicated_tenant_threads = 31;
    assert!(matches!(
        manifest.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut manifest = sample_manifest("lunatic-model-mutation");
    manifest
        .candidate_build
        .execution_topology
        .tenant_execution_model = TenantExecutionModel::DedicatedThreads;
    assert!(matches!(
        manifest.validate(),
        Err(EvidenceError::Invariant(_))
    ));
}

#[test]
fn one_process_no_sidecar_topology_is_a_strict_gate() {
    let mut manifest = sample_manifest("process-count-mutation");
    manifest.candidate_build.execution_topology.os_processes = 2;
    assert!(matches!(
        manifest.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut manifest = sample_manifest("helper-process-mutation");
    manifest
        .candidate_build
        .execution_topology
        .long_lived_helper_processes = 1;
    assert!(matches!(
        manifest.validate(),
        Err(EvidenceError::Invariant(_))
    ));
}

#[test]
fn source_freeze_is_a_bound_content_addressed_snapshot() {
    let manifest = sample_manifest("source-freeze-valid");
    assert_eq!(
        manifest.candidate_build.source_revision,
        manifest.candidate_build.source_snapshot.sha256.as_str()
    );
    assert_eq!(
        manifest.candidate_build.source_tree_sha256,
        manifest.candidate_build.source_snapshot.sha256
    );
    assert!(manifest.validate().is_ok());

    let mut legacy = serde_json::to_value(&manifest).unwrap();
    let build = legacy["candidate_build"].as_object_mut().unwrap();
    build.remove("source_snapshot");
    build.insert("source_clean".into(), Value::Bool(true));
    assert!(serde_json::from_value::<RunManifest>(legacy).is_err());

    let mut false_revision = sample_manifest("source-freeze-false-revision");
    false_revision.candidate_build.source_revision = "f".repeat(64);
    assert!(matches!(
        false_revision.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut aliased = sample_manifest("source-freeze-alias");
    aliased.candidate_build.source_snapshot.path = aliased.candidate_build.executable.path.clone();
    assert!(matches!(
        aliased.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let temporary = TestDirectory::new("source-snapshot-tamper");
    let run = create_run(&temporary.path, "source-snapshot-tamper");
    populate_bound_artifacts(&run);
    fs::write(run.path().join("candidate/source-snapshot.tar"), b"mutated").unwrap();
    assert!(matches!(
        run.verify_bound_artifacts(),
        Err(EvidenceError::Tampered(_))
    ));
}

#[test]
fn oracle_source_is_also_a_bound_content_addressed_snapshot() {
    let manifest = sample_manifest("oracle-source-valid");
    assert_eq!(
        manifest.oracle.revision,
        manifest.oracle.source_snapshot.sha256.as_str()
    );
    assert_eq!(
        manifest.oracle.source_tree_sha256,
        manifest.oracle.source_snapshot.sha256
    );
    assert!(manifest.validate().is_ok());

    let mut false_revision = sample_manifest("oracle-source-false-revision");
    false_revision.oracle.revision = "f".repeat(64);
    assert!(matches!(
        false_revision.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut false_tree = sample_manifest("oracle-source-false-tree");
    false_tree.oracle.source_tree_sha256 = digest('f');
    assert!(matches!(
        false_tree.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut legacy = serde_json::to_value(&manifest).unwrap();
    let oracle = legacy["oracle"].as_object_mut().unwrap();
    oracle.remove("source_tree_sha256");
    oracle.remove("source_snapshot");
    oracle.insert("revision".into(), Value::String("f".repeat(40)));
    assert!(serde_json::from_value::<RunManifest>(legacy).is_err());

    let mut aliased = sample_manifest("oracle-source-alias");
    aliased.oracle.source_snapshot.path = aliased.oracle.executable.path.clone();
    assert!(matches!(
        aliased.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let temporary = TestDirectory::new("oracle-source-tamper");
    let run = create_run(&temporary.path, "oracle-source-tamper");
    populate_bound_artifacts(&run);
    fs::write(run.path().join("oracle/source-snapshot.tar"), b"mutated").unwrap();
    assert!(matches!(
        run.verify_bound_artifacts(),
        Err(EvidenceError::Tampered(_))
    ));
}

#[test]
fn oracle_and_candidate_wasmtime_versions_are_independent_and_exact() {
    let mut extism = sample_manifest("extism-engine-valid");
    set_candidate(&mut extism, CandidateKind::Extism);
    assert_eq!(extism.environment.oracle_wasmtime, "46.0.1");
    assert_eq!(
        extism.candidate_build.embedded_wasm_engine.version,
        "43.0.2"
    );
    assert!(extism.validate().is_ok());

    extism.candidate_build.embedded_wasm_engine.version = "46.0.1".into();
    assert!(matches!(
        extism.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut manifest = sample_manifest("oracle-engine-mutation");
    manifest.environment.oracle_wasmtime = "43.0.2".into();
    assert!(matches!(
        manifest.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut manifest = sample_manifest("implementation-mutation");
    manifest.candidate_build.embedded_wasm_engine.implementation = "other".into();
    assert!(matches!(
        manifest.validate(),
        Err(EvidenceError::Invariant(_))
    ));
}

#[test]
fn both_mixed_windows_accept_a_real_zero_duration() {
    let mut outcome = completed_outcome();
    outcome.metrics_ns.failed_mixed_window.minimum_ns = 0;
    outcome.metrics_ns.failed_mixed_window.nearest_rank_p99_ns = 0;
    outcome.metrics_ns.failed_mixed_window.maximum_ns = 0;
    outcome.metrics_ns.valid_mixed_window.minimum_ns = 0;
    outcome.metrics_ns.valid_mixed_window.nearest_rank_p99_ns = 0;
    outcome.metrics_ns.valid_mixed_window.maximum_ns = 0;
    assert!(outcome.validate().is_ok());

    outcome.metrics_ns.warm_create.minimum_ns = 0;
    assert!(matches!(
        outcome.validate(),
        Err(EvidenceError::Invariant(_))
    ));
}

#[test]
fn completed_metrics_require_every_frozen_sample_cardinality() {
    let expected = [
        ("warm_create", 32_u64),
        ("normal", 10_240),
        ("pressure_admission", 64),
        ("sibling", 60),
        ("trap_replacement_ready", 30),
        ("cpu_replacement_ready", 30),
        ("cpu_failed_terminal", 30),
        ("failed_update_unavailability", 480),
        ("valid_update_unavailability", 480),
        ("failed_rollout", 30),
        ("valid_rollout", 30),
        ("failed_mixed_window", 30),
        ("valid_mixed_window", 30),
    ];
    for (name, count) in expected {
        let mut value = serde_json::to_value(completed_outcome()).unwrap();
        value["metrics_ns"][name]["sample_count"] = Value::from(count - 1);
        let outcome: CompletedOutcome = serde_json::from_value(value).unwrap();
        assert!(matches!(
            outcome.validate(),
            Err(EvidenceError::Invariant(_))
        ));
    }
}

#[test]
fn metric_pass_is_recomputed_for_p99_maximum_and_lower_bounds() {
    let mut outcome = completed_outcome();
    outcome.metrics_ns.warm_create.nearest_rank_p99_ns = 113;
    outcome.metrics_ns.warm_create.maximum_ns = 114;
    assert!(matches!(
        outcome.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut outcome = completed_outcome();
    outcome.metrics_ns.cpu_failed_terminal.slo_max_ns = 109;
    outcome.metrics_ns.cpu_failed_terminal.nearest_rank_p99_ns = 109;
    outcome.metrics_ns.cpu_failed_terminal.maximum_ns = 110;
    outcome.metrics_ns.cpu_failed_terminal.pass = true;
    assert!(matches!(
        outcome.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut outcome = completed_outcome();
    outcome.metrics_ns.cpu_failed_terminal.slo_min_ns = Some(109);
    assert!(matches!(
        outcome.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut outcome = completed_outcome();
    outcome.metrics_ns.normal.pass = false;
    outcome.overall_pass = false;
    assert!(matches!(
        outcome.validate(),
        Err(EvidenceError::Invariant(_))
    ));
}

#[test]
fn rss_derived_values_and_pass_are_recomputed() {
    let mut outcome = completed_outcome();
    outcome.rss.ready_delta_bytes += 1;
    assert!(matches!(
        outcome.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut outcome = completed_outcome();
    outcome.rss.maximum_post_cleanup_delta_bytes = 1;
    assert!(matches!(
        outcome.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut outcome = completed_outcome();
    outcome.rss.cleanup_slope_bytes_per_cycle = 1.0;
    assert!(matches!(
        outcome.validate(),
        Err(EvidenceError::Invariant(_))
    ));

    let mut outcome = completed_outcome();
    outcome.rss.pass = false;
    outcome.overall_pass = false;
    assert!(matches!(
        outcome.validate(),
        Err(EvidenceError::Invariant(_))
    ));
}
