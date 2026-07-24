use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::convert::TryFrom;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::protocol::{
    validate_run_id, AuthorityProbeErrorClass, AuthorityProbeKind, AuthorityProbeResult,
    AuthorityProbeStage, AuthorityProbeTerminalEvent, CandidateKind, EMPTY_SHA256,
};

pub const TENANT_COUNT: u32 = 32;
pub const MAILBOX_CAPACITY: u32 = 64;
pub const MAX_PAYLOAD_BYTES: u32 = 1_024;
pub const NORMAL_ROUNDS: u32 = 320;
pub const NORMAL_COMMANDS: u32 = TENANT_COUNT * NORMAL_ROUNDS;
pub const DELAYED_DUPLICATES: u32 = 80;
pub const UPDATE_CYCLES: u32 = 30;
pub const PROOF_WAVES: u32 = 3;
pub const UPDATE_TARGETS: u32 = 16;
pub const CLEANUP_WARMUP_CYCLES: u32 = 5;
pub const CLEANUP_RETAINED_CYCLES: u32 = 30;
pub const BLOCK_COUNT: u32 = 10;
pub const NORMAL_WARMUP_ROUNDS: u32 = 32;
pub const FAULT_ROUNDS: u32 = 30;
pub const AUTHORITY_CANARIES: u32 = 6;
pub const UPDATE_ATTEMPTS: u32 = UPDATE_CYCLES * 2;
pub const UPDATE_WORKER_PREFIX: u64 = 1_u64 << 60;
pub const UPDATE_PROOF_PREFIX: u64 = 2_u64 << 60;
pub const CLEANUP_PREFIX: u64 = 3_u64 << 60;
pub const STRUCTURED_ATTEMPT_SHIFT: u32 = 48;
pub const STRUCTURED_TENANT_SHIFT: u32 = 40;
pub const WORKER_MAX_SEQUENCE: u64 = (1_u64 << STRUCTURED_TENANT_SHIFT) - 1;
pub const EXPECTED_STATIC_ACTIONS_PER_BLOCK: u64 = 24_747;
pub const EXPECTED_RETAINED_ACTIONS: u64 = 22_898;
pub const AUTHORITY_CANARY_IDS: [&str; 6] = [
    "wasi_p1_fs_read",
    "wasi_p1_fs_mutate",
    "lunatic_tcp",
    "lunatic_udp",
    "lunatic_sqlite_create",
    "extism_http",
];

pub const EMBEDDED_SCENARIO_JSON: &str = include_str!("../../../scenario-final.json");
pub const EMBEDDED_SCENARIO_SHA256: &str =
    "e5bcbc12981d4621fdeb551972e2c168fa8c48261b9ad8994ebccdf8ce1b166e";
pub const EMBEDDED_PLAN_BUNDLE_SHA256: &str =
    "a451737674dd18b9ac69fd882d2024766c29d9b2e6c67c26d2e02326be525b32";

#[derive(Debug, Error)]
pub enum WorkloadError {
    #[error("scenario JSON is invalid: {0}")]
    ScenarioJson(#[from] serde_json::Error),
    #[error("scenario invariant failed: {0}")]
    Scenario(String),
    #[error("oracle state violation: {0}")]
    State(String),
    #[error("immutable run directory I/O failed: {0}")]
    Io(#[from] io::Error),
}

macro_rules! frozen_struct {
    ($name:ident { $($field:ident : $field_type:ty),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            $(pub $field: $field_type,)*
        }
    };
}

frozen_struct!(IdRange {
    first: u32,
    last_inclusive: u32,
});
frozen_struct!(CountedRange {
    first: u32,
    last_inclusive: u32,
    count: u32,
});
frozen_struct!(PayloadGenerationSpec {
    domain: String,
    encoding: String,
    block_index: String,
    command_id: String,
    chunk_index: String,
    retry_rule: String,
});
frozen_struct!(IncarnationSpec {
    owner: String,
    encoding: String,
    initial: String,
    advance: String,
    rollout: String,
});
frozen_struct!(TenantStateSpec {
    initial_counter: i64,
    initial_version: String,
    accepted_increment: String,
    duplicate: String,
    fault_replacement: String,
    rollout: String,
});
frozen_struct!(TenantsSpec {
    count: u32,
    id_range: IdRange,
    mailbox_capacity: u32,
    payload_limit_bytes: u32,
    payload_generation: PayloadGenerationSpec,
    tokio_worker_threads: u32,
    incarnation: IncarnationSpec,
    state: TenantStateSpec,
});
frozen_struct!(IncrementOperationSpec {
    kind: String,
    delta: i64,
    payload_bytes: u32,
});
frozen_struct!(CommandIdRule {
    base: u64,
    formula: String,
});
frozen_struct!(NormalWarmupSpec {
    rounds: u32,
    commands_per_round: u32,
    retained: bool,
    tenant_order_formula: String,
    operation: IncrementOperationSpec,
    command_id: CommandIdRule,
});
frozen_struct!(NormalMeasuredSpec {
    rounds: u32,
    commands_per_round: u32,
    total_commands: u32,
    concurrency: String,
    round_barrier: String,
    tenant_order_formula: String,
    operation: IncrementOperationSpec,
    command_id: CommandIdRule,
    allowed_rejections: Vec<String>,
});
frozen_struct!(DelayedDuplicatesSpec {
    cases: u32,
    requests_per_case: u32,
    total_requests: u32,
    index: IdRange,
    source_round_formula: String,
    source_round_range: IdRange,
    source_tenant_formula: String,
    cross_tenant_formula: String,
    source_command_id_formula: String,
    dispatch: String,
    minimum_later_completed_rounds: u32,
    sequence: Vec<String>,
    allowed_rejections: Vec<String>,
});
frozen_struct!(NormalSpec {
    warmup: NormalWarmupSpec,
    measured: NormalMeasuredSpec,
    delayed_duplicates: DelayedDuplicatesSpec,
});
frozen_struct!(ClosedGateSpec {
    gate_rule: String,
    submitted: u32,
    accepted: u32,
    retryable_rejected: u32,
    rejected_submission_index: u32,
    payload_bytes: u32,
    rejection_reason: String,
    drain: String,
    retry: String,
    retry_expected: String,
});
frozen_struct!(OversizeBoundarySpec {
    payload_bytes: u32,
    expected: String,
    reason: String,
    mutation: String,
    retries: u32,
});
frozen_struct!(LimitBoundarySpec {
    payload_bytes: u32,
    expected: String,
    mutation: String,
});
frozen_struct!(PayloadBoundarySpec {
    order: Vec<String>,
    oversize: OversizeBoundarySpec,
    limit: LimitBoundarySpec,
});
frozen_struct!(PressureCommandIdSpec {
    base: u64,
    closed_gate_formula: String,
    oversize: u64,
    limit: u64,
});
frozen_struct!(PressureSpec {
    tenant_formula: String,
    closed_gate: ClosedGateSpec,
    payload_boundary: PayloadBoundarySpec,
    command_id: PressureCommandIdSpec,
});
frozen_struct!(FaultSeedSpec {
    count: u32,
    dispatch: String,
    operation: String,
    expected: String,
    allowed_rejections: Vec<String>,
});
frozen_struct!(TrapFaultSpec {
    count: u32,
    tenant_formula: String,
    execution: String,
    terminal_reason: String,
    replacement: String,
});
frozen_struct!(CpuFaultSpec {
    count: u32,
    tenant_formula: String,
    execution: String,
    deadline_ms: u32,
    admission_to_execution_started_min_exclusive_ms: u32,
    admission_to_execution_started_max_ms: u32,
    execution_started_to_failed_min_ms: u32,
    execution_started_to_failed_max_ms: u32,
    terminal_reason: String,
    replacement: String,
});
frozen_struct!(FaultStaleSpec {
    cases: u32,
    requests_per_case: u32,
    total_requests: u32,
    dispatch: String,
    command_id: String,
    sequence: Vec<String>,
    stale_terminal: String,
    retries: u32,
});
frozen_struct!(SiblingProbeSpec {
    count: u32,
    tenant_formula: String,
    trap_dispatch: String,
    cpu_dispatch: String,
    operation: String,
    allowed_rejections: Vec<String>,
});
frozen_struct!(FaultCommandIdSpec {
    base: u64,
    trap_sibling_formula: String,
    trap_stale_formula: String,
    cpu_sibling_formula: String,
    cpu_stale_formula: String,
});
frozen_struct!(FaultsSpec {
    rounds: u32,
    round_order: Vec<String>,
    seed: FaultSeedSpec,
    trap: TrapFaultSpec,
    cpu: CpuFaultSpec,
    stale: FaultStaleSpec,
    sibling_probe: SiblingProbeSpec,
    recovery_clock: String,
    recovery_deadline_ms: u32,
    command_id: FaultCommandIdSpec,
});

frozen_struct!(UpdateTargetsSpec {
    first: u32,
    last_inclusive: u32,
    count: u32,
});
frozen_struct!(UpdateParitySpec {
    start_version: String,
    failed_attempt: String,
    valid_attempt: String,
});
frozen_struct!(BadArtifactSpec {
    failure_tenant: u32,
    rule: String,
    other_targets: String,
});
frozen_struct!(WorkerTraceCardinalitySpec {
    anchor_logical_operations_per_tenant_per_attempt: u32,
    continuous_logical_operations_per_tenant_per_attempt_min: u32,
    continuous_logical_operations_per_tenant_per_attempt_max: String,
    logical_count_rule: String,
    transport_attempt_count_rule: String,
    terminal_rule: String,
});
frozen_struct!(ContinuousWorkersSpec {
    count: u32,
    payload_bytes: u32,
    assignment: String,
    start: String,
    run: String,
    stop: String,
    drain: String,
    on_retryable_rejection: String,
    max_retries_per_operation: u32,
    accepted_rule: String,
    trace_cardinality: WorkerTraceCardinalitySpec,
});
frozen_struct!(UpdateProofSpec {
    waves_per_attempt: u32,
    targets_per_wave: u32,
    probes_per_attempt: u32,
    operation: String,
    dispatch: String,
    failed_expected: String,
    valid_expected: String,
});
frozen_struct!(WorkerCommandLayoutSpec {
    domain_prefix: String,
    attempt_shift_bits: u32,
    tenant_shift_bits: u32,
    max_attempt_index: u32,
    max_tenant_id: u32,
    max_sequence: String,
    anchor_sequence: String,
    continuous_first_sequence: String,
    operation_formula: String,
    formula: String,
});
frozen_struct!(ProofCommandLayoutSpec {
    domain_prefix: String,
    attempt_shift_bits: u32,
    tenant_shift_bits: u32,
    max_attempt_index: u32,
    max_tenant_id: u32,
    max_sequence: String,
    sequence_formula: String,
    formula: String,
});
frozen_struct!(UpdateCommandIdSpec {
    attempt_index_formula: String,
    worker_layout: WorkerCommandLayoutSpec,
    proof_layout: ProofCommandLayoutSpec,
});
frozen_struct!(UpdatesSpec {
    cycles: u32,
    cycle_index: IdRange,
    targets: UpdateTargetsSpec,
    attempts_per_cycle: u32,
    failed_attempts_total: u32,
    valid_attempts_total: u32,
    artifacts: Vec<String>,
    even_cycle: UpdateParitySpec,
    odd_cycle: UpdateParitySpec,
    bad_artifact: BadArtifactSpec,
    continuous_workers: ContinuousWorkersSpec,
    proof: UpdateProofSpec,
    external_terminal: String,
    allowed_terminal_states: Vec<String>,
    in_doubt: String,
    unavailability_formula: String,
    rollout_duration_formula: String,
    mixed_window_formula: String,
    command_id: UpdateCommandIdSpec,
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityCanaryId {
    WasiP1FsRead,
    WasiP1FsMutate,
    LunaticTcp,
    LunaticUdp,
    LunaticSqliteCreate,
    ExtismHttp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityErrorClass {
    AbsentAtLink,
    PolicyDenied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityStage {
    Compile,
    Instantiate,
    Invoke,
}

frozen_struct!(AuthorityResultContractSpec {
    allowed_error_classes: Vec<AuthorityErrorClass>,
    allowed_stages: Vec<AuthorityStage>,
    absent_at_link_evidence: String,
    policy_denied_evidence: String,
    all_other_errors: String,
});
frozen_struct!(AuthorityCanarySpec {
    id: AuthorityCanaryId,
    abi: String,
    attempt: String,
    positive_effect: String,
    pass: String,
});
frozen_struct!(AuthoritySpec {
    run_scope: String,
    policy: String,
    nonce_domain: String,
    nonce_encoding: String,
    nonce_fields: Vec<String>,
    candidate_wire_tokens: Vec<CandidateKind>,
    positive_control: String,
    result_contract: AuthorityResultContractSpec,
    canaries: Vec<AuthorityCanarySpec>,
    canary_order_formula: String,
    observer_tail_ms: u32,
    unrecognized_production_import: String,
    comparison_only_deny_import: String,
    positive_control_failure: String,
});
frozen_struct!(CleanupCommandIdSpec {
    domain_prefix: String,
    cycle_shift_bits: u32,
    tenant_shift_bits: u32,
    max_cycle_index: u32,
    max_tenant_id: u32,
    sequence: String,
    formula: String,
});
frozen_struct!(CleanupSpec {
    precondition: String,
    warmup_cycles: u32,
    retained_cycles: u32,
    total_cycles: u32,
    cycle: Vec<String>,
    post_teardown_rss_sample_offsets_ms: Vec<u32>,
    tenant_order_formula: String,
    recreate_rule: String,
    stale_endpoint_reason: String,
    hidden_authority_or_endpoint: String,
    command_id: CleanupCommandIdSpec,
});
frozen_struct!(CandidateOrderSpec {
    block_mod_3_0: [CandidateKind; 3],
    block_mod_3_1: [CandidateKind; 3],
    block_mod_3_2: [CandidateKind; 3],
});
frozen_struct!(PhaseActionCountsSpec {
    initial_create: u64,
    normal_warmup: u64,
    normal_measured: u64,
    delayed_duplicates: u64,
    pressure: u64,
    faults: u64,
    updates_static: u64,
    authority_with_positive_controls: u64,
    main_teardown_precondition: u64,
    cleanup: u64,
});
frozen_struct!(StaticPlanCardinalitySpec {
    total_actions_per_candidate_block: u64,
    retained_actions_block_0: u64,
    retained_actions_each_block_1_through_9: u64,
    nonretained_actions_each_block_1_through_9: u64,
    phase_actions: PhaseActionCountsSpec,
    dynamic_update_workers: String,
});
frozen_struct!(BlocksSpec {
    count: u32,
    index: IdRange,
    instrumentation_warmup: u32,
    retained: CountedRange,
    candidates: Vec<CandidateKind>,
    candidate_order: CandidateOrderSpec,
    replacement: String,
    execution: String,
    phase_order: Vec<String>,
    static_plan_cardinality: StaticPlanCardinalitySpec,
});
frozen_struct!(AckTerminalTimeoutSpec {
    ack: u32,
    terminal: u32,
});
frozen_struct!(TrapTimeoutSpec {
    terminal: u32,
    replacement_ready_from_control_write: u32,
});
frozen_struct!(CpuTimeoutSpec {
    execution_started_from_admission_min_exclusive: u32,
    execution_started_from_admission_max: u32,
    failed_from_execution_started_min: u32,
    failed_from_execution_started_max: u32,
    replacement_ready_from_control_write: u32,
});
frozen_struct!(RolloutTimeoutSpec {
    ack: u32,
    external_proof: u32,
});
frozen_struct!(PhaseDeadlinesSpec {
    hello_terminal: u32,
    init_terminal: u32,
    create: AckTerminalTimeoutSpec,
    normal_probe_snapshot: AckTerminalTimeoutSpec,
    trap: TrapTimeoutSpec,
    cpu: CpuTimeoutSpec,
    rollout: RolloutTimeoutSpec,
    authority_terminal: u32,
    teardown_terminal: u32,
    shutdown_terminal: u32,
    process_exit: u32,
});
frozen_struct!(PhaseRejectionsSpec {
    normal: Vec<String>,
    delayed_duplicate: Vec<String>,
    pressure_closed_gate: Vec<String>,
    pressure_oversize: Vec<String>,
    fault_stale: Vec<String>,
    update_worker: Vec<String>,
    cleanup_old_endpoint: Vec<String>,
});
frozen_struct!(TimeoutsSpec {
    clock: String,
    positive_elapsed_ns_required: bool,
    phase_deadlines_ms: PhaseDeadlinesSpec,
    deadline_reset: String,
    unrelated_event: String,
    timeout_result: String,
    transport_retry: String,
    candidate_hidden_retry: String,
    only_oracle_retries: Vec<String>,
    phase_rejections: PhaseRejectionsSpec,
    all_other_rejections: String,
});
frozen_struct!(StableSampleSpec {
    count: u32,
    spacing_ms: u32,
    reducer: String,
});
frozen_struct!(RssFormulasSpec {
    ready_delta_bytes: String,
    peak_delta_bytes: String,
    cleanup_delta_i_bytes: String,
    cleanup_slope_bytes_per_cycle: String,
});
frozen_struct!(RssLimitsSpec {
    ready_32_total: u64,
    peak_delta: u64,
    post_cleanup_delta: u64,
    cleanup_slope_per_cycle: u64,
});
frozen_struct!(StatisticsSpec {
    nearest_rank_percentile: String,
    run_scope: String,
    outliers: String,
    pair_ratio: String,
    relative_result: String,
    ratio_limit: f64,
    sensitivity_thresholds: Vec<f64>,
});
frozen_struct!(RssSpec {
    scope: String,
    periodic_sample_ms: u32,
    stable_sample: StableSampleSpec,
    points: Vec<String>,
    preallocation_before_minimal_host: String,
    formulas: RssFormulasSpec,
    instrumentation_validity: String,
    limits_bytes: RssLimitsSpec,
    statistics: StatisticsSpec,
});
frozen_struct!(AbsoluteSlosSpec {
    warm_create_p99_ms: u32,
    normal_p99_ms: u32,
    pressure_admission_p99_ms: u32,
    sibling_p99_ms: u32,
    trap_replacement_ready_p99_ms: u32,
    cpu_replacement_ready_p99_ms: u32,
    cpu_failed_terminal_min_ms: u32,
    cpu_failed_terminal_max_ms: u32,
    failed_update_unavailability_p99_ms: u32,
    valid_update_unavailability_p99_ms: u32,
    failed_rollout_p99_ms: u32,
    valid_rollout_p99_ms: u32,
    failed_mixed_window_max_ms: u32,
    valid_mixed_window_max_ms: u32,
    update_rpo_completed_commands: u32,
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyMetricName {
    SharedInitNs,
    WarmCreateP99Ns,
    NormalP99Ns,
    NormalTotalNs,
    PressureAdmissionP99Ns,
    SiblingP99Ns,
    TrapRecoveryP99Ns,
    CpuRecoveryP99Ns,
    FailedUpdateUnavailabilityP99Ns,
    ValidUpdateUnavailabilityP99Ns,
    FailedRolloutP99Ns,
    ValidRolloutP99Ns,
    #[serde(rename = "ready_32_total_rss_bytes")]
    Ready32TotalRssBytes,
    PeakTotalRssBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum VerdictRuleId {
    PositioningRejected,
    AlternativeDominates,
    LunaticPackagingValueSupported,
    LunaticPackagingValueUnsupported,
    TestedSetPackagingGap,
    Inconclusive,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerdictRuleSpec {
    pub rank: u32,
    pub id: VerdictRuleId,
    pub when: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

frozen_struct!(AlternativeLabelsSpec {
    extism: String,
    raw_wasmtime: String,
});
frozen_struct!(VerdictSpec {
    absolute_slos: AbsoluteSlosSpec,
    mandatory_gates: Vec<String>,
    key_metrics: Vec<KeyMetricName>,
    retained_run_rule: String,
    eligibility: String,
    alternative_reference: String,
    alternative_dominates: String,
    performance_condition: String,
    strong_packaging: String,
    moderate_packaging: String,
    exclusive_precedence: Vec<VerdictRuleSpec>,
    exhaustive: bool,
    first_matching_rule_wins: bool,
    forbidden_claims: Vec<String>,
    alternative_labels: AlternativeLabelsSpec,
});

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioDocument {
    pub schema_version: u32,
    pub experiment_id: String,
    pub tenants: TenantsSpec,
    pub normal: NormalSpec,
    pub pressure: PressureSpec,
    pub faults: FaultsSpec,
    pub updates: UpdatesSpec,
    pub authority: AuthoritySpec,
    pub cleanup: CleanupSpec,
    pub blocks: BlocksSpec,
    pub timeouts: TimeoutsSpec,
    pub rss: RssSpec,
    pub verdict: VerdictSpec,
}

pub const KEY_METRICS: &[KeyMetricName; 14] = &[
    KeyMetricName::SharedInitNs,
    KeyMetricName::WarmCreateP99Ns,
    KeyMetricName::NormalP99Ns,
    KeyMetricName::NormalTotalNs,
    KeyMetricName::PressureAdmissionP99Ns,
    KeyMetricName::SiblingP99Ns,
    KeyMetricName::TrapRecoveryP99Ns,
    KeyMetricName::CpuRecoveryP99Ns,
    KeyMetricName::FailedUpdateUnavailabilityP99Ns,
    KeyMetricName::ValidUpdateUnavailabilityP99Ns,
    KeyMetricName::FailedRolloutP99Ns,
    KeyMetricName::ValidRolloutP99Ns,
    KeyMetricName::Ready32TotalRssBytes,
    KeyMetricName::PeakTotalRssBytes,
];

impl ScenarioDocument {
    pub fn embedded() -> Result<Self, WorkloadError> {
        let digest = Sha256::digest(EMBEDDED_SCENARIO_JSON.as_bytes());
        let actual = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        if actual != EMBEDDED_SCENARIO_SHA256 {
            return Err(WorkloadError::Scenario(format!(
                "embedded scenario SHA-256 drifted: {actual}"
            )));
        }
        let scenario: Self = serde_json::from_str(EMBEDDED_SCENARIO_JSON)?;
        scenario.validate()?;
        let plan_bundle_sha256 = plan_bundle_sha256(&scenario)?;
        if plan_bundle_sha256 != EMBEDDED_PLAN_BUNDLE_SHA256 {
            return Err(WorkloadError::Scenario(format!(
                "embedded canonical plan bundle SHA-256 drifted: {plan_bundle_sha256}"
            )));
        }
        Ok(scenario)
    }

    pub fn validate(&self) -> Result<(), WorkloadError> {
        let frozen: ScenarioDocument = serde_json::from_str(EMBEDDED_SCENARIO_JSON)?;
        if self != &frozen {
            return Err(WorkloadError::Scenario(
                "scenario must exactly equal the SHA-256-bound embedded frozen document".into(),
            ));
        }
        if self.schema_version != 3
            || self.tenants.count != TENANT_COUNT
            || self.tenants.mailbox_capacity != MAILBOX_CAPACITY
            || self.tenants.payload_limit_bytes != MAX_PAYLOAD_BYTES
            || self.normal.warmup.rounds != NORMAL_WARMUP_ROUNDS
            || self.normal.measured.rounds != NORMAL_ROUNDS
            || self.normal.delayed_duplicates.cases != DELAYED_DUPLICATES
            || self.faults.rounds != FAULT_ROUNDS
            || self.updates.cycles != UPDATE_CYCLES
            || self.updates.continuous_workers.payload_bytes != 64
            || self.updates.proof.waves_per_attempt != PROOF_WAVES
            || self.updates.proof.targets_per_wave != UPDATE_TARGETS
            || self.cleanup.warmup_cycles != CLEANUP_WARMUP_CYCLES
            || self.cleanup.retained_cycles != CLEANUP_RETAINED_CYCLES
            || self.blocks.count != BLOCK_COUNT
            || self
                .blocks
                .static_plan_cardinality
                .total_actions_per_candidate_block
                != EXPECTED_STATIC_ACTIONS_PER_BLOCK
        {
            return Err(WorkloadError::Scenario(
                "typed scenario disagrees with executable workload constants".into(),
            ));
        }
        if self.verdict.key_metrics.as_slice() != KEY_METRICS {
            return Err(WorkloadError::Scenario(
                "verdict.key_metrics is not the exact closed 14-metric registry".into(),
            ));
        }
        let canary_ids: Vec<_> = self
            .authority
            .canaries
            .iter()
            .map(|canary| canary.id)
            .collect();
        if canary_ids.as_slice()
            != [
                AuthorityCanaryId::WasiP1FsRead,
                AuthorityCanaryId::WasiP1FsMutate,
                AuthorityCanaryId::LunaticTcp,
                AuthorityCanaryId::LunaticUdp,
                AuthorityCanaryId::LunaticSqliteCreate,
                AuthorityCanaryId::ExtismHttp,
            ]
        {
            return Err(WorkloadError::Scenario(
                "authority canary registry is not the frozen six-ID union".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanKind {
    InitialCreate,
    NormalWarmupIncrement,
    NormalIncrement,
    DelayedOriginalDuplicate,
    CrossTenantFirst,
    CrossTenantDuplicate,
    Oversize1025Rejected,
    Boundary1024Accepted,
    PressureGateClose,
    PressureAccepted,
    PressureOverflowRejected,
    PressureGateOpen,
    PressureDrain,
    PressureRetry,
    FaultSeed,
    TrapFault,
    CpuFault,
    OldIncarnationStale,
    NewIncarnationMutation,
    NewIncarnationDuplicate,
    SiblingProbe,
    UpdateBaselineSnapshot,
    UpdateWorkerStart,
    UpdateWorkerAnchorRead,
    RolloutRequest,
    UpdateWorkerContinuous,
    RolloutTerminal,
    UpdateWorkerDrain,
    UpdateCompletedReplay,
    UpdateProofSnapshot,
    AuthorityPositiveControl,
    AuthorityCanary,
    MainTeardown,
    MainActiveZero,
    CleanupCreate,
    CleanupIncrement,
    CleanupQuiesce,
    CleanupSnapshot,
    CleanupTeardown,
    CleanupStaleProbe,
    CleanupActiveZero,
    CleanupPostRss,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedAction {
    pub ordinal: u64,
    pub block: u32,
    pub kind: PlanKind,
    pub tenant_id: Option<u32>,
    pub round: Option<u32>,
    pub cycle: Option<u32>,
    pub rollout_index: Option<u32>,
    pub proof_wave: Option<u32>,
    pub command_id: Option<String>,
    pub payload_bytes: Option<u32>,
    pub payload_sha256: Option<String>,
    pub label: Option<String>,
    pub sample_offset_ms: Option<u32>,
    pub retained: bool,
}

impl PlannedAction {
    fn new(block: u32, kind: PlanKind, retained: bool) -> Self {
        Self {
            ordinal: 0,
            block,
            kind,
            tenant_id: None,
            round: None,
            cycle: None,
            rollout_index: None,
            proof_wave: None,
            command_id: None,
            payload_bytes: None,
            payload_sha256: None,
            label: None,
            sample_offset_ms: None,
            retained,
        }
    }

    fn tenant(mut self, tenant: u32) -> Self {
        self.tenant_id = Some(tenant);
        self
    }

    fn round(mut self, round: u32) -> Self {
        self.round = Some(round);
        self
    }

    fn cycle(mut self, cycle: u32) -> Self {
        self.cycle = Some(cycle);
        self
    }

    fn attempt(mut self, attempt: u32) -> Self {
        self.rollout_index = Some(attempt);
        self
    }

    fn wave(mut self, wave: u32) -> Self {
        self.proof_wave = Some(wave);
        self
    }

    fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    fn sample_offset(mut self, offset_ms: u32) -> Self {
        self.sample_offset_ms = Some(offset_ms);
        self
    }

    fn command(
        mut self,
        block: u32,
        command_id: u64,
        payload_bytes: u32,
    ) -> Result<Self, WorkloadError> {
        let payload = deterministic_payload(block, command_id, payload_bytes)?;
        self.command_id = Some(command_id.to_string());
        self.payload_bytes = Some(payload_bytes);
        self.payload_sha256 = Some(sha256_hex(&payload));
        Ok(self)
    }
}

fn push_action(actions: &mut Vec<PlannedAction>, mut action: PlannedAction) {
    action.ordinal = actions.len() as u64;
    actions.push(action);
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullShapeSummary {
    pub block: BlockOrder,
    pub total_actions: u64,
    pub retained_actions: u64,
    pub normal_warmup_commands: u64,
    pub normal_commands: u64,
    pub delayed_duplicate_groups: u64,
    pub pressure_accepted_before_overflow: u64,
    pub pressure_overflow_rejections: u64,
    pub pressure_retries: u64,
    pub fault_seeds: u64,
    pub trap_faults: u64,
    pub cpu_faults: u64,
    pub sibling_probes: u64,
    pub recovery_triplets: u64,
    pub failed_rollouts: u64,
    pub valid_rollouts: u64,
    pub update_anchor_reads: u64,
    pub update_worker_descriptors: u64,
    pub update_completed_replays: u64,
    pub update_proof_snapshots: u64,
    pub authority_positive_controls: u64,
    pub authority_canaries: u64,
    pub main_teardowns: u64,
    pub cleanup_warmup_cycles: u64,
    pub cleanup_retained_cycles: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockOrder {
    pub block: u32,
    pub retained: bool,
    pub candidates: [CandidateKind; 3],
}

pub fn block_order(block: u32) -> Result<BlockOrder, WorkloadError> {
    if block >= BLOCK_COUNT {
        return Err(WorkloadError::Scenario(format!(
            "block {block} is outside 0..{}",
            BLOCK_COUNT - 1
        )));
    }
    let candidates = match block % 3 {
        0 => [
            CandidateKind::Lunatic,
            CandidateKind::Extism,
            CandidateKind::RawWasmtime,
        ],
        1 => [
            CandidateKind::Extism,
            CandidateKind::RawWasmtime,
            CandidateKind::Lunatic,
        ],
        _ => [
            CandidateKind::RawWasmtime,
            CandidateKind::Lunatic,
            CandidateKind::Extism,
        ],
    };
    Ok(BlockOrder {
        block,
        retained: block != 0,
        candidates,
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn deterministic_payload(
    block: u32,
    command_id: u64,
    len: u32,
) -> Result<Vec<u8>, WorkloadError> {
    if block >= BLOCK_COUNT || len > MAX_PAYLOAD_BYTES + 1 {
        return Err(WorkloadError::Scenario(
            "payload generator input is outside the frozen block/length bounds".into(),
        ));
    }
    let mut output = Vec::with_capacity(len as usize);
    let mut chunk = 0_u32;
    while output.len() < len as usize {
        let mut hasher = Sha256::new();
        hasher.update(b"embedded-v3-payload-v1");
        hasher.update(block.to_be_bytes());
        hasher.update(command_id.to_be_bytes());
        hasher.update(chunk.to_be_bytes());
        output.extend_from_slice(&hasher.finalize());
        chunk = chunk
            .checked_add(1)
            .ok_or_else(|| WorkloadError::Scenario("payload chunk counter overflow".into()))?;
    }
    output.truncate(len as usize);
    Ok(output)
}

fn encode_structured_id(prefix: u64, index: u32, tenant: u32, sequence: u64) -> u64 {
    prefix
        | (u64::from(index) << STRUCTURED_ATTEMPT_SHIFT)
        | (u64::from(tenant) << STRUCTURED_TENANT_SHIFT)
        | sequence
}

pub fn worker_command_id(attempt: u32, tenant: u32, sequence: u64) -> Result<u64, WorkloadError> {
    if attempt >= UPDATE_ATTEMPTS || tenant >= UPDATE_TARGETS || sequence > WORKER_MAX_SEQUENCE {
        return Err(WorkloadError::Scenario(
            "worker command-id component is outside attempt<=59, tenant<=15, sequence<=2^40-1"
                .into(),
        ));
    }
    Ok(encode_structured_id(
        UPDATE_WORKER_PREFIX,
        attempt,
        tenant,
        sequence,
    ))
}

pub fn proof_command_id(attempt: u32, tenant: u32, wave: u32) -> Result<u64, WorkloadError> {
    if attempt >= UPDATE_ATTEMPTS || tenant >= UPDATE_TARGETS || wave >= PROOF_WAVES {
        return Err(WorkloadError::Scenario(
            "proof command-id component is outside attempt<=59, tenant<=15, wave<=2".into(),
        ));
    }
    Ok(encode_structured_id(
        UPDATE_PROOF_PREFIX,
        attempt,
        tenant,
        u64::from(wave),
    ))
}

pub fn cleanup_command_id(cycle: u32, tenant: u32) -> Result<u64, WorkloadError> {
    if cycle >= CLEANUP_WARMUP_CYCLES + CLEANUP_RETAINED_CYCLES || tenant >= TENANT_COUNT {
        return Err(WorkloadError::Scenario(
            "cleanup command-id component is outside cycle<=34, tenant<=31".into(),
        ));
    }
    Ok(encode_structured_id(CLEANUP_PREFIX, cycle, tenant, 0))
}

fn candidate_wire_name(candidate: CandidateKind) -> &'static str {
    match candidate {
        CandidateKind::Lunatic => "lunatic",
        CandidateKind::Extism => "extism",
        CandidateKind::RawWasmtime => "raw_wasmtime",
    }
}

pub fn authority_nonce(
    experiment_id: &str,
    block: u32,
    candidate: CandidateKind,
    run_id: &str,
) -> Result<String, WorkloadError> {
    block_order(block)?;
    let block_bytes = block.to_be_bytes();
    let fields: [&[u8]; 5] = [
        b"embedded-v3-authority-v1",
        experiment_id.as_bytes(),
        &block_bytes,
        candidate_wire_name(candidate).as_bytes(),
        run_id.as_bytes(),
    ];
    let mut hasher = Sha256::new();
    for field in fields {
        let len = u32::try_from(field.len())
            .map_err(|_| WorkloadError::Scenario("authority nonce field exceeds u32".into()))?;
        hasher.update(len.to_be_bytes());
        hasher.update(field);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub fn build_full_plan(
    scenario: &ScenarioDocument,
    block: u32,
) -> Result<Vec<PlannedAction>, WorkloadError> {
    scenario.validate()?;
    block_order(block)?;
    let mut actions = Vec::with_capacity(EXPECTED_STATIC_ACTIONS_PER_BLOCK as usize);
    plan_initial_normal_pressure_faults(&mut actions, block)?;
    plan_updates(&mut actions, block)?;
    plan_authority_and_cleanup(&mut actions, block)?;
    verify_full_shape(&actions)?;
    Ok(actions)
}

fn plan_initial_normal_pressure_faults(
    actions: &mut Vec<PlannedAction>,
    block: u32,
) -> Result<(), WorkloadError> {
    let retained = block != 0;
    let rotated_tenant = |round: u32, slot: u32| (block + round + slot) % TENANT_COUNT;

    for slot in 0..TENANT_COUNT {
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::InitialCreate, retained)
                .tenant(rotated_tenant(0, slot)),
        );
    }

    for round in 0..NORMAL_WARMUP_ROUNDS {
        for slot in 0..TENANT_COUNT {
            let tenant = rotated_tenant(round, slot);
            let id = 1_u64 + u64::from(round) * u64::from(TENANT_COUNT) + u64::from(tenant);
            push_action(
                actions,
                PlannedAction::new(block, PlanKind::NormalWarmupIncrement, false)
                    .tenant(tenant)
                    .round(round)
                    .command(block, id, 64)?,
            );
        }
    }

    for round in 0..NORMAL_ROUNDS {
        for slot in 0..TENANT_COUNT {
            let tenant = rotated_tenant(round, slot);
            let id = 100_000_u64 + u64::from(round) * u64::from(TENANT_COUNT) + u64::from(tenant);
            push_action(
                actions,
                PlannedAction::new(block, PlanKind::NormalIncrement, retained)
                    .tenant(tenant)
                    .round(round)
                    .command(block, id, 64)?,
            );
        }
    }

    for j in 0..DELAYED_DUPLICATES {
        let round = 2 * j;
        let source = j % TENANT_COUNT;
        let cross = (source + 1) % TENANT_COUNT;
        let id = 100_000_u64 + u64::from(round) * u64::from(TENANT_COUNT) + u64::from(source);
        for (kind, tenant) in [
            (PlanKind::DelayedOriginalDuplicate, source),
            (PlanKind::CrossTenantFirst, cross),
            (PlanKind::CrossTenantDuplicate, cross),
        ] {
            push_action(
                actions,
                PlannedAction::new(block, kind, retained)
                    .tenant(tenant)
                    .round(round)
                    .command(block, id, 64)?,
            );
        }
    }

    let pressure_tenant = (block * 3) % TENANT_COUNT;
    push_action(
        actions,
        PlannedAction::new(block, PlanKind::Oversize1025Rejected, retained)
            .tenant(pressure_tenant)
            .command(block, 1_000_100, MAX_PAYLOAD_BYTES + 1)?,
    );
    push_action(
        actions,
        PlannedAction::new(block, PlanKind::Boundary1024Accepted, retained)
            .tenant(pressure_tenant)
            .command(block, 1_000_101, MAX_PAYLOAD_BYTES)?,
    );
    push_action(
        actions,
        PlannedAction::new(block, PlanKind::PressureGateClose, retained).tenant(pressure_tenant),
    );
    for submission in 0..MAILBOX_CAPACITY {
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::PressureAccepted, retained)
                .tenant(pressure_tenant)
                .round(submission)
                .command(block, 1_000_000 + u64::from(submission), 64)?,
        );
    }
    let overflow_id = 1_000_000 + u64::from(MAILBOX_CAPACITY);
    push_action(
        actions,
        PlannedAction::new(block, PlanKind::PressureOverflowRejected, retained)
            .tenant(pressure_tenant)
            .round(MAILBOX_CAPACITY)
            .command(block, overflow_id, 64)?,
    );
    push_action(
        actions,
        PlannedAction::new(block, PlanKind::PressureGateOpen, retained).tenant(pressure_tenant),
    );
    push_action(
        actions,
        PlannedAction::new(block, PlanKind::PressureDrain, retained).tenant(pressure_tenant),
    );
    push_action(
        actions,
        PlannedAction::new(block, PlanKind::PressureRetry, retained)
            .tenant(pressure_tenant)
            .round(MAILBOX_CAPACITY)
            .command(block, overflow_id, 64)?,
    );

    for i in 0..FAULT_ROUNDS {
        let trap_tenant = (block + i) % TENANT_COUNT;
        let trap_sibling = (trap_tenant + 1) % TENANT_COUNT;
        let trap_sibling_id = 2_000_000_u64 + u64::from(i) * 4;
        let trap_seed_id = trap_sibling_id + 1;
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::FaultSeed, retained)
                .tenant(trap_tenant)
                .cycle(i)
                .label("trap")
                .command(block, trap_seed_id, 64)?,
        );
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::TrapFault, retained)
                .tenant(trap_tenant)
                .cycle(i),
        );
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::SiblingProbe, retained)
                .tenant(trap_sibling)
                .cycle(i)
                .label("trap")
                .command(block, trap_sibling_id, 64)?,
        );
        for kind in [
            PlanKind::OldIncarnationStale,
            PlanKind::NewIncarnationMutation,
            PlanKind::NewIncarnationDuplicate,
        ] {
            push_action(
                actions,
                PlannedAction::new(block, kind, retained)
                    .tenant(trap_tenant)
                    .cycle(i)
                    .label("trap")
                    .command(block, trap_seed_id, 64)?,
            );
        }

        let cpu_tenant = (block + i + 16) % TENANT_COUNT;
        let cpu_sibling = (cpu_tenant + 1) % TENANT_COUNT;
        let cpu_sibling_id = 2_000_000_u64 + u64::from(i) * 4 + 2;
        let cpu_seed_id = cpu_sibling_id + 1;
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::FaultSeed, retained)
                .tenant(cpu_tenant)
                .cycle(i)
                .label("cpu")
                .command(block, cpu_seed_id, 64)?,
        );
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::CpuFault, retained)
                .tenant(cpu_tenant)
                .cycle(i),
        );
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::SiblingProbe, retained)
                .tenant(cpu_sibling)
                .cycle(i)
                .label("cpu")
                .command(block, cpu_sibling_id, 64)?,
        );
        for kind in [
            PlanKind::OldIncarnationStale,
            PlanKind::NewIncarnationMutation,
            PlanKind::NewIncarnationDuplicate,
        ] {
            push_action(
                actions,
                PlannedAction::new(block, kind, retained)
                    .tenant(cpu_tenant)
                    .cycle(i)
                    .label("cpu")
                    .command(block, cpu_seed_id, 64)?,
            );
        }
    }
    Ok(())
}

fn plan_updates(actions: &mut Vec<PlannedAction>, block: u32) -> Result<(), WorkloadError> {
    let retained = block != 0;
    for cycle in 0..UPDATE_CYCLES {
        let (old_version, failed_artifact, new_version) = if cycle % 2 == 0 {
            ("A", "bad_B", "B")
        } else {
            ("B", "bad_A", "A")
        };
        for valid in [false, true] {
            let attempt = cycle * 2 + u32::from(valid);
            let target_artifact = if valid { new_version } else { failed_artifact };
            for tenant in 0..UPDATE_TARGETS {
                push_action(
                    actions,
                    PlannedAction::new(block, PlanKind::UpdateBaselineSnapshot, retained)
                        .tenant(tenant)
                        .cycle(cycle)
                        .attempt(attempt)
                        .label(old_version),
                );
                push_action(
                    actions,
                    PlannedAction::new(block, PlanKind::UpdateWorkerStart, retained)
                        .tenant(tenant)
                        .cycle(cycle)
                        .attempt(attempt),
                );
                let anchor_id = worker_command_id(attempt, tenant, 0)?;
                push_action(
                    actions,
                    PlannedAction::new(block, PlanKind::UpdateWorkerAnchorRead, retained)
                        .tenant(tenant)
                        .cycle(cycle)
                        .attempt(attempt)
                        .label("read")
                        .command(block, anchor_id, 64)?,
                );
            }
            push_action(
                actions,
                PlannedAction::new(block, PlanKind::RolloutRequest, retained)
                    .cycle(cycle)
                    .attempt(attempt)
                    .label(format!(
                        "{old_version}->{target_artifact};activation_failure_tenant=7"
                    )),
            );
            push_action(
                actions,
                PlannedAction::new(block, PlanKind::UpdateWorkerContinuous, retained)
                    .cycle(cycle)
                    .attempt(attempt)
                    .label("seq_odd=increment;seq_even=read"),
            );
            push_action(
                actions,
                PlannedAction::new(block, PlanKind::RolloutTerminal, retained)
                    .cycle(cycle)
                    .attempt(attempt)
                    .label(if valid {
                        "succeeded"
                    } else {
                        "failed_rolled_back"
                    }),
            );
            push_action(
                actions,
                PlannedAction::new(block, PlanKind::UpdateWorkerDrain, retained)
                    .cycle(cycle)
                    .attempt(attempt),
            );
            for tenant in 0..UPDATE_TARGETS {
                let anchor_id = worker_command_id(attempt, tenant, 0)?;
                push_action(
                    actions,
                    PlannedAction::new(block, PlanKind::UpdateCompletedReplay, retained)
                        .tenant(tenant)
                        .cycle(cycle)
                        .attempt(attempt)
                        .label("exact_anchor_cache_replay")
                        .command(block, anchor_id, 64)?,
                );
            }
            for wave in 0..PROOF_WAVES {
                for tenant in 0..UPDATE_TARGETS {
                    let proof_id = proof_command_id(attempt, tenant, wave)?;
                    push_action(
                        actions,
                        PlannedAction::new(block, PlanKind::UpdateProofSnapshot, retained)
                            .tenant(tenant)
                            .cycle(cycle)
                            .attempt(attempt)
                            .wave(wave)
                            .label(if valid { new_version } else { old_version })
                            .command(block, proof_id, 64)?,
                    );
                }
            }
        }
    }
    Ok(())
}

fn plan_authority_and_cleanup(
    actions: &mut Vec<PlannedAction>,
    block: u32,
) -> Result<(), WorkloadError> {
    let retained = block != 0;
    let authority_rotation = (block as usize) % AUTHORITY_CANARY_IDS.len();
    for offset in 0..AUTHORITY_CANARY_IDS.len() {
        let id = AUTHORITY_CANARY_IDS[(authority_rotation + offset) % AUTHORITY_CANARY_IDS.len()];
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::AuthorityPositiveControl, retained).label(id),
        );
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::AuthorityCanary, retained).label(id),
        );
    }

    let rotated_tenant = |slot: u32| (block + slot) % TENANT_COUNT;
    for slot in 0..TENANT_COUNT {
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::MainTeardown, retained)
                .tenant(rotated_tenant(slot)),
        );
    }
    push_action(
        actions,
        PlannedAction::new(block, PlanKind::MainActiveZero, retained),
    );

    for cycle in 0..(CLEANUP_WARMUP_CYCLES + CLEANUP_RETAINED_CYCLES) {
        let cleanup_retained = retained && cycle >= CLEANUP_WARMUP_CYCLES;
        let cleanup_tenant = |slot: u32| (block + cycle + slot) % TENANT_COUNT;
        for slot in 0..TENANT_COUNT {
            push_action(
                actions,
                PlannedAction::new(block, PlanKind::CleanupCreate, cleanup_retained)
                    .tenant(cleanup_tenant(slot))
                    .cycle(cycle),
            );
        }
        for slot in 0..TENANT_COUNT {
            let tenant = cleanup_tenant(slot);
            let id = cleanup_command_id(cycle, tenant)?;
            push_action(
                actions,
                PlannedAction::new(block, PlanKind::CleanupIncrement, cleanup_retained)
                    .tenant(tenant)
                    .cycle(cycle)
                    .command(block, id, 64)?,
            );
        }
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::CleanupQuiesce, cleanup_retained).cycle(cycle),
        );
        for slot in 0..TENANT_COUNT {
            push_action(
                actions,
                PlannedAction::new(block, PlanKind::CleanupSnapshot, cleanup_retained)
                    .tenant(cleanup_tenant(slot))
                    .cycle(cycle),
            );
        }
        for slot in 0..TENANT_COUNT {
            push_action(
                actions,
                PlannedAction::new(block, PlanKind::CleanupTeardown, cleanup_retained)
                    .tenant(cleanup_tenant(slot))
                    .cycle(cycle),
            );
        }
        for slot in 0..TENANT_COUNT {
            let tenant = cleanup_tenant(slot);
            let id = cleanup_command_id(cycle, tenant)?;
            push_action(
                actions,
                PlannedAction::new(block, PlanKind::CleanupStaleProbe, cleanup_retained)
                    .tenant(tenant)
                    .cycle(cycle)
                    .command(block, id, 64)?,
            );
        }
        push_action(
            actions,
            PlannedAction::new(block, PlanKind::CleanupActiveZero, cleanup_retained).cycle(cycle),
        );
        for offset_ms in [1_000, 1_100, 1_200] {
            push_action(
                actions,
                PlannedAction::new(block, PlanKind::CleanupPostRss, cleanup_retained)
                    .cycle(cycle)
                    .sample_offset(offset_ms),
            );
        }
    }
    Ok(())
}

fn count_kind(actions: &[PlannedAction], kind: PlanKind) -> u64 {
    actions.iter().filter(|action| action.kind == kind).count() as u64
}

fn validate_exact_plan_counts(actions: &[PlannedAction], block: u32) -> Result<u64, WorkloadError> {
    let exact = [
        (PlanKind::InitialCreate, 32),
        (PlanKind::NormalWarmupIncrement, 1_024),
        (PlanKind::NormalIncrement, 10_240),
        (PlanKind::DelayedOriginalDuplicate, 80),
        (PlanKind::CrossTenantFirst, 80),
        (PlanKind::CrossTenantDuplicate, 80),
        (PlanKind::Oversize1025Rejected, 1),
        (PlanKind::Boundary1024Accepted, 1),
        (PlanKind::PressureGateClose, 1),
        (PlanKind::PressureAccepted, 64),
        (PlanKind::PressureOverflowRejected, 1),
        (PlanKind::PressureGateOpen, 1),
        (PlanKind::PressureDrain, 1),
        (PlanKind::PressureRetry, 1),
        (PlanKind::FaultSeed, 60),
        (PlanKind::TrapFault, 30),
        (PlanKind::CpuFault, 30),
        (PlanKind::SiblingProbe, 60),
        (PlanKind::OldIncarnationStale, 60),
        (PlanKind::NewIncarnationMutation, 60),
        (PlanKind::NewIncarnationDuplicate, 60),
        (PlanKind::UpdateBaselineSnapshot, 960),
        (PlanKind::UpdateWorkerStart, 960),
        (PlanKind::UpdateWorkerAnchorRead, 960),
        (PlanKind::RolloutRequest, 60),
        (PlanKind::UpdateWorkerContinuous, 60),
        (PlanKind::RolloutTerminal, 60),
        (PlanKind::UpdateWorkerDrain, 60),
        (PlanKind::UpdateCompletedReplay, 960),
        (PlanKind::UpdateProofSnapshot, 2_880),
        (PlanKind::AuthorityPositiveControl, 6),
        (PlanKind::AuthorityCanary, 6),
        (PlanKind::MainTeardown, 32),
        (PlanKind::MainActiveZero, 1),
        (PlanKind::CleanupCreate, 1_120),
        (PlanKind::CleanupIncrement, 1_120),
        (PlanKind::CleanupQuiesce, 35),
        (PlanKind::CleanupSnapshot, 1_120),
        (PlanKind::CleanupTeardown, 1_120),
        (PlanKind::CleanupStaleProbe, 1_120),
        (PlanKind::CleanupActiveZero, 35),
        (PlanKind::CleanupPostRss, 105),
    ];
    for (kind, expected) in exact {
        let actual = count_kind(actions, kind);
        if actual != expected {
            return Err(WorkloadError::Scenario(format!(
                "{kind:?}: expected {expected}, found {actual}"
            )));
        }
    }
    if actions.len() as u64 != EXPECTED_STATIC_ACTIONS_PER_BLOCK {
        return Err(WorkloadError::Scenario(format!(
            "static plan cardinality: expected {EXPECTED_STATIC_ACTIONS_PER_BLOCK}, found {}",
            actions.len()
        )));
    }
    let retained_actions = actions.iter().filter(|action| action.retained).count() as u64;
    let expected_retained = if block == 0 {
        0
    } else {
        EXPECTED_RETAINED_ACTIONS
    };
    if retained_actions != expected_retained {
        return Err(WorkloadError::Scenario(format!(
            "retained action cardinality: expected {expected_retained}, found {retained_actions}"
        )));
    }
    Ok(retained_actions)
}

fn validate_command_metadata(actions: &[PlannedAction], block: u32) -> Result<(), WorkloadError> {
    let mut command_payloads: BTreeMap<u64, (u32, String)> = BTreeMap::new();
    for action in actions {
        match (
            &action.command_id,
            action.payload_bytes,
            &action.payload_sha256,
        ) {
            (Some(id_text), Some(len), Some(digest)) => {
                let id = id_text
                    .parse::<u64>()
                    .map_err(|_| WorkloadError::Scenario("non-u64 command ID in plan".into()))?;
                if id.to_string() != *id_text {
                    return Err(WorkloadError::Scenario(
                        "command ID is not canonical unsigned decimal".into(),
                    ));
                }
                let expected_digest = sha256_hex(&deterministic_payload(block, id, len)?);
                if *digest != expected_digest {
                    return Err(WorkloadError::Scenario(format!(
                        "payload digest drift for command {id}"
                    )));
                }
                if let Some(previous) = command_payloads.insert(id, (len, digest.clone())) {
                    if previous != (len, digest.clone()) {
                        return Err(WorkloadError::Scenario(format!(
                            "command {id} reused with different bytes"
                        )));
                    }
                }
            }
            (None, None, None) => {}
            _ => {
                return Err(WorkloadError::Scenario(
                    "partial command/payload metadata".into(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_plan_order(actions: &[PlannedAction], block: u32) -> Result<(), WorkloadError> {
    let initial: Vec<_> = actions
        .iter()
        .filter(|action| action.kind == PlanKind::InitialCreate)
        .collect();
    for (slot, action) in initial.iter().enumerate() {
        if action.tenant_id != Some((block + slot as u32) % TENANT_COUNT) {
            return Err(WorkloadError::Scenario(
                "initial create rotation drifted".into(),
            ));
        }
    }

    for (kind, rounds) in [
        (PlanKind::NormalWarmupIncrement, NORMAL_WARMUP_ROUNDS),
        (PlanKind::NormalIncrement, NORMAL_ROUNDS),
    ] {
        let phase: Vec<_> = actions
            .iter()
            .filter(|action| action.kind == kind)
            .collect();
        for round in 0..rounds {
            for slot in 0..TENANT_COUNT {
                let action = phase[(round * TENANT_COUNT + slot) as usize];
                let expected_tenant = (block + round + slot) % TENANT_COUNT;
                if action.round != Some(round) || action.tenant_id != Some(expected_tenant) {
                    return Err(WorkloadError::Scenario(format!(
                        "{kind:?} tenant order drifted at round {round} slot {slot}"
                    )));
                }
            }
        }
    }

    let pressure: Vec<_> = actions
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
    let pressure_fixed = [
        (0, PlanKind::Oversize1025Rejected),
        (1, PlanKind::Boundary1024Accepted),
        (2, PlanKind::PressureGateClose),
        (67, PlanKind::PressureOverflowRejected),
        (68, PlanKind::PressureGateOpen),
        (69, PlanKind::PressureDrain),
        (70, PlanKind::PressureRetry),
    ];
    for (index, kind) in pressure_fixed {
        if pressure[index].kind != kind {
            return Err(WorkloadError::Scenario("pressure ordering drifted".into()));
        }
    }

    for action in actions
        .iter()
        .filter(|action| matches!(action.kind, PlanKind::TrapFault | PlanKind::CpuFault))
    {
        let next = actions
            .get(action.ordinal as usize + 1)
            .ok_or_else(|| WorkloadError::Scenario("fault missing sibling successor".into()))?;
        let expected_label = if action.kind == PlanKind::TrapFault {
            "trap"
        } else {
            "cpu"
        };
        if next.kind != PlanKind::SiblingProbe || next.label.as_deref() != Some(expected_label) {
            return Err(WorkloadError::Scenario(
                "fault and sibling are not back-to-back".into(),
            ));
        }
    }

    let update_anchors: BTreeMap<_, _> = actions
        .iter()
        .filter(|action| action.kind == PlanKind::UpdateWorkerAnchorRead)
        .map(|action| ((action.rollout_index, action.tenant_id), action))
        .collect();
    for replay in actions
        .iter()
        .filter(|action| action.kind == PlanKind::UpdateCompletedReplay)
    {
        let key = (replay.rollout_index, replay.tenant_id);
        let anchor = update_anchors.get(&key).ok_or_else(|| {
            WorkloadError::Scenario("update cache replay has no matching anchor".into())
        })?;
        if replay.cycle != anchor.cycle
            || replay.command_id != anchor.command_id
            || replay.payload_bytes != anchor.payload_bytes
            || replay.payload_sha256 != anchor.payload_sha256
            || replay.ordinal <= anchor.ordinal
        {
            return Err(WorkloadError::Scenario(
                "update cache replay is not the exact completed anchor identity".into(),
            ));
        }
    }

    let cleanup_increments: BTreeMap<_, _> = actions
        .iter()
        .filter(|action| action.kind == PlanKind::CleanupIncrement)
        .map(|action| ((action.cycle, action.tenant_id), action))
        .collect();
    for stale in actions
        .iter()
        .filter(|action| action.kind == PlanKind::CleanupStaleProbe)
    {
        let increment = cleanup_increments
            .get(&(stale.cycle, stale.tenant_id))
            .ok_or_else(|| {
                WorkloadError::Scenario("cleanup stale probe has no matching increment".into())
            })?;
        if stale.command_id != increment.command_id
            || stale.payload_bytes != increment.payload_bytes
            || stale.payload_sha256 != increment.payload_sha256
            || stale.ordinal <= increment.ordinal
        {
            return Err(WorkloadError::Scenario(
                "cleanup stale probe is not bound to the byte-identical old command".into(),
            ));
        }
    }

    let authority: Vec<_> = actions
        .iter()
        .filter(|action| {
            matches!(
                action.kind,
                PlanKind::AuthorityPositiveControl | PlanKind::AuthorityCanary
            )
        })
        .collect();
    for offset in 0..AUTHORITY_CANARY_IDS.len() {
        let expected =
            AUTHORITY_CANARY_IDS[((block as usize) + offset) % AUTHORITY_CANARY_IDS.len()];
        let positive = authority[offset * 2];
        let canary = authority[offset * 2 + 1];
        if positive.kind != PlanKind::AuthorityPositiveControl
            || canary.kind != PlanKind::AuthorityCanary
            || positive.label.as_deref() != Some(expected)
            || canary.label.as_deref() != Some(expected)
        {
            return Err(WorkloadError::Scenario(
                "six-canary rotation or positive-control pairing drifted".into(),
            ));
        }
    }
    Ok(())
}

pub fn verify_full_shape(actions: &[PlannedAction]) -> Result<FullShapeSummary, WorkloadError> {
    let first = actions
        .first()
        .ok_or_else(|| WorkloadError::Scenario("full plan is empty".into()))?;
    let block = first.block;
    block_order(block)?;
    for (ordinal, action) in actions.iter().enumerate() {
        if action.ordinal != ordinal as u64 || action.block != block {
            return Err(WorkloadError::Scenario(
                "action ordinals or block binding are not contiguous/uniform".into(),
            ));
        }
    }
    let retained_actions = validate_exact_plan_counts(actions, block)?;
    validate_command_metadata(actions, block)?;
    validate_plan_order(actions, block)?;

    let failed_rollouts = actions
        .iter()
        .filter(|action| {
            action.kind == PlanKind::RolloutTerminal
                && action.label.as_deref() == Some("failed_rolled_back")
        })
        .count() as u64;
    let valid_rollouts = actions
        .iter()
        .filter(|action| {
            action.kind == PlanKind::RolloutTerminal && action.label.as_deref() == Some("succeeded")
        })
        .count() as u64;
    if failed_rollouts != 30 || valid_rollouts != 30 {
        return Err(WorkloadError::Scenario(
            "failed/valid rollout terminals drifted".into(),
        ));
    }

    Ok(FullShapeSummary {
        block: block_order(block)?,
        total_actions: actions.len() as u64,
        retained_actions,
        normal_warmup_commands: count_kind(actions, PlanKind::NormalWarmupIncrement),
        normal_commands: count_kind(actions, PlanKind::NormalIncrement),
        delayed_duplicate_groups: count_kind(actions, PlanKind::DelayedOriginalDuplicate),
        pressure_accepted_before_overflow: count_kind(actions, PlanKind::PressureAccepted),
        pressure_overflow_rejections: count_kind(actions, PlanKind::PressureOverflowRejected),
        pressure_retries: count_kind(actions, PlanKind::PressureRetry),
        fault_seeds: count_kind(actions, PlanKind::FaultSeed),
        trap_faults: count_kind(actions, PlanKind::TrapFault),
        cpu_faults: count_kind(actions, PlanKind::CpuFault),
        sibling_probes: count_kind(actions, PlanKind::SiblingProbe),
        recovery_triplets: count_kind(actions, PlanKind::OldIncarnationStale),
        failed_rollouts,
        valid_rollouts,
        update_anchor_reads: count_kind(actions, PlanKind::UpdateWorkerAnchorRead),
        update_worker_descriptors: count_kind(actions, PlanKind::UpdateWorkerContinuous),
        update_completed_replays: count_kind(actions, PlanKind::UpdateCompletedReplay),
        update_proof_snapshots: count_kind(actions, PlanKind::UpdateProofSnapshot),
        authority_positive_controls: count_kind(actions, PlanKind::AuthorityPositiveControl),
        authority_canaries: count_kind(actions, PlanKind::AuthorityCanary),
        main_teardowns: count_kind(actions, PlanKind::MainTeardown),
        cleanup_warmup_cycles: u64::from(CLEANUP_WARMUP_CYCLES),
        cleanup_retained_cycles: u64::from(CLEANUP_RETAINED_CYCLES),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerOperation {
    Read,
    IncrementOne,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRetryReason {
    Backpressure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerTransportOutcome {
    RetryableRejection {
        reason: WorkerRetryReason,
        rejected_ns: u64,
    },
    AcceptedTerminal {
        terminal_ns: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerTransportTrace {
    pub request_id: u64,
    pub command_id: u64,
    pub payload_sha256: String,
    pub issued_ns: u64,
    pub outcome: WorkerTransportOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerLogicalTrace {
    pub sequence: u64,
    pub operation: WorkerOperation,
    pub command_id: u64,
    pub payload_bytes: u32,
    pub payload_sha256: String,
    pub rollout_flush_ns: u64,
    pub candidate_terminal_ns: u64,
    pub drain_completed_ns: u64,
    pub proof_started_ns: u64,
    pub transports: Vec<WorkerTransportTrace>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerTraceCardinality {
    pub logical_operations: u64,
    pub continuous_logical_operations: u64,
    pub transport_attempts: u64,
    pub retries: u64,
}

pub fn validate_worker_trace_cardinality(
    block: u32,
    attempt: u32,
    tenant: u32,
    operations: &[WorkerLogicalTrace],
) -> Result<WorkerTraceCardinality, WorkloadError> {
    block_order(block)?;
    if attempt >= UPDATE_ATTEMPTS || tenant >= UPDATE_TARGETS || operations.is_empty() {
        return Err(WorkloadError::Scenario(
            "worker trace must identify a frozen attempt/tenant and contain anchor sequence zero"
                .into(),
        ));
    }
    let first = &operations[0];
    let window = (
        first.rollout_flush_ns,
        first.candidate_terminal_ns,
        first.drain_completed_ns,
        first.proof_started_ns,
    );
    if window.0 == 0 || !(window.0 < window.1 && window.1 <= window.2 && window.2 <= window.3) {
        return Err(WorkloadError::Scenario(
            "worker trace window must be rollout_flush < candidate_terminal <= drain <= proof"
                .into(),
        ));
    }
    let mut transport_attempts = 0_u64;
    let mut retries = 0_u64;
    let mut request_ids = BTreeSet::new();
    let mut previous_terminal_ns = None;
    for (index, operation) in operations.iter().enumerate() {
        let expected_sequence = index as u64;
        if operation.sequence != expected_sequence || operation.sequence > WORKER_MAX_SEQUENCE {
            return Err(WorkloadError::Scenario(
                "worker logical sequences must be contiguous from zero and within 2^40-1".into(),
            ));
        }
        if (
            operation.rollout_flush_ns,
            operation.candidate_terminal_ns,
            operation.drain_completed_ns,
            operation.proof_started_ns,
        ) != window
        {
            return Err(WorkloadError::Scenario(
                "all logical records for a worker attempt must share the observed time window"
                    .into(),
            ));
        }
        let expected_operation = if operation.sequence == 0 || operation.sequence % 2 == 0 {
            WorkerOperation::Read
        } else {
            WorkerOperation::IncrementOne
        };
        if operation.operation != expected_operation {
            return Err(WorkloadError::Scenario(
                "worker operation must be sequence zero read, then odd increment-one/even read"
                    .into(),
            ));
        }
        let expected_id = worker_command_id(attempt, tenant, operation.sequence)?;
        let expected_payload_sha256 = sha256_hex(&deterministic_payload(block, expected_id, 64)?);
        if operation.command_id != expected_id
            || operation.payload_bytes != 64
            || operation.payload_sha256 != expected_payload_sha256
        {
            return Err(WorkloadError::Scenario(
                "worker command ID, 64-byte payload length, or deterministic payload digest drifted"
                    .into(),
            ));
        }
        if !(1..=2).contains(&operation.transports.len()) {
            return Err(WorkloadError::Scenario(
                "worker logical operation must have one transport or one rejection plus one retry"
                    .into(),
            ));
        }
        for transport in &operation.transports {
            if !request_ids.insert(transport.request_id) {
                return Err(WorkloadError::Scenario(
                    "worker transport request_id must be unique across the trace".into(),
                ));
            }
            if transport.command_id != operation.command_id
                || transport.payload_sha256 != operation.payload_sha256
                || transport.issued_ns >= operation.candidate_terminal_ns
            {
                return Err(WorkloadError::Scenario(
                    "worker transport must preserve command ID/bytes and be issued before terminal"
                        .into(),
                ));
            }
        }
        let (accepted_issued_ns, accepted_terminal_ns) = match operation.transports.as_slice() {
            [accepted] => match accepted.outcome {
                WorkerTransportOutcome::AcceptedTerminal { terminal_ns } => {
                    (accepted.issued_ns, terminal_ns)
                }
                WorkerTransportOutcome::RetryableRejection { .. } => {
                    return Err(WorkloadError::Scenario(
                        "a single worker transport must be accepted and terminal".into(),
                    ));
                }
            },
            [rejected, accepted] => {
                let rejected_ns = match rejected.outcome {
                    WorkerTransportOutcome::RetryableRejection {
                        reason: WorkerRetryReason::Backpressure,
                        rejected_ns,
                    } => rejected_ns,
                    WorkerTransportOutcome::AcceptedTerminal { .. } => {
                        return Err(WorkloadError::Scenario(
                            "the first of two worker transports must be backpressure rejection"
                                .into(),
                        ));
                    }
                };
                let terminal_ns = match accepted.outcome {
                    WorkerTransportOutcome::AcceptedTerminal { terminal_ns } => terminal_ns,
                    WorkerTransportOutcome::RetryableRejection { .. } => {
                        return Err(WorkloadError::Scenario(
                            "the one allowed worker retry must be accepted and terminal".into(),
                        ));
                    }
                };
                if rejected.request_id == accepted.request_id
                    || rejected_ns <= rejected.issued_ns
                    || accepted.issued_ns <= rejected_ns
                {
                    return Err(WorkloadError::Scenario(
                        "worker retry must use a new request after observed pre-acceptance rejection"
                            .into(),
                    ));
                }
                retries += 1;
                (accepted.issued_ns, terminal_ns)
            }
            _ => unreachable!("transport cardinality checked above"),
        };
        if accepted_terminal_ns <= accepted_issued_ns
            || accepted_terminal_ns > operation.drain_completed_ns
            || accepted_terminal_ns > operation.proof_started_ns
        {
            return Err(WorkloadError::Scenario(
                "accepted worker operation must become terminal after issue and by drain/proof"
                    .into(),
            ));
        }
        let first_issued_ns = operation.transports[0].issued_ns;
        if operation.sequence == 0 {
            if accepted_terminal_ns >= operation.rollout_flush_ns {
                return Err(WorkloadError::Scenario(
                    "worker sequence zero read must complete before rollout flush".into(),
                ));
            }
        } else if first_issued_ns < operation.rollout_flush_ns {
            return Err(WorkloadError::Scenario(
                "continuous worker sequence was issued before rollout flush".into(),
            ));
        }
        if let Some(previous) = previous_terminal_ns {
            if first_issued_ns < previous {
                return Err(WorkloadError::Scenario(
                    "worker has more than one outstanding logical operation".into(),
                ));
            }
        }
        previous_terminal_ns = Some(accepted_terminal_ns);
        transport_attempts += operation.transports.len() as u64;
    }
    let logical_operations = operations.len() as u64;
    Ok(WorkerTraceCardinality {
        logical_operations,
        continuous_logical_operations: logical_operations - 1,
        transport_attempts,
        retries,
    })
}

pub fn hash_canonical_plan_bundle(plans: &[Vec<PlannedAction>]) -> Result<String, WorkloadError> {
    if plans.len() != BLOCK_COUNT as usize {
        return Err(WorkloadError::Scenario(
            "canonical plan bundle must contain blocks 0 through 9 exactly once".into(),
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update(b"embedded-v3-plan-bundle-v1\0");
    let scenario_hash = EMBEDDED_SCENARIO_SHA256.as_bytes();
    hasher.update(
        u32::try_from(scenario_hash.len())
            .map_err(|_| WorkloadError::Scenario("scenario hash frame overflow".into()))?
            .to_be_bytes(),
    );
    hasher.update(scenario_hash);
    hasher.update(BLOCK_COUNT.to_be_bytes());
    for (block, actions) in plans.iter().enumerate() {
        hasher.update((block as u32).to_be_bytes());
        hasher.update(
            u64::try_from(actions.len())
                .map_err(|_| WorkloadError::Scenario("action-count frame overflow".into()))?
                .to_be_bytes(),
        );
        for action in actions {
            let canonical = serde_json::to_vec(action)?;
            hasher.update(
                u32::try_from(canonical.len())
                    .map_err(|_| WorkloadError::Scenario("action frame overflow".into()))?
                    .to_be_bytes(),
            );
            hasher.update(canonical);
        }
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub fn plan_bundle_sha256(scenario: &ScenarioDocument) -> Result<String, WorkloadError> {
    scenario.validate()?;
    let plans = (0..BLOCK_COUNT)
        .map(|block| build_full_plan(scenario, block))
        .collect::<Result<Vec<_>, _>>()?;
    hash_canonical_plan_bundle(&plans)
}

pub use crate::state_oracle::*;

pub fn business_result_digest(generation: u64, counter: i64) -> String {
    let canonical = format!(r#"{{"counter":{counter},"generation":{generation}}}"#);
    sha256_hex(canonical.as_bytes())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeadlinePhase {
    Admission,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeadlineState {
    deadline_ns: u64,
    revision: u64,
    phase: DeadlinePhase,
}

#[derive(Debug, Default)]
pub struct DeadlineTracker {
    next_revision: u64,
    active: BTreeMap<u64, DeadlineState>,
    heap: BinaryHeap<Reverse<(u64, u64, u64, DeadlinePhase)>>,
}

impl DeadlineTracker {
    pub fn register(
        &mut self,
        request_id: u64,
        written_ns: u64,
        admission_budget_ns: u64,
    ) -> Result<(), WorkloadError> {
        if self.active.contains_key(&request_id) {
            return Err(WorkloadError::State(format!(
                "request {request_id} already has an active deadline"
            )));
        }
        self.install(
            request_id,
            written_ns,
            admission_budget_ns,
            DeadlinePhase::Admission,
        )
    }

    pub fn accepted(
        &mut self,
        request_id: u64,
        accepted_ns: u64,
        terminal_budget_ns: u64,
    ) -> Result<(), WorkloadError> {
        match self.active.get(&request_id) {
            Some(state) if state.phase == DeadlinePhase::Admission => {}
            Some(_) => {
                return Err(WorkloadError::State(format!(
                    "request {request_id} is already waiting for terminal"
                )))
            }
            None => {
                return Err(WorkloadError::State(format!(
                    "request {request_id} has no live admission deadline"
                )))
            }
        }
        self.install(
            request_id,
            accepted_ns,
            terminal_budget_ns,
            DeadlinePhase::Terminal,
        )
    }

    pub fn terminal(&mut self, request_id: u64) -> Result<(), WorkloadError> {
        self.active.remove(&request_id).map(|_| ()).ok_or_else(|| {
            WorkloadError::State(format!(
                "request {request_id} has no live deadline at terminal"
            ))
        })
    }

    pub fn expire(&mut self, now_ns: u64) -> Vec<(u64, DeadlinePhase)> {
        let mut expired = Vec::new();
        while let Some(Reverse((deadline_ns, request_id, revision, phase))) =
            self.heap.peek().copied()
        {
            if deadline_ns > now_ns {
                break;
            }
            self.heap.pop();
            let current = self.active.get(&request_id).copied();
            if current
                == Some(DeadlineState {
                    deadline_ns,
                    revision,
                    phase,
                })
            {
                self.active.remove(&request_id);
                expired.push((request_id, phase));
            }
        }
        expired
    }

    fn install(
        &mut self,
        request_id: u64,
        start_ns: u64,
        budget_ns: u64,
        phase: DeadlinePhase,
    ) -> Result<(), WorkloadError> {
        let deadline_ns = start_ns.checked_add(budget_ns).ok_or_else(|| {
            WorkloadError::State(format!("request {request_id} deadline overflow"))
        })?;
        self.next_revision = self
            .next_revision
            .checked_add(1)
            .ok_or_else(|| WorkloadError::State("deadline revision overflow".into()))?;
        let state = DeadlineState {
            deadline_ns,
            revision: self.next_revision,
            phase,
        };
        self.active.insert(request_id, state);
        self.heap.push(Reverse((
            deadline_ns,
            request_id,
            state.revision,
            state.phase,
        )));
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CpuStartOrigin {
    GuestFirstActionObserver,
    HostSynthesized,
}

pub fn validate_cpu_observation(
    write_ns: u64,
    accepted_ns: u64,
    execution_started_ns: u64,
    failed_ns: u64,
    ready_ns: u64,
    origin: CpuStartOrigin,
) -> Result<(), WorkloadError> {
    if origin != CpuStartOrigin::GuestFirstActionObserver {
        return Err(WorkloadError::State(
            "CPU execution_started was not guest-originated first-action observer proof".into(),
        ));
    }
    let start = execution_started_ns
        .checked_sub(accepted_ns)
        .ok_or_else(|| WorkloadError::State("CPU execution_started preceded admission".into()))?;
    let fail = failed_ns
        .checked_sub(execution_started_ns)
        .ok_or_else(|| WorkloadError::State("CPU deadline preceded execution_started".into()))?;
    let recovery = ready_ns
        .checked_sub(write_ns)
        .ok_or_else(|| WorkloadError::State("CPU ready preceded write".into()))?;
    if start == 0 || start > 20_000_000 {
        return Err(WorkloadError::State(
            "CPU start proof outside (0,20ms]".into(),
        ));
    }
    if !(40_000_000..=100_000_000).contains(&fail) {
        return Err(WorkloadError::State(
            "CPU interrupt outside [40ms,100ms]".into(),
        ));
    }
    if recovery > 500_000_000 {
        return Err(WorkloadError::State("CPU recovery exceeded 500ms".into()));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityCanaryEvidence {
    pub terminal: AuthorityProbeTerminalEvent,
    pub positive_control_detected: bool,
    pub tail_complete: bool,
    pub tail_elapsed_ms: u64,
    pub secret_or_digest_observed: bool,
    pub external_effect_observed: bool,
    pub sibling_probe_succeeded: bool,
    pub production_linker_inventory_contains_exact_import: bool,
    pub actual_link_failure_observed: bool,
    pub typed_production_policy_denial_observed: bool,
}

pub fn validate_authority_canary(evidence: &AuthorityCanaryEvidence) -> Result<(), WorkloadError> {
    if !evidence.positive_control_detected
        || !evidence.tail_complete
        || evidence.tail_elapsed_ms < 500
        || evidence.secret_or_digest_observed
        || evidence.external_effect_observed
        || !evidence.sibling_probe_succeeded
    {
        return Err(WorkloadError::State(
            "authority canary requires a detected positive control, complete 500ms zero-effect tail, and live sibling"
                .into(),
        ));
    }
    if evidence.terminal.guest_return_sha256 != EMPTY_SHA256 {
        return Err(WorkloadError::State(
            "authority denial must have no guest return or guest-trap substitute".into(),
        ));
    }
    match evidence.terminal.result {
        AuthorityProbeResult::AbsentAtLink => {
            if !matches!(
                evidence.terminal.stage,
                AuthorityProbeStage::Compile | AuthorityProbeStage::Instantiate
            ) || evidence.terminal.error_class != AuthorityProbeErrorClass::UnknownImport
                || evidence.production_linker_inventory_contains_exact_import
                || !evidence.actual_link_failure_observed
                || evidence.typed_production_policy_denial_observed
            {
                return Err(WorkloadError::State(
                    "absent_at_link requires exact production inventory exclusion plus actual compile/instantiate link failure"
                        .into(),
                ));
            }
        }
        AuthorityProbeResult::PolicyDenied => {
            let expected_error = match evidence.terminal.probe {
                AuthorityProbeKind::WasiP1FsRead | AuthorityProbeKind::WasiP1FsMutate => {
                    AuthorityProbeErrorClass::FilesystemCapabilityDenied
                }
                AuthorityProbeKind::LunaticTcp | AuthorityProbeKind::LunaticUdp => {
                    AuthorityProbeErrorClass::NetworkCapabilityDenied
                }
                AuthorityProbeKind::LunaticSqliteCreate => {
                    AuthorityProbeErrorClass::DatabaseCapabilityDenied
                }
                AuthorityProbeKind::ExtismHttp => AuthorityProbeErrorClass::HttpCapabilityDenied,
            };
            if !matches!(
                evidence.terminal.stage,
                AuthorityProbeStage::Instantiate | AuthorityProbeStage::Invoke
            ) || evidence.terminal.error_class != expected_error
                || !evidence.production_linker_inventory_contains_exact_import
                || evidence.actual_link_failure_observed
                || !evidence.typed_production_policy_denial_observed
            {
                return Err(WorkloadError::State(
                    "policy_denied requires the exact production import to resolve and a typed production policy denial before effect"
                        .into(),
                ));
            }
        }
    }
    Ok(())
}

pub struct ImmutableRunDirectory {
    path: PathBuf,
}

impl ImmutableRunDirectory {
    pub fn create(root: &Path, run_id: &str) -> Result<Self, WorkloadError> {
        validate_run_id(run_id).map_err(WorkloadError::Scenario)?;
        fs::create_dir_all(root)?;
        let path = root.join(run_id);
        fs::create_dir(&path)?;
        let manifest = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.join("run-manifest.json"))?;
        serde_json::to_writer_pretty(
            manifest,
            &serde_json::json!({"schema_version": 3, "run_id": run_id, "immutable": true}),
        )?;
        Ok(Self { path })
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn raw_trace_path(&self) -> PathBuf {
        self.path.join("raw-trace.ndjson")
    }
    pub fn write_summary<T: Serialize>(&self, summary: &T) -> Result<PathBuf, WorkloadError> {
        let path = self.path.join("summary.json");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        serde_json::to_writer_pretty(&mut file, summary)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        Ok(path)
    }
    pub fn create_evidence_file(&self, name: &str) -> Result<File, WorkloadError> {
        if name.contains(['/', '\\']) || name.is_empty() {
            return Err(WorkloadError::Scenario("invalid evidence filename".into()));
        }
        Ok(OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.path.join(name))?)
    }
}
