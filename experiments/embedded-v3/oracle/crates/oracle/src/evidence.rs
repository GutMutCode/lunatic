use std::collections::HashSet;
use std::convert::TryFrom;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::analysis::KeyMetrics;
use crate::protocol::CandidateKind;
use crate::workload::{block_order, EMBEDDED_PLAN_BUNDLE_SHA256, EMBEDDED_SCENARIO_SHA256};

pub const EVIDENCE_SCHEMA_VERSION: u32 = 3;
pub const EXPERIMENT_ID: &str = "embedded-v3-2026-07-23";
pub const RSS_READY_32_TOTAL_LIMIT_BYTES: u64 = 805_306_368;
pub const RSS_PEAK_DELTA_LIMIT_BYTES: u64 = 536_870_912;
pub const RSS_POST_CLEANUP_DELTA_LIMIT_BYTES: i64 = 67_108_864;
pub const RSS_CLEANUP_SLOPE_LIMIT_BYTES_PER_CYCLE: f64 = 1_048_576.0;

#[derive(Debug, Error)]
pub enum EvidenceError {
    #[error("evidence I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("evidence JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("evidence invariant failed: {0}")]
    Invariant(String),
    #[error("evidence content was changed: {0}")]
    Tampered(String),
    #[error("trace is already finalized")]
    TraceFinalized,
    #[error("trace record must be an object without a trace_seq field")]
    InvalidTraceRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct Sha256Digest(String);

impl Sha256Digest {
    pub fn parse(value: impl Into<String>) -> Result<Self, EvidenceError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(EvidenceError::Invariant(
                "SHA-256 must be 64 lowercase hexadecimal characters".into(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RelativeEvidencePath(String);

impl RelativeEvidencePath {
    pub fn parse(value: impl Into<String>) -> Result<Self, EvidenceError> {
        let value = value.into();
        if value.is_empty() || value.contains('\\') || Path::new(&value).is_absolute() {
            return Err(EvidenceError::Invariant(
                "evidence path must be a non-empty forward-slash relative path".into(),
            ));
        }
        if Path::new(&value)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(EvidenceError::Invariant(
                "evidence path cannot contain dot, parent, root, or prefix components".into(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RelativeEvidencePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactEvidence {
    pub path: RelativeEvidencePath,
    pub sha256: Sha256Digest,
    pub bytes: u64,
}

impl ArtifactEvidence {
    pub fn validate_nonempty(&self) -> Result<(), EvidenceError> {
        if self.bytes == 0 {
            return Err(EvidenceError::Invariant(format!(
                "artifact {} must not be empty",
                self.path.as_str()
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileEvidence {
    pub path: RelativeEvidencePath,
    pub sha256: Sha256Digest,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioEvidence {
    pub path: RelativeEvidencePath,
    pub sha256: Sha256Digest,
    pub schema_path: RelativeEvidencePath,
    pub schema_sha256: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleEvidence {
    pub revision: String,
    pub source_tree_sha256: Sha256Digest,
    pub source_snapshot: ArtifactEvidence,
    pub executable: ArtifactEvidence,
    pub protocol_source_sha256: Sha256Digest,
    pub raw_summary_schema_sha256: Sha256Digest,
    pub run_manifest_schema_sha256: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddedWasmEngineEvidence {
    pub implementation: String,
    pub version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantExecutionModel {
    TokioTasks,
    DedicatedThreads,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionTopologyEvidence {
    pub os_processes: u32,
    pub long_lived_helper_processes: u32,
    pub tenant_execution_model: TenantExecutionModel,
    pub tokio_worker_threads: u32,
    pub dedicated_tenant_threads: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateBuildEvidence {
    pub source_revision: String,
    pub source_tree_sha256: Sha256Digest,
    pub source_snapshot: ArtifactEvidence,
    pub executable: ArtifactEvidence,
    pub argv: Vec<String>,
    pub cargo_lock: ArtifactEvidence,
    pub sloc_manifest: ArtifactEvidence,
    pub sloc_result: ArtifactEvidence,
    pub production_sloc: u64,
    pub direct_dependencies: Vec<String>,
    pub transitive_package_count: u64,
    pub binary_bytes: u64,
    pub execution_topology: ExecutionTopologyEvidence,
    pub embedded_wasm_engine: EmbeddedWasmEngineEvidence,
    pub build_command: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuestArtifactsEvidence {
    #[serde(rename = "A")]
    pub a: ArtifactEvidence,
    #[serde(rename = "B")]
    pub b: ArtifactEvidence,
    #[serde(rename = "bad_A")]
    pub bad_a: ArtifactEvidence,
    #[serde(rename = "bad_B")]
    pub bad_b: ArtifactEvidence,
    pub authority: ArtifactEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentEvidence {
    pub os: String,
    pub cpu: String,
    pub logical_processors: u32,
    pub physical_memory_bytes: u64,
    pub rustc: String,
    pub cargo: String,
    pub oracle_wasmtime: String,
    pub tokio: String,
    pub node: String,
    pub machine_fingerprint_sha256: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffortEvidence {
    pub checkpoint_elapsed_minutes: u32,
    pub completion_elapsed_minutes: u32,
    pub tuning_elapsed_minutes: u32,
    pub tuning_revisions: u32,
    pub architecture_review_sha256: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunManifest {
    pub schema_version: u32,
    pub experiment_id: String,
    pub run_id: String,
    pub block_index: u32,
    pub retained: bool,
    pub candidate: CandidateKind,
    pub candidate_order_position: u32,
    pub freeze_utc: String,
    pub plan_bundle_sha256: Sha256Digest,
    pub authority_policy_sha256: Sha256Digest,
    pub authority_policy: ArtifactEvidence,
    pub source_review_sha256: Sha256Digest,
    pub source_review: ArtifactEvidence,
    pub scenario: ScenarioEvidence,
    pub oracle: OracleEvidence,
    pub candidate_build: CandidateBuildEvidence,
    pub guest_artifacts: GuestArtifactsEvidence,
    pub environment: EnvironmentEvidence,
    pub effort: EffortEvidence,
}

impl RunManifest {
    pub fn validate(&self) -> Result<(), EvidenceError> {
        if self.schema_version != EVIDENCE_SCHEMA_VERSION {
            return Err(invariant("run manifest schema_version must be 3"));
        }
        if self.experiment_id != EXPERIMENT_ID {
            return Err(invariant("run manifest experiment_id is not frozen"));
        }
        validate_run_id(&self.run_id)?;
        if self.block_index > 9 || self.retained != (self.block_index != 0) {
            return Err(invariant("block_index and retained do not match"));
        }
        let order = block_order(self.block_index)
            .map_err(|error| invariant(format!("invalid block order: {error}")))?;
        let position = usize::try_from(self.candidate_order_position)
            .map_err(|_| invariant("candidate_order_position cannot fit usize"))?;
        if order.candidates.get(position) != Some(&self.candidate) {
            return Err(invariant(
                "candidate and candidate_order_position do not match the frozen Latin order",
            ));
        }
        validate_canonical_utc(&self.freeze_utc)?;
        if self.scenario.path.as_str() != "scenario-final.json"
            || self.scenario.schema_path.as_str() != "schemas/scenario-final.schema.json"
            || self.scenario.sha256.as_str() != EMBEDDED_SCENARIO_SHA256
            || self.plan_bundle_sha256.as_str() != EMBEDDED_PLAN_BUNDLE_SHA256
        {
            return Err(invariant(
                "scenario or plan does not match the frozen workload",
            ));
        }
        if self.oracle.revision != self.oracle.source_snapshot.sha256.as_str()
            || self.oracle.source_tree_sha256 != self.oracle.source_snapshot.sha256
        {
            return Err(invariant(
                "oracle source revision and tree hash must bind the immutable source_snapshot artifact",
            ));
        }
        if self.authority_policy.sha256 != self.authority_policy_sha256
            || self.source_review.sha256 != self.source_review_sha256
        {
            return Err(invariant(
                "policy/source-review hash fields do not match their bound artifacts",
            ));
        }
        let mut artifact_paths = HashSet::new();
        for artifact in self.bound_artifacts() {
            artifact.validate_nonempty()?;
            if !artifact_paths.insert(artifact.path.as_str()) {
                return Err(invariant(
                    "bound artifacts must use distinct evidence paths",
                ));
            }
        }
        self.validate_candidate_build()?;
        self.validate_environment()?;
        if self.effort.checkpoint_elapsed_minutes > 960
            || self.effort.tuning_elapsed_minutes > 960
            || self.effort.tuning_revisions > 4
        {
            return Err(invariant("effort exceeds the frozen limits"));
        }
        Ok(())
    }

    pub fn bound_artifacts(&self) -> [&ArtifactEvidence; 14] {
        [
            &self.authority_policy,
            &self.source_review,
            &self.oracle.source_snapshot,
            &self.oracle.executable,
            &self.candidate_build.source_snapshot,
            &self.candidate_build.executable,
            &self.candidate_build.cargo_lock,
            &self.candidate_build.sloc_manifest,
            &self.candidate_build.sloc_result,
            &self.guest_artifacts.a,
            &self.guest_artifacts.b,
            &self.guest_artifacts.bad_a,
            &self.guest_artifacts.bad_b,
            &self.guest_artifacts.authority,
        ]
    }

    fn validate_candidate_build(&self) -> Result<(), EvidenceError> {
        let build = &self.candidate_build;
        if build.source_revision != build.source_snapshot.sha256.as_str()
            || build.source_tree_sha256 != build.source_snapshot.sha256
        {
            return Err(invariant(
                "candidate source revision and tree hash must bind the immutable source_snapshot artifact",
            ));
        }
        if build.argv.is_empty()
            || build.argv.iter().any(String::is_empty)
            || build.binary_bytes == 0
            || build.binary_bytes != build.executable.bytes
            || build.build_command != "cargo build --release --locked"
        {
            return Err(invariant(
                "candidate build violates the frozen build contract",
            ));
        }
        let expected_topology = match self.candidate {
            CandidateKind::Lunatic | CandidateKind::RawWasmtime => ExecutionTopologyEvidence {
                os_processes: 1,
                long_lived_helper_processes: 0,
                tenant_execution_model: TenantExecutionModel::TokioTasks,
                tokio_worker_threads: 4,
                dedicated_tenant_threads: 0,
            },
            CandidateKind::Extism => ExecutionTopologyEvidence {
                os_processes: 1,
                long_lived_helper_processes: 0,
                tenant_execution_model: TenantExecutionModel::DedicatedThreads,
                tokio_worker_threads: 0,
                dedicated_tenant_threads: 32,
            },
        };
        if build.execution_topology != expected_topology {
            return Err(invariant(
                "candidate execution topology does not match the frozen candidate architecture",
            ));
        }
        let expected_engine_version = match self.candidate {
            CandidateKind::Lunatic | CandidateKind::RawWasmtime => "46.0.1",
            CandidateKind::Extism => "43.0.2",
        };
        if build.embedded_wasm_engine.implementation != "wasmtime"
            || build.embedded_wasm_engine.version != expected_engine_version
        {
            return Err(invariant(
                "candidate embedded Wasm engine does not match its frozen Cargo.lock",
            ));
        }
        let mut dependencies = HashSet::new();
        for dependency in &build.direct_dependencies {
            let mut parts = dependency.split('@');
            let name = parts.next().unwrap_or_default();
            let version = parts.next().unwrap_or_default();
            if parts.next().is_some()
                || name.is_empty()
                || !name
                    .as_bytes()
                    .iter()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
                || version.is_empty()
                || !version.as_bytes()[0].is_ascii_digit()
                || !version
                    .as_bytes()
                    .iter()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'+' | b'-'))
                || !dependencies.insert(dependency)
            {
                return Err(invariant(
                    "direct dependencies must be unique name@version strings",
                ));
            }
        }
        Ok(())
    }

    fn validate_environment(&self) -> Result<(), EvidenceError> {
        let environment = &self.environment;
        if environment.os != "Windows 11 Home 10.0.26200 build 26200"
            || environment.cpu != "Intel Core i7-14700K"
            || environment.logical_processors != 28
            || environment.physical_memory_bytes != 68_475_179_008
            || environment.rustc != "1.95.0"
            || environment.cargo != "1.95.0"
            || environment.oracle_wasmtime != "46.0.1"
            || environment.tokio != "1.53.1"
            || environment.node != "24.4.1"
        {
            return Err(invariant("environment is not the frozen measurement host"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessExitEvidence {
    pub observed_monotonic_ns: u64,
    pub code: Option<i32>,
    pub success: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawEvidenceFiles {
    pub run_manifest: FileEvidence,
    pub stream_trace: FileEvidence,
    pub rss_trace: FileEvidence,
    pub filesystem_observer_trace: FileEvidence,
    pub network_observer_trace: FileEvidence,
    pub stderr_trace: FileEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncompleteStatus {
    Failed,
    TimedOut,
    Crashed,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailurePhase {
    HelloAndMinimalRss,
    SharedInit,
    Create,
    Normal,
    Pressure,
    Faults,
    Updates,
    Authority,
    Cleanup,
    Shutdown,
    ProcessExit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunFailure {
    pub phase: FailurePhase,
    pub code: String,
    pub message: String,
    pub request_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartialCounts {
    pub controls_sent: u64,
    pub events_received: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncompleteOutcome {
    pub status: IncompleteStatus,
    pub failure: RunFailure,
    pub partial_counts: PartialCounts,
    pub protocol_violations: u64,
    pub hidden_retry_count: u64,
}

impl IncompleteOutcome {
    fn validate(&self) -> Result<(), EvidenceError> {
        if self.failure.code.is_empty()
            || self.failure.message.is_empty()
            || self.failure.request_id == Some(0)
        {
            return Err(invariant(
                "incomplete failure fields violate the summary schema",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletedStatus {
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedCounts {
    pub warmup_commands: u64,
    pub normal_original_commands: u64,
    pub delayed_duplicate_cases: u64,
    pub delayed_duplicate_requests: u64,
    pub pressure_submissions: u64,
    pub pressure_initial_accepted: u64,
    pub pressure_retryable_rejected: u64,
    pub pressure_retries: u64,
    pub fault_seed_commands: u64,
    pub trap_faults: u64,
    pub cpu_faults: u64,
    pub fault_post_recovery_cases: u64,
    pub fault_post_recovery_requests: u64,
    pub sibling_probes: u64,
    pub update_failed_attempts: u64,
    pub update_valid_attempts: u64,
    pub update_proof_probes: u64,
    pub update_worker_attempts: u64,
    pub authority_canaries: u64,
    pub cleanup_warmup_cycles: u64,
    pub cleanup_retained_cycles: u64,
    pub cleanup_old_endpoint_probes: u64,
    pub accepted_commands: u64,
    pub terminal_accepted_commands: u64,
}

impl CompletedCounts {
    fn validate(&self) -> Result<(), EvidenceError> {
        let exact = [
            (self.warmup_commands, 1_024),
            (self.normal_original_commands, 10_240),
            (self.delayed_duplicate_cases, 80),
            (self.delayed_duplicate_requests, 240),
            (self.pressure_submissions, 68),
            (self.pressure_initial_accepted, 64),
            (self.pressure_retryable_rejected, 1),
            (self.pressure_retries, 1),
            (self.fault_seed_commands, 60),
            (self.trap_faults, 30),
            (self.cpu_faults, 30),
            (self.fault_post_recovery_cases, 60),
            (self.fault_post_recovery_requests, 180),
            (self.sibling_probes, 60),
            (self.update_failed_attempts, 30),
            (self.update_valid_attempts, 30),
            (self.update_proof_probes, 2_880),
            (self.authority_canaries, 6),
            (self.cleanup_warmup_cycles, 5),
            (self.cleanup_retained_cycles, 30),
            (self.cleanup_old_endpoint_probes, 1_120),
        ];
        if exact.iter().any(|(actual, expected)| actual != expected)
            || self.update_worker_attempts < 960
            || self.accepted_commands == 0
            || self.terminal_accepted_commands == 0
        {
            return Err(invariant(
                "completed counts disagree with the frozen workload",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateResult {
    pub pass: bool,
    pub evidence_refs: Vec<RelativeEvidencePath>,
}

impl GateResult {
    fn validate(&self) -> Result<(), EvidenceError> {
        let mut unique = HashSet::new();
        if self.evidence_refs.is_empty()
            || self
                .evidence_refs
                .iter()
                .any(|reference| !unique.insert(reference))
        {
            return Err(invariant(
                "gate evidence references must be non-empty and unique",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MandatoryGates {
    pub isolation: GateResult,
    pub authority: GateResult,
    pub admission: GateResult,
    pub recovery: GateResult,
    pub update_safety: GateResult,
    pub state_semantics: GateResult,
    pub cleanup: GateResult,
}

impl MandatoryGates {
    fn validate(&self) -> Result<(), EvidenceError> {
        for gate in self.all() {
            gate.validate()?;
        }
        Ok(())
    }

    fn all(&self) -> [&GateResult; 7] {
        [
            &self.isolation,
            &self.authority,
            &self.admission,
            &self.recovery,
            &self.update_safety,
            &self.state_semantics,
            &self.cleanup,
        ]
    }

    fn all_pass(&self) -> bool {
        self.all().iter().all(|gate| gate.pass)
    }

    fn validate_evidence_refs(&self, evidence: &RawEvidenceFiles) -> Result<(), EvidenceError> {
        let raw_paths: HashSet<&RelativeEvidencePath> =
            evidence.files().iter().map(|file| &file.path).collect();
        if self
            .all()
            .iter()
            .flat_map(|gate| gate.evidence_refs.iter())
            .any(|reference| !raw_paths.contains(reference))
        {
            return Err(invariant(
                "gate evidence reference does not resolve to a bound raw evidence file",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricEvidence {
    pub sample_count: u64,
    pub minimum_ns: u64,
    pub nearest_rank_p99_ns: u64,
    pub maximum_ns: u64,
    pub slo_min_ns: Option<u64>,
    pub slo_max_ns: u64,
    pub pass: bool,
}

impl MetricEvidence {
    fn validate_nonnegative(
        &self,
        expected_sample_count: u64,
        use_maximum: bool,
    ) -> Result<(), EvidenceError> {
        if self.sample_count != expected_sample_count
            || self.minimum_ns > self.nearest_rank_p99_ns
            || self.nearest_rank_p99_ns > self.maximum_ns
            || self.slo_max_ns == 0
        {
            return Err(invariant(
                "metric distribution is empty, unordered, or has no SLO",
            ));
        }
        let upper_ns = if use_maximum {
            self.maximum_ns
        } else {
            self.nearest_rank_p99_ns
        };
        let derived_pass = upper_ns <= self.slo_max_ns
            && self
                .slo_min_ns
                .is_none_or(|minimum| self.minimum_ns >= minimum);
        if self.pass != derived_pass {
            return Err(invariant(
                "metric pass does not match its samples, statistic, and configured SLO",
            ));
        }
        Ok(())
    }

    fn validate_positive(
        &self,
        expected_sample_count: u64,
        use_maximum: bool,
    ) -> Result<(), EvidenceError> {
        self.validate_nonnegative(expected_sample_count, use_maximum)?;
        if self.minimum_ns == 0 || self.slo_min_ns == Some(0) {
            return Err(invariant("metric distribution must be positive"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalMetricEvidence {
    pub sample_count: u64,
    pub minimum_ns: u64,
    pub nearest_rank_p99_ns: u64,
    pub maximum_ns: u64,
    pub total_ns: u64,
    pub slo_min_ns: Option<u64>,
    pub slo_max_ns: u64,
    pub pass: bool,
}

impl NormalMetricEvidence {
    fn validate(&self, expected_sample_count: u64) -> Result<(), EvidenceError> {
        MetricEvidence {
            sample_count: self.sample_count,
            minimum_ns: self.minimum_ns,
            nearest_rank_p99_ns: self.nearest_rank_p99_ns,
            maximum_ns: self.maximum_ns,
            slo_min_ns: self.slo_min_ns,
            slo_max_ns: self.slo_max_ns,
            pass: self.pass,
        }
        .validate_positive(expected_sample_count, false)?;
        if self.total_ns == 0 {
            return Err(invariant("normal total duration must be positive"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedMetrics {
    pub shared_init_ns: u64,
    pub warm_create: MetricEvidence,
    pub normal: NormalMetricEvidence,
    pub pressure_admission: MetricEvidence,
    pub sibling: MetricEvidence,
    pub trap_replacement_ready: MetricEvidence,
    pub cpu_replacement_ready: MetricEvidence,
    pub cpu_failed_terminal: MetricEvidence,
    pub failed_update_unavailability: MetricEvidence,
    pub valid_update_unavailability: MetricEvidence,
    pub failed_rollout: MetricEvidence,
    pub valid_rollout: MetricEvidence,
    pub failed_mixed_window: MetricEvidence,
    pub valid_mixed_window: MetricEvidence,
}

impl CompletedMetrics {
    fn validate(&self) -> Result<(), EvidenceError> {
        if self.shared_init_ns == 0 {
            return Err(invariant("shared init duration must be positive"));
        }
        self.warm_create.validate_positive(32, false)?;
        self.normal.validate(10_240)?;
        self.pressure_admission.validate_positive(64, false)?;
        self.sibling.validate_positive(60, false)?;
        self.trap_replacement_ready.validate_positive(30, false)?;
        self.cpu_replacement_ready.validate_positive(30, false)?;
        self.cpu_failed_terminal.validate_positive(30, true)?;
        self.failed_update_unavailability
            .validate_positive(480, false)?;
        self.valid_update_unavailability
            .validate_positive(480, false)?;
        self.failed_rollout.validate_positive(30, false)?;
        self.valid_rollout.validate_positive(30, false)?;
        self.failed_mixed_window.validate_nonnegative(30, true)?;
        self.valid_mixed_window.validate_nonnegative(30, true)?;
        Ok(())
    }

    fn distributions(&self) -> [&MetricEvidence; 12] {
        [
            &self.warm_create,
            &self.pressure_admission,
            &self.sibling,
            &self.trap_replacement_ready,
            &self.cpu_replacement_ready,
            &self.cpu_failed_terminal,
            &self.failed_update_unavailability,
            &self.valid_update_unavailability,
            &self.failed_rollout,
            &self.valid_rollout,
            &self.failed_mixed_window,
            &self.valid_mixed_window,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RssEvidence {
    pub units: String,
    pub process_tree_complete: bool,
    pub minimal_host_stable_median_bytes: u64,
    pub shared_init_stable_median_bytes: u64,
    pub ready_32_stable_median_bytes: u64,
    pub ready_delta_bytes: u64,
    pub phase_peak_bytes: u64,
    pub peak_delta_bytes: u64,
    pub cleanup_delta_bytes: Vec<i64>,
    pub maximum_post_cleanup_delta_bytes: i64,
    pub cleanup_slope_bytes_per_cycle: f64,
    pub pass: bool,
}

impl RssEvidence {
    fn validate(&self) -> Result<(), EvidenceError> {
        if self.units != "bytes"
            || !self.process_tree_complete
            || self.minimal_host_stable_median_bytes == 0
            || self.shared_init_stable_median_bytes == 0
            || self.ready_32_stable_median_bytes == 0
            || self.ready_delta_bytes == 0
            || self.phase_peak_bytes == 0
            || self.peak_delta_bytes == 0
            || self.cleanup_delta_bytes.len() != 30
            || !self.cleanup_slope_bytes_per_cycle.is_finite()
        {
            return Err(invariant("RSS result violates the frozen schema"));
        }
        let ready_delta_bytes = self
            .ready_32_stable_median_bytes
            .checked_sub(self.minimal_host_stable_median_bytes)
            .ok_or_else(|| invariant("ready RSS is below minimal-host RSS"))?;
        let peak_delta_bytes = self
            .phase_peak_bytes
            .checked_sub(self.minimal_host_stable_median_bytes)
            .ok_or_else(|| invariant("peak RSS is below minimal-host RSS"))?;
        let maximum_post_cleanup_delta_bytes = *self
            .cleanup_delta_bytes
            .iter()
            .max()
            .ok_or_else(|| invariant("RSS cleanup distribution is empty"))?;
        let cleanup_slope_bytes_per_cycle = frozen_cleanup_slope(&self.cleanup_delta_bytes)
            .map_err(|message| invariant(format!("invalid RSS cleanup slope: {message}")))?;
        if self.ready_delta_bytes != ready_delta_bytes
            || self.peak_delta_bytes != peak_delta_bytes
            || self.maximum_post_cleanup_delta_bytes != maximum_post_cleanup_delta_bytes
            || self.cleanup_slope_bytes_per_cycle.to_bits()
                != cleanup_slope_bytes_per_cycle.to_bits()
        {
            return Err(invariant(
                "RSS derived fields do not match the frozen formulas",
            ));
        }
        let derived_pass = frozen_rss_pass(
            self.ready_32_stable_median_bytes,
            peak_delta_bytes,
            maximum_post_cleanup_delta_bytes,
            cleanup_slope_bytes_per_cycle,
        );
        if self.pass != derived_pass {
            return Err(invariant(
                "RSS pass does not match the frozen absolute limits",
            ));
        }
        Ok(())
    }
}

pub fn frozen_cleanup_slope(values: &[i64]) -> Result<f64, String> {
    if values.len() != 30 {
        return Err("cleanup slope requires 30 values".to_owned());
    }
    let mean_x = 15.5_f64;
    let mean_y = values.iter().map(|value| *value as f64).sum::<f64>() / 30.0;
    let mut numerator = 0.0;
    let mut denominator = 0.0;
    for (index, value) in values.iter().enumerate() {
        let x = (index + 1) as f64;
        numerator += (x - mean_x) * (*value as f64 - mean_y);
        denominator += (x - mean_x).powi(2);
    }
    if denominator == 0.0 {
        return Err("cleanup slope denominator is zero".to_owned());
    }
    let slope = numerator / denominator;
    if !slope.is_finite() {
        return Err("cleanup slope is not finite".to_owned());
    }
    Ok(slope)
}

pub fn frozen_rss_pass(
    ready_32_total_bytes: u64,
    peak_delta_bytes: u64,
    maximum_post_cleanup_delta_bytes: i64,
    cleanup_slope_bytes_per_cycle: f64,
) -> bool {
    ready_32_total_bytes <= RSS_READY_32_TOTAL_LIMIT_BYTES
        && peak_delta_bytes <= RSS_PEAK_DELTA_LIMIT_BYTES
        && maximum_post_cleanup_delta_bytes <= RSS_POST_CLEANUP_DELTA_LIMIT_BYTES
        && cleanup_slope_bytes_per_cycle <= RSS_CLEANUP_SLOPE_LIMIT_BYTES_PER_CYCLE
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryAudit {
    pub delayed_duplicate_cases: u64,
    pub delayed_duplicate_requests: u64,
    pub pressure_retries: u64,
    pub update_worker_retries: u64,
    pub oracle_transport_retries: u64,
    pub candidate_hidden_retries: u64,
    pub unauthorized_retries: u64,
}

impl RetryAudit {
    fn validate(&self) -> Result<(), EvidenceError> {
        if self.delayed_duplicate_cases != 80
            || self.delayed_duplicate_requests != 240
            || self.pressure_retries != 1
            || self.oracle_transport_retries != 0
            || self.candidate_hidden_retries != 0
            || self.unauthorized_retries != 0
        {
            return Err(invariant("retry audit violates the frozen contract"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolAudit {
    pub violations: u64,
    pub invalid_json_lines: u64,
    pub late_events: u64,
    pub unexpected_rejections: u64,
    pub timeouts: u64,
    pub nonpositive_durations: u64,
}

impl ProtocolAudit {
    fn validate(&self) -> Result<(), EvidenceError> {
        if [
            self.violations,
            self.invalid_json_lines,
            self.late_events,
            self.unexpected_rejections,
            self.timeouts,
            self.nonpositive_durations,
        ]
        .iter()
        .any(|value| *value != 0)
        {
            return Err(invariant(
                "completed protocol audit must contain only zeros",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedOutcome {
    pub status: CompletedStatus,
    pub overall_pass: bool,
    pub counts: CompletedCounts,
    pub gates: MandatoryGates,
    pub metrics_ns: CompletedMetrics,
    pub rss: RssEvidence,
    pub retry_audit: RetryAudit,
    pub protocol_audit: ProtocolAudit,
}

impl CompletedOutcome {
    pub fn validate(&self) -> Result<(), EvidenceError> {
        self.counts.validate()?;
        self.gates.validate()?;
        self.metrics_ns.validate()?;
        self.rss.validate()?;
        self.retry_audit.validate()?;
        self.protocol_audit.validate()?;
        let all_metric_slos = self.metrics_ns.normal.pass
            && self
                .metrics_ns
                .distributions()
                .iter()
                .all(|metric| metric.pass);
        let derived_pass = self.gates.all_pass()
            && all_metric_slos
            && self.rss.pass
            && self.key_metrics().all_positive();
        if self.overall_pass != derived_pass {
            return Err(invariant(
                "overall_pass does not equal gates, SLOs, RSS, and positive key metrics",
            ));
        }
        Ok(())
    }

    pub fn key_metrics(&self) -> KeyMetrics {
        KeyMetrics {
            shared_init_ns: self.metrics_ns.shared_init_ns,
            warm_create_p99_ns: self.metrics_ns.warm_create.nearest_rank_p99_ns,
            normal_p99_ns: self.metrics_ns.normal.nearest_rank_p99_ns,
            normal_total_ns: self.metrics_ns.normal.total_ns,
            pressure_admission_p99_ns: self.metrics_ns.pressure_admission.nearest_rank_p99_ns,
            sibling_p99_ns: self.metrics_ns.sibling.nearest_rank_p99_ns,
            trap_recovery_p99_ns: self.metrics_ns.trap_replacement_ready.nearest_rank_p99_ns,
            cpu_recovery_p99_ns: self.metrics_ns.cpu_replacement_ready.nearest_rank_p99_ns,
            failed_update_unavailability_p99_ns: self
                .metrics_ns
                .failed_update_unavailability
                .nearest_rank_p99_ns,
            valid_update_unavailability_p99_ns: self
                .metrics_ns
                .valid_update_unavailability
                .nearest_rank_p99_ns,
            failed_rollout_p99_ns: self.metrics_ns.failed_rollout.nearest_rank_p99_ns,
            valid_rollout_p99_ns: self.metrics_ns.valid_rollout.nearest_rank_p99_ns,
            ready_32_total_rss_bytes: self.rss.ready_32_stable_median_bytes,
            peak_total_rss_bytes: self.rss.phase_peak_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawRunSummary<O> {
    pub schema_version: u32,
    pub experiment_id: String,
    pub run_id: String,
    pub block_index: u32,
    pub retained: bool,
    pub candidate: CandidateKind,
    pub scenario_sha256: Sha256Digest,
    pub manifest_sha256: Sha256Digest,
    pub started_monotonic_ns: u64,
    pub finished_monotonic_ns: u64,
    pub process_exit: ProcessExitEvidence,
    pub outcome: O,
    pub evidence: RawEvidenceFiles,
}

impl RawRunSummary<IncompleteOutcome> {
    pub fn validate(&self, manifest: &RunManifest) -> Result<(), EvidenceError> {
        validate_summary_envelope(self, manifest)?;
        self.outcome.validate()
    }
}

impl RawRunSummary<CompletedOutcome> {
    pub fn validate(&self, manifest: &RunManifest) -> Result<(), EvidenceError> {
        validate_summary_envelope(self, manifest)?;
        self.outcome.validate()?;
        self.outcome.gates.validate_evidence_refs(&self.evidence)
    }
}

fn validate_summary_envelope<O>(
    summary: &RawRunSummary<O>,
    manifest: &RunManifest,
) -> Result<(), EvidenceError> {
    if summary.schema_version != EVIDENCE_SCHEMA_VERSION
        || summary.experiment_id != EXPERIMENT_ID
        || summary.run_id != manifest.run_id
        || summary.block_index != manifest.block_index
        || summary.retained != manifest.retained
        || summary.candidate != manifest.candidate
        || summary.scenario_sha256 != manifest.scenario.sha256
        || summary.started_monotonic_ns == 0
        || summary.finished_monotonic_ns < summary.started_monotonic_ns
        || summary.process_exit.observed_monotonic_ns < summary.started_monotonic_ns
        || summary.process_exit.observed_monotonic_ns > summary.finished_monotonic_ns
    {
        return Err(invariant(
            "raw summary identity or monotonic envelope mismatch",
        ));
    }
    if summary.evidence.run_manifest.sha256 != summary.manifest_sha256 {
        return Err(invariant(
            "raw summary manifest hash does not match run_manifest evidence",
        ));
    }
    Ok(())
}

pub struct ImmutableEvidenceDirectory {
    path: PathBuf,
    manifest: RunManifest,
    manifest_evidence: FileEvidence,
}

impl ImmutableEvidenceDirectory {
    pub fn create(root: &Path, manifest: RunManifest) -> Result<Self, EvidenceError> {
        manifest.validate()?;
        fs::create_dir_all(root)?;
        let path = root.join(&manifest.run_id);
        fs::create_dir(&path)?;

        let manifest_path = RelativeEvidencePath::parse("run-manifest.json")?;
        let manifest_bytes = json_document_bytes(&manifest)?;
        let manifest_evidence = write_create_new(&path, manifest_path, &manifest_bytes, true)?;
        Ok(Self {
            path,
            manifest,
            manifest_evidence,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn manifest(&self) -> &RunManifest {
        &self.manifest
    }

    pub fn manifest_evidence(&self) -> &FileEvidence {
        &self.manifest_evidence
    }

    pub fn create_evidence_file(&self, path: RelativeEvidencePath) -> Result<File, EvidenceError> {
        let target = checked_join(&self.path, &path)?;
        create_parent_directories(&target)?;
        Ok(OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(target)?)
    }

    pub fn write_evidence(
        &self,
        path: RelativeEvidencePath,
        bytes: &[u8],
    ) -> Result<FileEvidence, EvidenceError> {
        write_create_new(&self.path, path, bytes, true)
    }

    pub fn write_bound_artifact(
        &self,
        expected: &ArtifactEvidence,
        bytes: &[u8],
    ) -> Result<FileEvidence, EvidenceError> {
        expected.validate_nonempty()?;
        if bytes.len() as u64 != expected.bytes || sha256_digest(bytes)? != expected.sha256 {
            return Err(invariant(format!(
                "bound artifact bytes do not match {}",
                expected.path.as_str()
            )));
        }
        write_create_new(&self.path, expected.path.clone(), bytes, true)
    }

    pub fn verify_bound_artifacts(&self) -> Result<(), EvidenceError> {
        for artifact in self.manifest.bound_artifacts() {
            verify_artifact(&self.path, artifact)?;
        }
        Ok(())
    }

    pub fn write_incomplete_summary(
        &self,
        summary: &RawRunSummary<IncompleteOutcome>,
    ) -> Result<FileEvidence, EvidenceError> {
        summary.validate(&self.manifest)?;
        if summary.manifest_sha256 != self.manifest_evidence.sha256 {
            return Err(invariant(
                "summary manifest hash does not bind the immutable manifest bytes",
            ));
        }
        self.verify_bound_artifacts()?;
        for evidence in summary.evidence.files() {
            verify_file(&self.path, evidence)?;
        }
        let bytes = json_document_bytes(summary)?;
        write_create_new(
            &self.path,
            RelativeEvidencePath::parse("raw-summary.json")?,
            &bytes,
            true,
        )
    }

    pub fn write_completed_summary(
        &self,
        summary: &RawRunSummary<CompletedOutcome>,
    ) -> Result<FileEvidence, EvidenceError> {
        summary.validate(&self.manifest)?;
        if summary.manifest_sha256 != self.manifest_evidence.sha256 {
            return Err(invariant(
                "summary manifest hash does not bind the immutable manifest bytes",
            ));
        }
        self.verify_bound_artifacts()?;
        for evidence in summary.evidence.files() {
            verify_file(&self.path, evidence)?;
        }
        let bytes = json_document_bytes(summary)?;
        write_create_new(
            &self.path,
            RelativeEvidencePath::parse("raw-summary.json")?,
            &bytes,
            true,
        )
    }

    pub fn verify_manifest(&self) -> Result<(), EvidenceError> {
        verify_file(&self.path, &self.manifest_evidence)
    }
}

impl RawEvidenceFiles {
    fn files(&self) -> [&FileEvidence; 6] {
        [
            &self.run_manifest,
            &self.stream_trace,
            &self.rss_trace,
            &self.filesystem_observer_trace,
            &self.network_observer_trace,
            &self.stderr_trace,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceFinalization {
    pub records: u64,
    pub bytes: u64,
    pub sha256: Sha256Digest,
}

#[derive(Clone)]
pub struct OrderedTraceSink {
    inner: Arc<Mutex<OrderedTraceState>>,
}

struct OrderedTraceState {
    writer: BufWriter<File>,
    next_trace_seq: u64,
    bytes: u64,
    hasher: Sha256,
    finalized: bool,
}

impl OrderedTraceSink {
    pub fn create(path: &Path) -> Result<Self, EvidenceError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create_new(true).write(true).open(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(OrderedTraceState {
                writer: BufWriter::new(file),
                next_trace_seq: 1,
                bytes: 0,
                hasher: Sha256::new(),
                finalized: false,
            })),
        })
    }

    pub fn record<T: Serialize>(&self, record: &T) -> Result<u64, EvidenceError> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| invariant("ordered trace mutex poisoned"))?;
        if state.finalized {
            return Err(EvidenceError::TraceFinalized);
        }
        let trace_seq = state.next_trace_seq;
        let mut value = serde_json::to_value(record)?;
        let object = value
            .as_object_mut()
            .ok_or(EvidenceError::InvalidTraceRecord)?;
        if object.contains_key("trace_seq") {
            return Err(EvidenceError::InvalidTraceRecord);
        }
        object.insert("trace_seq".into(), Value::from(trace_seq));
        let mut line = serde_json::to_vec(&value)?;
        line.push(b'\n');
        state.writer.write_all(&line)?;
        state.writer.flush()?;
        state.hasher.update(&line);
        state.bytes = state
            .bytes
            .checked_add(line.len() as u64)
            .ok_or_else(|| invariant("ordered trace byte count overflow"))?;
        state.next_trace_seq = state
            .next_trace_seq
            .checked_add(1)
            .ok_or_else(|| invariant("ordered trace sequence overflow"))?;
        Ok(trace_seq)
    }

    pub fn finalize(&self) -> Result<TraceFinalization, EvidenceError> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| invariant("ordered trace mutex poisoned"))?;
        if state.finalized {
            return Err(EvidenceError::TraceFinalized);
        }
        state.writer.flush()?;
        state.writer.get_ref().sync_all()?;
        state.finalized = true;
        let sha256 = digest_to_hex(state.hasher.clone().finalize())?;
        Ok(TraceFinalization {
            records: state.next_trace_seq - 1,
            bytes: state.bytes,
            sha256,
        })
    }

    pub fn is_finalized(&self) -> Result<bool, EvidenceError> {
        self.inner
            .lock()
            .map(|state| state.finalized)
            .map_err(|_| invariant("ordered trace mutex poisoned"))
    }
}

pub fn verify_file(root: &Path, evidence: &FileEvidence) -> Result<(), EvidenceError> {
    let path = checked_join(root, &evidence.path)?;
    let mut file = File::open(&path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let actual_sha256 = sha256_digest(&bytes)?;
    if bytes.len() as u64 != evidence.bytes || actual_sha256 != evidence.sha256 {
        return Err(EvidenceError::Tampered(evidence.path.as_str().into()));
    }
    Ok(())
}

pub fn verify_artifact(root: &Path, evidence: &ArtifactEvidence) -> Result<(), EvidenceError> {
    verify_file(
        root,
        &FileEvidence {
            path: evidence.path.clone(),
            sha256: evidence.sha256.clone(),
            bytes: evidence.bytes,
        },
    )
}

pub fn file_evidence(
    root: &Path,
    path: RelativeEvidencePath,
) -> Result<FileEvidence, EvidenceError> {
    let target = checked_join(root, &path)?;
    let mut file = File::open(target)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(FileEvidence {
        path,
        sha256: sha256_digest(&bytes)?,
        bytes: bytes.len() as u64,
    })
}

fn write_create_new(
    root: &Path,
    path: RelativeEvidencePath,
    bytes: &[u8],
    sync: bool,
) -> Result<FileEvidence, EvidenceError> {
    let target = checked_join(root, &path)?;
    create_parent_directories(&target)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(target)?;
    file.write_all(bytes)?;
    file.flush()?;
    if sync {
        file.sync_all()?;
    }
    Ok(FileEvidence {
        path,
        sha256: sha256_digest(bytes)?,
        bytes: bytes.len() as u64,
    })
}

fn checked_join(root: &Path, path: &RelativeEvidencePath) -> Result<PathBuf, EvidenceError> {
    let target = root.join(path.as_str());
    if !target.starts_with(root) {
        return Err(invariant(
            "evidence path escaped the immutable run directory",
        ));
    }
    Ok(target)
}

fn create_parent_directories(target: &Path) -> Result<(), EvidenceError> {
    let parent = target
        .parent()
        .ok_or_else(|| invariant("evidence target has no parent directory"))?;
    fs::create_dir_all(parent)?;
    Ok(())
}

fn json_document_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, EvidenceError> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn sha256_digest(bytes: &[u8]) -> Result<Sha256Digest, EvidenceError> {
    digest_to_hex(Sha256::digest(bytes))
}

fn digest_to_hex(bytes: impl AsRef<[u8]>) -> Result<Sha256Digest, EvidenceError> {
    Sha256Digest::parse(
        bytes
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    )
}

fn validate_run_id(value: &str) -> Result<(), EvidenceError> {
    if value.is_empty()
        || value.len() > 128
        || !value.as_bytes()[0].is_ascii_lowercase() && !value.as_bytes()[0].is_ascii_digit()
        || !value.as_bytes().iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(invariant("run_id does not match the frozen pattern"));
    }
    Ok(())
}

fn validate_canonical_utc(value: &str) -> Result<(), EvidenceError> {
    let bytes = value.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
        || bytes.iter().enumerate().any(|(index, byte)| {
            !matches!(index, 4 | 7 | 10 | 13 | 16 | 19) && !byte.is_ascii_digit()
        })
    {
        return Err(invariant(
            "freeze_utc must be canonical YYYY-MM-DDTHH:MM:SSZ",
        ));
    }
    let number = |start: usize, end: usize| {
        std::str::from_utf8(&bytes[start..end])
            .ok()
            .and_then(|part| part.parse::<u32>().ok())
    };
    let year = number(0, 4).unwrap_or(0);
    let month = number(5, 7).unwrap_or(0);
    let day = number(8, 10).unwrap_or(0);
    let hour = number(11, 13).unwrap_or(24);
    let minute = number(14, 16).unwrap_or(60);
    let second = number(17, 19).unwrap_or(60);
    if year == 0
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(invariant("freeze_utc contains an out-of-range component"));
    }
    Ok(())
}

fn invariant(message: impl Into<String>) -> EvidenceError {
    EvidenceError::Invariant(message.into())
}
