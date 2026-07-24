//! Immutable, decision-bearing orchestration for one embedded-v3 candidate block.
//!
//! A run is prepared from content-addressed source inputs, executed from the
//! resulting immutable bundle, and finalized into raw traces plus the frozen
//! summary DTOs. Candidate-specific code is never linked into the oracle.

use std::collections::HashSet;
use std::convert::{TryFrom, TryInto};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context, Result};
use embedded_v3_authority_guests::{build_all as build_authority_guests, Probe};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::analysis::RetainedRun;
use crate::authority_harness::{AuthorityHarness, DenialObservationCoverage};
use crate::evidence::{
    ArtifactEvidence, CandidateBuildEvidence, CompletedCounts, CompletedMetrics, CompletedOutcome,
    CompletedStatus, EffortEvidence, EmbeddedWasmEngineEvidence, EnvironmentEvidence,
    ExecutionTopologyEvidence, FailurePhase, GateResult, GuestArtifactsEvidence,
    ImmutableEvidenceDirectory, IncompleteOutcome, IncompleteStatus, MandatoryGates,
    OracleEvidence, PartialCounts, ProcessExitEvidence, ProtocolAudit, RawEvidenceFiles,
    RawRunSummary, RelativeEvidencePath, RetryAudit, RunFailure, RunManifest, ScenarioEvidence,
    Sha256Digest, TenantExecutionModel, EVIDENCE_SCHEMA_VERSION, EXPERIMENT_ID,
};
use crate::full_runner::{
    AuthorityRunEvidence, CleanupRssTarget, TimingSamples, WorkloadCounts, WorkloadRunner,
    ARTIFACT_A_SHA256, ARTIFACT_BAD_A_SHA256, ARTIFACT_BAD_B_SHA256, ARTIFACT_B_SHA256,
};
use crate::measurement::{
    maximum_metric_evidence, metric_evidence, nonnegative_metric_evidence, normal_metric_evidence,
    rss_evidence, stable_anchor, StableRssAnchor,
};
use crate::protocol::CandidateKind;
use crate::sampling::{ProcessSampler, ProcessTreeSample, SamplerStatus};
use crate::transport::{CandidateProcess, OracleSession, TimeoutTable};
use crate::workload::{
    authority_nonce, block_order, build_full_plan, verify_full_shape, ScenarioDocument,
    EMBEDDED_PLAN_BUNDLE_SHA256, EMBEDDED_SCENARIO_SHA256,
};

const AUTHORITY_POLICY_PATH: &str = "authority-policy.json";
const SOURCE_REVIEW_PATH: &str = "source-review.json";
const ORACLE_SOURCE_SNAPSHOT_PATH: &str = "oracle/source-snapshot.tar";
const ORACLE_EXECUTABLE_PATH: &str = "oracle/embedded-v3-full-run.exe";
const CANDIDATE_SOURCE_SNAPSHOT_PATH: &str = "candidate/source-snapshot.tar";
const CANDIDATE_EXECUTABLE_PATH: &str = "candidate/candidate.exe";
const CANDIDATE_LOCK_PATH: &str = "candidate/Cargo.lock";
const CANDIDATE_SLOC_MANIFEST_PATH: &str = "candidate/sloc-manifest.json";
const CANDIDATE_SLOC_RESULT_PATH: &str = "candidate/sloc-result.json";
const GUEST_A_PATH: &str = "guest-artifacts/tenant-a.wasm";
const GUEST_B_PATH: &str = "guest-artifacts/tenant-b.wasm";
const GUEST_BAD_A_PATH: &str = "guest-artifacts/tenant-bad-a.wasm";
const GUEST_BAD_B_PATH: &str = "guest-artifacts/tenant-bad-b.wasm";
const AUTHORITY_MANIFEST_PATH: &str = "guest-artifacts/authority/manifest.json";
const SCENARIO_PATH: &str = "scenario-final.json";
const SCENARIO_SCHEMA_PATH: &str = "schemas/scenario-final.schema.json";
const PROTOCOL_SOURCE_PATH: &str = "protocol/candidate-wire-frozen.rs";
const RAW_SUMMARY_SCHEMA_PATH: &str = "schemas/raw-summary.schema.json";
const RUN_MANIFEST_SCHEMA_PATH: &str = "schemas/run-manifest.schema.json";
const STREAM_TRACE_PATH: &str = "stream-trace.ndjson";
const RSS_TRACE_PATH: &str = "rss-trace.ndjson";
const FILESYSTEM_TRACE_PATH: &str = "filesystem-observer.ndjson";
const NETWORK_TRACE_PATH: &str = "network-observer.ndjson";
const STDERR_TRACE_PATH: &str = "stderr.ndjson";
const RETAINED_RUN_PATH: &str = "retained-run.json";
const CANDIDATE_BUILD_COMMAND: &str = "cargo build --release --locked";
const SLOC_ACCOUNTING_POLICY: &str = "embedded-v3-sloc-frozen-v3";
const DIRECT_DEPENDENCY_POLICY: &str = "embedded-v3-direct-dependencies-v1";
const SOURCE_REVIEW_PASS: &str = "static_review_pass_for_execution_freeze";

/// All external files read once before the immutable run directory is created.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullRunSources {
    pub authority_policy: PathBuf,
    pub source_review: PathBuf,
    pub oracle_source_snapshot: PathBuf,
    pub oracle_executable: PathBuf,
    pub candidate_source_snapshot: PathBuf,
    pub candidate_executable: PathBuf,
    pub candidate_cargo_lock: PathBuf,
    pub candidate_sloc_manifest: PathBuf,
    pub candidate_sloc_result: PathBuf,
    pub guest_a: PathBuf,
    pub guest_b: PathBuf,
    pub guest_bad_a: PathBuf,
    pub guest_bad_b: PathBuf,
    pub authority_guest_source_root: PathBuf,
    pub scenario: PathBuf,
    pub scenario_schema: PathBuf,
    pub protocol_source: PathBuf,
    pub raw_summary_schema: PathBuf,
    pub run_manifest_schema: PathBuf,
}

/// Effort metadata whose review digest is bound to the source-review artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullRunEffort {
    pub checkpoint_elapsed_minutes: u32,
    pub completion_elapsed_minutes: u32,
    pub tuning_elapsed_minutes: u32,
    pub tuning_revisions: u32,
}

/// Attested metadata that cannot be derived from the supplied file bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullRunMetadata {
    pub run_id: String,
    pub block_index: u32,
    pub candidate: CandidateKind,
    pub freeze_utc: String,
    #[serde(default)]
    pub candidate_arguments: Vec<String>,
    pub environment: EnvironmentEvidence,
    pub effort: FullRunEffort,
}

/// A complete single-block execution request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullRunConfiguration {
    pub evidence_root: PathBuf,
    pub scratch_root: PathBuf,
    pub metadata: FullRunMetadata,
    pub sources: FullRunSources,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FullRunReport {
    pub run_path: PathBuf,
    pub raw_summary_path: PathBuf,
    pub retained_run_path: Option<PathBuf>,
    pub completed: bool,
    pub overall_pass: Option<bool>,
}

struct PreparedRun {
    run: ImmutableEvidenceDirectory,
    harness: AuthorityHarness,
    scratch: ScratchDirectory,
    candidate_arguments: Vec<String>,
}

struct ScratchDirectory {
    path: PathBuf,
}

impl ScratchDirectory {
    fn create(root: &Path, run_id: &str) -> Result<Self> {
        fs::create_dir_all(root)
            .with_context(|| format!("create scratch root {}", root.display()))?;
        for attempt in 0..1_000_u32 {
            let path = root.join(format!(
                "embedded-v3-full-{}-{}-{attempt}",
                std::process::id(),
                short_digest(run_id.as_bytes())
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error).context("create full-run scratch directory"),
            }
        }
        bail!("could not allocate a unique full-run scratch directory")
    }
}

impl Drop for ScratchDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[derive(Debug)]
struct BoundBytes {
    artifact: ArtifactEvidence,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct AuxiliaryBytes {
    path: RelativeEvidencePath,
    bytes: Vec<u8>,
    expected_sha256: Sha256Digest,
}

#[derive(Debug, Deserialize)]
struct SlocResultDocument {
    schema_version: u32,
    accounting_policy: String,
    candidate: String,
    manifest_sha256: Sha256Digest,
    production_sloc: u64,
    cargo_metadata: SlocCargoMetadata,
}

#[derive(Debug, Deserialize)]
struct SlocCargoMetadata {
    status: String,
    package_count: u64,
    candidate_package_count: u64,
    direct_dependencies: SlocDirectDependencies,
}

#[derive(Debug, Deserialize)]
struct SlocDirectDependencies {
    policy: String,
    policy_sha256: Sha256Digest,
    declarations: Vec<SlocDependency>,
}

#[derive(Debug, Deserialize)]
struct SlocDependency {
    name: String,
    version: String,
}

#[derive(Debug)]
struct DerivedBuildMetadata {
    production_sloc: u64,
    direct_dependencies: Vec<String>,
    transitive_package_count: u64,
}

#[derive(Debug)]
struct ExpectedSlocFingerprint {
    candidate: &'static str,
    production_sloc: u64,
    transitive_package_count: u64,
    direct_dependency_policy_sha256: &'static str,
    direct_dependencies: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SourceReviewDocument {
    schema_version: u32,
    experiment_id: String,
    candidate: CandidateKind,
    blocking_findings: Vec<Value>,
    overall_verdict: String,
}

/// Executes one frozen candidate/block pair and always returns a report when
/// the candidate process was successfully spawned. Setup failures are errors.
pub fn execute_full_run(configuration: FullRunConfiguration) -> Result<FullRunReport> {
    let prepared = prepare_run(&configuration)?;
    execute_prepared(prepared)
}

fn prepare_run(configuration: &FullRunConfiguration) -> Result<PreparedRun> {
    let metadata = &configuration.metadata;
    ensure!(metadata.block_index < 10, "block_index must be in 0..9");
    ensure!(
        !metadata.candidate_arguments.iter().any(String::is_empty),
        "candidate arguments cannot contain an empty value"
    );
    let scenario = ScenarioDocument::embedded().context("load frozen scenario")?;
    let plan = build_full_plan(&scenario, metadata.block_index).context("build frozen plan")?;
    verify_full_shape(&plan).context("verify frozen plan shape")?;

    let scratch = ScratchDirectory::create(&configuration.scratch_root, &metadata.run_id)?;
    let nonce = authority_nonce(
        EXPERIMENT_ID,
        metadata.block_index,
        metadata.candidate,
        &metadata.run_id,
    )
    .context("derive frozen authority nonce")?;
    let harness = AuthorityHarness::bind_with_nonce(&scratch.path, nonce)
        .context("bind run-specific authority observers")?;
    let authority_output = scratch.path.join("authority-artifacts");
    let authority_manifest_source = build_authority_guests(
        &configuration.sources.authority_guest_source_root,
        &authority_output,
        harness.parameters(),
    )
    .map_err(|error| anyhow!(error.to_string()))
    .context("generate run-specific authority suite")?;
    verify_generated_authority_suite(&harness, &authority_output, &authority_manifest_source)?;

    let mut bound = vec![
        bound_bytes(
            AUTHORITY_POLICY_PATH,
            &configuration.sources.authority_policy,
        )?,
        bound_bytes(SOURCE_REVIEW_PATH, &configuration.sources.source_review)?,
        bound_bytes(
            ORACLE_SOURCE_SNAPSHOT_PATH,
            &configuration.sources.oracle_source_snapshot,
        )?,
        bound_bytes(
            ORACLE_EXECUTABLE_PATH,
            &configuration.sources.oracle_executable,
        )?,
        bound_bytes(
            CANDIDATE_SOURCE_SNAPSHOT_PATH,
            &configuration.sources.candidate_source_snapshot,
        )?,
        bound_bytes(
            CANDIDATE_EXECUTABLE_PATH,
            &configuration.sources.candidate_executable,
        )?,
        bound_bytes(
            CANDIDATE_LOCK_PATH,
            &configuration.sources.candidate_cargo_lock,
        )?,
        bound_bytes(
            CANDIDATE_SLOC_MANIFEST_PATH,
            &configuration.sources.candidate_sloc_manifest,
        )?,
        bound_bytes(
            CANDIDATE_SLOC_RESULT_PATH,
            &configuration.sources.candidate_sloc_result,
        )?,
        bound_bytes(GUEST_A_PATH, &configuration.sources.guest_a)?,
        bound_bytes(GUEST_B_PATH, &configuration.sources.guest_b)?,
        bound_bytes(GUEST_BAD_A_PATH, &configuration.sources.guest_bad_a)?,
        bound_bytes(GUEST_BAD_B_PATH, &configuration.sources.guest_bad_b)?,
        bound_bytes(AUTHORITY_MANIFEST_PATH, &authority_manifest_source)?,
    ];
    ensure!(bound.len() == 14, "bound artifact cardinality drift");
    verify_core_guest_hash(&bound[9], ARTIFACT_A_SHA256, "A")?;
    verify_core_guest_hash(&bound[10], ARTIFACT_B_SHA256, "B")?;
    verify_core_guest_hash(&bound[11], ARTIFACT_BAD_A_SHA256, "bad_A")?;
    verify_core_guest_hash(&bound[12], ARTIFACT_BAD_B_SHA256, "bad_B")?;
    verify_source_review(metadata.candidate, &bound[1].bytes)?;
    let build_metadata =
        derive_build_metadata(metadata.candidate, &bound[7].artifact, &bound[8].bytes)?;

    let scenario_bytes = read_nonempty(&configuration.sources.scenario)?;
    let scenario_sha256 = digest(&scenario_bytes)?;
    ensure!(
        scenario_sha256.as_str() == EMBEDDED_SCENARIO_SHA256,
        "scenario source does not match the embedded frozen scenario"
    );
    let scenario_schema_bytes = read_nonempty(&configuration.sources.scenario_schema)?;
    let scenario_schema_sha256 = digest(&scenario_schema_bytes)?;
    let protocol_source_bytes = read_nonempty(&configuration.sources.protocol_source)?;
    let protocol_source_sha256 = digest(&protocol_source_bytes)?;
    let raw_summary_schema_bytes = read_nonempty(&configuration.sources.raw_summary_schema)?;
    let raw_summary_schema_sha256 = digest(&raw_summary_schema_bytes)?;
    let run_manifest_schema_bytes = read_nonempty(&configuration.sources.run_manifest_schema)?;
    let run_manifest_schema_sha256 = digest(&run_manifest_schema_bytes)?;

    let authority_policy = bound[0].artifact.clone();
    let source_review = bound[1].artifact.clone();
    let oracle_source_snapshot = bound[2].artifact.clone();
    let oracle_executable = bound[3].artifact.clone();
    let candidate_source_snapshot = bound[4].artifact.clone();
    let candidate_executable = bound[5].artifact.clone();
    let candidate_lock = bound[6].artifact.clone();
    let candidate_sloc_manifest = bound[7].artifact.clone();
    let candidate_sloc_result = bound[8].artifact.clone();
    let guest_a = bound[9].artifact.clone();
    let guest_b = bound[10].artifact.clone();
    let guest_bad_a = bound[11].artifact.clone();
    let guest_bad_b = bound[12].artifact.clone();
    let authority_manifest = bound[13].artifact.clone();
    let candidate_order_position = block_order(metadata.block_index)?
        .candidates
        .iter()
        .position(|candidate| *candidate == metadata.candidate)
        .ok_or_else(|| anyhow!("candidate is absent from frozen block order"))?
        .try_into()
        .context("candidate order position does not fit u32")?;
    let mut argv = vec![CANDIDATE_EXECUTABLE_PATH.to_owned()];
    argv.extend(metadata.candidate_arguments.iter().cloned());
    let manifest = RunManifest {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        experiment_id: EXPERIMENT_ID.to_owned(),
        run_id: metadata.run_id.clone(),
        block_index: metadata.block_index,
        retained: metadata.block_index != 0,
        candidate: metadata.candidate,
        candidate_order_position,
        freeze_utc: metadata.freeze_utc.clone(),
        plan_bundle_sha256: Sha256Digest::parse(EMBEDDED_PLAN_BUNDLE_SHA256)?,
        authority_policy_sha256: authority_policy.sha256.clone(),
        authority_policy,
        source_review_sha256: source_review.sha256.clone(),
        source_review: source_review.clone(),
        scenario: ScenarioEvidence {
            path: relative(SCENARIO_PATH)?,
            sha256: scenario_sha256.clone(),
            schema_path: relative(SCENARIO_SCHEMA_PATH)?,
            schema_sha256: scenario_schema_sha256.clone(),
        },
        oracle: OracleEvidence {
            revision: oracle_source_snapshot.sha256.as_str().to_owned(),
            source_tree_sha256: oracle_source_snapshot.sha256.clone(),
            source_snapshot: oracle_source_snapshot,
            executable: oracle_executable,
            protocol_source_sha256,
            raw_summary_schema_sha256,
            run_manifest_schema_sha256,
        },
        candidate_build: CandidateBuildEvidence {
            source_revision: candidate_source_snapshot.sha256.as_str().to_owned(),
            source_tree_sha256: candidate_source_snapshot.sha256.clone(),
            source_snapshot: candidate_source_snapshot,
            executable: candidate_executable.clone(),
            argv,
            cargo_lock: candidate_lock,
            sloc_manifest: candidate_sloc_manifest,
            sloc_result: candidate_sloc_result,
            production_sloc: build_metadata.production_sloc,
            direct_dependencies: build_metadata.direct_dependencies,
            transitive_package_count: build_metadata.transitive_package_count,
            binary_bytes: candidate_executable.bytes,
            execution_topology: execution_topology(metadata.candidate),
            embedded_wasm_engine: embedded_wasm_engine(metadata.candidate),
            build_command: CANDIDATE_BUILD_COMMAND.to_owned(),
        },
        guest_artifacts: GuestArtifactsEvidence {
            a: guest_a,
            b: guest_b,
            bad_a: guest_bad_a,
            bad_b: guest_bad_b,
            authority: authority_manifest,
        },
        environment: metadata.environment.clone(),
        effort: EffortEvidence {
            checkpoint_elapsed_minutes: metadata.effort.checkpoint_elapsed_minutes,
            completion_elapsed_minutes: metadata.effort.completion_elapsed_minutes,
            tuning_elapsed_minutes: metadata.effort.tuning_elapsed_minutes,
            tuning_revisions: metadata.effort.tuning_revisions,
            architecture_review_sha256: source_review.sha256,
        },
    };
    manifest
        .validate()
        .context("validate truthful frozen run manifest")?;

    let run = ImmutableEvidenceDirectory::create(&configuration.evidence_root, manifest)
        .context("create immutable evidence directory")?;
    for item in bound.drain(..) {
        run.write_bound_artifact(&item.artifact, &item.bytes)
            .with_context(|| format!("write bound artifact {}", item.artifact.path.as_str()))?;
    }
    let mut auxiliary = vec![
        AuxiliaryBytes {
            path: relative(SCENARIO_PATH)?,
            bytes: scenario_bytes,
            expected_sha256: scenario_sha256,
        },
        AuxiliaryBytes {
            path: relative(SCENARIO_SCHEMA_PATH)?,
            bytes: scenario_schema_bytes,
            expected_sha256: scenario_schema_sha256,
        },
        AuxiliaryBytes {
            path: relative(PROTOCOL_SOURCE_PATH)?,
            bytes: protocol_source_bytes,
            expected_sha256: run.manifest().oracle.protocol_source_sha256.clone(),
        },
        AuxiliaryBytes {
            path: relative(RAW_SUMMARY_SCHEMA_PATH)?,
            bytes: raw_summary_schema_bytes,
            expected_sha256: run.manifest().oracle.raw_summary_schema_sha256.clone(),
        },
        AuxiliaryBytes {
            path: relative(RUN_MANIFEST_SCHEMA_PATH)?,
            bytes: run_manifest_schema_bytes,
            expected_sha256: run.manifest().oracle.run_manifest_schema_sha256.clone(),
        },
    ];
    for probe in Probe::ALL {
        let bytes = read_nonempty(&authority_output.join(probe.file_name()))?;
        auxiliary.push(AuxiliaryBytes {
            path: relative(&format!("guest-artifacts/authority/{}", probe.file_name()))?,
            expected_sha256: digest(&bytes)?,
            bytes,
        });
    }
    for item in auxiliary {
        let evidence = run.write_evidence(item.path, &item.bytes)?;
        ensure!(
            evidence.sha256 == item.expected_sha256,
            "auxiliary evidence digest changed while writing"
        );
    }
    run.verify_manifest()?;
    run.verify_bound_artifacts()?;

    Ok(PreparedRun {
        run,
        harness,
        scratch,
        candidate_arguments: metadata.candidate_arguments.clone(),
    })
}

fn bound_bytes(destination: &str, source: &Path) -> Result<BoundBytes> {
    let bytes = read_nonempty(source)?;
    Ok(BoundBytes {
        artifact: ArtifactEvidence {
            path: relative(destination)?,
            sha256: digest(&bytes)?,
            bytes: bytes
                .len()
                .try_into()
                .context("artifact size does not fit u64")?,
        },
        bytes,
    })
}

fn verify_source_review(candidate: CandidateKind, bytes: &[u8]) -> Result<()> {
    let document: SourceReviewDocument =
        serde_json::from_slice(bytes).context("parse bound source-review JSON")?;
    ensure!(
        document.schema_version == 1,
        "source review schema version drift"
    );
    ensure!(
        document.experiment_id == EXPERIMENT_ID,
        "source review experiment mismatch"
    );
    ensure!(
        document.candidate == candidate,
        "source review candidate mismatch"
    );
    ensure!(
        document.blocking_findings.is_empty(),
        "source review has blocking findings"
    );
    ensure!(
        document.overall_verdict == SOURCE_REVIEW_PASS,
        "source review does not permit the execution freeze"
    );
    Ok(())
}

fn derive_build_metadata(
    candidate: CandidateKind,
    sloc_manifest: &ArtifactEvidence,
    bytes: &[u8],
) -> Result<DerivedBuildMetadata> {
    let document: SlocResultDocument =
        serde_json::from_slice(bytes).context("parse bound SLOC result JSON")?;
    let expected = expected_sloc_fingerprint(candidate);
    ensure!(
        document.schema_version == 3,
        "SLOC result schema version drift"
    );
    ensure!(
        document.accounting_policy == SLOC_ACCOUNTING_POLICY,
        "SLOC accounting policy drift"
    );
    ensure!(
        document.candidate == expected.candidate,
        "SLOC result candidate mismatch"
    );
    ensure!(
        document.manifest_sha256 == sloc_manifest.sha256,
        "SLOC result does not bind the supplied SLOC manifest"
    );
    ensure!(
        document.production_sloc == expected.production_sloc,
        "SLOC result production count drift"
    );
    ensure!(
        document.cargo_metadata.status == "verified"
            && document.cargo_metadata.candidate_package_count == 1,
        "SLOC Cargo metadata was not verified for exactly one candidate package"
    );
    ensure!(
        document.cargo_metadata.package_count == expected.transitive_package_count,
        "SLOC transitive package count drift"
    );
    ensure!(
        document.cargo_metadata.direct_dependencies.policy == DIRECT_DEPENDENCY_POLICY
            && document
                .cargo_metadata
                .direct_dependencies
                .policy_sha256
                .as_str()
                == expected.direct_dependency_policy_sha256,
        "direct-dependency policy drift"
    );

    let declarations = document.cargo_metadata.direct_dependencies.declarations;
    ensure!(
        !declarations.is_empty() && declarations.len() == expected.direct_dependencies.len(),
        "direct-dependency declarations are missing or have unexpected cardinality"
    );
    let mut names = HashSet::new();
    let mut direct_dependencies = Vec::with_capacity(declarations.len());
    for declaration in declarations {
        ensure!(
            !declaration.name.is_empty()
                && !declaration.version.is_empty()
                && names.insert(declaration.name.clone()),
            "direct-dependency declarations contain a missing or duplicate dependency"
        );
        direct_dependencies.push(format!("{}@{}", declaration.name, declaration.version));
    }
    ensure!(
        direct_dependencies == expected.direct_dependencies,
        "direct-dependency declarations drift from the frozen candidate fingerprint"
    );

    Ok(DerivedBuildMetadata {
        production_sloc: document.production_sloc,
        direct_dependencies,
        transitive_package_count: document.cargo_metadata.package_count,
    })
}

fn expected_sloc_fingerprint(candidate: CandidateKind) -> ExpectedSlocFingerprint {
    let (name, sloc, packages, policy_sha256, dependencies): (&str, u64, u64, &str, &[&str]) =
        match candidate {
            CandidateKind::Lunatic => (
                "lunatic",
                1_587,
                244,
                "3e6621bccfff101ca554cd4dc1dce854d539190213677f57a5d3da0d92e3fb9f",
                &[
                    "anyhow@1.0.100",
                    "lunatic-process@0.14.0",
                    "serde@1.0.229",
                    "serde_json@1.0.151",
                    "sha2@0.10.9",
                    "tokio@1.53.1",
                    "wasmtime@46.0.1",
                ],
            ),
            CandidateKind::Extism => (
                "extism",
                1_831,
                286,
                "158ec6c8286a0866e4f1c45546016ed9cfef0ab394d2c0be6d9151650a5dc415",
                &[
                    "anyhow@1.0.100",
                    "extism@1.30.0",
                    "serde@1.0.229",
                    "serde_json@1.0.151",
                    "sha2@0.10.9",
                ],
            ),
            CandidateKind::RawWasmtime => (
                "direct-wasmtime",
                1_887,
                111,
                "39f750f76d9d8376573bbe8076300865dac1537db22b9eead6f07b95cb6decdd",
                &[
                    "anyhow@1.0.100",
                    "serde@1.0.229",
                    "serde_json@1.0.151",
                    "sha2@0.10.9",
                    "tokio@1.53.1",
                    "wasmtime@46.0.1",
                ],
            ),
        };
    ExpectedSlocFingerprint {
        candidate: name,
        production_sloc: sloc,
        transitive_package_count: packages,
        direct_dependency_policy_sha256: policy_sha256,
        direct_dependencies: dependencies
            .iter()
            .map(|dependency| (*dependency).to_owned())
            .collect(),
    }
}

fn verify_core_guest_hash(item: &BoundBytes, expected: &str, label: &str) -> Result<()> {
    ensure!(
        item.artifact.sha256.as_str() == expected,
        "{label} guest artifact does not match the frozen identity"
    );
    Ok(())
}

fn execution_topology(candidate: CandidateKind) -> ExecutionTopologyEvidence {
    match candidate {
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
    }
}

fn embedded_wasm_engine(candidate: CandidateKind) -> EmbeddedWasmEngineEvidence {
    EmbeddedWasmEngineEvidence {
        implementation: "wasmtime".to_owned(),
        version: match candidate {
            CandidateKind::Lunatic | CandidateKind::RawWasmtime => "46.0.1",
            CandidateKind::Extism => "43.0.2",
        }
        .to_owned(),
    }
}

fn verify_generated_authority_suite(
    harness: &AuthorityHarness,
    output: &Path,
    manifest_path: &Path,
) -> Result<()> {
    let manifest_bytes = read_nonempty(manifest_path)?;
    let manifest: Value =
        serde_json::from_slice(&manifest_bytes).context("parse generated authority manifest")?;
    ensure!(
        manifest
            .get("suite_parameter_sha256")
            .and_then(Value::as_str)
            == Some(harness.suite_parameter_sha256()),
        "generated authority suite parameter digest drift"
    );
    let records = manifest
        .get("artifacts")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("generated authority manifest has no artifacts array"))?;
    ensure!(
        records.len() == Probe::ALL.len(),
        "authority artifact cardinality drift"
    );
    for probe in Probe::ALL {
        let generated = harness.artifact(probe)?;
        let file = probe.file_name();
        let bytes = read_nonempty(&output.join(&file))?;
        ensure!(
            bytes == generated.bytes,
            "authority {file} differs from canary bytes"
        );
        let record = records
            .iter()
            .find(|record| record.get("probe").and_then(Value::as_str) == Some(probe.as_str()))
            .ok_or_else(|| anyhow!("authority manifest omitted {}", probe.as_str()))?;
        ensure!(
            record.get("file").and_then(Value::as_str) == Some(file.as_str())
                && record.get("sha256").and_then(Value::as_str)
                    == Some(generated.artifact_sha256.as_str())
                && record.get("parameter_sha256").and_then(Value::as_str)
                    == Some(generated.parameter_sha256.as_str())
                && record.get("bytes").and_then(Value::as_u64) == Some(u64::try_from(bytes.len())?),
            "authority manifest record for {} does not bind the generated module",
            probe.as_str()
        );
    }
    Ok(())
}

fn read_nonempty(path: &Path) -> Result<Vec<u8>> {
    let bytes =
        fs::read(path).with_context(|| format!("read evidence input {}", path.display()))?;
    ensure!(
        !bytes.is_empty(),
        "evidence input {} is empty",
        path.display()
    );
    Ok(bytes)
}

fn relative(path: &str) -> Result<RelativeEvidencePath> {
    RelativeEvidencePath::parse(path).map_err(Into::into)
}

fn digest(bytes: &[u8]) -> Result<Sha256Digest> {
    Sha256Digest::parse(format!("{:x}", Sha256::digest(bytes))).map_err(Into::into)
}

fn short_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))[..16].to_owned()
}

struct WorkloadProduct {
    minimal_anchor: StableRssAnchor,
    shared_anchor: StableRssAnchor,
    ready_anchor: StableRssAnchor,
    authority: Vec<AuthorityRunEvidence>,
    cleanup: Vec<CleanupRssTarget>,
}

#[derive(Debug)]
struct PhaseFailure {
    phase: FailurePhase,
    status: IncompleteStatus,
    code: &'static str,
    error: anyhow::Error,
}

impl PhaseFailure {
    fn workload(phase: FailurePhase, error: anyhow::Error) -> Self {
        let message = format!("{error:#}").to_ascii_lowercase();
        let status = if message.contains("timeout") || message.contains("deadline") {
            IncompleteStatus::TimedOut
        } else {
            IncompleteStatus::Failed
        };
        Self {
            phase,
            status,
            code: "workload_contract_failed",
            error,
        }
    }

    fn invalid(phase: FailurePhase, code: &'static str, error: anyhow::Error) -> Self {
        Self {
            phase,
            status: IncompleteStatus::Invalid,
            code,
            error,
        }
    }

    fn crashed(error: anyhow::Error) -> Self {
        Self {
            phase: FailurePhase::ProcessExit,
            status: IncompleteStatus::Crashed,
            code: "candidate_process_exit_failed",
            error,
        }
    }
}

fn run_workload(
    runner: &mut WorkloadRunner,
    plan: &[crate::workload::PlannedAction],
    harness: &AuthorityHarness,
    run_id: &str,
    policy_sha256: &str,
) -> std::result::Result<WorkloadProduct, PhaseFailure> {
    let hello_terminal = runner
        .hello()
        .map_err(|error| PhaseFailure::workload(FailurePhase::HelloAndMinimalRss, error))?;
    let minimal_anchor = stable_anchor(hello_terminal).map_err(|message| {
        PhaseFailure::invalid(
            FailurePhase::HelloAndMinimalRss,
            "minimal_rss_anchor_invalid",
            anyhow!(message),
        )
    })?;
    thread::sleep(Duration::from_millis(230));

    let init_terminal = runner
        .init(run_id)
        .map_err(|error| PhaseFailure::workload(FailurePhase::SharedInit, error))?;
    let shared_anchor = stable_anchor(init_terminal).map_err(|message| {
        PhaseFailure::invalid(
            FailurePhase::SharedInit,
            "shared_rss_anchor_invalid",
            anyhow!(message),
        )
    })?;
    thread::sleep(Duration::from_millis(230));

    let ready_terminal = runner
        .run_initial_create(plan)
        .map_err(|error| PhaseFailure::workload(FailurePhase::Create, error))?;
    let ready_anchor = stable_anchor(ready_terminal).map_err(|message| {
        PhaseFailure::invalid(
            FailurePhase::Create,
            "ready_rss_anchor_invalid",
            anyhow!(message),
        )
    })?;
    thread::sleep(Duration::from_millis(230));

    runner
        .run_normal_pressure(plan)
        .map_err(|error| PhaseFailure::workload(FailurePhase::Normal, error))?;
    runner
        .run_faults(plan)
        .map_err(|error| PhaseFailure::workload(FailurePhase::Faults, error))?;
    runner
        .run_updates(plan)
        .map_err(|error| PhaseFailure::workload(FailurePhase::Updates, error))?;
    let authority = runner
        .run_authority(plan, harness, policy_sha256)
        .map_err(|error| PhaseFailure::workload(FailurePhase::Authority, error))?;
    runner
        .run_main_teardown(plan)
        .map_err(|error| PhaseFailure::workload(FailurePhase::Cleanup, error))?;
    let cleanup = runner
        .run_cleanup(plan)
        .map_err(|error| PhaseFailure::workload(FailurePhase::Cleanup, error))?;
    runner
        .shutdown()
        .map_err(|error| PhaseFailure::workload(FailurePhase::Shutdown, error))?;
    Ok(WorkloadProduct {
        minimal_anchor,
        shared_anchor,
        ready_anchor,
        authority,
        cleanup,
    })
}

fn execute_prepared(prepared: PreparedRun) -> Result<FullRunReport> {
    let manifest = prepared.run.manifest();
    let scenario = ScenarioDocument::embedded()?;
    let plan = build_full_plan(&scenario, manifest.block_index)?;
    verify_full_shape(&plan)?;
    let candidate_path = prepared.run.path().join(CANDIDATE_EXECUTABLE_PATH);
    let combined_trace_path = prepared.scratch.path.join("ordered-trace.ndjson");
    let mut command = Command::new(&candidate_path);
    command
        .args(&prepared.candidate_arguments)
        .current_dir(prepared.run.path());
    let process = CandidateProcess::spawn(command, &combined_trace_path)
        .with_context(|| format!("spawn bound candidate {}", candidate_path.display()))?;
    let clock = process.measurement_parts().0;
    let started_monotonic_ns = clock.now_ns();
    let sampler = ProcessSampler::start(&process);
    let session = OracleSession::new(process, TimeoutTable::default());
    let mut runner = WorkloadRunner::new(session, manifest.candidate, manifest.block_index)?;
    let run_result = run_workload(
        &mut runner,
        &plan,
        &prepared.harness,
        &manifest.run_id,
        manifest.authority_policy_sha256.as_str(),
    );

    let (sampler_status, samples) = sampler.stop_with_samples();
    let (mut session, model, timings, counts) = runner.into_parts();
    let mut failure = run_result.as_ref().err().map(|value| PhaseFailure {
        phase: value.phase,
        status: value.status,
        code: value.code,
        error: anyhow!(format!("{:#}", value.error)),
    });
    if let SamplerStatus::Invalid(problem) = sampler_status {
        if failure.is_none() {
            failure = Some(PhaseFailure::invalid(
                FailurePhase::Cleanup,
                "process_sampler_invalid",
                anyhow!(format!("{problem:?}")),
            ));
        }
    }
    if samples.iter().any(|sample| sample.process_count != 1) && failure.is_none() {
        failure = Some(PhaseFailure::invalid(
            FailurePhase::Cleanup,
            "one_process_profile_violated",
            anyhow!("RSS sampler observed a process tree with process_count != 1"),
        ));
    }
    if model.active_tenants() != 0 && failure.is_none() {
        failure = Some(PhaseFailure::invalid(
            FailurePhase::Cleanup,
            "active_tenants_after_run",
            anyhow!(
                "workload ended with {} active tenants",
                model.active_tenants()
            ),
        ));
    }

    session.process_mut().close_stdin();
    let exit_result = session.process_mut().wait_for_exit(Duration::from_secs(2));
    if let Ok(exit) = &exit_result {
        if !exit.success() && failure.is_none() {
            failure = Some(PhaseFailure::crashed(anyhow!(
                "candidate exited unsuccessfully: {exit}"
            )));
        }
    } else if failure.is_none() {
        failure = Some(PhaseFailure::crashed(anyhow!(format!(
            "candidate exit could not be observed: {}",
            exit_result.as_ref().unwrap_err()
        ))));
    }
    drop(session);
    let split = split_ordered_trace(&combined_trace_path)
        .context("split finalized candidate trace without reserializing records")?;
    let finished_monotonic_ns = clock.now_ns();
    let process_exit = split
        .process_exit
        .clone()
        .unwrap_or_else(|| synthetic_process_exit(&exit_result, finished_monotonic_ns));
    let raw_evidence = write_raw_evidence(
        &prepared.run,
        split,
        run_result
            .as_ref()
            .ok()
            .map(|product| product.authority.as_slice()),
    )?;

    if let Some(problem) = failure {
        return write_incomplete_report(
            &prepared.run,
            started_monotonic_ns,
            finished_monotonic_ns,
            process_exit,
            &counts,
            raw_evidence,
            problem,
        );
    }

    let product = run_result.expect("no failure requires a completed workload product");
    let outcome = match completed_outcome(&scenario, &timings, &counts, &samples, &product) {
        Ok(outcome) => outcome,
        Err(error) => {
            return write_incomplete_report(
                &prepared.run,
                started_monotonic_ns,
                finished_monotonic_ns,
                process_exit,
                &counts,
                raw_evidence,
                PhaseFailure::invalid(FailurePhase::Cleanup, "completed_evidence_invalid", error),
            );
        }
    };
    let summary = RawRunSummary {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        experiment_id: EXPERIMENT_ID.to_owned(),
        run_id: manifest.run_id.clone(),
        block_index: manifest.block_index,
        retained: manifest.retained,
        candidate: manifest.candidate,
        scenario_sha256: manifest.scenario.sha256.clone(),
        manifest_sha256: prepared.run.manifest_evidence().sha256.clone(),
        started_monotonic_ns,
        finished_monotonic_ns,
        process_exit,
        outcome,
        evidence: raw_evidence,
    };
    if let Err(error) = summary.validate(manifest) {
        return write_incomplete_report(
            &prepared.run,
            started_monotonic_ns,
            finished_monotonic_ns,
            summary.process_exit.clone(),
            &counts,
            summary.evidence.clone(),
            PhaseFailure::invalid(
                FailurePhase::Cleanup,
                "completed_summary_invalid",
                error.into(),
            ),
        );
    }
    prepared.run.write_completed_summary(&summary)?;
    prepared.run.verify_manifest()?;
    let retained = retained_run(
        &summary,
        samples.iter().all(|sample| sample.process_count == 1),
    )?;
    let retained_path = prepared.run.write_evidence(
        relative(RETAINED_RUN_PATH)?,
        &json_document_bytes(&retained)?,
    )?;
    Ok(FullRunReport {
        run_path: prepared.run.path().to_path_buf(),
        raw_summary_path: prepared.run.path().join("raw-summary.json"),
        retained_run_path: Some(prepared.run.path().join(retained_path.path.as_str())),
        completed: true,
        overall_pass: Some(summary.outcome.overall_pass),
    })
}

fn write_incomplete_report(
    run: &ImmutableEvidenceDirectory,
    started_monotonic_ns: u64,
    finished_monotonic_ns: u64,
    process_exit: ProcessExitEvidence,
    counts: &WorkloadCounts,
    evidence: RawEvidenceFiles,
    problem: PhaseFailure,
) -> Result<FullRunReport> {
    let manifest = run.manifest();
    let summary = RawRunSummary {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        experiment_id: EXPERIMENT_ID.to_owned(),
        run_id: manifest.run_id.clone(),
        block_index: manifest.block_index,
        retained: manifest.retained,
        candidate: manifest.candidate,
        scenario_sha256: manifest.scenario.sha256.clone(),
        manifest_sha256: run.manifest_evidence().sha256.clone(),
        started_monotonic_ns,
        finished_monotonic_ns,
        process_exit,
        outcome: IncompleteOutcome {
            status: problem.status,
            failure: RunFailure {
                phase: problem.phase,
                code: problem.code.to_owned(),
                message: format!("{:#}", problem.error),
                request_id: None,
            },
            partial_counts: PartialCounts {
                controls_sent: counts.controls_sent,
                events_received: counts.events_received,
            },
            protocol_violations: 0,
            hidden_retry_count: 0,
        },
        evidence,
    };
    run.write_incomplete_summary(&summary)?;
    Ok(FullRunReport {
        run_path: run.path().to_path_buf(),
        raw_summary_path: run.path().join("raw-summary.json"),
        retained_run_path: None,
        completed: false,
        overall_pass: None,
    })
}

fn synthetic_process_exit(
    exit: &std::result::Result<ExitStatus, crate::TransportError>,
    observed_monotonic_ns: u64,
) -> ProcessExitEvidence {
    match exit {
        Ok(status) => ProcessExitEvidence {
            observed_monotonic_ns,
            code: status.code(),
            success: status.success(),
        },
        Err(_) => ProcessExitEvidence {
            observed_monotonic_ns,
            code: None,
            success: false,
        },
    }
}

struct SplitTrace {
    stream: Vec<u8>,
    rss: Vec<u8>,
    stderr: Vec<u8>,
    process_exit: Option<ProcessExitEvidence>,
}

fn split_ordered_trace(path: &Path) -> Result<SplitTrace> {
    let file = File::open(path).with_context(|| format!("open trace {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut stream = Vec::new();
    let mut rss = Vec::new();
    let mut stderr = Vec::new();
    let mut process_exit = None;
    let mut expected_trace_seq = 1_u64;
    loop {
        let mut line = Vec::new();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        ensure!(
            line.ends_with(b"\n"),
            "ordered trace ended with a partial line"
        );
        let value: Value = serde_json::from_slice(&line).context("parse ordered trace wrapper")?;
        ensure!(
            value.get("trace_seq").and_then(Value::as_u64) == Some(expected_trace_seq),
            "ordered trace sequence is not contiguous at {expected_trace_seq}"
        );
        expected_trace_seq = expected_trace_seq
            .checked_add(1)
            .ok_or_else(|| anyhow!("trace sequence overflow"))?;
        let kind = value
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("ordered trace record has no kind"))?;
        match kind {
            "resource_sample" | "resource_sample_error" => rss.extend_from_slice(&line),
            "stderr" => stderr.extend_from_slice(&line),
            "stdin" | "stdout" => stream.extend_from_slice(&line),
            "process_exit" => {
                ensure!(
                    process_exit.is_none(),
                    "ordered trace has multiple process exits"
                );
                let code = value
                    .get("code")
                    .and_then(Value::as_i64)
                    .map(i32::try_from)
                    .transpose()
                    .context("process exit code does not fit i32")?;
                process_exit = Some(ProcessExitEvidence {
                    observed_monotonic_ns: value
                        .get("monotonic_ns")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| anyhow!("process exit has no monotonic timestamp"))?,
                    code,
                    success: value
                        .get("success")
                        .and_then(Value::as_bool)
                        .ok_or_else(|| anyhow!("process exit has no success flag"))?,
                });
                stream.extend_from_slice(&line);
            }
            other => bail!("ordered trace contains unknown kind {other:?}"),
        }
    }
    ensure!(expected_trace_seq > 1, "ordered trace is empty");
    Ok(SplitTrace {
        stream,
        rss,
        stderr,
        process_exit,
    })
}

fn write_raw_evidence(
    run: &ImmutableEvidenceDirectory,
    split: SplitTrace,
    authority: Option<&[AuthorityRunEvidence]>,
) -> Result<RawEvidenceFiles> {
    let stream_trace = run.write_evidence(relative(STREAM_TRACE_PATH)?, &split.stream)?;
    let rss_trace = run.write_evidence(relative(RSS_TRACE_PATH)?, &split.rss)?;
    let stderr_trace = run.write_evidence(relative(STDERR_TRACE_PATH)?, &split.stderr)?;
    let (filesystem, network) = authority_observer_traces(authority.unwrap_or_default())?;
    let filesystem_observer_trace =
        run.write_evidence(relative(FILESYSTEM_TRACE_PATH)?, &filesystem)?;
    let network_observer_trace = run.write_evidence(relative(NETWORK_TRACE_PATH)?, &network)?;
    Ok(RawEvidenceFiles {
        run_manifest: run.manifest_evidence().clone(),
        stream_trace,
        rss_trace,
        filesystem_observer_trace,
        network_observer_trace,
        stderr_trace,
    })
}

fn authority_observer_traces(authority: &[AuthorityRunEvidence]) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut filesystem = Vec::new();
    let mut network = Vec::new();
    let mut filesystem_seq = 1_u64;
    let mut network_seq = 1_u64;
    for evidence in authority {
        let (target, sequence) = match evidence.no_effect.coverage {
            DenialObservationCoverage::NoPortableReadAudit
            | DenialObservationCoverage::PolledDurableState => {
                (&mut filesystem, &mut filesystem_seq)
            }
            DenialObservationCoverage::BoundEndpointAttempts => (&mut network, &mut network_seq),
        };
        let mut value = serde_json::to_value(evidence)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| anyhow!("authority evidence did not serialize to an object"))?;
        object.insert("trace_schema_version".into(), Value::from(1));
        object.insert("trace_seq".into(), Value::from(*sequence));
        object.insert("kind".into(), Value::from("authority_observation"));
        *sequence = sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("authority observer sequence overflow"))?;
        serde_json::to_writer(&mut *target, &value)?;
        target.write_all(b"\n")?;
    }
    Ok((filesystem, network))
}

fn completed_outcome(
    scenario: &ScenarioDocument,
    timings: &TimingSamples,
    counts: &WorkloadCounts,
    samples: &[ProcessTreeSample],
    product: &WorkloadProduct,
) -> Result<CompletedOutcome> {
    ensure!(
        counts.accepted_commands == counts.terminal_accepted_commands,
        "accepted and terminal accepted command counts differ"
    );
    ensure!(
        product.authority.len() == 6,
        "authority evidence cardinality drift"
    );
    let cleanup_anchors: Vec<_> = product
        .cleanup
        .iter()
        .map(|target| StableRssAnchor {
            target_monotonic_ns: target.target_monotonic_ns,
        })
        .collect();
    let rss = rss_evidence(
        samples,
        &product.minimal_anchor,
        &product.shared_anchor,
        &product.ready_anchor,
        &cleanup_anchors,
    )
    .map_err(|message| anyhow!(message))?;
    let slos = &scenario.verdict.absolute_slos;
    let metrics = CompletedMetrics {
        shared_init_ns: timings.shared_init_ns,
        warm_create: metric_evidence(
            &timings.warm_create_ns,
            None,
            milliseconds(slos.warm_create_p99_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
        normal: normal_metric_evidence(
            &timings.normal_ns,
            timings.normal_total_ns,
            milliseconds(slos.normal_p99_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
        pressure_admission: metric_evidence(
            &timings.pressure_admission_ns,
            None,
            milliseconds(slos.pressure_admission_p99_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
        sibling: metric_evidence(
            &timings.sibling_ns,
            None,
            milliseconds(slos.sibling_p99_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
        trap_replacement_ready: metric_evidence(
            &timings.trap_replacement_ready_ns,
            None,
            milliseconds(slos.trap_replacement_ready_p99_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
        cpu_replacement_ready: metric_evidence(
            &timings.cpu_replacement_ready_ns,
            None,
            milliseconds(slos.cpu_replacement_ready_p99_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
        cpu_failed_terminal: maximum_metric_evidence(
            &timings.cpu_failed_terminal_ns,
            Some(milliseconds(slos.cpu_failed_terminal_min_ms)?),
            milliseconds(slos.cpu_failed_terminal_max_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
        failed_update_unavailability: metric_evidence(
            &timings.failed_update_unavailability_ns,
            None,
            milliseconds(slos.failed_update_unavailability_p99_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
        valid_update_unavailability: metric_evidence(
            &timings.valid_update_unavailability_ns,
            None,
            milliseconds(slos.valid_update_unavailability_p99_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
        failed_rollout: metric_evidence(
            &timings.failed_rollout_ns,
            None,
            milliseconds(slos.failed_rollout_p99_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
        valid_rollout: metric_evidence(
            &timings.valid_rollout_ns,
            None,
            milliseconds(slos.valid_rollout_p99_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
        failed_mixed_window: nonnegative_metric_evidence(
            &timings.failed_mixed_window_ns,
            milliseconds(slos.failed_mixed_window_max_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
        valid_mixed_window: nonnegative_metric_evidence(
            &timings.valid_mixed_window_ns,
            milliseconds(slos.valid_mixed_window_max_ms)?,
        )
        .map_err(|message| anyhow!(message))?,
    };
    let trace_backed_worker_attempt_floor = counts
        .update_worker_logical_operations
        .checked_add(counts.update_worker_retries)
        .ok_or_else(|| anyhow!("update worker attempt count overflow"))?;
    ensure!(
        counts.update_worker_transport_attempts >= trace_backed_worker_attempt_floor,
        "physical update worker transports are below completed logical operations plus retries"
    );
    let completed_counts = CompletedCounts {
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
        update_worker_attempts: counts.update_worker_transport_attempts,
        authority_canaries: product.authority.len().try_into()?,
        cleanup_warmup_cycles: 5,
        cleanup_retained_cycles: product.cleanup.len().try_into()?,
        cleanup_old_endpoint_probes: 1_120,
        accepted_commands: counts.accepted_commands,
        terminal_accepted_commands: counts.terminal_accepted_commands,
    };
    let stream = relative(STREAM_TRACE_PATH)?;
    let rss_ref = relative(RSS_TRACE_PATH)?;
    let filesystem = relative(FILESYSTEM_TRACE_PATH)?;
    let network = relative(NETWORK_TRACE_PATH)?;
    let gates = MandatoryGates {
        isolation: passing_gate(vec![stream.clone()]),
        authority: passing_gate(vec![stream.clone(), filesystem, network]),
        admission: passing_gate(vec![stream.clone()]),
        recovery: passing_gate(vec![stream.clone()]),
        update_safety: passing_gate(vec![stream.clone()]),
        state_semantics: passing_gate(vec![stream.clone()]),
        cleanup: passing_gate(vec![stream, rss_ref]),
    };
    let overall_pass = metrics_pass(&metrics) && rss.pass;
    let outcome = CompletedOutcome {
        status: CompletedStatus::Completed,
        overall_pass,
        counts: completed_counts,
        gates,
        metrics_ns: metrics,
        rss,
        retry_audit: RetryAudit {
            delayed_duplicate_cases: 80,
            delayed_duplicate_requests: 240,
            pressure_retries: 1,
            update_worker_retries: counts.update_worker_retries,
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
    };
    outcome.validate()?;
    Ok(outcome)
}

fn passing_gate(evidence_refs: Vec<RelativeEvidencePath>) -> GateResult {
    GateResult {
        pass: true,
        evidence_refs,
    }
}

fn metrics_pass(metrics: &CompletedMetrics) -> bool {
    metrics.warm_create.pass
        && metrics.normal.pass
        && metrics.pressure_admission.pass
        && metrics.sibling.pass
        && metrics.trap_replacement_ready.pass
        && metrics.cpu_replacement_ready.pass
        && metrics.cpu_failed_terminal.pass
        && metrics.failed_update_unavailability.pass
        && metrics.valid_update_unavailability.pass
        && metrics.failed_rollout.pass
        && metrics.valid_rollout.pass
        && metrics.failed_mixed_window.pass
        && metrics.valid_mixed_window.pass
}

fn milliseconds(value: u32) -> Result<u64> {
    u64::from(value)
        .checked_mul(1_000_000)
        .ok_or_else(|| anyhow!("millisecond SLO overflow"))
}

fn retained_run(
    summary: &RawRunSummary<CompletedOutcome>,
    one_process_profile_passed: bool,
) -> Result<RetainedRun> {
    summary.outcome.validate()?;
    let mandatory_gates_passed = summary.outcome.gates.isolation.pass
        && summary.outcome.gates.authority.pass
        && summary.outcome.gates.admission.pass
        && summary.outcome.gates.recovery.pass
        && summary.outcome.gates.update_safety.pass
        && summary.outcome.gates.state_semantics.pass
        && summary.outcome.gates.cleanup.pass;
    let absolute_slos_passed =
        metrics_pass(&summary.outcome.metrics_ns) && summary.outcome.rss.pass;
    Ok(RetainedRun {
        block: summary
            .block_index
            .try_into()
            .context("block does not fit u8")?,
        raw_trace_verified: true,
        manifest_verified: true,
        cardinality_verified: true,
        source_review_passed: true,
        mandatory_gates_passed,
        absolute_slos_passed,
        one_process_profile_passed,
        metrics: summary.outcome.key_metrics(),
    })
}

fn json_document_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEST: AtomicU64 = AtomicU64::new(1);

    fn test_directory(label: &str) -> ScratchDirectory {
        let unique = NEXT_TEST.fetch_add(1, Ordering::Relaxed);
        ScratchDirectory::create(
            &std::env::temp_dir(),
            &format!("full-evidence-test-{label}-{}-{unique}", std::process::id()),
        )
        .unwrap()
    }

    #[test]
    fn trace_split_preserves_original_line_bytes_and_global_sequences() {
        let scratch = test_directory("split");
        let path = scratch.path.join("ordered.ndjson");
        let lines = [
            r#"{"kind":"stdin","trace_schema_version":1,"monotonic_ns":1,"line":"control","trace_seq":1}"#,
            r#"{"kind":"resource_sample","trace_schema_version":1,"monotonic_ns":2,"root_pid":9,"root_creation_time_100ns":1,"process_count":1,"sampled_process_count":1,"inaccessible_process_count":0,"thread_count":4,"working_set_bytes":10,"private_usage_bytes":10,"trace_seq":2}"#,
            r#"{"kind":"stderr","trace_schema_version":1,"monotonic_ns":3,"line":"diagnostic","trace_seq":3}"#,
            r#"{"kind":"stdout","trace_schema_version":1,"monotonic_ns":4,"line":"event","trace_seq":4}"#,
            r#"{"kind":"process_exit","trace_schema_version":1,"monotonic_ns":5,"code":0,"success":true,"trace_seq":5}"#,
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();
        let split = split_ordered_trace(&path).unwrap();
        assert_eq!(
            split.stream,
            format!("{}\n{}\n{}\n", lines[0], lines[3], lines[4]).as_bytes()
        );
        assert_eq!(split.rss, format!("{}\n", lines[1]).as_bytes());
        assert_eq!(split.stderr, format!("{}\n", lines[2]).as_bytes());
        assert_eq!(
            split.process_exit,
            Some(ProcessExitEvidence {
                observed_monotonic_ns: 5,
                code: Some(0),
                success: true,
            })
        );
    }

    #[test]
    fn trace_split_rejects_a_gap_before_writing_derived_evidence() {
        let scratch = test_directory("gap");
        let path = scratch.path.join("ordered.ndjson");
        fs::write(
            &path,
            b"{\"kind\":\"stdin\",\"trace_schema_version\":1,\"monotonic_ns\":1,\"line\":\"x\",\"trace_seq\":2}\n",
        )
        .unwrap();
        assert!(split_ordered_trace(&path).is_err());
    }

    #[test]
    fn generated_authority_manifest_transitively_binds_the_live_suite() {
        let scratch = test_directory("authority");
        let harness =
            AuthorityHarness::bind_with_nonce(&scratch.path, "test-authority-nonce").unwrap();
        let output = scratch.path.join("generated");
        let source_root =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../guests/authority");
        let manifest = build_authority_guests(&source_root, &output, harness.parameters()).unwrap();
        verify_generated_authority_suite(&harness, &output, &manifest).unwrap();

        let first = output.join(Probe::WasiP1FsRead.file_name());
        let mut bytes = fs::read(&first).unwrap();
        bytes[0] ^= 1;
        fs::write(first, bytes).unwrap();
        assert!(verify_generated_authority_suite(&harness, &output, &manifest).is_err());
    }

    #[test]
    fn sloc_metadata_is_derived_and_rejects_candidate_manifest_or_dependency_drift() {
        let embedded_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let manifest = bound_bytes(
            CANDIDATE_SLOC_MANIFEST_PATH,
            &embedded_root.join("candidates/lunatic/sloc-manifest.json"),
        )
        .unwrap();
        let result =
            fs::read(embedded_root.join("evidence-inputs/generated/lunatic-sloc-result.json"))
                .unwrap();
        let derived =
            derive_build_metadata(CandidateKind::Lunatic, &manifest.artifact, &result).unwrap();
        assert_eq!(derived.production_sloc, 1_587);
        assert_eq!(derived.transitive_package_count, 244);
        assert_eq!(
            derived.direct_dependencies,
            expected_sloc_fingerprint(CandidateKind::Lunatic).direct_dependencies
        );

        let mut wrong_candidate: Value = serde_json::from_slice(&result).unwrap();
        wrong_candidate["candidate"] = Value::String("extism".to_owned());
        assert!(derive_build_metadata(
            CandidateKind::Lunatic,
            &manifest.artifact,
            &serde_json::to_vec(&wrong_candidate).unwrap(),
        )
        .is_err());

        let wrong_manifest = bound_bytes(
            CANDIDATE_SLOC_MANIFEST_PATH,
            &embedded_root.join("oracle/Cargo.toml"),
        )
        .unwrap();
        assert!(
            derive_build_metadata(CandidateKind::Lunatic, &wrong_manifest.artifact, &result)
                .is_err()
        );

        let mut duplicate_dependency: Value = serde_json::from_slice(&result).unwrap();
        duplicate_dependency["cargo_metadata"]["direct_dependencies"]["declarations"][1]["name"] =
            duplicate_dependency["cargo_metadata"]["direct_dependencies"]["declarations"][0]
                ["name"]
                .clone();
        assert!(derive_build_metadata(
            CandidateKind::Lunatic,
            &manifest.artifact,
            &serde_json::to_vec(&duplicate_dependency).unwrap(),
        )
        .is_err());
    }

    #[test]
    fn candidate_specific_dependency_policy_hashes_pass_and_cross_hashes_fail() {
        let embedded_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let cases = [
            (
                CandidateKind::RawWasmtime,
                "direct-wasmtime",
                "direct-wasmtime-sloc-result.json",
            ),
            (
                CandidateKind::Lunatic,
                "lunatic",
                "lunatic-sloc-result.json",
            ),
            (CandidateKind::Extism, "extism", "extism-sloc-result.json"),
        ];
        for (candidate, candidate_directory, result_name) in cases {
            let manifest = bound_bytes(
                CANDIDATE_SLOC_MANIFEST_PATH,
                &embedded_root.join(format!(
                    "candidates/{candidate_directory}/sloc-manifest.json"
                )),
            )
            .unwrap();
            let result =
                fs::read(embedded_root.join(format!("evidence-inputs/generated/{result_name}")))
                    .unwrap();
            let document: Value = serde_json::from_slice(&result).unwrap();
            assert_eq!(
                document["cargo_metadata"]["direct_dependencies"]["policy_sha256"],
                expected_sloc_fingerprint(candidate).direct_dependency_policy_sha256
            );
            derive_build_metadata(candidate, &manifest.artifact, &result).unwrap();
        }

        let extism_manifest = bound_bytes(
            CANDIDATE_SLOC_MANIFEST_PATH,
            &embedded_root.join("candidates/extism/sloc-manifest.json"),
        )
        .unwrap();
        let extism_result =
            fs::read(embedded_root.join("evidence-inputs/generated/extism-sloc-result.json"))
                .unwrap();
        let mut cross_candidate_hash: Value = serde_json::from_slice(&extism_result).unwrap();
        cross_candidate_hash["cargo_metadata"]["direct_dependencies"]["policy_sha256"] =
            Value::String(
                expected_sloc_fingerprint(CandidateKind::Lunatic)
                    .direct_dependency_policy_sha256
                    .to_owned(),
            );
        assert!(derive_build_metadata(
            CandidateKind::Extism,
            &extism_manifest.artifact,
            &serde_json::to_vec(&cross_candidate_hash).unwrap(),
        )
        .is_err());

        let mut mutated_hash: Value = serde_json::from_slice(&extism_result).unwrap();
        mutated_hash["cargo_metadata"]["direct_dependencies"]["policy_sha256"] = Value::String(
            "058ec6c8286a0866e4f1c45546016ed9cfef0ab394d2c0be6d9151650a5dc415".to_owned(),
        );
        assert!(derive_build_metadata(
            CandidateKind::Extism,
            &extism_manifest.artifact,
            &serde_json::to_vec(&mutated_hash).unwrap(),
        )
        .is_err());
    }

    #[test]
    fn source_review_pass_is_derived_and_blocking_state_is_rejected() {
        let embedded_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let review =
            fs::read(embedded_root.join("evidence-inputs/lunatic/source-review.json")).unwrap();
        verify_source_review(CandidateKind::Lunatic, &review).unwrap();

        let mut wrong_verdict: Value = serde_json::from_slice(&review).unwrap();
        wrong_verdict["overall_verdict"] = Value::String("review_failed".to_owned());
        assert!(verify_source_review(
            CandidateKind::Lunatic,
            &serde_json::to_vec(&wrong_verdict).unwrap(),
        )
        .is_err());

        let mut blocked: Value = serde_json::from_slice(&review).unwrap();
        blocked["blocking_findings"] = serde_json::json!(["unresolved"]);
        assert!(verify_source_review(
            CandidateKind::Lunatic,
            &serde_json::to_vec(&blocked).unwrap(),
        )
        .is_err());

        assert!(verify_source_review(CandidateKind::Extism, &review).is_err());
    }

    #[test]
    fn preparation_writes_and_verifies_all_fourteen_bound_artifacts() {
        let scratch = test_directory("prepare");
        let inputs = scratch.path.join("inputs");
        fs::create_dir(&inputs).unwrap();
        let write = |name: &str, bytes: &[u8]| {
            let path = inputs.join(name);
            fs::write(&path, bytes).unwrap();
            path
        };
        let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let embedded_root = crate_root.join("../../..");
        let guest_root = embedded_root.join("guests/core/artifacts");
        let sources = FullRunSources {
            authority_policy: write("authority-policy.json", b"{\"deny\":true}\n"),
            source_review: embedded_root.join("evidence-inputs/lunatic/source-review.json"),
            oracle_source_snapshot: write(
                "oracle-source-snapshot.tar",
                b"oracle source snapshot bytes",
            ),
            oracle_executable: write("oracle.exe", b"MZ-oracle-test"),
            candidate_source_snapshot: write("source-snapshot.tar", b"source snapshot bytes"),
            candidate_executable: write("candidate.exe", b"MZ-candidate-test"),
            candidate_cargo_lock: write("Cargo.lock", b"# lock\n"),
            candidate_sloc_manifest: embedded_root.join("candidates/lunatic/sloc-manifest.json"),
            candidate_sloc_result: embedded_root
                .join("evidence-inputs/generated/lunatic-sloc-result.json"),
            guest_a: guest_root.join("tenant-a.wasm"),
            guest_b: guest_root.join("tenant-b.wasm"),
            guest_bad_a: guest_root.join("tenant-bad-a.wasm"),
            guest_bad_b: guest_root.join("tenant-bad-b.wasm"),
            authority_guest_source_root: embedded_root.join("guests/authority"),
            scenario: embedded_root.join("oracle/scenario-final.json"),
            scenario_schema: embedded_root.join("oracle/schemas/scenario-final.schema.json"),
            protocol_source: embedded_root.join("protocol/candidate-wire-frozen.rs"),
            raw_summary_schema: embedded_root.join("oracle/schemas/raw-summary.schema.json"),
            run_manifest_schema: embedded_root.join("oracle/schemas/run-manifest.schema.json"),
        };
        let configuration = FullRunConfiguration {
            evidence_root: scratch.path.join("evidence"),
            scratch_root: scratch.path.join("staging"),
            metadata: FullRunMetadata {
                run_id: "full-evidence-prepare-test".to_owned(),
                block_index: 1,
                candidate: CandidateKind::Lunatic,
                freeze_utc: "2026-07-23T00:00:00Z".to_owned(),
                candidate_arguments: Vec::new(),
                environment: EnvironmentEvidence {
                    os: "Windows 11 Home 10.0.26200 build 26200".to_owned(),
                    cpu: "Intel Core i7-14700K".to_owned(),
                    logical_processors: 28,
                    physical_memory_bytes: 68_475_179_008,
                    rustc: "1.95.0".to_owned(),
                    cargo: "1.95.0".to_owned(),
                    oracle_wasmtime: "46.0.1".to_owned(),
                    tokio: "1.53.1".to_owned(),
                    node: "24.4.1".to_owned(),
                    machine_fingerprint_sha256: Sha256Digest::parse("3".repeat(64)).unwrap(),
                },
                effort: FullRunEffort {
                    checkpoint_elapsed_minutes: 1,
                    completion_elapsed_minutes: 2,
                    tuning_elapsed_minutes: 0,
                    tuning_revisions: 0,
                },
            },
            sources,
        };
        let prepared = prepare_run(&configuration).unwrap();
        assert_eq!(prepared.run.manifest().bound_artifacts().len(), 14);
        prepared.run.verify_bound_artifacts().unwrap();
        assert_eq!(
            prepared.run.manifest().oracle.revision,
            prepared
                .run
                .manifest()
                .oracle
                .source_snapshot
                .sha256
                .as_str()
        );
        assert_eq!(
            prepared.run.manifest().candidate_build.source_tree_sha256,
            prepared
                .run
                .manifest()
                .candidate_build
                .source_snapshot
                .sha256
        );
        assert_eq!(
            prepared.run.manifest().candidate_build.execution_topology,
            execution_topology(CandidateKind::Lunatic)
        );
        assert_eq!(
            prepared.run.manifest().candidate_build.production_sloc,
            1_587
        );
        assert_eq!(
            prepared
                .run
                .manifest()
                .candidate_build
                .transitive_package_count,
            244
        );
        assert_eq!(
            prepared.run.manifest().candidate_build.direct_dependencies,
            expected_sloc_fingerprint(CandidateKind::Lunatic).direct_dependencies
        );
        assert_eq!(
            prepared.run.manifest().candidate_build.build_command,
            CANDIDATE_BUILD_COMMAND
        );
        for probe in Probe::ALL {
            assert!(prepared
                .run
                .path()
                .join(format!("guest-artifacts/authority/{}", probe.file_name()))
                .is_file());
        }
    }

    #[test]
    fn candidate_topology_and_engine_mappings_are_truthful_and_closed() {
        assert_eq!(
            execution_topology(CandidateKind::RawWasmtime).tenant_execution_model,
            TenantExecutionModel::TokioTasks
        );
        let extism = execution_topology(CandidateKind::Extism);
        assert_eq!(
            extism.tenant_execution_model,
            TenantExecutionModel::DedicatedThreads
        );
        assert_eq!(extism.tokio_worker_threads, 0);
        assert_eq!(extism.dedicated_tenant_threads, 32);
        assert_eq!(
            embedded_wasm_engine(CandidateKind::Extism).version,
            "43.0.2"
        );
        assert_eq!(
            embedded_wasm_engine(CandidateKind::Lunatic).version,
            "46.0.1"
        );
    }

    #[test]
    fn millisecond_conversion_is_exact() {
        assert_eq!(milliseconds(20).unwrap(), 20_000_000);
        assert_eq!(milliseconds(2_000).unwrap(), 2_000_000_000);
    }
}
