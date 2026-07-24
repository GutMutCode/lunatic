use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::protocol::{CandidateKind, TenantId};
use crate::sampling::{least_squares, LeastSquares};

#[derive(Debug, Error, Clone, PartialEq)]
pub enum AnalysisError {
    #[error("analysis input is empty: {0}")]
    Empty(&'static str),
    #[error("analysis input is inconsistent: {0}")]
    Inconsistent(String),
    #[error("analysis contains a non-finite value")]
    NonFinite,
}

pub fn nearest_rank_u64(values: &[u64], percentile: f64) -> Result<u64, AnalysisError> {
    if values.is_empty() {
        return Err(AnalysisError::Empty("nearest-rank samples"));
    }
    if !percentile.is_finite() || !(0.0..=1.0).contains(&percentile) {
        return Err(AnalysisError::NonFinite);
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = if percentile == 0.0 {
        1
    } else {
        (percentile * sorted.len() as f64).ceil() as usize
    };
    Ok(sorted[rank.saturating_sub(1)])
}

pub fn median_f64(values: &[f64]) -> Result<f64, AnalysisError> {
    if values.is_empty() {
        return Err(AnalysisError::Empty("median samples"));
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Err(AnalysisError::NonFinite);
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    Ok(if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    })
}

fn median3(values: [u64; 3]) -> u64 {
    let mut sorted = values;
    sorted.sort_unstable();
    sorted[1]
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoodCompletion {
    pub tenant_id: TenantId,
    pub monotonic_ns: u64,
    pub logical_version: String,
    pub build_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutAnalysisInput {
    pub targets: Vec<TenantId>,
    pub rollout_write_ns: u64,
    pub proof_ns: u64,
    pub baseline_version: String,
    /// One last known-good completion before the rollout for every target,
    /// followed by every good completion through proof wave one or later.
    pub good_completions: Vec<GoodCompletion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantDowntime {
    pub tenant_id: TenantId,
    pub max_gap_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutAnalysis {
    pub duration_ns: u64,
    pub per_tenant_downtime: Vec<TenantDowntime>,
    pub downtime_p99_ns: u64,
    pub mixed_span_ns: u64,
    pub mixed_at_proof: bool,
}

pub fn analyze_rollout(input: &RolloutAnalysisInput) -> Result<RolloutAnalysis, AnalysisError> {
    if input.targets.is_empty() {
        return Err(AnalysisError::Empty("rollout targets"));
    }
    if input.proof_ns < input.rollout_write_ns {
        return Err(AnalysisError::Inconsistent(
            "proof precedes rollout write".into(),
        ));
    }
    let target_set: BTreeSet<_> = input.targets.iter().copied().collect();
    if target_set.len() != input.targets.len() {
        return Err(AnalysisError::Inconsistent(
            "rollout targets contain duplicates".into(),
        ));
    }

    let mut by_tenant: BTreeMap<TenantId, Vec<&GoodCompletion>> = BTreeMap::new();
    for completion in &input.good_completions {
        if target_set.contains(&completion.tenant_id) {
            by_tenant
                .entry(completion.tenant_id)
                .or_default()
                .push(completion);
        }
    }

    let mut per_tenant_downtime = Vec::with_capacity(input.targets.len());
    for tenant in &input.targets {
        let completions = by_tenant.get_mut(tenant).ok_or_else(|| {
            AnalysisError::Inconsistent(format!("tenant {} has no good completions", tenant.0))
        })?;
        completions.sort_by_key(|completion| completion.monotonic_ns);
        if completions.len() < 2 {
            return Err(AnalysisError::Inconsistent(format!(
                "tenant {} needs a pre-rollout anchor and a post-write completion",
                tenant.0
            )));
        }
        let mut max_gap = None;
        for pair in completions.windows(2) {
            let left = pair[0].monotonic_ns;
            let right = pair[1].monotonic_ns;
            if right < left {
                return Err(AnalysisError::Inconsistent(
                    "completion timestamps are not monotonic".into(),
                ));
            }
            // The open gap intersects the closed rollout proof interval.
            if left < input.proof_ns && right > input.rollout_write_ns {
                max_gap = Some(max_gap.unwrap_or(0_u64).max(right - left));
            }
        }
        let max_gap_ns = max_gap.ok_or_else(|| {
            AnalysisError::Inconsistent(format!(
                "tenant {} has no good-completion gap intersecting rollout",
                tenant.0
            ))
        })?;
        per_tenant_downtime.push(TenantDowntime {
            tenant_id: *tenant,
            max_gap_ns,
        });
    }
    per_tenant_downtime.sort_by_key(|sample| sample.tenant_id);
    let downtime_samples: Vec<_> = per_tenant_downtime
        .iter()
        .map(|sample| sample.max_gap_ns)
        .collect();

    let (mixed_span_ns, mixed_at_proof) = mixed_version_span(input)?;
    Ok(RolloutAnalysis {
        duration_ns: input.proof_ns - input.rollout_write_ns,
        per_tenant_downtime,
        downtime_p99_ns: nearest_rank_u64(&downtime_samples, 0.99)?,
        mixed_span_ns,
        mixed_at_proof,
    })
}

fn mixed_version_span(input: &RolloutAnalysisInput) -> Result<(u64, bool), AnalysisError> {
    let target_set: BTreeSet<_> = input.targets.iter().copied().collect();
    let mut observed: BTreeMap<TenantId, String> = input
        .targets
        .iter()
        .map(|tenant| (*tenant, input.baseline_version.clone()))
        .collect();
    let mut events: Vec<_> = input
        .good_completions
        .iter()
        .filter(|completion| {
            target_set.contains(&completion.tenant_id)
                && completion.monotonic_ns >= input.rollout_write_ns
                && completion.monotonic_ns <= input.proof_ns
        })
        .collect();
    events.sort_by_key(|completion| completion.monotonic_ns);

    let mut mixed = false;
    let mut first_entry = None;
    let mut last_exit = None;
    for completion in events {
        observed.insert(completion.tenant_id, completion.logical_version.clone());
        let now_mixed = observed.values().collect::<BTreeSet<_>>().len() > 1;
        match (mixed, now_mixed) {
            (false, true) => first_entry.get_or_insert(completion.monotonic_ns),
            (true, false) => {
                last_exit = Some(completion.monotonic_ns);
                &mut 0
            }
            _ => &mut 0,
        };
        mixed = now_mixed;
    }

    if mixed {
        let first = first_entry.unwrap_or(input.rollout_write_ns);
        Ok((input.proof_ns - first, true))
    } else {
        Ok(match (first_entry, last_exit) {
            (Some(first), Some(last)) => (last - first, false),
            _ => (0, false),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupRssCycle {
    pub cycle: u32,
    pub active_tenants_pre_teardown: u32,
    pub pre_teardown_samples: [u64; 3],
    pub post_teardown_samples: [u64; 3],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RssAnalysisInput {
    pub baseline_samples: [u64; 3],
    pub initialized_samples: [u64; 3],
    pub ready_samples: [u64; 3],
    pub create_to_ready_periodic_samples: Vec<u64>,
    pub retained_cleanup: Vec<CleanupRssCycle>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupRssPoint {
    pub cycle: u32,
    pub live_bytes: u64,
    pub cleanup_bytes: u64,
    pub excess_bytes: i128,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RssAnalysis {
    pub baseline_bytes: u64,
    pub initialized_bytes: u64,
    pub ready_bytes: u64,
    pub peak_bytes: u64,
    pub delta_32_bytes: i128,
    pub cleanup: Vec<CleanupRssPoint>,
    pub cleanup_slope_bytes_per_cycle: f64,
    pub cleanup_r_squared: f64,
}

pub fn analyze_rss(input: &RssAnalysisInput) -> Result<RssAnalysis, AnalysisError> {
    let baseline = median3(input.baseline_samples);
    let initialized = median3(input.initialized_samples);
    let ready = median3(input.ready_samples);
    let peak = *input
        .create_to_ready_periodic_samples
        .iter()
        .max()
        .ok_or(AnalysisError::Empty("create-to-ready RSS samples"))?;
    if input.retained_cleanup.len() != 30 {
        return Err(AnalysisError::Inconsistent(format!(
            "expected 30 retained cleanup cycles, received {}",
            input.retained_cleanup.len()
        )));
    }

    let mut cleanup = Vec::with_capacity(30);
    for (index, cycle) in input.retained_cleanup.iter().enumerate() {
        if cycle.cycle != (index + 1) as u32 {
            return Err(AnalysisError::Inconsistent(
                "cleanup cycles must be contiguous 1..30".into(),
            ));
        }
        if cycle.active_tenants_pre_teardown != 32 {
            return Err(AnalysisError::Inconsistent(format!(
                "cleanup cycle {} was sampled without 32 active tenants",
                cycle.cycle
            )));
        }
        let live = median3(cycle.pre_teardown_samples);
        let cleaned = median3(cycle.post_teardown_samples);
        cleanup.push(CleanupRssPoint {
            cycle: cycle.cycle,
            live_bytes: live,
            cleanup_bytes: cleaned,
            excess_bytes: i128::from(cleaned) - i128::from(baseline),
        });
    }

    let x: Vec<_> = cleanup.iter().map(|point| f64::from(point.cycle)).collect();
    let y: Vec<_> = cleanup
        .iter()
        .map(|point| point.cleanup_bytes as f64)
        .collect();
    let LeastSquares {
        slope, r_squared, ..
    } = least_squares(&x, &y).ok_or_else(|| {
        AnalysisError::Inconsistent("cleanup RSS slope is not identifiable".into())
    })?;

    Ok(RssAnalysis {
        baseline_bytes: baseline,
        initialized_bytes: initialized,
        ready_bytes: ready,
        peak_bytes: peak,
        delta_32_bytes: i128::from(peak) - i128::from(baseline),
        cleanup,
        cleanup_slope_bytes_per_cycle: slope,
        cleanup_r_squared: r_squared,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FinalVerdict {
    PositioningRejected,
    AlternativeDominates,
    LunaticPackagingValueSupported,
    LunaticPackagingValueUnsupported,
    TestedSetPackagingGap,
    Inconclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackagingStrength {
    Strong,
    Moderate,
    Unsupported,
}

/// Closed lower-is-better metric registry for one retained block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyMetrics {
    pub shared_init_ns: u64,
    pub warm_create_p99_ns: u64,
    pub normal_p99_ns: u64,
    pub normal_total_ns: u64,
    pub pressure_admission_p99_ns: u64,
    pub sibling_p99_ns: u64,
    pub trap_recovery_p99_ns: u64,
    pub cpu_recovery_p99_ns: u64,
    pub failed_update_unavailability_p99_ns: u64,
    pub valid_update_unavailability_p99_ns: u64,
    pub failed_rollout_p99_ns: u64,
    pub valid_rollout_p99_ns: u64,
    pub ready_32_total_rss_bytes: u64,
    pub peak_total_rss_bytes: u64,
}

impl KeyMetrics {
    pub const NAMES: [&'static str; 14] = [
        "shared_init_ns",
        "warm_create_p99_ns",
        "normal_p99_ns",
        "normal_total_ns",
        "pressure_admission_p99_ns",
        "sibling_p99_ns",
        "trap_recovery_p99_ns",
        "cpu_recovery_p99_ns",
        "failed_update_unavailability_p99_ns",
        "valid_update_unavailability_p99_ns",
        "failed_rollout_p99_ns",
        "valid_rollout_p99_ns",
        "ready_32_total_rss_bytes",
        "peak_total_rss_bytes",
    ];

    pub fn values(self) -> [u64; 14] {
        [
            self.shared_init_ns,
            self.warm_create_p99_ns,
            self.normal_p99_ns,
            self.normal_total_ns,
            self.pressure_admission_p99_ns,
            self.sibling_p99_ns,
            self.trap_recovery_p99_ns,
            self.cpu_recovery_p99_ns,
            self.failed_update_unavailability_p99_ns,
            self.valid_update_unavailability_p99_ns,
            self.failed_rollout_p99_ns,
            self.valid_rollout_p99_ns,
            self.ready_32_total_rss_bytes,
            self.peak_total_rss_bytes,
        ]
    }

    pub fn all_positive(self) -> bool {
        self.values().iter().all(|value| *value > 0)
    }
}

/// Reporting form of exact block-paired median ratios.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyMetricRatios {
    pub shared_init_ns: f64,
    pub warm_create_p99_ns: f64,
    pub normal_p99_ns: f64,
    pub normal_total_ns: f64,
    pub pressure_admission_p99_ns: f64,
    pub sibling_p99_ns: f64,
    pub trap_recovery_p99_ns: f64,
    pub cpu_recovery_p99_ns: f64,
    pub failed_update_unavailability_p99_ns: f64,
    pub valid_update_unavailability_p99_ns: f64,
    pub failed_rollout_p99_ns: f64,
    pub valid_rollout_p99_ns: f64,
    pub ready_32_total_rss_bytes: f64,
    pub peak_total_rss_bytes: f64,
}

impl KeyMetricRatios {
    fn from_values(values: [f64; 14]) -> Self {
        Self {
            shared_init_ns: values[0],
            warm_create_p99_ns: values[1],
            normal_p99_ns: values[2],
            normal_total_ns: values[3],
            pressure_admission_p99_ns: values[4],
            sibling_p99_ns: values[5],
            trap_recovery_p99_ns: values[6],
            cpu_recovery_p99_ns: values[7],
            failed_update_unavailability_p99_ns: values[8],
            valid_update_unavailability_p99_ns: values[9],
            failed_rollout_p99_ns: values[10],
            valid_rollout_p99_ns: values[11],
            ready_32_total_rss_bytes: values[12],
            peak_total_rss_bytes: values[13],
        }
    }

    pub fn values(self) -> [f64; 14] {
        [
            self.shared_init_ns,
            self.warm_create_p99_ns,
            self.normal_p99_ns,
            self.normal_total_ns,
            self.pressure_admission_p99_ns,
            self.sibling_p99_ns,
            self.trap_recovery_p99_ns,
            self.cpu_recovery_p99_ns,
            self.failed_update_unavailability_p99_ns,
            self.valid_update_unavailability_p99_ns,
            self.failed_rollout_p99_ns,
            self.valid_rollout_p99_ns,
            self.ready_32_total_rss_bytes,
            self.peak_total_rss_bytes,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedRun {
    pub block: u8,
    pub raw_trace_verified: bool,
    pub manifest_verified: bool,
    pub cardinality_verified: bool,
    pub source_review_passed: bool,
    pub mandatory_gates_passed: bool,
    pub absolute_slos_passed: bool,
    pub one_process_profile_passed: bool,
    pub metrics: KeyMetrics,
}

impl RetainedRun {
    fn eligible(&self, expected_block: u8) -> bool {
        self.block == expected_block
            && self.raw_trace_verified
            && self.manifest_verified
            && self.cardinality_verified
            && self.source_review_passed
            && self.mandatory_gates_passed
            && self.absolute_slos_passed
            && self.one_process_profile_passed
            && self.metrics.all_positive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateEvidence {
    pub candidate: CandidateKind,
    pub retained_runs: Vec<RetainedRun>,
    pub candidate_specific_sloc: u64,
    pub time_to_first_full_pass_minutes: u64,
    pub extra_long_lived_process_or_service: bool,
    pub external_database_required: bool,
    pub implementation_complete: bool,
}

impl CandidateEvidence {
    pub fn eligible(&self) -> bool {
        self.implementation_complete
            && self.candidate_specific_sloc > 0
            && self.retained_runs.len() == 9
            && self
                .retained_runs
                .iter()
                .zip(1_u8..=9)
                .all(|(run, expected_block)| run.eligible(expected_block))
    }

    fn no_extra_process_or_database(&self) -> bool {
        !self.extra_long_lived_process_or_service && !self.external_database_required
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlternativeFailureReview {
    pub independently_reviewed: bool,
    pub at_least_one_failure_reproduced: bool,
    pub every_failure_reproducibly_maps_to_mandatory_contract_or_slo: bool,
    pub incomplete_or_timeboxed: bool,
    pub adapter_bug: bool,
    pub environment_invalid: bool,
    pub oracle_invalid: bool,
}

impl AlternativeFailureReview {
    fn supports_tested_set_gap(self, candidate: &CandidateEvidence) -> bool {
        candidate.implementation_complete
            && self.independently_reviewed
            && self.at_least_one_failure_reproduced
            && self.every_failure_reproducibly_maps_to_mandatory_contract_or_slo
            && !self.incomplete_or_timeboxed
            && !self.adapter_bug
            && !self.environment_invalid
            && !self.oracle_invalid
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerdictInput {
    pub lunatic: CandidateEvidence,
    pub extism: CandidateEvidence,
    pub direct_wasmtime: CandidateEvidence,
    pub extism_failure_review: AlternativeFailureReview,
    pub direct_wasmtime_failure_review: AlternativeFailureReview,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackagingComparison {
    pub reference_sloc: u64,
    pub sloc_delta: u64,
    pub reference_time_to_first_full_pass_minutes: u64,
    pub lunatic_to_worst_eligible_alternative_median_ratios: KeyMetricRatios,
    pub performance_within_two_of_every_eligible_alternative: bool,
    pub strength: PackagingStrength,
    pub dominating_alternatives: Vec<CandidateKind>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerdictOutput {
    pub verdict: FinalVerdict,
    pub lunatic_eligible: bool,
    pub extism_eligible: bool,
    pub direct_wasmtime_eligible: bool,
    pub packaging: Option<PackagingComparison>,
}

pub fn decide_verdict(input: &VerdictInput) -> Result<VerdictOutput, AnalysisError> {
    if input.lunatic.candidate != CandidateKind::Lunatic
        || input.extism.candidate != CandidateKind::Extism
        || input.direct_wasmtime.candidate != CandidateKind::RawWasmtime
    {
        return Err(AnalysisError::Inconsistent(
            "candidate evidence is assigned to the wrong role".into(),
        ));
    }
    let lunatic_eligible = input.lunatic.eligible();
    let extism_eligible = input.extism.eligible();
    let direct_wasmtime_eligible = input.direct_wasmtime.eligible();

    if !lunatic_eligible {
        return Ok(VerdictOutput {
            verdict: FinalVerdict::PositioningRejected,
            lunatic_eligible,
            extism_eligible,
            direct_wasmtime_eligible,
            packaging: None,
        });
    }

    let eligible_alternatives: Vec<_> = [&input.extism, &input.direct_wasmtime]
        .iter()
        .copied()
        .filter(|candidate| candidate.eligible())
        .collect();
    if !eligible_alternatives.is_empty() {
        let packaging = packaging_comparison(&input.lunatic, &eligible_alternatives)?;
        let alternative_dominates = !packaging.dominating_alternatives.is_empty();
        return Ok(VerdictOutput {
            verdict: if alternative_dominates {
                FinalVerdict::AlternativeDominates
            } else if matches!(
                packaging.strength,
                PackagingStrength::Strong | PackagingStrength::Moderate
            ) {
                FinalVerdict::LunaticPackagingValueSupported
            } else {
                FinalVerdict::LunaticPackagingValueUnsupported
            },
            lunatic_eligible,
            extism_eligible,
            direct_wasmtime_eligible,
            packaging: Some(packaging),
        });
    }

    let verdict = if input
        .extism_failure_review
        .supports_tested_set_gap(&input.extism)
        && input
            .direct_wasmtime_failure_review
            .supports_tested_set_gap(&input.direct_wasmtime)
    {
        FinalVerdict::TestedSetPackagingGap
    } else {
        FinalVerdict::Inconclusive
    };
    Ok(VerdictOutput {
        verdict,
        lunatic_eligible,
        extism_eligible,
        direct_wasmtime_eligible,
        packaging: None,
    })
}

fn packaging_comparison(
    lunatic: &CandidateEvidence,
    alternatives: &[&CandidateEvidence],
) -> Result<PackagingComparison, AnalysisError> {
    let reference_sloc = alternatives
        .iter()
        .map(|candidate| candidate.candidate_specific_sloc)
        .min()
        .expect("eligible alternatives is nonempty");
    let reference_time = alternatives
        .iter()
        .map(|candidate| candidate.time_to_first_full_pass_minutes)
        .min()
        .expect("eligible alternatives is nonempty");

    let mut worst_lunatic_ratio = None::<MetricFractions>;
    let mut performance_within_two = true;
    let mut dominating_alternatives = Vec::new();
    for alternative in alternatives {
        let lunatic_ratio = paired_metric_medians(lunatic, alternative)?;
        worst_lunatic_ratio = Some(match worst_lunatic_ratio {
            Some(current) => current.componentwise_max(lunatic_ratio),
            None => lunatic_ratio,
        });
        performance_within_two &= lunatic_ratio.all_at_most(2, 1);

        let alternative_ratio = paired_metric_medians(alternative, lunatic)?;
        let weakly_better_everywhere = alternative_ratio.all_at_most(1, 1);
        let strictly_better_somewhere = alternative.candidate_specific_sloc
            < lunatic.candidate_specific_sloc
            || alternative_ratio.any_less_than(1, 1);
        if alternative.candidate_specific_sloc <= lunatic.candidate_specific_sloc
            && weakly_better_everywhere
            && strictly_better_somewhere
            && alternative.no_extra_process_or_database()
        {
            dominating_alternatives.push(alternative.candidate);
        }
    }

    let sloc_delta = reference_sloc.saturating_sub(lunatic.candidate_specific_sloc);
    let strong = lunatic.candidate_specific_sloc
        <= ((u128::from(reference_sloc) * 70) / 100) as u64
        && sloc_delta >= 300
        && performance_within_two
        && lunatic.no_extra_process_or_database();
    let moderate = !strong
        && lunatic.candidate_specific_sloc <= ((u128::from(reference_sloc) * 85) / 100) as u64
        && sloc_delta >= 150
        && performance_within_two
        && lunatic.no_extra_process_or_database();
    let strength = if strong {
        PackagingStrength::Strong
    } else if moderate {
        PackagingStrength::Moderate
    } else {
        PackagingStrength::Unsupported
    };

    Ok(PackagingComparison {
        reference_sloc,
        sloc_delta,
        reference_time_to_first_full_pass_minutes: reference_time,
        lunatic_to_worst_eligible_alternative_median_ratios: worst_lunatic_ratio
            .expect("eligible alternatives is nonempty")
            .to_ratios(),
        performance_within_two_of_every_eligible_alternative: performance_within_two,
        strength,
        dominating_alternatives,
    })
}

#[derive(Debug, Clone, Copy)]
struct Fraction {
    numerator: u64,
    denominator: u64,
}

impl Fraction {
    fn compare(self, other: Self) -> std::cmp::Ordering {
        (u128::from(self.numerator) * u128::from(other.denominator))
            .cmp(&(u128::from(other.numerator) * u128::from(self.denominator)))
    }

    fn at_most(self, numerator: u64, denominator: u64) -> bool {
        u128::from(self.numerator) * u128::from(denominator)
            <= u128::from(numerator) * u128::from(self.denominator)
    }

    fn less_than(self, numerator: u64, denominator: u64) -> bool {
        u128::from(self.numerator) * u128::from(denominator)
            < u128::from(numerator) * u128::from(self.denominator)
    }

    fn to_f64(self) -> f64 {
        self.numerator as f64 / self.denominator as f64
    }
}

#[derive(Debug, Clone, Copy)]
struct MetricFractions([Fraction; 14]);

impl MetricFractions {
    fn all_at_most(self, numerator: u64, denominator: u64) -> bool {
        self.0
            .iter()
            .all(|ratio| ratio.at_most(numerator, denominator))
    }

    fn any_less_than(self, numerator: u64, denominator: u64) -> bool {
        self.0
            .iter()
            .any(|ratio| ratio.less_than(numerator, denominator))
    }

    fn componentwise_max(self, other: Self) -> Self {
        let mut result = self.0;
        for (index, value) in result.iter_mut().enumerate() {
            if value.compare(other.0[index]).is_lt() {
                *value = other.0[index];
            }
        }
        Self(result)
    }

    fn to_ratios(self) -> KeyMetricRatios {
        let mut values = [0.0; 14];
        for (index, ratio) in self.0.iter().enumerate() {
            values[index] = ratio.to_f64();
        }
        KeyMetricRatios::from_values(values)
    }
}

/// Computes numerator/denominator median-of-nine ratios after joining by block.
///
/// Reordered vectors are accepted here so audit tests can prove positional
/// pairing is not used. Canonical eligibility separately requires order 1..=9.
pub fn median_metric_ratios_by_block(
    numerator: &CandidateEvidence,
    denominator: &CandidateEvidence,
) -> Result<KeyMetricRatios, AnalysisError> {
    Ok(paired_metric_medians(numerator, denominator)?.to_ratios())
}

fn paired_metric_medians(
    numerator: &CandidateEvidence,
    denominator: &CandidateEvidence,
) -> Result<MetricFractions, AnalysisError> {
    let numerator_by_block = index_runs_by_block(&numerator.retained_runs)?;
    let denominator_by_block = index_runs_by_block(&denominator.retained_runs)?;
    let mut samples: [Vec<Fraction>; 14] = std::array::from_fn(|_| Vec::with_capacity(9));

    for block in 1_u8..=9 {
        let numerator_values = numerator_by_block[usize::from(block - 1)].metrics.values();
        let denominator_values = denominator_by_block[usize::from(block - 1)]
            .metrics
            .values();
        for index in 0..14 {
            if numerator_values[index] == 0 || denominator_values[index] == 0 {
                return Err(AnalysisError::Inconsistent(format!(
                    "metric {:?} is zero in retained block {block}",
                    KeyMetrics::NAMES[index]
                )));
            }
            samples[index].push(Fraction {
                numerator: numerator_values[index],
                denominator: denominator_values[index],
            });
        }
    }

    let mut medians = [Fraction {
        numerator: 1,
        denominator: 1,
    }; 14];
    for (index, values) in samples.iter_mut().enumerate() {
        values.sort_by(|left, right| (*left).compare(*right));
        medians[index] = values[4];
    }
    Ok(MetricFractions(medians))
}

fn index_runs_by_block(runs: &[RetainedRun]) -> Result<[&RetainedRun; 9], AnalysisError> {
    if runs.len() != 9 {
        return Err(AnalysisError::Inconsistent(format!(
            "expected retained blocks 1..=9, received {} runs",
            runs.len()
        )));
    }
    let mut indexed: [Option<&RetainedRun>; 9] = [None; 9];
    for run in runs {
        if !(1..=9).contains(&run.block) {
            return Err(AnalysisError::Inconsistent(format!(
                "retained block {} is outside 1..=9",
                run.block
            )));
        }
        let slot = &mut indexed[usize::from(run.block - 1)];
        if slot.replace(run).is_some() {
            return Err(AnalysisError::Inconsistent(format!(
                "retained block {} appears more than once",
                run.block
            )));
        }
    }
    Ok(indexed.map(|run| run.expect("nine unique blocks in 1..=9 fill every slot")))
}
