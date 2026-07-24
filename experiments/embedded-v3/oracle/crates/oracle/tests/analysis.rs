use embedded_v3_oracle::analysis::*;
use embedded_v3_oracle::protocol::{CandidateKind, TenantId};

#[test]
fn rollout_downtime_counts_the_slow_gap_and_mixed_span() {
    let input = RolloutAnalysisInput {
        targets: vec![TenantId(0), TenantId(1)],
        rollout_write_ns: 100,
        proof_ns: 300,
        baseline_version: "A".into(),
        good_completions: vec![
            GoodCompletion {
                tenant_id: TenantId(0),
                monotonic_ns: 90,
                logical_version: "A".into(),
                build_id: "a".into(),
            },
            GoodCompletion {
                tenant_id: TenantId(1),
                monotonic_ns: 95,
                logical_version: "A".into(),
                build_id: "a".into(),
            },
            GoodCompletion {
                tenant_id: TenantId(0),
                monotonic_ns: 150,
                logical_version: "B".into(),
                build_id: "b".into(),
            },
            GoodCompletion {
                tenant_id: TenantId(1),
                monotonic_ns: 230,
                logical_version: "B".into(),
                build_id: "b".into(),
            },
            GoodCompletion {
                tenant_id: TenantId(0),
                monotonic_ns: 280,
                logical_version: "B".into(),
                build_id: "b".into(),
            },
            GoodCompletion {
                tenant_id: TenantId(1),
                monotonic_ns: 290,
                logical_version: "B".into(),
                build_id: "b".into(),
            },
        ],
    };
    let analysis = analyze_rollout(&input).unwrap();
    assert_eq!(analysis.duration_ns, 200);
    assert_eq!(analysis.per_tenant_downtime[0].max_gap_ns, 130);
    assert_eq!(analysis.per_tenant_downtime[1].max_gap_ns, 135);
    assert_eq!(analysis.downtime_p99_ns, 135);
    assert_eq!(analysis.mixed_span_ns, 80);
    assert!(!analysis.mixed_at_proof);
}

#[test]
fn rss_uses_median3_signed_excess_and_unclamped_slope() {
    let cleanup = (1..=30)
        .map(|cycle| CleanupRssCycle {
            cycle,
            active_tenants_pre_teardown: 32,
            pre_teardown_samples: [200, 202, 201],
            post_teardown_samples: [
                90 + u64::from(cycle),
                91 + u64::from(cycle),
                92 + u64::from(cycle),
            ],
        })
        .collect();
    let result = analyze_rss(&RssAnalysisInput {
        baseline_samples: [100, 101, 99],
        initialized_samples: [110, 111, 109],
        ready_samples: [180, 181, 179],
        create_to_ready_periodic_samples: vec![100, 190, 185],
        retained_cleanup: cleanup,
    })
    .unwrap();
    assert_eq!(result.baseline_bytes, 100);
    assert_eq!(result.delta_32_bytes, 90);
    assert_eq!(result.cleanup[0].excess_bytes, -8);
    assert!((result.cleanup_slope_bytes_per_cycle - 1.0).abs() < 1e-12);
}

fn metrics(value: u64) -> KeyMetrics {
    KeyMetrics {
        shared_init_ns: value,
        warm_create_p99_ns: value,
        normal_p99_ns: value,
        normal_total_ns: value,
        pressure_admission_p99_ns: value,
        sibling_p99_ns: value,
        trap_recovery_p99_ns: value,
        cpu_recovery_p99_ns: value,
        failed_update_unavailability_p99_ns: value,
        valid_update_unavailability_p99_ns: value,
        failed_rollout_p99_ns: value,
        valid_rollout_p99_ns: value,
        ready_32_total_rss_bytes: value,
        peak_total_rss_bytes: value,
    }
}

fn retained_run(block: u8, passes: bool, metric: u64) -> RetainedRun {
    RetainedRun {
        block,
        raw_trace_verified: passes,
        manifest_verified: passes,
        cardinality_verified: passes,
        source_review_passed: passes,
        mandatory_gates_passed: passes,
        absolute_slos_passed: passes,
        one_process_profile_passed: passes,
        metrics: metrics(metric),
    }
}

fn evidence(candidate: CandidateKind, eligible: bool, sloc: u64, metric: u64) -> CandidateEvidence {
    CandidateEvidence {
        candidate,
        retained_runs: (1..=9)
            .map(|block| retained_run(block, eligible, metric))
            .collect(),
        candidate_specific_sloc: sloc,
        time_to_first_full_pass_minutes: 60,
        extra_long_lived_process_or_service: false,
        external_database_required: false,
        implementation_complete: true,
    }
}

fn failure_review(conclusive: bool) -> AlternativeFailureReview {
    AlternativeFailureReview {
        independently_reviewed: conclusive,
        at_least_one_failure_reproduced: conclusive,
        every_failure_reproducibly_maps_to_mandatory_contract_or_slo: conclusive,
        incomplete_or_timeboxed: false,
        adapter_bug: false,
        environment_invalid: false,
        oracle_invalid: false,
    }
}

fn verdict_input(
    lunatic: CandidateEvidence,
    extism: CandidateEvidence,
    direct_wasmtime: CandidateEvidence,
) -> VerdictInput {
    VerdictInput {
        lunatic,
        extism,
        direct_wasmtime,
        extism_failure_review: failure_review(false),
        direct_wasmtime_failure_review: failure_review(false),
    }
}

#[test]
fn key_metric_registry_rejects_missing_and_extra_fields() {
    let mut missing = serde_json::to_value(metrics(1)).unwrap();
    missing.as_object_mut().unwrap().remove("shared_init_ns");
    assert!(serde_json::from_value::<KeyMetrics>(missing).is_err());

    let mut extra = serde_json::to_value(metrics(1)).unwrap();
    extra
        .as_object_mut()
        .unwrap()
        .insert("invented_metric".into(), serde_json::Value::from(1));
    assert!(serde_json::from_value::<KeyMetrics>(extra).is_err());
}

#[test]
fn every_zero_key_metric_makes_candidate_ineligible() {
    for name in KeyMetrics::NAMES {
        let mut candidate = evidence(CandidateKind::Lunatic, true, 600, 1);
        let mut encoded = serde_json::to_value(candidate.retained_runs[0].metrics).unwrap();
        encoded
            .as_object_mut()
            .unwrap()
            .insert(name.into(), serde_json::Value::from(0));
        candidate.retained_runs[0].metrics = serde_json::from_value(encoded).unwrap();
        assert!(!candidate.eligible(), "{} accepted zero", name);
    }
}

#[test]
fn eligibility_requires_positive_sloc() {
    let candidate = evidence(CandidateKind::Lunatic, true, 0, 1);
    assert!(!candidate.eligible());
}

#[test]
fn eligibility_requires_exact_ordered_blocks_one_through_nine_once() {
    let mut candidate = evidence(CandidateKind::Extism, true, 1_000, 1);
    assert!(candidate.eligible());

    candidate.retained_runs.reverse();
    assert!(!candidate.eligible());

    candidate = evidence(CandidateKind::Extism, true, 1_000, 1);
    candidate.retained_runs[8].block = 8;
    assert!(!candidate.eligible());

    candidate = evidence(CandidateKind::Extism, true, 1_000, 1);
    candidate.retained_runs.pop();
    assert!(!candidate.eligible());

    candidate = evidence(CandidateKind::Extism, true, 1_000, 1);
    candidate.retained_runs.push(retained_run(9, true, 1));
    assert!(!candidate.eligible());
}

#[test]
fn eligibility_checks_every_evidence_gate() {
    for gate in 0..7 {
        let mut candidate = evidence(CandidateKind::Extism, true, 1_000, 1);
        let run = &mut candidate.retained_runs[0];
        match gate {
            0 => run.raw_trace_verified = false,
            1 => run.manifest_verified = false,
            2 => run.cardinality_verified = false,
            3 => run.source_review_passed = false,
            4 => run.mandatory_gates_passed = false,
            5 => run.absolute_slos_passed = false,
            6 => run.one_process_profile_passed = false,
            _ => unreachable!(),
        }
        assert!(!candidate.eligible(), "gate {} was ignored", gate);
    }

    let mut incomplete = evidence(CandidateKind::Extism, true, 1_000, 1);
    incomplete.implementation_complete = false;
    assert!(!incomplete.eligible());
}

#[test]
fn metric_ratios_pair_by_block_not_vector_position() {
    let mut numerator = evidence(CandidateKind::Lunatic, true, 600, 100);
    for run in &mut numerator.retained_runs {
        run.metrics.shared_init_ns = u64::from(run.block) * 10;
    }
    let mut denominator = numerator.clone();
    denominator.candidate = CandidateKind::Extism;
    denominator.retained_runs.reverse();

    assert!(!denominator.eligible());
    let ratios = median_metric_ratios_by_block(&numerator, &denominator).unwrap();
    assert_eq!(ratios.values(), [1.0; 14]);
}

#[test]
fn block_pairing_rejects_duplicate_and_out_of_range_blocks() {
    let numerator = evidence(CandidateKind::Lunatic, true, 600, 100);
    let mut denominator = evidence(CandidateKind::Extism, true, 1_000, 100);
    denominator.retained_runs[8].block = 8;
    assert!(median_metric_ratios_by_block(&numerator, &denominator).is_err());

    denominator = evidence(CandidateKind::Extism, true, 1_000, 100);
    denominator.retained_runs[8].block = 10;
    assert!(median_metric_ratios_by_block(&numerator, &denominator).is_err());
}

#[test]
fn paired_median_uses_the_fifth_exact_ratio() {
    let mut numerator = evidence(CandidateKind::Lunatic, true, 600, 100);
    let denominator = evidence(CandidateKind::Extism, true, 1_000, 100);
    let values = [1, 2, 3, 4, 150, 200, 300, 400, 500];
    for (run, value) in numerator.retained_runs.iter_mut().zip(values) {
        run.metrics.shared_init_ns = value;
    }
    let ratios = median_metric_ratios_by_block(&numerator, &denominator).unwrap();
    assert_eq!(ratios.shared_init_ns, 1.5);
}

#[test]
fn positioning_rejection_has_first_precedence() {
    let mut input = verdict_input(
        evidence(CandidateKind::Lunatic, false, 600, 100),
        evidence(CandidateKind::Extism, true, 500, 50),
        evidence(CandidateKind::RawWasmtime, true, 500, 50),
    );
    input.extism_failure_review = failure_review(true);
    input.direct_wasmtime_failure_review = failure_review(true);
    let rejected = decide_verdict(&input).unwrap();
    assert_eq!(rejected.verdict, FinalVerdict::PositioningRejected);
    assert!(rejected.packaging.is_none());
}

#[test]
fn eligible_strictly_better_alternative_dominates() {
    let output = decide_verdict(&verdict_input(
        evidence(CandidateKind::Lunatic, true, 600, 100),
        evidence(CandidateKind::Extism, true, 600, 90),
        evidence(CandidateKind::RawWasmtime, true, 1_200, 100),
    ))
    .unwrap();
    assert_eq!(output.verdict, FinalVerdict::AlternativeDominates);
    assert_eq!(
        output.packaging.unwrap().dominating_alternatives,
        vec![CandidateKind::Extism]
    );
}

#[test]
fn lower_sloc_alternative_with_equal_metrics_dominates() {
    let output = decide_verdict(&verdict_input(
        evidence(CandidateKind::Lunatic, true, 600, 100),
        evidence(CandidateKind::Extism, true, 599, 100),
        evidence(CandidateKind::RawWasmtime, true, 1_200, 100),
    ))
    .unwrap();
    assert_eq!(output.verdict, FinalVerdict::AlternativeDominates);
}

#[test]
fn equal_candidate_is_not_a_dominator_without_a_strict_dimension() {
    let output = decide_verdict(&verdict_input(
        evidence(CandidateKind::Lunatic, true, 600, 100),
        evidence(CandidateKind::Extism, true, 600, 100),
        evidence(CandidateKind::RawWasmtime, true, 1_200, 100),
    ))
    .unwrap();
    assert_ne!(output.verdict, FinalVerdict::AlternativeDominates);
}

#[test]
fn one_worse_metric_prevents_alternative_dominance() {
    let lunatic = evidence(CandidateKind::Lunatic, true, 600, 100);
    let mut extism = evidence(CandidateKind::Extism, true, 500, 90);
    for run in extism.retained_runs.iter_mut().take(5) {
        run.metrics.normal_p99_ns = 101;
    }
    let output = decide_verdict(&verdict_input(
        lunatic,
        extism,
        evidence(CandidateKind::RawWasmtime, true, 1_200, 100),
    ))
    .unwrap();
    assert_ne!(output.verdict, FinalVerdict::AlternativeDominates);
}

#[test]
fn alternative_with_extra_process_or_database_cannot_dominate() {
    for database in [false, true] {
        let mut alternative = evidence(CandidateKind::Extism, true, 500, 90);
        alternative.extra_long_lived_process_or_service = !database;
        alternative.external_database_required = database;
        let output = decide_verdict(&verdict_input(
            evidence(CandidateKind::Lunatic, true, 600, 100),
            alternative,
            evidence(CandidateKind::RawWasmtime, true, 1_200, 100),
        ))
        .unwrap();
        assert_ne!(output.verdict, FinalVerdict::AlternativeDominates);
    }
}

#[test]
fn strong_packaging_path_is_supported() {
    let supported = decide_verdict(&verdict_input(
        evidence(CandidateKind::Lunatic, true, 600, 150),
        evidence(CandidateKind::Extism, true, 1_000, 100),
        evidence(CandidateKind::RawWasmtime, true, 1_200, 100),
    ))
    .unwrap();
    assert_eq!(
        supported.verdict,
        FinalVerdict::LunaticPackagingValueSupported
    );
    assert_eq!(
        supported.packaging.unwrap().strength,
        PackagingStrength::Strong
    );
}

#[test]
fn moderate_packaging_path_is_supported() {
    let supported = decide_verdict(&verdict_input(
        evidence(CandidateKind::Lunatic, true, 800, 150),
        evidence(CandidateKind::Extism, true, 1_000, 100),
        evidence(CandidateKind::RawWasmtime, true, 1_200, 100),
    ))
    .unwrap();
    assert_eq!(
        supported.verdict,
        FinalVerdict::LunaticPackagingValueSupported
    );
    assert_eq!(
        supported.packaging.unwrap().strength,
        PackagingStrength::Moderate
    );
}

#[test]
fn no_packaging_tier_is_unsupported() {
    let unsupported = decide_verdict(&verdict_input(
        evidence(CandidateKind::Lunatic, true, 900, 100),
        evidence(CandidateKind::Extism, true, 1_000, 100),
        evidence(CandidateKind::RawWasmtime, true, 1_200, 100),
    ))
    .unwrap();
    assert_eq!(
        unsupported.verdict,
        FinalVerdict::LunaticPackagingValueUnsupported
    );
    assert_eq!(
        unsupported.packaging.unwrap().strength,
        PackagingStrength::Unsupported
    );
}

#[test]
fn conclusive_failures_of_both_complete_alternatives_form_tested_set_gap() {
    let mut input = verdict_input(
        evidence(CandidateKind::Lunatic, true, 600, 100),
        evidence(CandidateKind::Extism, false, 1_000, 100),
        evidence(CandidateKind::RawWasmtime, false, 1_200, 100),
    );
    input.extism_failure_review = failure_review(true);
    input.direct_wasmtime_failure_review = failure_review(true);
    assert_eq!(
        decide_verdict(&input).unwrap().verdict,
        FinalVerdict::TestedSetPackagingGap
    );
}

#[test]
fn every_noncanonical_failure_attribution_forces_inconclusive() {
    for rejected_field in 0..7 {
        let mut input = verdict_input(
            evidence(CandidateKind::Lunatic, true, 600, 100),
            evidence(CandidateKind::Extism, false, 1_000, 100),
            evidence(CandidateKind::RawWasmtime, false, 1_200, 100),
        );
        input.extism_failure_review = failure_review(true);
        input.direct_wasmtime_failure_review = failure_review(true);
        match rejected_field {
            0 => input.extism_failure_review.independently_reviewed = false,
            1 => input.extism_failure_review.at_least_one_failure_reproduced = false,
            2 => {
                input
                    .extism_failure_review
                    .every_failure_reproducibly_maps_to_mandatory_contract_or_slo = false
            }
            3 => input.extism_failure_review.incomplete_or_timeboxed = true,
            4 => input.extism_failure_review.adapter_bug = true,
            5 => input.extism_failure_review.environment_invalid = true,
            6 => input.extism_failure_review.oracle_invalid = true,
            _ => unreachable!(),
        }
        assert_eq!(
            decide_verdict(&input).unwrap().verdict,
            FinalVerdict::Inconclusive,
            "failure-review condition {rejected_field} was ignored"
        );
    }

    let mut input = verdict_input(
        evidence(CandidateKind::Lunatic, true, 600, 100),
        evidence(CandidateKind::Extism, false, 1_000, 100),
        evidence(CandidateKind::RawWasmtime, false, 1_200, 100),
    );
    input.extism.implementation_complete = false;
    input.extism_failure_review = failure_review(true);
    input.direct_wasmtime_failure_review = failure_review(true);
    assert_eq!(
        decide_verdict(&input).unwrap().verdict,
        FinalVerdict::Inconclusive
    );

    let mut input = verdict_input(
        evidence(CandidateKind::Lunatic, true, 600, 100),
        evidence(CandidateKind::Extism, false, 1_000, 100),
        evidence(CandidateKind::RawWasmtime, false, 1_200, 100),
    );
    input.extism_failure_review = failure_review(true);
    input.direct_wasmtime_failure_review = failure_review(false);
    assert_eq!(
        decide_verdict(&input).unwrap().verdict,
        FinalVerdict::Inconclusive
    );
}

#[test]
fn candidate_roles_are_not_interchangeable() {
    let error = decide_verdict(&verdict_input(
        evidence(CandidateKind::Extism, true, 600, 100),
        evidence(CandidateKind::Lunatic, true, 1_000, 100),
        evidence(CandidateKind::RawWasmtime, true, 1_200, 100),
    ))
    .unwrap_err();
    assert!(matches!(error, AnalysisError::Inconsistent(_)));
}

fn packaging_strength(
    lunatic_sloc: u64,
    reference_sloc: u64,
    lunatic_metric: u64,
    alternative_metric: u64,
) -> PackagingStrength {
    decide_verdict(&verdict_input(
        evidence(CandidateKind::Lunatic, true, lunatic_sloc, lunatic_metric),
        evidence(
            CandidateKind::Extism,
            true,
            reference_sloc,
            alternative_metric,
        ),
        evidence(
            CandidateKind::RawWasmtime,
            true,
            reference_sloc + 200,
            alternative_metric,
        ),
    ))
    .unwrap()
    .packaging
    .unwrap()
    .strength
}

#[test]
fn strong_percentage_threshold_is_inclusive_and_exact() {
    assert_eq!(
        packaging_strength(700, 1_000, 100, 100),
        PackagingStrength::Strong
    );
    assert_eq!(
        packaging_strength(701, 1_000, 100, 100),
        PackagingStrength::Moderate
    );
}

#[test]
fn strong_delta_threshold_is_inclusive_and_exact() {
    assert_eq!(
        packaging_strength(200, 500, 100, 100),
        PackagingStrength::Strong
    );
    assert_eq!(
        packaging_strength(201, 500, 100, 100),
        PackagingStrength::Moderate
    );
}

#[test]
fn moderate_percentage_threshold_is_inclusive_and_exact() {
    assert_eq!(
        packaging_strength(850, 1_000, 100, 100),
        PackagingStrength::Moderate
    );
    assert_eq!(
        packaging_strength(851, 1_000, 100, 100),
        PackagingStrength::Unsupported
    );
}

#[test]
fn moderate_delta_threshold_is_inclusive_and_exact() {
    assert_eq!(
        packaging_strength(350, 500, 100, 100),
        PackagingStrength::Moderate
    );
    assert_eq!(
        packaging_strength(351, 500, 100, 100),
        PackagingStrength::Unsupported
    );
}

#[test]
fn performance_ratio_two_is_inclusive_and_exact() {
    assert_eq!(
        packaging_strength(600, 1_000, 200, 100),
        PackagingStrength::Strong
    );
    assert_eq!(
        packaging_strength(600, 1_000, 201, 100),
        PackagingStrength::Unsupported
    );
}

#[test]
fn performance_gate_checks_every_eligible_alternative() {
    let exact = decide_verdict(&verdict_input(
        evidence(CandidateKind::Lunatic, true, 600, 80),
        evidence(CandidateKind::Extism, true, 1_000, 100),
        evidence(CandidateKind::RawWasmtime, true, 1_200, 40),
    ))
    .unwrap();
    assert_eq!(exact.packaging.unwrap().strength, PackagingStrength::Strong);

    let over = decide_verdict(&verdict_input(
        evidence(CandidateKind::Lunatic, true, 600, 80),
        evidence(CandidateKind::Extism, true, 1_000, 100),
        evidence(CandidateKind::RawWasmtime, true, 1_200, 39),
    ))
    .unwrap();
    let packaging = over.packaging.unwrap();
    assert!(!packaging.performance_within_two_of_every_eligible_alternative);
    assert_eq!(packaging.strength, PackagingStrength::Unsupported);
}

#[test]
fn reference_sloc_is_minimum_of_eligible_alternatives() {
    let output = decide_verdict(&verdict_input(
        evidence(CandidateKind::Lunatic, true, 600, 100),
        evidence(CandidateKind::Extism, true, 1_100, 100),
        evidence(CandidateKind::RawWasmtime, true, 1_000, 100),
    ))
    .unwrap();
    let packaging = output.packaging.unwrap();
    assert_eq!(packaging.reference_sloc, 1_000);
    assert_eq!(packaging.sloc_delta, 400);
}

#[test]
fn lunatic_extra_infrastructure_blocks_both_packaging_tiers() {
    for database in [false, true] {
        let mut lunatic = evidence(CandidateKind::Lunatic, true, 600, 100);
        lunatic.extra_long_lived_process_or_service = !database;
        lunatic.external_database_required = database;
        let output = decide_verdict(&verdict_input(
            lunatic,
            evidence(CandidateKind::Extism, true, 1_000, 100),
            evidence(CandidateKind::RawWasmtime, true, 1_200, 100),
        ))
        .unwrap();
        assert_eq!(
            output.packaging.unwrap().strength,
            PackagingStrength::Unsupported
        );
    }
}

#[test]
fn worst_reported_ratio_is_componentwise_across_all_alternatives() {
    let mut lunatic = evidence(CandidateKind::Lunatic, true, 600, 100);
    let mut extism = evidence(CandidateKind::Extism, true, 1_000, 100);
    let mut direct = evidence(CandidateKind::RawWasmtime, true, 1_200, 100);
    for run in lunatic.retained_runs.iter_mut().take(5) {
        run.metrics.shared_init_ns = 200;
        run.metrics.normal_p99_ns = 200;
    }
    for run in extism.retained_runs.iter_mut().take(5) {
        run.metrics.normal_p99_ns = 200;
    }
    for run in direct.retained_runs.iter_mut().take(5) {
        run.metrics.shared_init_ns = 50;
    }

    let output = decide_verdict(&verdict_input(lunatic, extism, direct)).unwrap();
    let ratios = output
        .packaging
        .unwrap()
        .lunatic_to_worst_eligible_alternative_median_ratios;
    assert_eq!(ratios.shared_init_ns, 4.0);
    assert_eq!(ratios.normal_p99_ns, 2.0);
}
