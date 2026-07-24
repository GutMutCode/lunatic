use std::convert::TryFrom;

use crate::evidence::{
    frozen_cleanup_slope, frozen_rss_pass, MetricEvidence, NormalMetricEvidence, RssEvidence,
};
use crate::sampling::ProcessTreeSample;

pub const SAMPLE_MATCH_TOLERANCE_NS: u64 = 20_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableRssAnchor {
    pub target_monotonic_ns: [u64; 3],
}

pub fn positive_duration(start_ns: u64, finish_ns: u64) -> Result<u64, String> {
    let duration = finish_ns
        .checked_sub(start_ns)
        .ok_or_else(|| "duration endpoint preceded its start".to_owned())?;
    if duration == 0 {
        Err("duration must be positive".to_owned())
    } else {
        Ok(duration)
    }
}

pub fn nearest_rank_u64(
    values: &[u64],
    percentile_numerator: u64,
    percentile_denominator: u64,
) -> Option<u64> {
    if values.is_empty()
        || percentile_denominator == 0
        || percentile_numerator > percentile_denominator
    {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let count = sorted.len() as u64;
    let rank = if percentile_numerator == 0 {
        1
    } else {
        percentile_numerator
            .checked_mul(count)?
            .checked_add(percentile_denominator - 1)?
            / percentile_denominator
    };
    sorted.get(rank.saturating_sub(1) as usize).copied()
}

pub fn metric_evidence(
    samples: &[u64],
    slo_min_ns: Option<u64>,
    slo_max_ns: u64,
) -> Result<MetricEvidence, String> {
    if samples.is_empty() || samples.contains(&0) {
        return Err("metric samples must be nonempty and positive".to_owned());
    }
    let minimum_ns = *samples.iter().min().expect("nonempty");
    let maximum_ns = *samples.iter().max().expect("nonempty");
    let nearest_rank_p99_ns =
        nearest_rank_u64(samples, 99, 100).ok_or_else(|| "invalid p99 inputs".to_owned())?;
    let pass =
        nearest_rank_p99_ns <= slo_max_ns && slo_min_ns.is_none_or(|minimum| minimum_ns >= minimum);
    Ok(MetricEvidence {
        sample_count: samples.len() as u64,
        minimum_ns,
        nearest_rank_p99_ns,
        maximum_ns,
        slo_min_ns,
        slo_max_ns,
        pass,
    })
}

pub fn nonnegative_metric_evidence(
    samples: &[u64],
    slo_max_ns: u64,
) -> Result<MetricEvidence, String> {
    if samples.is_empty() {
        return Err("metric samples must be nonempty".to_owned());
    }
    let minimum_ns = *samples.iter().min().expect("nonempty");
    let maximum_ns = *samples.iter().max().expect("nonempty");
    let nearest_rank_p99_ns =
        nearest_rank_u64(samples, 99, 100).ok_or_else(|| "invalid p99 inputs".to_owned())?;
    Ok(MetricEvidence {
        sample_count: samples.len() as u64,
        minimum_ns,
        nearest_rank_p99_ns,
        maximum_ns,
        slo_min_ns: None,
        slo_max_ns,
        pass: maximum_ns <= slo_max_ns,
    })
}

pub fn maximum_metric_evidence(
    samples: &[u64],
    slo_min_ns: Option<u64>,
    slo_max_ns: u64,
) -> Result<MetricEvidence, String> {
    if samples.is_empty() || samples.contains(&0) {
        return Err("metric samples must be nonempty and positive".to_owned());
    }
    let minimum_ns = *samples.iter().min().expect("nonempty");
    let maximum_ns = *samples.iter().max().expect("nonempty");
    let nearest_rank_p99_ns =
        nearest_rank_u64(samples, 99, 100).ok_or_else(|| "invalid p99 inputs".to_owned())?;
    let pass = maximum_ns <= slo_max_ns && slo_min_ns.is_none_or(|minimum| minimum_ns >= minimum);
    Ok(MetricEvidence {
        sample_count: samples.len() as u64,
        minimum_ns,
        nearest_rank_p99_ns,
        maximum_ns,
        slo_min_ns,
        slo_max_ns,
        pass,
    })
}

pub fn normal_metric_evidence(
    samples: &[u64],
    total_ns: u64,
    slo_max_ns: u64,
) -> Result<NormalMetricEvidence, String> {
    let metric = metric_evidence(samples, None, slo_max_ns)?;
    if total_ns == 0 {
        return Err("normal total duration must be positive".to_owned());
    }
    Ok(NormalMetricEvidence {
        sample_count: metric.sample_count,
        minimum_ns: metric.minimum_ns,
        nearest_rank_p99_ns: metric.nearest_rank_p99_ns,
        maximum_ns: metric.maximum_ns,
        total_ns,
        slo_min_ns: None,
        slo_max_ns,
        pass: metric.pass,
    })
}

pub fn stable_anchor(start_ns: u64) -> Result<StableRssAnchor, String> {
    Ok(StableRssAnchor {
        target_monotonic_ns: [
            start_ns,
            start_ns
                .checked_add(100_000_000)
                .ok_or_else(|| "stable RSS target overflow".to_owned())?,
            start_ns
                .checked_add(200_000_000)
                .ok_or_else(|| "stable RSS target overflow".to_owned())?,
        ],
    })
}

fn select_nearest(
    samples: &[ProcessTreeSample],
    target_ns: u64,
) -> Result<&ProcessTreeSample, String> {
    let sample = samples
        .iter()
        .min_by_key(|sample| sample.monotonic_ns.abs_diff(target_ns))
        .ok_or_else(|| "RSS trace contains no samples".to_owned())?;
    let distance = sample.monotonic_ns.abs_diff(target_ns);
    if distance > SAMPLE_MATCH_TOLERANCE_NS {
        return Err(format!(
            "nearest RSS sample is {distance}ns from target {target_ns}"
        ));
    }
    if sample.process_count == 0
        || sample.sampled_process_count != sample.process_count
        || sample.inaccessible_process_count != 0
        || sample.working_set_bytes == 0
    {
        return Err("RSS sample does not cover the complete process tree".to_owned());
    }
    Ok(sample)
}

pub fn stable_median_bytes(
    samples: &[ProcessTreeSample],
    anchor: &StableRssAnchor,
) -> Result<u64, String> {
    let mut values = [
        select_nearest(samples, anchor.target_monotonic_ns[0])?.working_set_bytes,
        select_nearest(samples, anchor.target_monotonic_ns[1])?.working_set_bytes,
        select_nearest(samples, anchor.target_monotonic_ns[2])?.working_set_bytes,
    ];
    values.sort_unstable();
    Ok(values[1])
}

pub fn rss_evidence(
    samples: &[ProcessTreeSample],
    minimal: &StableRssAnchor,
    shared: &StableRssAnchor,
    ready: &StableRssAnchor,
    retained_cleanup: &[StableRssAnchor],
) -> Result<RssEvidence, String> {
    if retained_cleanup.len() != 30 {
        return Err("exactly 30 retained cleanup RSS anchors are required".to_owned());
    }
    if samples.iter().any(|sample| {
        sample.process_count == 0
            || sample.sampled_process_count != sample.process_count
            || sample.inaccessible_process_count != 0
            || sample.working_set_bytes == 0
    }) {
        return Err("RSS trace contains an incomplete process-tree sample".to_owned());
    }

    let minimal_bytes = stable_median_bytes(samples, minimal)?;
    let shared_bytes = stable_median_bytes(samples, shared)?;
    let ready_bytes = stable_median_bytes(samples, ready)?;
    let ready_delta = ready_bytes
        .checked_sub(minimal_bytes)
        .ok_or_else(|| "ready RSS is below minimal-host RSS".to_owned())?;
    if ready_delta == 0 {
        return Err("ready RSS delta must be positive".to_owned());
    }
    let phase_peak_bytes = samples
        .iter()
        .map(|sample| sample.working_set_bytes)
        .max()
        .ok_or_else(|| "RSS trace contains no samples".to_owned())?;
    let peak_delta = phase_peak_bytes
        .checked_sub(minimal_bytes)
        .ok_or_else(|| "peak RSS is below minimal-host RSS".to_owned())?;
    if peak_delta == 0 {
        return Err("peak RSS delta must be positive".to_owned());
    }

    let cleanup_delta_bytes = retained_cleanup
        .iter()
        .map(|anchor| {
            let value = stable_median_bytes(samples, anchor)?;
            i64::try_from(value)
                .and_then(|value| i64::try_from(minimal_bytes).map(|base| value - base))
                .map_err(|_| "RSS value cannot fit signed cleanup delta".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let maximum_post_cleanup_delta_bytes =
        *cleanup_delta_bytes.iter().max().expect("30 cleanup points");
    let cleanup_slope_bytes_per_cycle = frozen_cleanup_slope(&cleanup_delta_bytes)?;
    let pass = frozen_rss_pass(
        ready_bytes,
        peak_delta,
        maximum_post_cleanup_delta_bytes,
        cleanup_slope_bytes_per_cycle,
    );

    Ok(RssEvidence {
        units: "bytes".to_owned(),
        process_tree_complete: true,
        minimal_host_stable_median_bytes: minimal_bytes,
        shared_init_stable_median_bytes: shared_bytes,
        ready_32_stable_median_bytes: ready_bytes,
        ready_delta_bytes: ready_delta,
        phase_peak_bytes,
        peak_delta_bytes: peak_delta,
        cleanup_delta_bytes,
        maximum_post_cleanup_delta_bytes,
        cleanup_slope_bytes_per_cycle,
        pass,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(monotonic_ns: u64, working_set_bytes: u64) -> ProcessTreeSample {
        ProcessTreeSample {
            monotonic_ns,
            root_pid: 42,
            root_creation_time_100ns: 7,
            process_count: 1,
            sampled_process_count: 1,
            inaccessible_process_count: 0,
            thread_count: 4,
            working_set_bytes,
            private_usage_bytes: working_set_bytes,
        }
    }

    fn add_anchor_samples(
        samples: &mut Vec<ProcessTreeSample>,
        anchor: &StableRssAnchor,
        bytes: u64,
    ) {
        for target in anchor.target_monotonic_ns {
            samples.push(sample(target, bytes));
        }
    }

    #[test]
    fn nearest_rank_uses_frozen_non_interpolating_definition() {
        let values: Vec<_> = (1..=101).collect();
        assert_eq!(nearest_rank_u64(&values, 99, 100), Some(100));
        assert_eq!(nearest_rank_u64(&[4, 1, 3, 2], 50, 100), Some(2));
        assert_eq!(nearest_rank_u64(&[], 99, 100), None);
    }

    #[test]
    fn metrics_reject_zero_and_apply_bounds() {
        assert!(metric_evidence(&[0], None, 1).is_err());
        let zero = nonnegative_metric_evidence(&[0, 0], 2_000_000_000).unwrap();
        assert_eq!(zero.nearest_rank_p99_ns, 0);
        assert!(zero.pass);
        let metric = metric_evidence(&[40, 50, 60], Some(40), 100).unwrap();
        assert!(metric.pass);
        assert_eq!(metric.nearest_rank_p99_ns, 60);
        let too_slow = metric_evidence(&[40, 101], None, 100).unwrap();
        assert!(!too_slow.pass);

        let mut outlier = vec![50; 100];
        outlier.push(101);
        assert!(metric_evidence(&outlier, None, 100).unwrap().pass);
        assert!(!maximum_metric_evidence(&outlier, None, 100).unwrap().pass);
        assert!(!nonnegative_metric_evidence(&outlier, 100).unwrap().pass);
    }

    #[test]
    fn rss_uses_three_point_medians_and_frozen_cleanup_slope() {
        let minimal = stable_anchor(1_000_000_000).unwrap();
        let shared = stable_anchor(2_000_000_000).unwrap();
        let ready = stable_anchor(3_000_000_000).unwrap();
        let cleanup: Vec<_> = (0..30)
            .map(|index| stable_anchor(4_000_000_000 + index * 1_000_000_000).unwrap())
            .collect();
        let mut samples = Vec::new();
        add_anchor_samples(&mut samples, &minimal, 100_000_000);
        add_anchor_samples(&mut samples, &shared, 110_000_000);
        add_anchor_samples(&mut samples, &ready, 120_000_000);
        for (index, anchor) in cleanup.iter().enumerate() {
            add_anchor_samples(
                &mut samples,
                anchor,
                100_000_000 + (index as u64 + 1) * 1_024,
            );
        }
        let evidence = rss_evidence(&samples, &minimal, &shared, &ready, &cleanup).unwrap();
        assert_eq!(evidence.ready_delta_bytes, 20_000_000);
        assert!((evidence.cleanup_slope_bytes_per_cycle - 1_024.0).abs() < 1e-9);
        assert!(evidence.pass);
    }

    #[test]
    fn stable_points_reject_sampling_gaps() {
        let anchor = stable_anchor(1_000_000_000).unwrap();
        let samples = [sample(2_000_000_000, 1)];
        assert!(stable_median_bytes(&samples, &anchor).is_err());
    }
}
