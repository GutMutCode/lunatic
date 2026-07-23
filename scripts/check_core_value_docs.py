#!/usr/bin/env python3
"""Fail when active core-value claims drift from their executable evidence.

This is deliberately a small acceptance contract, not a natural-language
fact checker. It protects the high-risk checkboxes and evidence boundaries
that have previously drifted between README.md, CORE_VALUES.md, the canonical
status, and the benchmark guide. When a current benchmark artifact is passed,
the script also compares the measured labels with the evidence class of the
corresponding product target: a fast micro/component result can never promote
a production-E2E checkbox.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from io import StringIO
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def fail(message: str) -> None:
    print(f"core-value documentation contract error: {message}", file=sys.stderr)
    raise SystemExit(1)


@dataclass(frozen=True)
class CheckboxContract:
    label: str
    checked: bool
    evidence_path: str | None = None
    evidence_symbol: str | None = None


@dataclass(frozen=True)
class PerformanceContract:
    checkbox_label: str
    benchmark_label: str | None
    evidence_class: str
    target_us: float


CHECKBOXES = (
    CheckboxContract("Process spawn < 10μs", False),
    CheckboxContract("Live hot reload < 100ms", False),
    CheckboxContract("End-to-end process message passing < 1μs", False),
    CheckboxContract("Memory overhead < 1KB per process", False),
    CheckboxContract(
        "Local live hot reload state preservation with acknowledgement and rollback proof",
        True,
        "tests/live_hot_reload.rs",
        "production_atomic_reload_waits_for_acks_and_restores_every_process",
    ),
    CheckboxContract("99.999% uptime", False),
)

PERFORMANCE = (
    PerformanceContract("Process spawn < 10μs", "spawn process", "component", 10.0),
    PerformanceContract(
        "Live hot reload < 100ms", "hot_reload_FULL_CYCLE", "component", 100_000.0
    ),
    PerformanceContract(
        "End-to-end process message passing < 1μs",
        "message_round_trip",
        "micro",
        1.0,
    ),
    PerformanceContract("Memory overhead < 1KB per process", None, "projection", 0.0),
)

REQUIRED_EVIDENCE = (
    (
        "tests/distributed_registry_e2e.rs",
        "guest_registry_resolves_a_live_remote_mailbox_and_cleans_up_owner_exit",
    ),
    (
        "tests/live_hot_reload.rs",
        "production_atomic_reload_waits_for_acks_and_restores_every_process",
    ),
    (
        "benches/distributed_latency.rs",
        "distributed_registry_live_mailbox_round_trip",
    ),
    (
        "tests/wasm_scale_soak.rs",
        "actual_wasm_scale_soak_and_resource_pressure_are_bounded",
    ),
    (
        "tests/wasm_scale_soak.rs",
        "extended_actual_wasm_soak_is_bounded",
    ),
    (
        "tests/live_hot_reload_scale.rs",
        "production_hot_reload_scale_soak_preserves_full_mailboxes_and_rolls_back",
    ),
    (
        "tests/wasm_link_death.rs",
        "actual_wasm_trap_drives_supervisor_restart_policy",
    ),
    (
        "crates/lunatic-distributed/tests/registry_coordination.rs",
        "recovered_node_resynchronizes_a_commit_missed_during_partition",
    ),
    (
        "crates/lunatic-distributed/src/congestion/mod.rs",
        "adversarial_slow_destination_fairness_benchmark",
    ),
    (
        ".github/workflows/production-soak.yml",
        "LUNATIC_EXTENDED_SOAK_DURATION_SECS: \"7200\"",
    ),
    (
        ".github/workflows/ci.yml",
        "uses: ./.github/workflows/production-soak.yml",
    ),
    (
        ".github/workflows/ci.yml",
        "test_or_release, release_soak",
    ),
)

FORBIDDEN_ACTIVE_PHRASES = (
    "live cross-node guest mailbox delivery is not yet proven end to end",
    "the live running-Wasm reload path is not yet production-verified",
    "live running-Wasm state preservation is not yet production-path verified",
    "acknowledgement, atomic commit, and tested live instance replacement remain pending",
    "guest name lookup, automatic process/node-lifecycle-triggered ownership cleanup, and delivery into a cross-node mailbox are not yet wired",
    "The measured distributed production path currently uses native processes",
    "live hot reload and guest-Wasm messaging still lack production E2E benchmarks",
)

REQUIRED_WORKLOAD_CLAIMS = {
    "README.md": (
        "two actual-Wasm guests complete 16 measured registry lookup",
        "single-baseline cumulative live-population RSS delta divided by guest count",
    ),
    "CORE_VALUES.md": (
        "16 processes, 10 rounds, 5 commit + 5 rollback, 1,280 FIFO messages, and 160 full denials",
        "three immediate traps, performs three replacements, reaches a stable fourth start",
    ),
    "docs/core_values/status.md": (
        "performs 16 echo rounds per measured batch",
        "Sunday/manual two-hour population-32 soak workflow",
    ),
    "docs/benchmarks/BENCHMARK_SUITE.md": (
        "Two-hour population-32",
    ),
}

ACTIVE_DOCS = (
    "README.md",
    "CORE_VALUES.md",
    "docs/core_values/status.md",
    "docs/benchmarks/BENCHMARK_SUITE.md",
)

TIME_RE = re.compile(
    r"^(?P<name>\S.+?)\s+time:\s+\[[\d.]+\s*(?P<unit>\S+)\s+"
    r"[\d.]+\s*\S+\s+(?P<high>[\d.]+)\s*(?P<unit2>\S+)\]"
)
BENCHER_RE = re.compile(
    r"^test\s+(?P<name>.+?)\s+\.\.\.\s+bench:\s+"
    r"(?P<value>[\d,]+)\s+ns/iter"
)
LIVE_RELOAD_RE = re.compile(
    r"live_hot_reload_scale (?P<kind>commit|rollback): samples=(?P<samples>\d+) "
    r"p50=(?P<p50>[\d.]+)ms p95=(?P<p95>[\d.]+)ms p99=(?P<p99>[\d.]+)ms"
)
LIVE_RELOAD_SUMMARY_RE = re.compile(
    r"LUNATIC_LIVE_RELOAD_EVIDENCE (?P<record>\{.*\})"
)
SCALE_EVIDENCE_RE = re.compile(r"LUNATIC_SCALE_EVIDENCE (?P<record>\{.*\})")
RESILIENCE_EVIDENCE_RE = re.compile(r"LUNATIC_RESILIENCE_EVIDENCE (?P<record>\{.*\})")
VERIFIED_SOAK_RE = re.compile(
    r"Verified two-hour soak: \[GitHub Actions run \d+\]"
    r"\((?P<url>https://github\.com/[^/\s]+/[^/\s]+/actions/runs/\d+)\) "
    r"at commit `(?P<sha>[0-9a-f]{40})`"
)
UNIT_TO_US = {"ns": 0.001, "us": 1.0, "µs": 1.0, "μs": 1.0, "ms": 1_000.0, "s": 1_000_000.0}
EXTENDED_SOAK_MIN_SECONDS = 2 * 60 * 60
EXTENDED_SOAK_POPULATION = 32
EXTENDED_SOAK_ECHO_ROUNDS = 4
EXTENDED_SOAK_MAX_RSS_GROWTH_BYTES = 512 * 1024 * 1024
SCALE_ECHO_ROUNDS = 16
SCALE_MEASURED_BATCHES = 5


def read(relative: str) -> str:
    path = ROOT / relative
    if not path.is_file():
        fail(f"required file is missing: {relative}")
    return path.read_text(encoding="utf-8")


def checkbox_state(document: str, label: str) -> bool:
    matches: list[bool] = []
    for line in document.splitlines():
        match = re.match(r"^- \[(?P<state>[ xX])\] (?P<label>.+)$", line)
        if match and match.group("label").startswith(label):
            matches.append(match.group("state").lower() == "x")
    if len(matches) != 1:
        fail(f"expected exactly one CORE_VALUES checkbox beginning {label!r}; found {len(matches)}")
    return matches[0]


def validate_evidence_path(relative: str, symbol: str) -> None:
    source = read(relative)
    if symbol not in source:
        fail(f"evidence symbol {symbol!r} is missing from {relative}")


def parse_benchmarks(text: str) -> dict[str, float]:
    results: dict[str, float] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        match = TIME_RE.match(line)
        if match:
            unit = match.group("unit")
            if unit != match.group("unit2") or unit not in UNIT_TO_US:
                fail(f"unsupported or mismatched benchmark unit in: {raw_line}")
            results[match.group("name").strip()] = float(match.group("high")) * UNIT_TO_US[unit]
            continue
        match = BENCHER_RE.match(line)
        if match:
            results[match.group("name").strip()] = float(match.group("value").replace(",", "")) * 0.001
    return results


def validate_dates(status: str, benchmark_guide: str, index: str) -> None:
    status_match = re.search(r"(?m)^Reviewed: (\d{4}-\d{2}-\d{2})$", status)
    suite_match = re.search(r"(?m)^Evidence review: (\d{4}-\d{2}-\d{2})$", benchmark_guide)
    index_match = re.search(r"\(reviewed (\d{4}-\d{2}-\d{2})(?:,|\))", index)
    if not status_match or not suite_match or not index_match:
        fail("review dates are missing from status, benchmark guide, or core-value index")
    dates = {status_match.group(1), suite_match.group(1), index_match.group(1)}
    if len(dates) != 1:
        fail(f"review dates disagree: {sorted(dates)}")


def validate_reviewed_baseline(status: str) -> None:
    match = re.search(r"(?m)^Reviewed implementation baseline: `([0-9a-f]{7,40})`", status)
    if not match:
        fail("status.md must name a reviewed implementation baseline commit")
    baseline = match.group(1)
    if "current checkout SHA recorded by CI" not in status:
        fail("status.md must bind this review's evidence to the CI checkout SHA")
    ci_workflow = read(".github/workflows/ci.yml")
    if "git rev-parse HEAD" not in ci_workflow:
        fail("CI must record the exact checkout SHA used for production evidence")
    try:
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", baseline, "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        fail(f"reviewed implementation baseline {baseline} is not an ancestor of HEAD")


def validate_soak_document_state(
    status: str, benchmark_guide: str, active_text: str
) -> None:
    pending_markers = (
        "has not yet produced a remote result for this branch",
        "Implemented but not yet run remotely for this branch",
    )
    status_pending = pending_markers[0] in status
    benchmark_pending = pending_markers[1] in benchmark_guide
    status_verified = VERIFIED_SOAK_RE.search(status)
    benchmark_verified = VERIFIED_SOAK_RE.search(benchmark_guide)

    if status_pending or benchmark_pending:
        if not (status_pending and benchmark_pending):
            fail("status and benchmark guide disagree about whether the two-hour soak is pending")
        if status_verified or benchmark_verified:
            fail("two-hour soak documentation mixes pending and verified states")
        for marker in pending_markers:
            if active_text.count(marker) != 1:
                fail(f"two-hour soak pending marker must appear exactly once: {marker!r}")
        return

    if status_verified is None or benchmark_verified is None:
        fail(
            "completed two-hour soak documentation must link the same GitHub Actions run "
            "and 40-character checkout SHA in status.md and BENCHMARK_SUITE.md"
        )
    status_evidence = (status_verified.group("url"), status_verified.group("sha"))
    benchmark_evidence = (
        benchmark_verified.group("url"),
        benchmark_verified.group("sha"),
    )
    if status_evidence != benchmark_evidence:
        fail("status and benchmark guide reference different two-hour soak evidence")

    evidence_url, evidence_sha = status_evidence
    url_match = re.fullmatch(
        r"https://github\.com/(?P<repository>[^/\s]+/[^/\s]+)/actions/runs/\d+",
        evidence_url,
    )
    if url_match is None:
        fail("verified two-hour soak URL is not a GitHub Actions run URL")

    allowed_repositories: set[str] = set()
    if github_repository := os.environ.get("GITHUB_REPOSITORY"):
        allowed_repositories.add(github_repository)
    else:
        remotes = subprocess.run(
            ["git", "remote", "-v"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        for line in remotes.splitlines():
            fields = line.split()
            if len(fields) < 2:
                continue
            remote_match = re.search(
                r"(?:https://github\.com/|git@[^:]+:)([^/\s]+/[^/\s]+?)(?:\.git)?$",
                fields[1],
            )
            if remote_match:
                allowed_repositories.add(remote_match.group(1))
    if url_match.group("repository") not in allowed_repositories:
        fail(
            "verified two-hour soak URL does not belong to the current GitHub repository "
            "or a configured remote"
        )

    try:
        subprocess.run(
            ["git", "cat-file", "-e", f"{evidence_sha}^{{commit}}"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", evidence_sha, "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        fail(
            f"verified two-hour soak commit {evidence_sha} is missing or not an ancestor of HEAD"
        )


def validate_benchmark_contract(path: Path, core_values: str) -> None:
    results = parse_benchmarks(path.read_text(encoding="utf-8"))
    for contract in PERFORMANCE:
        checked = checkbox_state(core_values, contract.checkbox_label)
        if contract.benchmark_label is None:
            if checked:
                fail(f"{contract.checkbox_label!r} is checked without a measured workload")
            continue
        if contract.benchmark_label not in results:
            fail(
                f"latest benchmark output is missing {contract.benchmark_label!r} "
                f"for {contract.checkbox_label!r}"
            )
        measured = results[contract.benchmark_label]
        qualifies = contract.evidence_class == "production_e2e" and measured <= contract.target_us
        if checked and not qualifies:
            fail(
                f"{contract.checkbox_label!r} is checked, but latest {contract.evidence_class} "
                f"evidence is {measured:.3f}µs (target {contract.target_us:.3f}µs)"
            )
        print(
            f"benchmark evidence: {contract.checkbox_label}: {measured:.3f}µs, "
            f"class={contract.evidence_class}, checked={checked}"
        )


def validate_scale_output(
    path: Path,
    core_values: str,
    require_extended_soak: bool = False,
    require_linux_rss: bool = False,
) -> None:
    records = [
        json.loads(match.group("record"))
        for match in SCALE_EVIDENCE_RE.finditer(path.read_text(encoding="utf-8"))
    ]
    scale_entries = [record for record in records if record.get("kind") == "scale"]
    scale_populations = {record.get("population") for record in scale_entries}
    bounded_records_present = bool(scale_populations)
    soak: list[dict[str, object]] = []
    if bounded_records_present:
        if len(scale_entries) != 3 or scale_populations != {1, 8, 32}:
            fail(
                "latest scale evidence must contain exactly one record for each live-Wasm "
                f"population [1, 8, 32]: {sorted(scale_populations)}"
            )
        scale_records = {
            record["population"]: record
            for record in records
            if record.get("kind") == "scale"
        }
        linux_rss_baselines: set[int] = set()
        for population in (1, 8, 32):
            record = scale_records[population]
            expected_process_samples = population * SCALE_MEASURED_BATCHES
            expected_echo_samples = expected_process_samples * SCALE_ECHO_ROUNDS
            if record.get("warmup_batches") != 1:
                fail(f"population {population} is missing its unmeasured warm-up batch")
            if record.get("measured_batches") != SCALE_MEASURED_BATCHES:
                fail(
                    f"population {population} used {record.get('measured_batches')!r} "
                    f"measured batches instead of {SCALE_MEASURED_BATCHES}"
                )
            if record.get("spawn_samples") != expected_process_samples:
                fail(
                    f"population {population} has {record.get('spawn_samples')!r} "
                    "spawn samples"
                )
            if record.get("spawn_batch_elapsed_us", 0) <= 0:
                fail(f"population {population} has no spawn throughput interval")
            if record.get("spawn_rate_per_sec", 0) <= 0:
                fail(f"population {population} has no positive spawn throughput rate")
            for prefix in ("spawn", "ready", "mailbox_echo"):
                percentiles = [
                    record.get(f"{prefix}_{percentile}_us")
                    for percentile in ("p50", "p95", "p99")
                ]
                if (
                    any(
                        not isinstance(value, (int, float))
                        or isinstance(value, bool)
                        or value < 0
                        for value in percentiles
                    )
                    or percentiles != sorted(percentiles)
                ):
                    fail(
                        f"population {population} has missing or inconsistent "
                        f"{prefix} percentiles"
                    )
            if record.get("ready_samples") != expected_process_samples:
                fail(
                    f"population {population} has {record.get('ready_samples')!r} "
                    "readiness samples"
                )
            if record.get("mailbox_echo_rounds_per_batch") != SCALE_ECHO_ROUNDS:
                fail(
                    f"population {population} used "
                    f"{record.get('mailbox_echo_rounds_per_batch')!r} mailbox rounds "
                    f"per batch instead of {SCALE_ECHO_ROUNDS}"
                )
            if record.get("mailbox_echo_samples") != expected_echo_samples:
                fail(
                    f"population {population} has {record.get('mailbox_echo_samples')!r} "
                    f"mailbox echo samples instead of {expected_echo_samples}"
                )
            if record.get("mailbox_echo_elapsed_us", 0) <= 0:
                fail(f"population {population} has no mailbox throughput interval")
            if record.get("mailbox_echo_rate_per_sec", 0) <= 0:
                fail(f"population {population} has no positive mailbox throughput rate")
            if record.get("wasm_committed_bytes_per_guest") != 64 * 1024:
                fail(
                    f"population {population} did not report one committed Wasm page per guest"
                )
            if (
                record.get("rss_attribution")
                != "observed_process_wide_not_allocator_attributable"
            ):
                fail(
                    f"population {population} overstates or omits the RSS attribution boundary"
                )
            if (
                record.get("rss_curve_method")
                != "single_baseline_incremental_live_populations_1_8_32"
            ):
                fail(f"population {population} does not use the cumulative live RSS curve")
            if "observed_process_rss_delta_per_guest_bytes" not in record:
                fail(f"population {population} omits the observed RSS-per-guest curve field")
            if (
                record.get("observed_process_rss_delta_per_guest_method")
                != "process_wide_delta_divided_by_live_guest_count"
            ):
                fail(f"population {population} omits the RSS-per-guest calculation method")
            if require_linux_rss:
                rss_fields = (
                    "observed_process_rss_before_bytes",
                    "observed_process_rss_after_bytes",
                    "observed_process_rss_delta_bytes",
                    "observed_process_rss_delta_per_guest_bytes",
                )
                for field in rss_fields:
                    value = record.get(field)
                    if not isinstance(value, int) or isinstance(value, bool):
                        fail(
                            f"population {population} Linux evidence field {field!r} "
                            "must be an integer"
                        )
                before = record["observed_process_rss_before_bytes"]
                after = record["observed_process_rss_after_bytes"]
                delta = record["observed_process_rss_delta_bytes"]
                linux_rss_baselines.add(before)
                if before < 0 or after < 0 or delta != after - before:
                    fail(f"population {population} has inconsistent process-wide RSS evidence")
                expected_per_guest = abs(delta) // population
                if delta < 0:
                    expected_per_guest = -expected_per_guest
                if record["observed_process_rss_delta_per_guest_bytes"] != expected_per_guest:
                    fail(f"population {population} has inconsistent RSS-per-guest evidence")
        if require_linux_rss and len(linux_rss_baselines) != 1:
            fail("Linux live-population RSS records do not share one pre-guest baseline")
        pressure = [
            record for record in records if record.get("kind") == "resource_pressure"
        ]
        if len(pressure) != 1 or pressure[0].get("memory_grow_results") != [1, -1]:
            fail("latest scale evidence is missing the mailbox/memory pressure contract")
        if (
            not pressure[0].get("capacity_reused")
            or pressure[0].get("rejection") != "mailbox_full"
        ):
            fail("latest scale evidence did not prove bounded-mailbox rejection and reuse")
        soak = [record for record in records if record.get("kind") == "soak"]
        if (
            len(soak) != 1
            or soak[0].get("cycles", 0) < 10
            or soak[0].get("process_lifecycles", 0) < 80
        ):
            fail("latest scale evidence is missing the bounded ten-cycle/80-lifecycle soak")
        if soak[0].get("registered_after_each_cycle") != 1:
            fail("bounded soak retained a guest registration after a cleanup cycle")
    elif not require_extended_soak:
        fail("latest scale evidence has no live-Wasm population curve")
    if checkbox_state(core_values, "Memory overhead < 1KB per process"):
        fail("the process-wide RSS curve cannot complete the per-process memory target")

    extended = [record for record in records if record.get("kind") == "extended_soak"]
    if require_extended_soak:
        if len(extended) != 1:
            fail("extended-soak validation requires exactly one machine-readable record")
        extended_record = extended[0]
        integer_fields = (
            "requested_duration_seconds",
            "elapsed_seconds",
            "population",
            "echo_rounds_per_cycle",
            "cycles",
            "process_lifecycles",
            "mailbox_echo_samples",
            "registered_after_each_cycle",
            "registered_after_shutdown",
            "wasm_committed_bytes_per_guest",
            "rss_baseline_bytes",
            "rss_peak_bytes",
            "rss_final_bytes",
            "rss_samples",
            "rss_growth_bytes",
            "rss_growth_limit_bytes",
        )
        for field in integer_fields:
            value = extended_record.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                fail(f"extended soak field {field!r} must be a non-negative integer")

        requested = extended_record["requested_duration_seconds"]
        elapsed = extended_record["elapsed_seconds"]
        population = extended_record["population"]
        echo_rounds = extended_record["echo_rounds_per_cycle"]
        cycles = extended_record["cycles"]
        if requested < EXTENDED_SOAK_MIN_SECONDS:
            fail(
                "extended soak was configured below the required two-hour duration: "
                f"{requested!r}"
            )
        if elapsed < requested:
            fail(
                "extended soak ended before its requested duration: "
                f"elapsed={elapsed!r}, requested={requested!r}"
            )
        if population != EXTENDED_SOAK_POPULATION:
            fail(
                f"extended soak population is {population!r}; "
                f"expected {EXTENDED_SOAK_POPULATION}"
            )
        if echo_rounds != EXTENDED_SOAK_ECHO_ROUNDS:
            fail(
                f"extended soak used {echo_rounds!r} echo rounds per cycle; "
                f"expected {EXTENDED_SOAK_ECHO_ROUNDS}"
            )
        if cycles < 1:
            fail("extended soak did not complete a process cycle")
        expected_lifecycles = cycles * population
        if extended_record["process_lifecycles"] != expected_lifecycles:
            fail("extended soak process lifecycle count is inconsistent")
        expected_echo_samples = expected_lifecycles * echo_rounds
        if extended_record["mailbox_echo_samples"] != expected_echo_samples:
            fail("extended soak mailbox echo sample count is inconsistent")
        if extended_record.get("registered_after_each_cycle") != 1:
            fail("extended soak leaked a guest registration between cycles")
        if extended_record.get("registered_after_shutdown") != 0:
            fail("extended soak retained processes after observer shutdown")
        if extended_record["wasm_committed_bytes_per_guest"] != 64 * 1024:
            fail("extended soak did not report one committed Wasm page per guest")
        rss_baseline = extended_record["rss_baseline_bytes"]
        rss_peak = extended_record["rss_peak_bytes"]
        rss_final = extended_record["rss_final_bytes"]
        rss_growth = extended_record["rss_growth_bytes"]
        if rss_peak < rss_baseline or rss_peak < rss_final:
            fail("extended soak RSS peak is inconsistent with its baseline or final sample")
        if rss_growth != rss_peak - rss_baseline:
            fail("extended soak RSS growth does not equal peak minus baseline")
        if (
            extended_record["rss_growth_limit_bytes"]
            != EXTENDED_SOAK_MAX_RSS_GROWTH_BYTES
        ):
            fail("extended soak did not use the fixed 512MiB RSS-growth guard")
        if rss_growth > extended_record["rss_growth_limit_bytes"]:
            fail("extended soak exceeded its configured RSS-growth guard")
        if extended_record["rss_samples"] < cycles * 2 + 2:
            fail("extended soak did not sample RSS before, during, and after every cycle")
        if (
            extended_record.get("rss_attribution")
            != "observed_process_wide_not_allocator_attributable"
        ):
            fail("extended soak overstates or omits the RSS attribution boundary")
        if extended_record.get("rss_guard_passed") is not True:
            fail("extended soak exceeded its configured RSS-growth guard")
    if bounded_records_present:
        print(
            "production evidence: live-Wasm populations [1, 8, 32], mailbox/memory pressure, "
            f"and {soak[0]['process_lifecycles']} bounded lifecycles; memory target remains unchecked"
        )
    if require_extended_soak:
        print(
            "production evidence: two-hour extended actual-Wasm soak completed within its "
            "registration and RSS-growth guards"
        )


def validate_resilience_output(path: Path) -> None:
    records = [
        json.loads(match.group("record"))
        for match in RESILIENCE_EVIDENCE_RE.finditer(path.read_text(encoding="utf-8"))
    ]
    by_kind = {record.get("kind"): record for record in records}
    required = {
        "guest_wasm_cross_node_round_trip",
        "registry_partition_recovery",
        "slow_consumer_fairness",
        "repeated_wasm_crash_restart",
    }
    missing = required.difference(by_kind)
    if missing or len(records) != len(required) or set(by_kind) != required:
        fail(f"production resilience output is missing evidence kinds: {sorted(missing)}")

    guest = by_kind["guest_wasm_cross_node_round_trip"]
    if guest.get("samples") != 16 or guest.get("loopback_mtls") is not True:
        fail("guest-Wasm cross-node evidence lacks sixteen loopback-mTLS round trips")
    elapsed_us = guest.get("elapsed_us")
    average_us = guest.get("average_us")
    rate_per_sec = guest.get("rate_per_sec")
    if any(
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
        for value in (elapsed_us, average_us, rate_per_sec)
    ):
        fail("guest-Wasm cross-node evidence lacks a measured timing interval")
    expected_average = elapsed_us / guest["samples"]
    if abs(average_us - expected_average) > max(1.0, expected_average * 0.01):
        fail("guest-Wasm cross-node average is inconsistent with elapsed time and samples")
    expected_rate = guest["samples"] * 1_000_000.0 / elapsed_us
    if abs(rate_per_sec - expected_rate) > expected_rate * 0.01:
        fail("guest-Wasm cross-node rate is inconsistent with elapsed time and samples")
    guest_percentiles = [guest.get(field, -1) for field in ("p50_us", "p95_us", "p99_us")]
    if (
        any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value < 0
            for value in guest_percentiles
        )
        or guest_percentiles != sorted(guest_percentiles)
    ):
        fail("guest-Wasm cross-node latency percentiles are missing or inconsistent")

    partition = by_kind["registry_partition_recovery"]
    if partition.get("cycles") != 3 or partition.get("converged") is not True:
        fail("registry partition evidence lacks three successful recovery cycles")
    partition_percentiles = [
        partition.get("recovery_p50_ms"),
        partition.get("recovery_p99_ms"),
    ]
    if (
        any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value < 0
            for value in partition_percentiles
        )
        or partition_percentiles != sorted(partition_percentiles)
    ):
        fail("registry partition recovery timings are missing or inconsistent")

    fairness = by_kind["slow_consumer_fairness"]
    if fairness.get("samples", 0) < 16 or not fairness.get(
        "stalled_lane_remained_bounded"
    ):
        fail("slow-consumer evidence lacks bounded-lane fairness samples")
    if fairness.get("healthy_lane_p99_us", sys.maxsize) > fairness.get("limit_us", -1):
        fail("slow-consumer healthy-lane p99 exceeded its declared guard")

    restart = by_kind["repeated_wasm_crash_restart"]
    failures = restart.get("failures", 0)
    if failures < 3 or restart.get("starts") != failures + 1:
        fail("actual-Wasm supervisor evidence lacks three crash-driven replacements")
    if not restart.get("stable_replacement_active"):
        fail("actual-Wasm supervisor never reached a stable replacement")
    if restart.get("registered_after_shutdown") != 0:
        fail("actual-Wasm supervisor retained a process after shutdown")

    print(
        "production evidence: timed guest-Wasm cross-node traffic, three partition "
        "recoveries, bounded slow-consumer fairness, and three crash-driven restarts"
    )


def validate_live_reload_output(path: Path, core_values: str) -> None:
    output = path.read_text(encoding="utf-8")
    matches = {
        match.group("kind"): {
            "samples": int(match.group("samples")),
            "p99_ms": float(match.group("p99")),
        }
        for match in LIVE_RELOAD_RE.finditer(output)
    }
    if set(matches) != {"commit", "rollback"}:
        fail(f"latest live-reload evidence is incomplete: {sorted(matches)}")
    if any(result["samples"] < 5 for result in matches.values()):
        fail("latest live-reload evidence has fewer than five commit or rollback samples")
    summaries = [
        json.loads(match.group("record"))
        for match in LIVE_RELOAD_SUMMARY_RE.finditer(output)
    ]
    if len(summaries) != 1:
        fail("latest live-reload evidence lacks one machine-readable workload summary")
    summary = summaries[0]
    expected = {
        "processes": 16,
        "rounds": 10,
        "commit_samples": 5,
        "rollback_samples": 5,
        "fifo_messages": 1_280,
        "mailbox_full_denials": 160,
        "registered_after_shutdown": 0,
    }
    for field, expected_value in expected.items():
        if summary.get(field) != expected_value:
            fail(
                f"live-reload workload field {field!r} is {summary.get(field)!r}; "
                f"expected {expected_value!r}"
            )
    if checkbox_state(core_values, "Live hot reload < 100ms"):
        fail(
            "one same-runner live-reload gate cannot complete the portable <100ms product target"
        )
    print(
        "production evidence: live reload "
        f"commit p99={matches['commit']['p99_ms']:.3f}ms, "
        f"rollback p99={matches['rollback']['p99_ms']:.3f}ms; "
        "portable product target remains unchecked"
    )


def validate_repository(
    benchmark_output: Path | None,
    scale_output: Path | None,
    live_reload_output: Path | None,
    resilience_output: Path | None,
    require_extended_soak: bool,
    require_linux_rss: bool,
) -> None:
    documents = {relative: read(relative) for relative in ACTIVE_DOCS}
    core_values = documents["CORE_VALUES.md"]

    for contract in CHECKBOXES:
        actual = checkbox_state(core_values, contract.label)
        if actual != contract.checked:
            fail(
                f"checkbox {contract.label!r} is {'checked' if actual else 'unchecked'}; "
                f"expected {'checked' if contract.checked else 'unchecked'}"
            )
        if contract.evidence_path and contract.evidence_symbol:
            validate_evidence_path(contract.evidence_path, contract.evidence_symbol)

    for relative, symbol in REQUIRED_EVIDENCE:
        validate_evidence_path(relative, symbol)

    active_text = "\n".join(documents.values())
    for phrase in FORBIDDEN_ACTIVE_PHRASES:
        if phrase in active_text:
            fail(f"stale active-document claim is present: {phrase!r}")

    for relative, claims in REQUIRED_WORKLOAD_CLAIMS.items():
        for claim in claims:
            if claim not in documents[relative]:
                fail(f"{relative} is missing fixed workload claim {claim!r}")

    if "two actual-Wasm guests complete 16 measured registry lookup" not in documents["README.md"]:
        fail("README does not describe the verified cross-node guest mailbox boundary")
    if "waits for acknowledgements, and commits or rolls back locally" not in documents["README.md"]:
        fail("README does not describe the verified local live-reload boundary")

    index = read("docs/core_values/README.md")
    validate_dates(
        documents["docs/core_values/status.md"],
        documents["docs/benchmarks/BENCHMARK_SUITE.md"],
        index,
    )
    validate_reviewed_baseline(documents["docs/core_values/status.md"])
    validate_soak_document_state(
        documents["docs/core_values/status.md"],
        documents["docs/benchmarks/BENCHMARK_SUITE.md"],
        active_text + "\n" + read("docs/CI_BENCHMARK_INTEGRATION.md"),
    )

    if benchmark_output is not None:
        if not benchmark_output.is_file():
            fail(f"benchmark output does not exist: {benchmark_output}")
        validate_benchmark_contract(benchmark_output, core_values)
    if scale_output is not None:
        if not scale_output.is_file():
            fail(f"scale output does not exist: {scale_output}")
        validate_scale_output(
            scale_output,
            core_values,
            require_extended_soak,
            require_linux_rss,
        )
    elif require_extended_soak:
        fail("--require-extended-soak requires --scale-output")
    elif require_linux_rss:
        fail("--require-linux-rss requires --scale-output")
    if live_reload_output is not None:
        if not live_reload_output.is_file():
            fail(f"live-reload output does not exist: {live_reload_output}")
        validate_live_reload_output(live_reload_output, core_values)
    if resilience_output is not None:
        if not resilience_output.is_file():
            fail(f"resilience output does not exist: {resilience_output}")
        validate_resilience_output(resilience_output)

    print("core-value documentation contract OK")


def self_test() -> None:
    fixture = """\
spawn process time: [9.0 µs 10.0 µs 11.0 µs]
test message_round_trip ... bench: 1,234 ns/iter (+/- 10)
"""
    parsed = parse_benchmarks(fixture)
    if parsed != {"spawn process": 11.0, "message_round_trip": 1.234}:
        fail(f"benchmark parser self-test failed: {parsed!r}")
    live = LIVE_RELOAD_RE.search(
        "live_hot_reload_scale commit: samples=5 p50=4.000ms p95=5.000ms p99=6.000ms"
    )
    if live is None or float(live.group("p99")) != 6.0:
        fail("live-reload parser self-test failed")
    live_summary = LIVE_RELOAD_SUMMARY_RE.search(
        'LUNATIC_LIVE_RELOAD_EVIDENCE {"processes":16,"rounds":10}'
    )
    if live_summary is None or json.loads(live_summary.group("record"))["rounds"] != 10:
        fail("live-reload summary parser self-test failed")
    scale = SCALE_EVIDENCE_RE.search(
        'LUNATIC_SCALE_EVIDENCE {"kind":"scale","population":1}'
    )
    if scale is None or json.loads(scale.group("record"))["population"] != 1:
        fail("scale-evidence parser self-test failed")
    resilience = RESILIENCE_EVIDENCE_RE.search(
        'LUNATIC_RESILIENCE_EVIDENCE {"kind":"registry_partition_recovery","cycles":3}'
    )
    if resilience is None or json.loads(resilience.group("record"))["cycles"] != 3:
        fail("resilience-evidence parser self-test failed")

    def run_quietly(action: Callable[[], None]) -> None:
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            action()

    def expect_failure(label: str, action: Callable[[], None]) -> None:
        try:
            run_quietly(action)
        except SystemExit as error:
            if error.code != 1:
                fail(f"{label} self-test exited with unexpected status {error.code!r}")
        else:
            fail(f"{label} self-test unexpectedly passed")

    core_values = read("CORE_VALUES.md")
    with tempfile.TemporaryDirectory(prefix="lunatic-core-value-self-test-") as directory:
        fixture_dir = Path(directory)

        def write_scale_records(name: str, records: list[dict[str, object]]) -> Path:
            path = fixture_dir / name
            path.write_text(
                "\n".join(
                    f"LUNATIC_SCALE_EVIDENCE {json.dumps(record, separators=(',', ':'))}"
                    for record in records
                )
                + "\n",
                encoding="utf-8",
            )
            return path

        scale_records: list[dict[str, object]] = []
        baseline = 10_000_000
        for population in (1, 8, 32):
            delta = population * 4_096
            process_samples = population * SCALE_MEASURED_BATCHES
            scale_records.append(
                {
                    "kind": "scale",
                    "population": population,
                    "warmup_batches": 1,
                    "measured_batches": SCALE_MEASURED_BATCHES,
                    "spawn_samples": process_samples,
                    "spawn_batch_elapsed_us": 1_000,
                    "spawn_rate_per_sec": 1_000.0,
                    "spawn_p50_us": 10,
                    "spawn_p95_us": 20,
                    "spawn_p99_us": 30,
                    "ready_samples": process_samples,
                    "ready_p50_us": 20,
                    "ready_p95_us": 30,
                    "ready_p99_us": 40,
                    "mailbox_echo_rounds_per_batch": SCALE_ECHO_ROUNDS,
                    "mailbox_echo_samples": process_samples * SCALE_ECHO_ROUNDS,
                    "mailbox_echo_elapsed_us": 2_000,
                    "mailbox_echo_rate_per_sec": 2_000.0,
                    "mailbox_echo_p50_us": 30,
                    "mailbox_echo_p95_us": 40,
                    "mailbox_echo_p99_us": 50,
                    "wasm_committed_bytes_per_guest": 64 * 1024,
                    "observed_process_rss_before_bytes": baseline,
                    "observed_process_rss_after_bytes": baseline + delta,
                    "observed_process_rss_delta_bytes": delta,
                    "observed_process_rss_delta_per_guest_bytes": 4_096,
                    "observed_process_rss_delta_per_guest_method": "process_wide_delta_divided_by_live_guest_count",
                    "rss_curve_method": "single_baseline_incremental_live_populations_1_8_32",
                    "rss_attribution": "observed_process_wide_not_allocator_attributable",
                }
            )
        scale_records.extend(
            [
                {
                    "kind": "resource_pressure",
                    "memory_grow_results": [1, -1],
                    "capacity_reused": True,
                    "rejection": "mailbox_full",
                },
                {
                    "kind": "soak",
                    "cycles": 10,
                    "process_lifecycles": 80,
                    "registered_after_each_cycle": 1,
                },
            ]
        )
        valid_scale_path = write_scale_records("valid-scale.txt", scale_records)
        run_quietly(
            lambda: validate_scale_output(
                valid_scale_path,
                core_values,
                require_linux_rss=True,
            )
        )

        wrong_batches = copy.deepcopy(scale_records)
        wrong_batches[0]["measured_batches"] = SCALE_MEASURED_BATCHES - 1
        wrong_batches_path = write_scale_records("wrong-batches.txt", wrong_batches)
        expect_failure(
            "scale measured-batch rejection",
            lambda: validate_scale_output(
                wrong_batches_path,
                core_values,
                require_linux_rss=True,
            ),
        )

        wrong_rss = copy.deepcopy(scale_records)
        wrong_rss[1]["observed_process_rss_before_bytes"] = baseline + 1
        wrong_rss[1]["observed_process_rss_after_bytes"] = baseline + 1 + 8 * 4_096
        wrong_rss_path = write_scale_records("wrong-rss.txt", wrong_rss)
        expect_failure(
            "scale shared-RSS-baseline rejection",
            lambda: validate_scale_output(
                wrong_rss_path,
                core_values,
                require_linux_rss=True,
            ),
        )

        extended_record: dict[str, object] = {
            "kind": "extended_soak",
            "requested_duration_seconds": EXTENDED_SOAK_MIN_SECONDS,
            "elapsed_seconds": EXTENDED_SOAK_MIN_SECONDS,
            "population": EXTENDED_SOAK_POPULATION,
            "echo_rounds_per_cycle": EXTENDED_SOAK_ECHO_ROUNDS,
            "cycles": 2,
            "process_lifecycles": 64,
            "mailbox_echo_samples": 256,
            "registered_after_each_cycle": 1,
            "registered_after_shutdown": 0,
            "wasm_committed_bytes_per_guest": 64 * 1024,
            "rss_baseline_bytes": baseline,
            "rss_peak_bytes": baseline + 4_096,
            "rss_final_bytes": baseline + 2_048,
            "rss_samples": 6,
            "rss_growth_bytes": 4_096,
            "rss_growth_limit_bytes": EXTENDED_SOAK_MAX_RSS_GROWTH_BYTES,
            "rss_guard_passed": True,
            "rss_attribution": "observed_process_wide_not_allocator_attributable",
        }
        valid_extended_path = write_scale_records("valid-extended.txt", [extended_record])
        run_quietly(
            lambda: validate_scale_output(
                valid_extended_path,
                core_values,
                require_extended_soak=True,
            )
        )
        for label, field, value in (
            ("extended duration rejection", "requested_duration_seconds", 7_199),
            ("extended population rejection", "population", 1),
            ("extended RSS arithmetic rejection", "rss_growth_bytes", 0),
            (
                "extended RSS-guard rejection",
                "rss_growth_limit_bytes",
                EXTENDED_SOAK_MAX_RSS_GROWTH_BYTES + 1,
            ),
        ):
            invalid_extended = copy.deepcopy(extended_record)
            invalid_extended[field] = value
            invalid_path = write_scale_records(f"{field}.txt", [invalid_extended])
            expect_failure(
                label,
                lambda path=invalid_path: validate_scale_output(
                    path,
                    core_values,
                    require_extended_soak=True,
                ),
            )

        def write_resilience_records(
            name: str, records: list[dict[str, object]]
        ) -> Path:
            path = fixture_dir / name
            path.write_text(
                "\n".join(
                    f"LUNATIC_RESILIENCE_EVIDENCE {json.dumps(record, separators=(',', ':'))}"
                    for record in records
                )
                + "\n",
                encoding="utf-8",
            )
            return path

        resilience_records: list[dict[str, object]] = [
            {
                "kind": "guest_wasm_cross_node_round_trip",
                "samples": 16,
                "elapsed_us": 1_600,
                "average_us": 100.0,
                "p50_us": 90,
                "p95_us": 110,
                "p99_us": 120,
                "rate_per_sec": 10_000.0,
                "loopback_mtls": True,
            },
            {
                "kind": "registry_partition_recovery",
                "cycles": 3,
                "converged": True,
                "recovery_p50_ms": 100.0,
                "recovery_p99_ms": 200.0,
            },
            {
                "kind": "slow_consumer_fairness",
                "samples": 16,
                "healthy_lane_p99_us": 10,
                "limit_us": 100,
                "stalled_lane_remained_bounded": True,
            },
            {
                "kind": "repeated_wasm_crash_restart",
                "failures": 3,
                "starts": 4,
                "stable_replacement_active": True,
                "registered_after_shutdown": 0,
            },
        ]
        valid_resilience_path = write_resilience_records(
            "valid-resilience.txt", resilience_records
        )
        run_quietly(lambda: validate_resilience_output(valid_resilience_path))

        wrong_average = copy.deepcopy(resilience_records)
        wrong_average[0]["average_us"] = 50.0
        wrong_average_path = write_resilience_records(
            "wrong-average.txt", wrong_average
        )
        expect_failure(
            "guest round-trip average rejection",
            lambda: validate_resilience_output(wrong_average_path),
        )

        wrong_partition = copy.deepcopy(resilience_records)
        wrong_partition[1]["recovery_p50_ms"] = 300.0
        wrong_partition_path = write_resilience_records(
            "wrong-partition.txt", wrong_partition
        )
        expect_failure(
            "partition percentile rejection",
            lambda: validate_resilience_output(wrong_partition_path),
        )

    pending_status = "has not yet produced a remote result for this branch"
    pending_benchmark = "Implemented but not yet run remotely for this branch"
    run_quietly(
        lambda: validate_soak_document_state(
            pending_status,
            pending_benchmark,
            f"{pending_status}\n{pending_benchmark}",
        )
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    repository = os.environ.get("GITHUB_REPOSITORY", "lunatic-solutions/lunatic")
    verified = (
        "Verified two-hour soak: [GitHub Actions run 1]"
        f"(https://github.com/{repository}/actions/runs/1) at commit `{head}`"
    )
    run_quietly(lambda: validate_soak_document_state(verified, verified, verified))
    wrong_repository = verified.replace(repository, "invalid-owner/invalid-repository")
    expect_failure(
        "verified-soak repository rejection",
        lambda: validate_soak_document_state(
            wrong_repository,
            wrong_repository,
            wrong_repository,
        ),
    )
    wrong_sha = verified.replace(head, "0" * 40)
    expect_failure(
        "verified-soak commit rejection",
        lambda: validate_soak_document_state(wrong_sha, wrong_sha, wrong_sha),
    )

    print("core-value documentation parser and acceptance self-test OK")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-output", type=Path)
    parser.add_argument("--scale-output", type=Path)
    parser.add_argument("--live-reload-output", type=Path)
    parser.add_argument("--resilience-output", type=Path)
    parser.add_argument("--require-extended-soak", action="store_true")
    parser.add_argument("--require-linux-rss", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
    validate_repository(
        args.benchmark_output,
        args.scale_output,
        args.live_reload_output,
        args.resilience_output,
        args.require_extended_soak,
        args.require_linux_rss,
    )


if __name__ == "__main__":
    main()
