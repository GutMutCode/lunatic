#!/usr/bin/env python3
"""Run Criterion benches and enforce latency thresholds.

The script executes the configured benches, parses Criterion's textual output,
converts the reported upper-bound latency into microseconds, and fails if any
result exceeds its limit. It emits a small summary suitable for CI logs.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

# Bench configuration.
# Thresholds are expressed in microseconds.
@dataclass
class BenchTarget:
    threshold_us: float
    warn_us: float


@dataclass
class BenchConfig:
    cli_args: Tuple[str, ...]
    targets: Dict[str, BenchTarget]


BENCHES: Dict[str, BenchConfig] = {
    "spawn": BenchConfig(
        cli_args=("--sample-size", "30"),
        targets={
            # Wasmtime 46 measures around 54 µs on GitHub's shared Linux
            # runners. Warn above that baseline and retain a hard regression
            # ceiling with enough room for hosted-runner variance.
            "spawn process": BenchTarget(threshold_us=75.0, warn_us=60.0),
        },
    ),
    "messaging": BenchConfig(
        cli_args=("--sample-size", "40"),
        targets={
            "message_round_trip": BenchTarget(threshold_us=12.0, warn_us=6.0),
            "message_round_trip_selective": BenchTarget(threshold_us=15.0, warn_us=8.0),
        },
    ),
    "distributed_messaging": BenchConfig(
        cli_args=("--sample-size", "40"),
        targets={
            "distributed_request_encode_1kb": BenchTarget(
                threshold_us=500.0,
                warn_us=250.0,
            ),
            "distributed_request_decode_1kb": BenchTarget(
                threshold_us=400.0,
                warn_us=200.0,
            ),
            "distributed_quic_message_dispatch_2kb": BenchTarget(
                threshold_us=5_000.0,
                warn_us=1_000.0,
            ),
        },
    ),
    "distributed_latency": BenchConfig(
        cli_args=("--sample-size", "20"),
        targets={
            "control_lookup_nodes": BenchTarget(
                threshold_us=30_000.0,
                warn_us=15_000.0,
            ),
        },
    ),
}

# Regex lines look like:
# "spawn process           time:   [21.040 µs 21.069 µs 21.093 µs]"
TIME_RE = re.compile(
    r"^(?P<name>\S.+?)\s+time:\s+\[(?P<low>[\d\.]+)\s*(?P<unit>[^\s]+)\s+"
    r"(?P<mid>[\d\.]+)\s*[^\s]+\s+(?P<high>[\d\.]+)\s*(?P<unit2>[^\s]+)\]"
)
TIME_RE_NO_NAME = re.compile(
    r"^\s*time:\s+\[(?P<low>[\d\.]+)\s*(?P<unit>[^\s]+)\s+"
    r"(?P<mid>[\d\.]+)\s*[^\s]+\s+(?P<high>[\d\.]+)\s*(?P<unit2>[^\s]+)\]"
)
UNIT_TO_MICROS = {
    "ns": 1e-3,
    "us": 1.0,
    "µs": 1.0,
    "μs": 1.0,
    "ms": 1e3,
    "s": 1e6,
}


@dataclass
class BenchResult:
    bench_name: str
    upper_bound_us: float
    raw_line: str


def run_bench(name: str, config: BenchConfig) -> Tuple[str, List[BenchResult]]:
    cmd = [
        "cargo",
        "bench",
        "--bench",
        name,
        "--",
        *config.cli_args,
    ]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    stdout = proc.stdout
    stderr = proc.stderr

    print(stdout, end="")
    if stderr:
        print(stderr, end="", file=sys.stderr)

    if proc.returncode != 0:
        raise RuntimeError(
            f"Bench '{name}' failed with exit code {proc.returncode}."
        )

    results = parse_results(stdout.splitlines())
    if not results:
        raise RuntimeError(f"Unable to parse Criterion output for bench '{name}'.")

    return stdout, results


def parse_results(lines: Iterable[str]) -> List[BenchResult]:
    parsed: List[BenchResult] = []
    pending_name: str | None = None

    for line in lines:
        line = line.rstrip()

        if "time:" in line:
            match = TIME_RE.search(line)
            label: str | None = None
            if match:
                label = match.group("name").strip()
            else:
                alt = TIME_RE_NO_NAME.search(line)
                if alt and pending_name:
                    match = alt
                    label = pending_name
            pending_name = None

            if not match or not label:
                continue

            unit = match.group("unit")
            unit2 = match.group("unit2")
            if unit != unit2:
                raise ValueError(f"Mismatched units in line: '{line}'")
            multiplier = UNIT_TO_MICROS.get(unit)
            if multiplier is None:
                raise ValueError(f"Unsupported unit '{unit}' in line: '{line}'")

            high = float(match.group("high"))
            parsed.append(
                BenchResult(
                    bench_name=label,
                    upper_bound_us=high * multiplier,
                    raw_line=line,
                )
            )
            continue

        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Benchmarking") or stripped.startswith("change:") or stripped.startswith("Found"):
            continue
        pending_name = stripped
    return parsed


def build_summary(entries: List[Tuple[str, BenchResult, BenchTarget]]) -> str:
    rows = ["| Bench | Upper bound (µs) | Limit (µs) |", "| --- | --- | --- |"]
    for bench_id, result, target in entries:
        rows.append(
            f"| {bench_id} | {result.upper_bound_us:.3f} | {target.threshold_us:.3f} |"
        )
    return "\n".join(rows) + "\n"


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    failed = False
    summary_entries: List[Tuple[str, BenchResult, BenchTarget]] = []
    requested_benches = sys.argv[1:]
    unknown_benches = set(requested_benches) - set(BENCHES)
    if unknown_benches:
        raise ValueError(
            f"Unknown bench selection: {sorted(unknown_benches)}. "
            f"Available benches: {sorted(BENCHES)}"
        )
    selected_benches = requested_benches or list(BENCHES)

    for bench_name in selected_benches:
        config = BENCHES[bench_name]
        stdout, results = run_bench(bench_name, config)

        # Index results by bench label
        result_map = {res.bench_name: res for res in results}
        missing = set(config.targets) - set(result_map)
        if missing:
            raise RuntimeError(
                f"Bench '{bench_name}' missing expected measurements: {sorted(missing)}"
            )

        for label, target in config.targets.items():
            res = result_map[label]
            summary_entries.append((label, res, target))

            if res.upper_bound_us > target.threshold_us:
                print(
                    f"::error ::{label} upper bound {res.upper_bound_us:.3f} µs "
                    f"exceeds limit {target.threshold_us:.3f} µs",
                    file=sys.stderr,
                )
                failed = True
            elif res.upper_bound_us > target.warn_us:
                print(
                    f"::warning ::{label} regression detected: {res.upper_bound_us:.3f} µs "
                    f"> target {target.warn_us:.3f} µs",
                    file=sys.stderr,
                )

    summary = build_summary(summary_entries)
    print("\nBench summary:\n" + summary)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as handle:
            handle.write("## Benchmark Thresholds\n")
            handle.write(summary)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
