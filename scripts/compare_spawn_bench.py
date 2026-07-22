#!/usr/bin/env python3
"""Compare Lunatic's spawn benchmark at two Git commits on one machine.

This runner deliberately treats repeated, adjacent base/head executions as the
independent observations. Criterion's samples within one execution are useful
for estimating that execution's slope, but pooling them would hide shared
runner drift and overstate the amount of independent evidence.

The script creates clean detached worktrees, builds each revision into a
separate Cargo target directory, alternates base/head measurement order, and
writes both raw Criterion data and a machine-readable JSON report. It uses only
the Python standard library.
"""

from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import hashlib
import json
import math
import os
import platform
import random
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
import traceback
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple


SCHEMA_VERSION = 1
BOOTSTRAP_RESAMPLES = 50_000
BOOTSTRAP_SEED = 0x4C554E41544943  # ASCII-ish "LUNATIC", fixed for reproducibility.
WARNING_RATIO = 1.05
RELATIVE_FAILURE_RATIO = 1.10
ABSOLUTE_FAILURE_US = 75.0
MAX_RATIO_CI_WIDTH = 0.10
DEFAULT_PARITY_PATHS = ("benches/spawn.rs", "wat/hello.wat")

EXIT_OK = 0
EXIT_REGRESSION = 1
EXIT_INCONCLUSIVE = 2
EXIT_ERROR = 3


class BenchmarkError(RuntimeError):
    """An operational or evidence-integrity error."""


@dataclass(frozen=True)
class CommandResult:
    command: Tuple[str, ...]
    cwd: str
    returncode: int
    stdout: str
    stderr: str
    elapsed_seconds: float


@dataclass(frozen=True)
class BootstrapInterval:
    point: float
    lower: float
    upper: float

    @property
    def width(self) -> float:
        return self.upper - self.lower

    def as_dict(self) -> Dict[str, float]:
        return {
            "point": self.point,
            "lower": self.lower,
            "upper": self.upper,
            "width": self.width,
        }


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def render_command(command: Sequence[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(list(command))
    return shlex.join(command)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def percentile(sorted_values: Sequence[float], probability: float) -> float:
    """Return a linearly interpolated percentile from already-sorted values."""

    if not sorted_values:
        raise ValueError("percentile requires at least one value")
    if not 0.0 <= probability <= 1.0:
        raise ValueError("probability must be between zero and one")
    position = (len(sorted_values) - 1) * probability
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return float(sorted_values[lower_index])
    fraction = position - lower_index
    return float(
        sorted_values[lower_index] * (1.0 - fraction)
        + sorted_values[upper_index] * fraction
    )


def bootstrap_median_interval(
    values: Sequence[float],
    *,
    seed: int,
    resamples: int = BOOTSTRAP_RESAMPLES,
) -> BootstrapInterval:
    """Return a fixed-seed percentile-bootstrap 95% CI for a median."""

    if not values:
        raise ValueError("bootstrap requires at least one value")
    if any(not math.isfinite(value) for value in values):
        raise ValueError("bootstrap values must all be finite")
    if resamples < 1:
        raise ValueError("resamples must be positive")

    sample = tuple(float(value) for value in values)
    rng = random.Random(seed)
    sample_size = len(sample)
    estimates = []
    append = estimates.append
    for _ in range(resamples):
        append(statistics.median(sample[rng.randrange(sample_size)] for _ in range(sample_size)))
    estimates.sort()
    return BootstrapInterval(
        point=float(statistics.median(sample)),
        lower=percentile(estimates, 0.025),
        upper=percentile(estimates, 0.975),
    )


def paired_ratio_interval(
    base_ns: Sequence[float],
    head_ns: Sequence[float],
    *,
    resamples: int = BOOTSTRAP_RESAMPLES,
) -> Tuple[BootstrapInterval, List[float]]:
    if len(base_ns) != len(head_ns):
        raise ValueError("base and head observations must have equal length")
    if not base_ns:
        raise ValueError("at least one paired observation is required")
    if any(value <= 0.0 or not math.isfinite(value) for value in (*base_ns, *head_ns)):
        raise ValueError("benchmark slopes must be finite and greater than zero")

    log_ratios = [math.log(head / base) for base, head in zip(base_ns, head_ns)]
    log_interval = bootstrap_median_interval(
        log_ratios,
        seed=BOOTSTRAP_SEED,
        resamples=resamples,
    )
    return (
        BootstrapInterval(
            point=math.exp(log_interval.point),
            lower=math.exp(log_interval.lower),
            upper=math.exp(log_interval.upper),
        ),
        log_ratios,
    )


def measurement_order(pair_index: int) -> Tuple[str, str]:
    """Alternate adjacent pair order to cancel first-order temporal drift."""

    if pair_index < 0:
        raise ValueError("pair index cannot be negative")
    return ("base", "head") if pair_index % 2 == 0 else ("head", "base")


def classify_result(
    ratio: BootstrapInterval,
    head_us: BootstrapInterval,
    *,
    exhausted_pairs: bool,
) -> Tuple[str, int, List[str]]:
    reasons: List[str] = []
    relative_failure = ratio.lower > RELATIVE_FAILURE_RATIO
    absolute_failure = head_us.lower > ABSOLUTE_FAILURE_US

    if relative_failure:
        reasons.append(
            f"paired ratio 95% CI lower bound {ratio.lower:.4f} exceeds "
            f"{RELATIVE_FAILURE_RATIO:.4f}"
        )
    if absolute_failure:
        reasons.append(
            f"head median 95% bootstrap lower bound {head_us.lower:.3f} us exceeds "
            f"{ABSOLUTE_FAILURE_US:.3f} us"
        )
    if relative_failure or absolute_failure:
        if relative_failure and absolute_failure:
            return "hard_fail_relative_and_absolute", EXIT_REGRESSION, reasons
        if relative_failure:
            return "hard_fail_relative", EXIT_REGRESSION, reasons
        return "hard_fail_absolute", EXIT_REGRESSION, reasons

    if ratio.width > MAX_RATIO_CI_WIDTH and exhausted_pairs:
        reasons.append(
            f"paired ratio 95% CI width {ratio.width:.4f} exceeds "
            f"{MAX_RATIO_CI_WIDTH:.4f} after the maximum pair count"
        )
        return "inconclusive_noise", EXIT_INCONCLUSIVE, reasons

    if ratio.point > WARNING_RATIO:
        reasons.append(
            f"median paired slowdown {(ratio.point - 1.0) * 100.0:.2f}% exceeds "
            f"the {(WARNING_RATIO - 1.0) * 100.0:.2f}% warning threshold"
        )
        return "warning", EXIT_OK, reasons

    reasons.append("paired result is within configured relative and absolute limits")
    return "pass", EXIT_OK, reasons


def read_text_if_present(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except (FileNotFoundError, OSError):
        return None


def parse_os_release() -> Dict[str, str]:
    result: Dict[str, str] = {}
    text = read_text_if_present(Path("/etc/os-release"))
    if not text:
        return result
    for line in text.splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key] = value.strip().strip('"')
    return result


def memory_metadata() -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    meminfo = read_text_if_present(Path("/proc/meminfo"))
    if meminfo:
        wanted = {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}
        parsed: Dict[str, str] = {}
        for line in meminfo.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            if key in wanted:
                parsed[key] = value.strip()
        result["proc_meminfo"] = parsed

    if os.name == "nt":
        class MemoryStatusEx(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatusEx()
        status.dwLength = ctypes.sizeof(status)
        try:
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
                result["windows"] = {
                    "total_physical_bytes": status.ullTotalPhys,
                    "available_physical_bytes": status.ullAvailPhys,
                    "memory_load_percent": status.dwMemoryLoad,
                }
        except (AttributeError, OSError):
            pass
    elif platform.system() == "Darwin" and shutil.which("sysctl"):
        probe = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if probe.returncode == 0:
            result["darwin_total_bytes"] = probe.stdout.strip()
    return result


def cgroup_metadata() -> Dict[str, Any]:
    paths = [
        "/proc/self/cgroup",
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/cpu.stat",
        "/sys/fs/cgroup/cpuset.cpus.effective",
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory.current",
        "/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
        "/sys/fs/cgroup/cpu/cpu.cfs_period_us",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    values: Dict[str, str] = {}
    for raw_path in paths:
        value = read_text_if_present(Path(raw_path))
        if value is not None:
            values[raw_path] = value
    return values


def probe_lscpu() -> Optional[Any]:
    executable = shutil.which("lscpu")
    if not executable:
        return None
    proc = subprocess.run(
        [executable, "--json"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if proc.returncode != 0:
        return {"error": proc.stderr.strip(), "returncode": proc.returncode}
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {"raw": proc.stdout.strip()}


RUNNER_ENV_KEYS = (
    "CI",
    "GITHUB_ACTIONS",
    "GITHUB_BASE_REF",
    "GITHUB_HEAD_REF",
    "GITHUB_RUN_ATTEMPT",
    "GITHUB_RUN_ID",
    "GITHUB_RUN_NUMBER",
    "GITHUB_SERVER_URL",
    "GITHUB_SHA",
    "GITHUB_WORKFLOW",
    "ImageOS",
    "ImageVersion",
    "RUNNER_ARCH",
    "RUNNER_ENVIRONMENT",
    "RUNNER_NAME",
    "RUNNER_OS",
)


def environment_metadata() -> Dict[str, Any]:
    uname = platform.uname()
    return {
        "captured_at": utc_now(),
        "os": {
            "platform": platform.platform(),
            "system": uname.system,
            "node": uname.node,
            "release": uname.release,
            "version": uname.version,
            "machine": uname.machine,
            "processor": uname.processor,
            "os_release": parse_os_release(),
        },
        "cpu": {
            "logical_count": os.cpu_count(),
            "platform_processor": platform.processor(),
            "lscpu": probe_lscpu(),
        },
        "memory": memory_metadata(),
        "cgroup": cgroup_metadata(),
        "runner": {key: os.environ[key] for key in RUNNER_ENV_KEYS if key in os.environ},
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "executable": sys.executable,
        },
    }


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, path)


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def markdown_summary(report: Mapping[str, Any]) -> str:
    decision = report.get("decision") or {}
    revisions = report.get("revisions") or {}
    protocol = report.get("protocol") or {}
    statistics_payload = report.get("statistics") or {}
    environment = report.get("environment") or {}
    os_metadata = environment.get("os") or {}
    cpu_metadata = environment.get("cpu") or {}
    memory = environment.get("memory") or {}
    toolchain = report.get("toolchain") or {}

    def revision_value(label: str, key: str, fallback: str = "unavailable") -> str:
        value = (revisions.get(label) or {}).get(key)
        return str(value) if value not in (None, "") else fallback

    lines = [
        "# Paired spawn benchmark",
        "",
        f"- Status: **{decision.get('status', 'unknown')}**",
        f"- Exit code: `{decision.get('exit_code', 'unknown')}`",
        f"- Base: `{revision_value('base', 'sha')}` — {revision_value('base', 'subject')}",
        f"- Head: `{revision_value('head', 'sha')}` — {revision_value('head', 'subject')}",
        f"- Started: `{report.get('started_at', 'unavailable')}`",
        f"- Finished: `{report.get('finished_at', 'unavailable')}`",
        "",
        "## Decision",
        "",
    ]
    reasons = decision.get("reasons") or ["No decision reason was recorded."]
    lines.extend(f"- {reason}" for reason in reasons)

    lines.extend(["", "## Results", ""])
    if statistics_payload:
        ratio = statistics_payload.get("head_over_base_ratio") or {}
        base_us = statistics_payload.get("base_median_us") or {}
        head_us = statistics_payload.get("head_median_us") or {}
        lines.extend(
            [
                "| Metric | Point | 95% CI |",
                "| --- | ---: | ---: |",
                (
                    f"| Base median | {base_us.get('point', float('nan')):.3f} us | "
                    f"{base_us.get('lower', float('nan')):.3f}.."
                    f"{base_us.get('upper', float('nan')):.3f} us |"
                ),
                (
                    f"| Head median | {head_us.get('point', float('nan')):.3f} us | "
                    f"{head_us.get('lower', float('nan')):.3f}.."
                    f"{head_us.get('upper', float('nan')):.3f} us |"
                ),
                (
                    f"| Head/base ratio | {ratio.get('point', float('nan')):.4f} | "
                    f"{ratio.get('lower', float('nan')):.4f}.."
                    f"{ratio.get('upper', float('nan')):.4f} |"
                ),
                "",
                f"Paired outer observations: `{statistics_payload.get('pair_count', 'unknown')}`.",
            ]
        )
    else:
        lines.append("No complete statistical result was produced.")

    rustc_stdout = ((toolchain.get("rustc") or {}).get("stdout") or "unavailable").splitlines()
    rustc_first_line = rustc_stdout[0] if rustc_stdout else "unavailable"
    proc_memory = memory.get("proc_meminfo") or {}
    windows_memory = memory.get("windows") or {}
    if proc_memory.get("MemTotal"):
        memory_summary = str(proc_memory["MemTotal"])
    elif windows_memory.get("total_physical_bytes"):
        total_gib = float(windows_memory["total_physical_bytes"]) / (1024.0**3)
        memory_summary = f"{total_gib:.2f} GiB"
    elif memory.get("darwin_total_bytes"):
        total_gib = float(memory["darwin_total_bytes"]) / (1024.0**3)
        memory_summary = f"{total_gib:.2f} GiB"
    else:
        memory_summary = "unavailable"
    lines.extend(
        [
            "",
            "## Fixed protocol and environment",
            "",
            f"- Toolchain request: `{toolchain.get('requested', 'unavailable')}`",
            f"- rustc: `{rustc_first_line}`",
            f"- OS: `{os_metadata.get('platform', 'unavailable')}`",
            f"- CPU: `{cpu_metadata.get('platform_processor') or 'unavailable'}`; "
            f"logical CPUs `{cpu_metadata.get('logical_count', 'unavailable')}`",
            f"- Memory: `{memory_summary}`",
            f"- Initial/max pairs: `{protocol.get('initial_pairs', 'unknown')}` / "
            f"`{protocol.get('max_pairs', 'unknown')}`",
            f"- Criterion sample size: `{protocol.get('sample_size_per_invocation', 'unknown')}`",
            f"- Warm-up/measurement: `{protocol.get('warm_up_seconds_per_invocation', 'unknown')}`s / "
            f"`{protocol.get('measurement_seconds_per_invocation', 'unknown')}`s",
            f"- Raw evidence: `{report.get('log_root', 'unavailable')}`",
            "",
            "The independent statistical unit is one adjacent outer base/head pair; "
            "Criterion samples from separate invocations are not pooled as independent runs.",
            "",
        ]
    )
    return "\n".join(lines)


def write_command_log(
    path: Path,
    result: CommandResult,
    env_overrides: Optional[Mapping[str, str]] = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_env = dict(env_overrides or {})
    with path.open("w", encoding="utf-8", errors="replace", newline="\n") as handle:
        handle.write(f"command: {render_command(result.command)}\n")
        handle.write(f"cwd: {result.cwd}\n")
        handle.write(f"returncode: {result.returncode}\n")
        handle.write(f"elapsed_seconds: {result.elapsed_seconds:.6f}\n")
        if safe_env:
            handle.write("environment_overrides:\n")
            for key in sorted(safe_env):
                handle.write(f"  {key}={safe_env[key]}\n")
        handle.write("\n--- stdout ---\n")
        handle.write(result.stdout)
        if result.stdout and not result.stdout.endswith("\n"):
            handle.write("\n")
        handle.write("\n--- stderr ---\n")
        handle.write(result.stderr)
        if result.stderr and not result.stderr.endswith("\n"):
            handle.write("\n")


class CommandRunner:
    def __init__(self, timeout_seconds: float) -> None:
        self.timeout_seconds = timeout_seconds

    def run(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        env_overrides: Optional[Mapping[str, str]] = None,
        log_path: Optional[Path] = None,
        check: bool = True,
        announce: bool = True,
    ) -> CommandResult:
        command_tuple = tuple(str(part) for part in command)
        if announce:
            print(f"[spawn-paired] {render_command(command_tuple)}", flush=True)
        env: MutableMapping[str, str] = os.environ.copy()
        if env_overrides:
            env.update({key: str(value) for key, value in env_overrides.items()})
        started = time.monotonic()
        creationflags = (
            getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0
        )
        proc = subprocess.Popen(
            command_tuple,
            cwd=str(cwd),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=creationflags,
            start_new_session=os.name != "nt",
        )
        try:
            stdout, stderr = proc.communicate(timeout=self.timeout_seconds)
            result = CommandResult(
                command=command_tuple,
                cwd=str(cwd),
                returncode=proc.returncode,
                stdout=stdout,
                stderr=stderr,
                elapsed_seconds=time.monotonic() - started,
            )
        except subprocess.TimeoutExpired:
            termination_note = self._terminate_process_tree(proc)
            stdout, stderr = proc.communicate()
            result = CommandResult(
                command=command_tuple,
                cwd=str(cwd),
                returncode=124,
                stdout=stdout,
                stderr=(
                    stderr
                    + f"\ncommand timed out after {self.timeout_seconds:.1f} seconds\n"
                    + termination_note
                ),
                elapsed_seconds=time.monotonic() - started,
            )

        if log_path:
            write_command_log(log_path, result, env_overrides)
        if check and result.returncode != 0:
            if result.stdout:
                print(result.stdout[-4000:], file=sys.stderr)
            if result.stderr:
                print(result.stderr[-4000:], file=sys.stderr)
            location = f"; full log: {log_path}" if log_path else ""
            raise BenchmarkError(
                f"command failed with exit code {result.returncode}: "
                f"{render_command(command_tuple)}{location}"
            )
        return result

    @staticmethod
    def _terminate_process_tree(proc: subprocess.Popen[str]) -> str:
        """Terminate a timed-out Cargo tree rather than leaving compiler children."""

        if proc.poll() is not None:
            return "process exited while timeout handling began\n"

        if os.name == "nt":
            taskkill = shutil.which("taskkill")
            if taskkill:
                try:
                    killed = subprocess.run(
                        [taskkill, "/PID", str(proc.pid), "/T", "/F"],
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        timeout=30,
                        check=False,
                    )
                except subprocess.TimeoutExpired:
                    proc.kill()
                    return "taskkill timed out; killed the direct process only\n"
                if proc.poll() is None:
                    proc.kill()
                return (
                    f"taskkill exit={killed.returncode}; "
                    f"stdout={killed.stdout.strip()!r}; stderr={killed.stderr.strip()!r}\n"
                )
            proc.kill()
            return "taskkill unavailable; killed the direct process only\n"

        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return "process group exited while timeout handling began\n"
        try:
            proc.wait(timeout=5)
            leader_status = "group leader exited after SIGTERM"
        except subprocess.TimeoutExpired:
            leader_status = "group leader ignored SIGTERM"
        try:
            # The group leader may exit while a compiler descendant ignores
            # SIGTERM and still owns the captured pipes. Kill any survivors.
            os.killpg(proc.pid, signal.SIGKILL)
            survivor_status = "sent SIGKILL to remaining group members"
        except ProcessLookupError:
            survivor_status = "no group members remained"
        if proc.poll() is None:
            proc.kill()
        return f"{leader_status}; {survivor_status}\n"


def git_output(
    runner: CommandRunner,
    git: str,
    repo: Path,
    arguments: Sequence[str],
    *,
    check: bool = True,
) -> str:
    return runner.run(
        [git, *arguments],
        cwd=repo,
        check=check,
        announce=False,
    ).stdout.strip()


def resolve_commit(runner: CommandRunner, git: str, repo: Path, revision: str) -> str:
    try:
        commit = git_output(
            runner,
            git,
            repo,
            ["rev-parse", "--verify", f"{revision}^{{commit}}"],
        )
    except BenchmarkError as error:
        raise BenchmarkError(f"cannot resolve revision {revision!r}: {error}") from error
    if len(commit) != 40 or any(character not in "0123456789abcdefABCDEF" for character in commit):
        raise BenchmarkError(f"git returned an invalid commit id for {revision!r}: {commit!r}")
    return commit.lower()


def dirty_status(runner: CommandRunner, git: str, worktree: Path) -> List[str]:
    output = git_output(
        runner,
        git,
        worktree,
        ["status", "--porcelain=v1", "--untracked-files=all"],
    )
    return output.splitlines() if output else []


def commit_description(runner: CommandRunner, git: str, worktree: Path) -> Dict[str, str]:
    raw = git_output(
        runner,
        git,
        worktree,
        ["show", "-s", "--format=%H%x00%cI%x00%s", "HEAD"],
    )
    parts = raw.split("\x00", 2)
    if len(parts) != 3:
        raise BenchmarkError(f"unexpected git show output in {worktree}: {raw!r}")
    return {"sha": parts[0], "committed_at": parts[1], "subject": parts[2]}


def locked_package_versions(lock_path: Path, package_names: Iterable[str]) -> Dict[str, List[str]]:
    """Extract selected package versions without depending on tomllib availability."""

    wanted = set(package_names)
    found: Dict[str, List[str]] = {name: [] for name in wanted}
    text = lock_path.read_text(encoding="utf-8", errors="replace")
    current_name: Optional[str] = None
    current_version: Optional[str] = None

    def flush() -> None:
        nonlocal current_name, current_version
        if current_name in wanted and current_version is not None:
            found[current_name].append(current_version)
        current_name = None
        current_version = None

    for line in text.splitlines():
        stripped = line.strip()
        if stripped == "[[package]]":
            flush()
        elif stripped.startswith("name = \"") and stripped.endswith('"'):
            current_name = stripped[len('name = "') : -1]
        elif stripped.startswith("version = \"") and stripped.endswith('"'):
            current_version = stripped[len('version = "') : -1]
    flush()
    return {name: sorted(set(versions)) for name, versions in sorted(found.items())}


class WorktreePair:
    def __init__(
        self,
        *,
        runner: CommandRunner,
        git: str,
        repo: Path,
        temporary_root: Path,
        cleanup_log: Path,
    ) -> None:
        self.runner = runner
        self.git = git
        self.repo = repo
        self.temporary_root = temporary_root
        self.cleanup_log = cleanup_log
        self.paths = {
            "base": temporary_root / "worktree-base",
            "head": temporary_root / "worktree-head",
        }
        self.added: List[Path] = []

    def add(self, label: str, commit: str, log_path: Path) -> Path:
        path = self.paths[label]
        result = self.runner.run(
            [self.git, "worktree", "add", "--detach", str(path), commit],
            cwd=self.repo,
            log_path=log_path,
            check=False,
        )
        if path.exists():
            self.added.append(path)
        if result.returncode != 0:
            raise BenchmarkError(
                f"git worktree add failed with exit code {result.returncode} for "
                f"{label} commit {commit}; full log: {log_path}"
            )
        if path not in self.added:
            self.added.append(path)
        return path

    @staticmethod
    def _remove_readonly(function: Any, path: str, _error: Any) -> None:
        try:
            os.chmod(path, 0o700)
            function(path)
        except OSError:
            pass

    def cleanup(self) -> List[str]:
        errors: List[str] = []
        lines: List[str] = []
        for path in reversed(self.added):
            result = self.runner.run(
                [self.git, "worktree", "remove", "--force", str(path)],
                cwd=self.repo,
                check=False,
                announce=False,
            )
            lines.append(
                f"{utc_now()} remove {path}: exit={result.returncode}\n"
                f"stdout={result.stdout}\nstderr={result.stderr}\n"
            )
            if result.returncode != 0:
                try:
                    if path.exists():
                        shutil.rmtree(path, onerror=self._remove_readonly)
                except OSError as error:
                    errors.append(f"failed to remove worktree directory {path}: {error}")
                retry = self.runner.run(
                    [self.git, "worktree", "remove", "--force", str(path)],
                    cwd=self.repo,
                    check=False,
                    announce=False,
                )
                lines.append(
                    f"{utc_now()} remove-retry {path}: exit={retry.returncode}\n"
                    f"stdout={retry.stdout}\nstderr={retry.stderr}\n"
                )
            if path.exists():
                errors.append(f"worktree directory still exists after cleanup: {path}")
        worktree_list = self.runner.run(
            [self.git, "worktree", "list", "--porcelain"],
            cwd=self.repo,
            check=False,
            announce=False,
        )
        lines.append(
            f"{utc_now()} list-after-cleanup: exit={worktree_list.returncode}\n"
            f"stdout={worktree_list.stdout}\nstderr={worktree_list.stderr}\n"
        )
        if worktree_list.returncode != 0:
            errors.append(
                f"git worktree list failed during cleanup verification: "
                f"{worktree_list.stderr.strip()}"
            )
        else:
            registered = {
                os.path.normcase(os.path.abspath(line[len("worktree ") :]))
                for line in worktree_list.stdout.splitlines()
                if line.startswith("worktree ")
            }
            for path in self.added:
                normalized = os.path.normcase(os.path.abspath(path))
                if normalized in registered:
                    errors.append(f"worktree registration still exists after cleanup: {path}")
        try:
            if self.temporary_root.exists():
                shutil.rmtree(self.temporary_root, onerror=self._remove_readonly)
        except OSError as error:
            errors.append(f"failed to remove temporary root {self.temporary_root}: {error}")
        if self.temporary_root.exists():
            errors.append(f"temporary root still exists after cleanup: {self.temporary_root}")
        try:
            self.cleanup_log.parent.mkdir(parents=True, exist_ok=True)
            self.cleanup_log.write_text(
                "\n".join(lines), encoding="utf-8", errors="replace"
            )
        except OSError as error:
            errors.append(f"failed to write cleanup log {self.cleanup_log}: {error}")
        return errors


def cargo_command(cargo: str, toolchain: Optional[str], arguments: Sequence[str]) -> List[str]:
    command = [cargo]
    if toolchain:
        command.append(f"+{toolchain}")
    command.extend(arguments)
    return command


def toolchain_metadata(
    runner: CommandRunner,
    *,
    cargo: str,
    rustc: str,
    toolchain: Optional[str],
    cwd: Path,
) -> Dict[str, Any]:
    selector = [f"+{toolchain}"] if toolchain else []
    rustc_result = runner.run(
        [rustc, *selector, "-Vv"],
        cwd=cwd,
        check=False,
        announce=False,
    )
    cargo_result = runner.run(
        [cargo, *selector, "-Vv"],
        cwd=cwd,
        check=False,
        announce=False,
    )
    return {
        "requested": toolchain,
        "rustc": {
            "returncode": rustc_result.returncode,
            "stdout": rustc_result.stdout.strip(),
            "stderr": rustc_result.stderr.strip(),
        },
        "cargo": {
            "returncode": cargo_result.returncode,
            "stdout": cargo_result.stdout.strip(),
            "stderr": cargo_result.stderr.strip(),
        },
    }


def locate_estimates(criterion_home: Path, baseline_name: str) -> Path:
    candidates = sorted(criterion_home.glob(f"**/{baseline_name}/estimates.json"))
    if len(candidates) != 1:
        rendered = ", ".join(str(path) for path in candidates) or "none"
        raise BenchmarkError(
            f"expected exactly one Criterion estimates.json below {criterion_home}, "
            f"found {len(candidates)}: {rendered}"
        )
    return candidates[0]


def parse_slope_point(estimates_path: Path) -> Tuple[float, Dict[str, Any]]:
    try:
        payload = json.loads(estimates_path.read_text(encoding="utf-8"))
        slope = payload["slope"]
        point = float(slope["point_estimate"])
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        raise BenchmarkError(
            f"cannot parse Criterion slope point estimate from {estimates_path}: {error}"
        ) from error
    if not math.isfinite(point) or point <= 0.0:
        raise BenchmarkError(
            f"Criterion slope point estimate must be finite and positive, got {point!r}"
        )
    return point, payload


def relative_path_or_string(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def validate_args(args: argparse.Namespace) -> None:
    if not args.base or not args.head:
        raise BenchmarkError("--base and --head are required unless --self-test is used")
    if args.initial_pairs < 2:
        raise BenchmarkError("--initial-pairs must be at least 2")
    if args.max_pairs < args.initial_pairs:
        raise BenchmarkError("--max-pairs must be greater than or equal to --initial-pairs")
    if args.sample_size < 10:
        raise BenchmarkError("--sample-size must be at least 10 for Criterion")
    if args.output_dir is None:
        raise BenchmarkError("--output-dir is required unless --self-test is used")
    if args.warm_up_time <= 0.0:
        raise BenchmarkError("--warm-up-time must be greater than zero")
    if args.measurement_time <= 0.0:
        raise BenchmarkError("--measurement-time must be greater than zero")
    if args.command_timeout_seconds <= 0.0:
        raise BenchmarkError("--command-timeout-seconds must be greater than zero")
    for option, raw_path in (("--harness", args.harness), ("--fixture", args.fixture)):
        path = Path(raw_path)
        if path.is_absolute() or ".." in path.parts:
            raise BenchmarkError(
                f"{option} must be a repository-relative path without '..': {raw_path!r}"
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a reproducible paired Criterion comparison of Lunatic's spawn "
            "benchmark at two Git commits. Raw logs and a JSON evidence report are retained."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "base",
        nargs="?",
        help="Git revision for the comparison baseline (required unless --self-test)",
    )
    parser.add_argument(
        "head",
        nargs="?",
        help="Git revision under evaluation (required unless --self-test)",
    )
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path.cwd(),
        help="path inside the Lunatic Git repository",
    )
    parser.add_argument(
        "--toolchain",
        default="1.95.0",
        help="pinned rustup toolchain used to build and measure both revisions",
    )
    parser.add_argument(
        "--harness",
        default=DEFAULT_PARITY_PATHS[0],
        help="repository-relative benchmark harness that must match byte-for-byte",
    )
    parser.add_argument(
        "--fixture",
        default=DEFAULT_PARITY_PATHS[1],
        help="repository-relative workload fixture that must match byte-for-byte",
    )
    parser.add_argument("--initial-pairs", type=int, default=10)
    parser.add_argument("--max-pairs", type=int, default=15)
    parser.add_argument("--sample-size", type=int, default=50)
    parser.add_argument("--warm-up-time", type=float, default=1.0)
    parser.add_argument("--measurement-time", type=float, default=3.0)
    parser.add_argument(
        "--command-timeout-seconds",
        type=float,
        default=3600.0,
        help="timeout applied separately to every Git, build, and benchmark command",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="required output directory for report.json, summary.md, and raw logs",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="run deterministic pure-function unit tests without invoking Git or Cargo",
    )
    return parser


def run_self_tests() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(PureFunctionTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return EXIT_OK if result.wasSuccessful() else EXIT_ERROR


def run_comparison(args: argparse.Namespace) -> int:
    validate_args(args)
    output_dir = args.output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise BenchmarkError(
            f"--output-dir must be absent or empty so prior evidence is not overwritten: "
            f"{output_dir}"
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "report.json"
    summary_path = output_dir / "summary.md"
    run_log_root = output_dir / "raw"
    run_log_root.mkdir(parents=True, exist_ok=False)

    report: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "started_at": utc_now(),
        "finished_at": None,
        "output_path": str(output_path),
        "summary_path": str(summary_path),
        "log_root": str(run_log_root),
        "environment": environment_metadata(),
        "protocol": {
            "bench": "spawn",
            "benchmark_label": "spawn process",
            "initial_pairs": args.initial_pairs,
            "max_pairs": args.max_pairs,
            "sample_size_per_invocation": args.sample_size,
            "warm_up_seconds_per_invocation": args.warm_up_time,
            "measurement_seconds_per_invocation": args.measurement_time,
            "criterion_samples_per_revision_initial": args.sample_size * args.initial_pairs,
            "independent_statistical_unit": "adjacent outer base/head pair",
            "bootstrap": {
                "resamples": BOOTSTRAP_RESAMPLES,
                "seed": BOOTSTRAP_SEED,
                "confidence_level": 0.95,
                "statistic": "median paired log(head_slope/base_slope)",
            },
            "thresholds": {
                "warning_ratio": WARNING_RATIO,
                "relative_failure_ci_lower_ratio": RELATIVE_FAILURE_RATIO,
                "absolute_failure_head_median_ci_lower_us": ABSOLUTE_FAILURE_US,
                "maximum_ratio_ci_width": MAX_RATIO_CI_WIDTH,
            },
            "order_rule": "even pairs base/head; odd pairs head/base",
            "cargo_locked": True,
            "separate_target_directories": True,
            "separate_criterion_home_per_invocation": True,
        },
        "requested_revisions": {"base": args.base, "head": args.head},
        "revisions": {},
        "parity": {},
        "measurements": [],
        "statistics": None,
        "decision": {"status": "running", "exit_code": None, "reasons": []},
        "cleanup": {"attempted": False, "errors": []},
    }

    exit_code = EXIT_ERROR
    runner = CommandRunner(args.command_timeout_seconds)
    worktrees: Optional[WorktreePair] = None
    temporary_root: Optional[Path] = None
    caught_error: Optional[BaseException] = None

    try:
        git = shutil.which("git")
        cargo = shutil.which("cargo")
        rustc = shutil.which("rustc")
        if not git:
            raise BenchmarkError("git executable was not found on PATH")
        if not cargo:
            raise BenchmarkError("cargo executable was not found on PATH")
        if not rustc:
            raise BenchmarkError("rustc executable was not found on PATH")

        repo_candidate = args.repo.resolve()
        root_text = git_output(
            runner,
            git,
            repo_candidate,
            ["rev-parse", "--show-toplevel"],
        )
        repo_root = Path(root_text).resolve()
        report["repository"] = {
            "root": str(repo_root),
            "original_dirty_entries": dirty_status(runner, git, repo_root),
        }
        report["toolchain"] = toolchain_metadata(
            runner,
            cargo=cargo,
            rustc=rustc,
            toolchain=args.toolchain,
            cwd=repo_root,
        )

        base_commit = resolve_commit(runner, git, repo_root, args.base)
        head_commit = resolve_commit(runner, git, repo_root, args.head)
        if base_commit == head_commit:
            raise BenchmarkError(
                f"base and head resolve to the same commit {base_commit}; no comparison is possible"
            )
        report["resolved_revisions"] = {"base": base_commit, "head": head_commit}

        temporary_root = Path(tempfile.mkdtemp(prefix="lunatic-spawn-paired-"))
        worktrees = WorktreePair(
            runner=runner,
            git=git,
            repo=repo_root,
            temporary_root=temporary_root,
            cleanup_log=run_log_root / "cleanup.log",
        )
        paths = {
            "base": worktrees.add(
                "base", base_commit, run_log_root / "worktree-base-add.log"
            ),
            "head": worktrees.add(
                "head", head_commit, run_log_root / "worktree-head-add.log"
            ),
        }
        targets = {
            "base": temporary_root / "target-base",
            "head": temporary_root / "target-head",
        }

        parity_paths = (args.harness, args.fixture)
        parity_hashes: Dict[str, Dict[str, str]] = {"base": {}, "head": {}}
        for label in ("base", "head"):
            initial_dirty = dirty_status(runner, git, paths[label])
            if initial_dirty:
                raise BenchmarkError(
                    f"detached {label} worktree is unexpectedly dirty: {initial_dirty}"
                )
            description = commit_description(runner, git, paths[label])
            lock_path = paths[label] / "Cargo.lock"
            if not lock_path.is_file():
                raise BenchmarkError(f"{label} revision has no Cargo.lock: {lock_path}")
            report["revisions"][label] = {
                **description,
                "dirty_before": False,
                "dirty_before_entries": [],
                "cargo_lock_sha256": sha256_file(lock_path),
                "locked_packages": locked_package_versions(
                    lock_path, ("criterion", "wasmtime", "wasi-common")
                ),
            }
            for relative in parity_paths:
                candidate = paths[label] / relative
                if not candidate.is_file():
                    raise BenchmarkError(
                        f"required parity file {relative!r} is missing at {label} revision"
                    )
                parity_hashes[label][relative] = sha256_file(candidate)

        mismatches = [
            relative
            for relative in parity_paths
            if parity_hashes["base"][relative] != parity_hashes["head"][relative]
        ]
        report["parity"] = {
            "paths": list(parity_paths),
            "hash_algorithm": "sha256",
            "hashes": parity_hashes,
            "matched": not mismatches,
            "mismatches": mismatches,
        }
        if mismatches:
            details = "; ".join(
                f"{relative}: base={parity_hashes['base'][relative]}, "
                f"head={parity_hashes['head'][relative]}"
                for relative in mismatches
            )
            raise BenchmarkError(
                "benchmark harness/fixture parity check failed closed; " + details
            )

        for label in ("base", "head"):
            build_env = {
                "CARGO_INCREMENTAL": "0",
                "CARGO_TARGET_DIR": str(targets[label]),
            }
            runner.run(
                cargo_command(
                    cargo,
                    args.toolchain,
                    ["bench", "--locked", "--bench", "spawn", "--no-run"],
                ),
                cwd=paths[label],
                env_overrides=build_env,
                log_path=run_log_root / f"build-{label}.log",
            )

        slopes: Dict[str, List[float]] = {"base": [], "head": []}
        pair_index = 0
        final_ratio: Optional[BootstrapInterval] = None
        final_log_ratios: List[float] = []

        while pair_index < args.max_pairs:
            order = measurement_order(pair_index)
            pair_record: Dict[str, Any] = {
                "pair_index": pair_index,
                "order": list(order),
                "runs": {},
            }
            for sequence_index, label in enumerate(order):
                invocation_name = f"pair-{pair_index:02d}-{sequence_index}-{label}"
                criterion_home = run_log_root / "criterion" / invocation_name
                criterion_home.mkdir(parents=True, exist_ok=False)
                run_env = {
                    "CARGO_INCREMENTAL": "0",
                    "CARGO_TARGET_DIR": str(targets[label]),
                    "CRITERION_HOME": str(criterion_home),
                }
                command = cargo_command(
                    cargo,
                    args.toolchain,
                    [
                        "bench",
                        "--locked",
                        "--bench",
                        "spawn",
                        "--",
                        "--sample-size",
                        str(args.sample_size),
                        "--warm-up-time",
                        str(args.warm_up_time),
                        "--measurement-time",
                        str(args.measurement_time),
                        "--noplot",
                    ],
                )
                command_result = runner.run(
                    command,
                    cwd=paths[label],
                    env_overrides=run_env,
                    log_path=run_log_root / f"{invocation_name}.log",
                )
                estimates_path = locate_estimates(criterion_home, "base")
                slope_ns, estimates = parse_slope_point(estimates_path)
                slopes[label].append(slope_ns)
                pair_record["runs"][label] = {
                    "sequence_index": sequence_index,
                    "slope_point_ns": slope_ns,
                    "slope_point_us": slope_ns / 1000.0,
                    "criterion_slope": estimates["slope"],
                    "elapsed_seconds": command_result.elapsed_seconds,
                    "command_log": relative_path_or_string(
                        run_log_root / f"{invocation_name}.log", run_log_root
                    ),
                    "criterion_home": relative_path_or_string(criterion_home, run_log_root),
                    "estimates_json": relative_path_or_string(estimates_path, run_log_root),
                }
            pair_record["head_over_base_ratio"] = (
                pair_record["runs"]["head"]["slope_point_ns"]
                / pair_record["runs"]["base"]["slope_point_ns"]
            )
            pair_record["log_head_over_base_ratio"] = math.log(
                pair_record["head_over_base_ratio"]
            )
            report["measurements"].append(pair_record)
            pair_index += 1

            if pair_index >= args.initial_pairs:
                final_ratio, final_log_ratios = paired_ratio_interval(
                    slopes["base"], slopes["head"]
                )
                if final_ratio.width <= MAX_RATIO_CI_WIDTH:
                    break
                print(
                    f"[spawn-paired] ratio CI width {final_ratio.width:.4f} exceeds "
                    f"{MAX_RATIO_CI_WIDTH:.4f}; extending to pair {pair_index + 1} "
                    f"of {args.max_pairs}",
                    flush=True,
                )

        if final_ratio is None:
            final_ratio, final_log_ratios = paired_ratio_interval(
                slopes["base"], slopes["head"]
            )
        base_us = bootstrap_median_interval(
            [value / 1000.0 for value in slopes["base"]],
            seed=BOOTSTRAP_SEED + 1,
        )
        head_us = bootstrap_median_interval(
            [value / 1000.0 for value in slopes["head"]],
            seed=BOOTSTRAP_SEED + 2,
        )
        exhausted = pair_index >= args.max_pairs
        status, exit_code, reasons = classify_result(
            final_ratio,
            head_us,
            exhausted_pairs=exhausted,
        )
        report["statistics"] = {
            "pair_count": pair_index,
            "base_slope_ns": slopes["base"],
            "head_slope_ns": slopes["head"],
            "paired_log_ratios": final_log_ratios,
            "head_over_base_ratio": final_ratio.as_dict(),
            "slowdown_percent": {
                "point": (final_ratio.point - 1.0) * 100.0,
                "lower": (final_ratio.lower - 1.0) * 100.0,
                "upper": (final_ratio.upper - 1.0) * 100.0,
                "ci_width_percentage_points": final_ratio.width * 100.0,
            },
            "base_median_us": base_us.as_dict(),
            "head_median_us": head_us.as_dict(),
        }
        report["decision"] = {
            "status": status,
            "exit_code": exit_code,
            "reasons": reasons,
        }

        for label in ("base", "head"):
            after_entries = dirty_status(runner, git, paths[label])
            report["revisions"][label]["dirty_after"] = bool(after_entries)
            report["revisions"][label]["dirty_after_entries"] = after_entries
            if after_entries:
                raise BenchmarkError(
                    f"{label} worktree became dirty during measurement: {after_entries}"
                )

        summary = (
            f"base median {base_us.point:.3f} us; head median {head_us.point:.3f} us; "
            f"paired ratio {final_ratio.point:.4f} "
            f"(95% CI {final_ratio.lower:.4f}..{final_ratio.upper:.4f}); "
            f"decision={status}"
        )
        report["summary"] = summary
        print(f"[spawn-paired] {summary}")
        if status == "warning":
            print(f"::warning ::{'; '.join(reasons)}")
        elif exit_code != EXIT_OK:
            print(f"::error ::{'; '.join(reasons)}", file=sys.stderr)

    except KeyboardInterrupt as error:
        caught_error = error
        exit_code = 130
        report["decision"] = {
            "status": "interrupted",
            "exit_code": exit_code,
            "reasons": ["benchmark comparison interrupted by the user"],
        }
    except BaseException as error:  # Preserve a partial evidence report for every failure.
        caught_error = error
        exit_code = EXIT_ERROR
        report["decision"] = {
            "status": "error",
            "exit_code": exit_code,
            "reasons": [str(error)],
        }
        report["error"] = {
            "type": type(error).__name__,
            "message": str(error),
            "traceback": traceback.format_exc(),
        }
        print(f"::error ::spawn paired benchmark failed: {error}", file=sys.stderr)
    finally:
        cleanup_errors: List[str] = []
        if worktrees is not None:
            report["cleanup"]["attempted"] = True
            cleanup_errors = worktrees.cleanup()
        elif temporary_root is not None:
            report["cleanup"]["attempted"] = True
            try:
                shutil.rmtree(temporary_root)
            except OSError as error:
                cleanup_errors.append(
                    f"failed to remove temporary root {temporary_root}: {error}"
                )
        report["cleanup"]["errors"] = cleanup_errors
        if cleanup_errors:
            previous = dict(report["decision"])
            exit_code = EXIT_ERROR
            report["decision"] = {
                "status": "cleanup_error",
                "exit_code": exit_code,
                "reasons": cleanup_errors,
                "previous": previous,
            }
            print(
                "::error ::spawn benchmark cleanup failed: " + "; ".join(cleanup_errors),
                file=sys.stderr,
            )
        report["finished_at"] = utc_now()
        try:
            atomic_write_json(output_path, report)
            atomic_write_text(summary_path, markdown_summary(report))
            print(f"[spawn-paired] JSON report: {output_path}")
            print(f"[spawn-paired] Markdown summary: {summary_path}")
            print(f"[spawn-paired] raw logs: {run_log_root}")
        except OSError as write_error:
            print(f"failed to write JSON report {output_path}: {write_error}", file=sys.stderr)
            exit_code = EXIT_ERROR

    if caught_error is not None and isinstance(caught_error, KeyboardInterrupt):
        return 130
    return exit_code


class PureFunctionTests(unittest.TestCase):
    def test_cli_contract_and_defaults(self) -> None:
        args = build_parser().parse_args(
            ["base-sha", "head-sha", "--output-dir", "evidence"]
        )
        validate_args(args)
        self.assertEqual(args.base, "base-sha")
        self.assertEqual(args.head, "head-sha")
        self.assertEqual(args.toolchain, "1.95.0")
        self.assertEqual(args.initial_pairs, 10)
        self.assertEqual(args.max_pairs, 15)
        self.assertEqual(args.sample_size, 50)
        self.assertEqual(args.warm_up_time, 1.0)
        self.assertEqual(args.measurement_time, 3.0)

    def test_measurement_order_alternates(self) -> None:
        self.assertEqual(measurement_order(0), ("base", "head"))
        self.assertEqual(measurement_order(1), ("head", "base"))
        self.assertEqual(measurement_order(2), ("base", "head"))

    def test_bootstrap_is_reproducible(self) -> None:
        first = bootstrap_median_interval([1.0, 2.0, 3.0], seed=7, resamples=500)
        second = bootstrap_median_interval([1.0, 2.0, 3.0], seed=7, resamples=500)
        self.assertEqual(first, second)

    def test_command_timeout_terminates_the_process_group(self) -> None:
        result = CommandRunner(0.2).run(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            cwd=Path.cwd(),
            check=False,
            announce=False,
        )
        self.assertEqual(result.returncode, 124)
        self.assertIn("command timed out", result.stderr)
        self.assertLess(result.elapsed_seconds, 10.0)

    def test_constant_relative_regression_hard_fails(self) -> None:
        base = [50_000.0] * 10
        head = [60_000.0] * 10
        ratio, _ = paired_ratio_interval(base, head, resamples=500)
        head_us = bootstrap_median_interval(
            [value / 1000.0 for value in head], seed=8, resamples=500
        )
        status, code, _ = classify_result(ratio, head_us, exhausted_pairs=False)
        self.assertEqual(status, "hard_fail_relative")
        self.assertEqual(code, EXIT_REGRESSION)

    def test_warning_is_nonzero_effect_but_success_exit(self) -> None:
        ratio = BootstrapInterval(point=1.06, lower=0.99, upper=1.09)
        head = BootstrapInterval(point=53.0, lower=50.0, upper=56.0)
        status, code, _ = classify_result(ratio, head, exhausted_pairs=False)
        self.assertEqual(status, "warning")
        self.assertEqual(code, EXIT_OK)

    def test_absolute_lower_bound_hard_fails(self) -> None:
        ratio = BootstrapInterval(point=1.0, lower=0.99, upper=1.01)
        head = BootstrapInterval(point=80.0, lower=78.0, upper=82.0)
        status, code, _ = classify_result(ratio, head, exhausted_pairs=False)
        self.assertEqual(status, "hard_fail_absolute")
        self.assertEqual(code, EXIT_REGRESSION)

    def test_wide_interval_is_inconclusive_only_when_exhausted(self) -> None:
        ratio = BootstrapInterval(point=1.0, lower=0.94, upper=1.06)
        head = BootstrapInterval(point=50.0, lower=45.0, upper=55.0)
        status, code, _ = classify_result(ratio, head, exhausted_pairs=True)
        self.assertEqual(status, "inconclusive_noise")
        self.assertEqual(code, EXIT_INCONCLUSIVE)

    def test_decisive_wide_interval_remains_a_hard_failure(self) -> None:
        ratio = BootstrapInterval(point=1.20, lower=1.11, upper=1.25)
        head = BootstrapInterval(point=80.0, lower=78.0, upper=82.0)
        status, code, _ = classify_result(ratio, head, exhausted_pairs=True)
        self.assertEqual(status, "hard_fail_relative_and_absolute")
        self.assertEqual(code, EXIT_REGRESSION)

    def test_markdown_summary_handles_operational_error(self) -> None:
        report = {
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": "2026-01-01T00:00:01Z",
            "decision": {"status": "error", "exit_code": EXIT_ERROR, "reasons": ["boom"]},
            "protocol": {},
            "environment": {},
            "revisions": {},
        }
        summary = markdown_summary(report)
        self.assertIn("**error**", summary)
        self.assertIn("boom", summary)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.self_test:
        return run_self_tests()
    try:
        return run_comparison(args)
    except BenchmarkError as error:
        # Argument validation can fail before the evidence report is initialized.
        print(f"error: {error}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
