use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::transport::{CandidateProcess, MonotonicClock, TraceSink};

pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(10);
const TRACE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub creation_time_100ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessTreeSample {
    pub monotonic_ns: u64,
    pub root_pid: u32,
    pub root_creation_time_100ns: u64,
    pub process_count: u32,
    pub sampled_process_count: u32,
    pub inaccessible_process_count: u32,
    pub thread_count: u32,
    pub working_set_bytes: u64,
    pub private_usage_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SamplerFailureKind {
    Initialization,
    Sample,
    TraceRecord,
    WorkerPanic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SamplerFailure {
    pub kind: SamplerFailureKind,
    pub monotonic_ns: u64,
    pub root_pid: u32,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SamplerStatus {
    Running,
    Stopped,
    Invalid(SamplerFailure),
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SampleTraceRecord<'a> {
    ResourceSample {
        trace_schema_version: u32,
        #[serde(flatten)]
        sample: &'a ProcessTreeSample,
    },
    ResourceSampleError {
        trace_schema_version: u32,
        monotonic_ns: u64,
        root_pid: u32,
        error: &'a str,
    },
}

/// A 10 ms external sampler for a candidate and every descendant identity it
/// has observed. Sampling and trace-write failures are sticky and retrievable.
pub struct ProcessSampler {
    root_pid: u32,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<SamplerStatus>>,
    samples: Arc<Mutex<Vec<ProcessTreeSample>>>,
    worker: Option<JoinHandle<()>>,
}

impl ProcessSampler {
    pub fn start(process: &CandidateProcess) -> Self {
        let root_pid = process.id();
        let (clock, trace) = process.measurement_parts();
        Self::start_inner(root_pid, clock, trace)
    }

    fn start_inner(root_pid: u32, clock: MonotonicClock, trace: TraceSink) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let status = Arc::new(Mutex::new(SamplerStatus::Running));
        let samples = Arc::new(Mutex::new(Vec::new()));
        let worker_stop = stop.clone();
        let worker_status = status.clone();
        let worker_samples = samples.clone();
        let worker = thread::spawn(move || {
            let mut sampler = match PlatformTreeSampler::new(root_pid) {
                Ok(sampler) => sampler,
                Err(error) => {
                    let monotonic_ns = clock.now_ns();
                    record_sampling_failure(
                        &worker_status,
                        &trace,
                        SamplerFailure {
                            kind: SamplerFailureKind::Initialization,
                            monotonic_ns,
                            root_pid,
                            error: error.to_string(),
                        },
                    );
                    return;
                }
            };

            let mut next_tick = Instant::now();
            while !worker_stop.load(Ordering::Acquire) {
                let monotonic_ns = clock.now_ns();
                match sampler.sample(monotonic_ns) {
                    Ok(sample) => {
                        if let Err(error) = trace.record(&SampleTraceRecord::ResourceSample {
                            trace_schema_version: TRACE_SCHEMA_VERSION,
                            sample: &sample,
                        }) {
                            set_invalid(
                                &worker_status,
                                SamplerFailure {
                                    kind: SamplerFailureKind::TraceRecord,
                                    monotonic_ns,
                                    root_pid,
                                    error: error.to_string(),
                                },
                            );
                            return;
                        }
                        lock_samples(&worker_samples).push(sample);
                    }
                    Err(error) => {
                        record_sampling_failure(
                            &worker_status,
                            &trace,
                            SamplerFailure {
                                kind: SamplerFailureKind::Sample,
                                monotonic_ns,
                                root_pid,
                                error: error.to_string(),
                            },
                        );
                        return;
                    }
                }

                next_tick += SAMPLE_INTERVAL;
                let now = Instant::now();
                if next_tick > now {
                    thread::sleep(next_tick - now);
                } else {
                    next_tick = now;
                }
            }
            set_stopped(&worker_status);
        });
        Self {
            root_pid,
            stop,
            status,
            samples,
            worker: Some(worker),
        }
    }

    pub fn status(&self) -> SamplerStatus {
        lock_status(&self.status).clone()
    }

    pub fn failure(&self) -> Option<SamplerFailure> {
        match self.status() {
            SamplerStatus::Invalid(failure) => Some(failure),
            SamplerStatus::Running | SamplerStatus::Stopped => None,
        }
    }

    /// Returns a point-in-time copy of every successfully recorded sample.
    pub fn samples(&self) -> Vec<ProcessTreeSample> {
        lock_samples(&self.samples).clone()
    }

    /// Stops the sampler and returns both its sticky status and the complete
    /// sample history. This prevents the caller from racing the final tick.
    pub fn stop_with_samples(mut self) -> (SamplerStatus, Vec<ProcessTreeSample>) {
        self.stop_and_join();
        (self.status(), self.samples())
    }

    pub fn stop(mut self) -> SamplerStatus {
        self.stop_and_join();
        self.status()
    }

    fn stop_and_join(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                set_invalid(
                    &self.status,
                    SamplerFailure {
                        kind: SamplerFailureKind::WorkerPanic,
                        monotonic_ns: 0,
                        root_pid: self.root_pid,
                        error: "sampler worker panicked".into(),
                    },
                );
            }
        }
        set_stopped(&self.status);
    }
}

impl Drop for ProcessSampler {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

fn lock_status(status: &Mutex<SamplerStatus>) -> std::sync::MutexGuard<'_, SamplerStatus> {
    status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lock_samples(
    samples: &Mutex<Vec<ProcessTreeSample>>,
) -> std::sync::MutexGuard<'_, Vec<ProcessTreeSample>> {
    samples
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn set_invalid(status: &Mutex<SamplerStatus>, failure: SamplerFailure) {
    let mut status = lock_status(status);
    if !matches!(*status, SamplerStatus::Invalid(_)) {
        *status = SamplerStatus::Invalid(failure);
    }
}

fn set_stopped(status: &Mutex<SamplerStatus>) {
    let mut status = lock_status(status);
    if matches!(*status, SamplerStatus::Running) {
        *status = SamplerStatus::Stopped;
    }
}

fn record_sampling_failure(
    status: &Mutex<SamplerStatus>,
    trace: &TraceSink,
    failure: SamplerFailure,
) {
    let trace_result = trace.record(&SampleTraceRecord::ResourceSampleError {
        trace_schema_version: TRACE_SCHEMA_VERSION,
        monotonic_ns: failure.monotonic_ns,
        root_pid: failure.root_pid,
        error: &failure.error,
    });
    match trace_result {
        Ok(()) => set_invalid(status, failure),
        Err(trace_error) => set_invalid(
            status,
            SamplerFailure {
                kind: SamplerFailureKind::TraceRecord,
                monotonic_ns: failure.monotonic_ns,
                root_pid: failure.root_pid,
                error: format!(
                    "{}; recording resource_sample_error also failed: {trace_error}",
                    failure.error
                ),
            },
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessNode {
    pid: u32,
    parent_pid: u32,
    thread_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveProcess {
    identity: ProcessIdentity,
    node: ProcessNode,
}

#[derive(Debug)]
struct LineageTracker {
    root: ProcessIdentity,
    members: HashSet<ProcessIdentity>,
}

impl LineageTracker {
    fn new(root: ProcessIdentity) -> Self {
        Self {
            root,
            members: HashSet::from([root]),
        }
    }

    fn resolve_active<F>(
        &mut self,
        nodes: &[ProcessNode],
        mut identify: F,
    ) -> io::Result<Vec<ActiveProcess>>
    where
        F: FnMut(u32) -> io::Result<ProcessIdentity>,
    {
        let mut nodes_by_pid = HashMap::with_capacity(nodes.len());
        for node in nodes {
            if nodes_by_pid.insert(node.pid, *node).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("process snapshot contains duplicate PID {}", node.pid),
                ));
            }
        }

        let root_node = nodes_by_pid.get(&self.root.pid).copied().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("root process {} disappeared", self.root.pid),
            )
        })?;
        let mut identities = HashMap::new();
        let observed_root = cached_identity(self.root.pid, &mut identities, &mut identify)?;
        if observed_root != self.root {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "root PID {} was reused: expected creation {}, observed {}",
                    self.root.pid, self.root.creation_time_100ns, observed_root.creation_time_100ns
                ),
            ));
        }

        let mut active = HashMap::from([(
            self.root.pid,
            ActiveProcess {
                identity: self.root,
                node: root_node,
            },
        )]);
        let known_members: Vec<_> = self.members.iter().copied().collect();
        for identity in known_members {
            if identity == self.root {
                continue;
            }
            let Some(node) = nodes_by_pid.get(&identity.pid).copied() else {
                continue;
            };
            let observed = cached_identity(identity.pid, &mut identities, &mut identify)?;
            if observed == identity {
                active.insert(identity.pid, ActiveProcess { identity, node });
            }
        }

        loop {
            let mut changed = false;
            for node in nodes {
                if active.contains_key(&node.pid) {
                    continue;
                }
                let Some(parent_identity) =
                    active.get(&node.parent_pid).map(|parent| parent.identity)
                else {
                    continue;
                };
                let identity = cached_identity(node.pid, &mut identities, &mut identify)?;
                if identity.creation_time_100ns < parent_identity.creation_time_100ns {
                    continue;
                }
                active.insert(
                    node.pid,
                    ActiveProcess {
                        identity,
                        node: *node,
                    },
                );
                changed = true;
            }
            if !changed {
                break;
            }
        }

        self.members = active.values().map(|process| process.identity).collect();
        let mut active: Vec<_> = active.into_values().collect();
        active.sort_by_key(|process| (process.identity.pid, process.identity.creation_time_100ns));
        Ok(active)
    }
}

fn cached_identity<F>(
    pid: u32,
    identities: &mut HashMap<u32, ProcessIdentity>,
    identify: &mut F,
) -> io::Result<ProcessIdentity>
where
    F: FnMut(u32) -> io::Result<ProcessIdentity>,
{
    if let Some(identity) = identities.get(&pid) {
        return Ok(*identity);
    }
    let identity = identify(pid)?;
    if identity.pid != pid {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("identity query for PID {pid} returned PID {}", identity.pid),
        ));
    }
    identities.insert(pid, identity);
    Ok(identity)
}

#[cfg(windows)]
struct PlatformTreeSampler {
    root_pid: u32,
    lineage: LineageTracker,
}

#[cfg(windows)]
impl PlatformTreeSampler {
    fn new(root_pid: u32) -> io::Result<Self> {
        let root = query_process(root_pid)?;
        Ok(Self {
            root_pid,
            lineage: LineageTracker::new(root.identity),
        })
    }

    fn sample(&mut self, monotonic_ns: u64) -> io::Result<ProcessTreeSample> {
        let nodes = snapshot_processes()?;
        let mut measurements = HashMap::new();
        let active = self.lineage.resolve_active(&nodes, |pid| {
            let measurement = query_process(pid)?;
            let identity = measurement.identity;
            measurements.insert(pid, measurement);
            Ok(identity)
        })?;
        if active.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "process-tree sample selected zero processes",
            ));
        }

        let process_count = u32::try_from(active.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "process-tree process count exceeds u32",
            )
        })?;
        let mut thread_count = 0_u32;
        let mut working_set_bytes = 0_u64;
        let mut private_usage_bytes = 0_u64;
        for process in active {
            let measurement = measurements.get(&process.identity.pid).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "PID {} was selected without a measurement",
                        process.identity.pid
                    ),
                )
            })?;
            if measurement.identity != process.identity {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "PID {} changed identity during sampling",
                        process.identity.pid
                    ),
                ));
            }
            thread_count = thread_count
                .checked_add(process.node.thread_count)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "thread count overflow")
                })?;
            working_set_bytes = working_set_bytes
                .checked_add(measurement.working_set_bytes)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "working-set overflow")
                })?;
            private_usage_bytes = private_usage_bytes
                .checked_add(measurement.private_usage_bytes)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "private-usage overflow")
                })?;
        }
        if working_set_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "process-tree working-set sample is zero",
            ));
        }

        Ok(ProcessTreeSample {
            monotonic_ns,
            root_pid: self.root_pid,
            root_creation_time_100ns: self.lineage.root.creation_time_100ns,
            process_count,
            sampled_process_count: process_count,
            inaccessible_process_count: 0,
            thread_count,
            working_set_bytes,
            private_usage_bytes,
        })
    }
}

#[cfg(not(windows))]
struct PlatformTreeSampler;

#[cfg(not(windows))]
impl PlatformTreeSampler {
    fn new(_root_pid: u32) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the process-tree sampler is only implemented on Windows",
        ))
    }

    fn sample(&mut self, _monotonic_ns: u64) -> io::Result<ProcessTreeSample> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the process-tree sampler is only implemented on Windows",
        ))
    }
}

#[cfg(windows)]
#[derive(Debug, Clone, Copy)]
struct ProcessMeasurement {
    identity: ProcessIdentity,
    working_set_bytes: u64,
    private_usage_bytes: u64,
}

#[cfg(windows)]
fn snapshot_processes() -> io::Result<Vec<ProcessNode>> {
    use std::mem::size_of;

    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_NO_MORE_FILES, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Process32FirstW, Process32NextW, PROCESSENTRY32W,
        TH32CS_SNAPPROCESS,
    };

    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(with_context(
            io::Error::last_os_error(),
            "CreateToolhelp32Snapshot failed",
        ));
    }

    let result = (|| {
        let mut nodes = Vec::new();
        let mut entry = PROCESSENTRY32W {
            dwSize: size_of::<PROCESSENTRY32W>() as u32,
            ..Default::default()
        };
        if unsafe { Process32FirstW(snapshot, &mut entry) } == 0 {
            return Err(with_context(
                io::Error::last_os_error(),
                "Process32FirstW failed",
            ));
        }
        loop {
            nodes.push(ProcessNode {
                pid: entry.th32ProcessID,
                parent_pid: entry.th32ParentProcessID,
                thread_count: entry.cntThreads,
            });
            entry = PROCESSENTRY32W {
                dwSize: size_of::<PROCESSENTRY32W>() as u32,
                ..Default::default()
            };
            if unsafe { Process32NextW(snapshot, &mut entry) } == 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                    break;
                }
                return Err(with_context(error, "Process32NextW failed"));
            }
        }
        if nodes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Toolhelp returned a zero-process snapshot",
            ));
        }
        Ok(nodes)
    })();
    let close_result = unsafe { CloseHandle(snapshot) };
    if close_result == 0 {
        return Err(with_context(
            io::Error::last_os_error(),
            "CloseHandle(snapshot) failed",
        ));
    }
    result
}

#[cfg(windows)]
fn query_process(pid: u32) -> io::Result<ProcessMeasurement> {
    use std::mem::size_of;

    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME};
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS, PROCESS_MEMORY_COUNTERS_EX,
    };
    use windows_sys::Win32::System::Threading::{
        GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_VM_READ,
    };

    let handle =
        unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_VM_READ, 0, pid) };
    if handle.is_null() {
        return Err(with_context(
            io::Error::last_os_error(),
            format!("OpenProcess({pid}) failed"),
        ));
    }

    let result = (|| {
        let mut creation = FILETIME::default();
        let mut exit = FILETIME::default();
        let mut kernel = FILETIME::default();
        let mut user = FILETIME::default();
        if unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) } == 0
        {
            return Err(with_context(
                io::Error::last_os_error(),
                format!("GetProcessTimes({pid}) failed"),
            ));
        }

        let mut counters = PROCESS_MEMORY_COUNTERS_EX {
            cb: size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32,
            ..Default::default()
        };
        if unsafe {
            GetProcessMemoryInfo(
                handle,
                (&raw mut counters).cast::<PROCESS_MEMORY_COUNTERS>(),
                size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32,
            )
        } == 0
        {
            return Err(with_context(
                io::Error::last_os_error(),
                format!("GetProcessMemoryInfo({pid}) failed"),
            ));
        }
        Ok(ProcessMeasurement {
            identity: ProcessIdentity {
                pid,
                creation_time_100ns: filetime_u64(creation),
            },
            working_set_bytes: counters.WorkingSetSize as u64,
            private_usage_bytes: counters.PrivateUsage as u64,
        })
    })();
    let close_result = unsafe { CloseHandle(handle) };
    if close_result == 0 {
        return Err(with_context(
            io::Error::last_os_error(),
            format!("CloseHandle(process {pid}) failed"),
        ));
    }
    result
}

#[cfg(windows)]
fn filetime_u64(value: windows_sys::Win32::Foundation::FILETIME) -> u64 {
    (u64::from(value.dwHighDateTime) << 32) | u64::from(value.dwLowDateTime)
}

#[cfg(windows)]
fn with_context(error: io::Error, context: impl std::fmt::Display) -> io::Error {
    io::Error::new(error.kind(), format!("{context}: {error}"))
}

#[cfg(windows)]
pub fn sample_process_tree(root_pid: u32, monotonic_ns: u64) -> io::Result<ProcessTreeSample> {
    let mut sampler = PlatformTreeSampler::new(root_pid)?;
    sampler.sample(monotonic_ns)
}

#[cfg(not(windows))]
pub fn sample_process_tree(_root_pid: u32, _monotonic_ns: u64) -> io::Result<ProcessTreeSample> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "the process-tree sampler is only implemented on Windows",
    ))
}

pub fn nearest_rank(values: &[f64], percentile: f64) -> Option<f64> {
    if values.is_empty()
        || !percentile.is_finite()
        || !(0.0..=1.0).contains(&percentile)
        || values.iter().any(|value| !value.is_finite())
    {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = if percentile == 0.0 {
        1
    } else {
        (percentile * sorted.len() as f64).ceil() as usize
    };
    sorted.get(rank.saturating_sub(1)).copied()
}

pub fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        Some((sorted[middle - 1] + sorted[middle]) / 2.0)
    } else {
        Some(sorted[middle])
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LeastSquares {
    pub slope: f64,
    pub intercept: f64,
    pub r_squared: f64,
}

pub fn least_squares(x: &[f64], y: &[f64]) -> Option<LeastSquares> {
    if x.len() != y.len() || x.len() < 2 || x.iter().chain(y).any(|value| !value.is_finite()) {
        return None;
    }

    let count = x.len() as f64;
    let mean_x = x.iter().sum::<f64>() / count;
    let mean_y = y.iter().sum::<f64>() / count;
    let ss_x = x.iter().map(|value| (value - mean_x).powi(2)).sum::<f64>();
    if ss_x == 0.0 {
        return None;
    }
    let covariance = x
        .iter()
        .zip(y)
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>();
    let slope = covariance / ss_x;
    let intercept = mean_y - slope * mean_x;
    let ss_total = y.iter().map(|value| (value - mean_y).powi(2)).sum::<f64>();
    let ss_residual = x
        .iter()
        .zip(y)
        .map(|(x, y)| (y - (intercept + slope * x)).powi(2))
        .sum::<f64>();
    let r_squared = if ss_total == 0.0 {
        if ss_residual == 0.0 {
            1.0
        } else {
            0.0
        }
    } else {
        1.0 - ss_residual / ss_total
    };

    Some(LeastSquares {
        slope,
        intercept,
        r_squared,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(pid: u32, creation_time_100ns: u64) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            creation_time_100ns,
        }
    }

    fn node(pid: u32, parent_pid: u32) -> ProcessNode {
        ProcessNode {
            pid,
            parent_pid,
            thread_count: 1,
        }
    }

    fn resolve(
        tracker: &mut LineageTracker,
        nodes: &[ProcessNode],
        identities: &HashMap<u32, ProcessIdentity>,
    ) -> io::Result<Vec<ActiveProcess>> {
        tracker.resolve_active(nodes, |pid| {
            identities.get(&pid).copied().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("PID {pid} inaccessible"),
                )
            })
        })
    }

    #[test]
    fn lineage_retains_a_detached_grandchild_by_creation_identity() {
        let root = identity(1, 10);
        let child = identity(2, 20);
        let grandchild = identity(3, 30);
        let mut tracker = LineageTracker::new(root);

        let first = resolve(
            &mut tracker,
            &[node(1, 0), node(2, 1), node(3, 2)],
            &HashMap::from([(1, root), (2, child), (3, grandchild)]),
        )
        .unwrap();
        assert_eq!(first.len(), 3);

        let detached = resolve(
            &mut tracker,
            &[node(1, 0), node(3, 999)],
            &HashMap::from([(1, root), (3, grandchild)]),
        )
        .unwrap();
        assert_eq!(
            detached
                .iter()
                .map(|process| process.identity)
                .collect::<Vec<_>>(),
            vec![root, grandchild]
        );
    }

    #[test]
    fn lineage_does_not_transfer_membership_across_pid_reuse() {
        let root = identity(1, 10);
        let original_child = identity(2, 20);
        let reused_pid = identity(2, 21);
        let mut tracker = LineageTracker::new(root);

        resolve(
            &mut tracker,
            &[node(1, 0), node(2, 1)],
            &HashMap::from([(1, root), (2, original_child)]),
        )
        .unwrap();
        let unrelated_reuse = resolve(
            &mut tracker,
            &[node(1, 0), node(2, 999)],
            &HashMap::from([(1, root), (2, reused_pid)]),
        )
        .unwrap();
        assert_eq!(
            unrelated_reuse
                .iter()
                .map(|process| process.identity)
                .collect::<Vec<_>>(),
            vec![root]
        );

        let new_real_child = resolve(
            &mut tracker,
            &[node(1, 0), node(2, 1)],
            &HashMap::from([(1, root), (2, reused_pid)]),
        )
        .unwrap();
        assert_eq!(
            new_real_child
                .iter()
                .map(|process| process.identity)
                .collect::<Vec<_>>(),
            vec![root, reused_pid]
        );
    }

    #[test]
    fn lineage_rejects_a_child_older_than_its_reused_parent_pid() {
        let root = identity(1, 100);
        let old_orphan = identity(3, 50);
        let mut tracker = LineageTracker::new(root);
        let active = resolve(
            &mut tracker,
            &[node(1, 0), node(3, 1)],
            &HashMap::from([(1, root), (3, old_orphan)]),
        )
        .unwrap();
        assert_eq!(
            active
                .iter()
                .map(|process| process.identity)
                .collect::<Vec<_>>(),
            vec![root]
        );
    }

    #[test]
    fn lineage_treats_root_disappearance_and_selected_inaccessibility_as_invalid() {
        let root = identity(1, 10);
        let child = identity(2, 20);
        let mut tracker = LineageTracker::new(root);
        assert!(resolve(&mut tracker, &[], &HashMap::new()).is_err());

        let mut tracker = LineageTracker::new(root);
        let error = tracker
            .resolve_active(&[node(1, 0), node(2, 1)], |pid| match pid {
                1 => Ok(root),
                2 => Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied")),
                _ => Ok(child),
            })
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn sampler_failure_is_sticky() {
        let status = Mutex::new(SamplerStatus::Running);
        let first = SamplerFailure {
            kind: SamplerFailureKind::Sample,
            monotonic_ns: 10,
            root_pid: 1,
            error: "first".into(),
        };
        set_invalid(&status, first.clone());
        set_invalid(
            &status,
            SamplerFailure {
                kind: SamplerFailureKind::TraceRecord,
                monotonic_ns: 20,
                root_pid: 1,
                error: "second".into(),
            },
        );
        assert_eq!(*lock_status(&status), SamplerStatus::Invalid(first));
        set_stopped(&status);
        assert!(matches!(*lock_status(&status), SamplerStatus::Invalid(_)));
    }

    #[cfg(windows)]
    #[test]
    fn windows_live_sample_has_stable_identity_and_nonzero_ex_counters() {
        let pid = std::process::id();
        let sample = sample_process_tree(pid, 123).unwrap();
        assert_eq!(sample.monotonic_ns, 123);
        assert_eq!(sample.root_pid, pid);
        assert!(sample.root_creation_time_100ns > 0);
        assert!(sample.process_count > 0);
        assert_eq!(sample.sampled_process_count, sample.process_count);
        assert_eq!(sample.inaccessible_process_count, 0);
        assert!(sample.thread_count > 0);
        assert!(sample.working_set_bytes > 0);
        assert!(sample.private_usage_bytes > 0);
    }

    #[test]
    fn nearest_rank_uses_the_frozen_definition() {
        let values = [4.0, 1.0, 3.0, 2.0];
        assert_eq!(nearest_rank(&values, 0.0), Some(1.0));
        assert_eq!(nearest_rank(&values, 0.5), Some(2.0));
        assert_eq!(nearest_rank(&values, 0.99), Some(4.0));
        assert_eq!(nearest_rank(&[], 0.99), None);
        assert_eq!(nearest_rank(&values, 1.01), None);
    }

    #[test]
    fn median_handles_odd_and_even_samples() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), Some(2.5));
        assert_eq!(median(&[]), None);
    }

    #[test]
    fn least_squares_reports_slope_intercept_and_fit() {
        let fit = least_squares(&[0.0, 1.0, 2.0], &[1.0, 3.0, 5.0]).unwrap();
        assert!((fit.slope - 2.0).abs() < 1e-12);
        assert!((fit.intercept - 1.0).abs() < 1e-12);
        assert!((fit.r_squared - 1.0).abs() < 1e-12);
        assert!(least_squares(&[1.0, 1.0], &[2.0, 3.0]).is_none());
    }
}
