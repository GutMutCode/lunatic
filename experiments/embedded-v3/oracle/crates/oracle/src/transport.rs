use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde::Serialize;
use thiserror::Error;

use crate::automaton::{Oracle, OracleError, RequestPhase};
use crate::evidence::OrderedTraceSink;
use crate::protocol::{
    decode_event_line, ControlEnvelope, ControlMessage, EventEnvelope, RequestId,
};

const TRACE_SCHEMA_VERSION: u32 = 1;
pub const MAX_NDJSON_LINE_BYTES: usize = 1_048_576;
const MAX_STDOUT_BUFFERED_LINES: usize = 256;
const MAX_STDERR_BYTES: usize = 65_536;
const MAX_STDERR_LINES: usize = 1_024;

#[derive(Debug, Clone)]
pub struct TimeoutTable {
    pub hello: Duration,
    pub init: Duration,
    pub create_step: Duration,
    pub command_admission: Duration,
    pub command_terminal: Duration,
    pub fault_step: Duration,
    pub rollout_step: Duration,
    pub snapshot: Duration,
    pub teardown: Duration,
    pub shutdown: Duration,
}

impl Default for TimeoutTable {
    fn default() -> Self {
        Self {
            hello: Duration::from_secs(30),
            init: Duration::from_secs(30),
            create_step: Duration::from_secs(2),
            command_admission: Duration::from_millis(100),
            command_terminal: Duration::from_millis(250),
            fault_step: Duration::from_millis(500),
            rollout_step: Duration::from_secs(2),
            snapshot: Duration::from_millis(250),
            teardown: Duration::from_secs(2),
            shutdown: Duration::from_secs(2),
        }
    }
}

impl TimeoutTable {
    pub fn for_phase(&self, message: &ControlMessage, phase: RequestPhase) -> Duration {
        match (message, phase) {
            (ControlMessage::Hello(_), _) => self.hello,
            (ControlMessage::Init(_), _) => self.init,
            (ControlMessage::CreateTenant(_), RequestPhase::Pending) => Duration::from_millis(100),
            (ControlMessage::CreateTenant(_), _) => self.create_step,
            (ControlMessage::Command(_), RequestPhase::Pending) => self.command_admission,
            (ControlMessage::Command(_), _) => self.command_terminal,
            (ControlMessage::InjectFault(fault), RequestPhase::Pending) => match fault.fault {
                crate::protocol::FaultKind::Trap(_) => Duration::from_millis(250),
                crate::protocol::FaultKind::CpuHog(_) => Duration::from_millis(20),
            },
            (ControlMessage::InjectFault(_), _) => self.fault_step,
            (ControlMessage::Rollout(_), RequestPhase::Pending) => Duration::from_millis(500),
            (ControlMessage::Rollout(_), _) => self.rollout_step,
            (ControlMessage::Snapshot(_), _) => self.snapshot,
            (ControlMessage::SetDequeueGate(_), _) => self.snapshot,
            (ControlMessage::AuthorityProbe(_), _) => Duration::from_secs(1),
            (ControlMessage::Quiesce(_), _) => self.teardown,
            (ControlMessage::TeardownTenant(_), _) => self.teardown,
            (ControlMessage::Shutdown(_), _) => self.shutdown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SentControl {
    pub request_id: RequestId,
    pub sent_at_ns: u64,
    pub raw_line: String,
}

#[derive(Debug, Clone)]
pub struct ReceivedEvent {
    pub envelope: EventEnvelope,
    pub received_at_ns: u64,
    pub raw_line: String,
}

#[derive(Debug)]
pub enum SentOrEvent {
    Sent(SentControl),
    Event(ReceivedEvent),
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("candidate I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("candidate emitted invalid event JSON: {source}; line={line:?}")]
    InvalidEventJson {
        line: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("candidate stdout closed before the expected event")]
    StdoutClosed,
    #[error("candidate stdout reader failed: {0}")]
    StdoutReader(String),
    #[error("timed out waiting for candidate stdout")]
    Timeout,
    #[error("candidate did not exit within {0:?}")]
    ExitTimeout(Duration),
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Oracle(#[from] OracleError),
}

#[derive(Clone)]
pub(crate) struct MonotonicClock {
    inner: Arc<ClockInner>,
}

struct ClockInner {
    origin: Instant,
    last: AtomicU64,
}

impl MonotonicClock {
    fn new() -> Self {
        Self {
            inner: Arc::new(ClockInner {
                origin: Instant::now(),
                last: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn now_ns(&self) -> u64 {
        let raw = self
            .inner
            .origin
            .elapsed()
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        let mut seen = self.inner.last.load(Ordering::Relaxed);
        loop {
            let next = raw.max(seen.saturating_add(1));
            match self.inner.last.compare_exchange_weak(
                seen,
                next,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(actual) => seen = actual,
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct TraceSink {
    ordered: OrderedTraceSink,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StreamTraceRecord<'a> {
    Stdin {
        trace_schema_version: u32,
        monotonic_ns: u64,
        line: &'a str,
    },
    Stdout {
        trace_schema_version: u32,
        monotonic_ns: u64,
        line: &'a str,
    },
    Stderr {
        trace_schema_version: u32,
        monotonic_ns: u64,
        line: &'a str,
    },
    ProcessExit {
        trace_schema_version: u32,
        monotonic_ns: u64,
        code: Option<i32>,
        success: bool,
    },
}

impl TraceSink {
    fn create(path: &Path) -> io::Result<Self> {
        Ok(Self {
            ordered: OrderedTraceSink::create(path).map_err(io::Error::other)?,
        })
    }

    pub(crate) fn record<T: Serialize>(&self, record: &T) -> io::Result<()> {
        self.ordered
            .record(record)
            .map(|_| ())
            .map_err(io::Error::other)
    }

    fn finalize(&self) -> io::Result<()> {
        self.ordered
            .finalize()
            .map(|_| ())
            .map_err(io::Error::other)
    }

    fn finalize_if_open(&self) -> io::Result<()> {
        if self.ordered.is_finalized().map_err(io::Error::other)? {
            Ok(())
        } else {
            self.finalize()
        }
    }

    fn stdin(&self, monotonic_ns: u64, line: &str) -> io::Result<()> {
        self.record(&StreamTraceRecord::Stdin {
            trace_schema_version: TRACE_SCHEMA_VERSION,
            monotonic_ns,
            line,
        })
    }

    fn stdout(&self, monotonic_ns: u64, line: &str) -> io::Result<()> {
        self.record(&StreamTraceRecord::Stdout {
            trace_schema_version: TRACE_SCHEMA_VERSION,
            monotonic_ns,
            line,
        })
    }

    fn stderr(&self, monotonic_ns: u64, line: &str) -> io::Result<()> {
        self.record(&StreamTraceRecord::Stderr {
            trace_schema_version: TRACE_SCHEMA_VERSION,
            monotonic_ns,
            line,
        })
    }

    fn process_exit(&self, monotonic_ns: u64, status: ExitStatus) -> io::Result<()> {
        self.record(&StreamTraceRecord::ProcessExit {
            trace_schema_version: TRACE_SCHEMA_VERSION,
            monotonic_ns,
            code: status.code(),
            success: status.success(),
        })
    }
}

enum StdoutItem {
    Line { received_at_ns: u64, line: String },
    Eof,
    Error(String),
}

pub struct CandidateProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout_rx: mpsc::Receiver<StdoutItem>,
    stderr_lines: Arc<Mutex<Vec<String>>>,
    readers: Vec<JoinHandle<()>>,
    clock: MonotonicClock,
    trace: TraceSink,
    causal_gate: Arc<Mutex<()>>,
    exit_recorded: bool,
}

impl CandidateProcess {
    pub fn spawn(
        mut command: Command,
        trace_path: impl AsRef<Path>,
    ) -> Result<Self, TransportError> {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("candidate stdin was not piped"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("candidate stdout was not piped"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("candidate stderr was not piped"))?;

        let clock = MonotonicClock::new();
        let trace = TraceSink::create(trace_path.as_ref())?;
        let causal_gate = Arc::new(Mutex::new(()));
        let (stdout_tx, stdout_rx) = mpsc::sync_channel(MAX_STDOUT_BUFFERED_LINES);
        let stdout_reader = spawn_stdout_reader(
            stdout,
            stdout_tx.clone(),
            clock.clone(),
            causal_gate.clone(),
            trace.clone(),
        );
        let stderr_lines = Arc::new(Mutex::new(Vec::new()));
        let stderr_reader = spawn_stderr_reader(
            stderr,
            stderr_lines.clone(),
            stdout_tx,
            clock.clone(),
            trace.clone(),
        );

        Ok(Self {
            child,
            stdin: Some(stdin),
            stdout_rx,
            stderr_lines,
            readers: vec![stdout_reader, stderr_reader],
            clock,
            trace,
            causal_gate,
            exit_recorded: false,
        })
    }

    pub fn id(&self) -> u32 {
        self.child.id()
    }

    pub fn send_control(
        &mut self,
        control: &ControlEnvelope,
    ) -> Result<SentControl, TransportError> {
        self.send_controls(std::slice::from_ref(control))?
            .pop()
            .ok_or_else(|| io::Error::other("single-control batch returned no receipt").into())
    }

    /// Writes a group of already-registered controls under one causal gate and
    /// one flush. Candidate stdout cannot be sequenced ahead of any stdin trace
    /// in the group, which gives paired liveness probes one transport window.
    pub fn send_controls(
        &mut self,
        controls: &[ControlEnvelope],
    ) -> Result<Vec<SentControl>, TransportError> {
        let raw_lines = controls
            .iter()
            .map(|control| {
                let raw_line = serde_json::to_string(control).map_err(io::Error::other)?;
                if raw_line.len() > MAX_NDJSON_LINE_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "control NDJSON line exceeds hard byte limit",
                    ));
                }
                Ok(raw_line)
            })
            .collect::<Result<Vec<_>, io::Error>>()?;
        let causal_gate = self.causal_gate.clone();
        let _causal_guard = causal_gate
            .lock()
            .map_err(|_| io::Error::other("causal gate mutex poisoned"))?;
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "candidate stdin closed"))?;
        for raw_line in &raw_lines {
            stdin.write_all(raw_line.as_bytes())?;
            stdin.write_all(b"\n")?;
        }
        stdin.flush()?;
        controls
            .iter()
            .zip(raw_lines)
            .map(|(control, raw_line)| {
                let sent_at_ns = self.clock.now_ns();
                self.trace.stdin(sent_at_ns, &raw_line)?;
                Ok(SentControl {
                    request_id: control.request_id,
                    sent_at_ns,
                    raw_line,
                })
            })
            .collect()
    }

    /// Atomically consumes an already-sequenced stdout event or writes one
    /// control. The causal gate closes the check/write race used by workload
    /// decisions that must stop issuing work at an observed terminal.
    pub fn send_control_or_receive(
        &mut self,
        control: &ControlEnvelope,
    ) -> Result<SentOrEvent, TransportError> {
        let causal_gate = self.causal_gate.clone();
        let _causal_guard = causal_gate
            .lock()
            .map_err(|_| io::Error::other("causal gate mutex poisoned"))?;
        match self.stdout_rx.try_recv() {
            Ok(StdoutItem::Line {
                received_at_ns,
                line,
            }) => {
                let envelope = decode_event_line(&line).map_err(|source| {
                    TransportError::InvalidEventJson {
                        line: line.clone(),
                        source,
                    }
                })?;
                return Ok(SentOrEvent::Event(ReceivedEvent {
                    envelope,
                    received_at_ns,
                    raw_line: line,
                }));
            }
            Err(mpsc::TryRecvError::Empty) => {}
            Ok(StdoutItem::Eof) | Err(mpsc::TryRecvError::Disconnected) => {
                return Err(TransportError::StdoutClosed)
            }
            Ok(StdoutItem::Error(message)) => return Err(TransportError::StdoutReader(message)),
        }

        let raw_line = serde_json::to_string(control).map_err(io::Error::other)?;
        if raw_line.len() > MAX_NDJSON_LINE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "control NDJSON line exceeds hard byte limit",
            )
            .into());
        }
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "candidate stdin closed"))?;
        stdin.write_all(raw_line.as_bytes())?;
        stdin.write_all(b"\n")?;
        stdin.flush()?;
        let sent_at_ns = self.clock.now_ns();
        self.trace.stdin(sent_at_ns, &raw_line)?;
        Ok(SentOrEvent::Sent(SentControl {
            request_id: control.request_id,
            sent_at_ns,
            raw_line,
        }))
    }

    pub fn receive_event(&mut self, timeout: Duration) -> Result<ReceivedEvent, TransportError> {
        match self.stdout_rx.recv_timeout(timeout) {
            Ok(StdoutItem::Line {
                received_at_ns,
                line,
            }) => {
                let envelope = decode_event_line(&line).map_err(|source| {
                    TransportError::InvalidEventJson {
                        line: line.clone(),
                        source,
                    }
                })?;
                Ok(ReceivedEvent {
                    envelope,
                    received_at_ns,
                    raw_line: line,
                })
            }
            Ok(StdoutItem::Eof) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(TransportError::StdoutClosed)
            }
            Ok(StdoutItem::Error(error)) => Err(TransportError::StdoutReader(error)),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(TransportError::Timeout),
        }
    }

    pub fn try_receive_event(&mut self) -> Result<Option<ReceivedEvent>, TransportError> {
        match self.stdout_rx.try_recv() {
            Ok(StdoutItem::Line {
                received_at_ns,
                line,
            }) => {
                let envelope = decode_event_line(&line).map_err(|source| {
                    TransportError::InvalidEventJson {
                        line: line.clone(),
                        source,
                    }
                })?;
                Ok(Some(ReceivedEvent {
                    envelope,
                    received_at_ns,
                    raw_line: line,
                }))
            }
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Ok(StdoutItem::Eof) | Err(mpsc::TryRecvError::Disconnected) => {
                Err(TransportError::StdoutClosed)
            }
            Ok(StdoutItem::Error(message)) => Err(TransportError::StdoutReader(message)),
        }
    }

    pub fn stderr_lines(&self) -> Vec<String> {
        self.stderr_lines
            .lock()
            .map(|lines| lines.clone())
            .unwrap_or_else(|_| vec!["<stderr capture mutex poisoned>".into()])
    }

    pub fn close_stdin(&mut self) {
        self.stdin.take();
    }

    pub fn wait_for_exit(&mut self, timeout: Duration) -> Result<ExitStatus, TransportError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                self.join_readers();
                self.record_exit(status)?;
                self.trace.finalize_if_open()?;
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(TransportError::ExitTimeout(timeout));
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    pub(crate) fn measurement_parts(&self) -> (MonotonicClock, TraceSink) {
        (self.clock.clone(), self.trace.clone())
    }

    fn record_exit(&mut self, status: ExitStatus) -> Result<(), TransportError> {
        if !self.exit_recorded {
            self.trace.process_exit(self.clock.now_ns(), status)?;
            self.exit_recorded = true;
        }
        Ok(())
    }

    fn join_readers(&mut self) {
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}

impl Drop for CandidateProcess {
    fn drop(&mut self) {
        self.stdin.take();
        let status = match self.child.try_wait() {
            Ok(Some(status)) => Some(status),
            Ok(None) => {
                let _ = self.child.kill();
                self.child.wait().ok()
            }
            Err(_) => None,
        };
        self.join_readers();
        if let Some(status) = status {
            let _ = self.record_exit(status);
        }
        let _ = self.trace.finalize_if_open();
    }
}

pub struct OracleSession {
    process: CandidateProcess,
    oracle: Oracle,
    timeouts: TimeoutTable,
    controls: HashMap<RequestId, ControlMessage>,
}

impl OracleSession {
    pub fn new(process: CandidateProcess, timeouts: TimeoutTable) -> Self {
        Self {
            process,
            oracle: Oracle::new(),
            timeouts,
            controls: HashMap::new(),
        }
    }

    pub fn oracle(&self) -> &Oracle {
        &self.oracle
    }

    pub fn process(&self) -> &CandidateProcess {
        &self.process
    }

    pub fn process_mut(&mut self) -> &mut CandidateProcess {
        &mut self.process
    }

    pub fn request_phase(&self, request_id: RequestId) -> Option<RequestPhase> {
        self.oracle.phase(request_id)
    }

    pub fn request_timeout(&self, request_id: RequestId, phase: RequestPhase) -> Option<Duration> {
        self.controls
            .get(&request_id)
            .map(|control| self.timeouts.for_phase(control, phase))
    }

    pub fn expire_request(&mut self, request_id: RequestId) -> Result<(), SessionError> {
        self.oracle.expire_request(request_id)?;
        Ok(())
    }

    pub fn send(&mut self, control: &ControlEnvelope) -> Result<SentControl, SessionError> {
        self.oracle.register_control(control)?;
        self.controls
            .insert(control.request_id, control.message.clone());
        Ok(self.process.send_control(control)?)
    }

    pub fn send_batch(
        &mut self,
        controls: &[ControlEnvelope],
    ) -> Result<Vec<SentControl>, SessionError> {
        for control in controls {
            self.oracle.register_control(control)?;
            self.controls
                .insert(control.request_id, control.message.clone());
        }
        Ok(self.process.send_controls(controls)?)
    }

    pub fn send_or_receive(
        &mut self,
        control: &ControlEnvelope,
    ) -> Result<SentOrEvent, SessionError> {
        match self.process.send_control_or_receive(control) {
            Ok(SentOrEvent::Sent(sent)) => {
                self.oracle.register_control(control)?;
                self.controls
                    .insert(control.request_id, control.message.clone());
                Ok(SentOrEvent::Sent(sent))
            }
            Ok(SentOrEvent::Event(event)) => {
                self.oracle.observe_event(&event.envelope)?;
                Ok(SentOrEvent::Event(event))
            }
            Err(error @ TransportError::InvalidEventJson { .. }) => {
                let detail = error.to_string();
                let _ = self.oracle.poison(detail);
                Err(SessionError::Transport(error))
            }
            Err(error) => Err(SessionError::Transport(error)),
        }
    }

    pub fn receive(&mut self, timeout: Duration) -> Result<ReceivedEvent, SessionError> {
        match self.process.receive_event(timeout) {
            Ok(event) => {
                self.oracle.observe_event(&event.envelope)?;
                Ok(event)
            }
            Err(error @ TransportError::InvalidEventJson { .. }) => {
                let detail = error.to_string();
                let _ = self.oracle.poison(detail);
                Err(SessionError::Transport(error))
            }
            Err(error) => Err(SessionError::Transport(error)),
        }
    }

    pub fn try_receive(&mut self) -> Result<Option<ReceivedEvent>, SessionError> {
        match self.process.try_receive_event() {
            Ok(Some(event)) => {
                self.oracle.observe_event(&event.envelope)?;
                Ok(Some(event))
            }
            Ok(None) => Ok(None),
            Err(error @ TransportError::InvalidEventJson { .. }) => {
                let detail = error.to_string();
                let _ = self.oracle.poison(detail);
                Err(SessionError::Transport(error))
            }
            Err(error) => Err(SessionError::Transport(error)),
        }
    }

    /// Sends one request and drives its phase-specific timeout automaton.
    /// Events belonging to other outstanding requests remain validated, but do
    /// not reset this request's phase deadline.
    pub fn round_trip(
        &mut self,
        control: &ControlEnvelope,
    ) -> Result<Vec<ReceivedEvent>, SessionError> {
        self.round_trip_timed(control).map(|(_, events)| events)
    }

    pub fn round_trip_timed(
        &mut self,
        control: &ControlEnvelope,
    ) -> Result<(SentControl, Vec<ReceivedEvent>), SessionError> {
        let sent = self.send(control)?;
        let mut events = Vec::new();
        let mut phase = self
            .oracle
            .phase(control.request_id)
            .expect("registered request has a phase");
        let mut deadline = Instant::now() + self.timeouts.for_phase(&control.message, phase);

        while !self.oracle.is_terminal(control.request_id) {
            let now = Instant::now();
            if now >= deadline {
                self.oracle.expire_request(control.request_id)?;
                return Err(SessionError::Transport(TransportError::Timeout));
            }
            let event = match self
                .process
                .receive_event(deadline.saturating_duration_since(now))
            {
                Ok(event) => event,
                Err(TransportError::Timeout) => {
                    self.oracle.expire_request(control.request_id)?;
                    return Err(SessionError::Transport(TransportError::Timeout));
                }
                Err(error @ TransportError::InvalidEventJson { .. }) => {
                    let detail = error.to_string();
                    let _ = self.oracle.poison(detail);
                    return Err(SessionError::Transport(error));
                }
                Err(error) => return Err(SessionError::Transport(error)),
            };
            self.oracle.observe_event(&event.envelope)?;
            events.push(event);

            let new_phase = self
                .oracle
                .phase(control.request_id)
                .expect("registered request has a phase");
            if new_phase != phase && new_phase != RequestPhase::Terminal {
                phase = new_phase;
                deadline = Instant::now() + self.timeouts.for_phase(&control.message, phase);
            }
        }
        Ok((sent, events))
    }
}

fn spawn_stdout_reader(
    stdout: impl Read + Send + 'static,
    tx: mpsc::SyncSender<StdoutItem>,
    clock: MonotonicClock,
    causal_gate: Arc<Mutex<()>>,
    trace: TraceSink,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        loop {
            match read_ndjson_line(&mut reader) {
                Ok(Some(line)) => {
                    let causal_guard = match causal_gate.lock() {
                        Ok(guard) => guard,
                        Err(_) => {
                            let _ =
                                tx.try_send(StdoutItem::Error("causal gate mutex poisoned".into()));
                            return;
                        }
                    };
                    let received_at_ns = clock.now_ns();
                    if let Err(error) = trace.stdout(received_at_ns, &line) {
                        let _ = tx.try_send(StdoutItem::Error(error.to_string()));
                        return;
                    }
                    if tx
                        .send(StdoutItem::Line {
                            received_at_ns,
                            line,
                        })
                        .is_err()
                    {
                        return;
                    }
                    drop(causal_guard);
                }
                Ok(None) => {
                    let _ = tx.send(StdoutItem::Eof);
                    return;
                }
                Err(error) => {
                    let _ = tx.send(StdoutItem::Error(error.to_string()));
                    return;
                }
            }
        }
    })
}

fn spawn_stderr_reader(
    stderr: impl Read + Send + 'static,
    captured: Arc<Mutex<Vec<String>>>,
    error_tx: mpsc::SyncSender<StdoutItem>,
    clock: MonotonicClock,
    trace: TraceSink,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        loop {
            match read_ndjson_line(&mut reader) {
                Ok(Some(line)) => {
                    let received_at_ns = clock.now_ns();
                    if let Err(error) = trace.stderr(received_at_ns, &line) {
                        let _ = error_tx.send(StdoutItem::Error(error.to_string()));
                        return;
                    }
                    if let Ok(mut lines) = captured.lock() {
                        let used = lines.iter().map(|value| value.len() + 1).sum::<usize>();
                        if lines.len() >= MAX_STDERR_LINES
                            || used.saturating_add(line.len() + 1) > MAX_STDERR_BYTES
                        {
                            drop(lines);
                            let _ = error_tx
                                .send(StdoutItem::Error("stderr capture limit exceeded".into()));
                            return;
                        }
                        lines.push(line);
                    } else {
                        let _ = error_tx
                            .send(StdoutItem::Error("stderr capture mutex poisoned".into()));
                        return;
                    }
                }
                Ok(None) => return,
                Err(error) => {
                    let _ =
                        error_tx.send(StdoutItem::Error(format!("stderr reader failed: {error}")));
                    return;
                }
            }
        }
    })
}

fn read_ndjson_line(reader: &mut impl BufRead) -> io::Result<Option<String>> {
    let mut bytes = Vec::with_capacity(4_096);
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unterminated NDJSON line",
            ));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.unwrap_or(available.len());
        if bytes.len().saturating_add(take) > MAX_NDJSON_LINE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "NDJSON line exceeds hard byte limit",
            ));
        }
        bytes.extend_from_slice(&available[..take]);
        reader.consume(take + usize::from(newline.is_some()));
        if newline.is_some() {
            break;
        }
    }
    if bytes.last() == Some(&b'\r') {
        bytes.pop();
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}
