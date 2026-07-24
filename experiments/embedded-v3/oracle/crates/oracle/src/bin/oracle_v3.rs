use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use embedded_v3_oracle::protocol::*;
use embedded_v3_oracle::sampling::{ProcessSampler, SamplerStatus};
use embedded_v3_oracle::workload::{
    build_full_plan, business_result_digest, verify_full_shape, DeadlineTracker,
    ImmutableRunDirectory, ScenarioDocument, BLOCK_COUNT, EMBEDDED_SCENARIO_SHA256,
};
use embedded_v3_oracle::{
    CandidateProcess, OracleSession, ReceivedEvent, SentControl, TimeoutTable,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

const NORMAL_PAYLOAD_BYTES: usize = 64;
const TENANT_A_SHA256: &str = "9bce3d46eceef79abe98d88d16c2a8ac75ec1ffd68a7f328cc34a9c1a1fae0d1";

struct Arguments {
    candidate: CandidateIdentity,
    block: u32,
    program: PathBuf,
    run_root: PathBuf,
    run_id: String,
    program_args: Vec<String>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct VerticalSummary {
    schema_version: u32,
    run_id: String,
    candidate: CandidateIdentity,
    scenario_sha256: String,
    controls_sent: u64,
    tenants_created: u32,
    delayed_duplicate_verified: bool,
    back_to_back_fault_and_sibling_verified: bool,
    replacement_verified: bool,
    old_incarnation_stale_verified: bool,
    new_incarnation_fresh_then_deduplicated_verified: bool,
    teardown_old_endpoint_missing_verified: bool,
    passed: bool,
}

struct VerticalRunner {
    session: OracleSession,
    next_request_id: u64,
    controls_sent: u64,
}

impl VerticalRunner {
    fn new(session: OracleSession) -> Self {
        Self {
            session,
            next_request_id: 1,
            controls_sent: 0,
        }
    }

    fn envelope(&mut self, message: ControlMessage) -> ControlEnvelope {
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .expect("vertical request_id space cannot overflow");
        ControlEnvelope::new(request_id, message)
    }

    fn round_trip(
        &mut self,
        message: ControlMessage,
    ) -> Result<Vec<ReceivedEvent>, Box<dyn Error>> {
        let control = self.envelope(message);
        self.controls_sent += 1;
        Ok(self.session.round_trip(&control)?)
    }

    fn send(
        &mut self,
        message: ControlMessage,
    ) -> Result<(ControlEnvelope, SentControl), Box<dyn Error>> {
        let control = self.envelope(message);
        self.controls_sent += 1;
        let sent = self.session.send(&control)?;
        Ok((control, sent))
    }

    fn command(
        tenant_id: u32,
        incarnation: u64,
        command_id: u64,
        operation: CommandOperation,
        payload: Vec<u8>,
    ) -> ControlMessage {
        let payload_sha256 = sha256_hex(&payload);
        ControlMessage::Command(CommandControl {
            tenant_id: TenantId(tenant_id),
            incarnation: IncarnationToken::new(incarnation),
            command_id: CommandId(command_id),
            operation,
            payload,
            payload_sha256,
        })
    }

    fn finish(self) -> OracleSession {
        self.session
    }
}

fn parse_args() -> Result<Arguments, io::Error> {
    let mut args = env::args().skip(1);
    let mut candidate = None;
    let mut block = None;
    let mut program = None;
    let mut run_root = None;
    let mut run_id = None;
    let mut program_args = Vec::new();

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--candidate" => {
                let value = args
                    .next()
                    .ok_or_else(|| invalid("--candidate needs a value"))?;
                candidate = Some(if value == "fake" {
                    CandidateIdentity::fake_test_double()
                } else {
                    CandidateIdentity::Production(
                        value.parse().map_err(|error: String| invalid(error))?,
                    )
                });
            }
            "--block" => {
                let value = args
                    .next()
                    .ok_or_else(|| invalid("--block needs a value"))?
                    .parse::<u32>()
                    .map_err(|_| invalid("--block must be an unsigned integer"))?;
                if value >= BLOCK_COUNT {
                    return Err(invalid(format!(
                        "--block must be in 0..{}",
                        BLOCK_COUNT - 1
                    )));
                }
                block = Some(value);
            }
            "--program" => {
                program = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| invalid("--program needs a path"))?,
                ));
            }
            "--run-root" => {
                run_root = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| invalid("--run-root needs a path"))?,
                ));
            }
            "--run-id" => {
                run_id = Some(
                    args.next()
                        .ok_or_else(|| invalid("--run-id needs a value"))?,
                );
            }
            "--" => {
                program_args.extend(args);
                break;
            }
            "-h" | "--help" => {
                println!(
                    "usage: embedded-v3-oracle --candidate <fake|lunatic|extism|raw-wasmtime> \\\n                     --block <0..9> --program <adapter-executable> \\\n                     --run-root <directory> --run-id <id> [-- <adapter args>...]"
                );
                std::process::exit(0);
            }
            other => return Err(invalid(format!("unknown argument {other:?}"))),
        }
    }

    Ok(Arguments {
        candidate: candidate.ok_or_else(|| invalid("missing --candidate"))?,
        block: block.ok_or_else(|| invalid("missing --block"))?,
        program: program.ok_or_else(|| invalid("missing --program"))?,
        run_root: run_root.ok_or_else(|| invalid("missing --run-root"))?,
        run_id: run_id.ok_or_else(|| invalid("missing --run-id"))?,
        program_args,
    })
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = parse_args()?;
    let scenario = ScenarioDocument::embedded()?;
    let full_plan = build_full_plan(&scenario, arguments.block)?;
    let full_shape = verify_full_shape(&full_plan)?;
    if full_shape.total_actions == 0 {
        return Err("frozen full-shape plan is empty".into());
    }

    let run = ImmutableRunDirectory::create(&arguments.run_root, &arguments.run_id)?;
    let mut command = Command::new(&arguments.program);
    command.args(&arguments.program_args);
    let process = CandidateProcess::spawn(command, run.raw_trace_path())?;
    let sampler = ProcessSampler::start(&process);
    let session = OracleSession::new(process, TimeoutTable::default());
    let mut runner = VerticalRunner::new(session);

    runner.round_trip(ControlMessage::Hello(HelloControl {
        oracle_name: "embedded-v3-oracle".into(),
        expected_candidate: arguments.candidate,
    }))?;
    runner.round_trip(ControlMessage::Init(InitControl {
        run_id: arguments.run_id.clone(),
        tenant_capacity: 32,
    }))?;

    for tenant_id in [0, 1] {
        runner.round_trip(ControlMessage::CreateTenant(CreateTenantControl {
            tenant_id: TenantId(tenant_id),
            incarnation: IncarnationToken::new(0),
            logical_version: "A".into(),
            build_id: "A".into(),
            artifact_sha256: TENANT_A_SHA256.into(),
        }))?;
    }

    let normal_payload = vec![0x5a; NORMAL_PAYLOAD_BYTES];
    let large_exact_command_id = 9_007_199_254_740_993_u64;
    let fresh = runner.round_trip(VerticalRunner::command(
        0,
        0,
        large_exact_command_id,
        CommandOperation::Increment(IncrementOperation { delta: 1 }),
        normal_payload.clone(),
    ))?;
    validate_completed(&fresh, 0, 0, 1, false, "A", "A")?;

    let duplicate = runner.round_trip(VerticalRunner::command(
        0,
        0,
        large_exact_command_id,
        CommandOperation::Increment(IncrementOperation { delta: 1 }),
        normal_payload.clone(),
    ))?;
    validate_completed(&duplicate, 0, 0, 1, true, "A", "A")?;

    let seed_command_id = 2_000_001;
    let seed_payload = vec![0x2d; NORMAL_PAYLOAD_BYTES];
    let seed = runner.round_trip(VerticalRunner::command(
        0,
        0,
        seed_command_id,
        CommandOperation::Increment(IncrementOperation { delta: 1 }),
        seed_payload.clone(),
    ))?;
    validate_completed(&seed, 0, 0, 2, false, "A", "A")?;

    let (fault_control, fault_sent) =
        runner.send(ControlMessage::InjectFault(InjectFaultControl {
            fault_id: DecimalU64(1),
            tenant_id: TenantId(0),
            incarnation: IncarnationToken::new(0),
            expected_replacement_incarnation: IncarnationToken::new(1),
            replacement_logical_version: "A".into(),
            replacement_build_id: "A".into(),
            replacement_artifact_sha256: TENANT_A_SHA256.into(),
            fault: FaultKind::Trap(TrapFault::default()),
        }))?;
    let (sibling_control, sibling_sent) = runner.send(VerticalRunner::command(
        1,
        0,
        2_000_000,
        CommandOperation::Read(ReadOperation::default()),
        normal_payload.clone(),
    ))?;

    let concurrent = drive_fault_and_sibling(
        &mut runner.session,
        &fault_control,
        &fault_sent,
        &sibling_control,
        &sibling_sent,
    )?;
    validate_trap_sequence(&concurrent, fault_control.request_id)?;
    let sibling_events = events_for(&concurrent, sibling_control.request_id);
    validate_completed(&sibling_events, 0, 0, 0, false, "A", "A")?;

    let stale = runner.round_trip(VerticalRunner::command(
        0,
        0,
        seed_command_id,
        CommandOperation::Increment(IncrementOperation { delta: 1 }),
        seed_payload.clone(),
    ))?;
    validate_rejection(&stale, RejectionReason::StaleIncarnation)?;

    let replacement_fresh = runner.round_trip(VerticalRunner::command(
        0,
        1,
        seed_command_id,
        CommandOperation::Increment(IncrementOperation { delta: 1 }),
        seed_payload.clone(),
    ))?;
    validate_completed(&replacement_fresh, 1, 1, 1, false, "A", "A")?;
    let replacement_duplicate = runner.round_trip(VerticalRunner::command(
        0,
        1,
        seed_command_id,
        CommandOperation::Increment(IncrementOperation { delta: 1 }),
        seed_payload,
    ))?;
    validate_completed(&replacement_duplicate, 1, 1, 1, true, "A", "A")?;

    runner.round_trip(ControlMessage::TeardownTenant(TeardownTenantControl {
        tenant_id: TenantId(1),
        incarnation: IncarnationToken::new(0),
    }))?;
    let missing = runner.round_trip(VerticalRunner::command(
        1,
        0,
        9_000_001,
        CommandOperation::Read(ReadOperation::default()),
        normal_payload,
    ))?;
    validate_rejection(&missing, RejectionReason::TenantMissing)?;

    runner.round_trip(ControlMessage::Shutdown(ShutdownControl::default()))?;
    let controls_sent = runner.controls_sent;
    let mut session = runner.finish();
    let sampler_status = sampler.stop();
    if let SamplerStatus::Invalid(failure) = sampler_status {
        return Err(format!("external process sampler invalid: {failure:?}").into());
    }
    session.process_mut().close_stdin();
    let exit = session
        .process_mut()
        .wait_for_exit(Duration::from_secs(2))?;
    if !exit.success() {
        return Err(format!("candidate exited with {exit}").into());
    }

    let summary = VerticalSummary {
        schema_version: 3,
        run_id: arguments.run_id.clone(),
        candidate: arguments.candidate,
        scenario_sha256: EMBEDDED_SCENARIO_SHA256.into(),
        controls_sent,
        tenants_created: 2,
        delayed_duplicate_verified: true,
        back_to_back_fault_and_sibling_verified: true,
        replacement_verified: true,
        old_incarnation_stale_verified: true,
        new_incarnation_fresh_then_deduplicated_verified: true,
        teardown_old_endpoint_missing_verified: true,
        passed: true,
    };
    let summary_path = run.write_summary(&summary)?;
    println!(
        "candidate={} vertical-v3 passed; evidence={}",
        arguments.candidate,
        summary_path.display()
    );
    Ok(())
}

fn drive_fault_and_sibling(
    session: &mut OracleSession,
    fault: &ControlEnvelope,
    fault_sent: &SentControl,
    sibling: &ControlEnvelope,
    sibling_sent: &SentControl,
) -> Result<Vec<ReceivedEvent>, Box<dyn Error>> {
    let mut deadlines = DeadlineTracker::default();
    deadlines.register(fault.request_id.0, fault_sent.sent_at_ns, 500_000_000)?;
    deadlines.register(sibling.request_id.0, sibling_sent.sent_at_ns, 100_000_000)?;
    let expected = BTreeSet::from([fault.request_id, sibling.request_id]);
    let mut terminal = BTreeSet::new();
    let mut events = Vec::new();

    while terminal != expected {
        let event = session.receive(Duration::from_millis(600))?;
        let expired = deadlines.expire(event.received_at_ns);
        if !expired.is_empty() {
            return Err(format!("concurrent request deadlines expired: {expired:?}").into());
        }
        if event.envelope.request_id == sibling.request_id
            && matches!(event.envelope.message, EventMessage::CommandAccepted(_))
        {
            deadlines.accepted(sibling.request_id.0, event.received_at_ns, 250_000_000)?;
        }
        let request_id = event.envelope.request_id;
        if session.oracle().is_terminal(request_id) && terminal.insert(request_id) {
            deadlines.terminal(request_id.0)?;
        }
        events.push(event);
    }
    Ok(events)
}

fn events_for(events: &[ReceivedEvent], request_id: RequestId) -> Vec<ReceivedEvent> {
    events
        .iter()
        .filter(|event| event.envelope.request_id == request_id)
        .cloned()
        .collect()
}

fn validate_trap_sequence(
    events: &[ReceivedEvent],
    request_id: RequestId,
) -> Result<(), Box<dyn Error>> {
    let fault_events: Vec<_> = events
        .iter()
        .filter(|event| event.envelope.request_id == request_id)
        .map(|event| &event.envelope.message)
        .collect();
    if fault_events
        .iter()
        .any(|event| matches!(event, EventMessage::ExecutionStarted(_)))
    {
        return Err("trap emitted forbidden execution_started".into());
    }
    let failed = fault_events.iter().any(|event| {
        matches!(
            event,
            EventMessage::ExecutionFailed(ExecutionFailedEvent {
                fault_id: DecimalU64(1),
                reason: FaultFailureReason::GuestTrap,
                ..
            })
        )
    });
    let observed = fault_events.iter().any(|event| {
        matches!(
            event,
            EventMessage::FailureObserved(FailureObservedEvent {
                fault_id: DecimalU64(1),
                ..
            })
        )
    });
    let ready = fault_events.iter().any(|event| {
        matches!(
            event,
            EventMessage::TenantReady(TenantReadyEvent {
                tenant_id: TenantId(0),
                incarnation,
            }) if incarnation.value() == 1
        )
    });
    if failed && observed && ready {
        Ok(())
    } else {
        Err("trap sequence lacked failed/observed/replacement-ready evidence".into())
    }
}

fn validate_completed(
    events: &[ReceivedEvent],
    incarnation: u64,
    generation: u64,
    counter: i64,
    deduplicated: bool,
    logical_version: &str,
    build_id: &str,
) -> Result<(), Box<dyn Error>> {
    let results: Vec<_> = events
        .iter()
        .filter_map(|event| match &event.envelope.message {
            EventMessage::CommandCompleted(completed) => Some(&completed.result),
            _ => None,
        })
        .collect();
    if results.len() != 1 {
        return Err(format!("expected one completed result, observed {}", results.len()).into());
    }
    let result = results[0];
    let expected_digest = business_result_digest(generation, counter);
    if result.incarnation.value() != incarnation
        || result.generation.0 != generation
        || result.counter != counter
        || result.deduplicated != deduplicated
        || result.logical_version != logical_version
        || result.build_id != build_id
        || result.business_result_sha256 != expected_digest
    {
        return Err(format!("forged or unexpected command result: {result:?}").into());
    }
    Ok(())
}

fn validate_rejection(
    events: &[ReceivedEvent],
    expected: RejectionReason,
) -> Result<(), Box<dyn Error>> {
    let reasons: Vec<_> = events
        .iter()
        .filter_map(|event| match &event.envelope.message {
            EventMessage::CommandRejected(rejected) => Some(rejected.reason),
            _ => None,
        })
        .collect();
    if reasons == [expected] {
        Ok(())
    } else {
        Err(format!("expected rejection {expected:?}, observed {reasons:?}").into())
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_scenario_builds_verified_full_shape() {
        let scenario = ScenarioDocument::embedded().expect("frozen scenario must parse");
        let plan = build_full_plan(&scenario, 0).expect("frozen scenario must build");
        let summary = verify_full_shape(&plan).expect("full plan shape must be exact");
        assert_eq!(summary.normal_commands, 10_240);
        assert_eq!(summary.delayed_duplicate_groups, 80);
    }

    #[test]
    fn payload_digest_is_stable() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn primary_candidates_parse() {
        assert_eq!("extism".parse(), Ok(CandidateKind::Extism));
        assert_eq!("raw-wasmtime".parse(), Ok(CandidateKind::RawWasmtime));
    }
}
