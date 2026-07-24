use std::env;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::Duration;

use embedded_v3_oracle::authority_harness::AuthorityHarness;
use embedded_v3_oracle::full_runner::WorkloadRunner;
use embedded_v3_oracle::measurement::stable_anchor;
use embedded_v3_oracle::protocol::CandidateKind;
use embedded_v3_oracle::sampling::{ProcessSampler, SamplerStatus};
use embedded_v3_oracle::workload::{
    authority_nonce, build_full_plan, verify_full_shape, ImmutableRunDirectory, ScenarioDocument,
};
use embedded_v3_oracle::{CandidateProcess, OracleSession, TimeoutTable};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Through {
    Updates,
    Cleanup,
}

struct Arguments {
    candidate: CandidateKind,
    block: u32,
    program: PathBuf,
    run_root: PathBuf,
    run_id: String,
    through: Through,
}

#[derive(Serialize)]
struct CoreSmokeSummary {
    schema_version: u32,
    run_id: String,
    candidate: CandidateKind,
    block: u32,
    through: String,
    controls_sent: u64,
    events_received: u64,
    accepted_commands: u64,
    terminal_accepted_commands: u64,
    update_worker_logical_operations: u64,
    update_worker_transport_attempts: u64,
    update_worker_retries: u64,
    warm_create_samples: usize,
    normal_samples: usize,
    failed_rollout_samples: usize,
    valid_rollout_samples: usize,
    failed_mixed_samples: usize,
    valid_mixed_samples: usize,
    authority_canaries: usize,
    cleanup_rss_anchors: usize,
    process_samples: usize,
    passed: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = parse_args()?;
    let scenario = ScenarioDocument::embedded()?;
    let plan = build_full_plan(&scenario, arguments.block)?;
    verify_full_shape(&plan)?;

    let run = ImmutableRunDirectory::create(&arguments.run_root, &arguments.run_id)?;
    let authority_nonce = authority_nonce(
        "embedded-v3-core-smoke",
        arguments.block,
        arguments.candidate,
        &arguments.run_id,
    )?;
    let authority = AuthorityHarness::bind_with_nonce(run.path(), authority_nonce)?;
    let policy_descriptor = format!("embedded-v3-core-smoke-policy-v1:{:?}", arguments.candidate);
    let policy_sha256 = format!("{:x}", Sha256::digest(policy_descriptor.as_bytes()));
    let process = CandidateProcess::spawn(Command::new(&arguments.program), run.raw_trace_path())?;
    let sampler = ProcessSampler::start(&process);
    let session = OracleSession::new(process, TimeoutTable::default());
    let mut runner = WorkloadRunner::new(session, arguments.candidate, arguments.block)?;

    let hello_terminal = runner.hello()?;
    thread::sleep(Duration::from_millis(230));
    let _minimal_anchor = stable_anchor(hello_terminal)?;

    let init_terminal = runner.init(&arguments.run_id)?;
    thread::sleep(Duration::from_millis(230));
    let _shared_anchor = stable_anchor(init_terminal)?;

    let ready_terminal = runner.run_initial_create(&plan)?;
    thread::sleep(Duration::from_millis(230));
    let _ready_anchor = stable_anchor(ready_terminal)?;

    runner.run_normal_pressure(&plan)?;
    runner.run_faults(&plan)?;
    runner.run_updates(&plan)?;
    let authority_evidence = runner.run_authority(&plan, &authority, &policy_sha256)?;
    runner.run_main_teardown(&plan)?;
    let cleanup = if arguments.through == Through::Cleanup {
        runner.run_cleanup(&plan)?
    } else {
        Vec::new()
    };
    runner.shutdown()?;

    let (mut session, model, timings, counts) = runner.into_parts();
    if model.active_tenants() != 0 {
        return Err("core smoke ended with active tenants".into());
    }
    let (sampler_status, samples) = sampler.stop_with_samples();
    if let SamplerStatus::Invalid(failure) = sampler_status {
        return Err(format!("process sampler invalid: {failure:?}").into());
    }
    session.process_mut().close_stdin();
    let exit = session
        .process_mut()
        .wait_for_exit(Duration::from_secs(2))?;
    if !exit.success() {
        return Err(format!("candidate exited with {exit}").into());
    }

    let summary = CoreSmokeSummary {
        schema_version: 3,
        run_id: arguments.run_id.clone(),
        candidate: arguments.candidate,
        block: arguments.block,
        through: if arguments.through == Through::Cleanup {
            "cleanup".to_owned()
        } else {
            "updates".to_owned()
        },
        controls_sent: counts.controls_sent,
        events_received: counts.events_received,
        accepted_commands: counts.accepted_commands,
        terminal_accepted_commands: counts.terminal_accepted_commands,
        update_worker_logical_operations: counts.update_worker_logical_operations,
        update_worker_transport_attempts: counts.update_worker_transport_attempts,
        update_worker_retries: counts.update_worker_retries,
        warm_create_samples: timings.warm_create_ns.len(),
        normal_samples: timings.normal_ns.len(),
        failed_rollout_samples: timings.failed_rollout_ns.len(),
        valid_rollout_samples: timings.valid_rollout_ns.len(),
        failed_mixed_samples: timings.failed_mixed_window_ns.len(),
        valid_mixed_samples: timings.valid_mixed_window_ns.len(),
        authority_canaries: authority_evidence.len(),
        cleanup_rss_anchors: cleanup.len(),
        process_samples: samples.len(),
        passed: true,
    };
    let summary_path = run.write_summary(&summary)?;
    println!("core smoke passed: {}", summary_path.display());
    Ok(())
}

fn parse_args() -> Result<Arguments, io::Error> {
    let mut values = env::args().skip(1);
    let mut candidate = None;
    let mut block = None;
    let mut program = None;
    let mut run_root = None;
    let mut run_id = None;
    let mut through = Through::Updates;
    while let Some(argument) = values.next() {
        match argument.as_str() {
            "--candidate" => {
                candidate = Some(
                    values
                        .next()
                        .ok_or_else(|| invalid("--candidate needs a value"))?
                        .parse()
                        .map_err(|error: String| invalid(error))?,
                );
            }
            "--block" => {
                block = Some(
                    values
                        .next()
                        .ok_or_else(|| invalid("--block needs a value"))?
                        .parse()
                        .map_err(|_| invalid("--block must be u32"))?,
                );
            }
            "--program" => {
                program = Some(PathBuf::from(
                    values
                        .next()
                        .ok_or_else(|| invalid("--program needs a value"))?,
                ));
            }
            "--run-root" => {
                run_root = Some(PathBuf::from(
                    values
                        .next()
                        .ok_or_else(|| invalid("--run-root needs a value"))?,
                ));
            }
            "--run-id" => {
                run_id = Some(
                    values
                        .next()
                        .ok_or_else(|| invalid("--run-id needs a value"))?,
                );
            }
            "--through" => {
                through = match values
                    .next()
                    .ok_or_else(|| invalid("--through needs updates|cleanup"))?
                    .as_str()
                {
                    "updates" => Through::Updates,
                    "cleanup" => Through::Cleanup,
                    _ => return Err(invalid("--through needs updates|cleanup")),
                };
            }
            other => return Err(invalid(format!("unknown argument {other}"))),
        }
    }
    Ok(Arguments {
        candidate: candidate.ok_or_else(|| invalid("missing --candidate"))?,
        block: block.ok_or_else(|| invalid("missing --block"))?,
        program: program.ok_or_else(|| invalid("missing --program"))?,
        run_root: run_root.ok_or_else(|| invalid("missing --run-root"))?,
        run_id: run_id.ok_or_else(|| invalid("missing --run-id"))?,
        through,
    })
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
