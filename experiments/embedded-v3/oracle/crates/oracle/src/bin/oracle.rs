use std::env;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use embedded_v3_oracle::protocol::*;
use embedded_v3_oracle::sampling::ProcessSampler;
use embedded_v3_oracle::{CandidateProcess, OracleSession, TimeoutTable};

struct Arguments {
    candidate: CandidateKind,
    program: PathBuf,
    trace: PathBuf,
    program_args: Vec<String>,
}

fn parse_args() -> Result<Arguments, io::Error> {
    let mut args = env::args().skip(1);
    let mut candidate = None;
    let mut program = None;
    let mut trace = None;
    let mut program_args = Vec::new();

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--candidate" => {
                let value = args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "--candidate needs a value")
                })?;
                candidate =
                    Some(value.parse().map_err(|error: String| {
                        io::Error::new(io::ErrorKind::InvalidInput, error)
                    })?);
            }
            "--program" => {
                program = Some(PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "--program needs a path")
                })?));
            }
            "--trace" => {
                trace = Some(PathBuf::from(args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "--trace needs a path")
                })?));
            }
            "--" => {
                program_args.extend(args);
                break;
            }
            "-h" | "--help" => {
                println!(
                    "usage: embedded-v3-oracle --candidate <lunatic|extism|raw-wasmtime> \\\n+                     --program <adapter-executable> --trace <trace.ndjson> [-- <adapter args>...]"
                );
                std::process::exit(0);
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument {other:?}"),
                ));
            }
        }
    }

    Ok(Arguments {
        candidate: candidate
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing --candidate"))?,
        program: program
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing --program"))?,
        trace: trace
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing --trace"))?,
        program_args,
    })
}

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = parse_args()?;
    let mut command = Command::new(&arguments.program);
    command.args(&arguments.program_args);
    let process = CandidateProcess::spawn(command, &arguments.trace)?;
    let sampler = ProcessSampler::start(&process);
    let mut session = OracleSession::new(process, TimeoutTable::default());

    session.round_trip(&ControlEnvelope::new(
        1,
        ControlMessage::Hello(HelloControl {
            oracle_name: "embedded-v3-oracle".into(),
            expected_candidate: arguments.candidate.into(),
        }),
    ))?;
    session.round_trip(&ControlEnvelope::new(
        2,
        ControlMessage::Init(InitControl {
            run_id: "cli-smoke".into(),
            tenant_capacity: 32,
        }),
    ))?;
    session.round_trip(&ControlEnvelope::new(
        3,
        ControlMessage::Shutdown(ShutdownControl::default()),
    ))?;

    session.process_mut().close_stdin();
    let status = session
        .process_mut()
        .wait_for_exit(Duration::from_secs(2))?;
    sampler.stop();
    if !status.success() {
        return Err(format!("candidate exited with {status}").into());
    }
    println!(
        "candidate={} protocol smoke passed; trace={}",
        arguments.candidate,
        arguments.trace.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extism_is_a_primary_cli_candidate() {
        assert_eq!("extism".parse::<CandidateKind>(), Ok(CandidateKind::Extism));
        assert_eq!(CandidateKind::Extism.to_string(), "extism");
    }
}
