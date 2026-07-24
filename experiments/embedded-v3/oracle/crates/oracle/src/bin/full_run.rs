use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::PathBuf;

use embedded_v3_oracle::full_evidence::{execute_full_run, FullRunConfiguration};

fn main() -> Result<(), Box<dyn Error>> {
    let config_path = parse_config_path()?;
    let bytes = fs::read(&config_path)?;
    let configuration: FullRunConfiguration = serde_json::from_slice(&bytes)?;
    let report = execute_full_run(configuration)?;
    println!("{}", serde_json::to_string(&report)?);
    if !report.completed {
        return Err("candidate run was incomplete; raw-summary.json contains the failure".into());
    }
    Ok(())
}

fn parse_config_path() -> Result<PathBuf, io::Error> {
    let mut arguments = env::args_os().skip(1);
    let flag = arguments
        .next()
        .ok_or_else(|| invalid("usage: embedded-v3-full-run --config <path>"))?;
    if flag != "--config" {
        return Err(invalid("usage: embedded-v3-full-run --config <path>"));
    }
    let path = arguments
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| invalid("--config needs a path"))?;
    if arguments.next().is_some() {
        return Err(invalid("unexpected arguments after --config <path>"));
    }
    Ok(path)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
