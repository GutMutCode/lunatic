use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::PathBuf;

use embedded_v3_oracle::analysis::{decide_verdict, VerdictInput};

fn main() -> Result<(), Box<dyn Error>> {
    let input_path = parse_input_path()?;
    let bytes = fs::read(input_path)?;
    let input: VerdictInput = serde_json::from_slice(&bytes)?;
    let verdict = decide_verdict(&input)?;
    println!("{}", serde_json::to_string_pretty(&verdict)?);
    Ok(())
}

fn parse_input_path() -> Result<PathBuf, io::Error> {
    let mut arguments = env::args_os().skip(1);
    let flag = arguments
        .next()
        .ok_or_else(|| invalid("usage: embedded-v3-verdict --input <path>"))?;
    if flag != "--input" {
        return Err(invalid("usage: embedded-v3-verdict --input <path>"));
    }
    let path = arguments
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| invalid("--input needs a path"))?;
    if arguments.next().is_some() {
        return Err(invalid("unexpected arguments after --input <path>"));
    }
    Ok(path)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
