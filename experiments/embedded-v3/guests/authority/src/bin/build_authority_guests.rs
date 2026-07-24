use std::env;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use embedded_v3_authority_guests::{build_all, Parameters};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut output = root.join("artifacts");
    let mut parameters = Parameters::default();
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("missing value after {flag}"))?;
        match flag.as_str() {
            "--output" => output = PathBuf::from(value),
            "--nonce" => parameters.nonce = value,
            "--wasi-preopen-fd" => parameters.wasi_preopen_fd = value.parse()?,
            "--wasi-read-path" => parameters.wasi_read_path = value,
            "--wasi-mutate-path" => parameters.wasi_mutate_path = value,
            "--wasi-create-path" => parameters.wasi_create_path = value,
            "--sqlite-path" => parameters.sqlite_path = value,
            "--observer-ipv4" => parameters.observer_ipv4 = value.parse::<Ipv4Addr>()?,
            "--tcp-port" => parameters.tcp_port = value.parse()?,
            "--udp-port" => parameters.udp_port = value.parse()?,
            "--http-port" => parameters.http_port = value.parse()?,
            "--http-path" => parameters.http_path = value,
            _ => return Err(format!("unknown option: {flag}").into()),
        }
    }
    let manifest = build_all(root, &output, &parameters)?;
    println!("{}", manifest.display());
    Ok(())
}
