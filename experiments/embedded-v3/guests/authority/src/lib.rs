use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use serde::Serialize;
use sha2::{Digest, Sha256};

pub const ABI_VERSION: u32 = 1;
pub const PARAMETER_DOMAIN: &[u8] = b"lunatic.embedded-v3.authority.v1";
pub const PARAMETER_PTR: u32 = 1024;

const WASI_READ_TEMPLATE: &str = include_str!("../templates/wasi_p1_fs_read.wat.in");
const WASI_MUTATE_TEMPLATE: &str = include_str!("../templates/wasi_p1_fs_mutate.wat.in");
const LUNATIC_TCP_TEMPLATE: &str = include_str!("../templates/lunatic_tcp.wat.in");
const LUNATIC_UDP_TEMPLATE: &str = include_str!("../templates/lunatic_udp.wat.in");
const LUNATIC_SQLITE_TEMPLATE: &str = include_str!("../templates/lunatic_sqlite_create.wat.in");
const EXTISM_HTTP_TEMPLATE: &str = include_str!("../templates/extism_http.wat.in");

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Probe {
    WasiP1FsRead,
    WasiP1FsMutate,
    LunaticTcp,
    LunaticUdp,
    LunaticSqliteCreate,
    ExtismHttp,
}

impl Probe {
    pub const ALL: [Probe; 6] = [
        Probe::WasiP1FsRead,
        Probe::WasiP1FsMutate,
        Probe::LunaticTcp,
        Probe::LunaticUdp,
        Probe::LunaticSqliteCreate,
        Probe::ExtismHttp,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Probe::WasiP1FsRead => "wasi_p1_fs_read",
            Probe::WasiP1FsMutate => "wasi_p1_fs_mutate",
            Probe::LunaticTcp => "lunatic_tcp",
            Probe::LunaticUdp => "lunatic_udp",
            Probe::LunaticSqliteCreate => "lunatic_sqlite_create",
            Probe::ExtismHttp => "extism_http",
        }
    }

    pub fn file_name(self) -> String {
        format!("{}.wasm", self.as_str())
    }

    pub const fn template_path(self) -> &'static str {
        match self {
            Probe::WasiP1FsRead => "templates/wasi_p1_fs_read.wat.in",
            Probe::WasiP1FsMutate => "templates/wasi_p1_fs_mutate.wat.in",
            Probe::LunaticTcp => "templates/lunatic_tcp.wat.in",
            Probe::LunaticUdp => "templates/lunatic_udp.wat.in",
            Probe::LunaticSqliteCreate => "templates/lunatic_sqlite_create.wat.in",
            Probe::ExtismHttp => "templates/extism_http.wat.in",
        }
    }

    pub const fn template(self) -> &'static str {
        match self {
            Probe::WasiP1FsRead => WASI_READ_TEMPLATE,
            Probe::WasiP1FsMutate => WASI_MUTATE_TEMPLATE,
            Probe::LunaticTcp => LUNATIC_TCP_TEMPLATE,
            Probe::LunaticUdp => LUNATIC_UDP_TEMPLATE,
            Probe::LunaticSqliteCreate => LUNATIC_SQLITE_TEMPLATE,
            Probe::ExtismHttp => EXTISM_HTTP_TEMPLATE,
        }
    }

    pub const fn effect(self) -> &'static str {
        match self {
            Probe::WasiP1FsRead => {
                "read the exact nonce bytes from the configured WASI preopen-relative path"
            }
            Probe::WasiP1FsMutate => {
                "overwrite, rename, and unlink the sentinel, then create the absent target with nonce bytes"
            }
            Probe::LunaticTcp => {
                "connect to the configured IPv4 TCP observer and write the exact nonce bytes"
            }
            Probe::LunaticUdp => {
                "send one UDP datagram containing the exact nonce bytes to the observer"
            }
            Probe::LunaticSqliteCreate => {
                "create the configured database and a nonce-named table containing the nonce"
            }
            Probe::ExtismHttp => {
                "POST the exact nonce body to the configured HTTP observer and expose its response body"
            }
        }
    }

    pub const fn imports(self) -> &'static [ImportSpec] {
        match self {
            Probe::WasiP1FsRead => WASI_READ_IMPORTS,
            Probe::WasiP1FsMutate => WASI_MUTATE_IMPORTS,
            Probe::LunaticTcp => LUNATIC_TCP_IMPORTS,
            Probe::LunaticUdp => LUNATIC_UDP_IMPORTS,
            Probe::LunaticSqliteCreate => LUNATIC_SQLITE_IMPORTS,
            Probe::ExtismHttp => EXTISM_HTTP_IMPORTS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ImportSpec {
    pub module: &'static str,
    pub name: &'static str,
    pub params: &'static [&'static str],
    pub results: &'static [&'static str],
}

const WASI_READ_IMPORTS: &[ImportSpec] = &[
    ImportSpec {
        module: "wasi_snapshot_preview1",
        name: "path_open",
        params: &[
            "i32", "i32", "i32", "i32", "i32", "i64", "i64", "i32", "i32",
        ],
        results: &["i32"],
    },
    ImportSpec {
        module: "wasi_snapshot_preview1",
        name: "fd_read",
        params: &["i32", "i32", "i32", "i32"],
        results: &["i32"],
    },
    ImportSpec {
        module: "wasi_snapshot_preview1",
        name: "fd_close",
        params: &["i32"],
        results: &["i32"],
    },
];

const WASI_MUTATE_IMPORTS: &[ImportSpec] = &[
    ImportSpec {
        module: "wasi_snapshot_preview1",
        name: "path_open",
        params: &[
            "i32", "i32", "i32", "i32", "i32", "i64", "i64", "i32", "i32",
        ],
        results: &["i32"],
    },
    ImportSpec {
        module: "wasi_snapshot_preview1",
        name: "fd_write",
        params: &["i32", "i32", "i32", "i32"],
        results: &["i32"],
    },
    ImportSpec {
        module: "wasi_snapshot_preview1",
        name: "fd_close",
        params: &["i32"],
        results: &["i32"],
    },
    ImportSpec {
        module: "wasi_snapshot_preview1",
        name: "path_rename",
        params: &["i32", "i32", "i32", "i32", "i32", "i32"],
        results: &["i32"],
    },
    ImportSpec {
        module: "wasi_snapshot_preview1",
        name: "path_unlink_file",
        params: &["i32", "i32", "i32"],
        results: &["i32"],
    },
];

const LUNATIC_TCP_IMPORTS: &[ImportSpec] = &[
    ImportSpec {
        module: "lunatic::networking",
        name: "tcp_connect",
        params: &["i32", "i32", "i32", "i32", "i32", "i64", "i32"],
        results: &["i32"],
    },
    ImportSpec {
        module: "lunatic::networking",
        name: "tcp_write_vectored",
        params: &["i64", "i32", "i32", "i32"],
        results: &["i32"],
    },
    ImportSpec {
        module: "lunatic::networking",
        name: "drop_tcp_stream",
        params: &["i64"],
        results: &[],
    },
];

const LUNATIC_UDP_IMPORTS: &[ImportSpec] = &[
    ImportSpec {
        module: "lunatic::networking",
        name: "udp_bind",
        params: &["i32", "i32", "i32", "i32", "i32", "i32"],
        results: &["i32"],
    },
    ImportSpec {
        module: "lunatic::networking",
        name: "udp_send_to",
        params: &[
            "i64", "i32", "i32", "i32", "i32", "i32", "i32", "i32", "i32",
        ],
        results: &["i32"],
    },
    ImportSpec {
        module: "lunatic::networking",
        name: "drop_udp_socket",
        params: &["i64"],
        results: &[],
    },
];

const LUNATIC_SQLITE_IMPORTS: &[ImportSpec] = &[
    ImportSpec {
        module: "lunatic::sqlite",
        name: "open",
        params: &["i32", "i32", "i32"],
        results: &["i64"],
    },
    ImportSpec {
        module: "lunatic::sqlite",
        name: "execute",
        params: &["i64", "i32", "i32"],
        results: &["i32"],
    },
    ImportSpec {
        module: "lunatic::sqlite",
        name: "sqlite3_close",
        params: &["i64"],
        results: &[],
    },
];

const EXTISM_HTTP_IMPORTS: &[ImportSpec] = &[
    ImportSpec {
        module: "extism:host/env",
        name: "alloc",
        params: &["i64"],
        results: &["i64"],
    },
    ImportSpec {
        module: "extism:host/env",
        name: "store_u8",
        params: &["i64", "i32"],
        results: &[],
    },
    ImportSpec {
        module: "extism:host/env",
        name: "http_request",
        params: &["i64", "i64"],
        results: &["i64"],
    },
    ImportSpec {
        module: "extism:host/env",
        name: "length",
        params: &["i64"],
        results: &["i64"],
    },
    ImportSpec {
        module: "extism:host/env",
        name: "output_set",
        params: &["i64", "i64"],
        results: &[],
    },
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Parameters {
    pub nonce: String,
    pub wasi_preopen_fd: u32,
    pub wasi_read_path: String,
    pub wasi_mutate_path: String,
    pub wasi_create_path: String,
    pub sqlite_path: String,
    pub observer_ipv4: Ipv4Addr,
    pub tcp_port: u16,
    pub udp_port: u16,
    pub http_port: u16,
    pub http_path: String,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            nonce: "embedded-v3-authority-canary-0001".to_owned(),
            wasi_preopen_fd: 3,
            wasi_read_path: "authority-read-sentinel.txt".to_owned(),
            wasi_mutate_path: "authority-mutate-sentinel.txt".to_owned(),
            wasi_create_path: "authority-created-sentinel.txt".to_owned(),
            sqlite_path: "authority-embedded-v3-authority-canary-0001.sqlite3".to_owned(),
            observer_ipv4: Ipv4Addr::LOCALHOST,
            tcp_port: 43171,
            udp_port: 43172,
            http_port: 43173,
            http_path: "/embedded-v3-authority".to_owned(),
        }
    }
}

impl Parameters {
    pub fn validate(&self) -> Result<(), String> {
        if self.nonce.is_empty()
            || self.nonce.len() > 128
            || !self
                .nonce
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        {
            return Err("nonce must be 1..=128 ASCII letters, digits, '-', '_' or '.'".to_owned());
        }
        validate_path("wasi_read_path", &self.wasi_read_path)?;
        validate_path("wasi_mutate_path", &self.wasi_mutate_path)?;
        validate_path("wasi_create_path", &self.wasi_create_path)?;
        validate_path("sqlite_path", &self.sqlite_path)?;
        if self.wasi_read_path.starts_with('/')
            || self.wasi_mutate_path.starts_with('/')
            || self.wasi_create_path.starts_with('/')
        {
            return Err("WASI paths must be preopen-relative, not absolute".to_owned());
        }
        if self.wasi_mutate_path == self.wasi_create_path {
            return Err("WASI mutation sentinel and create target must differ".to_owned());
        }
        if !self.sqlite_path.contains(&self.nonce) {
            return Err("sqlite_path must contain the exact nonce".to_owned());
        }
        if self.tcp_port == 0 || self.udp_port == 0 || self.http_port == 0 {
            return Err("observer ports must be non-zero".to_owned());
        }
        if !self.http_path.starts_with('/')
            || self.http_path.len() > 256
            || !self
                .http_path
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"/-_.".contains(&byte))
        {
            return Err("http_path must be an ASCII absolute path without query data".to_owned());
        }
        Ok(())
    }
}

fn validate_path(name: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 512 || value.as_bytes().contains(&0) {
        return Err(format!("{name} must be 1..=512 bytes and contain no NUL"));
    }
    Ok(())
}

pub fn parameter_blob(probe: Probe, parameters: &Parameters) -> Result<Vec<u8>, String> {
    parameters.validate()?;
    let mut blob = Vec::new();
    push_component(&mut blob, PARAMETER_DOMAIN);
    push_component(&mut blob, probe.as_str().as_bytes());
    push_field(&mut blob, "nonce", parameters.nonce.as_bytes());
    match probe {
        Probe::WasiP1FsRead => {
            push_field(
                &mut blob,
                "preopen_fd",
                parameters.wasi_preopen_fd.to_string().as_bytes(),
            );
            push_field(&mut blob, "path", parameters.wasi_read_path.as_bytes());
        }
        Probe::WasiP1FsMutate => {
            push_field(
                &mut blob,
                "preopen_fd",
                parameters.wasi_preopen_fd.to_string().as_bytes(),
            );
            push_field(
                &mut blob,
                "sentinel_path",
                parameters.wasi_mutate_path.as_bytes(),
            );
            push_field(
                &mut blob,
                "create_path",
                parameters.wasi_create_path.as_bytes(),
            );
        }
        Probe::LunaticTcp => {
            push_field(
                &mut blob,
                "observer_ipv4",
                parameters.observer_ipv4.to_string().as_bytes(),
            );
            push_field(
                &mut blob,
                "port",
                parameters.tcp_port.to_string().as_bytes(),
            );
        }
        Probe::LunaticUdp => {
            push_field(
                &mut blob,
                "observer_ipv4",
                parameters.observer_ipv4.to_string().as_bytes(),
            );
            push_field(
                &mut blob,
                "port",
                parameters.udp_port.to_string().as_bytes(),
            );
        }
        Probe::LunaticSqliteCreate => {
            push_field(&mut blob, "path", parameters.sqlite_path.as_bytes());
        }
        Probe::ExtismHttp => {
            push_field(
                &mut blob,
                "observer_ipv4",
                parameters.observer_ipv4.to_string().as_bytes(),
            );
            push_field(
                &mut blob,
                "port",
                parameters.http_port.to_string().as_bytes(),
            );
            push_field(&mut blob, "path", parameters.http_path.as_bytes());
        }
    }
    if blob.len() >= 3072 {
        return Err("parameter blob overlaps the next fixed guest data segment".to_owned());
    }
    Ok(blob)
}

pub fn suite_parameter_blob(parameters: &Parameters) -> Result<Vec<u8>, String> {
    parameters.validate()?;
    let mut blob = Vec::new();
    push_component(&mut blob, PARAMETER_DOMAIN);
    push_component(&mut blob, b"suite");
    push_field(&mut blob, "nonce", parameters.nonce.as_bytes());
    push_field(
        &mut blob,
        "wasi_preopen_fd",
        parameters.wasi_preopen_fd.to_string().as_bytes(),
    );
    push_field(
        &mut blob,
        "wasi_read_path",
        parameters.wasi_read_path.as_bytes(),
    );
    push_field(
        &mut blob,
        "wasi_mutate_path",
        parameters.wasi_mutate_path.as_bytes(),
    );
    push_field(
        &mut blob,
        "wasi_create_path",
        parameters.wasi_create_path.as_bytes(),
    );
    push_field(&mut blob, "sqlite_path", parameters.sqlite_path.as_bytes());
    push_field(
        &mut blob,
        "observer_ipv4",
        parameters.observer_ipv4.to_string().as_bytes(),
    );
    push_field(
        &mut blob,
        "tcp_port",
        parameters.tcp_port.to_string().as_bytes(),
    );
    push_field(
        &mut blob,
        "udp_port",
        parameters.udp_port.to_string().as_bytes(),
    );
    push_field(
        &mut blob,
        "http_port",
        parameters.http_port.to_string().as_bytes(),
    );
    push_field(&mut blob, "http_path", parameters.http_path.as_bytes());
    Ok(blob)
}

fn push_field(blob: &mut Vec<u8>, name: &str, value: &[u8]) {
    push_component(blob, name.as_bytes());
    push_component(blob, value);
}

fn push_component(blob: &mut Vec<u8>, value: &[u8]) {
    let length = u32::try_from(value.len()).expect("validated authority parameter fits u32");
    blob.extend_from_slice(&length.to_le_bytes());
    blob.extend_from_slice(value);
}

pub fn render_wat(probe: Probe, parameters: &Parameters) -> Result<String, String> {
    let blob = parameter_blob(probe, parameters)?;
    let nonce = parameters.nonce.as_bytes();
    let mut rendered = probe
        .template()
        .replace("__PARAM_BLOB__", &wat_escape(&blob))
        .replace("__PARAM_LEN__", &blob.len().to_string())
        .replace("__NONCE_BYTES__", &wat_escape(nonce))
        .replace("__NONCE_LEN__", &nonce.len().to_string())
        .replace("__PREOPEN_FD__", &parameters.wasi_preopen_fd.to_string())
        .replace(
            "__IPV4_BYTES__",
            &wat_escape(&parameters.observer_ipv4.octets()),
        )
        .replace("__TCP_PORT__", &parameters.tcp_port.to_string())
        .replace("__UDP_PORT__", &parameters.udp_port.to_string());

    let path = match probe {
        Probe::WasiP1FsRead => parameters.wasi_read_path.as_bytes(),
        Probe::WasiP1FsMutate => parameters.wasi_mutate_path.as_bytes(),
        _ => &[],
    };
    rendered = rendered
        .replace("__PATH_BYTES__", &wat_escape(path))
        .replace("__PATH_LEN__", &path.len().to_string())
        .replace(
            "__MUTATE_PATH_BYTES__",
            &wat_escape(parameters.wasi_mutate_path.as_bytes()),
        )
        .replace(
            "__MUTATE_PATH_LEN__",
            &parameters.wasi_mutate_path.len().to_string(),
        )
        .replace(
            "__CREATE_PATH_BYTES__",
            &wat_escape(parameters.wasi_create_path.as_bytes()),
        )
        .replace(
            "__CREATE_PATH_LEN__",
            &parameters.wasi_create_path.len().to_string(),
        )
        .replace(
            "__SQLITE_PATH_BYTES__",
            &wat_escape(parameters.sqlite_path.as_bytes()),
        )
        .replace(
            "__SQLITE_PATH_LEN__",
            &parameters.sqlite_path.len().to_string(),
        );
    if probe == Probe::LunaticSqliteCreate {
        let query = sqlite_query(parameters);
        rendered = rendered
            .replace("__SQLITE_QUERY_BYTES__", &wat_escape(query.as_bytes()))
            .replace("__SQLITE_QUERY_LEN__", &query.len().to_string());
    }

    if probe == Probe::ExtismHttp {
        let request = http_request_bytes(parameters)?;
        rendered = rendered
            .replace("__REQUEST_LEN__", &request.len().to_string())
            .replace("__REQUEST_STORE__", &store_u8_calls("request", &request))
            .replace("__BODY_LEN__", &nonce.len().to_string())
            .replace("__BODY_STORE__", &store_u8_calls("body", nonce));
    }
    if rendered.contains("__") {
        return Err(format!(
            "unresolved template placeholder remains for {}",
            probe.as_str()
        ));
    }
    Ok(rendered)
}

pub fn sqlite_table_name(parameters: &Parameters) -> Result<String, String> {
    parameters.validate()?;
    Ok(format!("authority_{}", parameters.nonce))
}

fn sqlite_query(parameters: &Parameters) -> String {
    format!(
        "CREATE TABLE \"authority_{}\" AS SELECT '{}' AS nonce",
        parameters.nonce, parameters.nonce
    )
}

#[derive(Serialize)]
struct ExtismHttpRequest<'a> {
    url: String,
    method: &'static str,
    headers: BTreeMap<&'static str, &'a str>,
}

fn http_request_bytes(parameters: &Parameters) -> Result<Vec<u8>, String> {
    let mut headers = BTreeMap::new();
    headers.insert("x-authority-nonce", parameters.nonce.as_str());
    let request = ExtismHttpRequest {
        url: format!(
            "http://{}:{}{}?nonce={}",
            parameters.observer_ipv4, parameters.http_port, parameters.http_path, parameters.nonce
        ),
        method: "POST",
        headers,
    };
    serde_json::to_vec(&request).map_err(|error| error.to_string())
}

fn store_u8_calls(local: &str, bytes: &[u8]) -> String {
    let mut output = String::new();
    for (offset, byte) in bytes.iter().enumerate() {
        writeln!(
            &mut output,
            "    (call $store_u8 (i64.add (local.get ${local}) (i64.const {offset})) (i32.const {byte}))"
        )
        .expect("writing to String cannot fail");
    }
    output.trim_end().to_owned()
}

fn wat_escape(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 3);
    for byte in bytes {
        write!(&mut output, "\\{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

pub fn artifact_bytes(probe: Probe, parameters: &Parameters) -> Result<Vec<u8>, String> {
    wat::parse_str(render_wat(probe, parameters)?).map_err(|error| error.to_string())
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[derive(Serialize)]
struct SourceRecord {
    path: &'static str,
    sha256: String,
}

#[derive(Serialize)]
struct Recipe {
    wat_crate: &'static str,
    command: &'static str,
}

#[derive(Serialize)]
struct ArtifactRecord {
    probe: &'static str,
    file: String,
    sha256: String,
    bytes: usize,
    parameter_sha256: String,
    parameter_blob_hex: String,
    imports: &'static [ImportSpec],
    effect: &'static str,
}

#[derive(Serialize)]
struct Manifest<'a> {
    schema_version: u32,
    abi_version: u32,
    parameter_encoding: &'static str,
    parameter_pointer: u32,
    suite_parameter_sha256: String,
    parameters: &'a Parameters,
    sources: Vec<SourceRecord>,
    recipe: Recipe,
    artifacts: Vec<ArtifactRecord>,
}

pub fn build_all(
    root: &Path,
    output_dir: &Path,
    parameters: &Parameters,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    parameters
        .validate()
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
    fs::create_dir_all(output_dir)?;

    let mut sources = Vec::with_capacity(Probe::ALL.len());
    let mut records = Vec::with_capacity(Probe::ALL.len());
    for probe in Probe::ALL {
        sources.push(SourceRecord {
            path: probe.template_path(),
            sha256: sha256_hex(probe.template().as_bytes()),
        });
        let blob = parameter_blob(probe, parameters)
            .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
        let bytes = artifact_bytes(probe, parameters)
            .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))?;
        let file = probe.file_name();
        write_if_changed(&output_dir.join(&file), &bytes)?;
        records.push(ArtifactRecord {
            probe: probe.as_str(),
            file,
            sha256: sha256_hex(&bytes),
            bytes: bytes.len(),
            parameter_sha256: sha256_hex(&blob),
            parameter_blob_hex: hex(&blob),
            imports: probe.imports(),
            effect: probe.effect(),
        });
    }

    let suite_blob = suite_parameter_blob(parameters)
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
    let manifest = Manifest {
        schema_version: 1,
        abi_version: ABI_VERSION,
        parameter_encoding: "domain-separated-u32le-length-prefixed-v1",
        parameter_pointer: PARAMETER_PTR,
        suite_parameter_sha256: sha256_hex(&suite_blob),
        parameters,
        sources,
        recipe: Recipe {
            wat_crate: "1.254.0",
            command: "cargo run --locked --bin build-authority-guests",
        },
        artifacts: records,
    };
    let mut manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    manifest_bytes.push(b'\n');
    let manifest_path = output_dir.join("manifest.json");
    write_if_changed(&manifest_path, &manifest_bytes)?;

    if !manifest_path.starts_with(root) {
        eprintln!(
            "note: generated authority artifacts outside source root: {}",
            output_dir.display()
        );
    }
    Ok(manifest_path)
}

fn write_if_changed(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if fs::read(path).ok().as_deref() == Some(bytes) {
        return Ok(());
    }
    fs::write(path, bytes)
}
