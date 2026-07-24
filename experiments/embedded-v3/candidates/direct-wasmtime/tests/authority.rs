use std::fs;
use std::path::{Path, PathBuf};

use embedded_v3_direct_wasmtime::protocol::AuthorityProbeKind;
use embedded_v3_direct_wasmtime::runtime::{authority_absent_at_link, SharedRuntime};

fn artifact_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("guest-artifacts")
}

fn authority_artifact(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("guests/authority/artifacts")
        .join(name);
    fs::read(path).unwrap()
}

#[test]
fn every_genuine_canary_is_rejected_by_the_exact_production_linker() {
    let runtime = SharedRuntime::initialize(&artifact_root()).unwrap();
    for (probe, file) in [
        (AuthorityProbeKind::WasiP1FsRead, "wasi_p1_fs_read.wasm"),
        (AuthorityProbeKind::WasiP1FsMutate, "wasi_p1_fs_mutate.wasm"),
        (AuthorityProbeKind::LunaticTcp, "lunatic_tcp.wasm"),
        (AuthorityProbeKind::LunaticUdp, "lunatic_udp.wasm"),
        (
            AuthorityProbeKind::LunaticSqliteCreate,
            "lunatic_sqlite_create.wasm",
        ),
        (AuthorityProbeKind::ExtismHttp, "extism_http.wasm"),
    ] {
        authority_absent_at_link(&runtime, probe, &authority_artifact(file)).unwrap();
    }
    runtime.stop_ticker().unwrap();
}

#[test]
fn an_unrelated_activation_trap_is_not_classified_as_unknown_import() {
    let artifact_root = artifact_root();
    let runtime = SharedRuntime::initialize(&artifact_root).unwrap();
    let bad_core = fs::read(artifact_root.join("tenant-bad-a.wasm")).unwrap();
    let error = authority_absent_at_link(&runtime, AuthorityProbeKind::WasiP1FsRead, &bad_core)
        .unwrap_err();
    assert!(error.to_string().contains("first import"));
    runtime.stop_ticker().unwrap();
}
