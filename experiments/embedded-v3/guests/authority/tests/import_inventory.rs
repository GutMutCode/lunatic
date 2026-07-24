use std::fs;
use std::path::Path;

use embedded_v3_authority_guests::{
    artifact_bytes, parameter_blob, sha256_hex, sqlite_table_name, suite_parameter_blob,
    Parameters, Probe,
};
use wasmtime::{Engine, ExternType, Module, ValType};

const LUNATIC_ALL_IMPORTS: &str = include_str!("../../../../../wat/all_imports.wat");

fn value_type_name(value_type: ValType) -> &'static str {
    match value_type {
        ValType::I32 => "i32",
        ValType::I64 => "i64",
        ValType::F32 => "f32",
        ValType::F64 => "f64",
        ValType::V128 => "v128",
        ValType::Ref(_) => "ref",
    }
}

#[test]
fn every_wasm_has_only_the_frozen_genuine_imports() {
    let engine = Engine::default();
    let parameters = Parameters::default();
    for probe in Probe::ALL {
        let bytes = artifact_bytes(probe, &parameters).unwrap();
        let module = Module::new(&engine, &bytes).unwrap();
        let actual: Vec<_> = module
            .imports()
            .map(|import| {
                let function = match import.ty() {
                    ExternType::Func(function) => function,
                    other => panic!("unexpected non-function import: {:?}", other),
                };
                (
                    import.module().to_owned(),
                    import.name().to_owned(),
                    function.params().map(value_type_name).collect::<Vec<_>>(),
                    function.results().map(value_type_name).collect::<Vec<_>>(),
                )
            })
            .collect();
        let expected: Vec<_> = probe
            .imports()
            .iter()
            .map(|import| {
                (
                    import.module.to_owned(),
                    import.name.to_owned(),
                    import.params.to_vec(),
                    import.results.to_vec(),
                )
            })
            .collect();
        assert_eq!(actual, expected, "import mismatch for {}", probe.as_str());
        assert!(
            actual
                .iter()
                .all(|(module, _, _, _)| module != "comparison"),
            "{} must not use a comparison-only deny import",
            probe.as_str()
        );
    }
}

#[test]
fn frozen_signatures_remain_in_lunatics_canonical_import_fixture() {
    for declaration in [
        r#"(import "lunatic::networking" "tcp_connect" (func (param i32 i32 i32 i32 i32 i64 i32) (result i32)))"#,
        r#"(import "lunatic::networking" "tcp_write_vectored" (func (param i64 i32 i32 i32) (result i32)))"#,
        r#"(import "lunatic::networking" "udp_bind" (func (param i32 i32 i32 i32 i32 i32) (result i32)))"#,
        r#"(import "lunatic::networking" "udp_send_to" (func (param i64 i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))"#,
        r#"(import "lunatic::sqlite" "open" (func (param i32 i32 i32) (result i64)))"#,
        r#"(import "lunatic::sqlite" "execute" (func (param i64 i32 i32) (result i32)))"#,
        r#"(import "wasi_snapshot_preview1" "path_open" (func (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))"#,
        r#"(import "wasi_snapshot_preview1" "fd_read" (func (param i32 i32 i32 i32) (result i32)))"#,
        r#"(import "wasi_snapshot_preview1" "fd_write" (func (param i32 i32 i32 i32) (result i32)))"#,
        r#"(import "wasi_snapshot_preview1" "path_rename" (func (param i32 i32 i32 i32 i32 i32) (result i32)))"#,
        r#"(import "wasi_snapshot_preview1" "path_unlink_file" (func (param i32 i32 i32) (result i32)))"#,
    ] {
        assert!(
            LUNATIC_ALL_IMPORTS.contains(declaration),
            "canonical import fixture drifted: {}",
            declaration
        );
    }
}

#[test]
fn every_wasm_exposes_the_frozen_invocation_and_parameter_shape() {
    let engine = Engine::default();
    let parameters = Parameters::default();
    for probe in Probe::ALL {
        let module = Module::new(&engine, artifact_bytes(probe, &parameters).unwrap()).unwrap();
        let exports: Vec<_> = module.exports().collect();
        for required in ["memory", "parameter_ptr", "parameter_len", "invoke"] {
            assert!(
                exports.iter().any(|export| export.name() == required),
                "{} lacks {required}",
                probe.as_str()
            );
        }
        let invoke = exports
            .iter()
            .find(|export| export.name() == "invoke")
            .unwrap();
        let invoke = match invoke.ty() {
            ExternType::Func(function) => function,
            other => panic!("invoke is not a function: {:?}", other),
        };
        assert_eq!(invoke.params().count(), 0);
        assert_eq!(
            invoke.results().map(value_type_name).collect::<Vec<_>>(),
            ["i32"]
        );
    }
}

#[test]
fn checked_in_artifacts_and_manifest_are_reproducible() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let parameters = Parameters::default();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("artifacts/manifest.json")).unwrap()).unwrap();
    assert_eq!(
        manifest["suite_parameter_sha256"],
        sha256_hex(&suite_parameter_blob(&parameters).unwrap())
    );
    let records = manifest["artifacts"].as_array().unwrap();
    assert_eq!(records.len(), Probe::ALL.len());

    for (probe, record) in Probe::ALL.iter().copied().zip(records) {
        assert_eq!(record["probe"], probe.as_str());
        let generated = artifact_bytes(probe, &parameters).unwrap();
        let checked_in = fs::read(root.join("artifacts").join(probe.file_name())).unwrap();
        assert_eq!(generated, checked_in, "{} bytes drifted", probe.as_str());
        assert_eq!(record["sha256"], sha256_hex(&generated));
        let blob = parameter_blob(probe, &parameters).unwrap();
        assert_eq!(record["parameter_sha256"], sha256_hex(&blob));
        assert!(
            generated
                .windows(blob.len())
                .any(|window| window == blob.as_slice()),
            "{} does not contain its canonical parameter blob",
            probe.as_str()
        );
    }
}

#[test]
fn nonce_changes_every_artifact_and_target_changes_only_its_probe() {
    let original = Parameters::default();
    let mut changed_nonce = original.clone();
    changed_nonce.nonce = "embedded-v3-authority-canary-0002".to_owned();
    changed_nonce.sqlite_path = "authority-embedded-v3-authority-canary-0002.sqlite3".to_owned();
    for probe in Probe::ALL {
        assert_ne!(
            artifact_bytes(probe, &original).unwrap(),
            artifact_bytes(probe, &changed_nonce).unwrap(),
            "{} failed to bind nonce",
            probe.as_str()
        );
    }

    let mut changed_tcp = original.clone();
    changed_tcp.tcp_port += 1;
    for probe in Probe::ALL {
        let equal = artifact_bytes(probe, &original).unwrap()
            == artifact_bytes(probe, &changed_tcp).unwrap();
        assert_eq!(
            equal,
            probe != Probe::LunaticTcp,
            "tcp target binding leaked or was omitted for {}",
            probe.as_str()
        );
    }
}

#[test]
fn mutation_and_sqlite_artifacts_bind_every_scenario_effect() {
    let parameters = Parameters::default();
    let mutation = artifact_bytes(Probe::WasiP1FsMutate, &parameters).unwrap();
    for target in [
        parameters.wasi_mutate_path.as_bytes(),
        parameters.wasi_create_path.as_bytes(),
        parameters.nonce.as_bytes(),
    ] {
        assert!(
            mutation
                .windows(target.len())
                .any(|window| window == target),
            "mutation artifact omitted a bound effect target"
        );
    }
    assert_eq!(
        Probe::WasiP1FsMutate
            .imports()
            .iter()
            .map(|import| import.name)
            .collect::<Vec<_>>(),
        [
            "path_open",
            "fd_write",
            "fd_close",
            "path_rename",
            "path_unlink_file"
        ]
    );

    let sqlite = artifact_bytes(Probe::LunaticSqliteCreate, &parameters).unwrap();
    let table = sqlite_table_name(&parameters).unwrap();
    assert!(
        sqlite
            .windows(table.len())
            .any(|window| window == table.as_bytes()),
        "SQLite artifact omitted the nonce table name"
    );
    assert!(
        sqlite
            .windows(parameters.nonce.len())
            .filter(|window| *window == parameters.nonce.as_bytes())
            .count()
            >= 2,
        "SQLite artifact must bind the nonce in parameters and SQL"
    );
    assert_eq!(
        Probe::LunaticSqliteCreate
            .imports()
            .iter()
            .map(|import| import.name)
            .collect::<Vec<_>>(),
        ["open", "execute", "sqlite3_close"]
    );
}

#[test]
fn invalid_or_ambiguous_parameters_are_rejected() {
    let parameters = Parameters {
        nonce: "../escape".to_owned(),
        ..Parameters::default()
    };
    assert!(parameters.validate().is_err());

    let parameters = Parameters {
        wasi_read_path: "/absolute".to_owned(),
        ..Parameters::default()
    };
    assert!(parameters.validate().is_err());

    let parameters = Parameters {
        http_path: "/path?unbound=query".to_owned(),
        ..Parameters::default()
    };
    assert!(parameters.validate().is_err());

    let parameters = Parameters {
        sqlite_path: "not-nonce-named.sqlite3".to_owned(),
        ..Parameters::default()
    };
    assert!(parameters.validate().is_err());
}
