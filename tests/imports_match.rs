use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::Result;
use lunatic_process::env::LunaticEnvironment;
use lunatic_process::runtimes::wasmtime::{default_config, WasmtimeRuntime};
use lunatic_process::state::ProcessState;
use lunatic_runtime::state::DefaultProcessState;
use lunatic_runtime::DefaultProcessConfig;
use tokio::sync::RwLock;
use wasmtime::{AsContext, Extern, Linker, Store, ValType};

#[derive(Clone, Debug, PartialEq, Eq)]
struct FuncSig {
    params: Vec<ValType>,
    results: Vec<ValType>,
}

fn func_sig_from_extern_type(ext: &wasmtime::ExternType) -> FuncSig {
    match ext {
        wasmtime::ExternType::Func(func_ty) => FuncSig {
            params: func_ty.params().collect::<Vec<_>>(),
            results: func_ty.results().collect::<Vec<_>>(),
        },
        other => panic!("non-function import encountered: {:?}", other),
    }
}

fn val_type_to_str(value: ValType) -> &'static str {
    match value {
        ValType::I32 => "i32",
        ValType::I64 => "i64",
        ValType::F32 => "f32",
        ValType::F64 => "f64",
        ValType::V128 => "v128",
        ValType::ExternRef => "externref",
        ValType::FuncRef => "funcref",
    }
}

fn format_func_sig(sig: &FuncSig) -> String {
    let mut parts = Vec::new();
    if !sig.params.is_empty() {
        let params = sig
            .params
            .iter()
            .map(|&ty| val_type_to_str(ty))
            .collect::<Vec<_>>()
            .join(" ");
        parts.push(format!("(param {})", params));
    }
    if !sig.results.is_empty() {
        let results = sig
            .results
            .iter()
            .map(|&ty| val_type_to_str(ty))
            .collect::<Vec<_>>()
            .join(" ");
        parts.push(format!("(result {})", results));
    }
    parts.join(" ")
}

fn format_import_line(module: &str, name: &str, sig: &FuncSig) -> String {
    let sig_str = format_func_sig(sig);
    if sig_str.is_empty() {
        format!("    (import \"{}\" \"{}\" (func))", module, name)
    } else {
        format!(
            "    (import \"{}\" \"{}\" (func {}))",
            module, name, sig_str
        )
    }
}

fn render_wat_file(imports: &BTreeMap<(String, String), FuncSig>) -> String {
    let mut output =
        String::from(";; This file is used for testing import signatures.\n\n(module\n");
    let mut current_module: Option<&str> = None;

    for ((module, name), sig) in imports.iter() {
        if current_module
            .map(|m| m != module.as_str())
            .unwrap_or(false)
        {
            output.push('\n');
        }
        current_module = Some(module.as_str());
        output.push_str(&format!("{}\n", format_import_line(module, name, sig)));
    }

    output.push_str(")\n");
    output
}

#[tokio::test]
async fn wat_imports_are_in_sync_with_runtime() -> Result<()> {
    let config = default_config();
    let runtime = WasmtimeRuntime::new(&config)?;
    let engine = runtime.engine();

    let mut linker: Linker<DefaultProcessState> = Linker::new(engine);
    <DefaultProcessState as ProcessState>::register(&mut linker)?;

    let raw_module = wat::parse_str("(module)")?;
    let compiled = Arc::new(runtime.compile_module::<DefaultProcessState>(raw_module.into())?);

    let env = Arc::new(LunaticEnvironment::new(0));
    let config = Arc::new(DefaultProcessConfig::default());
    let registry = Arc::new(RwLock::new(HashMap::new()));
    let state = DefaultProcessState::new(
        env.clone(),
        None,
        runtime.clone(),
        compiled,
        config,
        registry,
    )?;
    let mut store = Store::new(engine, state);

    let definitions: Vec<(String, String, Extern)> = linker
        .iter(&mut store)
        .map(|(module, name, ext)| (module.to_string(), name.to_string(), ext))
        .collect();

    let mut host_imports: BTreeMap<(String, String), FuncSig> = BTreeMap::new();
    for (module, name, ext) in definitions {
        let key = (module, name);
        let extern_type = ext.ty(store.as_context());
        let signature = func_sig_from_extern_type(&extern_type);
        let prev = host_imports.insert(key.clone(), signature);
        assert!(
            prev.is_none(),
            "duplicate host export detected for {:?}",
            key
        );
    }

    let expected_wat = render_wat_file(&host_imports);
    let current_wat = std::fs::read_to_string("wat/all_imports.wat")?.replace("\r\n", "\n");
    if current_wat != expected_wat {
        std::fs::write("target/expected_all_imports.wat", &expected_wat)?;
        panic!(
            "wat/all_imports.wat is out of sync with registered host functions. Replace its contents with:\n{}",
            expected_wat
        );
    }

    Ok(())
}
