use std::collections::VecDeque;

use embedded_v3_core_guests::{artifact_bytes, spec, Variant, VARIANTS};
use wasmtime::{Config, Engine, ExternType, Linker, Module, Store};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Activation {
    tenant: i32,
    version: i32,
    build: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResultEvent {
    handle: i64,
    counter: i64,
    kind: i32,
    version: i32,
    build: i64,
}

#[derive(Default)]
struct HostState {
    tenant: i32,
    restore: i64,
    handle: i64,
    activations: Vec<Activation>,
    results: Vec<ResultEvent>,
    execution_started: Vec<i64>,
    observer: Option<i64>,
    commands: VecDeque<(i32, i64)>,
}

fn engine() -> Engine {
    let mut config = Config::new();
    config.consume_fuel(true);
    Engine::new(&config).unwrap()
}

fn linker(engine: &Engine) -> Linker<HostState> {
    let mut linker = Linker::new(engine);
    linker
        .func_wrap(
            "comparison",
            "tenant_id",
            |caller: wasmtime::Caller<'_, HostState>| caller.data().tenant,
        )
        .unwrap();
    linker
        .func_wrap(
            "comparison",
            "restore_counter",
            |caller: wasmtime::Caller<'_, HostState>| caller.data().restore,
        )
        .unwrap();
    linker
        .func_wrap(
            "comparison",
            "command_handle",
            |caller: wasmtime::Caller<'_, HostState>| caller.data().handle,
        )
        .unwrap();
    linker
        .func_wrap(
            "comparison",
            "activation_started",
            |mut caller: wasmtime::Caller<'_, HostState>, tenant: i32, version: i32, build: i64| {
                caller.data_mut().activations.push(Activation {
                    tenant,
                    version,
                    build,
                });
            },
        )
        .unwrap();
    linker
        .func_wrap(
            "comparison",
            "emit_result",
            |mut caller: wasmtime::Caller<'_, HostState>,
             handle: i64,
             counter: i64,
             kind: i32,
             version: i32,
             build: i64| {
                caller.data_mut().results.push(ResultEvent {
                    handle,
                    counter,
                    kind,
                    version,
                    build,
                });
            },
        )
        .unwrap();
    linker
        .func_wrap(
            "comparison",
            "execution_started",
            |mut caller: wasmtime::Caller<'_, HostState>, handle: i64| {
                caller.data_mut().execution_started.push(handle);
            },
        )
        .unwrap();
    linker
        .func_wrap(
            "comparison",
            "bind_observer",
            |mut caller: wasmtime::Caller<'_, HostState>, observer: i64| {
                caller.data_mut().observer = Some(observer);
            },
        )
        .unwrap();
    linker
        .func_wrap(
            "comparison",
            "next_command",
            |mut caller: wasmtime::Caller<'_, HostState>| {
                let (opcode, handle) = caller.data_mut().commands.pop_front().unwrap_or((0, 0));
                caller.data_mut().handle = handle;
                opcode
            },
        )
        .unwrap();
    linker
}

fn instantiate(
    variant: Variant,
    tenant: i32,
    restore: i64,
) -> anyhow::Result<(Store<HostState>, wasmtime::Instance)> {
    let engine = engine();
    let module = Module::new(&engine, artifact_bytes(variant)?)?;
    let mut store = Store::new(
        &engine,
        HostState {
            tenant,
            restore,
            ..HostState::default()
        },
    );
    store.set_fuel(10_000_000)?;
    let instance = linker(&engine).instantiate(&mut store, &module)?;
    Ok((store, instance))
}

#[test]
fn every_variant_has_the_frozen_host_neutral_shape() {
    let engine = engine();
    for variant in VARIANTS {
        let module = Module::new(&engine, artifact_bytes(variant.variant).unwrap()).unwrap();
        let imports: Vec<_> = module
            .imports()
            .map(|import| (import.module().to_owned(), import.name().to_owned()))
            .collect();
        assert_eq!(imports.len(), 8);
        assert!(imports.iter().all(|(module, _)| module == "comparison"));

        let exports: Vec<_> = module
            .exports()
            .map(|export| export.name().to_owned())
            .collect();
        for required in [
            "memory",
            "activation_check",
            "increment",
            "probe",
            "snapshot",
            "trap",
            "cpu_loop",
            "run",
        ] {
            assert!(exports.iter().any(|name| name == required));
        }
        for export in module.exports() {
            if matches!(
                export.name(),
                "activation_check" | "increment" | "probe" | "snapshot" | "trap" | "cpu_loop"
            ) {
                let ExternType::Func(function) = export.ty() else {
                    panic!("{} is not a function", export.name());
                };
                assert_eq!(function.params().len(), 0);
                assert_eq!(function.results().len(), 0);
            }
        }
    }
}

#[test]
fn state_and_guest_markers_survive_one_shot_calls_and_transfer() {
    let (mut store, instance) = instantiate(Variant::A, 3, 0).unwrap();
    instance
        .get_typed_func::<(), ()>(&mut store, "activation_check")
        .unwrap()
        .call(&mut store, ())
        .unwrap();
    assert!(store.data().results.is_empty());
    for handle in [10, 11] {
        store.data_mut().handle = handle;
        instance
            .get_typed_func::<(), ()>(&mut store, "increment")
            .unwrap()
            .call(&mut store, ())
            .unwrap();
    }
    store.data_mut().handle = 12;
    instance
        .get_typed_func::<(), ()>(&mut store, "snapshot")
        .unwrap()
        .call(&mut store, ())
        .unwrap();
    assert_eq!(store.data().results.last().unwrap().counter, 2);
    assert_eq!(store.data().results.last().unwrap().kind, 3);
    assert_eq!(store.data().results.last().unwrap().version, 1);

    let (mut replacement, b) = instantiate(Variant::B, 3, 2).unwrap();
    replacement.data_mut().handle = 13;
    b.get_typed_func::<(), ()>(&mut replacement, "probe")
        .unwrap()
        .call(&mut replacement, ())
        .unwrap();
    let event = replacement.data().results.last().unwrap();
    assert_eq!(event.counter, 2);
    assert_eq!(event.version, 2);
    assert_eq!(event.build, spec(Variant::B).build_marker);
}

#[test]
fn long_lived_run_uses_the_same_operations() {
    let (mut store, instance) = instantiate(Variant::A, 4, 0).unwrap();
    store.data_mut().commands = VecDeque::from([(1, 21), (2, 22), (3, 23), (0, 0)]);
    instance
        .get_typed_func::<i64, ()>(&mut store, "run")
        .unwrap()
        .call(&mut store, 99)
        .unwrap();
    assert_eq!(store.data().observer, Some(99));
    assert_eq!(
        store
            .data()
            .results
            .iter()
            .map(|event| (event.handle, event.counter, event.kind))
            .collect::<Vec<_>>(),
        [(21, 1, 1), (22, 1, 2), (23, 1, 3)]
    );
}

#[test]
fn bad_builds_fail_only_tenant_seven_during_activation() {
    for variant in [Variant::BadA, Variant::BadB] {
        assert!(instantiate(variant, 7, 9).is_err());
        let (store, _) = instantiate(variant, 6, 9).unwrap();
        assert_eq!(store.data().activations.len(), 1);
        assert_eq!(store.data().activations[0].tenant, 6);
    }
}

#[test]
fn trap_and_cpu_loop_require_runtime_failure_and_interruption() {
    let (mut trap_store, trap_instance) = instantiate(Variant::A, 1, 0).unwrap();
    assert!(trap_instance
        .get_typed_func::<(), ()>(&mut trap_store, "trap")
        .unwrap()
        .call(&mut trap_store, ())
        .is_err());

    let (mut cpu_store, cpu_instance) = instantiate(Variant::A, 1, 0).unwrap();
    cpu_store.data_mut().handle = 55;
    cpu_store.set_fuel(50_000).unwrap();
    assert!(cpu_instance
        .get_typed_func::<(), ()>(&mut cpu_store, "cpu_loop")
        .unwrap()
        .call(&mut cpu_store, ())
        .is_err());
    assert_eq!(cpu_store.data().execution_started, [55]);
}
