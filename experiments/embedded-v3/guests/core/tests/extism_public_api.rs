use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use embedded_v3_core_guests::{artifact_bytes, spec, Variant};
use extism::{Function, Manifest, Plugin, PluginBuilder, UserData, Val, ValType, Wasm};

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
    activations: Vec<(i32, i32, i64)>,
    results: Vec<ResultEvent>,
    execution_started: Vec<i64>,
    observer: Option<i64>,
    commands: VecDeque<(i32, i64)>,
}

fn function(
    name: &str,
    params: impl IntoIterator<Item = ValType>,
    results: impl IntoIterator<Item = ValType>,
    data: UserData<HostState>,
    callback: impl 'static
        + Fn(
            &mut extism::CurrentPlugin,
            &[Val],
            &mut [Val],
            UserData<HostState>,
        ) -> Result<(), extism::Error>
        + Send
        + Sync,
) -> Function {
    Function::new(name, params, results, data, callback).with_namespace("comparison")
}

fn functions(data: UserData<HostState>) -> Vec<Function> {
    vec![
        function(
            "tenant_id",
            [],
            [ValType::I32],
            data.clone(),
            |_, _, output, data| {
                let shared = data.get()?;
                output[0] = Val::I32(shared.lock().unwrap().tenant);
                Ok(())
            },
        ),
        function(
            "restore_counter",
            [],
            [ValType::I64],
            data.clone(),
            |_, _, output, data| {
                let shared = data.get()?;
                output[0] = Val::I64(shared.lock().unwrap().restore);
                Ok(())
            },
        ),
        function(
            "command_handle",
            [],
            [ValType::I64],
            data.clone(),
            |_, _, output, data| {
                let shared = data.get()?;
                output[0] = Val::I64(shared.lock().unwrap().handle);
                Ok(())
            },
        ),
        function(
            "activation_started",
            [ValType::I32, ValType::I32, ValType::I64],
            [],
            data.clone(),
            |_, input, _, data| {
                let shared = data.get()?;
                shared.lock().unwrap().activations.push((
                    input[0].i32().unwrap(),
                    input[1].i32().unwrap(),
                    input[2].i64().unwrap(),
                ));
                Ok(())
            },
        ),
        function(
            "emit_result",
            [
                ValType::I64,
                ValType::I64,
                ValType::I32,
                ValType::I32,
                ValType::I64,
            ],
            [],
            data.clone(),
            |_, input, _, data| {
                let shared = data.get()?;
                shared.lock().unwrap().results.push(ResultEvent {
                    handle: input[0].i64().unwrap(),
                    counter: input[1].i64().unwrap(),
                    kind: input[2].i32().unwrap(),
                    version: input[3].i32().unwrap(),
                    build: input[4].i64().unwrap(),
                });
                Ok(())
            },
        ),
        function(
            "execution_started",
            [ValType::I64],
            [],
            data.clone(),
            |_, input, _, data| {
                let shared = data.get()?;
                shared
                    .lock()
                    .unwrap()
                    .execution_started
                    .push(input[0].i64().unwrap());
                Ok(())
            },
        ),
        function(
            "bind_observer",
            [ValType::I64],
            [],
            data.clone(),
            |_, input, _, data| {
                let shared = data.get()?;
                shared.lock().unwrap().observer = Some(input[0].i64().unwrap());
                Ok(())
            },
        ),
        function(
            "next_command",
            [],
            [ValType::I32],
            data,
            |_, _, output, data| {
                let shared = data.get()?;
                let mut state = shared.lock().unwrap();
                let (opcode, handle) = state.commands.pop_front().unwrap_or((0, 0));
                state.handle = handle;
                output[0] = Val::I32(opcode);
                Ok(())
            },
        ),
    ]
}

fn plugin(
    variant: Variant,
    tenant: i32,
    restore: i64,
) -> (Result<Plugin, extism::Error>, Arc<Mutex<HostState>>) {
    let data = UserData::new(HostState {
        tenant,
        restore,
        ..HostState::default()
    });
    let shared = data.get().unwrap();
    let wasm = Wasm::data(artifact_bytes(variant).unwrap());
    let manifest = Manifest::new([wasm]);
    let plugin = PluginBuilder::new(manifest)
        .with_wasi(false)
        .with_functions(functions(data))
        .build();
    (plugin, shared)
}

fn call(plugin: &mut Plugin, export: &str) -> Result<Vec<u8>, extism::Error> {
    plugin.call::<&[u8], Vec<u8>>(export, &[])
}

#[test]
fn exact_artifact_runs_through_extism_public_plugin_api() {
    let (plugin, shared) = plugin(Variant::A, 3, 0);
    let mut plugin = plugin.unwrap();
    assert!(call(&mut plugin, "activation_check").unwrap().is_empty());
    assert!(shared.lock().unwrap().results.is_empty());
    shared.lock().unwrap().handle = 70;
    assert!(call(&mut plugin, "increment").unwrap().is_empty());
    shared.lock().unwrap().handle = 71;
    assert!(call(&mut plugin, "probe").unwrap().is_empty());

    let state = shared.lock().unwrap();
    assert_eq!(
        state.activations,
        [
            (3, 1, spec(Variant::A).build_marker),
            (3, 1, spec(Variant::A).build_marker),
        ]
    );
    assert_eq!(
        state
            .results
            .iter()
            .map(|event| (event.handle, event.counter, event.kind))
            .collect::<Vec<_>>(),
        [(70, 1, 1), (71, 1, 2)]
    );
}

#[test]
fn extism_observes_bad_activation_and_intentional_trap() {
    let (bad, shared) = plugin(Variant::BadB, 7, 0);
    if let Ok(mut bad) = bad {
        assert!(call(&mut bad, "activation_check").is_err());
    }
    assert!(shared.lock().unwrap().results.is_empty());

    let (good, _) = plugin(Variant::BadB, 6, 0);
    let mut good = good.unwrap();
    assert!(call(&mut good, "activation_check").is_ok());
    assert!(call(&mut good, "probe").is_ok());
    assert!(call(&mut good, "trap").is_err());
}

#[test]
fn extism_cancel_handle_interrupts_after_guest_marker() {
    let (plugin, shared) = plugin(Variant::A, 1, 0);
    let mut plugin = plugin.unwrap();
    shared.lock().unwrap().handle = 88;
    let cancel = plugin.cancel_handle();
    let worker = thread::spawn(move || call(&mut plugin, "cpu_loop"));

    let deadline = Instant::now() + Duration::from_secs(2);
    while shared.lock().unwrap().execution_started.is_empty() {
        assert!(
            Instant::now() < deadline,
            "guest did not emit execution_started"
        );
        thread::yield_now();
    }
    cancel.cancel().unwrap();
    let result = worker.join().unwrap();
    assert!(result.is_err());
    assert_eq!(shared.lock().unwrap().execution_started, [88]);
}
