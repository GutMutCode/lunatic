use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use lunatic_otp_patterns::{
    encode_guest_otp_message, GuestOtpHeader, GUEST_OTP_MAGIC, GUEST_OTP_TIMEOUT,
};
use lunatic_process::{
    env::{Environment, LunaticEnvironment},
    message::{DataMessage, Message},
    runtimes::{
        wasmtime::{default_config, WasmtimeCompiledModule, WasmtimeRuntime},
        RawWasm,
    },
    state::{ProcessState, SignalSendError},
    wasm::spawn_wasm,
    Process, Signal,
};
use lunatic_runtime::{state::DefaultProcessState, DefaultProcessConfig};
use tokio::{sync::RwLock, task::JoinHandle, time::timeout};
use wasmtime::{Linker, Val};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

const OTP_GUEST: &str = r#"
(module
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::message" "data_size" (func $data_size (result i64)))
    (import "lunatic::message" "get_tag" (func $get_tag (result i64)))
    (import "lunatic::message" "read_data" (func $read_data (param i32 i32) (result i32)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))
    (import "lunatic::message" "send" (func $send (param i64) (result i32)))
    (import "lunatic::message" "send_receive_skip_search"
        (func $send_receive (param i64 i64 i64) (result i32)))
    (import "lunatic::message" "write_data" (func $write_data (param i32 i32) (result i32)))
    (import "lunatic::process" "process_id" (func $process_id (result i64)))

    (memory (export "memory") 1)

    (func $assert_status (param $actual i32) (param $expected i32)
        (if (i32.ne (local.get $actual) (local.get $expected)) (then unreachable)))

    (func $header (param $kind i32) (param $caller i64) (param $reply_tag i64)
        (i32.store (i32.const 0) (i32.const 0x3150544f))
        (i32.store (i32.const 4) (local.get $kind))
        (i64.store (i32.const 8) (local.get $caller))
        (i64.store (i32.const 16) (local.get $reply_tag)))

    (func $notify (param $observer i64) (param $tag i64)
        (call $create_data (local.get $tag) (i64.const 0))
        (call $assert_status (call $send (local.get $observer)) (i32.const 0)))

    (func $assert_reply (param $reply_tag i64) (param $expected i64)
        (if (i64.ne (call $data_size) (i64.const 32)) (then unreachable))
        (drop (call $read_data (i32.const 64) (i32.const 32)))
        (if (i32.ne (i32.load (i32.const 64)) (i32.const 0x3150544f))
            (then unreachable))
        (if (i32.ne (i32.load (i32.const 68)) (i32.const 4))
            (then unreachable))
        (if (i64.ne (i64.load (i32.const 72)) (i64.const 0))
            (then unreachable))
        (if (i64.ne (i64.load (i32.const 80)) (local.get $reply_tag))
            (then unreachable))
        (if (i64.ne (i64.load (i32.const 88)) (local.get $expected))
            (then unreachable)))

    ;; Minimal guest SDK server adapter: decode the OTP1 envelope, execute the
    ;; behavior, preserve correlation tags, acknowledge stop, then exit.
    (func (export "server") (param $observer i64)
        (local $count i64)
        (local $kind i32)
        (local $caller i64)
        (local $reply_tag i64)
        (local $operation i32)

        (call $notify (local.get $observer) (i64.const 99))
        (loop $serve
            (call $assert_status
                (call $receive (i32.const 0) (i32.const 0) (i64.const -1))
                (i32.const 0))
            (if (i64.lt_u (call $data_size) (i64.const 24)) (then unreachable))
            (drop (call $read_data (i32.const 0) (i32.const 40)))
            (if (i32.ne (i32.load (i32.const 0)) (i32.const 0x3150544f))
                (then unreachable))
            (local.set $kind (i32.load (i32.const 4)))
            (local.set $caller (i64.load (i32.const 8)))
            (local.set $reply_tag (i64.load (i32.const 16)))

            (if (i32.eq (local.get $kind) (i32.const 2))
                (then
                    (local.set $count (i64.load (i32.const 24)))
                    (br $serve)))

            (if (i32.eq (local.get $kind) (i32.const 1))
                (then
                    (local.set $operation (i32.load (i32.const 24)))
                    ;; Operation 2 intentionally does not reply so the caller
                    ;; exercises the runtime timeout path.
                    (if (i32.eq (local.get $operation) (i32.const 2))
                        (then (br $serve)))
                    (local.set $count
                        (i64.add (local.get $count) (i64.load (i32.const 32))))
                    (call $header
                        (i32.const 4)
                        (i64.const 0)
                        (local.get $reply_tag))
                    (i64.store (i32.const 24) (local.get $count))
                    (call $create_data (local.get $reply_tag) (i64.const 32))
                    (drop (call $write_data (i32.const 0) (i32.const 32)))
                    (call $assert_status
                        (call $send (local.get $caller))
                        (i32.const 0))
                    (br $serve)))

            (if (i32.eq (local.get $kind) (i32.const 3))
                (then
                    (call $header
                        (i32.const 4)
                        (i64.const 0)
                        (local.get $reply_tag))
                    (i64.store (i32.const 24) (local.get $count))
                    (call $create_data (local.get $reply_tag) (i64.const 32))
                    (drop (call $write_data (i32.const 0) (i32.const 32)))
                    (call $assert_status
                        (call $send (local.get $caller))
                        (i32.const 0))
                    (return)))
            unreachable))

    ;; Minimal guest SDK client adapter: cast, correlated call/reply, bounded
    ;; timeout, and acknowledged graceful stop all run inside Wasm.
    (func (export "client") (param $server i64) (param $observer i64)
        (local $self i64)
        (local.set $self (call $process_id))

        ;; cast: set counter to 40
        (call $header (i32.const 2) (i64.const 0) (i64.const 0))
        (i64.store (i32.const 24) (i64.const 40))
        (call $create_data (i64.const 0) (i64.const 32))
        (drop (call $write_data (i32.const 0) (i32.const 32)))
        (call $assert_status (call $send (local.get $server)) (i32.const 0))

        ;; call/reply: add 2 and require the correlated value 42
        (call $header (i32.const 1) (local.get $self) (i64.const 1001))
        (i32.store (i32.const 24) (i32.const 1))
        (i64.store (i32.const 32) (i64.const 2))
        (call $create_data (i64.const 0) (i64.const 40))
        (drop (call $write_data (i32.const 0) (i32.const 40)))
        (call $assert_status
            (call $send_receive (local.get $server) (i64.const 1001) (i64.const 1000))
            (i32.const 0))
        (if (i64.ne (call $get_tag) (i64.const 1001)) (then unreachable))
        (call $assert_reply (i64.const 1001) (i64.const 42))

        ;; bounded timeout: operation 2 deliberately receives no reply
        (call $header (i32.const 1) (local.get $self) (i64.const 1002))
        (i32.store (i32.const 24) (i32.const 2))
        (call $create_data (i64.const 0) (i64.const 28))
        (drop (call $write_data (i32.const 0) (i32.const 28)))
        (call $assert_status
            (call $send_receive (local.get $server) (i64.const 1002) (i64.const 25))
            (i32.const 9027))

        ;; graceful stop: the server acknowledges before exiting
        (call $header (i32.const 3) (local.get $self) (i64.const 1003))
        (call $create_data (i64.const 0) (i64.const 24))
        (drop (call $write_data (i32.const 0) (i32.const 24)))
        (call $assert_status
            (call $send_receive (local.get $server) (i64.const 1003) (i64.const 1000))
            (i32.const 0))
        (call $assert_reply (i64.const 1003) (i64.const 42))

        (call $notify (local.get $observer) (i64.const 42)))
)
"#;

struct TagObserver {
    id: u64,
    tags: tokio::sync::mpsc::UnboundedSender<i64>,
}

impl Process for TagObserver {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, signal: Signal) -> std::result::Result<(), SignalSendError> {
        if let Signal::Message(Message::Data(message)) = signal {
            let _ = self.tags.send(message.tag.unwrap_or(0));
        }
        Ok(())
    }
}

struct Harness {
    environment: Arc<LunaticEnvironment>,
    runtime: WasmtimeRuntime,
    module: Arc<WasmtimeCompiledModule<DefaultProcessState>>,
    registry: Arc<RwLock<HashMap<String, (u64, u64)>>>,
}

impl Harness {
    fn new() -> Result<Self> {
        assert_eq!(GUEST_OTP_MAGIC, u32::from_le_bytes(*b"OTP1"));
        assert_eq!(GUEST_OTP_TIMEOUT, 9_027);

        let runtime = WasmtimeRuntime::new(&default_config())?;
        let raw = RawWasm::new(None, wat::parse_str(OTP_GUEST)?);
        let module = wasmtime::Module::new(runtime.engine(), raw.as_slice())?;
        let mut linker = Linker::<DefaultProcessState>::new(runtime.engine());
        <DefaultProcessState as ProcessState>::register(&mut linker)?;
        let instance_pre = linker.instantiate_pre(&module)?;
        let module = Arc::new(WasmtimeCompiledModule::new(raw, module, instance_pre));
        Ok(Self {
            environment: Arc::new(LunaticEnvironment::new(30)),
            runtime,
            module,
            registry: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    async fn spawn(
        &self,
        function: &str,
        params: Vec<Val>,
    ) -> Result<(JoinHandle<Result<DefaultProcessState>>, Arc<dyn Process>)> {
        let state = DefaultProcessState::new(
            self.environment.clone(),
            None,
            self.runtime.clone(),
            self.module.clone(),
            Arc::new(DefaultProcessConfig::default()),
            self.registry.clone(),
        )?;
        spawn_wasm(
            self.environment.clone(),
            self.runtime.clone(),
            &self.module,
            state,
            function,
            params,
            None,
        )
        .await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_guest_wasm_otp_call_reply_timeout_and_stop_are_end_to_end() -> Result<()> {
    let harness = Harness::new()?;
    let (tag_sender, mut tag_receiver) = tokio::sync::mpsc::unbounded_channel();
    let observer_id = harness.environment.get_next_process_id();
    let observer: Arc<dyn Process> = Arc::new(TagObserver {
        id: observer_id,
        tags: tag_sender,
    });
    harness
        .environment
        .add_process(observer_id, observer.clone())?;

    let (server_join, server) = harness
        .spawn("server", vec![Val::I64(observer_id as i64)])
        .await?;
    let ready = timeout(TEST_TIMEOUT, tag_receiver.recv())
        .await
        .context("guest OTP server did not become ready")?
        .context("guest OTP observer closed before ready")?;
    assert_eq!(ready, 99);

    let server_id = server.id();
    // A host-encoded probe makes the E2E test fail if the public Rust wire
    // contract drifts from the decoder implemented by a guest adapter.
    let probe = encode_guest_otp_message(GuestOtpHeader::cast(), &39_i64.to_le_bytes());
    server.send(Signal::Message(Message::Data(DataMessage::new_from_vec(
        None, probe,
    ))))?;
    let (client_join, client) = harness
        .spawn(
            "client",
            vec![Val::I64(server_id as i64), Val::I64(observer_id as i64)],
        )
        .await?;
    let client_id = client.id();

    timeout(TEST_TIMEOUT, client_join)
        .await
        .context("guest OTP client timed out")?
        .context("guest OTP client task panicked")??;
    timeout(TEST_TIMEOUT, server_join)
        .await
        .context("guest OTP server timed out")?
        .context("guest OTP server task panicked")??;

    let completed = timeout(TEST_TIMEOUT, tag_receiver.recv())
        .await
        .context("guest OTP client did not report completion")?
        .context("guest OTP observer closed before completion")?;
    assert_eq!(completed, 42);
    assert!(harness.environment.get_process(server_id).is_none());
    assert!(harness.environment.get_process(client_id).is_none());
    assert!(harness.environment.remove_process(observer_id));
    Ok(())
}
