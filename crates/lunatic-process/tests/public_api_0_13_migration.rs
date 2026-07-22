use std::{future::Future, sync::Arc};

use anyhow::Result;
use lunatic_process::{
    env::{Environment, Environments, LunaticEnvironment, LunaticEnvironments},
    mailbox::{MailboxPushErrorKind, MessageMailbox},
    message::Message,
    reloadable_state::ReloadableState,
    spawn, spawn_native,
    state::{ProcessState, SignalSendError, SignalSendErrorKind},
    ExecutionResult, NativeProcess, Process, Signal,
};
use tokio::task::JoinHandle;

struct DownstreamProcess {
    id: u64,
}

// A 0.13 implementation returned `()` from `send`. The 0.14 contract returns
// ownership-preserving backpressure and closure errors instead.
impl Process for DownstreamProcess {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, _signal: Signal) -> std::result::Result<(), SignalSendError> {
        Ok(())
    }
}

// Compile the migrated form of the 0.13 `spawn` call. Keeping this in an
// integration test makes it a downstream check: only public items are visible.
#[allow(dead_code)]
fn migrated_spawn<T, F, K, R>(
    environment: Arc<dyn Environment>,
    function: F,
) -> Result<(JoinHandle<Result<T>>, NativeProcess)>
where
    T: ProcessState + Send + Sync + wasmtime::ResourceLimiter + ReloadableState + 'static,
    R: Into<ExecutionResult<T>> + Send + 'static,
    K: Future<Output = R> + Send + 'static,
    F: FnOnce(NativeProcess, MessageMailbox) -> K,
{
    let (join, process) = spawn(environment, function)?;
    Ok((join, process))
}

#[tokio::test]
async fn migrated_0_13_process_environment_and_mailbox_calls_compile_and_run() -> Result<()> {
    let environments = LunaticEnvironments::default();
    let environment = environments.create(41).await?;
    assert!(Arc::ptr_eq(
        &environment,
        &environments
            .get(41)
            .await
            .expect("environment must stay live")
    ));

    let process: Arc<dyn Process> = Arc::new(DownstreamProcess { id: 7 });
    environment.add_process(process.id(), process.clone())?;
    process.send(Signal::DieWhenLinkDies(false))?;
    environment.send(process.id(), Signal::Kill)?;
    assert!(environment.remove_process(process.id()));

    let missing = environment
        .send(process.id(), Signal::Kill)
        .expect_err("a missing destination must return the signal");
    assert_eq!(missing.kind(), SignalSendErrorKind::Closed);
    assert!(matches!(missing.into_signal(), Signal::Kill));

    let mailbox = MessageMailbox::new(1);
    mailbox.push(Message::LinkDied(Some(13)))?;
    let full = mailbox
        .push(Message::LinkDied(Some(14)))
        .expect_err("the bounded mailbox must report backpressure");
    assert_eq!(full.kind(), MailboxPushErrorKind::Full);
    let rejected = full.into_message();
    assert_eq!(mailbox.pop(None).await.tag(), Some(13));
    mailbox.push(rejected)?;
    assert_eq!(mailbox.pop(None).await.tag(), Some(14));

    // Host-side processes that do not own Wasm state use the dedicated 0.14
    // entry point and propagate registration/spawn failure with `?`.
    let native_environment: Arc<dyn Environment> = Arc::new(LunaticEnvironment::new(42));
    let (join, native) = spawn_native(native_environment.clone(), |_process, _mailbox| async {
        Ok(())
    })?;
    join.await??;
    assert!(native_environment.get_process(native.id()).is_none());

    Ok(())
}
