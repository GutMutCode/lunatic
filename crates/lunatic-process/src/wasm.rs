use std::sync::{atomic::Ordering, Arc};

use anyhow::{anyhow, Result};
use log::trace;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use wasmtime::{ResourceLimiter, Val};

use crate::env::Environment;
use crate::module_registry::{ModuleRegistry, ProcessKey};
use crate::runtimes::wasmtime::{WasmtimeCompiledModule, WasmtimeRuntime};
use crate::state::ProcessState;
use crate::{
    ExecutionResult, Process, ProcessContext, ProcessReloadStatus, ReloadCommand, Signal,
    WasmProcess,
};

enum WasmExecutionEvent {
    CallFinished(Result<()>),
    Reload(ReloadCommand),
    ReloadDriverClosed,
}

/// Optional lifecycle metadata for a Wasm process spawn.
pub struct WasmSpawnOptions {
    pub link: Option<(Option<i64>, Arc<dyn Process>)>,
    pub initial_module_version: Option<(u64, u32)>,
}

/// Owns the active Wasmtime call and its reload control channel in one future.
///
/// A signal handler must never wait for the instance lock while `call_ref` is
/// alive: the call borrows the Store until its future is dropped. Selecting the
/// command here makes the call future leave scope first, which cancels the
/// suspended Wasmtime fiber and returns exclusive access to the instance before
/// snapshotting it.
async fn run_wasm_execution<S>(
    context: ProcessContext<S>,
    env: Arc<dyn Environment>,
    mut reload_receiver: Receiver<ReloadCommand>,
    function: String,
    params: Vec<Val>,
) -> ExecutionResult<S>
where
    S: ProcessState + Send + ResourceLimiter + 'static,
{
    loop {
        let mut instance_guard = context.instance.write().await;
        let event = {
            let instance = instance_guard
                .as_mut()
                .expect("the Wasm execution driver must own an instance");
            let call = instance.call_ref(&function, params.clone());
            tokio::pin!(call);

            tokio::select! {
                biased;
                command = reload_receiver.recv() => match command {
                    Some(command) => WasmExecutionEvent::Reload(command),
                    None => WasmExecutionEvent::ReloadDriverClosed,
                },
                result = &mut call => WasmExecutionEvent::CallFinished(result),
            }
        };

        match event {
            WasmExecutionEvent::CallFinished(result) => {
                let instance = instance_guard
                    .take()
                    .expect("the completed Wasm call must retain its instance");
                return instance.into_execution_result(result);
            }
            WasmExecutionEvent::ReloadDriverClosed => {
                let instance = instance_guard
                    .take()
                    .expect("the Wasm call must retain its instance");
                return instance.into_execution_result(Err(anyhow!(
                    "Wasm reload execution driver closed unexpectedly"
                )));
            }
            WasmExecutionEvent::Reload(command) => {
                // The call future has left its lexical scope, so its Store
                // borrow and suspended fiber are cancelled before this lock is
                // released and the transaction reacquires it.
                let process_id = {
                    let instance = instance_guard
                        .as_ref()
                        .expect("the Wasm execution driver must retain an instance");
                    instance.state().id()
                };
                drop(instance_guard);

                let (action, module_id, expected_version, target_version, acknowledgement) =
                    command.into_parts();
                let Some(old_version) = context.current_version(module_id) else {
                    crate::acknowledge_reload(
                        acknowledgement,
                        process_id,
                        module_id,
                        0,
                        0,
                        ProcessReloadStatus::Failed(format!(
                            "Process does not run registered module {}",
                            module_id
                        )),
                    );
                    continue;
                };

                if old_version == target_version {
                    crate::acknowledge_reload(
                        acknowledgement,
                        process_id,
                        module_id,
                        old_version,
                        old_version,
                        ProcessReloadStatus::AlreadyAtTarget,
                    );
                    continue;
                }

                if expected_version.is_some_and(|expected| expected != old_version) {
                    crate::acknowledge_reload(
                        acknowledgement,
                        process_id,
                        module_id,
                        old_version,
                        old_version,
                        ProcessReloadStatus::Failed(format!(
                            "Version conflict: expected {:?}, running {}",
                            expected_version, old_version
                        )),
                    );
                    continue;
                }

                context.reload_in_progress.store(true, Ordering::SeqCst);

                let result = crate::perform_pending_reload(
                    &context,
                    env.clone(),
                    module_id,
                    old_version,
                    target_version,
                )
                .await;

                match result {
                    Ok(()) => {
                        match context.transition_current_version(
                            module_id,
                            old_version,
                            target_version,
                        ) {
                            Ok(()) => {
                                log::info!(
                                    "Wasm {:?} applied for module {}: {} -> {}",
                                    action,
                                    module_id,
                                    old_version,
                                    target_version
                                );
                                context.reload_in_progress.store(false, Ordering::SeqCst);
                                crate::acknowledge_reload(
                                    acknowledgement,
                                    process_id,
                                    module_id,
                                    old_version,
                                    target_version,
                                    ProcessReloadStatus::Applied,
                                );
                            }
                            Err(accounting_error) => {
                                let revert_result = crate::perform_pending_reload(
                                    &context,
                                    env.clone(),
                                    module_id,
                                    target_version,
                                    old_version,
                                )
                                .await;
                                context.reload_in_progress.store(false, Ordering::SeqCst);

                                match revert_result {
                                    Ok(()) => crate::acknowledge_reload(
                                        acknowledgement,
                                        process_id,
                                        module_id,
                                        old_version,
                                        old_version,
                                        ProcessReloadStatus::Failed(format!(
                                            "Version accounting commit failed and the instance was reverted: {}",
                                            accounting_error
                                        )),
                                    ),
                                    Err(revert_error) => {
                                        let message = format!(
                                            "Version accounting failed ({}) and instance rollback failed ({})",
                                            accounting_error, revert_error
                                        );
                                        crate::acknowledge_reload(
                                            acknowledgement,
                                            process_id,
                                            module_id,
                                            old_version,
                                            target_version,
                                            ProcessReloadStatus::Failed(message.clone()),
                                        );
                                        let mut instance_guard = context.instance.write().await;
                                        let instance = instance_guard.take().expect(
                                            "failed accounting rollback must retain an instance",
                                        );
                                        return instance
                                            .into_execution_result(Err(anyhow!(message)));
                                    }
                                }
                            }
                        }
                    }
                    Err(error) => {
                        // perform_pending_reload restores the untouched old
                        // instance before returning. Re-enter the same export
                        // with the same parameters on the next loop iteration.
                        log::error!(
                            "Wasm reload rolled back for module {} at version {}: {}",
                            module_id,
                            old_version,
                            error
                        );
                        context.reload_in_progress.store(false, Ordering::SeqCst);
                        crate::acknowledge_reload(
                            acknowledgement,
                            process_id,
                            module_id,
                            old_version,
                            old_version,
                            ProcessReloadStatus::Failed(error.to_string()),
                        );
                    }
                }
            }
        }
    }
}

/// Spawns a new wasm process from a compiled module.
///
/// A `Process` is created from a `module`, entry `function`, array of arguments and config. The
/// configuration will define some characteristics of the process, such as maximum memory, fuel
/// and host function properties (filesystem access, networking, ..).
///
/// After it's spawned the process will keep running in the background. A process can be killed
/// with `Signal::Kill` signal. If you would like to block until the process is finished you can
/// `.await` on the returned `JoinHandle<()>`.
pub async fn spawn_wasm<S>(
    env: Arc<dyn Environment>,
    runtime: WasmtimeRuntime,
    module: &WasmtimeCompiledModule<S>,
    state: S,
    function: &str,
    params: Vec<Val>,
    link: Option<(Option<i64>, Arc<dyn Process>)>,
) -> Result<(JoinHandle<Result<S>>, Arc<dyn Process>)>
where
    S: ProcessState
        + Send
        + Sync
        + ResourceLimiter
        + crate::reloadable_state::ReloadableState
        + 'static,
{
    spawn_wasm_with_options(
        env,
        runtime,
        module,
        state,
        function,
        params,
        WasmSpawnOptions {
            link,
            initial_module_version: None,
        },
    )
    .await
}

/// Spawns a Wasm process and records the registry version that backs its
/// initial instance. Reload-enabled entry points should use this function so a
/// later compatibility check compares against the version actually running.
pub async fn spawn_wasm_with_options<S>(
    env: Arc<dyn Environment>,
    runtime: WasmtimeRuntime,
    module: &WasmtimeCompiledModule<S>,
    state: S,
    function: &str,
    params: Vec<Val>,
    options: WasmSpawnOptions,
) -> Result<(JoinHandle<Result<S>>, Arc<dyn Process>)>
where
    S: ProcessState
        + Send
        + Sync
        + ResourceLimiter
        + crate::reloadable_state::ReloadableState
        + 'static,
{
    let WasmSpawnOptions {
        link,
        initial_module_version,
    } = options;
    let id = state.id();
    let exit_hook = state.process_exit_hook();
    trace!("Spawning process: {}", id);
    let signal_mailbox = state.signal_mailbox().clone();
    let message_mailbox = state.message_mailbox().clone();
    // Claim and publish the bounded process slot before allocating a Wasm
    // Store or running module initialization. If any subsequent setup fails,
    // the registration guard removes the provisional process automatically.
    let child_process_handle = Arc::new(WasmProcess::new(id, signal_mailbox.0.clone()));
    let registration = crate::env::ProcessRegistration::register_with_exit_hook(
        env.clone(),
        id,
        child_process_handle.clone(),
        exit_hook,
    )?;

    let instance = runtime.instantiate(module, state).await?;
    let function = function.to_string();
    let context = crate::ProcessContext::new(instance);
    if let Some((module_id, version)) = initial_module_version {
        let registry = env
            .get_module_registry()
            .ok_or_else(|| anyhow!("ModuleRegistry not available in environment"))?
            .downcast::<ModuleRegistry<S>>()
            .map_err(|_| anyhow!("Failed to downcast ModuleRegistry"))?;
        context.track_initial_version(
            registry,
            module_id,
            version,
            ProcessKey {
                environment_id: env.id(),
                process_id: id,
            },
        )?;
    }
    let reload_receiver = context.take_reload_receiver();

    // The runtime owns one epoch ticker per Engine, shared by every process
    // instantiated from that runtime.

    let fut = run_wasm_execution(
        context.clone(),
        env.clone(),
        reload_receiver,
        function,
        params,
    );
    // **Child link guarantees**:
    // The link signal is going to be put inside of the child's mailbox and is going to be
    // processed before any child code can run. This means that any failure inside the child
    // Wasm code will be correctly reported to the parent.
    //
    // We assume here that the code inside of `process::new()` will not fail during signal
    // handling.
    //
    // **Parent link guarantees**:
    // A `tokio::task::yield_now()` call is executed to allow the parent to link the child
    // before continuing any further execution. This should force the parent to process all
    // signals right away.
    //
    // The parent could have received a `kill` signal in its mailbox before this function was
    // called and this signal is going to be processed before the link is established (FIFO).
    // Only after the yield function we can guarantee that the child is going to be notified
    // if the parent fails. This is ok, as the actual spawning of the child happens after the
    // call, so the child wouldn't even exist if the parent failed before.
    //
    // TODO: The guarantees provided here don't hold anymore in a distributed environment and
    //       will require some rethinking. This function will be executed on a completely
    //       different computer and needs to be synced in a more robust way with the parent
    //       running somewhere else.
    if let Some((tag, process)) = link {
        // Send signal to itself to perform the linking
        process
            .send(Signal::Link(None, child_process_handle.clone()))
            .map_err(|error| anyhow!("failed to link spawning process to child {id}: {error}"))?;
        // Suspend itself to process all new signals
        tokio::task::yield_now().await;
        // Send signal to child to link it
        signal_mailbox
            .0
            .send(Signal::Link(tag, process))
            .map_err(|error| anyhow!("failed to link child {id} to spawning process: {error}"))?;
    }

    let child_process = crate::new(
        fut,
        id,
        signal_mailbox.1,
        message_mailbox,
        Some(context),
        registration,
    );

    // Spawn a background process
    trace!("Process size: {}", std::mem::size_of_val(&child_process));
    let join = tokio::task::spawn(child_process);
    Ok((join, child_process_handle))
}
