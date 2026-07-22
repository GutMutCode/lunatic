use std::{future::Future, sync::Arc};

use lunatic_process::{
    env::{Environment, Environments},
    mailbox::MessageMailbox,
    message::Message,
    spawn,
    state::ProcessState,
    ExecutionResult, NativeProcess, Process, Signal,
};

pub struct LegacyProcess {
    pub id: u64,
}

impl Process for LegacyProcess {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, _signal: Signal) {}
}

pub fn legacy_process_and_environment_calls(
    process: Arc<dyn Process>,
    environment: &dyn Environment,
) {
    let process_id = process.id();
    let _: () = environment.add_process(process_id, Arc::clone(&process));
    let _: () = process.send(Signal::Kill);
    let _: () = environment.send(process_id, Signal::Kill);
    let _: () = environment.remove_process(process_id);
}

pub fn legacy_mailbox_push(mailbox: &MessageMailbox) {
    let _: () = mailbox.push(Message::LinkDied(Some(13)));
}

pub async fn legacy_environment_create<E: Environments>(environments: &E) -> Arc<E::Env> {
    environments.create(13).await
}

pub fn legacy_spawn<T, F, K, R>(environment: Arc<dyn Environment>, function: F)
where
    T: ProcessState + Send + Sync + 'static,
    R: Into<ExecutionResult<T>> + Send + 'static,
    K: Future<Output = R> + Send + 'static,
    F: FnOnce(NativeProcess, MessageMailbox) -> K,
{
    let (_join, _process) = spawn(environment, function);
}
