use anyhow::{Error, Result};
use lunatic_otp_patterns::{
    ChildSpec, ChildType, ExitReason, RestartPolicy, RestartStrategy, ShutdownPolicy, Supervisor,
    SupervisorSpec,
};
use lunatic_process::{env::Environment, spawn_native, Process};
use std::{future, sync::Arc};

fn start_worker(environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
    let (_join, process) = spawn_native(environment, |_process, _mailbox| async move {
        future::pending::<anyhow::Result<()>>().await
    })
    .map_err(|error| error.to_string())?;
    Ok(Arc::new(process))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let spec = SupervisorSpec {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 3,
        max_seconds: 5,
        children: vec![ChildSpec {
            id: "worker".to_string(),
            start: start_worker,
            restart: RestartPolicy::Permanent,
            shutdown: ShutdownPolicy::Timeout(1_000),
            child_type: ChildType::Worker,
        }],
    };
    let mut supervisor = Supervisor::new(spec);
    supervisor.start_children().map_err(Error::msg)?;

    let original_process = supervisor.which_children()[0]
        .process_id
        .expect("worker should be running");
    supervisor
        .handle_child_exit("worker", ExitReason::Crash)
        .map_err(Error::msg)?;
    let restarted = &supervisor.which_children()[0];

    println!(
        "worker restarted: {} -> {}, count={}",
        original_process,
        restarted.process_id.expect("worker should be restarted"),
        restarted.restart_count
    );

    supervisor.shutdown().map_err(Error::msg)?;
    Ok(())
}
