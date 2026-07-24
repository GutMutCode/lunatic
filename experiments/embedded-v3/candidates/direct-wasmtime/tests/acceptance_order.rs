use std::sync::{Arc, Mutex};

use embedded_v3_direct_wasmtime::tenant::publish_after_accept;
use tokio::sync::{mpsc, oneshot};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_event_precedes_visibility_to_waiting_worker() {
    let (sender, mut receiver) = mpsc::channel(1);
    let permit = sender.try_reserve().expect("reserve command slot");
    let observed = Arc::new(Mutex::new(Vec::new()));
    let worker_observed = observed.clone();
    let (waiting_tx, waiting_rx) = oneshot::channel();
    let worker = tokio::spawn(async move {
        waiting_tx.send(()).expect("signal waiting worker");
        let command = receiver.recv().await.expect("receive published command");
        worker_observed
            .lock()
            .expect("lock worker observations")
            .push(("terminal", command));
    });
    waiting_rx.await.expect("worker reached receive");

    let accepted_observed = observed.clone();
    publish_after_accept(permit, 42_u8, || {
        accepted_observed
            .lock()
            .expect("lock accepted observations")
            .push(("accepted", 42));
        Ok(())
    })
    .expect("publish accepted command");
    worker.await.expect("join waiting worker");

    assert_eq!(
        *observed.lock().expect("lock final observations"),
        vec![("accepted", 42), ("terminal", 42)]
    );
}
