use lunatic_otp_patterns::{
    ChildInfo, ChildSpec, ChildStart, ChildType, ExitReason, RestartPolicy, RestartStrategy,
    ShutdownPolicy, Supervisor, SupervisorHandle, SupervisorSpec,
};
use lunatic_process::{
    env::{Environment, LunaticEnvironment},
    message::{DataMessage, Message},
    spawn_native, DeathReason, Process, Signal,
};
use std::{
    collections::HashMap,
    future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex as StdMutex, OnceLock,
    },
    time::Duration,
};
use tokio::{
    sync::Mutex,
    time::{sleep, timeout},
};

static TEST_LOCK: Mutex<()> = Mutex::const_new(());
static START_EVENTS: OnceLock<StdMutex<Vec<String>>> = OnceLock::new();
static STOP_EVENTS: OnceLock<StdMutex<Vec<String>>> = OnceLock::new();
static IMMEDIATE_ERROR_STARTS: AtomicUsize = AtomicUsize::new(0);
static IMMEDIATE_PANIC_STARTS: AtomicUsize = AtomicUsize::new(0);
static RESTART_PANIC_STARTS: AtomicUsize = AtomicUsize::new(0);

fn start_events() -> &'static StdMutex<Vec<String>> {
    START_EVENTS.get_or_init(|| StdMutex::new(Vec::new()))
}

fn stop_events() -> &'static StdMutex<Vec<String>> {
    STOP_EVENTS.get_or_init(|| StdMutex::new(Vec::new()))
}

struct StopEvent(&'static str);

impl Drop for StopEvent {
    fn drop(&mut self) {
        stop_events()
            .lock()
            .expect("stop event mutex poisoned")
            .push(self.0.to_string());
    }
}

fn spawn_worker(
    environment: Arc<dyn Environment>,
    child_id: &'static str,
) -> Result<Arc<dyn Process>, String> {
    start_events()
        .lock()
        .expect("start event mutex poisoned")
        .push(child_id.to_string());

    let (_join, process) = spawn_native(environment, move |_process, _mailbox| {
        let stop_event = StopEvent(child_id);
        async move {
            let _stop_event = stop_event;
            future::pending().await
        }
    })
    .map_err(|error| error.to_string())?;
    Ok(Arc::new(process))
}

fn start_a(environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
    spawn_worker(environment, "a")
}

fn start_b(environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
    spawn_worker(environment, "b")
}

fn start_c(environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
    spawn_worker(environment, "c")
}

#[derive(Clone, Copy)]
enum TriggeredOutcome {
    Normal,
    Error,
    Panic,
}

fn spawn_triggered_worker(
    environment: Arc<dyn Environment>,
    child_id: &'static str,
    outcome: TriggeredOutcome,
) -> Result<Arc<dyn Process>, String> {
    start_events()
        .lock()
        .expect("start event mutex poisoned")
        .push(child_id.to_string());

    let (_join, process) = spawn_native(environment, move |_process, mailbox| {
        let stop_event = StopEvent(child_id);
        async move {
            let _stop_event = stop_event;
            let _ = mailbox.pop(None).await;
            match outcome {
                TriggeredOutcome::Normal => Ok(()),
                TriggeredOutcome::Error => Err(anyhow::anyhow!("intentional child failure")),
                TriggeredOutcome::Panic => panic!("intentional child panic"),
            }
        }
    })
    .map_err(|error| error.to_string())?;
    Ok(Arc::new(process))
}

fn start_a_normal(environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
    spawn_triggered_worker(environment, "a", TriggeredOutcome::Normal)
}

fn start_a_error(environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
    spawn_triggered_worker(environment, "a", TriggeredOutcome::Error)
}

fn start_b_panic(environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
    spawn_triggered_worker(environment, "b", TriggeredOutcome::Panic)
}

fn spawn_immediate_worker(
    environment: Arc<dyn Environment>,
    child_id: &'static str,
    outcome: TriggeredOutcome,
) -> Result<Arc<dyn Process>, String> {
    start_events()
        .lock()
        .expect("start event mutex poisoned")
        .push(child_id.to_string());

    let (join, process) = spawn_native(environment, move |_process, _mailbox| {
        let stop_event = StopEvent(child_id);
        async move {
            let _stop_event = stop_event;
            match outcome {
                TriggeredOutcome::Normal => Ok(()),
                TriggeredOutcome::Error => Err(anyhow::anyhow!("immediate child failure")),
                TriggeredOutcome::Panic => panic!("immediate child panic"),
            }
        }
    })
    .map_err(|error| error.to_string())?;

    let completed =
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(join)).map_err(
            |error| format!("immediate child task failed outside its process runner: {error}"),
        )?;
    match outcome {
        TriggeredOutcome::Normal if completed.is_err() => {
            return Err("immediate normal child exited abnormally".to_string())
        }
        TriggeredOutcome::Error | TriggeredOutcome::Panic if completed.is_ok() => {
            return Err("immediate failing child exited normally".to_string())
        }
        TriggeredOutcome::Normal | TriggeredOutcome::Error | TriggeredOutcome::Panic => {}
    }
    Ok(Arc::new(process))
}

fn start_a_immediate_normal(environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
    spawn_immediate_worker(environment, "a", TriggeredOutcome::Normal)
}

fn start_a_immediate_error_once(
    environment: Arc<dyn Environment>,
) -> Result<Arc<dyn Process>, String> {
    if IMMEDIATE_ERROR_STARTS.fetch_add(1, Ordering::SeqCst) == 0 {
        spawn_immediate_worker(environment, "a", TriggeredOutcome::Error)
    } else {
        spawn_worker(environment, "a")
    }
}

fn start_a_immediate_panic_once(
    environment: Arc<dyn Environment>,
) -> Result<Arc<dyn Process>, String> {
    if IMMEDIATE_PANIC_STARTS.fetch_add(1, Ordering::SeqCst) == 0 {
        spawn_immediate_worker(environment, "a", TriggeredOutcome::Panic)
    } else {
        spawn_worker(environment, "a")
    }
}

fn panic_child_start(_environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
    panic!("intentional ChildStart panic")
}

fn start_b_panics_on_restart(
    environment: Arc<dyn Environment>,
) -> Result<Arc<dyn Process>, String> {
    if RESTART_PANIC_STARTS.fetch_add(1, Ordering::SeqCst) == 0 {
        spawn_worker(environment, "b")
    } else {
        panic!("intentional ChildStart restart panic")
    }
}

fn child(id: &str, start: ChildStart, restart: RestartPolicy) -> ChildSpec {
    ChildSpec {
        id: id.to_string(),
        start,
        restart,
        shutdown: ShutdownPolicy::Timeout(1_000),
        child_type: ChildType::Worker,
    }
}

fn supervisor(
    environment: Arc<LunaticEnvironment>,
    strategy: RestartStrategy,
    max_restarts: u32,
    children: Vec<ChildSpec>,
) -> Supervisor {
    Supervisor::with_environment(
        SupervisorSpec {
            strategy,
            max_restarts,
            max_seconds: 60,
            children,
        },
        environment,
    )
}

fn child_info(supervisor: &Supervisor) -> HashMap<String, ChildInfo> {
    supervisor
        .which_children()
        .into_iter()
        .map(|child| (child.id.clone(), child))
        .collect()
}

fn handle_child_info(supervisor: &SupervisorHandle) -> HashMap<String, ChildInfo> {
    supervisor
        .which_children()
        .into_iter()
        .map(|child| (child.id.clone(), child))
        .collect()
}

fn process_id(children: &HashMap<String, ChildInfo>, child_id: &str) -> u64 {
    children[child_id]
        .process_id
        .unwrap_or_else(|| panic!("child {child_id} should be active"))
}

fn clear_start_events() {
    start_events()
        .lock()
        .expect("start event mutex poisoned")
        .clear();
    stop_events()
        .lock()
        .expect("stop event mutex poisoned")
        .clear();
}

fn take_start_events() -> Vec<String> {
    std::mem::take(&mut *start_events().lock().expect("start event mutex poisoned"))
}

fn take_stop_events() -> Vec<String> {
    std::mem::take(&mut *stop_events().lock().expect("stop event mutex poisoned"))
}

fn trigger_process(environment: &Arc<LunaticEnvironment>, process_id: u64) {
    environment
        .get_process(process_id)
        .unwrap_or_else(|| panic!("process {process_id} should be registered"))
        .send(Signal::Message(Message::Data(DataMessage::default())))
        .unwrap();
}

async fn wait_for_process_change(
    supervisor: &SupervisorHandle,
    child_id: &str,
    previous_process_id: u64,
) -> u64 {
    timeout(Duration::from_secs(5), async {
        loop {
            let children = handle_child_info(supervisor);
            if let Some(process_id) = children
                .get(child_id)
                .and_then(|child| child.process_id)
                .filter(|process_id| *process_id != previous_process_id)
            {
                return process_id;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("child {child_id} was not restarted"))
}

async fn wait_for_child_stopped(supervisor: &SupervisorHandle, child_id: &str) {
    timeout(Duration::from_secs(5), async {
        loop {
            if handle_child_info(supervisor)
                .get(child_id)
                .is_some_and(|child| child.process_id.is_none())
            {
                return;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("child {child_id} did not stop"));
}

async fn wait_for_restart_count(
    supervisor: &SupervisorHandle,
    child_id: &str,
    expected_restart_count: u32,
) -> ChildInfo {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(child) = handle_child_info(supervisor)
                .remove(child_id)
                .filter(|child| child.restart_count == expected_restart_count)
            {
                return child;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("child {child_id} did not reach restart count {expected_restart_count}")
    })
}

fn monitor_process(target: &dyn Process, observer: Arc<dyn Process>) {
    let (acknowledgement, acknowledged) = std::sync::mpsc::sync_channel(1);
    target
        .send(Signal::Monitor {
            process: observer,
            acknowledgement: Some(acknowledgement),
        })
        .unwrap();
    tokio::task::block_in_place(|| {
        acknowledged
            .recv_timeout(Duration::from_secs(5))
            .expect("monitor relation was not acknowledged")
    });
}

fn process_death_observer(
    environment: Arc<dyn Environment>,
) -> (
    Arc<dyn Process>,
    std::sync::mpsc::Receiver<(u64, DeathReason)>,
) {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let (_join, process) = spawn_native(environment, move |_process, mailbox| async move {
        loop {
            if let Message::ProcessDied { process_id, reason } = mailbox.pop(None).await {
                let _ = sender.send((process_id, reason));
                return Ok(());
            }
        }
    })
    .unwrap();
    (Arc::new(process), receiver)
}

async fn wait_for_empty_environment(environment: &Arc<LunaticEnvironment>) {
    timeout(Duration::from_secs(5), async {
        while environment.process_count() != 0 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("environment still contains processes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_for_one_replaces_only_the_failed_actual_process() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    let environment = Arc::new(LunaticEnvironment::new(101));
    let mut supervisor = supervisor(
        environment.clone(),
        RestartStrategy::OneForOne,
        10,
        vec![
            child("a", start_a, RestartPolicy::Permanent),
            child("b", start_b, RestartPolicy::Permanent),
        ],
    );

    supervisor.start_children().unwrap();
    assert_eq!(take_start_events(), ["a", "b"]);
    let before = child_info(&supervisor);
    let old_a = process_id(&before, "a");
    let old_b = process_id(&before, "b");

    supervisor
        .handle_child_exit("a", ExitReason::Crash)
        .unwrap();

    assert_eq!(take_start_events(), ["a"]);
    assert_eq!(take_stop_events(), ["a"]);
    let after = child_info(&supervisor);
    let new_a = process_id(&after, "a");
    assert_ne!(new_a, old_a);
    assert_eq!(process_id(&after, "b"), old_b);
    assert_eq!(after["a"].restart_count, 1);
    assert_eq!(after["b"].restart_count, 0);
    assert!(environment.get_process(old_a).is_none());
    assert!(environment.get_process(old_b).is_some());
    assert!(environment.get_process(new_a).is_some());

    let duplicate = supervisor.start_child("a").unwrap_err();
    assert!(duplicate.contains("already running"));
    assert_eq!(process_id(&child_info(&supervisor), "a"), new_a);

    supervisor.shutdown().unwrap();
    assert_eq!(environment.process_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_for_all_stops_every_old_process_and_restarts_in_spec_order() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    let environment = Arc::new(LunaticEnvironment::new(102));
    let mut supervisor = supervisor(
        environment.clone(),
        RestartStrategy::OneForAll,
        6,
        vec![
            child("a", start_a, RestartPolicy::Permanent),
            child("b", start_b, RestartPolicy::Permanent),
            child("c", start_c, RestartPolicy::Permanent),
        ],
    );

    supervisor.start_children().unwrap();
    take_start_events();
    let before = child_info(&supervisor);
    let old_ids = ["a", "b", "c"].map(|id| process_id(&before, id));

    supervisor
        .handle_child_exit("b", ExitReason::Crash)
        .unwrap();

    assert_eq!(take_start_events(), ["a", "b", "c"]);
    assert_eq!(take_stop_events(), ["c", "b", "a"]);
    let after = child_info(&supervisor);
    for (child_id, old_id) in ["a", "b", "c"].into_iter().zip(old_ids) {
        assert_ne!(process_id(&after, child_id), old_id);
        assert_eq!(after[child_id].restart_count, 1);
        assert!(environment.get_process(old_id).is_none());
    }
    assert_eq!(
        supervisor
            .restart_history()
            .into_iter()
            .map(|(_, child_id)| child_id)
            .collect::<Vec<_>>(),
        ["a", "b", "c"]
    );

    supervisor.shutdown().unwrap();
    assert_eq!(environment.process_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_for_one_preserves_earlier_processes_and_restarts_the_suffix_in_order() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    let environment = Arc::new(LunaticEnvironment::new(103));
    let mut supervisor = supervisor(
        environment.clone(),
        RestartStrategy::RestForOne,
        6,
        vec![
            child("a", start_a, RestartPolicy::Permanent),
            child("b", start_b, RestartPolicy::Permanent),
            child("c", start_c, RestartPolicy::Permanent),
        ],
    );

    supervisor.start_children().unwrap();
    take_start_events();
    let before = child_info(&supervisor);
    let old_a = process_id(&before, "a");
    let old_b = process_id(&before, "b");
    let old_c = process_id(&before, "c");

    supervisor
        .handle_child_exit("b", ExitReason::Crash)
        .unwrap();

    assert_eq!(take_start_events(), ["b", "c"]);
    assert_eq!(take_stop_events(), ["c", "b"]);
    let after = child_info(&supervisor);
    assert_eq!(process_id(&after, "a"), old_a);
    assert_ne!(process_id(&after, "b"), old_b);
    assert_ne!(process_id(&after, "c"), old_c);
    assert_eq!(after["a"].restart_count, 0);
    assert_eq!(after["b"].restart_count, 1);
    assert_eq!(after["c"].restart_count, 1);
    assert!(environment.get_process(old_a).is_some());
    assert!(environment.get_process(old_b).is_none());
    assert!(environment.get_process(old_c).is_none());

    supervisor.shutdown().unwrap();
    assert_eq!(environment.process_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_policies_apply_to_actual_processes() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();

    let permanent_environment = Arc::new(LunaticEnvironment::new(104));
    let mut permanent = supervisor(
        permanent_environment.clone(),
        RestartStrategy::OneForOne,
        4,
        vec![child("a", start_a, RestartPolicy::Permanent)],
    );
    permanent.start_children().unwrap();
    let permanent_old = process_id(&child_info(&permanent), "a");
    permanent
        .handle_child_exit("a", ExitReason::Normal)
        .unwrap();
    assert_ne!(process_id(&child_info(&permanent), "a"), permanent_old);
    permanent.shutdown().unwrap();

    let transient_environment = Arc::new(LunaticEnvironment::new(105));
    let mut transient = supervisor(
        transient_environment.clone(),
        RestartStrategy::OneForOne,
        4,
        vec![child("a", start_a, RestartPolicy::Transient)],
    );
    transient.start_children().unwrap();
    let transient_old = process_id(&child_info(&transient), "a");
    transient
        .handle_child_exit("a", ExitReason::Normal)
        .unwrap();
    let transient_stopped = child_info(&transient);
    assert!(transient_stopped["a"].process_id.is_none());
    assert_eq!(transient_stopped["a"].restart_count, 0);
    assert!(transient_environment.get_process(transient_old).is_none());
    transient.start_child("a").unwrap();
    let transient_second = process_id(&child_info(&transient), "a");
    transient.handle_child_exit("a", ExitReason::Crash).unwrap();
    assert_ne!(process_id(&child_info(&transient), "a"), transient_second);
    assert_eq!(child_info(&transient)["a"].restart_count, 1);
    transient.shutdown().unwrap();

    let temporary_environment = Arc::new(LunaticEnvironment::new(106));
    let mut temporary = supervisor(
        temporary_environment.clone(),
        RestartStrategy::OneForOne,
        4,
        vec![child("a", start_a, RestartPolicy::Temporary)],
    );
    temporary.start_children().unwrap();
    let temporary_old = process_id(&child_info(&temporary), "a");
    temporary.handle_child_exit("a", ExitReason::Crash).unwrap();
    let temporary_stopped = child_info(&temporary);
    assert!(temporary_stopped["a"].process_id.is_none());
    assert_eq!(temporary_stopped["a"].restart_count, 0);
    assert!(temporary_environment.get_process(temporary_old).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_intensity_rejects_the_next_restart_without_reset_or_duplicate() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    let environment = Arc::new(LunaticEnvironment::new(107));
    let mut supervisor = supervisor(
        environment.clone(),
        RestartStrategy::OneForOne,
        2,
        vec![child("a", start_a, RestartPolicy::Permanent)],
    );

    supervisor.start_children().unwrap();
    supervisor
        .handle_child_exit("a", ExitReason::Crash)
        .unwrap();
    supervisor
        .handle_child_exit("a", ExitReason::Crash)
        .unwrap();
    let before_rejected_restart = child_info(&supervisor);
    let current_id = process_id(&before_rejected_restart, "a");
    assert_eq!(before_rejected_restart["a"].restart_count, 2);

    let error = supervisor
        .handle_child_exit("a", ExitReason::Crash)
        .unwrap_err();
    assert!(error.contains("Restart intensity limit exceeded"));
    let after_rejected_restart = child_info(&supervisor);
    assert_eq!(process_id(&after_rejected_restart, "a"), current_id);
    assert_eq!(after_rejected_restart["a"].restart_count, 2);
    assert_eq!(supervisor.restart_history().len(), 2);
    assert!(environment.get_process(current_id).is_some());

    supervisor.shutdown().unwrap();
    assert_eq!(environment.process_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn automatic_one_for_one_consumes_an_actual_child_failure_event() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    let environment = Arc::new(LunaticEnvironment::new(201));
    let supervisor = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 4,
            max_seconds: 60,
            children: vec![
                child("a", start_a_error, RestartPolicy::Permanent),
                child("b", start_b, RestartPolicy::Permanent),
            ],
        },
        environment.clone(),
    )
    .unwrap();

    assert_eq!(take_start_events(), ["a", "b"]);
    let before = handle_child_info(&supervisor);
    let old_a = process_id(&before, "a");
    let old_b = process_id(&before, "b");

    trigger_process(&environment, old_a);
    let new_a = wait_for_process_change(&supervisor, "a", old_a).await;

    assert_eq!(take_start_events(), ["a"]);
    assert_eq!(take_stop_events(), ["a"]);
    let after = handle_child_info(&supervisor);
    assert_eq!(process_id(&after, "a"), new_a);
    assert_eq!(process_id(&after, "b"), old_b);
    assert_eq!(after["a"].restart_count, 1);
    assert_eq!(after["b"].restart_count, 0);

    supervisor.shutdown().unwrap();
    wait_for_empty_environment(&environment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn automatic_one_for_all_ignores_late_events_from_intentional_kills() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    let environment = Arc::new(LunaticEnvironment::new(202));
    let supervisor = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForAll,
            max_restarts: 6,
            max_seconds: 60,
            children: vec![
                child("a", start_a, RestartPolicy::Permanent),
                child("b", start_b, RestartPolicy::Permanent),
                child("c", start_c, RestartPolicy::Permanent),
            ],
        },
        environment.clone(),
    )
    .unwrap();

    take_start_events();
    let before = handle_child_info(&supervisor);
    let old_ids = ["a", "b", "c"].map(|id| process_id(&before, id));
    environment
        .get_process(old_ids[1])
        .unwrap()
        .send(Signal::Kill)
        .unwrap();

    wait_for_process_change(&supervisor, "b", old_ids[1]).await;
    assert_eq!(take_start_events(), ["a", "b", "c"]);
    assert_eq!(take_stop_events(), ["b", "c", "a"]);
    let after = handle_child_info(&supervisor);
    for (child_id, old_id) in ["a", "b", "c"].into_iter().zip(old_ids) {
        assert_ne!(process_id(&after, child_id), old_id);
        assert_eq!(after[child_id].restart_count, 1);
    }
    assert_eq!(
        supervisor
            .restart_history()
            .into_iter()
            .map(|(_, child_id)| child_id)
            .collect::<Vec<_>>(),
        ["a", "b", "c"]
    );

    sleep(Duration::from_millis(50)).await;
    assert_eq!(supervisor.restart_history().len(), 3);
    assert!(take_start_events().is_empty());

    supervisor.shutdown().unwrap();
    wait_for_empty_environment(&environment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn automatic_rest_for_one_consumes_a_panicking_child_event_in_order() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    let environment = Arc::new(LunaticEnvironment::new(203));
    let supervisor = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::RestForOne,
            max_restarts: 6,
            max_seconds: 60,
            children: vec![
                child("a", start_a, RestartPolicy::Permanent),
                child("b", start_b_panic, RestartPolicy::Permanent),
                child("c", start_c, RestartPolicy::Permanent),
            ],
        },
        environment.clone(),
    )
    .unwrap();

    take_start_events();
    let before = handle_child_info(&supervisor);
    let old_a = process_id(&before, "a");
    let old_b = process_id(&before, "b");
    let old_c = process_id(&before, "c");
    trigger_process(&environment, old_b);

    wait_for_process_change(&supervisor, "b", old_b).await;
    assert_eq!(take_start_events(), ["b", "c"]);
    assert_eq!(take_stop_events(), ["b", "c"]);
    let after = handle_child_info(&supervisor);
    assert_eq!(process_id(&after, "a"), old_a);
    assert_ne!(process_id(&after, "b"), old_b);
    assert_ne!(process_id(&after, "c"), old_c);
    assert_eq!(after["a"].restart_count, 0);
    assert_eq!(after["b"].restart_count, 1);
    assert_eq!(after["c"].restart_count, 1);

    supervisor.shutdown().unwrap();
    wait_for_empty_environment(&environment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn automatic_monitor_reasons_drive_restart_policies() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();

    let permanent_environment = Arc::new(LunaticEnvironment::new(204));
    let permanent = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 2,
            max_seconds: 60,
            children: vec![child("a", start_a_normal, RestartPolicy::Permanent)],
        },
        permanent_environment.clone(),
    )
    .unwrap();
    let permanent_old = process_id(&handle_child_info(&permanent), "a");
    trigger_process(&permanent_environment, permanent_old);
    wait_for_process_change(&permanent, "a", permanent_old).await;
    permanent.shutdown().unwrap();
    wait_for_empty_environment(&permanent_environment).await;

    clear_start_events();
    let transient_environment = Arc::new(LunaticEnvironment::new(205));
    let transient = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 2,
            max_seconds: 60,
            children: vec![child("a", start_a_normal, RestartPolicy::Transient)],
        },
        transient_environment.clone(),
    )
    .unwrap();
    let transient_old = process_id(&handle_child_info(&transient), "a");
    trigger_process(&transient_environment, transient_old);
    wait_for_child_stopped(&transient, "a").await;
    assert_eq!(handle_child_info(&transient)["a"].restart_count, 0);
    transient.shutdown().unwrap();
    wait_for_empty_environment(&transient_environment).await;

    clear_start_events();
    let temporary_environment = Arc::new(LunaticEnvironment::new(206));
    let temporary = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 2,
            max_seconds: 60,
            children: vec![child("a", start_a_error, RestartPolicy::Temporary)],
        },
        temporary_environment.clone(),
    )
    .unwrap();
    let temporary_old = process_id(&handle_child_info(&temporary), "a");
    trigger_process(&temporary_environment, temporary_old);
    wait_for_child_stopped(&temporary, "a").await;
    assert_eq!(handle_child_info(&temporary)["a"].restart_count, 0);
    temporary.shutdown().unwrap();
    wait_for_empty_environment(&temporary_environment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_intensity_escalates_as_an_actual_supervisor_monitor_failure() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    let environment = Arc::new(LunaticEnvironment::new(207));
    let supervisor = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 2,
            max_seconds: 60,
            children: vec![child("a", start_a, RestartPolicy::Permanent)],
        },
        environment.clone(),
    )
    .unwrap();
    let supervisor_id = supervisor.id();
    let (observer, death) = process_death_observer(environment.clone());
    monitor_process(supervisor.process(), observer);

    let first = process_id(&handle_child_info(&supervisor), "a");
    environment
        .get_process(first)
        .unwrap()
        .send(Signal::Kill)
        .unwrap();
    let second = wait_for_process_change(&supervisor, "a", first).await;
    environment
        .get_process(second)
        .unwrap()
        .send(Signal::Kill)
        .unwrap();
    let third = wait_for_process_change(&supervisor, "a", second).await;
    environment
        .get_process(third)
        .unwrap()
        .send(Signal::Kill)
        .unwrap();

    let error = supervisor
        .wait_for_exit(Some(Duration::from_secs(5)))
        .unwrap_err();
    assert!(error.contains("Restart intensity limit exceeded"));
    let (observed_id, reason) = tokio::task::block_in_place(|| {
        death
            .recv_timeout(Duration::from_secs(5))
            .expect("supervisor monitor notification missing")
    });
    assert_eq!(observed_id, supervisor_id);
    assert_eq!(reason, DeathReason::Failure);
    wait_for_empty_environment(&environment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orderly_shutdown_is_reverse_ordered_and_reports_normal_monitor_exit() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    let environment = Arc::new(LunaticEnvironment::new(208));
    let supervisor = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForAll,
            max_restarts: 3,
            max_seconds: 60,
            children: vec![
                child("a", start_a, RestartPolicy::Permanent),
                child("b", start_b, RestartPolicy::Permanent),
                child("c", start_c, RestartPolicy::Permanent),
            ],
        },
        environment.clone(),
    )
    .unwrap();
    take_start_events();
    let supervisor_id = supervisor.id();
    let (observer, death) = process_death_observer(environment.clone());
    monitor_process(supervisor.process(), observer);

    supervisor.shutdown().unwrap();
    assert_eq!(take_stop_events(), ["c", "b", "a"]);
    let (observed_id, reason) = tokio::task::block_in_place(|| {
        death
            .recv_timeout(Duration::from_secs(5))
            .expect("supervisor monitor notification missing")
    });
    assert_eq!(observed_id, supervisor_id);
    assert_eq!(reason, DeathReason::Normal);
    wait_for_empty_environment(&environment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killing_the_supervisor_process_does_not_orphan_children() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    let environment = Arc::new(LunaticEnvironment::new(209));
    let supervisor = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 3,
            max_seconds: 60,
            children: vec![
                child("a", start_a, RestartPolicy::Permanent),
                child("b", start_b, RestartPolicy::Permanent),
            ],
        },
        environment.clone(),
    )
    .unwrap();
    let child_ids: Vec<_> = supervisor
        .which_children()
        .into_iter()
        .map(|child| child.process_id.unwrap())
        .collect();
    let supervisor_id = supervisor.id();
    let (observer, death) = process_death_observer(environment.clone());
    monitor_process(supervisor.process(), observer);

    supervisor.process().send(Signal::Kill).unwrap();
    let error = supervisor
        .wait_for_exit(Some(Duration::from_secs(5)))
        .unwrap_err();
    assert!(error.contains("Process killed"));
    let (observed_id, reason) = tokio::task::block_in_place(|| {
        death
            .recv_timeout(Duration::from_secs(5))
            .expect("supervisor monitor notification missing")
    });
    assert_eq!(observed_id, supervisor_id);
    assert_eq!(reason, DeathReason::Failure);

    wait_for_empty_environment(&environment).await;
    for child_id in child_ids {
        assert!(environment.get_process(child_id).is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_immediately_normal_transient_child_is_observed_without_restart() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    let environment = Arc::new(LunaticEnvironment::new(210));
    let supervisor = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 2,
            max_seconds: 60,
            children: vec![child(
                "a",
                start_a_immediate_normal,
                RestartPolicy::Transient,
            )],
        },
        environment.clone(),
    )
    .unwrap();

    wait_for_child_stopped(&supervisor, "a").await;
    assert_eq!(handle_child_info(&supervisor)["a"].restart_count, 0);
    assert!(supervisor.is_alive());

    supervisor.shutdown().unwrap();
    wait_for_empty_environment(&environment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_immediately_failing_permanent_child_restarts_once() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    IMMEDIATE_ERROR_STARTS.store(0, Ordering::SeqCst);
    let environment = Arc::new(LunaticEnvironment::new(211));
    let supervisor = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 2,
            max_seconds: 60,
            children: vec![child(
                "a",
                start_a_immediate_error_once,
                RestartPolicy::Permanent,
            )],
        },
        environment.clone(),
    )
    .unwrap();

    let restarted = wait_for_restart_count(&supervisor, "a", 1).await;
    assert!(restarted.process_id.is_some());
    assert_eq!(IMMEDIATE_ERROR_STARTS.load(Ordering::SeqCst), 2);

    supervisor.shutdown().unwrap();
    wait_for_empty_environment(&environment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_immediately_panicking_permanent_child_restarts_once() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    IMMEDIATE_PANIC_STARTS.store(0, Ordering::SeqCst);
    let environment = Arc::new(LunaticEnvironment::new(212));
    let supervisor = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 2,
            max_seconds: 60,
            children: vec![child(
                "a",
                start_a_immediate_panic_once,
                RestartPolicy::Permanent,
            )],
        },
        environment.clone(),
    )
    .unwrap();

    let restarted = wait_for_restart_count(&supervisor, "a", 1).await;
    assert!(restarted.process_id.is_some());
    assert_eq!(IMMEDIATE_PANIC_STARTS.load(Ordering::SeqCst), 2);

    supervisor.shutdown().unwrap();
    wait_for_empty_environment(&environment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_initial_child_start_does_not_orphan_earlier_children() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    let environment = Arc::new(LunaticEnvironment::new(213));
    let error = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 2,
            max_seconds: 60,
            children: vec![
                child("a", start_a, RestartPolicy::Permanent),
                child("b", panic_child_start, RestartPolicy::Permanent),
            ],
        },
        environment.clone(),
    )
    .unwrap_err();

    assert!(error.contains("intentional ChildStart panic"));
    wait_for_empty_environment(&environment).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_later_restart_does_not_orphan_the_partial_restart() {
    let _test = TEST_LOCK.lock().await;
    clear_start_events();
    RESTART_PANIC_STARTS.store(0, Ordering::SeqCst);
    let environment = Arc::new(LunaticEnvironment::new(214));
    let supervisor = Supervisor::spawn_with_environment(
        SupervisorSpec {
            strategy: RestartStrategy::OneForAll,
            max_restarts: 4,
            max_seconds: 60,
            children: vec![
                child("a", start_a_error, RestartPolicy::Permanent),
                child("b", start_b_panics_on_restart, RestartPolicy::Permanent),
            ],
        },
        environment.clone(),
    )
    .unwrap();

    let first_a = process_id(&handle_child_info(&supervisor), "a");
    trigger_process(&environment, first_a);
    let error = supervisor
        .wait_for_exit(Some(Duration::from_secs(5)))
        .unwrap_err();

    assert!(error.contains("intentional ChildStart restart panic"));
    assert_eq!(RESTART_PANIC_STARTS.load(Ordering::SeqCst), 2);
    wait_for_empty_environment(&environment).await;
}
