use lunatic_otp_patterns::{
    ChildInfo, ChildSpec, ChildStart, ChildType, ExitReason, RestartPolicy, RestartStrategy,
    ShutdownPolicy, Supervisor, SupervisorSpec,
};
use lunatic_process::{
    env::{Environment, LunaticEnvironment},
    spawn_native, Process,
};
use std::{
    collections::HashMap,
    future,
    sync::{Arc, Mutex, OnceLock},
};

static TEST_LOCK: Mutex<()> = Mutex::new(());
static START_EVENTS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
static STOP_EVENTS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

fn start_events() -> &'static Mutex<Vec<String>> {
    START_EVENTS.get_or_init(|| Mutex::new(Vec::new()))
}

fn stop_events() -> &'static Mutex<Vec<String>> {
    STOP_EVENTS.get_or_init(|| Mutex::new(Vec::new()))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_for_one_replaces_only_the_failed_actual_process() {
    let _test = TEST_LOCK.lock().expect("test mutex poisoned");
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
    let _test = TEST_LOCK.lock().expect("test mutex poisoned");
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
    let _test = TEST_LOCK.lock().expect("test mutex poisoned");
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
    let _test = TEST_LOCK.lock().expect("test mutex poisoned");
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
    let _test = TEST_LOCK.lock().expect("test mutex poisoned");
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
