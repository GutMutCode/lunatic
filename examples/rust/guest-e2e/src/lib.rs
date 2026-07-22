#![no_std]

use core::panic::PanicInfo;

const STATUS_OK: u32 = 0;
const STATUS_ERROR: u32 = 1;
const STATUS_TIMEOUT: u32 = 9_027;

const REQUEST_TAG: i64 = 41;
const REPLY_TAG: i64 = 42;
const COMPLETION_TAG: i64 = 4_201;
const MESSAGE_TIMEOUT_MS: u64 = 5_000;
const TIMEOUT_PROBE_MS: u64 = 25;

#[link(wasm_import_module = "lunatic::error")]
unsafe extern "C" {
    fn string_size(error_id: u64) -> u32;
    fn to_string(error_id: u64, error_str_ptr: *mut u8);
    #[link_name = "drop"]
    fn drop_error(error_id: u64);
}

#[link(wasm_import_module = "lunatic::message")]
unsafe extern "C" {
    fn create_data(tag: i64, buffer_capacity: u64);
    fn data_size() -> u64;
    fn get_tag() -> i64;
    fn read_data(data_ptr: *mut u8, data_len: u32) -> u32;
    fn receive(tags_ptr: *const i64, tags_len: u32, timeout_ms: u64) -> u32;
    fn send(process_id: u64) -> u32;
    fn send_receive_skip_search(process_id: u64, wait_on_tag: i64, timeout_ms: u64) -> u32;
    fn write_data(data_ptr: *const u8, data_len: u32) -> u32;
}

#[link(wasm_import_module = "lunatic::process")]
unsafe extern "C" {
    fn create_config() -> i64;
    fn process_id() -> u64;
    fn spawn(
        link: i64,
        config_id: i64,
        module_id: i64,
        function_ptr: *const u8,
        function_len: u32,
        params_ptr: *const u8,
        params_len: u32,
        id_ptr: *mut u64,
    ) -> u32;
}

#[panic_handler]
fn panic(_info: &PanicInfo<'_>) -> ! {
    trap()
}

#[cold]
#[inline(never)]
#[cfg(target_arch = "wasm32")]
fn trap() -> ! {
    core::arch::wasm32::unreachable()
}

#[cold]
#[inline(never)]
#[cfg(not(target_arch = "wasm32"))]
fn trap() -> ! {
    loop {
        core::hint::spin_loop();
    }
}

#[inline]
fn require(condition: bool) {
    if !condition {
        trap();
    }
}

fn encode_i64(value: i64) -> [u8; 17] {
    let mut encoded = [0_u8; 17];
    encoded[0] = 0x7e;
    encoded[1..].copy_from_slice(&(value as u128).to_le_bytes());
    encoded
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|candidate| candidate == needle)
}

fn send_value(process: u64, tag: i64, value: i64) -> u32 {
    let payload = value.to_le_bytes();
    unsafe {
        create_data(tag, payload.len() as u64);
        require(write_data(payload.as_ptr(), payload.len() as u32) == payload.len() as u32);
        send(process)
    }
}

fn assert_current_message(tag: i64, value: i64) {
    unsafe {
        require(get_tag() == tag);
        require(data_size() == 8);

        let mut payload = [0_u8; 8];
        require(read_data(payload.as_mut_ptr(), payload.len() as u32) == payload.len() as u32);
        require(i64::from_le_bytes(payload) == value);
    }
}

fn validate_spawn_denial() {
    const PROBE_EXPORT: &[u8] = b"permission_probe";
    const EXPECTED_ERROR_FRAGMENT: &[u8] = b"permissions to spawn";

    let mut error_id = u64::MAX;
    let status = unsafe {
        spawn(
            0,
            -1,
            -1,
            PROBE_EXPORT.as_ptr(),
            PROBE_EXPORT.len() as u32,
            core::ptr::null(),
            0,
            &mut error_id,
        )
    };
    require(status == STATUS_ERROR);
    require(error_id != u64::MAX);

    let error_len = unsafe { string_size(error_id) } as usize;
    let mut error = [0_u8; 96];
    require(error_len > 0 && error_len <= error.len());
    unsafe {
        to_string(error_id, error.as_mut_ptr());
        drop_error(error_id);
    }
    require(contains(&error[..error_len], EXPECTED_ERROR_FRAGMENT));
}

fn run_parent(observer_id: i64) {
    require(observer_id >= 0);

    let self_id = unsafe { process_id() };
    require(self_id > 0 && self_id <= i64::MAX as u64);

    let child_config = unsafe { create_config() };
    require(child_config >= 0);

    let child_params = encode_i64(self_id as i64);
    let child_export = b"child";
    let mut child_id = u64::MAX;
    let spawn_status = unsafe {
        spawn(
            0,
            child_config,
            -1,
            child_export.as_ptr(),
            child_export.len() as u32,
            child_params.as_ptr(),
            child_params.len() as u32,
            &mut child_id,
        )
    };
    require(spawn_status == STATUS_OK);
    require(child_id != u64::MAX);

    let request = REQUEST_TAG.to_le_bytes();
    let roundtrip_status = unsafe {
        create_data(REQUEST_TAG, request.len() as u64);
        require(write_data(request.as_ptr(), request.len() as u32) == request.len() as u32);
        send_receive_skip_search(child_id, REPLY_TAG, MESSAGE_TIMEOUT_MS)
    };
    require(roundtrip_status == STATUS_OK);
    assert_current_message(REPLY_TAG, REPLY_TAG);

    let timeout_tag = [REPLY_TAG + 1];
    let timeout_status = unsafe { receive(timeout_tag.as_ptr(), 1, TIMEOUT_PROBE_MS) };
    require(timeout_status == STATUS_TIMEOUT);

    if observer_id != 0 {
        unsafe {
            create_data(COMPLETION_TAG, 0);
            require(send(observer_id as u64) == STATUS_OK);
        }
    }
}

/// CLI entry point: run the same self-test without an external observer.
#[no_mangle]
pub extern "C" fn _start() {
    run_parent(0);
}

/// Host integration-test entry point. A non-zero observer receives tag 4201 on success.
#[no_mangle]
pub extern "C" fn parent(observer_id: i64) {
    run_parent(observer_id);
}

/// Same-module child spawned under a newly-created, spawn-denied configuration.
#[no_mangle]
pub extern "C" fn child(parent_id: i64) {
    require(parent_id > 0);
    let parent_id = parent_id as u64;

    validate_spawn_denial();

    let request_tags = [REQUEST_TAG];
    let receive_status = unsafe { receive(request_tags.as_ptr(), 1, MESSAGE_TIMEOUT_MS) };
    require(receive_status == STATUS_OK);
    assert_current_message(REQUEST_TAG, REQUEST_TAG);

    require(send_value(parent_id, REPLY_TAG, REPLY_TAG) == STATUS_OK);
}

/// A harmless valid export used to prove that the child's spawn failure is permission-based.
#[no_mangle]
pub extern "C" fn permission_probe() {}
