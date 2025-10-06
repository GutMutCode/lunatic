// Simple counter example demonstrating Rust → WASM compilation for Lunatic
// This is a minimal example showing state management in WASM

use std::sync::atomic::{AtomicI32, Ordering};

// Global counter using atomic for thread-safety (though not needed in WASM single-threaded context)
static COUNTER: AtomicI32 = AtomicI32::new(0);

#[no_mangle]
pub extern "C" fn _start() {
    COUNTER.store(0, Ordering::SeqCst);
    println!("Rust Counter initialized!");
}

#[no_mangle]
pub extern "C" fn increment() -> i32 {
    COUNTER.fetch_add(1, Ordering::SeqCst) + 1
}

#[no_mangle]
pub extern "C" fn get_count() -> i32 {
    COUNTER.load(Ordering::SeqCst)
}

#[no_mangle]
pub extern "C" fn reset() {
    COUNTER.store(0, Ordering::SeqCst);
}

#[no_mangle]
pub extern "C" fn set_count(value: i32) {
    COUNTER.store(value, Ordering::SeqCst);
}

#[no_mangle]
pub extern "C" fn multiply_count(factor: i32) -> i32 {
    let current = COUNTER.load(Ordering::SeqCst);
    let new_value = current * factor;
    COUNTER.store(new_value, Ordering::SeqCst);
    new_value
}

#[no_mangle]
pub extern "C" fn is_even() -> bool {
    COUNTER.load(Ordering::SeqCst) % 2 == 0
}

#[no_mangle]
pub extern "C" fn add_batch(values: &[i32]) -> i32 {
    let sum: i32 = values.iter().sum();
    COUNTER.fetch_add(sum, Ordering::SeqCst) + sum
}

// Required for WASM modules
fn main() {
    // Not called in WASM context, but required for compilation
}
