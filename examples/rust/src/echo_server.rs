// Echo server example demonstrating more advanced Rust concepts
// This shows how Rust's rich type system can be used in WASM modules

use std::collections::HashMap;

// Message buffer for storing recent messages
static mut MESSAGE_BUFFER: Option<HashMap<u32, String>> = None;
static mut MESSAGE_ID: u32 = 0;

#[no_mangle]
pub extern "C" fn _start() {
    unsafe {
        MESSAGE_BUFFER = Some(HashMap::new());
        MESSAGE_ID = 0;
    }
    println!("Echo Server initialized!");
}

#[no_mangle]
pub extern "C" fn store_message(msg_ptr: *const u8, msg_len: usize) -> u32 {
    unsafe {
        // Convert raw pointer to string
        let bytes = std::slice::from_raw_parts(msg_ptr, msg_len);
        let message = String::from_utf8_lossy(bytes).to_string();

        // Store in buffer
        if let Some(ref mut buffer) = MESSAGE_BUFFER {
            MESSAGE_ID += 1;
            buffer.insert(MESSAGE_ID, message);
            MESSAGE_ID
        } else {
            0
        }
    }
}

#[no_mangle]
pub extern "C" fn get_message_count() -> u32 {
    unsafe {
        MESSAGE_BUFFER.as_ref()
            .map(|b| b.len() as u32)
            .unwrap_or(0)
    }
}

#[no_mangle]
pub extern "C" fn clear_messages() {
    unsafe {
        if let Some(ref mut buffer) = MESSAGE_BUFFER {
            buffer.clear();
        }
    }
}

// Example of processing messages
#[no_mangle]
pub extern "C" fn process_messages() -> u32 {
    unsafe {
        MESSAGE_BUFFER.as_ref()
            .map(|buffer| {
                // Count total characters across all messages
                buffer.values()
                    .map(|msg| msg.len())
                    .sum::<usize>() as u32
            })
            .unwrap_or(0)
    }
}

fn main() {
    // Required for compilation but not called in WASM
}
