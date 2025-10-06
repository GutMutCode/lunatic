//! Counter example in Zig for Lunatic
//! Demonstrates basic WASM functionality

// Global counter variable
var counter: i32 = 0;

// Export increment function
export fn increment() i32 {
    counter += 1;
    return counter;
}

// Export get_count function
export fn get_count() i32 {
    return counter;
}

// Export reset function
export fn reset() void {
    counter = 0;
}

// WASM entry point
export fn _start() void {
    // Initialize counter to 0
    counter = 0;
}
