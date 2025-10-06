//! Counter example in C for Lunatic
//! Demonstrates basic WASM functionality with exported functions

#include <emscripten.h>

// Global counter variable
static int counter = 0;

// Export increment function
EMSCRIPTEN_KEEPALIVE
int increment() {
    counter++;
    return counter;
}

// Export get_count function
EMSCRIPTEN_KEEPALIVE
int get_count() {
    return counter;
}

// Export reset function
EMSCRIPTEN_KEEPALIVE
void reset() {
    counter = 0;
}

// WASM entry point
EMSCRIPTEN_KEEPALIVE
void _start() {
    // Initialize counter to 0
    counter = 0;
}