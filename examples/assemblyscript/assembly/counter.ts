// Simple counter example for Lunatic runtime
// Demonstrates basic WASM compilation from AssemblyScript

// Global counter state
let counter: i32 = 0;

// Initialize the counter
export function _start(): void {
  counter = 0;
  // Print initial message
  const msg = "AssemblyScript Counter initialized!";
  print(msg);
}

// Increment counter and return new value
export function increment(): i32 {
  counter += 1;
  return counter;
}

// Get current counter value
export function get_count(): i32 {
  return counter;
}

// Reset counter to zero
export function reset(): void {
  counter = 0;
}

// Set counter to specific value
export function set_count(value: i32): void {
  counter = value;
}

// Placeholder for a future Lunatic logging binding. This function deliberately
// emits no WASI or JavaScript `env` import.
function print(message: string): void {
  // Keep the parameter referenced so the example stays warning-free.
  if (message.length == 0) return;
}
