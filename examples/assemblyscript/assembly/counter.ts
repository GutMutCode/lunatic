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

// Simple print function using WASI
function print(message: string): void {
  // In a real implementation, this would use WASI fd_write
  // For now, this is a placeholder that demonstrates the API
  // The actual implementation would need to interface with Lunatic's
  // logging facilities
}

// Memory exports for Lunatic
export { memory };
