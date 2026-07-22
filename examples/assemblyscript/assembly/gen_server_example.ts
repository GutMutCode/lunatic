// EDUCATIONAL IN-MEMORY SIMULATION ONLY.
//
// This file demonstrates the shape of a GenServer-style API with ordinary
// AssemblyScript objects. It does not import Lunatic host functions, spawn a
// Lunatic process, use a mailbox, or implement the OTP1 guest wire contract.
// It is intentionally excluded from the default build and E2E verification.

// Message types for GenServer communication
export enum CounterRequestType {
  Increment = 0,
  Decrement = 1,
  Get = 2,
  Set = 3,
}

export enum CounterResponseType {
  Ok = 0,
  Value = 1,
  Error = 2,
}

export class CounterRequest {
  type: CounterRequestType;
  value: i32;

  constructor(type: CounterRequestType, value: i32 = 0) {
    this.type = type;
    this.value = value;
  }
}

export class CounterResponse {
  type: CounterResponseType;
  value: i32;

  constructor(type: CounterResponseType, value: i32 = 0) {
    this.type = type;
    this.value = value;
  }
}

// Counter GenServer state
export class CounterServer {
  private count: i32 = 0;

  // Handle synchronous calls (like GenServer.call)
  handleCall(request: CounterRequest): CounterResponse {
    switch (request.type) {
      case CounterRequestType.Increment:
        this.count++;
        return new CounterResponse(CounterResponseType.Ok, this.count);
      case CounterRequestType.Decrement:
        this.count--;
        return new CounterResponse(CounterResponseType.Ok, this.count);
      case CounterRequestType.Get:
        return new CounterResponse(CounterResponseType.Value, this.count);
      case CounterRequestType.Set:
        this.count = request.value;
        return new CounterResponse(CounterResponseType.Ok, this.count);
      default:
        return new CounterResponse(CounterResponseType.Error, 0);
    }
  }

  // Handle asynchronous casts (like GenServer.cast)
  handleCast(request: CounterRequest): void {
    switch (request.type) {
      case CounterRequestType.Increment:
        this.count++;
        break;
      case CounterRequestType.Decrement:
        this.count--;
        break;
      case CounterRequestType.Set:
        this.count = request.value;
        break;
      // "get" cast is ignored (no response)
    }
  }

  // Get current count (for testing)
  getCount(): i32 {
    return this.count;
  }
}

// GenServer handle for external communication
export class CounterHandle {
  private server: CounterServer;

  constructor() {
    this.server = new CounterServer();
  }

  // Synchronous call (blocks until response)
  call(requestType: CounterRequestType, value: i32 = 0): CounterResponse {
    const request = new CounterRequest(requestType, value);
    return this.server.handleCall(request);
  }

  // Asynchronous cast (fire and forget)
  cast(requestType: CounterRequestType, value: i32 = 0): void {
    const request = new CounterRequest(requestType, value);
    this.server.handleCast(request);
  }

  // Get current count (for testing)
  getCount(): i32 {
    return this.server.getCount();
  }
}

// Global counter handle for the example
let globalCounter: CounterHandle | null = null;

// Initialize the counter server
export function initCounterServer(): void {
  globalCounter = new CounterHandle();
}

// Synchronous call to global counter
export function callCounter(requestType: CounterRequestType, value: i32 = 0): i32 {
  if (!globalCounter) {
    initCounterServer();
  }
  const response = globalCounter!.call(requestType, value);
  return response.value;
}

// Asynchronous cast to global counter
export function castCounter(requestType: CounterRequestType, value: i32 = 0): void {
  if (!globalCounter) {
    initCounterServer();
  }
  globalCounter!.cast(requestType, value);
}

// Get current counter value
export function getCounterValue(): i32 {
  if (!globalCounter) {
    initCounterServer();
  }
  return globalCounter!.getCount();
}

//export run_gen_server_example
export function run_gen_server_example(): void {
  console.log("Running AssemblyScript GenServer Example...");

  // Initialize counter server
  initCounterServer();

  // Test synchronous calls
  console.log("Initial value: " + getCounterValue().toString());

  callCounter(CounterRequestType.Increment);
  console.log("After increment: " + getCounterValue().toString());

  callCounter(CounterRequestType.Set, 42);
  console.log("After set to 42: " + getCounterValue().toString());

  callCounter(CounterRequestType.Decrement);
  console.log("After decrement: " + getCounterValue().toString());

  // Test asynchronous casts
  castCounter(CounterRequestType.Increment);
  castCounter(CounterRequestType.Increment);
  console.log("After 2 casts: " + getCounterValue().toString());

  console.log("AssemblyScript GenServer example completed successfully!");
}

//export _start
export function _start(): void {
  run_gen_server_example();
}
