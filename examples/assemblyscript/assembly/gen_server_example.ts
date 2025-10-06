// GenServer pattern implementation in AssemblyScript for Lunatic
// This demonstrates OTP-style message passing using TypeScript classes

// Message types for GenServer communication
export class CounterRequest {
  type: string;
  value: i32;
  reply: CounterResponse | null;

  constructor(type: string, value: i32 = 0, reply: CounterResponse | null = null) {
    this.type = type;
    this.value = value;
    this.reply = reply;
  }
}

export class CounterResponse {
  type: string;
  value: i32;

  constructor(type: string, value: i32 = 0) {
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
      case "increment":
        this.count++;
        return new CounterResponse("ok", this.count);
      case "decrement":
        this.count--;
        return new CounterResponse("ok", this.count);
      case "get":
        return new CounterResponse("value", this.count);
      case "set":
        this.count = request.value;
        return new CounterResponse("ok", this.count);
      default:
        return new CounterResponse("error", 0);
    }
  }

  // Handle asynchronous casts (like GenServer.cast)
  handleCast(request: CounterRequest): void {
    switch (request.type) {
      case "increment":
        this.count++;
        break;
      case "decrement":
        this.count--;
        break;
      case "set":
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
  call(requestType: string, value: i32 = 0): CounterResponse {
    const request = new CounterRequest(requestType, value);
    return this.server.handleCall(request);
  }

  // Asynchronous cast (fire and forget)
  cast(requestType: string, value: i32 = 0): void {
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
export function callCounter(requestType: string, value: i32 = 0): i32 {
  if (!globalCounter) {
    initCounterServer();
  }
  const response = globalCounter!.call(requestType, value);
  return response.value;
}

// Asynchronous cast to global counter
export function castCounter(requestType: string, value: i32 = 0): void {
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

  callCounter("increment");
  console.log("After increment: " + getCounterValue().toString());

  callCounter("set", 42);
  console.log("After set to 42: " + getCounterValue().toString());

  callCounter("decrement");
  console.log("After decrement: " + getCounterValue().toString());

  // Test asynchronous casts
  castCounter("increment");
  castCounter("increment");
  console.log("After 2 casts: " + getCounterValue().toString());

  console.log("AssemblyScript GenServer example completed successfully!");
}

//export _start
export function _start(): void {
  run_gen_server_example();
}