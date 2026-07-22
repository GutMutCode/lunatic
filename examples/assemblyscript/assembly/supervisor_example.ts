// EDUCATIONAL IN-MEMORY SIMULATION ONLY.
//
// WorkerHandle is an ordinary object and failures are invoked manually below.
// This file does not spawn, link, or monitor Lunatic processes and is not a
// guest Supervisor adapter. It is intentionally excluded from the default
// build and E2E verification.

// Restart strategy types
export enum RestartStrategy {
  OneForOne = "one_for_one",
  OneForAll = "one_for_all",
  RestForOne = "rest_for_one",
  SimpleOneForOne = "simple_one_for_one",
}

// Restart policy types
export enum RestartPolicy {
  Permanent = "permanent",
  Transient = "transient",
  Temporary = "temporary",
}

// Shutdown policy types
export enum ShutdownPolicy {
  Brutal = "brutal",
  Timeout = "timeout",
  Infinity = "infinity",
}

// Child specification
export class ChildSpec {
  id: string;
  start: () => WorkerHandle | null;
  restart: RestartPolicy;
  shutdown: ShutdownPolicy;
  childType: string;

  constructor(
    id: string,
    start: () => WorkerHandle | null,
    restart: RestartPolicy = RestartPolicy.Permanent,
    shutdown: ShutdownPolicy = ShutdownPolicy.Brutal,
    childType: string = "worker"
  ) {
    this.id = id;
    this.start = start;
    this.restart = restart;
    this.shutdown = shutdown;
    this.childType = childType;
  }
}

// Supervisor specification
export class SupervisorSpec {
  strategy: RestartStrategy;
  maxRestarts: i32;
  maxSeconds: i32;
  children: ChildSpec[];

  constructor(
    strategy: RestartStrategy = RestartStrategy.OneForOne,
    maxRestarts: i32 = 3,
    maxSeconds: i32 = 5,
    children: ChildSpec[] = []
  ) {
    this.strategy = strategy;
    this.maxRestarts = maxRestarts;
    this.maxSeconds = maxSeconds;
    this.children = children;
  }
}

// Worker process simulation
export class WorkerHandle {
  private id: string;
  private running: bool;
  private restarts: i32;

  constructor(id: string) {
    this.id = id;
    this.running = false;
    this.restarts = 0;
  }

  // Start the worker
  start(): boolean {
    if (this.running) {
      return false;
    }
    this.running = true;
    // In real implementation, this would start a goroutine/worker
    console.log("Worker " + this.id + " started");
    return true;
  }

  // Stop the worker
  stop(): void {
    if (!this.running) {
      return;
    }
    this.running = false;
    console.log("Worker " + this.id + " stopped");
  }

  // Check if worker is running
  isRunning(): boolean {
    return this.running;
  }

  // Get worker info
  info(): Map<string, string> {
    const info = new Map<string, string>();
    info.set("id", this.id);
    info.set("running", this.running ? "true" : "false");
    info.set("restarts", this.restarts.toString());
    return info;
  }

  // Increment restart count
  incrementRestarts(): void {
    this.restarts++;
  }
}

// Supervisor state
export class Supervisor {
  private spec: SupervisorSpec;
  private children: Map<string, WorkerHandle>;
  private restartCount: i32;
  private lastRestart: i64;

  constructor(spec: SupervisorSpec) {
    this.spec = spec;
    this.children = new Map<string, WorkerHandle>();
    this.restartCount = 0;
    this.lastRestart = 0;
  }

  // Start all children
  startChildren(): boolean {
    for (let i = 0; i < this.spec.children.length; i++) {
      const childSpec = this.spec.children[i];
      if (!this.startChild(childSpec.id)) {
        return false;
      }
    }
    return true;
  }

  // Start a specific child
  startChild(childID: string): boolean {
    // Find child spec
    let childSpec: ChildSpec | null = null;
    for (let i = 0; i < this.spec.children.length; i++) {
      if (this.spec.children[i].id == childID) {
        childSpec = this.spec.children[i];
        break;
      }
    }

    if (!childSpec) {
      return false;
    }

    // Create and start worker
    const worker = childSpec.start();
    if (!worker) {
      return false;
    }

    if (!worker.start()) {
      return false;
    }

    this.children.set(childID, worker);
    return true;
  }

  // Handle child termination
  handleChildExit(childID: string, reason: string): boolean {
    const child = this.children.get(childID);
    if (!child) {
      return false;
    }

    // Check if restart is needed
    if (!this.shouldRestart(childID, reason)) {
      this.children.delete(childID);
      return true;
    }

    // Check restart intensity
    if (!this.checkRestartIntensity()) {
      return false;
    }

    // Apply restart strategy
    switch (this.spec.strategy) {
      case RestartStrategy.OneForOne:
        return this.restartChild(childID);
      case RestartStrategy.OneForAll:
        return this.restartAllChildren();
      case RestartStrategy.RestForOne:
        return this.restartFromChild(childID);
      default:
        return this.restartChild(childID);
    }
  }

  // Check if child should be restarted
  private shouldRestart(childID: string, reason: string): boolean {
    // Find child spec
    let childSpec: ChildSpec | null = null;
    for (let i = 0; i < this.spec.children.length; i++) {
      if (this.spec.children[i].id == childID) {
        childSpec = this.spec.children[i];
        break;
      }
    }

    if (!childSpec) {
      return false;
    }

    switch (childSpec.restart) {
      case RestartPolicy.Permanent:
        return true;
      case RestartPolicy.Transient:
        return reason != "normal";
      case RestartPolicy.Temporary:
        return false;
      default:
        return false;
    }
  }

  // Check restart intensity limit
  private checkRestartIntensity(): boolean {
    const now = Date.now();
    const windowStart = now - (this.spec.maxSeconds as i64 * 1000);

    // Reset counter if window has passed
    if (this.lastRestart < windowStart) {
      this.restartCount = 0;
    }

    this.restartCount++;
    this.lastRestart = now;

    return this.restartCount <= this.spec.maxRestarts;
  }

  // Restart a specific child
  private restartChild(childID: string): boolean {
    const child = this.children.get(childID);
    if (child) {
      child.stop();
      child.incrementRestarts();
    }

    return this.startChild(childID);
  }

  // Restart all children
  private restartAllChildren(): boolean {
    const childIDs: string[] = [];
    const values = this.children.values();
    for (let i = 0; i < this.children.size; i++) {
      const child = values[i];
      if (child) {
        childIDs.push(child.info().get("id")!);
      }
    }

    // Stop all children
    for (let i = 0; i < childIDs.length; i++) {
      const child = this.children.get(childIDs[i]);
      if (child) {
        child.stop();
        child.incrementRestarts();
      }
    }

    // Start all children
    for (let i = 0; i < childIDs.length; i++) {
      if (!this.startChild(childIDs[i])) {
        return false;
      }
    }

    return true;
  }

  // Restart child and all children started after it
  private restartFromChild(failedChildID: string): boolean {
    // Find index of failed child
    let failedIndex = -1;
    for (let i = 0; i < this.spec.children.length; i++) {
      if (this.spec.children[i].id == failedChildID) {
        failedIndex = i;
        break;
      }
    }

    if (failedIndex == -1) {
      return false;
    }

    // Collect children to restart
    const childrenToRestart: string[] = [];
    for (let i = failedIndex; i < this.spec.children.length; i++) {
      childrenToRestart.push(this.spec.children[i].id);
    }

    // Stop children
    for (let i = 0; i < childrenToRestart.length; i++) {
      const child = this.children.get(childrenToRestart[i]);
      if (child) {
        child.stop();
        child.incrementRestarts();
      }
    }

    // Start children
    for (let i = 0; i < childrenToRestart.length; i++) {
      if (!this.startChild(childrenToRestart[i])) {
        return false;
      }
    }

    return true;
  }

  // Get children information
  whichChildren(): Map<string, string>[] {
    const result: Map<string, string>[] = [];
    const values = this.children.values();
    for (let i = 0; i < this.children.size; i++) {
      const child = values[i];
      if (child) {
        result.push(child.info());
      }
    }
    return result;
  }

  // Count children
  countChildren(): Map<string, i32> {
    const count = new Map<string, i32>();
    count.set("specs", this.spec.children.length);
    count.set("active", 0);
    count.set("supervisors", 0);
    count.set("workers", 0);

    const values = this.children.values();
    for (let i = 0; i < this.children.size; i++) {
      const child = values[i];
      if (child && child.isRunning()) {
        count.set("active", count.get("active") + 1);
      }
    }

    for (let i = 0; i < this.spec.children.length; i++) {
      const childSpec = this.spec.children[i];
      if (childSpec.childType == "supervisor") {
        count.set("supervisors", count.get("supervisors") + 1);
      } else {
        count.set("workers", count.get("workers") + 1);
      }
    }

    return count;
  }

  // Stop the supervisor
  stop(): void {
    const values = this.children.values();
    for (let i = 0; i < this.children.size; i++) {
      const child = values[i];
      if (child) {
        child.stop();
      }
    }
    this.children.clear();
  }
}

// Global supervisor instance
let globalSupervisor: Supervisor | null = null;

// Create worker factory functions
function createWorker1(): WorkerHandle | null {
  return new WorkerHandle("worker1");
}

function createWorker2(): WorkerHandle | null {
  return new WorkerHandle("worker2");
}

//export run_supervisor_example
export function run_supervisor_example(): void {
  console.log("Running AssemblyScript Supervisor Example...");

  // Create supervisor spec
  const spec = new SupervisorSpec(
    RestartStrategy.OneForOne,
    3, // maxRestarts
    5, // maxSeconds
    [
      new ChildSpec("worker1", createWorker1, RestartPolicy.Permanent, ShutdownPolicy.Timeout, "worker"),
      new ChildSpec("worker2", createWorker2, RestartPolicy.Transient, ShutdownPolicy.Brutal, "worker"),
    ]
  );

  // Create and start supervisor
  globalSupervisor = new Supervisor(spec);
  if (!globalSupervisor.startChildren()) {
    console.log("Failed to start children");
    return;
  }

  // Check initial state
  const count = globalSupervisor.countChildren();
  console.log("Initial count - specs: " + count.get("specs").toString() + ", active: " + count.get("active").toString());

  const children = globalSupervisor.whichChildren();
  console.log("Children: " + children.length.toString());
  for (let i = 0; i < children.length; i++) {
    const child = children[i];
    console.log("  " + child.get("id") + ": running=" + child.get("running") + ", restarts=" + child.get("restarts"));
  }

  // Simulate child failure
  console.log("Simulating worker1 failure...");
  if (!globalSupervisor.handleChildExit("worker1", "crash")) {
    console.log("Failed to handle child exit");
  }

  // Check state after restart
  const countAfter = globalSupervisor.countChildren();
  console.log("After restart - specs: " + countAfter.get("specs").toString() + ", active: " + countAfter.get("active").toString());

  const childrenAfter = globalSupervisor.whichChildren();
  console.log("Children after restart: " + childrenAfter.length.toString());
  for (let i = 0; i < childrenAfter.length; i++) {
    const child = childrenAfter[i];
    console.log("  " + child.get("id") + ": running=" + child.get("running") + ", restarts=" + child.get("restarts"));
  }

  // Cleanup
  if (globalSupervisor) {
    globalSupervisor.stop();
  }

  console.log("AssemblyScript Supervisor example completed successfully!");
}

//export _start
export function _start(): void {
  run_supervisor_example();
}
