/** Exported memory */
export declare const memory: WebAssembly.Memory;
// Exported runtime interface
export declare function __new(size: number, id: number): number;
export declare function __pin(ptr: number): number;
export declare function __unpin(ptr: number): void;
export declare function __collect(): void;
export declare const __rtti_base: number;
/** assembly/gen_server_example/CounterRequestType */
export declare enum CounterRequestType {
  /** @type `i32` */
  Increment,
  /** @type `i32` */
  Decrement,
  /** @type `i32` */
  Get,
  /** @type `i32` */
  Set,
}
/** assembly/gen_server_example/CounterResponseType */
export declare enum CounterResponseType {
  /** @type `i32` */
  Ok,
  /** @type `i32` */
  Value,
  /** @type `i32` */
  Error,
}
/**
 * assembly/gen_server_example/initCounterServer
 */
export declare function initCounterServer(): void;
/**
 * assembly/gen_server_example/callCounter
 * @param requestType `i32`
 * @param value `i32`
 * @returns `i32`
 */
export declare function callCounter(requestType: number, value?: number): number;
/**
 * assembly/gen_server_example/castCounter
 * @param requestType `i32`
 * @param value `i32`
 */
export declare function castCounter(requestType: number, value?: number): void;
/**
 * assembly/gen_server_example/getCounterValue
 * @returns `i32`
 */
export declare function getCounterValue(): number;
/**
 * assembly/gen_server_example/run_gen_server_example
 */
export declare function run_gen_server_example(): void;
/**
 * assembly/gen_server_example/_start
 */
export declare function _start(): void;
