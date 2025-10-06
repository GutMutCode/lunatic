/** Exported memory */
export declare const memory: WebAssembly.Memory;
// Exported runtime interface
export declare function __new(size: number, id: number): number;
export declare function __pin(ptr: number): number;
export declare function __unpin(ptr: number): void;
export declare function __collect(): void;
export declare const __rtti_base: number;
/**
 * assembly/counter/_start
 */
export declare function _start(): void;
/**
 * assembly/counter/increment
 * @returns `i32`
 */
export declare function increment(): number;
/**
 * assembly/counter/get_count
 * @returns `i32`
 */
export declare function get_count(): number;
/**
 * assembly/counter/reset
 */
export declare function reset(): void;
/**
 * assembly/counter/set_count
 * @param value `i32`
 */
export declare function set_count(value: number): void;
