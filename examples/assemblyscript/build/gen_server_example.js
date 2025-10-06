async function instantiate(module, imports = {}) {
  const adaptedImports = {
    env: Object.assign(Object.create(globalThis), imports.env || {}, {
      abort(message, fileName, lineNumber, columnNumber) {
        // ~lib/builtins/abort(~lib/string/String | null?, ~lib/string/String | null?, u32?, u32?) => void
        message = __liftString(message >>> 0);
        fileName = __liftString(fileName >>> 0);
        lineNumber = lineNumber >>> 0;
        columnNumber = columnNumber >>> 0;
        (() => {
          // @external.js
          throw Error(`${message} in ${fileName}:${lineNumber}:${columnNumber}`);
        })();
      },
      "console.log"(text) {
        // ~lib/bindings/dom/console.log(~lib/string/String) => void
        text = __liftString(text >>> 0);
        console.log(text);
      },
    }),
  };
  const { exports } = await WebAssembly.instantiate(module, adaptedImports);
  const memory = exports.memory || imports.env.memory;
  const adaptedExports = Object.setPrototypeOf({
    CounterRequestType: (values => (
      // assembly/gen_server_example/CounterRequestType
      values[values.Increment = exports["CounterRequestType.Increment"].valueOf()] = "Increment",
      values[values.Decrement = exports["CounterRequestType.Decrement"].valueOf()] = "Decrement",
      values[values.Get = exports["CounterRequestType.Get"].valueOf()] = "Get",
      values[values.Set = exports["CounterRequestType.Set"].valueOf()] = "Set",
      values
    ))({}),
    CounterResponseType: (values => (
      // assembly/gen_server_example/CounterResponseType
      values[values.Ok = exports["CounterResponseType.Ok"].valueOf()] = "Ok",
      values[values.Value = exports["CounterResponseType.Value"].valueOf()] = "Value",
      values[values.Error = exports["CounterResponseType.Error"].valueOf()] = "Error",
      values
    ))({}),
  }, exports);
  function __liftString(pointer) {
    if (!pointer) return null;
    const
      end = pointer + new Uint32Array(memory.buffer)[pointer - 4 >>> 2] >>> 1,
      memoryU16 = new Uint16Array(memory.buffer);
    let
      start = pointer >>> 1,
      string = "";
    while (end - start > 1024) string += String.fromCharCode(...memoryU16.subarray(start, start += 1024));
    return string + String.fromCharCode(...memoryU16.subarray(start, end));
  }
  return adaptedExports;
}
export const {
  memory,
  __new,
  __pin,
  __unpin,
  __collect,
  __rtti_base,
  CounterRequestType,
  CounterResponseType,
  initCounterServer,
  callCounter,
  castCounter,
  getCounterValue,
  run_gen_server_example,
  _start,
} = await (async url => instantiate(
  await (async () => {
    const isNodeOrBun = typeof process != "undefined" && process.versions != null && (process.versions.node != null || process.versions.bun != null);
    if (isNodeOrBun) { return globalThis.WebAssembly.compile(await (await import("node:fs/promises")).readFile(url)); }
    else { return await globalThis.WebAssembly.compileStreaming(globalThis.fetch(url)); }
  })(), {
  }
))(new URL("gen_server_example.wasm", import.meta.url));
