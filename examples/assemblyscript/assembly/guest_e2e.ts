// Minimal AssemblyScript guest contract exercised by the Lunatic runtime.
//
// This module deliberately uses only core Wasm plus the production
// `lunatic::*` imports below. It does not depend on JavaScript `env` imports or
// WASI. The host integration test invokes `parent(observer_id)` directly, while
// `_start()` runs the same scenario as a self-contained CLI smoke test.

@external("lunatic::error", "drop")
declare function errorDrop(errorId: i64): void;

@external("lunatic::error", "string_size")
declare function errorStringSize(errorId: i64): i32;

@external("lunatic::message", "create_data")
declare function messageCreateData(tag: i64, capacity: i64): void;

@external("lunatic::message", "data_size")
declare function messageDataSize(): i64;

@external("lunatic::message", "get_tag")
declare function messageGetTag(): i64;

@external("lunatic::message", "read_data")
declare function messageReadData(dataPtr: i32, dataLen: i32): i32;

@external("lunatic::message", "receive")
declare function messageReceive(tagPtr: i32, tagLen: i32, timeoutMs: i64): i32;

@external("lunatic::message", "send")
declare function messageSend(processId: i64): i32;

@external("lunatic::message", "send_receive_skip_search")
declare function messageSendReceiveSkipSearch(
  processId: i64,
  waitOnTag: i64,
  timeoutMs: i64,
): i32;

@external("lunatic::message", "write_data")
declare function messageWriteData(dataPtr: i32, dataLen: i32): i32;

@external("lunatic::process", "config_can_spawn_processes")
declare function configCanSpawnProcesses(configId: i64): i32;

@external("lunatic::process", "create_config")
declare function createConfig(): i64;

@external("lunatic::process", "drop_config")
declare function dropConfig(configId: i64): void;

@external("lunatic::process", "process_id")
declare function processId(): i64;

@external("lunatic::process", "spawn")
declare function processSpawn(
  linkTag: i64,
  configId: i64,
  moduleId: i64,
  functionPtr: i32,
  functionLen: i32,
  paramsPtr: i32,
  paramsLen: i32,
  idPtr: i32,
): i32;

const SEND_QUEUED: i32 = 0;
const SPAWN_OK: i32 = 0;
const SPAWN_ERROR: i32 = 1;
const DATA_MESSAGE: i32 = 0;
const TIMEOUT: i32 = 9027;

const ROUNDTRIP_REQUEST_TAG: i64 = 41;
const ROUNDTRIP_REPLY_TAG: i64 = 42;
const NO_REPLY_REQUEST_TAG: i64 = 43;
const NO_REPLY_TAG: i64 = 44;
const CHILD_READY_TAG: i64 = 4201;
const CHILD_SAW_NO_REPLY_REQUEST_TAG: i64 = 4202;
export const COMPLETED_TAG: i64 = 4203;

const HOST_TIMEOUT_MS: i64 = 5_000;
const EXPECTED_TIMEOUT_MS: i64 = 25;

const CHILD_FUNCTION: usize = memory.data<u8>([99, 104, 105, 108, 100]);
const CHILD_FUNCTION_LEN: i32 = 5;
const NOOP_FUNCTION: usize = memory.data<u8>([
  110, 111, 111, 112, 95, 99, 104, 105, 108, 100,
]);
const NOOP_FUNCTION_LEN: i32 = 10;

// Lunatic encodes each Wasm spawn argument as one type byte followed by a
// little-endian 128-bit value. These fixed buffers avoid a guest allocator and
// keep the fixture compatible with AssemblyScript's stub runtime.
const SPAWN_PARAM: usize = memory.data(17, 1);
const SPAWN_RESULT: usize = memory.data(8, 8);
const MESSAGE_VALUE: usize = memory.data(8, 8);
const RECEIVE_TAG: usize = memory.data(8, 8);

@inline
function require(condition: bool): void {
  if (!condition) unreachable();
}

function encodeI64Param(value: i64): void {
  store<u8>(SPAWN_PARAM, 0x7e);
  store<i64>(SPAWN_PARAM + 1, value);
  store<i64>(SPAWN_PARAM + 9, 0);
}

function spawnSameModule(
  functionPtr: usize,
  functionLen: i32,
  configId: i64,
  argument: i64,
): i64 {
  encodeI64Param(argument);
  store<i64>(SPAWN_RESULT, 0);
  const status = processSpawn(
    0,
    configId,
    -1,
    functionPtr as i32,
    functionLen,
    SPAWN_PARAM as i32,
    17,
    SPAWN_RESULT as i32,
  );
  require(status == SPAWN_OK);
  return load<i64>(SPAWN_RESULT);
}

function notify(process: i64, tag: i64): void {
  messageCreateData(tag, 0);
  require(messageSend(process) == SEND_QUEUED);
}

function receiveTag(tag: i64, timeoutMs: i64): void {
  store<i64>(RECEIVE_TAG, tag);
  require(messageReceive(RECEIVE_TAG as i32, 1, timeoutMs) == DATA_MESSAGE);
  require(messageGetTag() == tag);
  require(messageDataSize() == 0);
}

function sendI64(process: i64, tag: i64, value: i64): i32 {
  store<i64>(MESSAGE_VALUE, value);
  messageCreateData(tag, 8);
  require(messageWriteData(MESSAGE_VALUE as i32, 8) == 8);
  return messageSend(process);
}

function sendI64AndWait(
  process: i64,
  requestTag: i64,
  replyTag: i64,
  value: i64,
  timeoutMs: i64,
): i32 {
  store<i64>(MESSAGE_VALUE, value);
  messageCreateData(requestTag, 8);
  require(messageWriteData(MESSAGE_VALUE as i32, 8) == 8);
  return messageSendReceiveSkipSearch(process, replyTag, timeoutMs);
}

// The child receives a least-privilege configuration created by `parent`.
// Before serving messages it proves that a nested same-module spawn is denied,
// that the returned value is a live error-resource handle, and that the handle
// can be released explicitly.
export function child(parentId: i64): void {
  require(parentId > 0);
  store<i64>(SPAWN_RESULT, 0);
  const denied = processSpawn(
    0,
    -1,
    -1,
    NOOP_FUNCTION as i32,
    NOOP_FUNCTION_LEN,
    0,
    0,
    SPAWN_RESULT as i32,
  );
  require(denied == SPAWN_ERROR);

  const errorId = load<i64>(SPAWN_RESULT);
  require(errorStringSize(errorId) > 0);
  errorDrop(errorId);
  notify(parentId, CHILD_READY_TAG);

  require(messageReceive(0, 0, HOST_TIMEOUT_MS) == DATA_MESSAGE);
  require(messageGetTag() == ROUNDTRIP_REQUEST_TAG);
  require(messageDataSize() == 8);
  require(messageReadData(MESSAGE_VALUE as i32, 8) == 8);
  require(load<i64>(MESSAGE_VALUE) == 41);
  require(sendI64(parentId, ROUNDTRIP_REPLY_TAG, 42) == SEND_QUEUED);

  require(messageReceive(0, 0, HOST_TIMEOUT_MS) == DATA_MESSAGE);
  require(messageGetTag() == NO_REPLY_REQUEST_TAG);
  notify(parentId, CHILD_SAW_NO_REPLY_REQUEST_TAG);
}

// Entry point used by the host integration test. The root process is expected
// to have create-config and spawn capabilities. A new child configuration is
// least-privilege by default, so the child can exchange messages but cannot
// create a grandchild.
export function parent(observerId: i64): void {
  const childConfig = createConfig();
  require(childConfig >= 0);
  require(configCanSpawnProcesses(childConfig) == 0);

  const childId = spawnSameModule(
    CHILD_FUNCTION,
    CHILD_FUNCTION_LEN,
    childConfig,
    processId(),
  );
  receiveTag(CHILD_READY_TAG, HOST_TIMEOUT_MS);

  require(
    sendI64AndWait(
      childId,
      ROUNDTRIP_REQUEST_TAG,
      ROUNDTRIP_REPLY_TAG,
      41,
      HOST_TIMEOUT_MS,
    ) == DATA_MESSAGE,
  );
  require(messageGetTag() == ROUNDTRIP_REPLY_TAG);
  require(messageDataSize() == 8);
  require(messageReadData(MESSAGE_VALUE as i32, 8) == 8);
  require(load<i64>(MESSAGE_VALUE) == 42);

  require(
    sendI64AndWait(
      childId,
      NO_REPLY_REQUEST_TAG,
      NO_REPLY_TAG,
      0,
      EXPECTED_TIMEOUT_MS,
    ) == TIMEOUT,
  );
  receiveTag(CHILD_SAW_NO_REPLY_REQUEST_TAG, HOST_TIMEOUT_MS);

  // With all child notifications consumed, the direct receive timeout must use
  // the stable guest-visible timeout status too.
  require(messageReceive(0, 0, EXPECTED_TIMEOUT_MS) == TIMEOUT);
  dropConfig(childConfig);

  if (observerId != 0) notify(observerId, COMPLETED_TAG);
}

// CLI entry point: run the exact same parent/child contract without requiring a
// host-provided observer. Any mismatch traps and makes `lunatic run` fail.
export function _start(): void {
  parent(0);
}

// This function must never run in the fixture. It only provides a valid
// same-module function name for the child's denied spawn attempt.
export function noop_child(): void {
  unreachable();
}
