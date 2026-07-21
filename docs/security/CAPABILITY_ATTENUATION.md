# Process Capability Attenuation

Reviewed: 2026-07-21

Lunatic process configurations follow a non-increasing authority rule: a
process may create a child with less authority, but it may not give the child a
capability or resource ceiling that the process itself does not have.

## Contract

`DefaultProcessConfig::default()` is an untrusted-process baseline. It grants no
module-compilation, config-creation, process-spawn, or filesystem-preopen
authority. The CLI launch paths in `src/mode/common.rs` and
`src/mode/cargo_test.rs` are trusted roots and explicitly grant the three
boolean capabilities and requested preopens needed by the initial process.

`create_config` derives a child from the caller instead of constructing an
independent default. The child starts with empty arguments, environment values,
and preopens; all boolean capabilities are false; and resource ceilings start
at the parent's ceilings.

| Authority | Delegation rule |
| --- | --- |
| Compile modules | Child `true` requires parent `true`. |
| Create configs | Child `true` requires parent `true`. |
| Spawn processes | Child `true` requires parent `true`. |
| Filesystem preopens | Every canonical child directory must be the same as or below a canonical parent preopen. The directory must exist. |
| Memory | Child maximum must be less than or equal to the parent maximum. |
| Fuel | An unlimited parent may delegate any limit. A finite parent may delegate only a finite limit less than or equal to its own; guest value `0` means unlimited and is rejected in that case. |
| Table elements | Child maximum must be less than or equal to the parent maximum. |
| File descriptors | Child maximum must be less than or equal to the parent maximum. |
| Network connections | Child maximum must be less than or equal to the parent maximum. |
| Mailbox messages | Child maximum must be less than or equal to the parent maximum. |
| Signal queue | Child maximum must be less than or equal to the parent maximum. |
| Message bytes | Child maximum must be less than or equal to the parent maximum. |
| Message resources | Child maximum must be less than or equal to the parent maximum. |

Guest mutations are transactional: clone the stored config, apply the requested
change, validate it against the caller, and replace the stored config only after
validation succeeds. A rejected setter therefore cannot leave elevated state
behind. The process API exposes set/get host imports for table, file-descriptor,
and network-connection ceilings in addition to the existing memory and fuel
imports.

Existing custom `ProcessConfig` and `ProcessConfigCtx` implementations remain
source-compatible. New attenuation methods have fail-closed defaults: guest
config creation, custom-config spawn, and distributed transfer remain denied
until the implementation defines its own policy. New table, FD, network,
mailbox, signal, message-byte, and message-resource context methods default to
zero/no-op. Custom runtimes should override these defaults before exposing the
corresponding guest APIs.

## Enforcement Boundaries

The selected config is revalidated before local `spawn`, local
`get_or_spawn`, and distributed-spawn serialization. `DefaultProcessState::new_state`
performs another validation at the common local state-construction boundary.
The distributed receiver also validates the deserialized config before it can
construct WASI state. This defense in depth prevents a future config-resource
mutation from bypassing local checks and prevents a crafted wire payload from
opening receiver-local filesystem paths.

Filesystem comparison resolves symlinks before containment checks. Preopen
configs store the resolved path used by WASI, so an in-scope symlink cannot be
used to select an already out-of-scope target. There is still a filesystem
time-of-check/time-of-use boundary if host directories are replaced between
validation and `Dir::open_ambient_dir`; eliminating that requires handle-based
authority rather than path strings.

Filesystem paths are host-local authority and are not portable across nodes.
Until a receiver-controlled path policy exists, distributed spawn therefore
fails closed when its selected config contains any preopen. The sender returns
an explicit `remote config denied` error, and the receiver independently
rejects a crafted serialized config before opening paths. To spawn remotely,
select a child config without preopens; inherited root configs that contain a
CLI preopen are intentionally rejected. A future node allowlist may safely
replace this conservative rule, but sender-side path equality alone is never
remote authorization.

The distributed receiver applies a fixed `DefaultProcessConfig::default()`
ceiling. Portable compile/create/spawn capabilities therefore remain denied,
and memory, table, FD, network, mailbox, signal, message-byte, and
message-resource limits cannot exceed that baseline. This is not yet an
operator-selectable receiver policy, and fuel is validated for internal
consistency but has no receiver-owned finite ceiling.

Every guest-visible TCP/TLS listener, TCP/TLS stream, UDP socket, and clone now
owns one paired FD/network lease; accept/connect/bind failure, cancellation,
drop, message transfer, and hot reload preserve or release that lease. DNS and
address iterators have a separate finite quota derived from the network
ceiling. The FD ceiling is still not connected to general WASI file handles,
and network destination allowlists are not part of `DefaultProcessConfig`.

## Errors and Audit Records

For ABI compatibility, `compile_module` and `create_config` return `-1` when
the caller lacks the corresponding capability. Valid capability denials remain
guest-visible status values and do not cross a Wasmtime host-trap boundary, so
policy failures never depend on host-error unwinding behavior.

New guests should use `lunatic::process::config_set_checked(config_id,
setting_id, value)`. It returns `-1` on success or a guest-readable
`lunatic::error` resource ID on failure. Setting IDs are `0` memory, `1` fuel,
`2` table elements, `3` file descriptors, `4` network connections, `5` compile,
`6` create-config, `7` spawn, `8` mailbox messages, `9` signal queue, `10`
message bytes, and `11` message resources. The legacy void setters remain available; a
denied legacy mutation is an audited no-op so existing modules cannot elevate
authority or abort the Windows runtime.

`lunatic::wasi::config_preopen_dir_checked` follows the same `-1`/error-resource
contract, including malformed memory ranges, invalid UTF-8, missing config IDs,
and policy denials. The legacy void preopen import turns any of those failures
into a safe no-op. Its operation guard records one typed terminal result for
policy decisions and early malformed/trap exits without including the raw path
or error. The
checked import is registered separately through
`lunatic_wasi_api::register_checked` so the legacy `register` API does not gain
an `ErrorCtx` bound. Local `spawn` returns status `1` and writes an error-resource
ID for permission/config failures. Distributed `spawn` uses status `3` for
local or remote capability/config rejection and also writes an error-resource
ID. These status/error paths avoid the same Windows unwind hazard. Other host
imports retain their documented malformed-ABI trap behavior.

Detailed error IDs are guest-owned resources. A guest should read them with
`lunatic::error::string_size`/`to_string` and release them with
`lunatic::error::drop`. Host insertion is bounded to 1,024 retained errors per
process. The first 1,023 slots retain detailed errors and the last is a stable,
guest-readable capacity error; later failures reuse that shared, non-owning ID
until ordinary resources are released. Calling `lunatic::error::drop` on the
capacity ID is intentionally a no-op so one caller cannot invalidate the shared
handle. Live guest-owned handles are never evicted by the bounded insertion
path. This bound applies to runtime host APIs that insert errors through
`ErrorCtx::add_error_resource`; direct mutation of the backing map bypasses it.

Config creation, setter/preopen decisions, compile, and spawn boundaries emit
versioned `AuditEventV1` JSON records on `target="audit"`. The stable schema
includes subject identity, typed numeric targets, result and machine reason
codes. Paths, guest strings, environment/argument values, payloads, and raw
errors cannot enter its string-free target type. See
[`AUDIT_LOGGING.md`](./AUDIT_LOGGING.md) for the event inventory and best-effort
delivery boundary.

## Executable Evidence

- `src/config.rs` unit tests cover default denial, child derivation, every
  boolean/resource ordering rule, path subsets, distributed-preopen rejection,
  and symlink escape on Unix.
- `tests/capability_attenuation.rs` drives the registered guest host imports.
  It verifies default compile/create/spawn/preopen denial, explicit checked
  escalation errors (including create-config) with rollback, safe legacy no-op
  behavior, both checked and legacy downward delegation followed by an actual
  child spawn, guest readability of returned error-resource IDs, the final
  local state-construction check, sender-side remote-preopen rejection, and
  both allowed and denied audit records. It also exercises actual local
  process-quota rejection through `spawn` and `get_or_spawn`; TCP/UDP bind and
  DNS success/quota denial; TLS and out-of-range-port validation failure; and
  local registry register/unregister/not-found outcomes. It asserts one typed,
  redacted audit event for each operation. It injects an invalid selected
  config through the actual `spawn` and `get_or_spawn` imports and verifies the
  1,024-resource bound under repeated checked denials. The complete matrix runs
  on Windows without an ignored host-trap test.
- `crates/lunatic-distributed/src/distributed/server.rs` tests deserialize
  allowed and host-local configs through the same decode-and-validate helper
  used by the production receive boundary.
- Compatibility tests in `lunatic-process` and `lunatic-process-api` compile
  pre-attenuation-style custom implementations and assert their new defaults
  fail closed.
