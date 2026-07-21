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

Guest mutations are transactional: clone the stored config, apply the requested
change, validate it against the caller, and replace the stored config only after
validation succeeds. A rejected setter therefore cannot leave elevated state
behind. The process API exposes set/get host imports for table, file-descriptor,
and network-connection ceilings in addition to the existing memory and fuel
imports.

Existing custom `ProcessConfig` and `ProcessConfigCtx` implementations remain
source-compatible. New attenuation methods have fail-closed defaults: guest
config creation, custom-config spawn, and distributed transfer remain denied
until the implementation defines its own policy. New table/FD/network context
methods default to zero/no-op. Custom runtimes should override these defaults
before exposing the corresponding guest APIs.

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

The current distributed trust model still treats an authenticated cluster node
as authoritative for serialized non-filesystem capability flags and resource
ceilings. The receiver does not independently cap compile/create/spawn, memory,
fuel, table, FD, or network values. A compromised authenticated node is outside
the restricted-guest attenuation guarantee and can supply elevated portable
fields; receiver-owned ceilings are a separate hardening requirement.

The FD and network values in this contract are configuration ceilings. FD
accounting is not yet connected to every host file operation, and network
accounting currently covers selected TCP paths rather than every listener,
TLS, UDP, and DNS resource. Network destination allowlists are also not part of
`DefaultProcessConfig`. Complete resource accounting is a separate follow-up;
this change guarantees that the represented ceilings do not increase during
delegation.

## Errors and Audit Records

For ABI compatibility, `compile_module` and `create_config` return `-1` when
the caller lacks the corresponding capability. Valid capability denials do not
cross a Wasmtime host-trap boundary: Wasmtime 8 can abort the Windows process
when a host error unwinds through its async fiber.

New guests should use `lunatic::process::config_set_checked(config_id,
setting_id, value)`. It returns `-1` on success or a guest-readable
`lunatic::error` resource ID on failure. Setting IDs are `0` memory, `1` fuel,
`2` table elements, `3` file descriptors, `4` network connections, `5` compile,
`6` create-config, and `7` spawn. The legacy void setters remain available; a
denied legacy mutation is an audited no-op so existing modules cannot elevate
authority or abort the Windows runtime.

`lunatic::wasi::config_preopen_dir_checked` follows the same `-1`/error-resource
contract, including malformed memory ranges, invalid UTF-8, missing config IDs,
and policy denials. The legacy void preopen import turns any of those failures
into a safe no-op; policy allow/deny decisions that reach config validation are
audited, while malformed input and missing IDs do not emit an audit record. The
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

Config creation, setter/preopen decisions, and final custom-config spawn checks
emit `target="audit"` records named `capability_delegation`, with the parent
process, config ID when available, operation, and allowed/denied outcome.
Successful local child creation continues to emit `process_spawn`. These are
the runtime's current formatted-text audit records, not the stable typed and
redaction-tested event schema planned separately.

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
  both allowed and denied audit records. It also injects an invalid selected
  config through the actual `spawn` and `get_or_spawn` imports and verifies the
  1,024-resource bound under repeated checked denials. The complete matrix runs
  on Windows without an ignored host-trap test.
- `crates/lunatic-distributed/src/distributed/server.rs` tests deserialize
  allowed and host-local configs through the same decode-and-validate helper
  used by the production receive boundary.
- Compatibility tests in `lunatic-process` and `lunatic-process-api` compile
  pre-attenuation-style custom implementations and assert their new defaults
  fail closed.
