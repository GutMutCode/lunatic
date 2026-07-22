# Migrating host integrations from Lunatic 0.13 to 0.14

Lunatic 0.14 is an intentionally breaking release for the Rust host/runtime crates. The baseline
for this guide is the published 0.13.2 source at commit
`0b325fb264f005b0a35648e142637e4fb14ad387`. Most guest Wasm imports remain compatible, but the
SQLite handle-width and close/finalize changes below are guest ABI breaks.

All publishable crates that belonged to the 0.13 release train now use version `0.14.0`; use a
single release line in downstream manifests:

```toml
[dependencies]
lunatic-process = "0.14"
lunatic-process-api = "0.14"
lunatic-runtime = "0.14"
```

`lunatic-otp-patterns` is a new, independently versioned `0.1.0` crate, but its Lunatic
dependencies require the `0.14` release line. The quarantined, unpublished
`lunatic-control-submillisecond` source is not part of the 0.14 workspace or release.

## Process, environment, and mailbox APIs

The 0.14 process boundary is bounded and ownership preserving. Calls that could silently discard a
signal or message in 0.13 now return the rejected value in an error.

| 0.13 public API | 0.14 public API | Migration |
| --- | --- | --- |
| `Process::send(Signal) -> ()` | `-> Result<(), SignalSendError>` | Propagate or inspect the error; recover the signal with `into_signal()`. Custom `Process` implementations must return the result. |
| `Environment::send(id, Signal) -> ()` | `-> Result<(), SignalSendError>` | Handle missing/closed destinations, mailbox or queue backpressure, oversized messages, and excess attached message resources. |
| `Environment::add_process(id, process) -> ()` | `-> anyhow::Result<()>` | Propagate duplicate/quota admission errors. Prefer `ProcessRegistration::register` for lifetime-bound registration. |
| `Environment::remove_process(id) -> ()` | `-> bool` | Use the boolean when removal must be confirmed. |
| `Environments::create(id) -> Arc<Env>` | `-> anyhow::Result<Arc<Env>>` | Propagate environment admission failure and retain the returned `Arc`. |
| no registry-aware constructor | required `Environments::create_with_registry(...) -> Result<Arc<Env>>` | Custom registries must implement it. |
| no broadcast method | required `Environment::send_to_all(Signal)` | Custom environments must define broadcast policy. |
| `LunaticEnvironment::new(id)` with no process ceiling | `new(id)` with `DEFAULT_MAX_PROCESSES` (100,000) | Use `LunaticEnvironment::unlimited(id)` only when an explicitly unbounded host policy is required. |
| `spawn(...) -> (JoinHandle<Result<T>>, NativeProcess)` | `-> Result<(JoinHandle<Result<T>>, NativeProcess)>` | Add `?`; state types must also satisfy `ReloadableState` and `wasmtime::ResourceLimiter`. |
| `MessageMailbox::push(Message) -> ()` | `-> Result<(), MailboxPushError>` | Handle bounded admission; recover the message with `into_message()` or message plus permit with `into_parts()`. |
| unbounded Tokio signal sender/receiver aliases | opaque bounded `SignalSender`, `SignalReceiver`, and `SignalEnvelope` | Use `default_mailboxes`, `mailboxes_with_capacity`, or `mailboxes_with_limits`; receive an envelope and call `into_signal()` when needed. |
| `WasmProcess::new(id, UnboundedSender<Signal>)` | `WasmProcess::new(id, SignalSender)` | Construct process mailboxes through the Lunatic helpers. |
| `ProcessState: Sized` | `ProcessState: Sized + ReloadableState` | Implement `serialize_state` and `deserialize_state`; `code_change` retains a default implementation. |

Before:

```rust,ignore
process.send(signal);
environment.send(process_id, another_signal);
mailbox.push(message);
let (join, process) = lunatic_process::spawn(environment, run);
```

After:

```rust,ignore
process.send(signal)?;
environment.send(process_id, another_signal)?;
mailbox.push(message)?;
let (join, process) = lunatic_process::spawn(environment, run)?;
```

If retry is part of the application policy, ownership is retained:

```rust,ignore
let signal = match process.send(signal) {
    Ok(()) => return Ok(()),
    Err(error) => error.into_signal(),
};

let message = match mailbox.push(message) {
    Ok(()) => return Ok(()),
    Err(error) => error.into_message(),
};
```

For host-side processes that do not own Wasm state, use `spawn_native(...)?` instead of inventing a
dummy `ProcessState`.

`LunaticEnvironments` now retains environments weakly and enforces finite admission. Keep the
returned `Arc<LunaticEnvironment>` alive for as long as later `get(id)` calls must find it. Process
IDs are monotonic across recreated generations, so downstream code must not assume that dropping
and recreating an environment resets its process IDs.

## Signal and message shapes

Downstream exhaustive matches and direct constructors must be updated:

| 0.13 shape | 0.14 shape |
| --- | --- |
| `Signal::Monitor(Arc<dyn Process>)` | `Signal::Monitor { process, acknowledgement }` |
| `Signal::ProcessDied(u64)` | `Signal::ProcessDied { process_id, reason }` |
| `Message::ProcessDied(u64)` | `Message::ProcessDied { process_id, reason }` |
| no reload variants | `Signal::{HotReload, Rollback}` |
| `Finished::{Normal(T), KillSignal}` | also `Finished::Panicked(String)` |

`Signal::Link` and `Signal::UnLink` remain source-visible for the 0.14 transition, but built-in
processes reject one-sided lifecycle mutation. Use `link_processes` and `unlink_process` so both
sides are admitted atomically.

## Wasmtime, WASI, and configuration

| 0.13 public API | 0.14 public API | Migration |
| --- | --- | --- |
| `DefaultProcessConfig::preopened_dirs() -> &[String]` | `-> &[(String, String)]` | Treat each entry as `(guest_path, resolved_host_path)`. Preserve the resolved host path instead of resolving `~` or relative paths again. |
| `build_wasi(args, envs, dirs: &[String])` | `build_wasi(args, envs, dirs: &[(String, String)])` | Pass `(directory.clone(), directory)` when the guest and host paths are intentionally identical. |
| public Wasmtime 8 and `wasi-common` 8 types | Wasmtime and `wasi-common` 46.0.1 types | Align direct dependencies, then rebuild linker, store, `Caller`, `Val`, `ExportType`, compiled-module, instance, and `WasiCtx` integrations against version 46. |

`DefaultProcessConfig` is publicly serializable. Its preopened-directory representation and new
resource-limit fields change the encoded form, including the bytes carried in distributed
`Spawn.config`; do not exchange serialized 0.13 configs with 0.14 nodes.

`lunatic-wasi-api::register` now installs preview0 as well as preview1. It temporarily enables
linker shadowing while replacing descriptor-producing wrappers and leaves shadowing disabled on
return; embedders that require another linker policy must restore it afterwards.

`ProcessConfigCtx` also gained finite module/config/SQLite ceilings and TLS credential capability
methods. Their defaults are deliberately fail-closed, so external implementations must explicitly
admit the resources they intend to expose.

## SQLite host and guest contracts

| 0.13 public API or ABI | 0.14 public API or ABI | Migration |
| --- | --- | --- |
| `SQLiteConnections = HashMapId<Arc<Mutex<Connection>>>` | `HashMapId<Arc<SQLiteConnectionResource>>` | Do not insert raw connections into the table. Let the registered SQLite host functions create entries so their quota leases and connection lifetime remain coupled. |
| `SQLiteStatements = HashMapId<(u64, Statement)>` | `HashMapId<SQLiteStatementResource>` | Let prepare/finalize host functions manage statement entries; wrapper construction is intentionally not public. |
| no quota method on `SQLiteCtx` | required `sqlite_quota(&self) -> Arc<dyn SQLiteResourceQuota>` | Return one shared finite quota object, normally backed by `SQLiteResourceStats`, from every custom context implementation. |
| guest `open(..., connection_id: *mut u32)` | `open(..., connection_id: *mut u64)` | Recompile the guest and store a 64-bit handle. |
| guest `sqlite3_finalize(connection_id)` closed a connection | `sqlite3_finalize(statement_id)` finalizes a statement; `sqlite3_close(connection_id)` closes a connection | Replace connection cleanup calls with `sqlite3_close`; keep `sqlite3_finalize` only for statement handles. |

There is no supported 0.14 equivalent for directly populating the public SQLite resource aliases
with raw Rusqlite values. Custom embedders should invoke the registered host functions or wrap a
higher-level guest call rather than bypassing quota admission.

## Networking resource types

The networking structs are not `#[non_exhaustive]`, so both struct literals and exhaustive
destructuring must change:

| 0.13 public type | 0.14 public type | Migration |
| --- | --- | --- |
| `TcpConnection { reader, writer, read_timeout, write_timeout, peek_timeout }` | also has `peer_addr` and `local_addr` | Construct with `TcpConnection::new(stream)` and use its address accessors instead of a struct literal. |
| `TlsConnection` without client metadata | also has `client_metadata: Option<TlsClientConnectionMetadata>` | Construct with `TlsConnection::new(stream)` or `with_client_metadata(stream, metadata)`. |
| `TlsListener { listener, certs, keys }` | `TlsListener { listener, acceptor: TlsAcceptor }` | Build a rustls server configuration first and store the configured `TlsAcceptor`; raw keys no longer live in the listener entry. |

Public dependency types also move from rustls 0.20 to 0.23.42 and tokio-rustls 0.23.4 to 0.26.4.
Align downstream manifests before passing certificate, key, stream, or acceptor values across the
Lunatic API boundary.

### Pre-0.14 main-branch snapshot users

`ResourceMigrationSnapshot` was not part of the published 0.13.2 API. Integrations that tracked it
from the unreleased main branch must nevertheless migrate: `ResourceSnapshot::TlsListener` is now
a version-2 local-address plus opaque credential-handle contract, raw certificate/private-key
fields are gone, and persistence goes through `to_bytes`/`from_bytes` rather than blanket serde.
New guest code should use `tls_bind_with_credential`; the raw-key `tls_bind` import remains
temporarily available and deprecated.

## Distributed client, transport, and wire protocol

| 0.13 public API | 0.14 public API | Migration |
| --- | --- | --- |
| `distributed::Client::new(...).await -> Result<Client>` | `Client::new(...) -> Client` | Remove `.await?`; use `new_with_limits` to set finite delivery and registry limits. |
| `message_process(node, env, pid, tag, data) -> Result<(), ClientError>` | `send(SendParams) -> Result<MessageId, SendError>` | Build `SendParams`, handle delivery acknowledgement, and recover rejected bytes with `SendError::into_data()`. `send_with_timeout` additionally bounds the acknowledgement wait. |
| `spawn(node_id, Spawn) -> Result<u64, ClientError>` | `spawn(SpawnParams) -> Result<MessageId>` | Await the message ID, then call `await_response(message_id)` and match `ResponseContent::Spawned(process_id)`. |
| public `next_message_id` and `InnerClient` | IDs and inner state are client-managed | Remove manual ID allocation and use the public `Client` operations. |
| `quic::Client::connect(...) -> (SendStream, RecvStream)` and `try_connect_forever` | `quic::Client::try_connect(...) -> quinn::Connection` | Use the returned Quinn connection to open a stream and `write_message` for Lunatic framing; implement any retry loop explicitly. |
| Quinn wrapper stream types (including `RecvStream::id`) and Quinn 0.9 values | native Quinn 0.11.11 connection/stream values | Align the downstream Quinn dependency and use `quinn::{Connection, SendStream, RecvStream}` directly; carry any application message ID separately. |

The serialized MessagePack node protocol also changed:

- `Request::Message` has a required `node_id`; `Spawn` has a required `response_node_id`.
- `Request::{Response, Registry}` were added. The old `Response` enum is now
  `Response { message_id, content: ResponseContent }`, and `ClientError` has explicit environment,
  backpressure, size, rejection, and timeout variants. Update struct literals and exhaustive
  matches.
- `ServerCtx` requires `node_client` and `allowed_envs`. The old public low-level
  `handle_message` hook is crate-private; run the authenticated public `node_server` boundary
  instead.
- `control::cert::{test_root_cert, root_cert}` now return `CertificateAuthority`, certificate
  helpers consume that authority, and `distributed::server::gen_node_cert` returns
  `CertificateRequest`. Replace rcgen 0.10 certificate values with the explicit signing APIs and
  rcgen 0.14.8 types.

Because these are wire as well as Rust API changes, do not mix 0.13 and 0.14 nodes in one cluster.
Drain or stop the old nodes and perform a coordinated control-plane and runtime cutover.

## Control-plane APIs

| 0.13 public API | 0.14 public API | Migration |
| --- | --- | --- |
| `lunatic_control::api::Registration` with a cloneable token `String` | `RegistrationResponse` with `WireBearerToken`, bearer generation/expiry, environments, and privilege state | Parse the response, then call `distributed::control::Registration::from_response(control_url, response)`. Token types intentionally do not implement `Clone` and redact debug output. |
| `ControlUrls` without refresh URL | required `node_refreshed` field | Populate the bearer-refresh endpoint in literals and serialized responses. |
| `NodeStarted { node_id }` and `Clone` | certificate chain, bearer generation, and expiry fields; no `Clone` | Consume the response and install it through the control client. |
| `control::Client::new(HttpClient, Registration, addr, attrs)` | `Client::new(secure_registration, addr, attrs)` | Let the client build its hardened HTTP client; create the secure registration with `register(...)` or `Registration::from_response(...)`. |
| `client.reg() -> Registration` | `-> RegistrationMetadata` | Read only non-secret metadata; bearer material is no longer returned. |
| public raw `get`, `post`, and `upload` helpers | typed `get_module`, `add_module`, `refresh_nodes`, `refresh_bearer`, and `shutdown` methods | Use the typed operation matching the endpoint. |
| control-axum `HostExtractor` | authenticated `NodeAuth` | Require the node identity and current bearer generation in protected routes. |
| `ApiError::{InvalidData, InvalidPathArg, InvalidQueryArg}` and `Custom { code, message }` | `Internal`, authentication/authorization variants, and `Custom { code }` | Use `ApiError::custom_code(code)`; do not return caller-controlled secret-bearing messages. |
| public `ControlServer::new(...)` | high-level `control_server(...)` or `control_server_from_tcp(...)` | Use a high-level server entry point. Direct custom construction is no longer a supported public extension point. |

Direct `ControlServer` mutation also became authenticated and fallible: `register` returns a
`WireBearerToken`; `start_node`, `stop_node`, and `add_module` require the current registration and
bearer generation and return `Result`. `Registered` no longer exposes a token string or implements
`Clone`, and `NodeDetails::attributes` is now `HashMap<String, String>` rather than arbitrary JSON.

## Other host API changes

- `TimerResources::add(JoinHandle, Instant)` is no longer public, and the default timer table is
  finite at 1,024 entries. Schedule through the registered `lunatic::timer::send_after` host call;
  custom direct timer-table insertion has no public 0.14 replacement because admission must be
  reserved before spawning the timer task.
- `lunatic_messaging_api::register<T>` now requires `T::Config: ProcessConfigCtx`. Add that bound to
  generic embedder registration functions and implement the finite configuration policy.
- `lunatic_version_api::register<T>` now requires `T: 'static`; add the bound wherever registration
  is forwarded through a generic helper.

## Compatibility and release policy

There is no source-level compatibility shim for changed Rust trait method return types. Applications
that cannot migrate immediately should remain on the latest `0.13.x` set as a unit. Do not mix
0.13 and 0.14 Lunatic host crates: internal path/registry requirements intentionally select one
release train.

For integrations that followed the pre-0.14 main-branch snapshot API, the serialized TLS listener
snapshot version is also a hard boundary. Unversioned, version-1, unknown-version, and
trailing-data snapshots fail closed; re-provision the listener credential and create a version-2
snapshot rather than rewriting key bytes into a new structure.

Before cutting a `v0.14.0` tag, move the Unreleased notes in `CHANGELOG.md` under an exact
`## v0.14.0` heading and add `Released YYYY-MM-DD.`. CI rejects a tag that does not match the root
package version or has empty/misformatted release notes.

## Verification

The checked-in legacy fixture compiles the representative 0.13.2 calls against the exact published
dependency, while the integration test compiles and runs their 0.14 replacements using only public
items:

```text
cargo check --locked --manifest-path tests/fixtures/public-api-0-13/Cargo.toml
cargo test -p lunatic-process --test public_api_0_13_migration
python scripts/check_release_contract.py
```

CI pushes, pull requests, and release tags run `cargo-semver-checks` for every pre-existing
publishable workspace crate. Pull requests compare with their base SHA, branch pushes compare with
the prior remote state, and release tags compare with the preceding release tag (falling back to
the pinned 0.13.2 baseline for the first 0.14 release). The explicit fixture remains necessary
because no public-API linter detects every generic bound, trait-implementation, or type-shape
migration.
