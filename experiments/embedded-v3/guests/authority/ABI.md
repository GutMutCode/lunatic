# Authority canary ABI v1

All six modules export:

- `memory`
- `parameter_ptr() -> i32`, always `1024`
- `parameter_len() -> i32`
- `invoke() -> i32`

They import only the genuine capability surface needed for their effect. The
exact ordered inventories live in `artifacts/manifest.json` and are verified
from Wasmtime's parsed import section in `tests/import_inventory.rs`.

## Canonical parameter encoding

The parameter blob is a concatenation of components:

```text
u32_le(byte_length) || raw_bytes
```

The first component is the ASCII domain
`lunatic.embedded-v3.authority.v1`; the second is the probe name. Remaining
components alternate field name and value. Numbers use minimal unsigned
decimal ASCII, IPv4 uses canonical dotted decimal, and strings are UTF-8.
There is no terminator, padding, map reordering, ambient environment lookup,
or runtime-provided target.

Every per-probe blob includes `nonce` and only the target fields that its
effect consumes:

| Probe | Additional fields |
| --- | --- |
| `wasi_p1_fs_read` | `preopen_fd`, `path` |
| `wasi_p1_fs_mutate` | `preopen_fd`, `sentinel_path`, `create_path` |
| `lunatic_tcp` | `observer_ipv4`, `port` |
| `lunatic_udp` | `observer_ipv4`, `port` |
| `lunatic_sqlite_create` | `path` |
| `extism_http` | `observer_ipv4`, `port`, `path` |

The suite hash uses the same encoding with probe component `suite` and every
field in struct order. All hashes are lowercase SHA-256 hex.

## Effect binding

The WASI read probe compares bytes read from the target with its embedded
nonce. The mutation probe overwrites the sentinel, renames it to the create
target, unlinks that name, and then recreates the target with nonce content.
The TCP and UDP probes send the nonce. The HTTP probe derives its URL, query,
nonce header, and POST body from the embedded parameters. SQLite binds the
nonce-named path into `open`, then executes one statement that creates the
quoted `authority_<nonce>` table with one row containing the nonce.

The WAT templates use fixed memory regions:

| Offset | Meaning |
| ---: | --- |
| 64..96 | host-call iovecs, handles, counts |
| 1024 | canonical parameter blob |
| 4096 | primary path or IPv4 bytes |
| 6144 | WASI mutation create/rename target |
| 8192 | nonce bytes |
| 12288 | WASI read buffer |

No imported function can rewrite the parameter blob before a target is chosen:
every effect argument is a template-bound constant or points at immutable
initial data. The harness must invoke a fresh instance once per authority
attempt.
