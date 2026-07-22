# CLI Credential Storage

Lunatic Cloud CLI credentials are stored in the operating system's protected credential store.
The ordinary global configuration file contains only non-secret provider metadata and an opaque
credential reference.

This contract applies to the `lunatic login`, authenticated platform, and `lunatic logout` paths.
It does not cover distributed node bearer tokens, TLS listener credentials, or credentials owned
by guest applications. The separate node bearer contract is documented in
[Node-Control Bearer Security](NODE_CONTROL_BEARER.md).

## Storage boundary

The protected backend is selected at compile time:

- macOS: Keychain;
- Windows: Credential Manager with machine-local (non-roaming) persistence;
- Linux: Secret Service over the user's session D-Bus.

The backend is created lazily and one dedicated worker thread owns its complete lifetime. All
create/read/write/delete/read-back operations are serialized on that worker, and the backend is
dropped there as well. This avoids nesting the Linux Secret Service runtime inside the CLI Tokio
runtime and preserves the thread-affinity required by Windows Credential Manager.

The protected entry contains a versioned UTF-8 Base64 envelope. Its decoded record binds the
credential to the canonical provider origin and CLI application ID, then carries the login
challenge identifier, request-cookie pairs, and expiry derived from `Max-Age` or `Expires`. This
binding prevents an edited configuration from redirecting an existing protected cookie to another
origin. The UTF-8 envelope also works with Secret Service implementations that cannot store an
arbitrary binary secret. Live credential records do not implement Serde or `Clone`, their `Debug`
output is redacted, and temporary encoded buffers are zeroized on drop. An encoded protected value
larger than 2,400 bytes is rejected before it reaches a backend so the same format fits every
supported store.

`~/.lunatic/lunatic.toml` contains the stable CLI application ID, schema version, provider URL,
and a random opaque reference. During an interrupted logout it may instead contain an opaque
pending-deletion reference. It does not contain the login identifier or cookies. Unknown fields
and malformed references fail closed instead of being retained. Symlinks and multiply linked
global config files are rejected before credential-bearing migration or logout so replacement
cannot leave plaintext in another pathname. On Unix the
directory and file are restricted to `0700` and `0600`; those permissions protect metadata and are
not a substitute for the OS credential store. Metadata updates use a private temporary file,
file `fsync`, and same-directory replacement. Unix also syncs the parent directory (including the
home directory after first creating `~/.lunatic`); Windows uses a write-through atomic replace.
Login, refresh, migration, and logout also share an exclusive cross-process transaction lock. If
replacement succeeds but directory durability cannot be confirmed, the CLI reports a distinct
uncertainty error and keeps the matching config/credential pair instead of creating a dangling
reference.

## Login, reuse, expiry, and logout

The CLI accepts root HTTPS origins. Plain HTTP is accepted only for a loopback origin, which keeps
local development possible without permitting credentials over a remote clear-text connection.
Loopback HTTP bypasses system proxies. Provider URLs with user information, a path, query, or
fragment are rejected.

After login, the CLI parses each `Set-Cookie` header and retains only its `name=value` pair. Cookie
attributes are never replayed as request cookies. Before the first protected write, configuration
is atomically staged with an opaque pending-deletion tombstone. The protected entry is then written
and read back before that tombstone is atomically promoted to the active provider reference. A
failure or interruption therefore leaves either no protected entry or a reference that
`lunatic logout` can safely retry. Authenticated clients send one sensitive `Cookie` header and do
not follow redirects. If any required cookie is expired, the whole credential is rejected; the CLI
never sends a partially expired credential. Cookies without an earlier server expiry receive a
30-day local maximum lifetime.

Refresh replaces the protected value for the existing provider while holding the same transaction
lock, and rollback is accepted only after the previous value is read back exactly. Changing
providers requires
`lunatic logout` first. Logout deletes the protected entry before clearing its reference from the
configuration by first replacing the active provider with an opaque pending-deletion tombstone. A
missing protected entry is treated as an idempotent logout, while a backend failure is reported
and the tombstone is retained for retry. Native deletion is read back to verify absence before the
tombstone is cleared. Legacy plaintext logout is a separate path: it atomically replaces the
legacy provider record with the deterministic opaque migration reference before opening the
credential store. Thus plaintext is scrubbed even if the old provider URL is invalid or the
native store is unavailable, while a possible protected copy from a post-write migration crash
remains discoverable and must be deleted by retrying `lunatic logout`. Logout is local deletion
only; the current provider API has no remote session-revocation endpoint.

## Unavailable and headless environments

There is no plaintext, environment-variable, or file-only fallback. If the protected backend is
missing, locked, inaccessible, or returns invalid data, credential operations fail closed with a
stable error before an authenticated request is constructed.

On headless Linux, Secret Service normally requires a user session D-Bus and an available,
unlocked collection. Configure that service before running `lunatic login` or authenticated cloud
commands. Container and CI jobs without it should expect `CLI credential store is unavailable`;
they must not inject cookies into `lunatic.toml`. On macOS and Windows, access can likewise fail
because of login-session or platform policy. Restore access to the native store and retry. If
legacy logout already scrubbed plaintext, rerun logout to drain its opaque pending-deletion
reference before logging in again.

## Plaintext migration

When the CLI first reads the legacy configuration shape containing `login_id` and `cookies`, it:

1. validates the provider and parses the legacy record in a narrow deserialize-only type;
2. derives a deterministic opaque reference so an interrupted retry addresses the same entry;
3. writes the credential to the protected backend and reads it back for verification;
4. atomically replaces the legacy file with metadata and the opaque reference;
5. attempts verified deletion of the deterministic entry after a write error, verification error,
   or pre-commit replacement failure, including a backend write-then-error partial success.

The plaintext file is left byte-for-byte unchanged when protected-store setup fails, and the CLI
does not authenticate from it. This deliberately avoids copying the secret into another fallback
location. Restore the native backend and retry migration. If the old login should be abandoned,
remove the legacy file only after accepting that the local CLI application ID and login state will
also be lost, then run `lunatic login` again. Existing backups, filesystem snapshots, or copies of
the legacy file are outside the migration's deletion guarantee and must be handled separately.

## Diagnostics and audit scope

Credential-store errors are mapped to stable categories without retaining platform error payloads.
Provider response bodies, response headers, cookies, login identifiers, and URLs are not included
in returned authentication errors. The cloud CLI authentication path emits no runtime `audit`
event; the V1 audit schema has no field that can accept a cookie, bearer token, or arbitrary secret.

## Verification boundary

Deterministic tests use an injected in-memory credential store and loopback HTTP servers. They
cover login, reuse, expiry, redirect/proxy and cross-origin refusal, cross-process serialization,
logout success, verified failure, tombstone retry, and interrupted-migration recovery, unavailable-
store fail-closed behavior, migration partial-success and durability outcomes, linked-config
rejection, persistent worker thread affinity, cookie sanitization/count/storage-size limits,
provider/app-ID binding, and distinctive secret-marker absence in configuration, Serde output,
`Debug`, and errors.

```bash
cargo test --bin lunatic mode::credential_store::tests
cargo test --bin lunatic mode::config::tests
cargo test --bin lunatic mode::login::tests
cargo test --bin lunatic mode::execution::tests::logout_subcommand_parses
```

CI compiles the selected native backend on Linux, macOS, and Windows, but fake-store tests do not
prove that a particular user's native keychain is configured or unlocked. Release validation must
therefore include a platform smoke test for login, reuse, and logout in representative user
sessions.

**Last reviewed:** 2026-07-22
