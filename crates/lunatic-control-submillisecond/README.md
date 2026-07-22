# Lunatic Control Server (Submillisecond, quarantined)

This crate is preserved only as migration source. It is not a supported or
production control server, is excluded from the root workspace, cannot be
published, and its binary exits without starting a listener.

The legacy implementation does not satisfy the active node-control bearer
contract:

- it persists recoverable bearer tokens in `control_server.db`;
- token-bearing registration records derive general Clone/Debug/Serde traits;
- absolute HTTP endpoints are constructed from the request Host header;
- registrations have no bounded lease, rotation, or stop/crash revocation;
- SQLite writes are not acknowledged before registration responses are sent;
- the independent Git-based toolchain is not exercised by root CI.

Reactivation requires a separate migration that stores only opaque/verifier
records, removes token-bearing Clone/Debug/Serde paths, uses a trusted HTTPS or
loopback origin, implements the same rotation/revocation lease as the active
Axum server, and adds its own CI and security E2E suite. Until every gate is
met, use `lunatic control`, which routes to `lunatic-control-axum`.

See [`../../docs/security/NODE_CONTROL_BEARER.md`](../../docs/security/NODE_CONTROL_BEARER.md)
for the active contract and the reactivation checklist.
