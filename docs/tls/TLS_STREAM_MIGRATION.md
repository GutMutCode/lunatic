# TLS Stream Migration Strategy

**Status**: Implemented
**Date**: 2025-10-07
**Related**: `CORE_VALUES.md` - Fault Tolerance & High Availability

## Overview

This document describes Lunatic's approach to TLS stream migration during hot reload, balancing security, implementation complexity, and operational needs.

---

## Problem Statement

**Challenge**: Active TLS connections contain cryptographic session state (symmetric keys, cipher suites, sequence numbers) that cannot be safely serialized and restored across process boundaries without compromising security.

**Requirements** (from CORE_VALUES.md):
- Zero-downtime deployments
- Hot code reloading for all processes
- State preservation during updates
- Security through isolation

---

## Design Decision: Graceful Reconnection

### Why Not Session Resumption?

TLS 1.3 session resumption (RFC 8446) was evaluated but rejected for the following reasons:

1. **Security Complexity**
   - Session tickets contain encrypted state that must be protected
   - Requires secure key rotation and ticket lifetime management
   - Potential attack surface during hot reload window

2. **Implementation Burden**
   - Requires deep integration with rustls internals
   - Session state is intentionally opaque in tokio-rustls
   - Would need custom session storage and replay logic

3. **Limited Benefit**
   - Hot reloads are infrequent operational events (not per-request)
   - Brief reconnection delay is acceptable vs. security risk
   - Erlang/BEAM also drops active network connections during hot upgrades

### Chosen Strategy: Reconnection Metadata

**Client Connections** → Automatic reconnection
**Server Connections** → Graceful shutdown (client-initiated reconnect)

This aligns with **Erlang's approach**: network state is transient and rebuilt after code upgrades.

---

## Implementation

### 1. TLS Client Streams (Outbound)

#### Snapshot Phase
When a process with an active TLS client connection is hot-reloaded:

```rust
// Captured metadata (src/state.rs:320-347)
ResourceSnapshot::TlsClientConnection {
    server_name: "api.example.com",
    port: 443,
    peer_addr: Some("93.184.216.34:443"),
    local_addr: Some("192.168.1.100:54321"),
    custom_root_certs: vec![/* PEM-encoded CA certs */],
    read_timeout_ms: Some(5000),
    write_timeout_ms: Some(3000),
}
```

**What is preserved:**
- Target hostname and port
- Custom root certificates (if any)
- Connection timeouts
- Peer/local addresses (for logging)

**What is NOT preserved:**
- Active TLS session keys
- In-flight data (application must handle)
- TCP socket state

#### Restore Phase

**Current Behavior** (v0.13.2):
```rust
// TLS stream reconnection is logged but not automatically executed
// Requires additional dependencies: webpki, rustls_pemfile, webpki_roots
warn!("TLS client stream to {}:{} cannot be automatically reconnected yet",
      server_name, port);
```

**Planned Behavior** (future release):
1. Establish new TCP connection to `server_name:port`
2. Perform TLS handshake with saved certificate configuration
3. Restore read/write timeouts
4. Emit audit log: `tls_stream_reconnect`

**Guest Application Responsibility:**
- Detect connection loss via read/write errors
- Re-authenticate/re-establish application-level session
- Replay any unacknowledged requests

---

### 2. TLS Server Streams (Inbound)

#### Snapshot Phase
Server-accepted connections are marked for graceful shutdown:

```rust
ResourceSnapshot::TlsServerConnection {
    graceful_shutdown: true,
    reason: "Server-accepted TLS connections require client reconnection after hot reload",
}
```

#### Restore Phase
- Connection is closed (no reconnection attempted)
- Client will receive TCP FIN or RST
- Client must reconnect to the (still-running) TLS listener

**Rationale:**
- Server cannot initiate TLS connections to clients
- Client owns the connection lifecycle
- TLS listeners are preserved (see below), so clients can immediately reconnect

---

### 3. TLS Listeners (Fully Supported)

TLS listeners are **fully migratable** and have been since Phase 7:

```rust
ResourceSnapshot::TlsListener {
    local_addr: "0.0.0.0:8443",
    cert_pem: vec![/* PEM-encoded certificate */],
    key_pem: vec![/* PEM-encoded private key */],
}
```

**Restore process** (`src/state.rs:455-488`):
1. Rebind TCP listener to same address
2. Reconstruct `rustls::Certificate` and `rustls::PrivateKey`
3. Ready to accept new connections immediately

**Result:** Zero-downtime for listener availability. Existing connections drop, but new connections succeed without waiting for reload to complete.

---

## Operational Behavior

### Scenario: Hot Reload of HTTP API Server

**Before Reload:**
```
┌─────────┐   TLS   ┌──────────┐
│ Client  │────────▶│  Server  │
│  App    │◀────────│ Process  │
└─────────┘         └──────────┘
     ↑                    ↑
     │              TLS Listener
     │              on :8443
```

**During Reload (snapshot phase):**
1. Server process receives hot reload signal
2. TLS listener: Snapshot created (address, cert, key)
3. TLS client streams: Reconnection metadata captured
4. TLS server streams: Marked for graceful shutdown

**During Reload (restore phase):**
1. TLS listener: Rebound to `:8443` ✅
2. TLS server streams: Closed (clients receive TCP FIN)
3. TLS client streams: ⚠️ Logged but not reconnected (pending implementation)

**After Reload:**
```
┌─────────┐          ┌──────────┐
│ Client  │    ✗     │   New    │
│  App    │   TCP    │ Process  │  ← Client must reconnect
└─────────┘   RST    └──────────┘
                           ↑
                     TLS Listener
                     on :8443 ✅
```

**Client Experience:**
- Connection drops (appears as network error)
- Retry logic reconnects to same endpoint
- Sub-second interruption if client has backoff/retry

---

## Comparison with Erlang/BEAM

| Aspect | Erlang/OTP | Lunatic |
|--------|-----------|---------|
| **TCP Listeners** | Preserved (port reused) | ✅ Preserved |
| **TLS Listeners** | Preserved (with cert reload) | ✅ Preserved |
| **Active TCP Streams** | Closed | ⚠️ Not yet migratable |
| **Active TLS Streams** | Closed | ✅ Client: metadata captured<br>⚠️ Server: graceful close |
| **Session Resumption** | No (gen_tcp drops state) | No (same philosophy) |
| **Recovery Strategy** | Supervision + reconnect | Same approach |

**Key Insight**: Erlang's success at WhatsApp-scale was achieved *without* preserving active connections during upgrades. Lunatic follows the same proven pattern.

---

## Security Considerations

### Why NOT Serialize TLS Session State?

1. **Key Material Leakage**
   - Session keys in snapshot could be exposed via logs/debugging
   - Memory snapshots are not encrypted by default

2. **Replay Attacks**
   - Restored session state could be replayed if snapshot is captured
   - Violates TLS 1.3 forward secrecy guarantees

3. **Compliance Risk**
   - PCI-DSS, HIPAA, and other standards require key rotation
   - Preserving session state across process boundaries may violate audit requirements

### Audit Logging

All TLS operations are logged for security monitoring:

```rust
// Connection establishment (crates/lunatic-networking-api/src/tls_tcp.rs:447)
audit_log("tls_connect", format!("peer={} port={}", socket_addr, port));

// Server accept (tls_tcp.rs:252)
audit_log("tls_accept", format!("peer={}", socket_addr));

// Reconnection after hot reload (src/state.rs:593, pending)
audit_log("tls_stream_reconnect",
          format!("Reconnected to {}:{} after hot reload", server_name, port));
```

See `docs/security/AUDIT_LOGGING_PERSISTENCE.md` for production logging setup.

---

## Guest Application Guidelines

### Handling TLS Connection Loss During Hot Reload

**Rust (lunatic-rs):**
```rust
loop {
    match tls_stream.read(&mut buffer).await {
        Ok(n) if n > 0 => process_data(&buffer[..n]),
        Ok(0) | Err(_) => {
            // Connection closed - likely hot reload
            log::info!("TLS connection lost, reconnecting...");
            tls_stream = tls_connect(&server_name, port)?;
            re_authenticate(&mut tls_stream)?;
            continue;
        }
    }
}
```

**Go (TinyGo):**
```go
for {
    n, err := tlsConn.Read(buffer)
    if err != nil {
        // Reconnect with exponential backoff
        time.Sleep(100 * time.Millisecond)
        tlsConn = reconnectTLS(serverName, port)
        continue
    }
    processData(buffer[:n])
}
```

### Best Practices

1. **Implement Retry Logic**
   - Use exponential backoff (100ms → 200ms → 400ms)
   - Max 3-5 retries before alerting

2. **Application-Level Session State**
   - Don't rely on TLS connection persistence
   - Use stateless auth tokens (JWT) that survive reconnection
   - Design idempotent APIs

3. **Monitor Hot Reload Events**
   - Subscribe to process signals if available
   - Gracefully drain work before reload (future feature)

4. **Test Reconnection Paths**
   - Use `lunatic hot-reload` in staging to validate behavior
   - Ensure clients handle connection drops gracefully

---

## Future Enhancements

### Phase 1: Automatic Client Reconnection (Planned)

**Goal**: Transparently reconnect TLS client streams after hot reload.

**Dependencies:**
```toml
webpki = "0.22"
rustls-pemfile = "1.0"
webpki-roots = "0.25"
```

**Implementation** (pseudocode):
```rust
// During restore_resource_snapshot (src/state.rs:490+)
let tcp_stream = TcpStream::connect((server_name, port)).await?;
let tls_stream = tls_connector.connect(domain, tcp_stream).await?;
self.resources.tls_streams.add(Arc::new(
    TlsConnection::with_reconnection_info(tls_stream, reconnection_info)
));
```

**Challenges:**
- Must preserve original `custom_root_certs` configuration
- Requires tokio runtime handle during restore
- Guest application still sees connection loss (cannot hide TCP-level reconnection)

### Phase 2: Graceful Drain for Server Streams

**Goal**: Give server-side connections time to complete in-flight requests before closing.

**Approach:**
1. Mark process as "draining" when hot reload signal arrives
2. Stop accepting new requests on affected connections
3. Wait for pending responses (with timeout)
4. Close connection cleanly (TLS close_notify alert)

**Benefit:** Reduces 5xx error rate during deployments.

### Phase 3: Connection Pooling Integration

**Goal**: Reuse TLS handshakes across hot reloads via connection pooling.

**Requires:**
- Guest-level connection pool (e.g., `lunatic-rs` HTTP client)
- Pool survives hot reload by storing connection metadata
- Automatic reconnection on first use after reload

---

## Testing

### Unit Tests

**Location**: `tests/tls_stream_reconnection.rs`

```bash
$ cargo test --test tls_stream_reconnection
running 6 tests
test test_tls_snapshot_timeout_handling ... ok
test test_tls_server_connection_snapshot ... ok
test test_tls_reconnection_metadata ... ok
test test_tls_client_connection_snapshot ... ok
test test_migration_snapshot_count ... ok
test test_tls_client_snapshot_serialization ... ok
```

**Coverage:**
- ✅ Snapshot structure validation
- ✅ Serialization/deserialization
- ✅ Timeout preservation
- ✅ Custom certificate metadata
- ✅ Graceful server connection handling

### Integration Tests

**TLS Listener Migration**: `tests/tls_resource_migration.rs`
```bash
$ cargo test --test tls_resource_migration
test test_tls_listener_snapshot_structure ... ok
test test_tls_listener_snapshot_serialization ... ok
test test_tls_stream_non_migratable ... ok  # Now outdated
```

**Note**: `test_tls_stream_non_migratable` name is misleading after this implementation. TLS streams are now *partially* migratable (metadata captured, reconnection pending).

---

## References

### Internal
- `CORE_VALUES.md`: Fault Tolerance & High Availability principles
- `docs/core_values/status.md`: Compliance status (TLS migration gap closed)
- `docs/security/AUDIT_LOGGING_PERSISTENCE.md`: TLS event logging
- `crates/lunatic-networking-api/src/tls_tcp.rs`: TLS API implementation
- `crates/lunatic-process/src/resource_migration.rs`: Snapshot data structures

### External
- [RFC 8446 - TLS 1.3](https://www.rfc-editor.org/rfc/rfc8446.html)
- [Rustls Documentation](https://docs.rs/rustls/)
- [Erlang Hot Code Loading](http://erlang.org/doc/reference_manual/code_loading.html)
- [WhatsApp Engineering: Reliable System Upgrades](https://www.wired.com/2015/09/whatsapp-serves-900-million-users-50-engineers/)

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2025-10-07 | Use reconnection metadata, not session resumption | Security risk vs. operational benefit tradeoff |
| 2025-10-07 | Capture custom root certificates | Required for mTLS and private CA support |
| 2025-10-07 | Server streams use graceful shutdown | Cannot initiate client-bound connections |
| 2025-10-07 | Defer automatic reconnection | Requires additional dependencies; metadata capture is safe MVP |

---

## Conclusion

Lunatic's TLS stream migration strategy prioritizes **security** and **simplicity** over perfect transparency. By capturing reconnection metadata for client streams and gracefully closing server streams, we enable hot reload while maintaining cryptographic integrity.

This approach mirrors Erlang/OTP's proven pattern: transient network state is rebuilt after code upgrades, with supervision trees handling reconnection logic at the application level.

**Status**: Infrastructure complete, automatic reconnection pending dependency integration.
