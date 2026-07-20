# Audit Logging

Lunatic emits `target="audit"` formatted text records on selected successful process-spawn and network bind/accept/connect paths.

> **Current boundary (reviewed 2026-07-21):** These records do not form a complete or typed security-event contract. Denial/failure coverage, stable schema and required fields, redaction tests, sink guarantees, and executable log assertions remain unimplemented. This document is an event inventory and routing guide; it is not production-readiness or compliance evidence. See [`docs/core_values/status.md`](../core_values/status.md).

## Overview

The operations listed below call the `audit` log target after selected successful actions. Coverage is not exhaustive, and routing/persistence depends on application logging configuration.

## Logged Operations

Lunatic logs the following privileged operations:

### Process Management
- **`process_spawn`** - New process creation
  - Location: `crates/lunatic-process-api/src/lib.rs:677`
  - Details: `parent={pid} child={pid}`

### Network Operations

#### TCP
- **`tcp_bind`** - TCP listener creation
  - Location: `crates/lunatic-networking-api/src/tcp.rs:96`
  - Details: `address={addr:port}`

- **`tcp_accept`** - Incoming TCP connection accepted
  - Location: `crates/lunatic-networking-api/src/tcp.rs:205`
  - Details: `peer={addr:port}`

- **`tcp_connect`** - Outgoing TCP connection established
  - Location: `crates/lunatic-networking-api/src/tcp.rs:284`
  - Details: `peer={addr:port}`

#### UDP
- **`udp_bind`** - UDP socket binding
  - Location: `crates/lunatic-networking-api/src/udp.rs:88`
  - Details: `address={addr:port}`

- **`udp_connect`** - UDP connection establishment
  - Location: `crates/lunatic-networking-api/src/udp.rs:282`
  - Details: `peer={addr:port}`

#### TLS
- **`tls_bind`** - TLS listener creation
  - Location: `crates/lunatic-networking-api/src/tls_tcp.rs:166`
  - Details: `address={addr:port}`

- **`tls_accept`** - Incoming TLS connection accepted
  - Location: `crates/lunatic-networking-api/src/tls_tcp.rs:252`
  - Details: `peer={addr:port}`

- **`tls_connect`** - Outgoing TLS connection established
  - Location: `crates/lunatic-networking-api/src/tls_tcp.rs:426`
  - Details: `peer={addr} port={port}`

## Implementation

### API

Audit logging uses the `audit_log` helper from `lunatic-common-api`:

```rust
use lunatic_common_api::audit_log;

// Log an audit event
audit_log("event_name", format!("key1=value1 key2=value2"));
```

### Internal Implementation

`crates/lunatic-common-api/src/lib.rs:112`:
```rust
/// Emit an audit log entry with `target = "audit"` so operators can route it separately.
pub fn audit_log(event: &str, details: impl AsRef<str>) {
    info!(target: "audit", "{} {}", event, details.as_ref());
}
```

All audit logs use:
- **Log level**: `INFO`
- **Log target**: `audit`
- **Format**: `{event} {details}`

### Example Output

With default env_logger configuration:
```
[2025-10-06T10:30:45Z INFO  audit] tcp_bind address=0.0.0.0:8080
[2025-10-06T10:30:46Z INFO  audit] process_spawn parent=1 child=42
[2025-10-06T10:30:47Z INFO  audit] tls_connect peer=api.example.com port=443
```

## Routing Audit Logs

### Simple: Filter by Target

Use env_logger filter to route audit logs separately:

```bash
# Only show audit logs
RUST_LOG=audit=info lunatic run app.wasm

# Show all logs but route audit separately
RUST_LOG=info,audit=info lunatic run app.wasm 2>&1 | tee >(grep "audit" >> audit.log)
```

### Operational Routing: See Persistence Guide

For deployment-oriented routing examples, see the [Audit Logging Persistence and Aggregation Guide](./AUDIT_LOGGING_PERSISTENCE.md), which covers:

- **Architecture Options**:
  - OpenTelemetry + OTLP (recommended for cloud-native)
  - Syslog integration (traditional Unix environments)
  - JSON + Fluent Bit (Kubernetes/containers)

- **Storage Recommendations**:
  - Retention policies (SOC 2, HIPAA, PCI DSS)
  - Storage backends (Elasticsearch, ClickHouse, S3)
  - Index lifecycle management

- **Analysis and Alerting**:
  - Key security queries
  - Anomaly detection
  - Alerting rules

- **Compliance**:
  - Log integrity (tamper-proofing)
  - Access control
  - PII redaction

## Adding New Audit Events

When adding privileged operations, emit audit logs:

1. **Import the helper**:
   ```rust
   use lunatic_common_api::audit_log;
   ```

2. **Log the operation**:
   ```rust
   audit_log("operation_name", format!("detail1={} detail2={}", val1, val2));
   ```

3. **Use consistent naming**:
   - Operation: `{resource}_{action}` (e.g., `tcp_bind`, `process_spawn`)
   - Details: `key=value` pairs separated by spaces

4. **Include relevant context**:
   - Resource identifiers (IDs, addresses)
   - Parent/child relationships
   - Success/failure indication (if applicable)

### Example: Adding File Access Auditing

```rust
use lunatic_common_api::audit_log;

pub fn open_file(path: &str, mode: &str) -> Result<File> {
    let file = File::open(path)?;

    // Audit the file access
    audit_log("file_open", format!("path={} mode={}", path, mode));

    Ok(file)
}
```

## Testing

### Verify Audit Logs

```bash
# Run with audit logging enabled
RUST_LOG=audit=info lunatic run test.wasm 2>&1 | grep audit
```

### Integration Tests

When testing privileged operations, verify audit logs are emitted:

```rust
#[test]
fn test_tcp_bind_logs_audit() {
    let _logs = capture_logs(|| {
        tcp_bind("0.0.0.0:8080").unwrap();
    });

    assert!(logs.contains("tcp_bind address=0.0.0.0:8080"));
}
```

## Security Considerations

### What is Logged
-  Process creation (parent/child relationships)
-  Network bindings and connections (addresses and ports)
-  TLS connections (peer addresses)

### What is NOT Logged
- L Payload data (message contents)
- L Authentication credentials
- L Process-internal state
- L Memory contents

### Privacy
- IP addresses are logged (may be PII under GDPR)
- Process IDs are logged (non-sensitive)
- No user data or payload contents are logged

For PII redaction strategies, see [Audit Logging Persistence Guide](./AUDIT_LOGGING_PERSISTENCE.md#3-pii-redaction).

## Related Documentation

- **[Audit Logging Persistence and Aggregation Guide](./AUDIT_LOGGING_PERSISTENCE.md)** - Operational routing ideas; not a runtime completeness guarantee
- [Security Through Isolation](../core_values/status.md#3-security-through-isolation) - Security core values
- [Core Values Status](../core_values/status.md) - Overall compliance status

## References

- Code: `crates/lunatic-common-api/src/lib.rs:112` - `audit_log` implementation
- Usage: Search codebase for `audit_log(` to find all audit events

---

**Last Updated**: 2026-07-21

**Status**: Scope-limited implementation note; production security-event coverage is incomplete
