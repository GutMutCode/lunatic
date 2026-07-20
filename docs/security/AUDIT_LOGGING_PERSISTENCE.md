# Audit Logging Persistence and Aggregation Guide

This guide provides deployment-oriented options for persisting, aggregating, and analyzing Lunatic's current audit log records.

> **Current boundary (reviewed 2026-07-21):** The runtime emits formatted `target="audit"` text only on selected successful process-spawn and network bind/accept/connect paths. It does not yet provide complete privileged-operation coverage, denial/failure events, a typed schema, redaction tests, sink guarantees, or executable log assertions. The configurations below are advisory examples, not proof of runtime completeness, compliance, or production readiness. See [`docs/core_values/status.md`](../core_values/status.md).

## Overview

Selected successful operations emit formatted records using the `target="audit"` log target. Depending on the application's logger, these may be routed to stdout/stderr and can support limited investigation and capacity analysis. Broader uses below require a future complete event contract:

- **Current limited use** - Correlate the selected successful spawn/network events that are emitted
- **Future security monitoring** - Detect unauthorized attempts only after denial/failure coverage exists
- **Future compliance support** - Supply evidence only after schema, completeness, retention, integrity, and control requirements are independently validated
- **Future incident investigation and capacity planning** - Expand after event coverage and field semantics are defined

## Current Implementation

### Audit Events

Lunatic currently logs selected successful paths for the following operations:

| Event | Location | Details Logged |
|-------|----------|----------------|
| `process_spawn` | `lunatic-process-api/src/lib.rs:677` | `parent={pid} child={pid}` |
| `tcp_bind` | `lunatic-networking-api/src/tcp.rs:96` | `address={addr:port}` |
| `tcp_accept` | `lunatic-networking-api/src/tcp.rs:205` | `peer={addr:port}` |
| `tcp_connect` | `lunatic-networking-api/src/tcp.rs:284` | `peer={addr:port}` |
| `udp_bind` | `lunatic-networking-api/src/udp.rs:88` | `address={addr:port}` |
| `udp_connect` | `lunatic-networking-api/src/udp.rs:282` | `peer={addr:port}` |
| `tls_bind` | `lunatic-networking-api/src/tls_tcp.rs:166` | `address={addr:port}` |
| `tls_accept` | `lunatic-networking-api/src/tls_tcp.rs:252` | `peer={addr:port}` |
| `tls_connect` | `lunatic-networking-api/src/tls_tcp.rs:426` | `peer={addr} port={port}` |

### Log Format

Audit logs use Rust's `log` crate with the `audit` target:

```rust
use lunatic_common_api::audit_log;

audit_log("tcp_bind", format!("address={}", socket_addr));
```

Output format (depends on env_logger/tracing configuration):
```
[2025-10-06T10:30:45Z INFO  audit] tcp_bind address=0.0.0.0:8080
[2025-10-06T10:30:46Z INFO  audit] process_spawn parent=1 child=42
[2025-10-06T10:30:47Z INFO  audit] tls_connect peer=api.example.com port=443
```

## Recommended Architectures

### Option 1: Structured Logging with OpenTelemetry (Recommended)

**Best for**: Production deployments requiring observability integration.

```
┌──────────────┐
│   Lunatic    │
│   Runtime    │──────┐
└──────────────┘      │
                      │ OTLP/gRPC
                      ↓
              ┌───────────────┐
              │  OpenTelemetry│
              │   Collector   │
              └───────────────┘
                      │
        ┌─────────────┼─────────────┐
        ↓             ↓             ↓
  ┌─────────┐   ┌─────────┐   ┌──────────┐
  │ Elastic │   │  Loki   │   │ Security │
  │ search  │   │  (Logs) │   │   SIEM   │
  └─────────┘   └─────────┘   └──────────┘
```

**Implementation**:

1. **Configure tracing-opentelemetry**

Add to `Cargo.toml`:
```toml
[dependencies]
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
tracing-opentelemetry = "0.22"
opentelemetry = "0.21"
opentelemetry-otlp = "0.14"
```

2. **Initialize tracing**

`src/main.rs`:
```rust
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;

fn init_telemetry() -> Result<()> {
    // OTLP exporter
    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://localhost:4317");

    let tracer = opentelemetry_otlp::new_pipeline()
        .logging()
        .with_exporter(otlp_exporter)
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    // Subscriber with audit log filter
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("audit=info"))
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    Ok(())
}
```

3. **Deploy OpenTelemetry Collector**

`otel-collector-config.yaml`:
```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

processors:
  batch:
    timeout: 10s
    send_batch_size: 1024

  # Filter audit logs
  filter/audit:
    logs:
      include:
        match_type: strict
        resource_attributes:
          - key: target
            value: audit

exporters:
  # Send to Elasticsearch
  elasticsearch:
    endpoints: ["https://elastic.example.com:9200"]
    logs_index: "lunatic-audit-logs"

  # Send to Loki
  loki:
    endpoint: "https://loki.example.com/loki/api/v1/push"

  # Send to SIEM
  otlp/siem:
    endpoint: "siem.example.com:4317"

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [filter/audit, batch]
      exporters: [elasticsearch, loki, otlp/siem]
```

**Advantages**:
- ✅ Vendor-neutral, open standard
- ✅ Multi-backend support (send to multiple destinations)
- ✅ Rich metadata and context
- ✅ Integration with existing observability stack
- ✅ Structured log parsing out of the box

**Disadvantages**:
- ⚠️ Additional infrastructure (OTEL Collector)
- ⚠️ Complexity for simple deployments

---

### Option 2: Direct Syslog Integration

**Best for**: Traditional Unix environments, compliance requirements.

```
┌──────────────┐
│   Lunatic    │
│   Runtime    │────────→ Syslog Socket
└──────────────┘            │
                            ↓
                    ┌────────────────┐
                    │  rsyslog/      │
                    │  syslog-ng     │
                    └────────────────┘
                            │
          ┌─────────────────┼─────────────────┐
          ↓                 ↓                 ↓
    ┌──────────┐     ┌──────────┐      ┌──────────┐
    │  Local   │     │  Remote  │      │  SIEM    │
    │   File   │     │  Syslog  │      │  (TCP)   │
    └──────────┘     └──────────┘      └──────────┘
```

**Implementation**:

1. **Add syslog dependency**

```toml
[dependencies]
syslog = "6.1"
tracing-syslog = "0.3"
```

2. **Configure syslog appender**

```rust
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use tracing_syslog::Syslog;

fn init_syslog_audit() -> Result<()> {
    let syslog = Syslog::new(
        syslog::Facility::LOG_AUDIT, // Use audit facility
        tracing_syslog::Options::default()
            .facility(syslog::Facility::LOG_AUDIT)
            .process("lunatic")
    )?;

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("audit=info"))
        .with(syslog)
        .init();

    Ok(())
}
```

3. **Configure rsyslog**

`/etc/rsyslog.d/50-lunatic-audit.conf`:
```
# Route lunatic audit logs to dedicated file
:programname, isequal, "lunatic" /var/log/lunatic/audit.log
& stop

# Optionally forward to remote syslog
:programname, isequal, "lunatic" @@syslog.example.com:514
& stop

# Optionally send to SIEM
:programname, isequal, "lunatic" @@siem.example.com:6514
& stop
```

4. **Log rotation**

`/etc/logrotate.d/lunatic-audit`:
```
/var/log/lunatic/audit.log {
    daily
    rotate 365          # Keep 1 year
    compress
    delaycompress
    missingok
    notifempty
    create 0640 lunatic adm
    sharedscripts
    postrotate
        systemctl reload rsyslog
    endscript
}
```

**Advantages**:
- ✅ Mature, battle-tested infrastructure
- ✅ Built-in log rotation and retention
- ✅ Native Unix integration
- ⚠️ Compliance and tamper evidence require separate access, immutability, forwarding, retention, and integrity controls; ordinary syslog files are not tamper-proof
- ⚠️ Delivery guarantees depend on the selected syslog transport, buffering, storage, and failure configuration

**Disadvantages**:
- ⚠️ Limited structure (text-based)
- ⚠️ Parsing required for analysis
- ⚠️ Platform-specific (Unix/Linux only)

---

### Option 3: JSON File Logging + Log Shipper

**Best for**: Kubernetes/containerized environments.

```
┌──────────────┐
│   Lunatic    │
│  Container   │───→ JSON logs to stdout
└──────────────┘
       │
       ↓
┌──────────────┐
│  Fluentd/    │
│  Fluent Bit  │
└──────────────┘
       │
       ↓
┌──────────────┐
│ Elasticsearch│
│  or similar  │
└──────────────┘
```

**Implementation**:

1. **Configure JSON logging**

```rust
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

fn init_json_audit() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("audit=info"))
        .with(
            fmt::layer()
                .json()
                .with_target(true)
                .with_current_span(false)
        )
        .init();

    Ok(())
}
```

2. **Fluent Bit configuration**

`fluent-bit.conf`:
```
[SERVICE]
    Flush        5
    Log_Level    info

[INPUT]
    Name         tail
    Path         /var/log/containers/lunatic-*.log
    Parser       docker
    Tag          lunatic.*

[FILTER]
    Name         grep
    Match        lunatic.*
    Regex        target audit

[FILTER]
    Name         parser
    Match        lunatic.*
    Key_Name     log
    Parser       json

[OUTPUT]
    Name         es
    Match        lunatic.*
    Host         elasticsearch
    Port         9200
    Index        lunatic-audit
    Type         _doc
```

3. **Kubernetes Deployment**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fluent-bit-config
data:
  fluent-bit.conf: |
    # ... config from above ...

---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fluent-bit
spec:
  selector:
    matchLabels:
      app: fluent-bit
  template:
    spec:
      containers:
      - name: fluent-bit
        image: fluent/fluent-bit:2.0
        volumeMounts:
        - name: varlog
          mountPath: /var/log
        - name: config
          mountPath: /fluent-bit/etc/
      volumes:
      - name: varlog
        hostPath:
          path: /var/log
      - name: config
        configMap:
          name: fluent-bit-config
```

**Advantages**:
- ✅ Cloud-native (Kubernetes-friendly)
- ✅ Structured JSON logs
- ✅ Horizontal scalability
- ✅ Many integrations (S3, CloudWatch, etc.)

**Disadvantages**:
- ⚠️ Requires log shipper infrastructure
- ⚠️ Potential log loss if shipper fails

---

## Storage Recommendations

### Retention Policies

Recommended retention based on compliance requirements:

| Compliance | Retention | Storage Estimate (1M events/day) |
|------------|-----------|----------------------------------|
| **None** | 30 days | ~5 GB |
| **SOC 2** | 1 year | ~60 GB |
| **HIPAA** | 6 years | ~360 GB |
| **PCI DSS** | 1 year | ~60 GB |
| **GDPR** | Varies | Consult legal |

**Storage calculation**:
- Average audit log: ~150 bytes
- 1M events/day = 150 MB/day = 4.5 GB/month
- Add 30% overhead for indexes: 5.85 GB/month

### Storage Backends

#### Elasticsearch

**Best for**: Full-text search, real-time analysis.

**Index template**:
```json
{
  "index_patterns": ["lunatic-audit-*"],
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1,
    "index.lifecycle.name": "lunatic-audit-policy"
  },
  "mappings": {
    "properties": {
      "timestamp": { "type": "date" },
      "event": { "type": "keyword" },
      "details": { "type": "text", "fields": { "keyword": { "type": "keyword" }}},
      "level": { "type": "keyword" },
      "target": { "type": "keyword" }
    }
  }
}
```

**ILM Policy** (Index Lifecycle Management):
```json
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover": {
            "max_size": "50GB",
            "max_age": "7d"
          }
        }
      },
      "warm": {
        "min_age": "30d",
        "actions": {
          "shrink": { "number_of_shards": 1 },
          "forcemerge": { "max_num_segments": 1 }
        }
      },
      "cold": {
        "min_age": "90d",
        "actions": {
          "freeze": {}
        }
      },
      "delete": {
        "min_age": "365d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}
```

#### ClickHouse

**Best for**: High-volume, analytical queries, cost-effective storage.

**Schema**:
```sql
CREATE TABLE lunatic_audit (
    timestamp DateTime,
    event String,
    details String,
    level String,
    target String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, event)
TTL timestamp + INTERVAL 1 YEAR;
```

**Advantages**:
- ✅ 10-100x compression vs Elasticsearch
- ✅ Fast aggregation queries
- ✅ Automatic TTL enforcement

#### S3 (Archive)

**Best for**: Long-term retention, compliance.

**Lifecycle policy**:
```json
{
  "Rules": [{
    "Id": "lunatic-audit-retention",
    "Status": "Enabled",
    "Transitions": [
      {
        "Days": 90,
        "StorageClass": "STANDARD_IA"
      },
      {
        "Days": 180,
        "StorageClass": "GLACIER"
      }
    ],
    "Expiration": {
      "Days": 2555
    }
  }]
}
```

**Daily export script**:
```bash
#!/bin/bash
# Export audit logs to S3 daily

DATE=$(date -d "yesterday" '+%Y-%m-%d')
INDEX="lunatic-audit-${DATE//-/.}"
BUCKET="s3://company-audit-logs/lunatic/"

# Export from Elasticsearch
elasticdump \
  --input="http://localhost:9200/${INDEX}" \
  --output="${BUCKET}${DATE}.json.gz" \
  --compress=gzip

# Verify and delete from ES after 90 days
if [ $(($(date +%s) - $(date -d "${DATE}" +%s))) -gt 7776000 ]; then
  curl -X DELETE "localhost:9200/${INDEX}"
fi
```

---

## Analysis and Alerting

### Key Queries

#### 1. Unusual Network Bindings

Elasticsearch:
```json
{
  "query": {
    "bool": {
      "must": [
        { "term": { "event": "tcp_bind" }},
        { "range": { "timestamp": { "gte": "now-1h" }}}
      ],
      "must_not": [
        { "terms": { "details.keyword": ["0.0.0.0:8080", "0.0.0.0:3000"] }}
      ]
    }
  }
}
```

#### 2. Process Spawn Rate Anomaly

ClickHouse:
```sql
SELECT
    toStartOfHour(timestamp) AS hour,
    count() AS spawns,
    avg(spawns) OVER (ORDER BY hour ROWS BETWEEN 24 PRECEDING AND CURRENT ROW) AS avg_spawns
FROM lunatic_audit
WHERE event = 'process_spawn'
  AND timestamp > now() - INTERVAL 7 DAY
GROUP BY hour
HAVING spawns > avg_spawns * 3;  -- 3x normal is anomaly
```

#### 3. Suspicious TLS Connections

Elasticsearch:
```json
{
  "query": {
    "bool": {
      "must": [
        { "term": { "event": "tls_connect" }}
      ],
      "filter": {
        "script": {
          "script": "!doc['details.keyword'].value.contains('known-domain.com')"
        }
      }
    }
  }
}
```

### Alerting Rules

#### Grafana Loki AlertManager

`alerts.yaml`:
```yaml
groups:
  - name: lunatic-audit
    interval: 1m
    rules:
      - alert: UnauthorizedNetworkBind
        expr: |
          count_over_time({target="audit", event="tcp_bind"}[5m]) > 10
        labels:
          severity: high
        annotations:
          summary: "Excessive network binds detected"

      - alert: SuspiciousTLSConnection
        expr: |
          rate({target="audit", event="tls_connect"}[1m]) > 100
        labels:
          severity: medium
        annotations:
          summary: "High rate of TLS connections"
```

#### Elasticsearch Watcher

```json
{
  "trigger": {
    "schedule": { "interval": "5m" }
  },
  "input": {
    "search": {
      "request": {
        "indices": ["lunatic-audit-*"],
        "body": {
          "query": {
            "bool": {
              "must": [
                { "term": { "event": "tcp_bind" }},
                { "range": { "timestamp": { "gte": "now-5m" }}}
              ]
            }
          }
        }
      }
    }
  },
  "condition": {
    "compare": { "ctx.payload.hits.total": { "gt": 50 }}
  },
  "actions": {
    "notify_security": {
      "webhook": {
        "url": "https://slack.com/api/webhooks/...",
        "body": "Excessive tcp_bind events detected"
      }
    }
  }
}
```

---

## Security Considerations

### 1. Log Integrity

**Requirement**: Prevent tampering with audit logs.

**Solutions**:
- **Append-only storage**: Use immutable storage (S3 Object Lock, WORM drives)
- **Cryptographic signing**: Sign log batches with private key
- **Separate credentials**: Use different AWS role for logging vs app

**Implementation (S3 Object Lock)**:
```bash
aws s3api put-object-lock-configuration \
  --bucket company-audit-logs \
  --object-lock-configuration '{
    "ObjectLockEnabled": "Enabled",
    "Rule": {
      "DefaultRetention": {
        "Mode": "COMPLIANCE",
        "Days": 2555
      }
    }
  }'
```

### 2. Access Control

**Requirement**: Limit who can view audit logs.

**Best practices**:
- Separate Elasticsearch cluster for audit logs
- Role-based access control (RBAC)
- Audit the auditors (log access to audit logs)

**Elasticsearch RBAC**:
```json
{
  "roles": {
    "audit_viewer": {
      "cluster": ["monitor"],
      "indices": [{
        "names": ["lunatic-audit-*"],
        "privileges": ["read", "view_index_metadata"]
      }]
    }
  },
  "users": {
    "security_team": {
      "roles": ["audit_viewer"]
    }
  }
}
```

### 3. PII Redaction

**Requirement**: Avoid logging sensitive data.

**Current status**: Audit logs contain IP addresses and process IDs, but no PII.

**Recommendations**:
- Hash IP addresses if required by GDPR
- Implement field-level encryption for sensitive details
- Document what is logged in privacy policy

**Example PII redaction filter** (Fluent Bit):
```
[FILTER]
    Name         lua
    Match        lunatic.*
    Script       redact.lua
    Call         redact_pii
```

`redact.lua`:
```lua
function redact_pii(tag, timestamp, record)
    if record["details"] then
        -- Redact IP addresses
        record["details"] = record["details"]:gsub("%d+%.%d+%.%d+%.%d+", "REDACTED")
    end
    return 2, timestamp, record
end
```

---

## Monitoring and Validation

### Validate Log Pipeline

**Test 1: End-to-end latency**
```bash
# Trigger audit event
curl -X POST http://localhost:8080/spawn

# Check if logged within 10s
timeout 10 bash -c 'until curl -s "http://elastic:9200/lunatic-audit-*/_search?q=process_spawn" | grep -q "process_spawn"; do sleep 1; done'
```

**Test 2: Log completeness**
```bash
# Count events in last hour from app
APP_COUNT=$(grep "audit" /var/log/lunatic/app.log | wc -l)

# Count events in Elasticsearch
ES_COUNT=$(curl -s "http://elastic:9200/lunatic-audit-*/_count?q=timestamp:[now-1h TO now]" | jq .count)

# Verify <1% loss
if [ $((APP_COUNT - ES_COUNT)) -gt $((APP_COUNT / 100)) ]; then
  echo "WARNING: Log loss detected"
fi
```

### Metrics to Track

1. **Log throughput** (events/second)
2. **Ingestion latency** (time from event to indexed)
3. **Storage growth** (GB/day)
4. **Query performance** (P95 query time)
5. **Pipeline health** (uptime, errors)

**Prometheus metrics**:
```
# HELP lunatic_audit_events_total Total audit events emitted
# TYPE lunatic_audit_events_total counter
lunatic_audit_events_total{event="tcp_bind"} 1523

# HELP lunatic_audit_pipeline_latency_seconds Latency from event to storage
# TYPE lunatic_audit_pipeline_latency_seconds histogram
lunatic_audit_pipeline_latency_seconds_bucket{le="0.1"} 980
lunatic_audit_pipeline_latency_seconds_bucket{le="1.0"} 1200
```

---

## Compliance Checklist

### SOC 2 Type II

- [ ] Audit logs cover all privileged operations
- [ ] Logs are tamper-proof (append-only storage)
- [ ] Access to logs is restricted and logged
- [ ] Logs retained for 1 year minimum
- [ ] Regular review of audit logs documented

### HIPAA

- [ ] Audit logs encrypted at rest and in transit
- [ ] Access logs for PHI-related operations
- [ ] 6-year retention policy enforced
- [ ] Breach notification procedures include log review
- [ ] Annual audit log review process

### PCI DSS

- [ ] All network access logged (tcp_bind, tcp_connect, etc.)
- [ ] Logs retained for 1 year minimum
- [ ] Daily log review process
- [ ] Automated alerting on suspicious activity
- [ ] Time synchronization (NTP) configured

---

## Troubleshooting

### Problem: Logs not appearing

**Symptoms**: Audit events emitted but not in storage.

**Diagnosis**:
```bash
# Check if logs are emitted
RUST_LOG=audit=debug lunatic run app.wasm 2>&1 | grep audit

# Check log shipper
systemctl status fluent-bit
journalctl -u fluent-bit -n 50

# Check destination
curl http://localhost:9200/_cluster/health
```

### Problem: High storage costs

**Symptoms**: Audit log storage growing too fast.

**Solutions**:
1. Enable compression (gzip, zstd)
2. Reduce retention period
3. Use tiered storage (hot/warm/cold)
4. Sample high-frequency events

**Sampling example**:
```rust
use std::sync::atomic::{AtomicU64, Ordering};

static SAMPLE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn audit_log_sampled(event: &str, details: impl AsRef<str>, rate: u64) {
    let count = SAMPLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    if count % rate == 0 {
        audit_log(event, details);
    }
}

// Only log 1 in 10 tcp_connect events
audit_log_sampled("tcp_connect", format!("peer={}", addr), 10);
```

### Problem: Query performance degradation

**Symptoms**: Slow queries, timeouts.

**Solutions**:
1. Add indexes on frequently queried fields
2. Use date-based index rollover
3. Archive old data to S3
4. Upgrade hardware or add replicas

---

## Next Steps

1. **Choose architecture** based on your environment:
   - Kubernetes → Option 3 (JSON + Fluent Bit)
   - Traditional → Option 2 (Syslog)
   - Cloud-native → Option 1 (OpenTelemetry)

2. **Implement basic pipeline**:
   - Start with local file logging
   - Add log shipper
   - Configure retention

3. **Set up alerting**:
   - Identify critical events
   - Define thresholds
   - Configure notifications

4. **Validate compliance**:
   - Review requirements
   - Implement necessary controls
   - Document procedures

5. **Monitor and iterate**:
   - Track metrics
   - Optimize storage
   - Refine alerts

---

## Related Documentation

- [Audit Logging Implementation](./AUDIT_LOGGING.md) - Code-level details
- [Security Through Isolation](../core_values/status.md#3-security-through-isolation) - Security core values
- [Distributed Testing](../testing/DISTRIBUTED_TESTING.md) - Testing audit logs

## References

- [SOC 2 Compliance Guide](https://www.aicpa.org/interestareas/frc/assuranceadvisoryservices/sorhome.html)
- [HIPAA Security Rule](https://www.hhs.gov/hipaa/for-professionals/security/index.html)
- [PCI DSS Requirements](https://www.pcisecuritystandards.org/)
- [OpenTelemetry Logging](https://opentelemetry.io/docs/reference/specification/logs/)

---

**Last Updated**: 2025-10-06
**Maintainer**: Lunatic Security Team
**Status**: Advisory operational guide; runtime audit coverage remains incomplete
