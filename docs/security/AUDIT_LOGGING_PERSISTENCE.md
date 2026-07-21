# Audit Logging Persistence and Aggregation

Lunatic's built-in audit boundary is a compact JSON message emitted through Rust's `log` facade with `target="audit"`. Lunatic does not currently implement an OTLP exporter, a syslog writer, a rotating JSONL file, durable acknowledgements, retention, or tamper-evident storage.

The examples below describe external routing from that actual boundary. They are architecture patterns, not bundled or continuously tested Lunatic integrations.

## Runtime boundary

```text
typed AuditEventV1
  -> bounded in-process queue (drop-newest, fail-open)
  -> LogAuditSink writer thread
  -> enabled Rust logger (`target=audit`, INFO)
  -> process stderr/stdout formatting
  -> operator-owned collector and storage
```

The runtime can observe delivery only through the `LogAuditSink` call. It cannot know whether a container runtime, journald, syslog daemon, OpenTelemetry Collector, network, or storage backend later lost the record.

At graceful CLI shutdown Lunatic closes new audit admission and requests a bounded 250 ms flush for events accepted before that boundary. Forced process termination, power loss, a stuck downstream logger, or queue overflow can still lose records.

## Enable and inspect events

The default runtime filter includes `audit=info`. If `RUST_LOG` is set explicitly, include it yourself:

```bash
RUST_LOG='warn,audit=info' lunatic run app.wasm
```

`env_logger` adds its own timestamp/level/target envelope. The message inside that envelope is the V1 JSON object. A collector must parse the outer runtime log format first and then parse the message as JSON; it must not assume the process emits bare JSONL.

Monitor `audit_stats()` in an embedder or an operator health endpoint. Alert on any increase in:

- `dropped_full`;
- `dropped_closed`;
- `dropped_sink_unavailable`;
- `sink_failures` or `flush_failures`;
- `flush_timeouts` or a flush that remains pending;
- an unhealthy or closed sink.

## Container collection

Run Lunatic with `audit=info` and let the container runtime capture stderr/stdout. Configure the node-level collector to select records whose outer logger target is `audit`, then parse the inner message as JSON. Preserve the complete V1 object and its `sequence` field.

Container logging is not durable by itself. Capacity, rotation, eviction, collector backpressure, network retries, and storage acknowledgement are deployment responsibilities. Do not sample security audit records.

## Syslog or journald

Lunatic has no direct syslog sink. A service manager may capture process stderr and forward it to journald/syslog. Configure routing on the outer logger target and parse the inner JSON at the collector or destination.

Unix datagram syslog can silently lose records. If a deployment requires stronger delivery, select a transport and daemon configuration with bounded disk buffering and acknowledged forwarding, then test outage and recovery behavior. Ordinary log files are not automatically immutable or tamper-evident.

## OpenTelemetry

Lunatic has no native OpenTelemetry log exporter. Use an external Collector receiver that reads the container, journald, syslog, or operator-provided custom sink output. The receiver must extract the `audit` target and parse the JSON message before exporting it through OTLP.

Do not add `tracing-opentelemetry` snippets to the Lunatic runtime and describe them as supported until they compile in this repository and have an end-to-end delivery test.

## Custom durable sink

Embedders can implement `AuditSink` and install a custom dispatcher. A file or remote sink should define and test:

- startup/open failure behavior;
- file permissions and directory permissions;
- record framing and partial-write recovery;
- rotation and concurrent-reader behavior;
- retry bounds and duplicate semantics;
- flush and fsync policy;
- outage capacity and overflow policy;
- acknowledgement boundary;
- retention, access control, encryption, and integrity controls.

A successful `write`/`flush` is not automatically transactionally coupled to the privileged action. Achieving action-plus-audit atomicity requires a purpose-built journal or transaction protocol and is outside the current runtime contract.

## Redaction and access

V1 omits raw endpoints, paths, registry names, credentials, payloads, environment values, argv, and diagnostic error strings before queueing. Downstream processors should preserve that restriction and must not enrich audit records with secrets from other logs.

Audit records still contain process, environment, node, module, and resource identifiers. Treat them as restricted operational metadata. Retention periods and regulatory applicability must be established for each deployment with its security and legal owners; this guide does not prescribe or certify SOC 2, HIPAA, PCI DSS, or other compliance.

## Deployment acceptance test

Before relying on an external route:

1. Generate known allowed, denied, failed, and resource-limit events.
2. Confirm every stored record parses as V1 and retains its sequence.
3. Confirm sentinel secrets, paths, IPv4/IPv6 addresses, hostnames, registry names, and payloads are absent.
4. Saturate the in-process queue and verify drop counters and alerts.
5. Stop the collector/storage, observe the documented loss or retry behavior, then recover it.
6. Exercise rotation and forced process termination.
7. Verify file/directory permissions, transport security, access control, retention, and deletion.
8. Record the exact acknowledgement boundary and residual loss window.

Only the tested deployment path—not these examples—can establish operational persistence.

**Last reviewed:** 2026-07-21
