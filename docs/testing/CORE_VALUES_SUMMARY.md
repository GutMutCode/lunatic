# Core Values Verification Summary

**Last reviewed**: 2026-07-22

The repository uses executable production-path tests instead of a synthetic
compliance score.

## Release Gate

```bash
cargo fmt -- --check
cargo clippy --examples --tests --benches -- -D warnings
cargo test --all
```

## Current Evidence

| Claim | Evidence | Boundary |
| --- | --- | --- |
| OTP adapters work on Lunatic processes and the guest message ABI | `lunatic-otp-patterns`, `tests/wasm_link_death.rs`, and `tests/otp_guest_wasm.rs` cover late/intake-safe Supervisor monitoring including an actual Wasm trap, named GenServer cleanup, one-worker-safe GenStatem/GenEvent lifecycles, and enveloped guest call/reply/timeout/stop | Packaged cross-language guest SDKs and distributed supervision remain open |
| Hot reload preserves Wasm memory across module replacement | Embedded v1/v2 Wasmtime integration test | This test covers memory replacement, not every runtime resource |
| Live in-process TLS sessions survive hot reload | Real handshake and traffic after state transfer | Serialized, restarted, and cross-process TLS restoration is intentionally unsupported |
| Global registration coordinates across authenticated nodes | Real mTLS QUIC tests cover 2/3/5-node contention, request-ID uniqueness, quorum, resync, forged leader/snapshot rejection with no registry mutation, and expected-peer response correlation | Cluster-wide resource quotas and crash chaos are not covered |
| Node-control bearer credentials remain transport-bound and revocable | `tests/node_control_security.rs` and crate tests cover redaction, exact-origin URL validation, HTTP downgrade rejection, redirect/authenticated-proxy/forged-target fail-closed behavior, acknowledgement-loss-safe generation-CAS rotation, deterministic expiry, cancelled shutdown, stop revocation, and normal production Axum lifecycle | Plain HTTP is loopback-only; remote deployment requires HTTPS and enrollment admission, and the excluded submillisecond implementation is quarantined |
| QUIC framing dispatches multi-chunk messages | 4,097-byte production-framed request E2E | Dispatch boundary only; no live process mailbox |
| Performance limits are measured | Criterion benches plus `scripts/check_bench_thresholds.py` | Results are environment-sensitive and are not correctness proofs |

## Corrections to Historical Claims

- Removed the 38-test/9-of-10 compliance claim. Its tests did not execute the
  behavior they certified.
- Removed sleep-based performance assertions. Performance belongs to Criterion.
- Removed obsolete mock distributed failure and cross-node hot-reload suites that
  no longer compiled against the production API.
- The repository does not currently claim distributed scheduler stress coverage
  or cluster-wide quota validation.

See `CORE_VALUES_TESTING.md` for commands and
`docs/core_values/status.md` for the detailed implementation/gap inventory.
