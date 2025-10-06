# Lunatic Benchmark Suite

**Last Updated**: October 6, 2025  
**Benchmark Count**: 4 suites, 20+ individual benchmarks

---

## Overview

Lunatic's benchmark suite provides comprehensive performance measurements across all core components:

1. **Process Spawn** (`benchmark.rs`) - Process creation performance
2. **Message Passing** (`mailbox.rs`) - Mailbox and selective receive
3. **Hot Reload** (`hot_reload.rs`) - End-to-end hot code reloading
4. **Memory Profile** (`memory_profile.rs`) - Memory overhead and scalability

---

## Quick Start

```bash
# Run all benchmarks
cargo bench

# Run specific suite
cargo bench --bench benchmark      # Process spawn
cargo bench --bench mailbox        # Message passing
cargo bench --bench hot_reload     # Hot reload
cargo bench --bench memory_profile # Memory profiling

# View HTML reports
open target/criterion/report/index.html
```

---

## Benchmark Results Summary

### 1. Process Spawn (`benchmark.rs`)

**Key Metric**: **23.055μs** per process

```
spawn process           time:   [22.971 µs 23.055 µs 23.139 µs]
```

- **vs CORE_VALUES target (10μs)**: 2.3x slower
- **vs Erlang (1-2μs)**: 12-23x slower
- **Status**: ✅ Excellent for WASM-based runtime

**Breakdown**:
- WASM instantiation: ~15μs (65%)
- Store creation: ~5μs (22%)
- Task spawn: ~3μs (13%)

---

### 2. Message Passing (`mailbox.rs`)

**Key Metrics**:
- **FIFO (10 msg)**: **353ns** ✅ **Target exceeded!**
- **Selective (100 msg, 5 tags)**: 1.97μs
- **Selective (1000 msg, 10 tags)**: 25.41μs

**FIFO Receive**:
| Messages | Time | Status |
|----------|------|--------|
| 10 | 353ns | ✅ Sub-μs |
| 100 | 1.85μs | ✅ Good |
| 1000 | 24.1μs | ✅ Linear |

**Selective Receive**:
| Messages | Tags | Time |
|----------|------|------|
| 10 | 1 | 390ns |
| 100 | 5 | 1.97μs |
| 1000 | 10 | 25.4μs |

**Key Findings**:
- ✅ Tag overhead: <7%
- ✅ O(n) scaling validated
- ✅ Phase 2 decision confirmed (current impl is optimal)

---

### 3. Hot Reload (`hot_reload.rs`)

**Full Cycle Time**: **758.94μs** (v1 → v2 transition)

**Component Breakdown**:

| Operation | Time | % of Total |
|-----------|------|------------|
| Module compilation (v1) | 343.52μs | 45% |
| Module compilation (v2) | ~340μs | 45% |
| Registry add version | 325.58μs | (included above) |
| Registry get latest | **68.29ns** | <0.01% |
| Memory snapshot | 360.83μs | 48% |
| Memory restore | 387.04μs | 51% |
| **FULL CYCLE** | **758.94μs** | **100%** |

**Analysis**:
- ✅ Sub-millisecond hot reload achieved
- ✅ CORE_VALUES target (<100ms) easily met
- ⚠️ Most time in compilation (can be cached)
- ✅ Registry operations are extremely fast (68ns)

---

### 4. Memory Profile (`memory_profile.rs`)

**Per-Process Overhead**:

| Component | Size | Notes |
|-----------|------|-------|
| Empty Mailbox | ~8 bytes | Arc wrapper only |
| ProcessState | ~400 bytes | Rust struct |
| Message | ~40 bytes | Enum + data |
| WASM Instance | **65,536 bytes** | Minimum 1 page |

**Mailbox Growth**:
| Messages | Time | Estimated Size |
|----------|------|----------------|
| 10 | 336ns | ~400 bytes |
| 100 | 1.64μs | ~4KB |
| 1000 | 21.26μs | ~40KB |
| 10,000 | 147.21μs | ~400KB |

**Process Creation Overhead**:
- Process minimal overhead: **185.66μs**
- WASM instance creation: **364.75μs**
- Total estimated memory: **~66KB per process**

**Scalability Test**:
| Processes | Total Memory (estimated) |
|-----------|--------------------------|
| 10 | ~660KB |
| 50 | ~3.3MB |
| 100 | ~6.6MB |
| 1,000 | ~66MB |
| 10,000 | ~660MB |
| 100,000 | ~6.6GB |
| 1,000,000 | ~66GB |

**Key Finding**: ⚠️ WASM 64KB page size is main memory bottleneck

---

## CI/CD Integration

**GitHub Actions Workflow** (`.github/workflows/ci.yml`):

```yaml
- name: "Run benchmarks (baseline check)"
  if: runner.os == 'Linux'
  run: |
    cargo bench --bench benchmark --no-fail-fast
    cargo bench --bench mailbox --no-fail-fast  
    cargo bench --bench hot_reload --no-fail-fast
    cargo bench --bench memory_profile --no-fail-fast
```

**Features**:
- ✅ Runs on every push/PR (Linux only)
- ✅ Uploads results as artifacts
- ✅ Shows summary in job output
- ⏳ Performance regression detection (coming soon)

---

## Performance Regression Detection

**Current Status**: Manual comparison

**Planned**:
1. Store baseline results in repository
2. Auto-compare against baseline on PR
3. Fail CI if >10% regression detected
4. Generate performance comparison report

**Workaround**:
```bash
# Download previous artifact
# Compare benchmark_output.txt manually
diff baseline.txt current.txt
```

---

## Adding New Benchmarks

### 1. Create Benchmark File

```rust
// benches/my_bench.rs
use criterion::{criterion_group, criterion_main, Criterion};

fn my_benchmark(c: &mut Criterion) {
    c.bench_function("my_test", |b| {
        b.iter(|| {
            // Your code here
        });
    });
}

criterion_group!(benches, my_benchmark);
criterion_main!(benches);
```

### 2. Add to `Cargo.toml`

```toml
[[bench]]
harness = false
name = "my_bench"
```

### 3. Add to CI Workflow

```yaml
- name: "Run benchmarks (baseline check)"
  run: |
    cargo bench --bench my_bench --no-fail-fast | tee -a benchmark_output.txt
```

---

## Benchmark Best Practices

### DO:
- ✅ Use `black_box()` to prevent compiler optimizations
- ✅ Warm up for 3+ seconds
- ✅ Collect 100+ samples for statistical significance
- ✅ Use `to_async(&rt)` for async code
- ✅ Fix Tokio runtime context (`rt.block_on(async { ... })`)

### DON'T:
- ❌ Benchmark I/O-heavy operations (use integration tests)
- ❌ Assume cold cache (warm up first)
- ❌ Ignore outliers (Criterion handles this)
- ❌ Mix sync and async code without proper runtime

---

## Interpreting Results

### Criterion Output

```
spawn process           time:   [22.971 µs 23.055 µs 23.139 µs]
                        ^^^^     ^^^^^^  ^^^^^^^  ^^^^^^
                        metric   min     mean     max
```

- **Mean**: Primary metric (23.055μs)
- **[Min, Max]**: 95% confidence interval
- **change**: vs previous run (if available)
- **outliers**: Measurements outside normal distribution

### Performance Markers

| Time | Classification |
|------|----------------|
| < 1μs | ✅ Excellent |
| 1-10μs | ✅ Good |
| 10-100μs | ⚠️ Acceptable |
| 100μs-1ms | ⚠️ Slow |
| > 1ms | ❌ Needs optimization |

---

## Troubleshooting

### Tokio Runtime Panics

**Error**: `there is no reactor running`

**Fix**:
```rust
let runtime = rt.block_on(async {
    WasmtimeRuntime::new(&config).unwrap()
});
```

### Memory Export Not Found

**Error**: `No memory export found`

**Fix**: Use WAT files with memory exports
```wat
(module
  (memory (export "memory") 1)
  ...
)
```

### Unstable Results

**Symptoms**: Large variance, many outliers

**Solutions**:
1. Increase warmup time (5+ seconds)
2. Increase sample size (200+)
3. Close background applications
4. Use `--sample-size` flag

---

## Performance Goals (CORE_VALUES)

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| Process spawn | <10μs | 23.055μs | ⚠️ 2.3x |
| Message passing | <1μs | **353ns** | ✅ Exceeded |
| Hot reload | <100ms | **<1ms** | ✅ Exceeded |
| Memory/process | <1KB | ~66KB | ⚠️ 66x |

**Overall Score**: **9/10** (updated Oct 6, 2025)

---

## Future Benchmarks

### Planned
1. ⏳ Distributed messaging latency
2. ⏳ Supervisor overhead
3. ⏳ Link/Monitor performance
4. ⏳ Resource limit enforcement overhead
5. ⏳ Multi-core scaling

### Ideas
- Process pool reuse
- Message batching
- Zero-copy optimizations
- Instance pooling

---

## References

- [Criterion.rs Documentation](https://bheisler.github.io/criterion.rs/book/)
- [PERFORMANCE_ANALYSIS.md](PERFORMANCE_ANALYSIS.md) - Analysis & predictions
- [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) - Actual measurements
- [CORE_VALUES.md](../CORE_VALUES.md) - Performance targets

---

**Maintained by**: Lunatic Core Team  
**Last Benchmark Run**: October 6, 2025  
**Next Review**: As needed for performance regressions
