# CI Benchmark Integration Recommendations

**Date**: October 6, 2025
**Status**: Recommendation Document
**Priority**: Medium

---

## Executive Summary

Lunatic has comprehensive benchmarks for critical performance paths but lacks automated CI integration to detect performance regressions. This document outlines a phased approach to integrate benchmarks into CI while maintaining fast feedback loops.

---

## Current State

### ✅ Existing Benchmarks

**Location**: `crates/lunatic-process/benches/`

1. **`process_spawn.rs`** - Process spawning performance
   - Target: <10μs (currently 23μs, acceptable for WASM)
   - Critical for scalability to millions of processes

2. **`messaging.rs`** - Message passing latency
   - Target: <1μs (currently 353ns - **exceeds target**)
   - Core communication primitive

3. **`instance_pool.rs`** - Instance pooling efficiency
   - Measures hit rate and overhead
   - Important for hot reload performance

### ❌ Missing CI Integration

- No automated regression detection
- Manual benchmark runs only
- No historical performance tracking
- No alerting on degradation

---

## Recommended Approach

### Phase 1: Baseline Establishment (Week 1)

**Goal**: Capture current performance baseline

```yaml
# .github/workflows/benchmark-baseline.yml
name: Performance Baseline

on:
  workflow_dispatch:  # Manual trigger only

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Run critical benchmarks
        run: |
          cargo bench --bench process_spawn -- --save-baseline main
          cargo bench --bench messaging -- --save-baseline main

      - name: Upload baseline
        uses: actions/upload-artifact@v3
        with:
          name: performance-baseline
          path: target/criterion/
```

**Action Items**:
1. Run benchmarks on `main` branch
2. Save results as baseline artifact
3. Document baseline metrics in `docs/benchmarks/BASELINE.md`

---

### Phase 2: Pull Request Checks (Week 2-3)

**Goal**: Compare PR performance against baseline

```yaml
# .github/workflows/benchmark-pr.yml
name: Performance Regression Check

on:
  pull_request:
    paths:
      - 'crates/lunatic-process/**'
      - 'crates/lunatic-networking-api/**'

jobs:
  benchmark-comparison:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Download baseline
        uses: dawidd6/action-download-artifact@v2
        with:
          workflow: benchmark-baseline.yml
          name: performance-baseline
          path: target/criterion/

      - name: Run PR benchmarks
        run: |
          cargo bench --bench process_spawn -- --baseline main
          cargo bench --bench messaging -- --baseline main

      - name: Check for regressions
        run: |
          # Parse Criterion output for significant changes (>10%)
          python scripts/check_benchmark_regression.py

      - name: Comment on PR
        uses: actions/github-script@v6
        with:
          script: |
            // Post benchmark comparison as PR comment
```

**Regression Thresholds**:
- **Critical** (fail PR): >25% degradation
- **Warning** (comment only): >10% degradation
- **Info**: Any measurable change

---

### Phase 3: Continuous Monitoring (Month 2)

**Goal**: Track performance trends over time

**Integration Options**:

#### Option A: Criterion.rs + GitHub Pages
```yaml
- name: Generate report
  run: |
    cargo bench --bench process_spawn -- --output-format bencher | \
    tee output.txt

- name: Store benchmark result
  uses: benchmark-action/github-action-benchmark@v1
  with:
    tool: 'cargo'
    output-file-path: output.txt
    github-token: ${{ secrets.GITHUB_TOKEN }}
    auto-push: true
```

**Pros**: Native Rust, simple integration
**Cons**: Limited visualization

#### Option B: Bencher.dev (Recommended)
```yaml
- name: Track with Bencher
  uses: bencherdev/bencher@main
  with:
    bencher-api-token: ${{ secrets.BENCHER_API_TOKEN }}
    bencher-project: lunatic
    bencher-adapter: rust_criterion
```

**Pros**:
- Purpose-built for performance tracking
- Historical graphs and alerting
- Free for open source
- Multi-metric tracking

**Cons**: External dependency

---

## Implementation Plan

### Week 1: Setup
- [ ] Create baseline workflow
- [ ] Run baselines on `main`
- [ ] Document current performance metrics

### Week 2-3: PR Integration
- [ ] Implement benchmark comparison workflow
- [ ] Create regression detection script
- [ ] Add PR commenting bot
- [ ] Test on non-critical PRs

### Week 4: Refinement
- [ ] Tune regression thresholds
- [ ] Add benchmark variance analysis
- [ ] Document benchmark running guide

### Month 2: Monitoring
- [ ] Set up Bencher.dev or GitHub Pages
- [ ] Configure alerting for critical paths
- [ ] Create performance dashboard

---

## Benchmark Selection Criteria

### Always Run (Every PR touching these paths)
- `process_spawn` - Core scalability metric
- `messaging` - Core communication primitive

### Run on Main Only (Nightly/Weekly)
- `instance_pool` - Complex, less critical
- Future: distributed messaging, hot reload latency

### Never Run in CI
- Micro-benchmarks (<1μs operations)
- Benchmarks requiring special hardware
- Exploratory/development benchmarks

---

## Cost Analysis

### Compute Resources
- **Baseline**: ~2 minutes per run (manual)
- **PR Check**: ~5 minutes per PR
- **Nightly**: ~10 minutes per night

**Estimated Monthly Cost**:
- GitHub Actions: ~500 minutes/month = **FREE** (within limits)
- Bencher.dev: **FREE** for open source

### Maintenance Effort
- **Initial Setup**: 8-16 hours
- **Ongoing Maintenance**: 2-4 hours/month
- **Threshold Tuning**: 4 hours (one-time)

---

## Failure Modes & Mitigation

### False Positives (Flaky Benchmarks)
**Problem**: Variance causes spurious regression alerts

**Mitigation**:
1. Run benchmarks 3x, use median
2. Use statistical significance tests
3. Require >2 consecutive failures

### CI Queue Congestion
**Problem**: Long benchmark runs block other checks

**Mitigation**:
1. Run benchmarks in parallel workflow
2. Mark as non-blocking (advisory only)
3. Only run for performance-critical paths

### Baseline Drift
**Problem**: Hardware changes invalidate comparisons

**Mitigation**:
1. Update baseline monthly
2. Track hardware specs in metadata
3. Use relative (%) not absolute metrics

---

## Alternative: Manual Benchmark Gates

If full CI integration is too complex, consider:

```markdown
## PR Checklist (for performance-critical changes)

- [ ] Ran `cargo bench --bench process_spawn` locally
- [ ] Verified no >10% regression vs main
- [ ] Posted benchmark results in PR description
```

**Pros**: Zero infrastructure cost
**Cons**: Relies on developer discipline

---

## Recommendations

### Immediate (This Week)
1. ✅ **Document Current Baselines**
   - Run benchmarks on `main`
   - Record results in `docs/benchmarks/BASELINE.md`
   - Establish "golden" metrics

### Short-term (Next Month)
2. **Implement PR Benchmark Comparison**
   - Start with `messaging` bench only (fastest)
   - Advisory-only (don't block PRs)
   - Gather data on variance/flakiness

### Long-term (Quarter 2)
3. **Full CI Integration with Monitoring**
   - Integrate Bencher.dev for trending
   - Expand to all critical benchmarks
   - Add automated alerting

---

## Success Metrics

- ✅ Zero performance regressions merged unknowingly
- ✅ Performance trends visible to all contributors
- ✅ Optimization PRs can demonstrate improvements
- ✅ <5% false positive rate on regression detection

---

## References

- Current Benchmarks: `crates/lunatic-process/benches/`
- Performance Analysis: `docs/benchmarks/PERFORMANCE_ANALYSIS.md`
- Core Values Metrics: `CORE_VALUES.md` (lines 323-346)
- Criterion.rs Docs: https://bheisler.github.io/criterion.rs/
- Bencher.dev: https://bencher.dev/

---

**Next Steps**: Review this proposal with core team and select Option A or B for Phase 3.
