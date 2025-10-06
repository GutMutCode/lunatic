# Phase 2 Mailbox Optimization — External Research Summary

## Executive Summary

- Internal benchmarks show HashSet/tag-index approaches regress typical workloads; Phase 2 is deferred. See `docs/PHASE2A_ANALYSIS.md` and `docs/PHASE2_DECISION.md`.
- External evidence from Erlang/OTP supports keeping linear scans for typical mailboxes and leveraging compiler/runtime optimizations only for specific request/response patterns.
- Recommended focus: architectural hygiene to keep mailboxes small, targeted diagnostics, and only consider lazy indexing when production data proves large-N, high-tag cases.

## Internal Findings (Context)

- Current code path scans `VecDeque<Message>` and matches tags via slice search; selective receive is effectively O(n·m) but with early exit when a match appears near the head.
- Attempted “O(n+m)” optimization (building `HashSet<i64>` per receive) regressed:
  - 10 msgs/1 tag: ~0% change; 1000 msgs/1 tag: up to +12% slower; 1000 msgs/10 tags: +3.5% slower (`docs/PHASE2A_ANALYSIS.md:25-31`).
  - Thresholded HashSet (`len > 8`) still slower due to branching and allocation overhead.
- Decision: Defer Phase 2; optimize architecture instead (`docs/PHASE2_DECISION.md`).

Repo docs to review:

- `docs/PHASE2A_ANALYSIS.md`
- `docs/PHASE2_DECISION.md`
- `docs/PHASE2_INDEXED_MAILBOX_DESIGN.md`
- `benches/mailbox.rs`

## External Research Highlights

### Erlang/OTP guidance and optimizations

- OTP 24 release notes: “Selective receive optimization will now be applied much more often” and `recv_opt_info` can report where it applies.
  - URL: https://www.erlang.org/news/148

- Compiler option `recv_opt_info`: emits informational warnings for selective receive optimizations; see compile options.
  - URL: https://www.erlang.org/doc/man/compile.html#options (search for `recv_opt_info`)

- Efficiency Guide (Processes): documents receive behavior and optimization diagnostics (queue scanning, save-queue behavior, and examples of optimized request/response using refs/monitors).
  - URL: https://www.erlang.org/doc/efficiency_guide/processes.html

Implication for Lunatic:

- BEAM stays O(n) scan in the general case, but compiler/runtime can skip older messages when all receive clauses match a newly created reference (request/response pattern). This matches our “typical workloads are small and match near the head” finding.

### Community experience on large mailboxes

- Demonstration of severe slowdowns with large mailboxes and selective receive; mitigation by offloading I/O to helper processes so the main mailbox stays small.
  - URL: https://github.com/benmmurphy/erlang_selective_receive

- Learn You Some Erlang: explains save-queue mechanics, warns about deep scans when unwanted messages accumulate, and suggests either architectural fixes, catch-all logging, or priority structures (e.g., trees/heaps) for explicit priority handling.
  - URL: https://learnyousomeerlang.com/more-on-multiprocessing

## Practical Recommendations

- Prefer architecture over micro-optimization:
  - Split responsibilities to prevent any actor from accumulating thousands of messages.
  - Ensure request/response always includes a unique reference so receives can quickly match fresh replies.
  - Audit for “stray” messages; add catch-all logging during testing to drain unexpected messages and fix senders.

- Add targeted diagnostics (dev-only):
  - Log metrics around selective receives: mailbox length, tag count, and “match distance” (index of first match) to identify problem actors.
  - Summarize percentiles by actor type to validate assumptions (<100 messages, <5 tags, early matches).

- Bench only when the data suggests it:
  - If production metrics show mailboxes often >1000 messages and tags >20, prototype “lazy index beyond threshold” (as outlined in `docs/PHASE2_INDEXED_MAILBOX_DESIGN.md`) and compare against current implementation using `benches/mailbox.rs`.
  - Keep strict regression gates for low-n cases.

- Priority-specific paths (when required by domain):
  - For genuine priority ordering, prototype a separate priority queue (tree/heap) path and benchmark end-to-end; verify FIFO semantics for `pop(None)` remain intact.

## When to Reconsider Phase 2

- Mailbox scanning exceeds ~5% of runtime in production.
- Typical mailbox depth exceeds ~1000 and tag counts exceed ~20.
- Benchmarks on representative workloads show ≥20% improvement without regressions in the common case.

## Suggested Near-Term Actions

1. Add optional diagnostics around selective receive to capture mailbox depth, tag count, and match distance.
2. Run short production or staging trial to collect distributions per actor type.
3. Prioritize architectural remedies for any actors with large, persistent queues (decomposition, helper processes, message bucketing).
4. Only if data justifies: implement and benchmark lazy indexing guarded by a high threshold; verify no regressions for small mailboxes.

## References

- Erlang/OTP 24 Release Notes (selective receive optimization, `recv_opt_info`):
  - https://www.erlang.org/news/148
- Erlang Compiler Options (`recv_opt_info`):
  - https://www.erlang.org/doc/man/compile.html#options
- Efficiency Guide — Processes (receive/save-queue behavior and diagnostics):
  - https://www.erlang.org/doc/efficiency_guide/processes.html
- Learn You Some Erlang — More on Multiprocessing (selective receive and save-queue):
  - https://learnyousomeerlang.com/more-on-multiprocessing
- Erlang selective receive performance demo (large mailbox impact, helper process mitigation):
  - https://github.com/benmmurphy/erlang_selective_receive
