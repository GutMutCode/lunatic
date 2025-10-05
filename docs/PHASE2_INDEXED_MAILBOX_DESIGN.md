# Phase 2: Indexed Mailbox Design Document

**Date**: October 5, 2025  
**Status**: 🚧 **DESIGN PHASE**

## Summary

Phase 2 aims to optimize selective receive from O(n) to O(1) using tag-indexed data structures. However, this optimization involves complex tradeoffs that require careful consideration.

---

## Problem Statement

### Current Implementation (O(n))

**Location**: `crates/lunatic-process/src/mailbox.rs:52-59`

```rust
if let Some(tags) = tags {
    let index = mailbox.messages.iter().position(|x| {  // O(n) scan
        if let Some(tag) = x.tag() {
            tags.contains(&tag)
        } else {
            false
        }
    });
    if let Some(index) = index {
        return mailbox.messages.remove(index).expect("must exist");
    }
}
```

**Performance Characteristics**:
- Selective receive: O(n) where n = mailbox size
- 1000 messages → ~500 comparisons average
- 10000 messages → ~5000 comparisons average

---

## Design Challenges

### Challenge 1: FIFO Ordering

**Requirement**: `pop(None)` must return messages in FIFO order, regardless of tags.

**Test case** (mailbox.rs:310-313):
```rust
// Messages pushed: tag1, tag2, tag3, tag4, tag5
mailbox.pop(Some(&[tag2])).await;  // Returns tag2
mailbox.pop(Some(&[tag1])).await;  // Returns tag1
mailbox.pop(None).await;           // Must return tag3 (next in FIFO order)
```

**Problem**: If we use `HashMap<i64, VecDeque<Message>>`, we lose global FIFO order.

### Challenge 2: Memory Overhead

**Option A**: Dual storage (FIFO queue + Hash index)
```rust
struct InnerMessageMailbox {
    messages: VecDeque<Message>,           // All messages (FIFO)
    tag_index: HashMap<i64, Vec<usize>>,   // Tag -> indices in messages
}
```
- **Pro**: Maintains FIFO, enables O(1) lookup
- **Con**: 2x memory overhead, index invalidation on removal

**Option B**: Separate queues with sequence numbers
```rust
struct InnerMessageMailbox {
    untagged: VecDeque<(u64, Message)>,
    tagged: HashMap<i64, VecDeque<(u64, Message)>>,
    next_seq: u64,
}
```
- **Pro**: O(1) selective receive
- **Con**: pop(None) requires finding min(seq) across all queues = O(k) where k = number of unique tags

### Challenge 3: Erlang's Actual Performance

**Research finding**: Erlang's selective receive is also **O(n)** in general case!

From Erlang documentation:
> "The receive statement scans the mailbox from the beginning until a matching message is found."

**Erlang optimizations**:
1. Compiler optimization for simple patterns
2. Highly optimized VM implementation
3. Accepts O(n) as acceptable tradeoff

**Implication**: O(n) may be acceptable if:
- Mailboxes stay small (< 100 messages)
- Linear scan is well-optimized
- Trade complexity for simplicity

---

## Design Options

### Option 1: Optimized Linear Scan (Recommended)

**Approach**: Keep current O(n) design but optimize the scan itself.

```rust
// Current
let index = mailbox.messages.iter().position(|x| {
    if let Some(tag) = x.tag() {
        tags.contains(&tag)  // This is O(m) where m = tags.len()
    } else {
        false
    }
});

// Optimized
let tags_set: HashSet<i64> = tags.iter().copied().collect();  // O(m)
let index = mailbox.messages.iter().position(|x| {
    x.tag().map_or(false, |t| tags_set.contains(&t))  // O(1) per message
});
```

**Performance**:
- Before: O(n * m) where n = messages, m = tags
- After: O(n + m)
- For typical case (m = 1-5): Significant improvement

**Pros**:
- Simple, minimal code change
- No memory overhead
- Maintains all ordering guarantees
- Easy to test and verify

**Cons**:
- Still O(n) in mailbox size
- Not a "true" O(1) solution

### Option 2: Hybrid with Index

**Approach**: Build index lazily when mailbox grows large.

```rust
struct InnerMessageMailbox {
    messages: VecDeque<Message>,
    // Lazy index: only built when messages.len() > THRESHOLD
    tag_index: Option<HashMap<i64, Vec<usize>>>,
}

const INDEX_THRESHOLD: usize = 100;

impl InnerMessageMailbox {
    fn pop_with_tags(&mut self, tags: &[i64]) -> Option<Message> {
        if self.messages.len() > INDEX_THRESHOLD {
            // Use index
            self.ensure_index_built();
            // ... O(1) lookup
        } else {
            // Linear scan for small mailboxes
            // ... O(n) scan
        }
    }
}
```

**Pros**:
- Best of both worlds
- No overhead for small mailboxes (common case)
- O(1) for large mailboxes (rare but important)

**Cons**:
- Complex implementation
- Index maintenance on insert/remove
- More code to test

### Option 3: Separate Tagged/Untagged Queues

**Approach**: As described in Challenge 2, Option B.

**Implementation complexity**: High  
**Performance**: O(1) selective receive, O(k) for pop(None)  
**Memory overhead**: Moderate  

**Verdict**: Only worthwhile if profiling shows tagged messages >> untagged.

---

## Recommendation

**Phase 2a (Immediate)**: Implement **Option 1** (Optimized Linear Scan)

**Rationale**:
1. **Simplicity**: Minimal code change, easy to verify
2. **Real-world impact**: Most mailboxes are small (< 100 messages)
3. **Erlang parity**: Erlang also uses O(n), so we're not falling behind
4. **Risk**: Very low - optimization, not architecture change

**Phase 2b (Future)**: Implement **Option 2** (Hybrid with Index)

**Conditions**:
1. Profiling shows mailboxes > 100 messages are common
2. Selective receive is a bottleneck in real applications
3. Sufficient test coverage for complex index logic

---

## Implementation Plan (Phase 2a)

### Step 1: Optimize tag lookup

```rust
// In pop() method
if let Some(tags) = tags {
    // Build HashSet once
    let tags_set: HashSet<i64> = tags.iter().copied().collect();
    
    let index = mailbox.messages.iter().position(|x| {
        x.tag().map_or(false, |t| tags_set.contains(&t))
    });
    
    if let Some(index) = index {
        return mailbox.messages.remove(index).expect("must exist");
    }
}
```

**Performance gain**:
- 1000 messages, 3 tags: 1000 × 3 = 3000 ops → 1000 + 3 = 1003 ops (**3x faster**)
- 10000 messages, 5 tags: 10000 × 5 = 50000 ops → 10000 + 5 = 10005 ops (**5x faster**)

### Step 2: Add benchmarks

Create `benches/mailbox.rs`:
```rust
#[bench]
fn bench_selective_receive_1000_msgs(b: &mut Bencher) {
    let mailbox = MessageMailbox::default();
    // Populate with 1000 messages
    for i in 0..1000 {
        mailbox.push(Message::LinkDied(Some(i)));
    }
    
    b.iter(|| {
        // Worst case: search for last message
        let result = mailbox.pop(Some(&[999]));
    });
}
```

### Step 3: Measure and document

- Benchmark before/after optimization
- Document in PHASE2_COMPLETE.md
- Update PRIORITY_IMPROVEMENTS.md with actual results

---

## Phase 2b Roadmap (Future)

**If and when needed**:

1. **Profiling** (1 day)
   - Instrument production workloads
   - Measure actual mailbox sizes
   - Identify if selective receive is a bottleneck

2. **Design** (2 days)
   - Finalize hybrid index design
   - Design index maintenance strategy
   - Plan testing approach

3. **Implementation** (3-5 days)
   - Implement lazy indexing
   - Add comprehensive tests
   - Benchmark against Option 1

4. **Validation** (1-2 days)
   - Performance regression tests
   - Memory overhead measurement
   - Production pilot

**Total**: ~7-10 days **only if profiling justifies it**

---

## Decision

**Proceed with Phase 2a** (Optimized Linear Scan)

**Defer Phase 2b** (Hybrid Index) until data shows it's needed

**Rationale**: Premature optimization is the root of all evil. The O(n*m) → O(n+m) optimization is:
- Low risk
- High value (3-5x improvement for common cases)
- Maintains simplicity
- Erlang-compatible

---

## References

- Erlang selective receive: http://erlang.org/doc/reference_manual/expressions.html#receive
- Erlang mailbox implementation: https://github.com/erlang/otp/blob/master/erts/emulator/beam/erl_message.c
- CORE_VALUES.md: "Simplicity scales better than complexity" (line 360)

---

**Next Steps**: Implement Phase 2a optimization (ETA: 1-2 hours)
