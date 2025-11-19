#Caveats

## FlowTransaction Design Caveats

## Missing MVCC Read Set Tracking

### The Issue

FlowTransaction does not currently track the **read set** - the set of keys that have been read during the transaction.
In a general-purpose MVCC (Multi-Version Concurrency Control) implementation, tracking reads is essential for conflict
detection.

**Why read tracking matters in MVCC:**

- When a transaction commits, it needs to verify that none of the keys it read have been modified by concurrent
  transactions
- This prevents **read-write conflicts** where:
    1. Transaction A reads key K at version N
    2. Transaction B modifies key K and commits at version N+1
    3. Transaction A commits at version N+2
    4. Without conflict detection, Transaction A's decisions based on the old value of K may be invalid

**Standard MVCC conflict detection:**

```rust
// Pseudocode for what would be needed
struct FlowTransaction {
    read_set: HashSet<EncodedKey>,  // Track all keys read
    read_version: CommitVersion,    // Version we read at
    // ... other fields
}

fn commit(&self, parent: &mut StandardCommandTransaction) -> Result<()> {
    // Validate read set hasn't changed
    for key in &self.read_set {
        if parent.has_changed_since(key, self.read_version)? {
            return Err(ConflictError::ReadWriteConflict);
        }
    }
    // Apply writes...
}
```

### Why It's Acceptable For Flow Processing

**This design limitation is acceptable for the current use case** because of how flows are processed:

1. **Sequential CDC Processing**
    - Flows are driven by Change Data Capture (CDC) events
    - CDC events arrive and are processed in sequential order
    - Each CDC event triggers a distinct, independent flow

2. **Parallel Processing of Independent Flows**
    - Only **independent flows** are processed in parallel
    - Independent flows, by definition, operate on disjoint key spaces
    - Flow A reading/writing keys {K1, K2, K3}
    - Flow B reading/writing keys {K4, K5, K6}
    - No overlap → no conflicts possible

3. **Design Guarantee**
    - The flow processing system ensures that flows processed in parallel never access overlapping keys
    - This is enforced at the flow scheduling level, not the transaction level
    - Therefore, read-write conflicts between parallel FlowTransactions **cannot occur by design**

### Example: Why This Works

```rust
// CDC Event Stream (sequential)
// Event 1: User A updated their profile → Flow A
// Event 2: User B updated their profile → Flow B
// Event 3: User A added a post → Flow C

// Parallel Processing Groups:
// Group 1: [Flow A, Flow B]  ✓ Safe - different users, disjoint keys
// Group 2: [Flow C]           ✓ Must wait - same user as Flow A

// Flow A reads/writes: user:A:profile, user:A:settings
// Flow B reads/writes: user:B:profile, user:B:settings
// No overlap → no conflict possible even without read tracking
```

### When This Would Be Insufficient

This design would **NOT** be safe for:

1. **General-purpose parallel transactions**
    - Multiple concurrent transactions operating on arbitrary overlapping key spaces
    - Example: Banking application with concurrent transfers affecting same accounts

2. **Interactive workloads**
    - User-initiated transactions that may conflict with each other
    - Example: Two users editing the same document simultaneously

3. **Optimistic concurrency control**
    - Applications relying on transaction-level conflict detection for correctness
    - Example: Inventory management with concurrent stock updates

### Future Considerations

If FlowTransaction needs to support general-purpose parallel processing, we would need to add:

1. **Read Set Tracking**
   ```rust
   pub struct FlowTransaction {
       read_set: HashSet<EncodedKey>,
       // ...
   }
   ```

2. **Conflict Detection on Commit**
   ```rust
   pub fn commit(self, parent: &mut StandardCommandTransaction) -> Result<()> {
       // Validate no read-write conflicts
       self.validate_read_set(parent)?;
       // Apply writes
       self.apply_pending_writes(parent)?;
       Ok(())
   }
   ```

3. **Serializable Snapshot Isolation (SSI)**
    - Track both read-write and write-read conflicts
    - Implement full serializability guarantees
    - See: "Serializable Snapshot Isolation in PostgreSQL" (2012)

## Summary

**Current State:**

- ✓ Safe for CDC-driven flow processing with independent flows
- ✓ Enables parallel execution without coordination overhead
- ✓ Simple implementation without read tracking complexity

**Limitation:**

- ✗ Not suitable for general-purpose parallel transactions
- ✗ Relies on application-level guarantees of flow independence
- ✗ No transaction-level conflict detection

**Recommendation:**

- Document flow independence requirements clearly
- Ensure flow scheduler enforces disjoint key spaces
- Add assertions/tests validating flow independence property

## Snapshot-Based Backfill Semantics

### The Behavior

When a flow is created, it performs a **snapshot-based backfill** - reading the current state of source tables at a
point in time, rather than replaying the full event history.

**What this means:**

If a source table had this history:

1. Insert row with value "A"
2. Update to "B"
3. Update to "C"

The backfill sees only a single **Insert "C"** - the final state.

### Comparison with Kafka

Apache Kafka and similar event streaming systems use **event replay semantics**:

- Replay all events from the beginning (or retention window)
- Process Insert → Update → Update in sequence
- Operators see the complete mutation history

ReifyDB's current implementation uses **snapshot semantics**:

- Single range scan at a point-in-time version
- Only sees current state of each row
- No mutation history

### Implications for Stateful Operators

**Stateless operators** (filter, map): No difference - final result is the same.

**Stateful operators** (distinct, aggregate, join, take): May produce different results:

**Example with COUNT aggregate:**

```
Event history: Insert(A), Update(A→B), Delete(B), Insert(B)

Event replay:
  - Insert(A): count=1
  - Update: count=1
  - Delete: count=0
  - Insert(B): count=1
  Final: count=1

Snapshot:
  - Sees row B exists
  - Insert(B): count=1
  Final: count=1
```

In this case, results match. But intermediate states differ, and some edge cases may produce different final results.

**Example with DISTINCT tracking first occurrence:**

```
Event history: Insert(row1, value=X), Insert(row2, value=X), Delete(row1)

Event replay:
  - Insert row1: emit row1 (first with value X)
  - Insert row2: no emit (duplicate)
  - Delete row1: emit row2 (new first occurrence)
  Final: row2 is the representative

Snapshot:
  - Only sees row2 exists with value X
  - Insert row2: emit row2
  Final: row2 is the representative
```

Results match here, but the semantic path is different.

### Performance Considerations

**Large backfills can slow down the entire system** because:

1. **Synchronous execution** - Backfill runs during flow registration, blocking the CDC consumer
2. **Full table scan** - Reads all rows from source tables into memory
3. **Sequential processing** - All rows processed through the operator pipeline before returning
4. **Lock contention** - May hold transaction locks for extended periods

**Impact:**

- Other flows waiting for CDC processing are delayed
- System appears unresponsive during large backfills
- Memory pressure from loading all rows at once

### Future Improvements

Potential optimizations:

1. **Async backfill** - Run backfill in background, buffer CDC events until complete
2. **Streaming backfill** - Process rows in batches without loading all into memory
3. **Parallel backfill** - Split range scan across multiple workers
4. **Event replay** - replay full event history for exact semantics

### Summary

**Current State:**

- ✓ Fast for small tables
- ✓ Simple implementation
- ✓ Sufficient for "current state" materialized views

**Limitations:**

- ✗ May differ from "as if flow existed from beginning" semantics
- ✗ Large backfills block CDC processing
- ✗ Memory pressure from full table loads
- ✗ No streaming/pagination for large tables
