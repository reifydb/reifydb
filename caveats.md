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
