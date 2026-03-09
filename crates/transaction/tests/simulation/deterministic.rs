// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::simulator::{
	executor::{Executor, OpResult},
	invariant::{Invariant, NoDirtyReads, NoLostUpdates, ReadYourOwnWrites, SnapshotConsistency},
	schedule::{Schedule, TxId},
};

#[test]
fn test_dirty_read_prevented() {
	// T1 writes a key, T2 reads the same key before T1 commits.
	// T2 should NOT see T1's uncommitted write.
	let schedule = Schedule::builder()
		.begin(1)
		.begin(2)
		.set(1, "x", "dirty_value")
		.get(2, "x") // step 3: should see None, not "dirty_value"
		.commit(1)
		.commit(2)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	// T2's read should return None (key didn't exist before T1 committed)
	let read_result = trace.get_value(3).expect("step 3 should be a Get");
	assert!(read_result.is_none(), "T2 should not see T1's uncommitted write");

	NoDirtyReads.check(&trace).expect("NoDirtyReads invariant should hold");
}

#[test]
fn test_dirty_read_prevented_with_prior_value() {
	// Setup: write initial value, then T1 overwrites, T2 reads before T1 commits.
	let schedule = Schedule::builder()
		// Setup: write initial value
		.begin(0)
		.set(0, "x", "initial")
		.commit(0)
		// T1 overwrites
		.begin(1)
		.begin(2)
		.set(1, "x", "updated")
		.get(2, "x") // step 6: should see "initial", not "updated"
		.commit(1)
		.commit(2)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	// T2's read should return "initial" (T1 hasn't committed yet)
	let read_result = trace.get_value(6).expect("step 6 should be a Get");
	assert!(read_result.is_some(), "T2 should see the initial value");

	NoDirtyReads.check(&trace).expect("NoDirtyReads invariant should hold");
}

#[test]
fn test_lost_update_prevented() {
	// T1 and T2 both read-modify-write the same key. At least one must abort.
	let schedule = Schedule::builder()
		// Setup
		.begin(0)
		.set(0, "x", "0")
		.commit(0)
		// T1 and T2 both read then write x
		.begin(1)
		.begin(2)
		.get(1, "x")
		.get(2, "x")
		.set(1, "x", "from_t1")
		.set(2, "x", "from_t2")
		.commit(1)
		.commit(2) // step 10: should conflict
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	NoLostUpdates.check(&trace).expect("NoLostUpdates invariant should hold");

	// At least one must have aborted or conflicted
	let t1_committed = trace.committed.contains_key(&TxId(1));
	let t2_committed = trace.committed.contains_key(&TxId(2));
	assert!(
		!(t1_committed && t2_committed),
		"both T1 and T2 committed — lost update! T1={}, T2={}",
		t1_committed,
		t2_committed
	);
}

#[test]
fn test_write_skew_detected() {
	// Classic write skew: T1 reads X writes Y, T2 reads Y writes X.
	// Under snapshot isolation with conflict detection on reads, at least one aborts.
	let schedule = Schedule::builder()
		// Setup
		.begin(0)
		.set(0, "x", "1")
		.set(0, "y", "1")
		.commit(0)
		// T1: read x, write y
		.begin(1)
		.begin(2)
		.get(1, "x")
		.get(2, "y")
		.set(1, "y", "t1_wrote_y")
		.set(2, "x", "t2_wrote_x")
		.commit(1)
		.commit(2)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	let t1_committed = trace.committed.contains_key(&TxId(1));
	let t2_committed = trace.committed.contains_key(&TxId(2));

	// Under serializable snapshot isolation with read tracking, at least one should conflict.
	// Note: some MVCC implementations allow write skew under snapshot isolation.
	// This test documents the engine's actual behavior.
	assert!(
		!(t1_committed && t2_committed),
		"both T1 and T2 committed — write skew allowed. T1={}, T2={}",
		t1_committed,
		t2_committed
	);
}

#[test]
fn test_rollback_no_effect() {
	// A rolled-back transaction should not affect the final state.
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "x", "initial")
		.commit(0)
		.begin(1)
		.set(1, "x", "should_not_persist")
		.rollback(1)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	assert_eq!(
		trace.final_state.get("x").map(String::as_str),
		Some("initial"),
		"rolled back write should not persist"
	);
}

#[test]
fn test_sequential_commits() {
	// Sequential non-overlapping transactions should all succeed.
	let schedule = Schedule::builder()
		.begin(1)
		.set(1, "a", "1")
		.commit(1)
		.begin(2)
		.set(2, "b", "2")
		.commit(2)
		.begin(3)
		.set(3, "c", "3")
		.commit(3)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	assert!(trace.committed.contains_key(&TxId(1)));
	assert!(trace.committed.contains_key(&TxId(2)));
	assert!(trace.committed.contains_key(&TxId(3)));
	assert_eq!(trace.final_state.get("a").map(String::as_str), Some("1"));
	assert_eq!(trace.final_state.get("b").map(String::as_str), Some("2"));
	assert_eq!(trace.final_state.get("c").map(String::as_str), Some("3"));
}

#[test]
fn test_scan_sees_snapshot() {
	// T2's scan should see the snapshot at T2's begin time.
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "a", "1")
		.set(0, "b", "2")
		.commit(0)
		.begin(1)
		.begin(2)
		.set(1, "c", "3")
		.scan(2) // step 7: should see a=1, b=2 but NOT c=3
		.commit(1)
		.commit(2)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	// Check scan results at step 7
	match &trace.results[7].result {
		OpResult::ScanResult(pairs) => {
			assert_eq!(pairs.len(), 2, "scan should see exactly 2 keys (a, b)");
		}
		other => panic!("expected ScanResult, got {:?}", other),
	}

	SnapshotConsistency.check(&trace).expect("SnapshotConsistency invariant should hold");
}

#[test]
fn test_read_your_own_writes() {
	// A transaction that sets a key should read it back.
	let schedule = Schedule::builder()
		.begin(1)
		.set(1, "x", "hello")
		.get(1, "x") // step 2: should see "hello"
		.commit(1)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	let read_result = trace.get_value(2).expect("step 2 should be a Get");
	assert!(read_result.is_some(), "should read back own write");

	ReadYourOwnWrites.check(&trace).expect("ReadYourOwnWrites invariant should hold");
}

#[test]
fn test_read_your_own_writes_after_remove() {
	// A transaction that removes a key should read None for it.
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "x", "initial")
		.commit(0)
		.begin(1)
		.remove(1, "x")
		.get(1, "x") // step 5: should see None (own remove)
		.commit(1)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	let read_result = trace.get_value(5).expect("step 5 should be a Get");
	assert!(read_result.is_none(), "should read None after own remove");

	ReadYourOwnWrites.check(&trace).expect("ReadYourOwnWrites invariant should hold");
}

#[test]
fn test_read_your_own_writes_overwrite() {
	// A transaction that writes a key twice should read the latest value.
	let schedule = Schedule::builder()
		.begin(1)
		.set(1, "x", "first")
		.set(1, "x", "second")
		.get(1, "x") // step 3: should see "second"
		.commit(1)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	ReadYourOwnWrites.check(&trace).expect("ReadYourOwnWrites invariant should hold");
}

#[test]
fn test_snapshot_consistency_scan_with_own_writes() {
	// Scan should include own writes merged with snapshot.
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "a", "1")
		.set(0, "b", "2")
		.commit(0)
		.begin(1)
		.set(1, "c", "3")
		.scan(1) // step 6: should see a=1, b=2, c=3
		.commit(1)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	match &trace.results[6].result {
		OpResult::ScanResult(pairs) => {
			assert_eq!(pairs.len(), 3, "scan should see 3 keys (a, b, c including own write)");
		}
		other => panic!("expected ScanResult, got {:?}", other),
	}

	SnapshotConsistency.check(&trace).expect("SnapshotConsistency invariant should hold");
}

#[test]
fn test_snapshot_consistency_scan_after_remove() {
	// Scan should exclude keys removed by own transaction.
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "a", "1")
		.set(0, "b", "2")
		.set(0, "c", "3")
		.commit(0)
		.begin(1)
		.remove(1, "b")
		.scan(1) // step 7: should see a=1, c=3 (b removed)
		.commit(1)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	match &trace.results[7].result {
		OpResult::ScanResult(pairs) => {
			assert_eq!(pairs.len(), 2, "scan should see 2 keys (a, c; b removed)");
		}
		other => panic!("expected ScanResult, got {:?}", other),
	}

	SnapshotConsistency.check(&trace).expect("SnapshotConsistency invariant should hold");
}

#[test]
fn test_no_dirty_reads_after_remove() {
	// T1 removes a key, T2 should still see the original value (not the remove).
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "x", "exists")
		.commit(0)
		.begin(1)
		.begin(2)
		.remove(1, "x")
		.get(2, "x") // step 6: should see "exists" (T1's remove is uncommitted)
		.commit(1)
		.commit(2)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	let read_result = trace.get_value(6).expect("step 6 should be a Get");
	assert!(read_result.is_some(), "T2 should still see 'exists' (T1's remove uncommitted)");

	NoDirtyReads.check(&trace).expect("NoDirtyReads invariant should hold");
}

#[test]
fn test_query_sees_committed_data() {
	// T0 writes key, commits. T1 begins query, reads key, sees committed value.
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "x", "hello")
		.commit(0)
		.begin_query(1)
		.get(1, "x") // step 4: should see "hello"
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	let read_result = trace.get_value(4).expect("step 4 should be a Get");
	assert!(read_result.is_some(), "query should see committed value");

	NoDirtyReads.check(&trace).expect("NoDirtyReads invariant should hold");
	SnapshotConsistency.check(&trace).expect("SnapshotConsistency invariant should hold");
}

#[test]
fn test_query_cannot_write() {
	// T0 begins query, attempts Set → expects error.
	let schedule = Schedule::builder()
		.begin_query(0)
		.set(0, "x", "nope") // step 1: should error
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	match &trace.results[1].result {
		OpResult::Error(msg) => {
			assert!(msg.contains("read transaction"), "expected read-only error, got: {}", msg);
		}
		other => panic!("expected Error for Set on query, got {:?}", other),
	}
}

#[test]
fn test_query_cannot_commit() {
	// T0 begins query, attempts Commit → expects error.
	let schedule = Schedule::builder()
		.begin_query(0)
		.commit(0) // step 1: should error
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	match &trace.results[1].result {
		OpResult::Error(msg) => {
			assert!(msg.contains("read transaction"), "expected read-only error, got: {}", msg);
		}
		other => panic!("expected Error for Commit on query, got {:?}", other),
	}
}

#[test]
fn test_query_snapshot_isolation() {
	// T0 writes k1, commits. T1 begins query, reads k1.
	// T2 begins command, writes k1 new value, commits.
	// T1 reads k1 again → still sees original snapshot value.
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "k1", "original")
		.commit(0)
		.begin_query(1)
		.get(1, "k1")  // step 4: sees "original"
		.begin(2)
		.set(2, "k1", "updated")
		.commit(2)
		.get(1, "k1")  // step 8: should still see "original"
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	let first_read = trace.get_value(4).expect("step 4 should be a Get");
	assert!(first_read.is_some(), "query should see committed value");

	let second_read = trace.get_value(8).expect("step 8 should be a Get");
	assert_eq!(first_read, second_read, "query should see same snapshot despite concurrent commit");

	NoDirtyReads.check(&trace).expect("NoDirtyReads invariant should hold");
	SnapshotConsistency.check(&trace).expect("SnapshotConsistency invariant should hold");
}

#[test]
fn test_query_scan_snapshot() {
	// T0 sets up keys, commits. T1 begins query. T2 begins command, writes new key, commits.
	// T1 scans → sees only snapshot at T1's begin time.
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "a", "1")
		.set(0, "b", "2")
		.commit(0)
		.begin_query(1)
		.begin(2)
		.set(2, "c", "3")
		.commit(2)
		.scan(1)  // step 8: should see a=1, b=2 but NOT c=3
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	match &trace.results[8].result {
		OpResult::ScanResult(pairs) => {
			assert_eq!(
				pairs.len(),
				2,
				"query scan should see exactly 2 keys (a, b), not the concurrently committed c"
			);
		}
		other => panic!("expected ScanResult, got {:?}", other),
	}

	SnapshotConsistency.check(&trace).expect("SnapshotConsistency invariant should hold");
}

#[test]
fn test_interleaved_query_and_command() {
	// Multiple command txns interleaved with a query txn.
	// Query reads remain consistent with its snapshot while command txns commit changes.
	let schedule = Schedule::builder()
		// Setup
		.begin(0)
		.set(0, "x", "v0")
		.set(0, "y", "v0")
		.commit(0)
		// Query starts
		.begin_query(1)
		.get(1, "x")  // step 5: sees "v0"
		// Command T2 updates x
		.begin(2)
		.set(2, "x", "v1")
		.commit(2)
		// Query still sees old x
		.get(1, "x")  // step 9: still "v0"
		// Command T3 updates y
		.begin(3)
		.set(3, "y", "v1")
		.commit(3)
		// Query still sees old y
		.get(1, "y")  // step 13: still "v0"
		// Query scan sees original snapshot
		.scan(1)  // step 14: sees x=v0, y=v0
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	// All query reads should see the original values
	let read_x_1 = trace.get_value(5).expect("step 5 should be a Get");
	let read_x_2 = trace.get_value(9).expect("step 9 should be a Get");
	let read_y = trace.get_value(13).expect("step 13 should be a Get");

	assert_eq!(read_x_1, read_x_2, "query should see same x value across reads");
	assert!(read_y.is_some(), "query should see y");

	match &trace.results[14].result {
		OpResult::ScanResult(pairs) => {
			assert_eq!(pairs.len(), 2, "query scan should see exactly 2 keys (x, y)");
		}
		other => panic!("expected ScanResult, got {:?}", other),
	}

	NoDirtyReads.check(&trace).expect("NoDirtyReads invariant should hold");
	SnapshotConsistency.check(&trace).expect("SnapshotConsistency invariant should hold");
}
