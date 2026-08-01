// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::simulator::{
	executor::{Executor, OpResult},
	invariant::{Invariant, NoDirtyReads, NoLostUpdates, ReadYourOwnWrites, SnapshotConsistency},
	schedule::{Schedule, TxId},
};

#[test]
fn test_dirty_read_prevented() {
	// T2 reads before T1 commits, so it must not observe T1's pending write.
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

	let read_result = trace.get_value(3).expect("step 3 should be a Get");
	assert!(read_result.is_none(), "T2 should not see T1's uncommitted write");

	NoDirtyReads.check(&trace).expect("NoDirtyReads invariant should hold");
}

#[test]
fn test_dirty_read_prevented_with_prior_value() {
	// With a prior committed value present, T2 must fall back to it rather than to none.
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "x", "initial")
		.commit(0)
		.begin(1)
		.begin(2)
		.set(1, "x", "updated")
		.get(2, "x") // step 6: should see "initial", not "updated"
		.commit(1)
		.commit(2)
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

	let read_result = trace.get_value(6).expect("step 6 should be a Get");
	assert!(read_result.is_some(), "T2 should see the initial value");

	NoDirtyReads.check(&trace).expect("NoDirtyReads invariant should hold");
}

#[test]
fn test_lost_update_prevented() {
	// Two concurrent read-modify-writes on one key: letting both commit loses one of them.
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "x", "0")
		.commit(0)
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

	let t1_committed = trace.committed.contains_key(&TxId(1));
	let t2_committed = trace.committed.contains_key(&TxId(2));
	assert!(
		!(t1_committed && t2_committed),
		"both T1 and T2 committed - lost update! T1={}, T2={}",
		t1_committed,
		t2_committed
	);
}

#[test]
fn test_write_skew_detected() {
	// Write skew: T1 reads x writes y, T2 reads y writes x. The write sets are disjoint, so only
	// read-write conflict detection can catch this one.
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "x", "1")
		.set(0, "y", "1")
		.commit(0)
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

	assert!(
		!(t1_committed && t2_committed),
		"both T1 and T2 committed - write skew allowed. T1={}, T2={}",
		t1_committed,
		t2_committed
	);
}

#[test]
fn test_rollback_no_effect() {
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
	// Non-overlapping lifetimes on distinct keys must never be reported as conflicting.
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
	// A scan resolves against the begin-time snapshot, so a concurrent insert stays invisible.
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
	// An own tombstone must mask the committed value, not fall through to it.
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
	// A scan merges pending writes over the snapshot; dropping them would hide the tx's own insert.
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
	// The merge must honour own tombstones, not just own sets.
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
	// An uncommitted tombstone is a dirty read just as much as an uncommitted set.
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
	// A query re-reading the same key must return the same value even though a command committed
	// over it in between; a moving read version would break repeatable reads.
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
	// A key committed after the query began must not appear in its scan (no phantom).
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
	// One long-lived query spanning several commits: every read and the final scan must still
	// resolve against its begin-time snapshot.
	let schedule = Schedule::builder()
		.begin(0)
		.set(0, "x", "v0")
		.set(0, "y", "v0")
		.commit(0)
		.begin_query(1)
		.get(1, "x")  // step 5: sees "v0"
		.begin(2)
		.set(2, "x", "v1")
		.commit(2)
		.get(1, "x")  // step 9: still "v0"
		.begin(3)
		.set(3, "y", "v1")
		.commit(3)
		.get(1, "y")  // step 13: still "v0"
		.scan(1)  // step 14: sees x=v0, y=v0
		.build();

	let mut executor = Executor::new();
	let trace = executor.run(&schedule);

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
