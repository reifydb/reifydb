// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{common::CommitVersion, encoded::key::EncodedKeyRange};
use reifydb_transaction::transaction::replica::ReplicaTransaction;

use super::test_multi;
use crate::{as_key, as_values, from_row, multi::transaction::FromRow};

/// Mirrors write.rs::test_write — basic replica write + query read.
#[test]
fn test_replica_write() {
	let engine = test_multi();
	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		assert_eq!(tx.version(), CommitVersion(100));

		tx.set(&as_key!("foo"), as_values!("foo1".to_string())).unwrap();
		tx.commit_at_version().unwrap();
	}

	{
		let rx = engine.begin_query().unwrap();
		assert_eq!(rx.version(), CommitVersion(100));
		let value: String = from_row!(String, rx.get(&as_key!("foo")).unwrap().unwrap().row());
		assert_eq!(value.as_str(), "foo1");
	}
}

/// Mirrors write.rs::test_multiple_write — multiple keys in one replica commit.
#[test]
fn test_replica_multiple_write() {
	let engine = test_multi();
	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		for i in 0..10 {
			tx.set(&as_key!(i), as_values!(i)).unwrap();
		}

		// Read-your-writes within the transaction
		let sv = tx.get(&as_key!(8)).unwrap().unwrap();
		assert_eq!(from_row!(i32, *sv.row()), 8);
		drop(sv);

		assert!(tx.contains_key(&as_key!(8)).unwrap());
		tx.commit_at_version().unwrap();
	}

	let rx = engine.begin_query().unwrap();
	assert!(rx.contains_key(&as_key!(8)).unwrap());
	let sv = rx.get(&as_key!(8)).unwrap().unwrap();
	assert_eq!(from_row!(i32, *sv.row()), 8);
}

/// Mirrors get.rs::test_read_after_write — sequential replica commits, query after each.
#[test]
fn test_replica_read_after_write() {
	let engine = test_multi();

	for i in 0u64..10 {
		let version = CommitVersion((i + 1) * 100);
		let k = as_key!(i);
		let v = as_values!(i);

		let mut tx = engine.begin_replica(version).unwrap();
		tx.set(&k, v.clone()).unwrap();
		tx.commit_at_version().unwrap();

		let rx = engine.begin_query().unwrap();
		let sv = rx.get(&k).unwrap().unwrap();
		assert_eq!(*sv.row(), v);
	}
}

/// Mirrors version.rs::test_versions — version tracking and time-travel reads.
#[test]
fn test_replica_versions() {
	let engine = test_multi();
	let k0 = as_key!(0);

	// Commit 9 versions at primary versions 100, 200, ..., 900
	for i in 1i32..10 {
		let version = CommitVersion(i as u64 * 100);
		let mut tx = engine.begin_replica(version).unwrap();
		tx.set(&k0, as_values!(i)).unwrap();
		tx.commit_at_version().unwrap();
		assert_eq!(engine.version().unwrap(), version);
	}

	// Time-travel reads at each historical version
	for idx in 1i32..10 {
		let read_version = CommitVersion(idx as u64 * 100 + 1); // exclusive: read at version+1 sees version
		let mut txn = engine.begin_command().unwrap();
		txn.read_as_of_version_exclusive(read_version);

		let tv = txn.get(&k0).unwrap().unwrap();
		assert_eq!(idx, from_row!(i32, tv.row()));
	}

	// Latest read sees version 900's value
	let rx = engine.begin_query().unwrap();
	let sv = rx.get(&k0).unwrap().unwrap();
	assert_eq!(9, from_row!(i32, sv.row()));
}

/// Mirrors range.rs::test_range — forward and reverse range after replica commit.
#[test]
fn test_replica_range() {
	let engine = test_multi();
	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&as_key!(1), as_values!(1)).unwrap();
		tx.set(&as_key!(2), as_values!(2)).unwrap();
		tx.set(&as_key!(3), as_values!(3)).unwrap();
		tx.commit_at_version().unwrap();
	}

	let four_to_one = EncodedKeyRange::start_end(Some(as_key!(4)), Some(as_key!(1)));

	let rx = engine.begin_query().unwrap();
	let items: Vec<_> = rx.range(four_to_one.clone(), 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=3).rev().zip(items) {
		assert_eq!(v.key, as_key!(expected));
		assert_eq!(v.row, as_values!(expected));
		assert_eq!(v.version, CommitVersion(100));
	}

	let items: Vec<_> = rx.range_rev(four_to_one, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=3).zip(items) {
		assert_eq!(v.key, as_key!(expected));
		assert_eq!(v.row, as_values!(expected));
		assert_eq!(v.version, CommitVersion(100));
	}
}

/// Mirrors range.rs::test_range2 — two replica commits, range sees merged data.
#[test]
fn test_replica_range_multiple_commits() {
	let engine = test_multi();

	// First commit at version 100
	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&as_key!(1), as_values!(1)).unwrap();
		tx.set(&as_key!(2), as_values!(2)).unwrap();
		tx.set(&as_key!(3), as_values!(3)).unwrap();
		tx.commit_at_version().unwrap();
	}

	// Second commit at version 200
	{
		let mut tx = engine.begin_replica(CommitVersion(200)).unwrap();
		tx.set(&as_key!(4), as_values!(4)).unwrap();
		tx.set(&as_key!(5), as_values!(5)).unwrap();
		tx.set(&as_key!(6), as_values!(6)).unwrap();
		tx.commit_at_version().unwrap();
	}

	let seven_to_one = EncodedKeyRange::start_end(Some(as_key!(7)), Some(as_key!(1)));

	let rx = engine.begin_query().unwrap();
	let items: Vec<_> = rx.range(seven_to_one, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	assert_eq!(items.len(), 6);
	for (expected, v) in (1..=6).rev().zip(items) {
		assert_eq!(v.key, as_key!(expected));
		assert_eq!(v.row, as_values!(expected));
	}
}

/// Mirrors rollback.rs::test_rollback_same_tx — rollback within replica transaction.
#[test]
fn test_replica_rollback_same_tx() {
	let engine = test_multi();
	let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
	tx.set(&as_key!(1), as_values!(1)).unwrap();
	tx.rollback().unwrap();
	assert!(tx.get(&as_key!(1)).unwrap().is_none());
}

/// Mirrors rollback.rs::test_rollback_different_tx — rollback not visible to queries.
#[test]
fn test_replica_rollback_different_tx() {
	let engine = test_multi();
	let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
	tx.set(&as_key!(1), as_values!(1)).unwrap();
	tx.rollback().unwrap();

	let rx = engine.begin_query().unwrap();
	assert!(rx.get(&as_key!(1)).unwrap().is_none());
}

/// Empty replica commit (no writes) should not error.
#[test]
fn test_replica_empty_commit() {
	let engine = test_multi();
	let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
	tx.commit_at_version().unwrap();
}

/// advance_version_for_replica — version gaps for data-only commits.
#[test]
fn test_advance_version_for_replica() {
	let engine = test_multi();

	// Replica commit at version 100
	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&as_key!("a"), as_values!("v1".to_string())).unwrap();
		tx.commit_at_version().unwrap();
	}

	// Skip version 200 (data-only commit on primary, no catalog changes)
	engine.advance_version_for_replica(CommitVersion(200));

	// Replica commit at version 300
	{
		let mut tx = engine.begin_replica(CommitVersion(300)).unwrap();
		tx.set(&as_key!("b"), as_values!("v2".to_string())).unwrap();
		tx.commit_at_version().unwrap();
	}

	assert_eq!(engine.version().unwrap(), CommitVersion(300));

	let rx = engine.begin_query().unwrap();
	let a: String = from_row!(String, rx.get(&as_key!("a")).unwrap().unwrap().row());
	let b: String = from_row!(String, rx.get(&as_key!("b")).unwrap().unwrap().row());
	assert_eq!(a, "v1");
	assert_eq!(b, "v2");
}

/// Read-your-writes within a replica transaction.
#[test]
fn test_replica_read_your_writes() {
	let engine = test_multi();
	let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();

	// Write key A, then read it back
	tx.set(&as_key!("a"), as_values!("val_a".to_string())).unwrap();
	let sv = tx.get(&as_key!("a")).unwrap().unwrap();
	assert_eq!(from_row!(String, *sv.row()), "val_a");
	drop(sv);

	// Write key B, then read both
	tx.set(&as_key!("b"), as_values!("val_b".to_string())).unwrap();
	assert!(tx.contains_key(&as_key!("a")).unwrap());
	assert!(tx.contains_key(&as_key!("b")).unwrap());

	tx.commit_at_version().unwrap();
}

/// Verify query version matches the latest replica version.
#[test]
fn test_replica_version_visible_to_queries() {
	let engine = test_multi();
	{
		let mut tx = engine.begin_replica(CommitVersion(500)).unwrap();
		tx.set(&as_key!(1), as_values!(1)).unwrap();
		tx.commit_at_version().unwrap();
	}

	let rx = engine.begin_query().unwrap();
	assert_eq!(rx.version(), CommitVersion(500));
}

/// Multiple sequential replica commits updating the same key.
#[test]
fn test_replica_sequential_commits() {
	let engine = test_multi();
	let k = as_key!(0);

	for i in 1i32..=3 {
		let version = CommitVersion(i as u64 * 10);
		let mut tx = engine.begin_replica(version).unwrap();
		tx.set(&k, as_values!(i)).unwrap();
		tx.commit_at_version().unwrap();
	}

	// Latest query sees version 30's value
	let rx = engine.begin_query().unwrap();
	assert_eq!(from_row!(i32, rx.get(&k).unwrap().unwrap().row()), 3);

	// Time-travel to version 20 sees value 2
	let mut txn = engine.begin_command().unwrap();
	txn.read_as_of_version_exclusive(CommitVersion(21));
	assert_eq!(from_row!(i32, txn.get(&k).unwrap().unwrap().row()), 2);
}

/// Replica overwrite — same key at different primary versions.
#[test]
fn test_replica_overwrite() {
	let engine = test_multi();
	let k = as_key!("foo");

	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&k, as_values!("v1".to_string())).unwrap();
		tx.commit_at_version().unwrap();
	}
	{
		let mut tx = engine.begin_replica(CommitVersion(200)).unwrap();
		tx.set(&k, as_values!("v2".to_string())).unwrap();
		tx.commit_at_version().unwrap();
	}

	// Latest sees v2
	let rx = engine.begin_query().unwrap();
	assert_eq!(from_row!(String, rx.get(&k).unwrap().unwrap().row()), "v2");

	// Time-travel to version 100 sees v1
	let mut txn = engine.begin_command().unwrap();
	txn.read_as_of_version_exclusive(CommitVersion(101));
	assert_eq!(from_row!(String, txn.get(&k).unwrap().unwrap().row()), "v1");
}

/// Replica remove — commit key then remove it at a later version.
#[test]
fn test_replica_remove() {
	let engine = test_multi();
	let k = as_key!(42);

	// Insert at version 100
	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&k, as_values!(42)).unwrap();
		tx.commit_at_version().unwrap();
	}

	// Remove at version 200
	{
		let mut tx = engine.begin_replica(CommitVersion(200)).unwrap();
		tx.remove(&k).unwrap();
		tx.commit_at_version().unwrap();
	}

	// Latest: key not found
	let rx = engine.begin_query().unwrap();
	assert!(rx.get(&k).unwrap().is_none());

	// Time-travel to version 100: key found
	let mut txn = engine.begin_command().unwrap();
	txn.read_as_of_version_exclusive(CommitVersion(101));
	let sv = txn.get(&k).unwrap().unwrap();
	assert_eq!(from_row!(i32, sv.row()), 42);
}

/// ReplicaTransaction basic write + commit + query.
#[test]
fn test_replica_transaction_write() {
	let engine = test_multi();
	{
		let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
		assert_eq!(tx.version(), CommitVersion(100));
		tx.set(&as_key!("x"), as_values!("y".to_string())).unwrap();
		tx.commit_at_version().unwrap();
	}

	let rx = engine.begin_query().unwrap();
	let v: String = from_row!(String, rx.get(&as_key!("x")).unwrap().unwrap().row());
	assert_eq!(v, "y");
}

/// Double commit returns AlreadyCommitted.
#[test]
fn test_replica_transaction_double_commit() {
	let engine = test_multi();
	let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
	tx.set(&as_key!(1), as_values!(1)).unwrap();
	tx.commit_at_version().unwrap();

	let err = tx.commit_at_version().unwrap_err();
	assert!(err.to_string().contains("committed"), "expected AlreadyCommitted, got: {err}");
}

/// Double rollback returns AlreadyRolledBack.
#[test]
fn test_replica_transaction_double_rollback() {
	let engine = test_multi();
	let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
	tx.set(&as_key!(1), as_values!(1)).unwrap();
	tx.rollback().unwrap();

	let err = tx.rollback().unwrap_err();
	assert!(err.to_string().contains("rolled back"), "expected AlreadyRolledBack, got: {err}");
}

/// Operations after commit return AlreadyCommitted.
#[test]
fn test_replica_transaction_set_after_commit() {
	let engine = test_multi();
	let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
	tx.commit_at_version().unwrap();

	let err = tx.set(&as_key!(1), as_values!(1)).unwrap_err();
	assert!(err.to_string().contains("committed"), "expected AlreadyCommitted, got: {err}");
}

/// Operations after rollback return AlreadyRolledBack.
#[test]
fn test_replica_transaction_set_after_rollback() {
	let engine = test_multi();
	let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
	tx.rollback().unwrap();

	let err = tx.set(&as_key!(1), as_values!(1)).unwrap_err();
	assert!(err.to_string().contains("rolled back"), "expected AlreadyRolledBack, got: {err}");
}

/// get() after commit returns AlreadyCommitted.
#[test]
fn test_replica_transaction_get_after_commit() {
	let engine = test_multi();
	let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
	tx.set(&as_key!(1), as_values!(1)).unwrap();
	tx.commit_at_version().unwrap();

	let err = tx.get(&as_key!(1)).unwrap_err();
	assert!(err.to_string().contains("committed"), "expected AlreadyCommitted, got: {err}");
}

/// Auto-rollback on drop — uncommitted writes not visible.
#[test]
fn test_replica_transaction_drop_auto_rollback() {
	let engine = test_multi();
	{
		let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
		tx.set(&as_key!(1), as_values!(1)).unwrap();
		// dropped without commit or rollback
	}

	let rx = engine.begin_query().unwrap();
	assert!(rx.get(&as_key!(1)).unwrap().is_none());
}

/// Replica unset — delete with tombstone preservation.
#[test]
fn test_replica_unset() {
	let engine = test_multi();

	// Insert at version 100
	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&as_key!(1), as_values!(42)).unwrap();
		tx.commit_at_version().unwrap();
	}

	// Unset at version 200
	{
		let mut tx = engine.begin_replica(CommitVersion(200)).unwrap();
		tx.unset(&as_key!(1), as_values!(42)).unwrap();
		tx.commit_at_version().unwrap();
	}

	// Latest: key not found
	let rx = engine.begin_query().unwrap();
	assert!(rx.get(&as_key!(1)).unwrap().is_none());

	// Time-travel to version 100: key exists
	let mut txn = engine.begin_command().unwrap();
	txn.read_as_of_version_exclusive(CommitVersion(101));
	let sv = txn.get(&as_key!(1)).unwrap().unwrap();
	assert_eq!(from_row!(i32, sv.row()), 42);
}

/// Replica prefix and prefix_rev queries.
#[test]
fn test_replica_prefix() {
	use reifydb_core::encoded::key::EncodedKey;
	use reifydb_type::util::cowvec::CowVec;

	let engine = test_multi();

	// Use raw byte keys with a shared prefix so prefix queries work correctly.
	let k_aa = EncodedKey(CowVec::new(vec![0x01, 0x01]));
	let k_ab = EncodedKey(CowVec::new(vec![0x01, 0x02]));
	let k_ac = EncodedKey(CowVec::new(vec![0x01, 0x03]));
	let k_ba = EncodedKey(CowVec::new(vec![0x02, 0x01]));
	let prefix_01 = EncodedKey(CowVec::new(vec![0x01]));

	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&k_aa, as_values!(11)).unwrap();
		tx.set(&k_ab, as_values!(12)).unwrap();
		tx.set(&k_ac, as_values!(13)).unwrap();
		tx.set(&k_ba, as_values!(21)).unwrap();
		tx.commit_at_version().unwrap();
	}

	let rx = engine.begin_query().unwrap();

	// Prefix 0x01 should return k_aa, k_ab, k_ac (3 items)
	let batch = rx.prefix(&prefix_01).unwrap();
	assert_eq!(batch.items.len(), 3);

	// Prefix_rev 0x01 should return same 3 items in reverse (descending)
	let batch_rev = rx.prefix_rev(&prefix_01).unwrap();
	assert_eq!(batch_rev.items.len(), 3);
	assert_eq!(batch_rev.items[0].key, k_ac);
	assert_eq!(batch_rev.items[2].key, k_aa);
}

/// Verify the version on rows returned by get() matches the primary version.
#[test]
fn test_replica_get_version_field() {
	let engine = test_multi();
	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&as_key!(1), as_values!(1)).unwrap();
		tx.commit_at_version().unwrap();
	}

	let rx = engine.begin_query().unwrap();
	let row = rx.get(&as_key!(1)).unwrap().unwrap();
	assert_eq!(row.version(), CommitVersion(100));
}
