// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKeyRange;
use reifydb_core::common::CommitVersion;
use reifydb_transaction::{multi::RangeScope, transaction::replica::ReplicaTransaction};

use super::test_multi;
use crate::{as_key, as_values, from_row, multi::transaction::FromRow};

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

#[test]
fn test_replica_multiple_write() {
	let engine = test_multi();
	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		for i in 0..10 {
			tx.set(&as_key!(i), as_values!(i)).unwrap();
		}

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

#[test]
fn test_replica_versions() {
	// A replica commit adopts the primary's version verbatim, so time travel must land on those
	// exact primary versions rather than any locally assigned sequence.
	let engine = test_multi();
	let k0 = as_key!(0);

	for i in 1i32..10 {
		let version = CommitVersion(i as u64 * 100);
		let mut tx = engine.begin_replica(version).unwrap();
		tx.set(&k0, as_values!(i)).unwrap();
		tx.commit_at_version().unwrap();
		assert_eq!(engine.version().unwrap(), version);
	}

	for idx in 1i32..10 {
		let read_version = CommitVersion(idx as u64 * 100 + 1); // exclusive: read at version+1 sees version
		let mut txn = engine.begin_command().unwrap();
		txn.read_as_of_version_exclusive(read_version);

		let tv = txn.get(&k0).unwrap().unwrap();
		assert_eq!(idx, from_row!(i32, tv.row()));
	}

	let rx = engine.begin_query().unwrap();
	let sv = rx.get(&k0).unwrap().unwrap();
	assert_eq!(9, from_row!(i32, sv.row()));
}

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
	let items: Vec<_> =
		rx.range(four_to_one.clone(), RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=3).rev().zip(items) {
		assert_eq!(v.key, as_key!(expected));
		assert_eq!(v.row, as_values!(expected));
		assert_eq!(v.version, CommitVersion(100));
	}

	let items: Vec<_> = rx.range_rev(four_to_one, RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	for (expected, v) in (1..=3).zip(items) {
		assert_eq!(v.key, as_key!(expected));
		assert_eq!(v.row, as_values!(expected));
		assert_eq!(v.version, CommitVersion(100));
	}
}

#[test]
fn test_replica_range_multiple_commits() {
	let engine = test_multi();

	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&as_key!(1), as_values!(1)).unwrap();
		tx.set(&as_key!(2), as_values!(2)).unwrap();
		tx.set(&as_key!(3), as_values!(3)).unwrap();
		tx.commit_at_version().unwrap();
	}

	{
		let mut tx = engine.begin_replica(CommitVersion(200)).unwrap();
		tx.set(&as_key!(4), as_values!(4)).unwrap();
		tx.set(&as_key!(5), as_values!(5)).unwrap();
		tx.set(&as_key!(6), as_values!(6)).unwrap();
		tx.commit_at_version().unwrap();
	}

	let seven_to_one = EncodedKeyRange::start_end(Some(as_key!(7)), Some(as_key!(1)));

	let rx = engine.begin_query().unwrap();
	let items: Vec<_> = rx.range(seven_to_one, RangeScope::All, 1024).collect::<Result<Vec<_>, _>>().unwrap();
	assert_eq!(items.len(), 6);
	for (expected, v) in (1..=6).rev().zip(items) {
		assert_eq!(v.key, as_key!(expected));
		assert_eq!(v.row, as_values!(expected));
	}
}

#[test]
fn test_replica_rollback_same_tx() {
	let engine = test_multi();
	let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
	tx.set(&as_key!(1), as_values!(1)).unwrap();
	tx.rollback().unwrap();
	assert!(tx.get(&as_key!(1)).unwrap().is_none());
}

#[test]
fn test_replica_rollback_different_tx() {
	let engine = test_multi();
	let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
	tx.set(&as_key!(1), as_values!(1)).unwrap();
	tx.rollback().unwrap();

	let rx = engine.begin_query().unwrap();
	assert!(rx.get(&as_key!(1)).unwrap().is_none());
}

#[test]
fn test_replica_empty_commit() {
	let engine = test_multi();
	let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
	tx.commit_at_version().unwrap();
}

#[test]
fn test_advance_version_for_replica() {
	// The primary can commit versions this replica never materializes, so the version clock has to
	// skip forward without a commit and still accept the next replicated version.
	let engine = test_multi();

	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&as_key!("a"), as_values!("v1".to_string())).unwrap();
		tx.commit_at_version().unwrap();
	}

	engine.advance_version_for_replica(CommitVersion(200));

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

#[test]
fn test_replica_read_your_writes() {
	let engine = test_multi();
	let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();

	tx.set(&as_key!("a"), as_values!("val_a".to_string())).unwrap();
	let sv = tx.get(&as_key!("a")).unwrap().unwrap();
	assert_eq!(from_row!(String, *sv.row()), "val_a");
	drop(sv);

	tx.set(&as_key!("b"), as_values!("val_b".to_string())).unwrap();
	assert!(tx.contains_key(&as_key!("a")).unwrap());
	assert!(tx.contains_key(&as_key!("b")).unwrap());

	tx.commit_at_version().unwrap();
}

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

	let rx = engine.begin_query().unwrap();
	assert_eq!(from_row!(i32, rx.get(&k).unwrap().unwrap().row()), 3);

	// Exclusive read at 21 must land on the version-20 write, not the later one.
	let mut txn = engine.begin_command().unwrap();
	txn.read_as_of_version_exclusive(CommitVersion(21));
	assert_eq!(from_row!(i32, txn.get(&k).unwrap().unwrap().row()), 2);
}

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

	let rx = engine.begin_query().unwrap();
	assert_eq!(from_row!(String, rx.get(&k).unwrap().unwrap().row()), "v2");

	let mut txn = engine.begin_command().unwrap();
	txn.read_as_of_version_exclusive(CommitVersion(101));
	assert_eq!(from_row!(String, txn.get(&k).unwrap().unwrap().row()), "v1");
}

#[test]
fn test_replica_remove() {
	// The tombstone must not erase history: a read before the removal version still sees the row.
	let engine = test_multi();
	let k = as_key!(42);

	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&k, as_values!(42)).unwrap();
		tx.commit_at_version().unwrap();
	}

	{
		let mut tx = engine.begin_replica(CommitVersion(200)).unwrap();
		tx.remove(&k).unwrap();
		tx.commit_at_version().unwrap();
	}

	let rx = engine.begin_query().unwrap();
	assert!(rx.get(&k).unwrap().is_none());

	let mut txn = engine.begin_command().unwrap();
	txn.read_as_of_version_exclusive(CommitVersion(101));
	let sv = txn.get(&k).unwrap().unwrap();
	assert_eq!(from_row!(i32, sv.row()), 42);
}

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

#[test]
fn test_replica_transaction_double_commit() {
	let engine = test_multi();
	let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
	tx.set(&as_key!(1), as_values!(1)).unwrap();
	tx.commit_at_version().unwrap();

	let err = tx.commit_at_version().unwrap_err();
	assert!(err.to_string().contains("committed"), "expected AlreadyCommitted, got: {err}");
}

#[test]
fn test_replica_transaction_double_rollback() {
	let engine = test_multi();
	let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
	tx.set(&as_key!(1), as_values!(1)).unwrap();
	tx.rollback().unwrap();

	let err = tx.rollback().unwrap_err();
	assert!(err.to_string().contains("rolled back"), "expected AlreadyRolledBack, got: {err}");
}

#[test]
fn test_replica_transaction_set_after_commit() {
	let engine = test_multi();
	let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
	tx.commit_at_version().unwrap();

	let err = tx.set(&as_key!(1), as_values!(1)).unwrap_err();
	assert!(err.to_string().contains("committed"), "expected AlreadyCommitted, got: {err}");
}

#[test]
fn test_replica_transaction_set_after_rollback() {
	let engine = test_multi();
	let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
	tx.rollback().unwrap();

	let err = tx.set(&as_key!(1), as_values!(1)).unwrap_err();
	assert!(err.to_string().contains("rolled back"), "expected AlreadyRolledBack, got: {err}");
}

#[test]
fn test_replica_transaction_get_after_commit() {
	let engine = test_multi();
	let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
	tx.set(&as_key!(1), as_values!(1)).unwrap();
	tx.commit_at_version().unwrap();

	let err = tx.get(&as_key!(1)).unwrap_err();
	assert!(err.to_string().contains("committed"), "expected AlreadyCommitted, got: {err}");
}

#[test]
fn test_replica_transaction_drop_auto_rollback() {
	let engine = test_multi();
	{
		let mut tx = ReplicaTransaction::new(engine.clone(), CommitVersion(100)).unwrap();
		tx.set(&as_key!(1), as_values!(1)).unwrap();
		// Dropped without commit or rollback.
	}

	let rx = engine.begin_query().unwrap();
	assert!(rx.get(&as_key!(1)).unwrap().is_none());
}

#[test]
fn test_replica_unset() {
	// An announced removal carries a pre-image, but must still leave the pre-removal version readable.
	let engine = test_multi();

	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&as_key!(1), as_values!(42)).unwrap();
		tx.commit_at_version().unwrap();
	}

	{
		let mut tx = engine.begin_replica(CommitVersion(200)).unwrap();
		tx.remove_with_pre(&as_key!(1), as_values!(42)).unwrap();
		tx.commit_at_version().unwrap();
	}

	let rx = engine.begin_query().unwrap();
	assert!(rx.get(&as_key!(1)).unwrap().is_none());

	let mut txn = engine.begin_command().unwrap();
	txn.read_as_of_version_exclusive(CommitVersion(101));
	let sv = txn.get(&as_key!(1)).unwrap().unwrap();
	assert_eq!(from_row!(i32, sv.row()), 42);
}

#[test]
fn test_replica_prefix() {
	use reifydb_codec::key::encoded::EncodedKey;

	let engine = test_multi();

	// Raw byte keys, because the as_key! encoding does not share a leading byte prefix.
	let k_aa = EncodedKey::new(vec![0x01, 0x01]);
	let k_ab = EncodedKey::new(vec![0x01, 0x02]);
	let k_ac = EncodedKey::new(vec![0x01, 0x03]);
	let k_ba = EncodedKey::new(vec![0x02, 0x01]);
	let prefix_01 = EncodedKey::new(vec![0x01]);

	{
		let mut tx = engine.begin_replica(CommitVersion(100)).unwrap();
		tx.set(&k_aa, as_values!(11)).unwrap();
		tx.set(&k_ab, as_values!(12)).unwrap();
		tx.set(&k_ac, as_values!(13)).unwrap();
		tx.set(&k_ba, as_values!(21)).unwrap();
		tx.commit_at_version().unwrap();
	}

	let rx = engine.begin_query().unwrap();

	// k_ba shares no prefix byte, so it must be excluded from both directions.
	let batch = rx.prefix(&prefix_01).unwrap();
	assert_eq!(batch.items.len(), 3);

	let batch_rev = rx.prefix_rev(&prefix_01).unwrap();
	assert_eq!(batch_rev.items.len(), 3);
	assert_eq!(batch_rev.items[0].key, k_ac);
	assert_eq!(batch_rev.items[2].key, k_aa);
}

#[test]
fn test_replica_get_version_field() {
	// Rows must carry the primary's version, not a locally assigned one, or replicas disagree.
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
