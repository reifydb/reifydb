// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::CommitVersion;

use super::test_multi;
use crate::{as_key, as_values, from_bytes, multi::transaction::FromRow};

#[test]
fn test_write() {
	let key = as_key!("foo");

	let engine = test_multi();
	{
		let mut tx = engine.begin_command().unwrap();
		assert_eq!(tx.version(), 1);

		tx.set(&key, as_values!("foo1".to_string())).unwrap();
		let value: String = from_bytes!(String, *tx.get(&key).unwrap().unwrap().bytes());
		assert_eq!(value.as_str(), "foo1");
		tx.commit(vec![]).unwrap();
	}

	{
		let rx = engine.begin_query().unwrap();
		assert_eq!(rx.version(), 2);
		let value: String = from_bytes!(String, rx.get(&key).unwrap().unwrap().bytes());
		assert_eq!(value.as_str(), "foo1");
	}
}

#[test]
fn test_multiple_write() {
	let engine = test_multi();

	{
		let mut txn = engine.begin_command().unwrap();
		for i in 0..10 {
			if let Err(e) = txn.set(&as_key!(i), as_values!(i)) {
				panic!("{e}");
			}
		}

		let key = as_key!(8);
		let sv = txn.get(&key).unwrap().unwrap();
		assert!(!sv.is_committed());
		assert_eq!(from_bytes!(i32, *sv.bytes()), 8);
		drop(sv);

		assert!(txn.contains_key(&as_key!(8)).unwrap());

		txn.commit(vec![]).unwrap();
	}

	let k = 8;
	let v = 8;
	let txn = engine.begin_query().unwrap();
	assert!(txn.contains_key(&as_key!(k)).unwrap());
	let sv = txn.get(&as_key!(k)).unwrap().unwrap();
	assert_eq!(from_bytes!(i32, *sv.bytes()), v);
}

#[test]
fn commit_self_lease_keeps_own_version_leasable_after_cutoff_advances() {
	// A post-commit interceptor leases ctx.version, so the committing transaction must pin its own
	// version until it drops; otherwise an advancing GC cutoff evicts it mid-post-commit.
	let engine = test_multi();

	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!("k"), as_values!("v".to_string())).unwrap();
	let version = txn.commit(vec![]).unwrap();

	// The GC cutoff advances past our own commit version while its post-commit phase is still open.
	engine.advance_version_to(CommitVersion(version.0 + 1));
	assert!(
		engine.query_done_until().0 >= version.0 + 1,
		"precondition: query watermark must be advanced past the commit version to exercise the eviction path"
	);

	engine.acquire_version_lease(version)
		.expect("committed version must remain leasable during its own post-commit phase");

	// Dropping releases the self-lease, proving that lease and not some other pin held the version.
	drop(txn);
	let err =
		engine.acquire_version_lease(version).expect_err("version must be evicted once self-lease is released");
	assert_eq!(err.0.code, "TXN_012");
}

#[test]
fn a_carried_lease_chains_the_floor_across_batches() {
	// An ephemeral consumer holds no watermark, so a lease carried from one batch into the next is
	// its only protection; without the chain it reads reclaimed history instead of failing loudly.
	let engine = test_multi();

	// Batch N leases its floor version while the query watermark is still below it.
	let carry = engine
		.acquire_version_lease(CommitVersion(14090))
		.expect("leasing at the current head must succeed before the watermark advances");

	// The query watermark then passes both the carried version and the next batch's versions.
	engine.advance_version_to(CommitVersion(14096));
	assert!(
		engine.query_done_until().0 >= 14096,
		"precondition: the query watermark must be advanced past the carried lease"
	);

	// Batch N+1 must lease before the carry is released; the held carry keeps the cutoff at 14090.
	let next = engine
		.acquire_version_lease(CommitVersion(14093))
		.expect("a version above a still-held carried lease must remain leasable");
	assert_eq!(next.version(), CommitVersion(14093));
	drop(carry);

	// The floor is now the new lease, not zero: the carry must not have opened all history.
	let err = engine
		.acquire_version_lease(CommitVersion(14092))
		.expect_err("a version below the carried lease must not be leasable");
	assert_eq!(err.0.code, "TXN_012");

	// Breaking the chain snaps the floor back to the query watermark.
	drop(next);
	let err = engine
		.acquire_version_lease(CommitVersion(14093))
		.expect_err("with no carried lease, a version below the query watermark must fail loudly");
	assert_eq!(err.0.code, "TXN_012");
}
