// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::CommitVersion;

use super::test_multi;
use crate::{as_key, as_values, from_row, multi::transaction::FromRow};

#[test]
fn test_write() {
	let key = as_key!("foo");

	let engine = test_multi();
	{
		let mut tx = engine.begin_command().unwrap();
		assert_eq!(tx.version(), 1);

		tx.set(&key, as_values!("foo1".to_string())).unwrap();
		let value: String = from_row!(String, *tx.get(&key).unwrap().unwrap().row());
		assert_eq!(value.as_str(), "foo1");
		tx.commit(vec![]).unwrap();
	}

	{
		let rx = engine.begin_query().unwrap();
		assert_eq!(rx.version(), 2);
		let value: String = from_row!(String, rx.get(&key).unwrap().unwrap().row());
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
		assert_eq!(from_row!(i32, *sv.row()), 8);
		drop(sv);

		assert!(txn.contains_key(&as_key!(8)).unwrap());

		txn.commit(vec![]).unwrap();
	}

	let k = 8;
	let v = 8;
	let txn = engine.begin_query().unwrap();
	assert!(txn.contains_key(&as_key!(k)).unwrap());
	let sv = txn.get(&as_key!(k)).unwrap().unwrap();
	assert_eq!(from_row!(i32, *sv.row()), v);
}

// A committing transaction must be able to read/lease its OWN just-committed version for the entire
// duration of its post-commit phase, even if the historical-GC cutoff (query.done_until) advances past
// that version in the meantime. This is the invariant behind the deferred-view-create TXN_012 regression:
// the post-commit interceptor calls acquire_version_lease(ctx.version) and was rejected as "evicted"
// because nothing pinned the just-committed version. commit() now holds a self-lease on its own version
// (released when the transaction drops, i.e. after the post-commit phase), so the lease is always grantable.
#[test]
fn commit_self_lease_keeps_own_version_leasable_after_cutoff_advances() {
	let engine = test_multi();

	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!("k"), as_values!("v".to_string())).unwrap();
	let version = txn.commit(vec![]).unwrap();

	// Simulate the GC cutoff advancing past our own commit version while its post-commit phase runs.
	engine.advance_version_to(CommitVersion(version.0 + 1));
	assert!(
		engine.query_done_until().0 >= version.0 + 1,
		"precondition: query watermark must be advanced past the commit version to exercise the eviction path"
	);

	// `txn` is still alive and therefore still holds its self-lease on `version`, so the version stays
	// leasable even though the cutoff is now above it - exactly what the post-commit interceptor needs.
	engine.acquire_version_lease(version)
		.expect("committed version must remain leasable during its own post-commit phase");

	// Dropping the transaction releases the self-lease; the advanced cutoff now evicts the version. This
	// proves the self-lease - not some other pin - is what keeps the version alive across post-commit.
	drop(txn);
	let err =
		engine.acquire_version_lease(version).expect_err("version must be evicted once self-lease is released");
	assert_eq!(err.0.code, "TXN_012");
}

// An ephemeral consumer (a subscription worker) never lowers the lease floor through a watermark
// anymore: its only protection is the lease it carries from one batch into the next. Acquiring the
// next batch's lease while still holding the previous one chains the floor forward; a worker that
// arrives with no carried lease and asks for a version below the query watermark gets TXN_012,
// which is the loud overtaken signal that triggers a resync instead of a silent read of reclaimed
// history.
#[test]
fn a_carried_lease_chains_the_floor_across_batches() {
	let engine = test_multi();

	// A worker processing batch N leases the batch's floor version while the query watermark is
	// still below it.
	let carry = engine
		.acquire_version_lease(CommitVersion(14090))
		.expect("leasing at the current head must succeed before the watermark advances");

	// The system moves on: the query watermark passes both the carried version and the next
	// batch's versions.
	engine.advance_version_to(CommitVersion(14096));
	assert!(
		engine.query_done_until().0 >= 14096,
		"precondition: the query watermark must be advanced past the carried lease"
	);

	// Batch N+1 leases its own floor BEFORE the carry is released: the held carry keeps the
	// cutoff at 14090, so the acquire succeeds despite the higher query watermark.
	let next = engine
		.acquire_version_lease(CommitVersion(14093))
		.expect("a version above a still-held carried lease must remain leasable");
	assert_eq!(next.version(), CommitVersion(14093));
	drop(carry);

	// With the chain intact the floor is now the new lease, not zero: a version below it is
	// rejected. This guards against the carry accidentally opening all history.
	let err = engine
		.acquire_version_lease(CommitVersion(14092))
		.expect_err("a version below the carried lease must not be leasable");
	assert_eq!(err.0.code, "TXN_012");

	// Dropping the last lease breaks the chain: the floor snaps back to the query watermark and
	// the version that was just leasable is now the overtaken signal.
	drop(next);
	let err = engine
		.acquire_version_lease(CommitVersion(14093))
		.expect_err("with no carried lease, a version below the query watermark must fail loudly");
	assert_eq!(err.0.code, "TXN_012");
}
