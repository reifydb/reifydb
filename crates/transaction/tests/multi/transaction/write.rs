// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::common::CommitVersion;
use reifydb_runtime::context::{RuntimeContext, transaction::TransactionContext};
use reifydb_transaction::multi::types::TransactionValue;

use super::test_multi;
use crate::{as_key, as_values, from_bytes, multi::transaction::FromRow};

fn system_ctx() -> Arc<TransactionContext> {
	Arc::new(TransactionContext::system(RuntimeContext::testing(0, 0)))
}

#[test]
fn test_write() {
	let key = as_key!("foo");

	let engine = test_multi();
	{
		let mut tx = engine.begin_command(engine.generate_transaction_id()).unwrap();
		assert_eq!(tx.version(), 1);

		tx.set(vec![(key.clone(), as_values!("foo1".to_string()))]).unwrap();
		let value: String = from_bytes!(
			String,
			*tx.get(&system_ctx(), &[key.clone()]).unwrap().pop().flatten().unwrap().bytes()
		);
		assert_eq!(value.as_str(), "foo1");
		tx.commit().unwrap();
	}

	{
		let rx = engine.begin_query(engine.generate_transaction_id()).unwrap();
		assert_eq!(rx.version(), 2);
		let value: String = from_bytes!(
			String,
			rx.get(&system_ctx(), &[key.clone()]).unwrap().pop().flatten().unwrap().bytes()
		);
		assert_eq!(value.as_str(), "foo1");
	}
}

#[test]
fn test_multiple_write() {
	let engine = test_multi();

	{
		let mut txn = engine.begin_command(engine.generate_transaction_id()).unwrap();
		for i in 0..10 {
			if let Err(e) = txn.set(vec![(as_key!(i), as_values!(i))]) {
				panic!("{e}");
			}
		}

		let key = as_key!(8);
		let sv = txn.get(&system_ctx(), &[key.clone()]).unwrap().pop().flatten().unwrap();
		assert!(
			matches!(sv, TransactionValue::Pending(_)),
			"a read inside the open transaction must come from its own pending write"
		);
		assert_eq!(from_bytes!(i32, *sv.bytes()), 8);
		drop(sv);

		assert!(txn.contains(&system_ctx(), &[as_key!(8)]).unwrap()[0]);

		txn.commit().unwrap();
	}

	let k = 8;
	let v = 8;
	let txn = engine.begin_query(engine.generate_transaction_id()).unwrap();
	assert!(txn.contains(&system_ctx(), &[as_key!(k)]).unwrap()[0]);
	let sv = txn.get(&system_ctx(), &[as_key!(k)]).unwrap().pop().flatten().unwrap();
	assert_eq!(from_bytes!(i32, *sv.bytes()), v);
}

#[test]
fn commit_self_lease_keeps_own_version_leasable_after_cutoff_advances() {
	// A post-commit interceptor leases ctx.version, so the committing transaction must pin its own
	// version until it drops; otherwise an advancing GC cutoff evicts it mid-post-commit.
	let engine = test_multi();

	let mut txn = engine.begin_command(engine.generate_transaction_id()).unwrap();
	txn.set(vec![(as_key!("k"), as_values!("v".to_string()))]).unwrap();
	let version = txn.commit().unwrap();

	// The GC cutoff advances past our own commit version while its post-commit phase is still open.
	let mut bump = engine.begin_command(engine.generate_transaction_id()).unwrap();
	bump.set(vec![(as_key!("bump"), as_values!("b".to_string()))]).unwrap();
	bump.commit().unwrap();
	drop(bump);
	assert!(
		engine.query_done_until().0 > version.0,
		"precondition: query done until must be advanced past the commit version to exercise the eviction path"
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
	// An ephemeral consumer holds no done-until mark, so a lease carried from one batch into the next is
	// its only protection; without the chain it reads reclaimed history instead of failing loudly.
	let engine = test_multi();

	// Batch N leases its floor version while the query done until is still below it.
	let carry_at = CommitVersion(engine.query_done_until().0 + 4);
	let carry = engine
		.acquire_version_lease(carry_at)
		.expect("leasing at the current head must succeed before done until advances");

	// The query done until then passes both the carried version and the next batch's versions.
	while engine.query_done_until().0 < carry_at.0 + 6 {
		let mut bump = engine.begin_command(engine.generate_transaction_id()).unwrap();
		bump.set(vec![(as_key!("bump"), as_values!("b".to_string()))]).unwrap();
		bump.commit().unwrap();
	}
	assert!(
		engine.query_done_until().0 >= carry_at.0 + 6,
		"precondition: the query done until must be advanced past the carried lease"
	);

	// Batch N+1 must lease before the carry is released; the held carry keeps the cutoff at the carried version.
	let next = engine
		.acquire_version_lease(CommitVersion(carry_at.0 + 3))
		.expect("a version above a still-held carried lease must remain leasable");
	assert_eq!(next.version(), CommitVersion(carry_at.0 + 3));
	drop(carry);

	// The floor is now the new lease, not zero: the carry must not have opened all history.
	let err = engine
		.acquire_version_lease(CommitVersion(carry_at.0 + 2))
		.expect_err("a version below the carried lease must not be leasable");
	assert_eq!(err.0.code, "TXN_012");

	// Breaking the chain snaps the floor back to the query done until.
	drop(next);
	let err = engine
		.acquire_version_lease(CommitVersion(carry_at.0 + 3))
		.expect_err("with no carried lease, a version below the query done until must fail loudly");
	assert_eq!(err.0.code, "TXN_012");
}
