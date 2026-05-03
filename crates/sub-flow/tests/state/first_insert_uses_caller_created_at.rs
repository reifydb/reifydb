// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Scenario: on the very first `state_set` for a key there is no prior row,
// so `created_at` and `updated_at` from the caller must round-trip unchanged.
// This guards against a fix that "always" overwrites the anchor and would
// otherwise zero out fresh inserts.

use reifydb_sub_flow::transaction::FlowTransaction;

use super::fixtures::{NODE_ID, deferred_txn, engine, ephemeral_txn, key, make_row, transactional_txn};

fn assert_first_insert_uses_caller_created_at(txn: &mut FlowTransaction) {
	let k = key("fresh-key");
	txn.state_set(NODE_ID, &k, make_row("v1", 4_242, 4_242)).unwrap();

	let stored = txn.state_get(NODE_ID, &k).unwrap().unwrap();
	assert_eq!(stored.created_at_nanos(), 4_242);
	assert_eq!(stored.updated_at_nanos(), 4_242);
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = deferred_txn(&e);
	assert_first_insert_uses_caller_created_at(&mut txn);
}

#[test]
fn transactional() {
	let e = engine();
	let mut txn = transactional_txn(&e);
	assert_first_insert_uses_caller_created_at(&mut txn);
}

#[test]
fn ephemeral() {
	let e = engine();
	let mut txn = ephemeral_txn(&e);
	assert_first_insert_uses_caller_created_at(&mut txn);
}
