// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_flow::transaction::FlowTransaction;
use reifydb_test_harness::operator::transaction::{FlowTxn, OPERATOR_ID, engine, key, make_row};
use reifydb_value::value::datetime::DateTime;

fn assert_first_insert_uses_caller_created_at(txn: &mut FlowTransaction) {
	// With no prior row the caller's anchors must round-trip unchanged, so an implementation that
	// always overwrites the anchor cannot zero out fresh inserts.
	let k = key("fresh-key");
	txn.state_set(OPERATOR_ID, &k, make_row("v1", 4_242, 4_242)).unwrap();

	let stored = txn.state_get(OPERATOR_ID, &k).unwrap().unwrap();
	assert_eq!(stored.created_at(), DateTime::from_nanos(4_242));
	assert_eq!(stored.updated_at(), DateTime::from_nanos(4_242));
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = e.flow_txn().deferred();
	assert_first_insert_uses_caller_created_at(&mut txn);
}

#[test]
fn ephemeral() {
	let e = engine();
	let mut txn = e.flow_txn().ephemeral();
	assert_first_insert_uses_caller_created_at(&mut txn);
}
