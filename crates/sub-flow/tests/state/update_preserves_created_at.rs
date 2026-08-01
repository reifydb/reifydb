// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_flow::transaction::FlowTransaction;
use reifydb_test_harness::operator::transaction::{FlowTxn, NODE_ID, engine, key, make_row, payload};
use reifydb_value::value::datetime::DateTime;

fn assert_update_uses_caller_anchors(txn: &mut FlowTransaction) {
	// Nothing reads an operator state row's header anchors, so the host must not read the prior row
	// back to carry `created_at` forward: that costs a store roundtrip per written key on every
	// flush and defeats the caches above it, whose purpose is keeping a warm key out of the txn.
	let k = key("update-key");

	txn.state_set(NODE_ID, &k, make_row("v1", 1_000, 1_000)).unwrap();
	txn.state_set(NODE_ID, &k, make_row("v2", 5_000, 5_000)).unwrap();

	let stored = txn.state_get(NODE_ID, &k).unwrap().unwrap();
	assert_eq!(
		stored.created_at(),
		DateTime::from_nanos(5_000),
		"the write's own created_at stands, unread and unmodified"
	);
	assert_eq!(stored.updated_at(), DateTime::from_nanos(5_000), "updated_at is whatever the writer stamped");
	// The second write replaced the row wholesale, payload included.
	assert_eq!(payload(&stored), b"v2");
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = e.flow_txn().deferred();
	assert_update_uses_caller_anchors(&mut txn);
}

#[test]
fn transactional() {
	let e = engine();
	let mut txn = e.flow_txn().transactional();
	assert_update_uses_caller_anchors(&mut txn);
}

#[test]
fn ephemeral() {
	let e = engine();
	let mut txn = e.flow_txn().ephemeral();
	assert_update_uses_caller_anchors(&mut txn);
}
