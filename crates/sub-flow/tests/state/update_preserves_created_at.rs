// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Scenario: a follow-up `state_set` on a key that already has a row keeps the
// anchors the caller stamped. Operator state rows are engine-internal - nothing
// reads their header anchors - so the host must not read the prior row back to
// carry its `created_at` forward: that read cost one store roundtrip per written
// key on every accumulator flush, and it defeats the operator-resident caches
// above it, whose whole purpose is to keep a warm key out of the transaction.

use reifydb_sub_flow::transaction::FlowTransaction;

use super::fixtures::{NODE_ID, deferred_txn, engine, ephemeral_txn, key, make_row, payload, transactional_txn};

fn assert_update_uses_caller_anchors(txn: &mut FlowTransaction) {
	let k = key("update-key");

	txn.state_set(NODE_ID, &k, make_row("v1", 1_000, 1_000)).unwrap();
	txn.state_set(NODE_ID, &k, make_row("v2", 5_000, 5_000)).unwrap();

	let stored = txn.state_get(NODE_ID, &k).unwrap().unwrap();
	assert_eq!(stored.created_at_nanos(), 5_000, "the write's own created_at stands, unread and unmodified");
	assert_eq!(stored.updated_at_nanos(), 5_000, "updated_at is whatever the writer stamped");
	// Sanity: the second write replaced the row wholesale, payload included.
	assert_eq!(payload(&stored), b"v2");
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = deferred_txn(&e);
	assert_update_uses_caller_anchors(&mut txn);
}

#[test]
fn transactional() {
	let e = engine();
	let mut txn = transactional_txn(&e);
	assert_update_uses_caller_anchors(&mut txn);
}

#[test]
fn ephemeral() {
	let e = engine();
	let mut txn = ephemeral_txn(&e);
	assert_update_uses_caller_anchors(&mut txn);
}
