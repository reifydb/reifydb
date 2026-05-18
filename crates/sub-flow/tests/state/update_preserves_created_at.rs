// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Scenario: when a key already has a row in operator state, a follow-up
// `state_set` must keep the original `created_at` and only advance
// `updated_at`. Simulates the buggy operator pattern where the caller stamps
// (now, now) on every emission. The host-side fix in
// `FlowTransaction::state_set` reads the prior row and overlays its
// `created_at` onto the incoming value.

use reifydb_sub_flow::transaction::FlowTransaction;

use super::fixtures::{NODE_ID, deferred_txn, engine, ephemeral_txn, key, make_row, payload, transactional_txn};

fn assert_update_preserves_created_at(txn: &mut FlowTransaction) {
	let k = key("update-key");

	txn.state_set(NODE_ID, &k, make_row("v1", 1_000, 1_000)).unwrap();
	txn.state_set(NODE_ID, &k, make_row("v2", 5_000, 5_000)).unwrap();

	let stored = txn.state_get(NODE_ID, &k).unwrap().unwrap();
	assert_eq!(stored.created_at_nanos(), 1_000, "created_at must be preserved across updates");
	assert_eq!(stored.updated_at_nanos(), 5_000, "updated_at must advance on every write");
	// Sanity: the rest of the row was overwritten by the second write, so
	// preservation is scoped to the timestamp bytes only.
	assert_eq!(payload(&stored), b"v2");
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = deferred_txn(&e);
	assert_update_preserves_created_at(&mut txn);
}

#[test]
fn transactional() {
	let e = engine();
	let mut txn = transactional_txn(&e);
	assert_update_preserves_created_at(&mut txn);
}

#[test]
fn ephemeral() {
	let e = engine();
	let mut txn = ephemeral_txn(&e);
	assert_update_preserves_created_at(&mut txn);
}
