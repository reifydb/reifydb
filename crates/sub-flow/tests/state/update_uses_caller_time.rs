// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_flow::transaction::{FlowTransaction, state::StateExtension};
use reifydb_test_harness::operator::transaction::{FlowTxn, OPERATOR_ID, engine, key, make_row};
use reifydb_value::value::datetime::DateTime;

fn assert_update_uses_caller_time<T: FlowTransaction>(txn: &mut T) {
	// The host must not read the prior row back to carry a stamp forward: that costs a store
	// roundtrip per written key on every flush and defeats the caches above it.
	let k = key("update-key");

	txn.state_set(OPERATOR_ID, &k, make_row("v1", 1_000)).unwrap();
	txn.state_set(OPERATOR_ID, &k, make_row("v2", 5_000)).unwrap();

	let stored = txn.state_get(OPERATOR_ID, &k).unwrap().unwrap();
	assert_eq!(stored.time(), DateTime::from_nanos(5_000), "the write's own time stands, unread and unmodified");
	// The second write replaced the row wholesale, body included.
	assert_eq!(stored.body(), b"v2");
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = e.flow_txn().deferred();
	assert_update_uses_caller_time(&mut txn);
}
