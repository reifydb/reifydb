// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_flow::transaction::{FlowTransaction, state::StateExtension};
use reifydb_test_harness::operator::transaction::{FlowTxn, OPERATOR_ID, engine, key, make_row};

fn assert_update_replaces_the_row_wholesale<T: FlowTransaction>(txn: &mut T) {
	// A repeat state_set must replace the stored row wholesale, or a merging write would leave the earlier body
	// readable.
	let k = key("update-key");

	txn.state_set(OPERATOR_ID, &k, make_row("v1")).unwrap();
	txn.state_set(OPERATOR_ID, &k, make_row("v2")).unwrap();

	let stored = txn.state_get(OPERATOR_ID, &k).unwrap().unwrap();
	assert_eq!(stored.body(), b"v2");
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = e.flow_txn().deferred();
	assert_update_replaces_the_row_wholesale(&mut txn);
}
