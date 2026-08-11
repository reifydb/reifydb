// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_flow::transaction::DepFlowTransaction;
use reifydb_test_harness::operator::transaction::{FlowTxn, OPERATOR_ID, engine, key, make_row};
use reifydb_value::value::datetime::DateTime;

fn assert_epoch_prior_time_is_not_pinned(txn: &mut DepFlowTransaction) {
	// Writes carry the time their caller stamped and never inherit from the row they replace, so a
	// row stamped at the epoch heals on its next write instead of pinning the stamp forever.
	let k = key("legacy-key");
	txn.state_set(OPERATOR_ID, &k, make_row("v0", 0)).unwrap();
	txn.state_set(OPERATOR_ID, &k, make_row("v1", 7_000)).unwrap();

	let stored = txn.state_get(OPERATOR_ID, &k).unwrap().unwrap();
	assert_eq!(stored.time(), DateTime::from_nanos(7_000), "an epoch prior stamp must not pin future writes");
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = e.flow_txn().deferred();
	assert_epoch_prior_time_is_not_pinned(&mut txn);
}

#[test]
fn ephemeral() {
	let e = engine();
	let mut txn = e.flow_txn().ephemeral();
	assert_epoch_prior_time_is_not_pinned(&mut txn);
}
