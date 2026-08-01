// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_flow::transaction::FlowTransaction;
use reifydb_test_harness::operator::transaction::{FlowTxn, OPERATOR_ID, engine, key, make_row};
use reifydb_value::value::datetime::DateTime;

fn assert_zero_prior_anchor_is_not_pinned(txn: &mut FlowTransaction) {
	// Writes carry the anchors their caller stamped and never inherit from the row they replace, so
	// a row with a zero `created_at` heals on its next write instead of pinning the anchor forever.
	let k = key("legacy-key");
	txn.state_set(OPERATOR_ID, &k, make_row("v0", 0, 0)).unwrap();
	txn.state_set(OPERATOR_ID, &k, make_row("v1", 7_000, 7_000)).unwrap();

	let stored = txn.state_get(OPERATOR_ID, &k).unwrap().unwrap();
	assert_eq!(stored.created_at(), DateTime::from_nanos(7_000), "zero prior anchor must not pin future writes");
	assert_eq!(stored.updated_at(), DateTime::from_nanos(7_000));
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = e.flow_txn().deferred();
	assert_zero_prior_anchor_is_not_pinned(&mut txn);
}

#[test]
fn transactional() {
	let e = engine();
	let mut txn = e.flow_txn().transactional();
	assert_zero_prior_anchor_is_not_pinned(&mut txn);
}

#[test]
fn ephemeral() {
	let e = engine();
	let mut txn = e.flow_txn().ephemeral();
	assert_zero_prior_anchor_is_not_pinned(&mut txn);
}
