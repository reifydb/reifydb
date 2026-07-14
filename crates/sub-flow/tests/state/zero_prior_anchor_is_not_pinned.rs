// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Scenario: a prior row with a zero `created_at` (uninitialised header, legacy
// state, or a row written with both timestamps cleared) must not leak its zero
// anchor into the next write. Writes carry the anchors their caller stamped and
// never inherit anything from the row they replace, so a legacy row heals on its
// next write instead of pinning the anchor at zero forever.

use reifydb_sub_flow::transaction::FlowTransaction;

use super::fixtures::{NODE_ID, deferred_txn, engine, ephemeral_txn, key, make_row, transactional_txn};

fn assert_zero_prior_anchor_is_not_pinned(txn: &mut FlowTransaction) {
	let k = key("legacy-key");
	txn.state_set(NODE_ID, &k, make_row("v0", 0, 0)).unwrap();
	txn.state_set(NODE_ID, &k, make_row("v1", 7_000, 7_000)).unwrap();

	let stored = txn.state_get(NODE_ID, &k).unwrap().unwrap();
	assert_eq!(stored.created_at_nanos(), 7_000, "zero prior anchor must not pin future writes");
	assert_eq!(stored.updated_at_nanos(), 7_000);
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = deferred_txn(&e);
	assert_zero_prior_anchor_is_not_pinned(&mut txn);
}

#[test]
fn transactional() {
	let e = engine();
	let mut txn = transactional_txn(&e);
	assert_zero_prior_anchor_is_not_pinned(&mut txn);
}

#[test]
fn ephemeral() {
	let e = engine();
	let mut txn = ephemeral_txn(&e);
	assert_zero_prior_anchor_is_not_pinned(&mut txn);
}
