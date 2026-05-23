// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Regression guard for review #4 (flush_state cadence), re-asserted across all
// three FlowTransaction variants. The native backend must DEFER flush_state to
// commit (flush_operator_states), not flush inside apply. FlushProbe writes its
// state only in flush_state, so after apply the state must NOT be visible and only
// after flush_operator_states must it be persisted - in every variant.

use reifydb_core::{
	common::CommitVersion,
	interface::change::{Change, Diffs},
};
use reifydb_sdk::operator::OperatorMetadata;
use reifydb_sub_flow::{
	operator::{Operator, native::NativeOperatorAdapter},
	transaction::FlowTransaction,
};
use reifydb_type::value::datetime::DateTime;

use super::fixtures::{NODE_ID, deferred_txn, engine, ephemeral_txn, transactional_txn};
use crate::common::{FlushProbe, flush_probe_key};

fn assert_flush_is_deferred(txn: &mut FlowTransaction) {
	let op = NativeOperatorAdapter::new(FlushProbe, NODE_ID, <FlushProbe as OperatorMetadata>::CAPABILITIES);
	let change = Change::from_flow(NODE_ID, CommitVersion(1), Diffs::new(), DateTime::from_nanos(0));

	op.apply(txn, change).unwrap();
	assert!(
		txn.state_get(NODE_ID, &flush_probe_key()).unwrap().is_none(),
		"native must defer flush_state to commit, but state was persisted during apply"
	);

	txn.flush_operator_states().unwrap();
	assert!(
		txn.state_get(NODE_ID, &flush_probe_key()).unwrap().is_some(),
		"flush_operator_states must persist the deferred state"
	);
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = deferred_txn(&e);
	assert_flush_is_deferred(&mut txn);
}

#[test]
fn transactional() {
	let e = engine();
	let mut txn = transactional_txn(&e);
	assert_flush_is_deferred(&mut txn);
}

#[test]
fn ephemeral() {
	let e = engine();
	let mut txn = ephemeral_txn(&e);
	assert_flush_is_deferred(&mut txn);
}
