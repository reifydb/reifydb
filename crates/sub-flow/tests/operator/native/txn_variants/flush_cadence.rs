// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The flush_state cadence re-asserted across all three FlowTransaction variants: the native backend
// defers flush_state to commit, so the probe's state must be invisible after apply and persisted
// only by the explicit flush, in every variant.

use reifydb_core::{
	common::CommitVersion,
	interface::change::{Change, Diffs},
};
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
use reifydb_sdk::operator::OperatorMetadata;
use reifydb_sub_flow::operator::native::{NativeBridgedOperator, NativeOperatorAdapter};
use reifydb_test_harness::operator::transaction::{FlowTxn, OPERATOR_ID, engine};
use reifydb_value::value::datetime::DateTime;

use crate::common::{FlushProbe, flush_probe_key};

fn assert_flush_is_deferred(txn: &mut FlowTransaction) {
	let capabilities = <FlushProbe as OperatorMetadata>::CAPABILITIES;
	let inner = NativeOperatorAdapter::new(FlushProbe, OPERATOR_ID, capabilities);
	let op = NativeBridgedOperator::new(Box::new(inner), OPERATOR_ID, capabilities);
	let change = Change::from_flow(OPERATOR_ID, CommitVersion(1), Diffs::new(), DateTime::from_nanos(0));

	op.apply(txn, change).unwrap();
	assert!(
		txn.state_get(OPERATOR_ID, &flush_probe_key()).unwrap().is_none(),
		"native must defer flush_state to commit, but state was persisted during apply"
	);

	txn.flush_operator_states().unwrap();
	assert!(
		txn.state_get(OPERATOR_ID, &flush_probe_key()).unwrap().is_some(),
		"flush_operator_states must persist the deferred state"
	);
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = e.flow_txn().deferred();
	assert_flush_is_deferred(&mut txn);
}

#[test]
fn transactional() {
	let e = engine();
	let mut txn = e.flow_txn().transactional();
	assert_flush_is_deferred(&mut txn);
}

#[test]
fn ephemeral() {
	let e = engine();
	let mut txn = e.flow_txn().ephemeral();
	assert_flush_is_deferred(&mut txn);
}
