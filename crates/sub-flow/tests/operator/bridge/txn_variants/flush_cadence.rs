// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::change::{Change, Diffs},
};
use reifydb_flow::{operator::Operator, transaction::interface::FlowTransaction};
use reifydb_sdk::flow::operator::OperatorMetadata;
use reifydb_sub_flow::operator::bridge::{BridgeOperator, BridgeOperatorAdapter};
use reifydb_test_harness::operator::transaction::{FlowTxn, OPERATOR_ID, engine};
use reifydb_value::value::datetime::DateTime;

use crate::common::{FlushProbe, flush_probe_key};

fn assert_flush_is_deferred<T: FlowTransaction>(txn: &mut T) {
	// Re-asserted across every FlowTransaction variant, or a variant that flushed during apply would go unnoticed.
	let capabilities = <FlushProbe as OperatorMetadata>::CAPABILITIES;
	let inner = BridgeOperatorAdapter::new(FlushProbe, OPERATOR_ID, capabilities);
	let op = BridgeOperator::new(Box::new(inner), OPERATOR_ID, capabilities);
	let change = Change::from_flow(OPERATOR_ID, CommitVersion(1), Diffs::new(), DateTime::from_nanos(0));

	op.apply(txn, change).unwrap();
	assert!(
		txn.state_get(OPERATOR_ID, &flush_probe_key()).unwrap().is_none(),
		"bridge must defer flush_state to commit, but state was persisted during apply"
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
fn ephemeral() {
	let e = engine();
	let mut txn = e.flow_txn().ephemeral();
	assert_flush_is_deferred(&mut txn);
}
