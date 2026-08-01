// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The lease control loop rides the mandatory flush choreography, never the sampling loop, which is
// a debugging tool that may never run in production. A commit-time flush must therefore already
// have resized the operator's grant to its reported demand plus 25% headroom.

use reifydb_core::{
	common::CommitVersion,
	interface::change::{Change, Diffs},
};
use reifydb_flow::operator::Operator;
use reifydb_sdk::operator::OperatorMetadata;
use reifydb_sub_flow::operator::native::{NativeBridgedOperator, NativeOperatorAdapter};
use reifydb_test_harness::operator::transaction::{FlowTxn, OPERATOR_ID, engine};
use reifydb_value::{byte_size::ByteSize, value::datetime::DateTime};

use crate::common::{LEASE_PROBE_REPORTED_BYTES, LeaseProbe};

#[test]
fn flush_resizes_the_lease_to_reported_demand_without_sampling() {
	let e = engine();
	let mut txn = e.flow_txn().deferred();
	let capabilities = <LeaseProbe as OperatorMetadata>::CAPABILITIES;
	let inner = NativeOperatorAdapter::new(LeaseProbe, OPERATOR_ID, capabilities);
	let op = NativeBridgedOperator::new(Box::new(inner), OPERATOR_ID, capabilities);

	let budget = txn.state_budget();
	budget.grant_lease(OPERATOR_ID, ByteSize::from_bytes(64 * 1024 * 1024));

	let change = Change::from_flow(OPERATOR_ID, CommitVersion(1), Diffs::new(), DateTime::from_nanos(0));
	op.apply(&mut txn, change).unwrap();
	txn.flush_operator_states().unwrap();

	let lease = budget.current_lease(OPERATOR_ID).unwrap();
	let expected = LEASE_PROBE_REPORTED_BYTES + LEASE_PROBE_REPORTED_BYTES / 4;
	assert_eq!(
		lease.grant.bytes(),
		ByteSize::from_bytes(expected),
		"the commit-time flush alone must resize the grant; no sample_operators call was made"
	);
	assert_eq!(lease.last.total_bytes(), ByteSize::from_bytes(LEASE_PROBE_REPORTED_BYTES));
}
