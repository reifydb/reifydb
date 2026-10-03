// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
	},
	value::{batch::batch, column::factory},
};
use reifydb_flow::context::FlowContext;
use reifydb_flow_async::operator::{HostOperator, gate::GateOperator, host::TxnHostContext};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::parse_expression;
use reifydb_runtime::context::{
	RuntimeContext,
	clock::{Clock, MockClock},
};
use reifydb_testing_sdk::in_process::transaction::TestFlowTransaction;
use reifydb_value::{
	Result,
	value::{column_view::ColumnView, datetime::DateTime, value_type::ValueType},
};

const GATE: OperatorId = OperatorId(1);

fn gate(condition: &str) -> GateOperator {
	GateOperator::new(
		None,
		GATE,
		parse_expression(condition).unwrap(),
		Routines::empty(),
		RuntimeContext::with_clock(Clock::Real),
		Arc::new(FlowContext::default()),
	)
	.unwrap()
}

fn insert(batch: RecordBatch) -> Change {
	Change::from_flow(
		OperatorId(0),
		CommitVersion(1).into(),
		vec![Diff::insert(batch)],
		DateTime::from_epoch_millis(0).unwrap(),
	)
}

fn apply(gate: &mut GateOperator, change: Change) -> Result<Change> {
	let mut txn = TestFlowTransaction::new(CommitVersion(1), Clock::Mock(MockClock::new(0)));
	let mut host = TxnHostContext::new(&mut txn, GATE);
	gate.apply(&mut host, change)
}

fn inserted(change: &Change, column: &str) -> Vec<String> {
	change.diffs
		.iter()
		.flat_map(|diff| match diff {
			Diff::Insert {
				post,
				..
			} => {
				let index = post.schema_ref().index_of(column).unwrap();
				let view = ColumnView::try_from((post.column(index), post.schema_ref().field(index)))
					.unwrap();
				(0..post.num_rows()).map(|row| view.as_string(row)).collect::<Vec<_>>()
			}
			_ => panic!("a gate over inserts must emit only inserts"),
		})
		.collect()
}

#[test]
fn the_gate_drops_none_false_and_non_boolean_rows() {
	// Only true may open the gate, and the lowered between must answer per row where the old path errors.
	let rows = batch(vec![factory::int4_optional("a", [Some(1), None, Some(5)])]).unwrap();

	let out = apply(&mut gate("a between 2 and 10"), insert(rows.clone())).unwrap();
	assert_eq!(inserted(&out, "a"), vec!["5"]);

	let out = apply(&mut gate("a"), insert(rows)).unwrap();
	assert!(out.diffs.is_empty());
}

#[test]
fn a_type_mismatch_builds_the_gate_and_fails_every_apply_even_on_all_none() {
	// The error must come back on every apply, otherwise a later batch opens a broken gate.
	let mut operator = gate("a > 'x'");
	let rows = batch(vec![factory::none_typed("a", ValueType::Int4, 2)]).unwrap();

	let first = apply(&mut operator, insert(rows.clone())).unwrap_err();
	let second = apply(&mut operator, insert(rows)).unwrap_err();

	assert_eq!(first, second);
	assert!(first.code.starts_with("OPERATOR_02"), "{}", first.code);
}
