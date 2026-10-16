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
use reifydb_flow::{context::FlowContext, operator::extend::ExtendOperator};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::parse_expression;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_value::{
	Result,
	value::{column_view::ColumnView, datetime::DateTime},
};

fn extend(expressions: &[&str]) -> Result<ExtendOperator> {
	ExtendOperator::new(
		None,
		OperatorId(1),
		expressions.iter().flat_map(|expression| parse_expression(expression).unwrap()).collect(),
		Routines::empty(),
		RuntimeContext::with_clock(Clock::Real),
		Arc::new(FlowContext::default()),
	)
}

fn insert(batch: RecordBatch) -> Change {
	Change::from_flow(
		OperatorId(0),
		CommitVersion(1).into(),
		vec![Diff::insert(batch)],
		DateTime::from_epoch_millis(0).unwrap(),
	)
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
			_ => panic!("an extend over inserts must emit only inserts"),
		})
		.collect()
}

#[test]
fn the_flow_extend_keeps_the_input_and_answers_between_per_row() {
	// The input must pass through, and between must answer per row over a nullable column.
	let rows = batch(vec![factory::int4_optional("a", [Some(1), None, Some(5)])]).unwrap();

	let out = extend(&["b: a between 2 and 10"]).unwrap().apply(insert(rows)).unwrap();

	assert_eq!(inserted(&out, "a"), vec!["1", "none", "5"]);
	assert_eq!(inserted(&out, "b"), vec!["false", "none", "true"]);
}

#[test]
fn a_type_mismatch_builds_the_extend_and_fails_every_apply() {
	// The error must come back on every apply, otherwise a later batch extends through a broken expression.
	let mut operator = extend(&["b: a > 'x'"]).unwrap();
	let rows = batch(vec![factory::int4("a", [1, 2])]).unwrap();

	let first = operator.apply(insert(rows.clone())).unwrap_err();
	let second = operator.apply(insert(rows)).unwrap_err();

	assert_eq!(first, second);
	assert!(first.code.starts_with("OPERATOR_02"), "{}", first.code);
}

#[test]
fn a_later_batch_with_moved_columns_reads_the_right_column() {
	// Without lowering again the moved column is read by its old position and the wrong value comes out.
	let mut operator = extend(&["c: a + 1"]).unwrap();
	let first = batch(vec![factory::int4("a", [2]), factory::int4("b", [0])]).unwrap();
	let moved = batch(vec![factory::int4("b", [0]), factory::int4("a", [5])]).unwrap();

	assert_eq!(inserted(&operator.apply(insert(first)).unwrap(), "c"), vec!["3"]);
	assert_eq!(inserted(&operator.apply(insert(moved)).unwrap(), "c"), vec!["6"]);
}

#[test]
fn a_broken_expression_fails_when_the_extend_is_built() {
	// A broken expression must fail at registration, otherwise the flow registers and only fails on data.
	let built = extend(&["b: a IS test::Status::Active"]);

	assert!(built.is_err(), "an unresolved variant must fail ExtendOperator::new");
}
