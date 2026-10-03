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
use reifydb_flow::{context::FlowContext, operator::map::MapOperator};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::parse_expression;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_value::value::{column_view::ColumnView, datetime::DateTime};

fn map(expressions: &[&str]) -> MapOperator {
	MapOperator::new(
		None,
		OperatorId(1),
		expressions.iter().flat_map(|expression| parse_expression(expression).unwrap()).collect(),
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
			_ => panic!("a map over inserts must emit only inserts"),
		})
		.collect()
}

#[test]
fn the_flow_map_names_each_column_by_its_label() {
	// Each column must carry its label, and the lowered between must answer per row where the old path errors.
	let rows = batch(vec![factory::int4_optional("a", [Some(1), None, Some(5)])]).unwrap();

	let out = map(&["a", "b: a between 2 and 10"]).apply(insert(rows)).unwrap();

	assert_eq!(inserted(&out, "a"), vec!["1", "none", "5"]);
	assert_eq!(inserted(&out, "b"), vec!["false", "none", "true"]);
}

#[test]
fn a_type_mismatch_builds_the_map_and_fails_every_apply() {
	// The error must come back on every apply, otherwise a later batch projects through a broken map.
	let mut operator = map(&["b: a > 'x'"]);
	let rows = batch(vec![factory::int4("a", [1, 2])]).unwrap();

	let first = operator.apply(insert(rows.clone())).unwrap_err();
	let second = operator.apply(insert(rows)).unwrap_err();

	assert_eq!(first, second);
	assert!(first.code.starts_with("OPERATOR_02"), "{}", first.code);
}

#[test]
fn a_later_batch_with_moved_columns_reads_the_right_column() {
	// Without lowering again the moved column is read by its old position and the wrong value comes out.
	let mut operator = map(&["b: a + 1"]);
	let first = batch(vec![factory::int4("a", [2]), factory::int4("b", [0])]).unwrap();
	let moved = batch(vec![factory::int4("b", [0]), factory::int4("a", [5])]).unwrap();

	assert_eq!(inserted(&operator.apply(insert(first)).unwrap(), "b"), vec!["3"]);
	assert_eq!(inserted(&operator.apply(insert(moved)).unwrap(), "b"), vec!["6"]);
}
