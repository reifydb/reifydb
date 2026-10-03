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
use reifydb_flow::{context::FlowContext, operator::filter::FilterOperator};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::parse_expression;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_value::value::{column_view::ColumnView, datetime::DateTime, value_type::ValueType};

fn filter(condition: &str) -> FilterOperator {
	FilterOperator::new(
		None,
		OperatorId(1),
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
			_ => panic!("a filter over inserts must emit only inserts"),
		})
		.collect()
}

#[test]
fn the_filter_drops_none_and_false_and_rejects_a_non_boolean() {
	// None and false must drop a row, and the lowered between must answer per row where the old path errors.
	let rows = batch(vec![factory::int4_optional("a", [Some(1), None, Some(5)])]).unwrap();

	let out = filter("a between 2 and 10").apply(insert(rows.clone())).unwrap();
	assert_eq!(inserted(&out, "a"), vec!["5"]);

	let error = filter("a").apply(insert(rows)).unwrap_err();
	assert_eq!(error.code, "INTERNAL_ERROR");
}

#[test]
fn a_type_mismatch_builds_the_filter_and_fails_every_apply_even_on_all_none() {
	// The error must come back on every apply, otherwise a later batch passes rows through a broken filter.
	let mut operator = filter("a > 'x'");
	let rows = batch(vec![factory::none_typed("a", ValueType::Int4, 2)]).unwrap();

	let first = operator.apply(insert(rows.clone())).unwrap_err();
	let second = operator.apply(insert(rows)).unwrap_err();

	assert_eq!(first, second);
	assert!(first.code.starts_with("OPERATOR_02"), "{}", first.code);
}

#[test]
fn an_empty_change_passes_a_broken_filter_without_an_error() {
	// A change without rows must never lower, otherwise an empty batch fails a flow that passes today.
	let mut operator = filter("a > 'x'");
	let rows = batch(vec![factory::int4("a", Vec::<i32>::new())]).unwrap();

	let out = operator.apply(insert(rows)).unwrap();

	assert!(out.diffs.is_empty());
}

#[test]
fn a_later_batch_with_moved_columns_reads_the_right_column() {
	// Without lowering again the moved column is read by its old position and the wrong row passes.
	let mut operator = filter("a > 1");
	let first = batch(vec![factory::int4("a", [2]), factory::int4("b", [0])]).unwrap();
	let moved = batch(vec![factory::int4("b", [0, 5]), factory::int4("a", [5, 0])]).unwrap();

	assert_eq!(inserted(&operator.apply(insert(first)).unwrap(), "a"), vec!["2"]);
	assert_eq!(inserted(&operator.apply(insert(moved)).unwrap(), "a"), vec!["5"]);
}
