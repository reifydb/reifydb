// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
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
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{
	column_view::ColumnView,
	datetime::DateTime,
	value_type::{
		ValueType,
		field::{FieldType, from_field, to_field},
	},
};

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
	// Each column must carry its label, and between must answer per row over a nullable column.
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

fn parent() -> SchemaRef {
	Arc::new(Schema::new(vec![
		to_field("a", &FieldType::from(ValueType::Utf8)),
		to_field("b", &FieldType::from(ValueType::Utf8)),
		to_field("flag", &FieldType::from(ValueType::Boolean)),
		to_field("oflag", &FieldType::from(ValueType::Option(Box::new(ValueType::Boolean)))),
		to_field("n", &FieldType::from(ValueType::Int4)),
	]))
}

fn typed_map(engine: &TestEngine, expressions: &[&str]) -> MapOperator {
	MapOperator::new(
		Some(parent()),
		OperatorId(1),
		expressions.iter().flat_map(|expression| parse_expression(expression).unwrap()).collect(),
		engine.executor().routines.clone(),
		engine.executor().runtime_context.clone(),
		Arc::new(FlowContext::default()),
	)
	.unwrap()
}

fn output_type(operator: &MapOperator, name: &str) -> Option<ValueType> {
	let schema = operator.output_schema().expect("a map always has an output schema");
	from_field(schema.field_with_name(name).expect("the output names the column")).unwrap().value_type
}

fn optional(value_type: ValueType) -> ValueType {
	ValueType::Option(Box::new(value_type))
}

#[test]
fn a_plain_column_keeps_its_exact_parent_field() {
	// Existing views and sinks read a bare column's field as is, so its name, type and metadata must never change.
	let engine = TestEngine::new();
	let operator = typed_map(&engine, &["a", "oflag"]);
	let schema = operator.output_schema().unwrap();

	assert_eq!(schema.field_with_name("a").unwrap(), parent().field_with_name("a").unwrap());
	assert_eq!(schema.field_with_name("oflag").unwrap(), parent().field_with_name("oflag").unwrap());
}

#[test]
fn a_computed_column_reports_the_type_its_expression_produces() {
	// A computed column reported as Any fails every downstream key type check, a lookup on it never registers.
	let engine = TestEngine::new();
	let cases = [
		("k: cast(n, utf8)", ValueType::Utf8),
		("k: if flag { a } else { b }", ValueType::Utf8),
		("k: n + 1", ValueType::Int8),
		("k: n > 1", ValueType::Boolean),
		("k: text::lower(a)", ValueType::Utf8),
	];
	let mut failures = Vec::new();
	for (expression, want) in cases {
		let got = output_type(&typed_map(&engine, &[expression]), "k");
		if got.as_ref() != Some(&want) {
			failures.push(format!("{expression}: got {got:?}, want {want:?}"));
		}
	}
	assert!(failures.is_empty(), "computed columns with the wrong type:\n{}", failures.join("\n"));
}

#[test]
fn an_if_with_a_none_branch_is_optional_and_an_optional_condition_is_not() {
	// A none branch must make the result optional, an optional condition never does since none takes else.
	let engine = TestEngine::new();

	assert_eq!(
		output_type(&typed_map(&engine, &["k: if flag { a } else { none }"]), "k"),
		Some(optional(ValueType::Utf8))
	);
	assert_eq!(output_type(&typed_map(&engine, &["k: if oflag { a } else { b }"]), "k"), Some(ValueType::Utf8));
}

#[test]
fn an_untypable_expression_stays_any() {
	// Guessing a type where none can be known would let a lookup register on a wrong type and match the wrong rows.
	let engine = TestEngine::new();

	assert_eq!(output_type(&typed_map(&engine, &["k: (a, b)"]), "k"), Some(ValueType::Any));
}
