// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{FieldRef, SchemaRef};
use reifydb_core::{
	common::{ChangeVersion, CommitVersion, JoinType},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
	},
	value::{
		batch::batch,
		column::{builder::ColumnBuilder, factory},
	},
};
use reifydb_flow::context::FlowContext;
use reifydb_flow_async::operator::{
	HostOperator,
	distinct::operator::DistinctOperator,
	host::TxnHostContext,
	join::operator::{JoinOperator, JoinSideConfig},
};
use reifydb_rql::expression::parse_expression;
use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
use reifydb_value::value::{
	Value,
	datetime::DateTime,
	digest::Digest,
	system_columns::{SystemColumn, with_system_column},
	value_type::ValueType,
};

const DISTINCT_OPERATOR: OperatorId = OperatorId(10);
const LEFT_OPERATOR: OperatorId = OperatorId(21);
const RIGHT_OPERATOR: OperatorId = OperatorId(22);
const JOIN_OPERATOR: OperatorId = OperatorId(23);

fn columns(named: Vec<(FieldRef, ArrayRef)>) -> RecordBatch {
	let row_count = named.first().map(|(_, array)| array.len()).unwrap_or(0);
	let at = DateTime::from_millis(1_000_000);
	let system = [
		(SystemColumn::RowNumbers, factory::uint8("#rownum", 1..=row_count as u64).1),
		(SystemColumn::CreatedAt, factory::datetime("#created_at", vec![at; row_count]).1),
		(SystemColumn::UpdatedAt, factory::datetime("#updated_at", vec![at; row_count]).1),
		(SystemColumn::Time, factory::datetime("#time", vec![at; row_count]).1),
	];
	system.into_iter().fold(batch(named).expect("user columns form a batch"), |columns, (column, array)| {
		with_system_column(columns, column, array).expect("a system column attaches")
	})
}

fn digest_buffer(name: &str, rows: &[&[f64]]) -> (FieldRef, ArrayRef) {
	let ty = ValueType::Digest {
		inner: Box::new(ValueType::Float8),
		accuracy: 10_000,
	};
	let mut buffer = ColumnBuilder::with_capacity(ty, rows.len());
	for values in rows {
		let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
		for value in *values {
			digest.add_value(&Value::float8(*value)).unwrap();
		}
		buffer.push_value(Value::Digest(Box::new(digest)));
	}
	buffer.finish(name)
}

fn change(origin: OperatorId, diffs: Vec<Diff>) -> Change {
	let diffs: Vec<Diff> = diffs
		.into_iter()
		.map(|mut diff| {
			diff.set_origin(Some(ChangeOrigin::Flow(origin)));
			diff
		})
		.collect();
	Change::from_flow(origin, ChangeVersion::from(CommitVersion(1)), diffs, DateTime::default())
}

fn distinct(engine: &TestEngine) -> DistinctOperator {
	DistinctOperator::new(
		None,
		DISTINCT_OPERATOR,
		Vec::new(),
		engine.executor().routines.clone(),
		engine.executor().runtime_context.clone(),
		Arc::new(FlowContext::default()),
	)
	.expect("the distinct operator must build")
}

fn inserted_rows(output: &Change) -> usize {
	output.diffs
		.iter()
		.map(|diff| match diff {
			Diff::Insert {
				post,
				..
			} => post.num_rows(),
			_ => 0,
		})
		.sum()
}

fn join(engine: &TestEngine, left_schema: SchemaRef, right_schema: SchemaRef) -> JoinOperator {
	JoinOperator::new(
		JoinSideConfig {
			operator: LEFT_OPERATOR,
			exprs: parse_expression("k").expect("left key parses"),
			schema: left_schema,
		},
		JoinSideConfig {
			operator: RIGHT_OPERATOR,
			exprs: parse_expression("k").expect("right key parses"),
			schema: right_schema,
		},
		JOIN_OPERATOR,
		JoinType::Inner,
		None,
		engine.executor().routines.clone(),
		engine.executor().runtime_context.clone(),
		false,
		false,
		None,
		None,
		None,
		Arc::new(FlowContext::default()),
	)
	.expect("the join operator must build")
}

#[test]
fn flow_distinct_keeps_rows_whose_text_only_matches_when_concatenated() {
	// Joining rendered values without a boundary makes ("ab", "c") and ("a", "bc") one row.
	let engine = TestEngine::new();
	let mut operator = distinct(&engine);
	let mut txn = engine.flow_txn().deferred();
	let input = columns(vec![
		factory::utf8("x", vec!["ab".to_string(), "a".to_string()]),
		factory::utf8("y", vec!["c".to_string(), "bc".to_string()]),
	]);

	let output = operator
		.apply(
			&mut TxnHostContext::new(&mut txn, DISTINCT_OPERATOR),
			change(LEFT_OPERATOR, vec![Diff::insert(input)]),
		)
		.expect("distinct applies");

	assert_eq!(inserted_rows(&output), 2, "two different rows must both survive distinct: {:?}", output.diffs);
}

#[test]
fn flow_distinct_keeps_a_none_apart_from_the_text_none() {
	// A missing value and the four letters "none" render alike but are different values.
	let engine = TestEngine::new();
	let mut operator = distinct(&engine);
	let mut txn = engine.flow_txn().deferred();
	let mut x = ColumnBuilder::with_capacity(ValueType::Utf8, 2);
	x.push_none();
	x.push_value(Value::Utf8("none".to_string()));
	let x = x.finish("x");

	let output = operator
		.apply(
			&mut TxnHostContext::new(&mut txn, DISTINCT_OPERATOR),
			change(LEFT_OPERATOR, vec![Diff::insert(columns(vec![x]))]),
		)
		.expect("distinct applies");

	assert_eq!(inserted_rows(&output), 2, "none and \"none\" must both survive distinct: {:?}", output.diffs);
}

#[test]
fn flow_distinct_on_a_digest_column_is_an_error() {
	// Two digests with equal counts render alike, so keying on the rendering merges different distributions.
	let engine = TestEngine::new();
	let mut operator = distinct(&engine);
	let mut txn = engine.flow_txn().deferred();
	let input = columns(vec![digest_buffer("d", &[&[1.0, 2.0], &[3.0, 4.0]])]);

	let result = operator.apply(
		&mut TxnHostContext::new(&mut txn, DISTINCT_OPERATOR),
		change(LEFT_OPERATOR, vec![Diff::insert(input)]),
	);

	assert!(result.is_err(), "a digest distinct key must be an error, got {:?}", result.map(|c| c.diffs));
}

#[test]
fn flow_join_on_a_digest_key_is_an_error() {
	// A digest has no key identity, so a flow join must refuse it like the batch join does.
	let engine = TestEngine::new();
	let schema = batch(vec![digest_buffer("k", &[&[1.0]])]).expect("the key column forms a batch").schema();
	let mut operator = join(&engine, schema.clone(), schema);
	let mut txn = engine.flow_txn().deferred();
	let left = columns(vec![digest_buffer("k", &[&[1.0]])]);

	let result = operator.apply(
		&mut TxnHostContext::new(&mut txn, JOIN_OPERATOR),
		change(LEFT_OPERATOR, vec![Diff::insert(left)]),
	);

	assert!(result.is_err(), "a digest join key must be an error, got {:?}", result.map(|c| c.diffs));
}
