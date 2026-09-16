// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, sync::Arc};

use reifydb_core::{
	common::{ChangeVersion, CommitVersion, JoinType},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_flow::{
	context::FlowContext,
	operator::{
		HostOperator,
		distinct::operator::DistinctOperator,
		host::TxnHostContext,
		join::operator::{JoinOperator, JoinSideConfig},
	},
};
use reifydb_rql::expression::{Expression, parse_expression};
use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
use reifydb_value::{
	Result,
	fragment::Fragment,
	value::{
		Value, datetime::DateTime, digest::Digest, row_number::RowNumber, system_columns::SystemColumns,
		value_type::ValueType,
	},
};

const DISTINCT_OPERATOR: OperatorId = OperatorId(30);
const LEFT_OPERATOR: OperatorId = OperatorId(31);
const RIGHT_OPERATOR: OperatorId = OperatorId(32);
const JOIN_OPERATOR: OperatorId = OperatorId(33);

fn columns_numbered(named: Vec<(&str, ColumnBuffer)>, numbers: &[u64]) -> Columns {
	let at = DateTime::from_millis(1_000_000);
	Columns::with_system(
		named.into_iter().map(|(name, buffer)| ColumnWithName::new(Fragment::internal(name), buffer)).collect(),
		SystemColumns::new(
			numbers.iter().copied().map(RowNumber).collect(),
			Vec::new(),
			vec![at; numbers.len()],
			vec![at; numbers.len()],
			vec![at; numbers.len()],
		),
	)
}

fn columns(named: Vec<(&str, ColumnBuffer)>) -> Columns {
	let row_count = named.first().map(|(_, buffer)| buffer.len()).unwrap_or(0) as u64;
	let numbers: Vec<u64> = (1..=row_count).collect();
	columns_numbered(named, &numbers)
}

fn digest_buffer(rows: &[&[f64]]) -> ColumnBuffer {
	let ty = ValueType::Digest {
		inner: Box::new(ValueType::Float8),
		accuracy: 10_000,
	};
	let mut buffer = ColumnBuffer::with_capacity(ty, rows.len());
	for values in rows {
		let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
		for value in *values {
			digest.add_value(&Value::float8(*value)).unwrap();
		}
		buffer.push_value(Value::Digest(Box::new(digest)));
	}
	buffer
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

fn keys(names: &[&str]) -> Vec<Expression> {
	names.iter().flat_map(|name| parse_expression(name).expect("distinct key parses")).collect()
}

fn distinct(engine: &TestEngine, expressions: Vec<Expression>) -> DistinctOperator {
	DistinctOperator::new(
		None,
		DISTINCT_OPERATOR,
		expressions,
		engine.executor().routines.clone(),
		engine.executor().runtime_context.clone(),
		Arc::new(FlowContext::default()),
	)
	.expect("the distinct operator must build")
}

fn join(engine: &TestEngine, schema: Columns) -> JoinOperator {
	JoinOperator::new(
		JoinSideConfig {
			operator: LEFT_OPERATOR,
			exprs: parse_expression("k").expect("left key parses"),
			schema: schema.clone(),
		},
		JoinSideConfig {
			operator: RIGHT_OPERATOR,
			exprs: parse_expression("k").expect("right key parses"),
			schema,
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

fn apply_distinct(engine: &TestEngine, expressions: Vec<Expression>, diffs: Vec<Vec<Diff>>) -> Result<Vec<Change>> {
	let mut operator = distinct(engine, expressions);
	let mut txn = engine.flow_txn().deferred();
	let mut outputs = Vec::new();
	for batch in diffs {
		outputs.push(operator
			.apply(&mut TxnHostContext::new(&mut txn, DISTINCT_OPERATOR), change(LEFT_OPERATOR, batch))?);
	}
	Ok(outputs)
}

fn inserted_rows(output: &Change) -> usize {
	output.diffs
		.iter()
		.map(|diff| match diff {
			Diff::Insert {
				post,
				..
			} => post.row_count(),
			_ => 0,
		})
		.sum()
}

fn error_code<T: Debug>(result: Result<T>) -> String {
	match result {
		Ok(value) => panic!("expected an error, got {value:?}"),
		Err(err) => err.diagnostic().code,
	}
}

#[test]
fn flow_distinct_on_named_columns_keeps_rows_whose_text_only_matches_when_concatenated() {
	// The named-key path evaluates its own key columns, so it must keep the value boundary too.
	let engine = TestEngine::new();
	let input = columns(vec![
		("x", ColumnBuffer::utf8(vec!["ab".to_string(), "a".to_string()])),
		("y", ColumnBuffer::utf8(vec!["c".to_string(), "bc".to_string()])),
	]);

	let outputs = apply_distinct(&engine, keys(&["x", "y"]), vec![vec![Diff::insert(input)]]).expect("applies");

	assert_eq!(inserted_rows(&outputs[0]), 2, "two different rows must both survive: {:?}", outputs[0].diffs);
}

#[test]
fn flow_distinct_keeps_int_rows_whose_digits_only_match_when_concatenated() {
	// (1, 23) and (12, 3) render to the same digits once joined, but are different rows.
	let engine = TestEngine::new();
	let input = columns(vec![("a", ColumnBuffer::int4(vec![1, 12])), ("b", ColumnBuffer::int4(vec![23, 3]))]);

	let outputs = apply_distinct(&engine, Vec::new(), vec![vec![Diff::insert(input)]]).expect("applies");

	assert_eq!(inserted_rows(&outputs[0]), 2, "two different rows must both survive: {:?}", outputs[0].diffs);
}

#[test]
fn flow_distinct_retracting_one_of_two_text_colliding_rows_removes_exactly_that_row() {
	// Sharing one entry turns the retraction into an update that swaps in the other row instead of a remove.
	let engine = TestEngine::new();
	let both = columns(vec![
		("x", ColumnBuffer::utf8(vec!["ab".to_string(), "a".to_string()])),
		("y", ColumnBuffer::utf8(vec!["c".to_string(), "bc".to_string()])),
	]);
	let second = columns_numbered(
		vec![
			("x", ColumnBuffer::utf8(vec!["a".to_string()])),
			("y", ColumnBuffer::utf8(vec!["bc".to_string()])),
		],
		&[2],
	);

	let outputs = apply_distinct(&engine, Vec::new(), vec![vec![Diff::insert(both)], vec![Diff::remove(second)]])
		.expect("applies");

	let retraction = &outputs[1].diffs;
	assert_eq!(retraction.len(), 1, "exactly one diff for one retracted row: {retraction:?}");
	let Diff::Remove {
		pre,
		..
	} = &retraction[0]
	else {
		panic!("retracting the only row under its key must remove it: {retraction:?}");
	};
	assert_eq!(pre.column("x").unwrap().data().get_value(0), Value::Utf8("a".to_string()), "wrong row removed");
}

#[test]
fn flow_distinct_merges_two_nones_into_one_row_like_group_by() {
	// Two missing values are the same key, as they are one group in `by`; splitting them duplicates the row.
	let engine = TestEngine::new();
	let input = columns(vec![("x", ColumnBuffer::none_typed(ValueType::Utf8, 2))]);

	let outputs = apply_distinct(&engine, Vec::new(), vec![vec![Diff::insert(input)]]).expect("applies");

	assert_eq!(inserted_rows(&outputs[0]), 1, "two nones are one distinct row: {:?}", outputs[0].diffs);
}

#[test]
fn flow_distinct_on_a_tuple_column_is_an_error_not_a_panic() {
	// The typed key encoding has no form for an any value, so reaching it would panic the flow.
	let engine = TestEngine::new();
	let tuples = ColumnBuffer::any(vec![
		Value::Tuple(vec![Value::Utf8("ab".to_string()), Value::Utf8("c".to_string())]),
		Value::Tuple(vec![Value::Utf8("a".to_string()), Value::Utf8("bc".to_string())]),
	]);
	let input = columns(vec![("t", tuples)]);

	let code = error_code(apply_distinct(&engine, Vec::new(), vec![vec![Diff::insert(input)]]));

	assert_eq!(code, "DISTINCT_001");
}

#[test]
fn flow_distinct_on_a_digest_column_reports_the_batch_code() {
	// Flow must refuse a digest key with the same code as the batch distinct, on both key paths.
	let engine = TestEngine::new();
	let all_columns = columns(vec![("d", digest_buffer(&[&[1.0, 2.0], &[3.0, 4.0]]))]);
	let named = all_columns.clone();

	let all_code = error_code(apply_distinct(&engine, Vec::new(), vec![vec![Diff::insert(all_columns)]]));
	let named_code = error_code(apply_distinct(&engine, keys(&["d"]), vec![vec![Diff::insert(named)]]));

	assert_eq!(all_code, "DISTINCT_001");
	assert_eq!(named_code, "DISTINCT_001");
}

#[test]
fn flow_join_on_a_digest_key_reports_the_batch_code_on_either_side() {
	// A right-side arrival hashes its key through the same path, so it must refuse a digest too.
	let engine = TestEngine::new();
	let schema = columns(vec![("k", digest_buffer(&[&[1.0]]))]);

	for origin in [LEFT_OPERATOR, RIGHT_OPERATOR] {
		let mut operator = join(&engine, schema.clone());
		let mut txn = engine.flow_txn().deferred();
		let rows = columns(vec![("k", digest_buffer(&[&[1.0]]))]);

		let result = operator.apply(
			&mut TxnHostContext::new(&mut txn, JOIN_OPERATOR),
			change(origin, vec![Diff::insert(rows)]),
		);

		assert_eq!(error_code(result.map(|c| c.diffs)), "JOIN_001", "origin {origin:?}");
	}
}

#[test]
fn flow_join_on_an_all_none_digest_key_is_an_error_like_the_batch_join() {
	// Batch refuses the key by its type, so a digest column holding only nones must not join silently in flow.
	let engine = TestEngine::new();
	let ty = ValueType::Digest {
		inner: Box::new(ValueType::Float8),
		accuracy: 10_000,
	};
	let schema = columns(vec![("k", ColumnBuffer::none_typed(ty.clone(), 1))]);
	let mut operator = join(&engine, schema.clone());
	let mut txn = engine.flow_txn().deferred();

	let result = operator.apply(
		&mut TxnHostContext::new(&mut txn, JOIN_OPERATOR),
		change(LEFT_OPERATOR, vec![Diff::insert(columns(vec![("k", ColumnBuffer::none_typed(ty, 1))]))]),
	);

	assert_eq!(error_code(result.map(|c| c.diffs)), "JOIN_001");
}
