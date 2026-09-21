// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_flow::operator::{HostOperator, aggregation::operator::AggregateOperator, host::TxnHostContext};
use reifydb_rql::expression::parse_expression;
use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
use reifydb_value::{
	fragment::Fragment,
	value::{Value, datetime::DateTime, row_number::RowNumber, system_columns::SystemColumns},
};

const SOURCE_OPERATOR: OperatorId = OperatorId(61);
const AGGREGATE_OPERATOR: OperatorId = OperatorId(62);

fn keyed_by(key: ColumnBuffer) -> Change {
	let at = DateTime::from_millis(1_000_000);
	let input = Columns::with_system(
		vec![
			ColumnWithName::new(Fragment::internal("k"), ColumnBuffer::int4(vec![1, 2])),
			ColumnWithName::new(Fragment::internal("l"), key),
		],
		SystemColumns::new(vec![RowNumber(1), RowNumber(2)], Vec::new(), vec![at; 2], vec![at; 2], vec![at; 2], Vec::new()),
	);
	let mut diff = Diff::insert(input);
	diff.set_origin(Some(ChangeOrigin::Flow(SOURCE_OPERATOR)));
	Change::from_flow(SOURCE_OPERATOR, ChangeVersion::from(CommitVersion(1)), vec![diff], at)
}

fn unkeyable_keys() -> Vec<(&'static str, ColumnBuffer)> {
	vec![
		("an any", ColumnBuffer::any(vec![Value::Int4(1), Value::Utf8("one".to_string())])),
		(
			"a list",
			ColumnBuffer::any(vec![
				Value::List(vec![Value::Int4(1), Value::Int4(2)]),
				Value::List(vec![Value::Int4(3)]),
			]),
		),
		(
			"a record",
			ColumnBuffer::any(vec![
				Value::Record(vec![("x".to_string(), Value::Int4(1))]),
				Value::Record(vec![("x".to_string(), Value::Int4(2))]),
			]),
		),
		(
			"a tuple",
			ColumnBuffer::any(vec![
				Value::Tuple(vec![Value::Int4(1), Value::Int4(2)]),
				Value::Tuple(vec![Value::Int4(3), Value::Int4(4)]),
			]),
		),
	]
}

#[test]
fn flow_aggregate_by_an_any_list_record_or_tuple_column_is_an_error_like_the_batch_group_by() {
	// Batch refuses these keys with AGGREGATE_008, so a view must not build groups batch never would.
	let mut failures = Vec::new();
	for (kind, key) in unkeyable_keys() {
		let engine = TestEngine::new();
		let mut operator = AggregateOperator::new(
			None,
			AGGREGATE_OPERATOR,
			parse_expression("l").expect("group key parses"),
			parse_expression("n: math::count(k)").expect("aggregation parses"),
			engine.executor().routines.clone(),
			engine.executor().runtime_context.clone(),
		)
		.expect("the aggregate operator must build");
		let mut txn = engine.flow_txn().deferred();

		let result = operator.apply(&mut TxnHostContext::new(&mut txn, AGGREGATE_OPERATOR), keyed_by(key));

		match result {
			Ok(output) => failures.push(format!("{kind} key built groups: {:?}", output.diffs)),
			Err(err) => {
				let diagnostic = err.diagnostic();
				if diagnostic.code != "AGGREGATE_008" || diagnostic.fragment.text() != "l" {
					failures.push(format!(
						"{kind} key refused with {} on '{}'",
						diagnostic.code,
						diagnostic.fragment.text()
					));
				}
			}
		}
	}

	assert!(failures.is_empty(), "{failures:#?}");
}
