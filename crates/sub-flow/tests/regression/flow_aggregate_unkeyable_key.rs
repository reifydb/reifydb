// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
	},
	value::{batch::batch, column::factory},
};
use reifydb_flow_async::operator::{HostOperator, aggregation::operator::AggregateOperator, host::TxnHostContext};
use reifydb_rql::expression::parse_expression;
use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
use reifydb_value::value::{
	Value,
	datetime::DateTime,
	system_columns::{SystemColumn, with_system_column},
};

const SOURCE_OPERATOR: OperatorId = OperatorId(61);
const AGGREGATE_OPERATOR: OperatorId = OperatorId(62);

fn keyed_by(key: (FieldRef, ArrayRef)) -> Change {
	let at = DateTime::from_millis(1_000_000);
	let system = [
		(SystemColumn::RowNumbers, factory::uint8("#rownum", [1u64, 2]).1),
		(SystemColumn::CreatedAt, factory::datetime("#created_at", [at; 2]).1),
		(SystemColumn::UpdatedAt, factory::datetime("#updated_at", [at; 2]).1),
		(SystemColumn::Time, factory::datetime("#time", [at; 2]).1),
	];
	let input = system.into_iter().fold(
		batch(vec![factory::int4("k", [1, 2]), key]).expect("user columns form a batch"),
		|columns, (column, array)| {
			with_system_column(columns, column, array).expect("a system column attaches")
		},
	);
	let mut diff = Diff::insert(input);
	diff.set_origin(Some(ChangeOrigin::Flow(SOURCE_OPERATOR)));
	Change::from_flow(SOURCE_OPERATOR, ChangeVersion::from(CommitVersion(1)), vec![diff], at)
}

fn unkeyable_keys() -> Vec<(&'static str, (FieldRef, ArrayRef))> {
	vec![
		("an any", factory::any("l", vec![Value::Int4(1), Value::Utf8("one".to_string())])),
		(
			"a list",
			factory::any(
				"l",
				vec![
					Value::List(vec![Value::Int4(1), Value::Int4(2)]),
					Value::List(vec![Value::Int4(3)]),
				],
			),
		),
		(
			"a record",
			factory::any(
				"l",
				vec![
					Value::Record(vec![("x".to_string(), Value::Int4(1))]),
					Value::Record(vec![("x".to_string(), Value::Int4(2))]),
				],
			),
		),
		(
			"a tuple",
			factory::any(
				"l",
				vec![
					Value::Tuple(vec![Value::Int4(1), Value::Int4(2)]),
					Value::Tuple(vec![Value::Int4(3), Value::Int4(4)]),
				],
			),
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
