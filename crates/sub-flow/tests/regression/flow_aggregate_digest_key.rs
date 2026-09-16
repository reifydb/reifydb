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
	value::{
		Value, datetime::DateTime, digest::Digest, row_number::RowNumber, system_columns::SystemColumns,
		value_type::ValueType,
	},
};

const SOURCE_OPERATOR: OperatorId = OperatorId(41);
const AGGREGATE_OPERATOR: OperatorId = OperatorId(42);

fn digest(values: &[f64]) -> Value {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	for value in values {
		digest.add_value(&Value::float8(*value)).unwrap();
	}
	Value::Digest(Box::new(digest))
}

#[test]
fn flow_aggregate_by_a_digest_column_is_an_error_like_the_batch_group_by() {
	// Batch refuses a digest group key with AGGREGATE_008, so a view grouping by one must not build groups from it.
	let engine = TestEngine::new();
	let mut operator = AggregateOperator::new(
		None,
		AGGREGATE_OPERATOR,
		parse_expression("d").expect("group key parses"),
		parse_expression("n: math::count(k)").expect("aggregation parses"),
		engine.executor().routines.clone(),
		engine.executor().runtime_context.clone(),
	)
	.expect("the aggregate operator must build");
	let ty = ValueType::Digest {
		inner: Box::new(ValueType::Float8),
		accuracy: 10_000,
	};
	let mut digests = ColumnBuffer::with_capacity(ty, 2);
	digests.push_value(digest(&[1.0, 2.0]));
	digests.push_value(digest(&[3.0, 4.0]));
	let at = DateTime::from_millis(1_000_000);
	let input = Columns::with_system(
		vec![
			ColumnWithName::new(Fragment::internal("k"), ColumnBuffer::int4(vec![1, 2])),
			ColumnWithName::new(Fragment::internal("d"), digests),
		],
		SystemColumns::new(vec![RowNumber(1), RowNumber(2)], Vec::new(), vec![at; 2], vec![at; 2], vec![at; 2]),
	);
	let mut diff = Diff::insert(input);
	diff.set_origin(Some(ChangeOrigin::Flow(SOURCE_OPERATOR)));
	let change = Change::from_flow(SOURCE_OPERATOR, ChangeVersion::from(CommitVersion(1)), vec![diff], at);
	let mut txn = engine.flow_txn().deferred();

	let result = operator.apply(&mut TxnHostContext::new(&mut txn, AGGREGATE_OPERATOR), change);

	match result {
		Ok(output) => panic!("a digest group key must be an error, got {:?}", output.diffs),
		Err(err) => assert_eq!(err.diagnostic().code, "AGGREGATE_008"),
	}
}
