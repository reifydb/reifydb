// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use reifydb_core::{
	common::{ChangeVersion, CommitVersion, JoinType},
	expression::{AccessObjectExpression, Expression},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
		identifier::{ColumnIdentifier, ColumnObject},
	},
	value::{
		batch::batch,
		column::{builder::ColumnBuilder, factory},
	},
};
use reifydb_flow::context::FlowContext;
use reifydb_flow_async::operator::{
	HostOperator,
	host::TxnHostContext,
	join::operator::{JoinOperator, JoinSideConfig},
};
use reifydb_rql::expression::parse_expression;
use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		datetime::DateTime,
		system_columns::{SystemColumn, with_system_column},
		value_type::ValueType,
	},
};

const LEFT_OPERATOR: OperatorId = OperatorId(1);
const RIGHT_OPERATOR: OperatorId = OperatorId(2);
const JOIN_OPERATOR: OperatorId = OperatorId(3);

fn left_columns() -> Vec<(&'static str, ValueType)> {
	vec![("lid", ValueType::Int8), ("a", ValueType::Option(Box::new(ValueType::Int4)))]
}

fn right_columns() -> Vec<(&'static str, ValueType)> {
	vec![("rid", ValueType::Int8), ("flag", ValueType::Boolean)]
}

fn schema(spec: &[(&str, ValueType)]) -> SchemaRef {
	Arc::new(Schema::new(
		spec.iter()
			.map(|(name, ty)| ColumnBuilder::with_capacity(ty.clone(), 0).finish(name).0)
			.collect::<Vec<_>>(),
	))
}

fn row(spec: &[(&str, ValueType)], number: u64, values: Vec<Value>) -> RecordBatch {
	let columns = spec
		.iter()
		.zip(values)
		.map(|((name, ty), value)| {
			let mut buffer = ColumnBuilder::with_capacity(ty.clone(), 1);
			buffer.push_value(value);
			buffer.finish(name)
		})
		.collect();
	let at = DateTime::from_millis(1_000_000 + i64::try_from(number).expect("row number fits in i64 millis"));
	let system = [
		(SystemColumn::RowNumbers, factory::uint8("#rownum", [number]).1),
		(SystemColumn::CreatedAt, factory::datetime("#created_at", [at]).1),
		(SystemColumn::UpdatedAt, factory::datetime("#updated_at", [at]).1),
		(SystemColumn::Time, factory::datetime("#time", [at]).1),
	];
	system.into_iter().fold(batch(columns).expect("the row forms a batch"), |columns, (column, array)| {
		with_system_column(columns, column, array).expect("a system column attaches")
	})
}

fn insert(columns: RecordBatch, origin: OperatorId) -> Change {
	let mut diff = Diff::insert(columns);
	diff.set_origin(Some(ChangeOrigin::Flow(origin)));
	Change::from_flow(LEFT_OPERATOR, ChangeVersion::from(CommitVersion(1)), vec![diff], DateTime::default())
}

fn join(engine: &TestEngine, left_key: Vec<Expression>, right_key: Vec<Expression>) -> JoinOperator {
	JoinOperator::new(
		JoinSideConfig {
			operator: LEFT_OPERATOR,
			exprs: left_key,
			schema: schema(&left_columns()),
		},
		JoinSideConfig {
			operator: RIGHT_OPERATOR,
			exprs: right_key,
			schema: schema(&right_columns()),
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

fn joined_rows(engine: &TestEngine, operator: &mut JoinOperator) -> usize {
	let mut txn = engine.flow_txn().deferred();
	operator.apply(
		&mut TxnHostContext::new(&mut txn, JOIN_OPERATOR),
		insert(row(&right_columns(), 100, vec![Value::Int8(100), Value::Boolean(true)]), RIGHT_OPERATOR),
	)
	.expect("the right row is stored");
	let joined = operator
		.apply(
			&mut TxnHostContext::new(&mut txn, JOIN_OPERATOR),
			insert(row(&left_columns(), 1, vec![Value::Int8(1), Value::Int4(5)]), LEFT_OPERATOR),
		)
		.expect("the left row is keyed without error");
	joined.diffs
		.iter()
		.map(|diff| match diff {
			Diff::Insert {
				post,
				..
			} => post.num_rows(),
			other => panic!("an inner join over inserts must emit only inserts, got {other:?}"),
		})
		.sum()
}

#[test]
fn a_computed_join_key_takes_the_lowered_path() {
	// Between over a nullable column keys per row only on the lowered path; the old path errors.
	let engine = TestEngine::new();
	let mut operator =
		join(&engine, parse_expression("a between 2 and 10").unwrap(), parse_expression("flag").unwrap());

	assert_eq!(joined_rows(&engine, &mut operator), 1);
}

#[test]
fn a_missing_right_key_column_joins_as_none() {
	// A key naming a column the side lacks must read as none and match nothing, never raise.
	let engine = TestEngine::new();
	let missing = Expression::AccessSource(AccessObjectExpression {
		column: ColumnIdentifier {
			object: ColumnObject::Alias(Fragment::internal("other")),
			name: Fragment::internal("missing"),
		},
	});
	let mut operator = join(&engine, parse_expression("a").unwrap(), vec![missing]);

	assert_eq!(joined_rows(&engine, &mut operator), 0);
}
