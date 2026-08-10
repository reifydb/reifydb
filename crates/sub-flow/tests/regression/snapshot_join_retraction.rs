// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The snapshot ledger exists so that a left row is retracted against the right value it was
//! actually emitted with, not the one the right side happens to hold later. Nothing downstream can
//! observe that: a materialised view sink recomputes its own delta from the row it already holds,
//! the aggregation operator reconciles against stored contributions, and the chaos oracle compares
//! a MaterializedView. All three reconcile a wrong `pre` away. So the property is only visible in
//! the diffs the operator itself returns, and that is what these tests read.

use std::sync::Arc;

use reifydb_core::{
	common::JoinType,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_flow::{
	context::FlowContext,
	operator::{
		Operator,
		join::operator::{JoinOperator, JoinSideConfig},
	},
};
use reifydb_rql::expression::parse_expression;
use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value, datetime::DateTime, row_number::RowNumber, system_columns::SystemColumns, value_type::ValueType,
	},
};

const LEFT_OPERATOR: OperatorId = OperatorId(1);
const RIGHT_OPERATOR: OperatorId = OperatorId(2);
const JOIN_OPERATOR: OperatorId = OperatorId(3);

const LEFT_COLUMNS: [(&str, ValueType); 3] =
	[("lid", ValueType::Int8), ("k", ValueType::Int4), ("lv", ValueType::Int8)];
const RIGHT_COLUMNS: [(&str, ValueType); 3] =
	[("rid", ValueType::Int8), ("k", ValueType::Int4), ("rv", ValueType::Int8)];

fn schema(spec: &[(&str, ValueType)]) -> Columns {
	Columns::new(
		spec.iter()
			.map(|(name, ty)| {
				ColumnWithName::new(
					Fragment::internal(*name),
					ColumnBuffer::with_capacity(ty.clone(), 0),
				)
			})
			.collect(),
	)
}

fn row(spec: &[(&str, ValueType); 3], number: u64, key: i32, value: i64) -> Columns {
	let mut buffers: Vec<ColumnBuffer> =
		spec.iter().map(|(_, ty)| ColumnBuffer::with_capacity(ty.clone(), 1)).collect();
	buffers[0].push_value(Value::Int8(number as i64));
	buffers[1].push_value(Value::Int4(key));
	buffers[2].push_value(Value::Int8(value));
	let columns = spec
		.iter()
		.zip(buffers)
		.map(|((name, _), buffer)| ColumnWithName::new(Fragment::internal(*name), buffer))
		.collect();
	let at = DateTime::from_millis(1_000_000 + number);
	Columns::with_system(
		columns,
		SystemColumns::new(vec![RowNumber(number)], Vec::new(), vec![at], vec![at], vec![at]),
	)
}

fn tagged(mut diff: Diff, origin: OperatorId) -> Diff {
	// The join reads the side off each diff rather than off the change, so the origin is what
	// decides whether these columns are treated as the left or the right input.
	diff.set_origin(Some(ChangeOrigin::Flow(origin)));
	diff
}

fn change(diffs: Vec<Diff>) -> Change {
	Change::from_flow(LEFT_OPERATOR, reifydb_core::common::CommitVersion(1), diffs, DateTime::default())
}

fn join(engine: &TestEngine) -> JoinOperator {
	JoinOperator::new(
		JoinSideConfig {
			operator: LEFT_OPERATOR,
			exprs: parse_expression("k").expect("left key parses"),
			schema: schema(&LEFT_COLUMNS),
		},
		JoinSideConfig {
			operator: RIGHT_OPERATOR,
			exprs: parse_expression("k").expect("right key parses"),
			schema: schema(&RIGHT_COLUMNS),
		},
		JOIN_OPERATOR,
		JoinType::Inner,
		None,
		engine.executor().routines.clone(),
		engine.executor().runtime_context.clone(),
		true,
		false,
		true,
		None,
		None,
		Arc::new(FlowContext::default()),
	)
}

/// The right-side value carried by a single-row `Columns`, looked up by name so a change in column
/// order cannot make the assertion read a different column and still pass.
fn right_value(columns: &Columns) -> i64 {
	let names: Vec<String> = columns.names.iter().map(|name| name.text().to_string()).collect();
	let idx = names
		.iter()
		.position(|name| name.ends_with("rv"))
		.unwrap_or_else(|| panic!("the joined row must carry the right side's rv column; got {names:?}"));
	match columns.columns[idx].get_value(0) {
		Value::Int8(v) => v,
		other => panic!("rv must be an int8, got {other:?}"),
	}
}

#[test]
fn a_left_update_retracts_against_the_right_value_it_was_emitted_with() {
	// The whole point of the ledger. The right side moves 10 -> 20 between the left row's insert
	// and its update, so the update's `pre` must still say 10: that is what the previous emission
	// carried, and a consumer that trusts `pre` (chaindex block_trade builds its retraction from
	// pre_data verbatim) subtracts exactly this. A `pre` of 20 subtracts something never added.
	let engine = TestEngine::new();
	let operator = join(&engine);
	let mut txn = engine.flow_txn().deferred();

	operator.apply(&mut txn, change(vec![tagged(Diff::insert(row(&RIGHT_COLUMNS, 100, 1, 10)), RIGHT_OPERATOR)]))
		.expect("the right slot is seeded");

	let inserted = operator
		.apply(&mut txn, change(vec![tagged(Diff::insert(row(&LEFT_COLUMNS, 1, 1, 7)), LEFT_OPERATOR)]))
		.expect("the left row joins");
	let [
		Diff::Insert {
			post,
			..
		},
	] = inserted.diffs.as_slice()
	else {
		panic!("a matched left insert must emit exactly one insert, got {:?}", inserted.diffs);
	};
	assert_eq!(right_value(post), 10, "the left row must be emitted against the slot it found");

	operator.apply(
		&mut txn,
		change(vec![tagged(
			Diff::update(row(&RIGHT_COLUMNS, 100, 1, 10), row(&RIGHT_COLUMNS, 100, 1, 20)),
			RIGHT_OPERATOR,
		)]),
	)
	.expect("the right slot moves");

	let updated = operator
		.apply(
			&mut txn,
			change(vec![tagged(
				Diff::update(row(&LEFT_COLUMNS, 1, 1, 7), row(&LEFT_COLUMNS, 1, 1, 8)),
				LEFT_OPERATOR,
			)]),
		)
		.expect("the left row updates");

	let [
		Diff::Update {
			pre,
			post,
			..
		},
	] = updated.diffs.as_slice()
	else {
		panic!("a left update under an unchanged key must emit one update, got {:?}", updated.diffs);
	};
	assert_eq!(
		right_value(pre),
		10,
		"the retraction must carry the right value the previous emission used; reading the live slot \
		 here would retract a row that was never emitted"
	);
	assert_eq!(right_value(post), 20, "and the new emission must carry the slot as it now stands");
}

#[test]
fn a_right_side_change_alone_emits_nothing() {
	// A latest+snapshot join stores the new right value for the next left arrival and publishes
	// nothing of its own. Re-emitting here would retract and re-issue every left row still inside
	// the ttl every time a dimension row ticked.
	let engine = TestEngine::new();
	let operator = join(&engine);
	let mut txn = engine.flow_txn().deferred();

	operator.apply(&mut txn, change(vec![tagged(Diff::insert(row(&RIGHT_COLUMNS, 100, 1, 10)), RIGHT_OPERATOR)]))
		.expect("the right slot is seeded");
	operator.apply(&mut txn, change(vec![tagged(Diff::insert(row(&LEFT_COLUMNS, 1, 1, 7)), LEFT_OPERATOR)]))
		.expect("the left row joins");

	let moved = operator
		.apply(
			&mut txn,
			change(vec![tagged(
				Diff::update(row(&RIGHT_COLUMNS, 100, 1, 10), row(&RIGHT_COLUMNS, 100, 1, 20)),
				RIGHT_OPERATOR,
			)]),
		)
		.expect("the right slot moves");

	assert!(moved.diffs.is_empty(), "the right side must publish nothing, got {:?}", moved.diffs);
}

#[test]
fn a_left_update_against_an_unchanged_slot_still_reports_both_sides() {
	// The shortcut path: when the slot has not moved the operator skips the ledger round trip. It
	// must still emit the same pair, with the right value on both halves.
	let engine = TestEngine::new();
	let operator = join(&engine);
	let mut txn = engine.flow_txn().deferred();

	operator.apply(&mut txn, change(vec![tagged(Diff::insert(row(&RIGHT_COLUMNS, 100, 1, 10)), RIGHT_OPERATOR)]))
		.expect("the right slot is seeded");
	operator.apply(&mut txn, change(vec![tagged(Diff::insert(row(&LEFT_COLUMNS, 1, 1, 7)), LEFT_OPERATOR)]))
		.expect("the left row joins");

	let updated = operator
		.apply(
			&mut txn,
			change(vec![tagged(
				Diff::update(row(&LEFT_COLUMNS, 1, 1, 7), row(&LEFT_COLUMNS, 1, 1, 8)),
				LEFT_OPERATOR,
			)]),
		)
		.expect("the left row updates");

	let [
		Diff::Update {
			pre,
			post,
			..
		},
	] = updated.diffs.as_slice()
	else {
		panic!("a left update must emit one update, got {:?}", updated.diffs);
	};
	assert_eq!(right_value(pre), 10);
	assert_eq!(right_value(post), 10, "an unmoved slot must appear on both halves");
}
