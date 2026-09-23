// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::{BooleanBuffer, NullBuffer};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
use reifydb_evaluate::expression::{logic::execute_logical_op, prefix::prefix_apply};
use reifydb_rql::expression::PrefixOperator;
use reifydb_value::{error::LogicalOp, fragment::Fragment, value::value_type::ValueType};

fn column(name: &str, data: ColumnBuffer) -> ColumnWithName {
	ColumnWithName::new(Fragment::internal(name), data)
}

fn none_bools(values: impl IntoIterator<Item = Option<bool>>) -> ColumnBuffer {
	let values: Vec<Option<bool>> = values.into_iter().collect();
	let bits: Vec<bool> = values.iter().map(|v| v.unwrap_or(false)).collect();
	let validity: Vec<bool> = values.iter().map(|v| v.is_some()).collect();
	ColumnBuffer::bool_with_bitvec(bits, BooleanBuffer::from(validity))
}

fn answers(op: LogicalOp, left: ColumnBuffer, right: ColumnBuffer) -> Vec<String> {
	let frag = Fragment::internal("logic");
	let result = execute_logical_op(&column("l", left), &column("r", right), &frag, op).unwrap();
	(0..result.data().len()).map(|i| result.data().as_string(i)).collect()
}

#[test]
fn false_and_none_is_false_and_true_or_none_is_true() {
	// These two rows are the whole point of three valued logic: plain propagation would answer none for both.
	assert_eq!(answers(LogicalOp::And, none_bools([Some(false)]), none_bools([None])), vec!["false".to_string()]);
	assert_eq!(answers(LogicalOp::Or, none_bools([Some(true)]), none_bools([None])), vec!["true".to_string()]);
}

#[test]
fn every_other_none_row_of_and_or_and_xor_stays_none() {
	// Outside the two absorbing rows a none operand must never be resolved to a concrete answer.
	assert_eq!(answers(LogicalOp::And, none_bools([Some(true)]), none_bools([None])), vec!["none".to_string()]);
	assert_eq!(answers(LogicalOp::Or, none_bools([Some(false)]), none_bools([None])), vec!["none".to_string()]);
	assert_eq!(answers(LogicalOp::And, none_bools([None]), none_bools([None])), vec!["none".to_string()]);
	assert_eq!(answers(LogicalOp::Or, none_bools([None]), none_bools([None])), vec!["none".to_string()]);

	for left in [Some(true), Some(false), None] {
		assert_eq!(
			answers(LogicalOp::Xor, none_bools([left]), none_bools([None])),
			vec!["none".to_string()],
			"xor with a none operand must stay none"
		);
	}
}

#[test]
fn the_defined_rows_of_and_or_and_xor_keep_their_two_valued_answers() {
	// A kernel swap must not disturb the eight rows that hold no none at all.
	let l = ColumnBuffer::bool([true, true, false, false]);
	let r = ColumnBuffer::bool([true, false, true, false]);

	assert_eq!(answers(LogicalOp::And, l.clone(), r.clone()), vec!["true", "false", "false", "false"]);
	assert_eq!(answers(LogicalOp::Or, l.clone(), r.clone()), vec!["true", "true", "true", "false"]);
	assert_eq!(answers(LogicalOp::Xor, l, r), vec!["false", "true", "true", "false"]);
}

#[test]
fn a_logical_op_over_two_non_nullable_columns_answers_a_non_nullable_column() {
	// A spurious null buffer would change the declared type to an option and the wire bytes with it.
	let frag = Fragment::internal("logic");

	for op in [LogicalOp::And, LogicalOp::Or, LogicalOp::Xor] {
		let result = execute_logical_op(
			&column("l", ColumnBuffer::bool([true, false])),
			&column("r", ColumnBuffer::bool([false, false])),
			&frag,
			op,
		)
		.unwrap();

		assert_eq!(result.data().get_type(), ValueType::Boolean);
	}
}

#[test]
fn a_logical_op_where_one_side_is_nullable_with_no_none_rows_answers_a_nullable_column() {
	// An all valid null buffer must survive, otherwise a column silently drops its option type.
	let frag = Fragment::internal("logic");
	let nullable = ColumnBuffer::bool([true, false]).with_nulls(NullBuffer::new(BooleanBuffer::new_set(2)));
	assert_eq!(nullable.get_type(), ValueType::Option(Box::new(ValueType::Boolean)));

	for op in [LogicalOp::And, LogicalOp::Or, LogicalOp::Xor] {
		let result = execute_logical_op(
			&column("l", nullable.clone()),
			&column("r", ColumnBuffer::bool([true, true])),
			&frag,
			op,
		)
		.unwrap();

		assert_eq!(result.data().get_type(), ValueType::Option(Box::new(ValueType::Boolean)));
	}
}

#[test]
fn not_of_a_none_row_is_none_and_keeps_the_column_nullable() {
	// Dropping the mask would turn a none row into a concrete true or false.
	let frag = Fragment::internal("not");
	let input = column("v", none_bools([Some(true), Some(false), None]));

	let result = prefix_apply(&input, &PrefixOperator::Not(frag.clone()), &frag).unwrap();

	assert_eq!(result.data().get_type(), ValueType::Option(Box::new(ValueType::Boolean)));
	let answers: Vec<String> = (0..3).map(|i| result.data().as_string(i)).collect();
	assert_eq!(answers, vec!["false", "true", "none"]);
}

#[test]
fn not_of_a_non_nullable_column_answers_a_non_nullable_column() {
	// A mask attached where there was none changes the declared type to an option.
	let frag = Fragment::internal("not");
	let input = column("v", ColumnBuffer::bool([true, false]));

	let result = prefix_apply(&input, &PrefixOperator::Not(frag.clone()), &frag).unwrap();

	assert_eq!(result.data().get_type(), ValueType::Boolean);
	assert_eq!(result.data().as_string(0), "false");
	assert_eq!(result.data().as_string(1), "true");
}
