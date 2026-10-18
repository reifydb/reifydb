// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::property::{ColumnPropertyKind, ColumnSaturationStrategy},
		evaluate::TargetColumn,
	},
	value::{batch::empty_batch, column::factory},
};
use reifydb_evaluate::lower::LoweredExpr;
use reifydb_value::value::value_type::ValueType;

use crate::common::{
	Env, Frozen, and, arith, batch, between, boolean, cast, column, compare, field_access, number, rows, strings,
};

#[test]
fn a_field_on_a_non_variable_object_keeps_the_old_error() {
	// A field on any other object must still run the object first and raise the old error.
	let mut frozen = Frozen::inline(&[
		"RUNTIME_009 \"\" Field 'f' not found on variable '<expression>' []",
		"QUERY_001 \"missing\" column not found []",
	]);
	let env = Env::new();
	let input = batch(vec![factory::int4("a", [1, 2])]);

	for (object, code) in [("a", "RUNTIME_009"), ("missing", "QUERY_001")] {
		let expression = field_access(column(object), "f");
		let lowered = env.lowered(&expression, input.clone()).unwrap_err();
		let old = frozen.old(&env, &expression, input.clone()).unwrap_err();
		assert_eq!(lowered, old, "error of {object}.f");
		assert_eq!(lowered.code, code, "code of {object}.f");
	}
}

#[test]
fn a_field_on_a_non_variable_object_lowers_with_the_rest_of_its_expression() {
	// The field error must surface at run time after the between over a nullable column answers.
	let env = Env::new();
	let input = batch(vec![factory::int4_optional("a", [Some(5), None, Some(20)])]);
	let expression = and(
		between(column("a"), number("2"), number("10")),
		compare("==", field_access(number("1"), "y"), number("1")),
	);

	let lowered = env.lowered(&expression, input).unwrap_err();

	assert_eq!(lowered.code, "RUNTIME_009");
}

#[test]
fn a_new_schema_lowers_again_and_reads_the_right_column() {
	// Keeping the first lowering would read column 0 by position after the columns moved.
	let env = Env::new();
	let expression = LoweredExpr::new(column("a"), "test");

	let first = batch(vec![factory::int4("a", [1]), factory::int4("b", [2])]);
	let moved = batch(vec![factory::int4("b", [20]), factory::int4("a", [10])]);

	assert_eq!(strings(&expression.evaluate(&env.ctx(first)).unwrap()), vec!["1"]);
	assert_eq!(strings(&expression.evaluate(&env.ctx(moved)).unwrap()), vec!["10"]);
}

#[test]
fn the_same_schema_reuses_the_lowering_for_later_batches() {
	// A second batch with the same columns must read its own rows, not the rows of the first batch.
	let env = Env::new();
	let expression = LoweredExpr::new(column("a"), "test");

	let first = batch(vec![factory::int4("a", [1, 2])]);
	let second = batch(vec![factory::int4("a", [3, 4, 5])]);

	assert_eq!(strings(&expression.evaluate(&env.ctx(first)).unwrap()), vec!["1", "2"]);
	assert_eq!(strings(&expression.evaluate(&env.ctx(second)).unwrap()), vec!["3", "4", "5"]);
}

#[test]
fn a_lowering_error_is_returned_on_every_evaluate() {
	// Returning Ok after the first error would let a broken filter pass rows on the second batch.
	let mut frozen = Frozen::inline(&[
		"OPERATOR_005 \"1andtrue\" Cannot apply AND operator to number [\"AND is a logical operator that combines boolean values\", \"To convert numbers to boolean, use comparison operators like: value != 0\", \"For bitwise operations on integers, use the bitwise AND (&) operator instead\"]",
	]);
	let env = Env::new();
	let expression = LoweredExpr::new(and(number("1"), boolean("true")), "test");

	let first = expression.evaluate(&env.ctx(rows(2))).unwrap_err();
	let second = expression.evaluate(&env.ctx(rows(2))).unwrap_err();

	assert_eq!(first.code, second.code);
	assert_eq!(first.fragment.text(), second.fragment.text());
	assert_eq!(first.code, frozen.old(&env, &and(number("1"), boolean("true")), rows(2)).unwrap_err().code);
}

#[test]
fn an_expression_on_a_batch_with_no_columns_answers_row_count_rows() {
	// A map with no input runs on an empty batch, so its row count must come from the context, never the batch.
	let mut frozen = Frozen::inline(&["Field { name: \"1 + 2\", data_type: Int16 } [\"3\"]"]);
	let env = Env::new();
	let expression = arith("+", number("1"), number("2"));
	let mut ctx = env.ctx(empty_batch());
	ctx.row_count = 1;

	let lowered = LoweredExpr::new(expression.clone(), "test").evaluate(&ctx).unwrap();
	let old = frozen.answer(LoweredExpr::new(expression.clone(), "test").evaluate(&ctx)).unwrap();

	assert_eq!(lowered, old);
	assert_eq!(strings(&lowered), vec!["3"]);
}

#[test]
fn a_field_that_holds_a_none_is_widened_to_an_option() {
	// A map builds a batch from the field, and a non-nullable field holding a none is refused there.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"cast(a, type)\", data_type: Int8, nullable: true } [\"1\", \"none\"]",
	]);
	let env = Env::new();
	let expression = cast(column("a"), ValueType::Int1);
	let mut ctx = env.ctx(batch(vec![factory::int4("a", [1, 300])]));
	ctx.target = Some(TargetColumn::Partial {
		source_name: None,
		column_name: None,
		column_type: ValueType::Int1,
		properties: vec![ColumnPropertyKind::Saturation(ColumnSaturationStrategy::None)],
	});

	let lowered = LoweredExpr::new(expression.clone(), "test").evaluate(&ctx).unwrap();
	let old = frozen.answer(LoweredExpr::new(expression.clone(), "test").evaluate(&ctx)).unwrap();

	assert!(lowered.0.is_nullable());
	assert_eq!(lowered, old);
	assert_eq!(strings(&lowered), vec!["1", "none"]);
}
