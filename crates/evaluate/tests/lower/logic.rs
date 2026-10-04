// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::BooleanBuffer;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory;

use crate::common::{Env, and, batch, boolean, column, none, not, number, or, rows, strings};

fn operands() -> RecordBatch {
	let left = [Some(true), Some(true), Some(true), Some(false), Some(false), Some(false), None, None, None];
	let right = [Some(true), Some(false), None, Some(true), Some(false), None, Some(true), Some(false), None];
	batch(vec![optional_bools("l", &left), optional_bools("r", &right)])
}

fn optional_bools(name: &str, values: &[Option<bool>]) -> (FieldRef, ArrayRef) {
	let bits: Vec<bool> = values.iter().map(|v| v.unwrap_or(false)).collect();
	let validity: Vec<bool> = values.iter().map(|v| v.is_some()).collect();
	factory::bool_with_bitvec(name, bits, BooleanBuffer::from(validity))
}

#[test]
fn and_follows_the_kleene_truth_table_with_none() {
	// false and none must be false and true and none must be none, or filters keep or drop the wrong rows.
	let env = Env::new();

	let result = env.lowered(&and(column("l"), column("r")), operands()).unwrap();

	assert_eq!(strings(&result), vec!["true", "false", "none", "false", "false", "false", "none", "false", "none"]);
}

#[test]
fn or_follows_the_kleene_truth_table_with_none() {
	// true or none must be true and false or none must be none, or filters keep or drop the wrong rows.
	let env = Env::new();

	let result = env.lowered(&or(column("l"), column("r")), operands()).unwrap();

	assert_eq!(strings(&result), vec!["true", "true", "true", "true", "false", "none", "true", "none", "none"]);
}

#[test]
fn not_keeps_none_and_flips_the_rest() {
	// not none must stay none, never become true, or a negated filter would keep unknown rows.
	let env = Env::new();

	let result = env.lowered(&not(column("l")), operands()).unwrap();

	assert_eq!(strings(&result), vec!["false", "false", "false", "true", "true", "true", "none", "none", "none"]);
}

#[test]
fn the_lowered_truth_tables_match_the_old_path_row_for_row() {
	// Any row where the two paths disagree is a filter result that changes under the new path.
	let env = Env::new();

	for expression in [and(column("l"), column("r")), or(column("l"), column("r")), not(column("l"))] {
		let lowered = env.lowered(&expression, operands()).unwrap();
		let old = env.old(&expression, operands()).unwrap();
		assert_eq!(strings(&lowered), strings(&old), "{expression:?}");
	}
}

#[test]
fn an_untyped_none_operand_acts_as_a_none_bool() {
	// A bare none literal must take part in three valued logic, not raise a type error.
	let env = Env::new();
	let cases = [
		(and(none(), boolean("true")), "none"),
		(and(boolean("false"), none()), "false"),
		(or(none(), boolean("true")), "true"),
		(or(boolean("false"), none()), "none"),
		(not(none()), "none"),
	];

	for (expression, expected) in cases {
		let lowered = env.lowered(&expression, rows(1)).unwrap();
		let old = env.old(&expression, rows(1)).unwrap();
		assert_eq!(strings(&lowered), vec![expected], "{expression:?}");
		assert_eq!(strings(&lowered), strings(&old), "{expression:?}");
	}
}

#[test]
fn a_number_operand_fails_with_the_same_error_as_the_old_path() {
	// The type error must keep its code and fragment, or goldens that pin it move.
	let env = Env::new();

	for expression in [and(number("1"), boolean("true")), or(boolean("false"), number("1")), not(number("1"))] {
		let lowered = env.lowered(&expression, rows(1)).unwrap_err();
		let old = env.old(&expression, rows(1)).unwrap_err();
		assert_eq!(lowered.code, old.code, "{expression:?}");
		assert_eq!(lowered.fragment.text(), old.fragment.text(), "{expression:?}");
	}
}

#[test]
fn a_type_error_on_the_right_is_raised_even_when_the_left_decides() {
	// The type check runs at lowering, so a deciding left side no longer hides a bad right side.
	let env = Env::new();

	let cases = [
		(or(boolean("true"), number("1")), or(number("1"), boolean("true")), "true"),
		(and(boolean("false"), number("1")), and(number("1"), boolean("false")), "false"),
	];

	for (expression, swapped, today) in cases {
		let lowered = env.lowered(&expression, rows(1)).unwrap_err();
		let old_swapped = env.old(&swapped, rows(1)).unwrap_err();
		assert_eq!(lowered.code, old_swapped.code, "{expression:?}");
		assert_eq!(strings(&env.old(&expression, rows(1)).unwrap()), vec![today], "{expression:?}");
	}
}

#[test]
fn a_typed_operand_that_is_all_none_fails_at_lowering() {
	// The type check runs on the field, so an int column errors even when this batch holds only none.
	let env = Env::new();
	let input = batch(vec![factory::int4_optional("n", [None, None])]);

	let lowered = env.lowered(&and(column("n"), boolean("true")), input.clone()).unwrap_err();
	let typed = env.lowered(&and(number("1"), boolean("true")), input).unwrap_err();

	assert_eq!(lowered.code, typed.code);
}
