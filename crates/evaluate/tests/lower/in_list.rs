// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory;

use crate::common::{Env, Frozen, and, batch, between, column, in_list, none, number, strings, text};

fn int_columns() -> Vec<(FieldRef, ArrayRef)> {
	vec![factory::int4("v", [1, 2, 3]), factory::int4_optional("n", [Some(1), None, Some(3)])]
}

#[test]
fn every_in_shape_matches_the_old_path() {
	// In is our equal folded with or, so any field, value or error that differs from today moves every golden on
	// it.
	let mut frozen = Frozen::load("every_in_shape_matches_the_old_path");
	let env = Env::new();
	let input = batch(int_columns());
	let shapes = [
		in_list(column("v"), vec![number("1"), number("3")], false),
		in_list(column("v"), vec![number("1"), number("3")], true),
		in_list(column("v"), vec![number("2")], false),
		in_list(column("n"), vec![number("1"), number("2")], false),
		in_list(column("n"), vec![number("1"), number("2")], true),
		in_list(column("v"), vec![number("1"), none()], false),
		in_list(column("v"), vec![number("1"), none()], true),
		in_list(column("v"), vec![number("300"), number("70000")], false),
		in_list(column("v"), vec![], false),
		in_list(column("n"), vec![], true),
	];
	for expression in shapes {
		let lowered = env.lowered(&expression, input.clone()).unwrap();
		let old = frozen.old(&env, &expression, input.clone()).unwrap();
		assert_eq!(lowered.0, old.0, "field of {expression:?}");
		assert_eq!(strings(&lowered), strings(&old), "values of {expression:?}");
	}
}

#[test]
fn in_over_text_matches_the_old_path() {
	// Text compares byte for byte, so a lowered in must keep that and not fold case or trim.
	let mut frozen =
		Frozen::inline(&["Field { name: \"in\", data_type: Boolean } [\"true\", \"false\", \"false\"]"]);
	let env = Env::new();
	let input = batch(vec![factory::utf8("s", ["a", "b", "A"])]);
	let expression = in_list(column("s"), vec![text("a"), text("c")], false);

	let lowered = env.lowered(&expression, input.clone()).unwrap();
	let old = frozen.old(&env, &expression, input).unwrap();

	assert_eq!(lowered.0, old.0);
	assert_eq!(strings(&lowered), vec!["true", "false", "false"]);
	assert_eq!(strings(&lowered), strings(&old));
}

#[test]
fn in_with_an_item_of_another_type_errors_like_the_old_path() {
	// The mismatch must keep the code of today's equal, or goldens that pin it move.
	let mut frozen = Frozen::inline(&[
		"OPERATOR_022 \"in\" Cannot apply '==' operator to Int4 and Utf8 [\"Left operand is of type: Int4\", \"Right operand is of type: Utf8\", \"Equality comparison is only supported between compatible types\"]",
	]);
	let env = Env::new();
	let input = batch(int_columns());
	let expression = in_list(column("v"), vec![text("x")], false);

	let lowered = env.lowered(&expression, input.clone()).unwrap_err();
	let old = frozen.old(&env, &expression, input).unwrap_err();

	assert_eq!(lowered.code, old.code);
	assert_eq!(lowered.fragment.text(), old.fragment.text());
}

#[test]
fn in_lowers_with_the_rest_of_its_expression() {
	// An in must keep the between beside it answering per row over a nullable column, never a type error.
	let env = Env::new();
	let input = batch(int_columns());
	let expression = and(
		between(column("n"), number("0"), number("10")),
		in_list(column("v"), vec![number("1"), number("2")], false),
	);

	let lowered = env.lowered(&expression, input).unwrap();

	assert_eq!(strings(&lowered), vec!["true", "none", "false"]);
}
