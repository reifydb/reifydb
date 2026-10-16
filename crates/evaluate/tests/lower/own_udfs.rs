// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	expression::{Expression, SumTypeConstructorExpression},
	value::column::factory,
};
use reifydb_value::value::Value;

use crate::common::{
	Env, and, batch, between, boolean, column, contains, frag, is_variant, none, number, strings, xor,
};

#[test]
fn xor_follows_the_old_truth_table_with_none() {
	// Xor has no DataFusion twin, so ours must answer every pair, none included, exactly as today.
	let env = Env::new();
	let values = [boolean("true"), boolean("false"), none()];
	for left in &values {
		for right in &values {
			let expression = xor(left.clone(), right.clone());
			let lowered = env.lowered(&expression, batch(vec![factory::int4("r", [1])])).unwrap();
			let old = env.old(&expression, batch(vec![factory::int4("r", [1])])).unwrap();
			assert_eq!(lowered.0, old.0, "field of {expression:?}");
			assert_eq!(strings(&lowered), strings(&old), "values of {expression:?}");
		}
	}
}

#[test]
fn xor_over_columns_matches_the_old_path() {
	// Columns run through the same kernel, so the per-row answers and the field must equal today's.
	let env = Env::new();
	let input = batch(vec![
		factory::bool("l", [true, true, false, false]),
		factory::bool("r", [true, false, true, false]),
	]);
	let expression = xor(column("l"), column("r"));

	let lowered = env.lowered(&expression, input.clone()).unwrap();

	assert_eq!(lowered, env.old(&expression, input).unwrap());
	assert_eq!(strings(&lowered), vec!["false", "true", "true", "false"]);
}

#[test]
fn xor_over_a_non_boolean_errors_like_the_old_path() {
	// The type check runs at lowering, so the code must still be today's.
	let env = Env::new();
	let input = batch(vec![factory::int4("v", [1])]);
	let expression = xor(column("v"), boolean("true"));

	let lowered = env.lowered(&expression, input.clone()).unwrap_err();

	assert_eq!(lowered.code, env.old(&expression, input).unwrap_err().code);
}

#[test]
fn contains_matches_the_old_path_on_list_rows() {
	// A row's list must hold every item, so the and over items and the false for non-lists must stay as today.
	let env = Env::new();
	let input = batch(vec![factory::any(
		"l",
		[Value::List(vec![Value::Int4(1), Value::Int4(2)]), Value::List(vec![Value::Int4(2)]), Value::Int4(1)],
	)]);
	for expression in [
		contains(column("l"), vec![number("1")]),
		contains(column("l"), vec![number("1"), number("2")]),
		contains(column("l"), vec![]),
	] {
		let lowered = env.lowered(&expression, input.clone()).unwrap();
		let old = env.old(&expression, input.clone()).unwrap();
		assert_eq!(lowered.0, old.0, "field of {expression:?}");
		assert_eq!(strings(&lowered), strings(&old), "values of {expression:?}");
	}
}

#[test]
fn is_variant_reads_the_tag_column_like_the_old_path() {
	// A variant test reads the hidden tag column; a missing or nullable tag must answer none as today.
	let env = Env::new();
	let tagged = batch(vec![factory::uint1("shape_tag", [0, 1, 0])]);
	let untagged = batch(vec![factory::int4("other", [1, 2, 3])]);
	let expression = is_variant("shape", Some(0));
	for input in [tagged, untagged] {
		let lowered = env.lowered(&expression, input.clone()).unwrap();
		let old = env.old(&expression, input).unwrap();
		assert_eq!(lowered.0, old.0);
		assert_eq!(strings(&lowered), strings(&old));
	}
}

#[test]
fn is_variant_without_a_known_enum_errors_like_the_old_path() {
	// The tag is resolved at plan time, so a missing one must keep today's CA_103.
	let env = Env::new();
	let input = batch(vec![factory::uint1("shape_tag", [0])]);
	let expression = is_variant("shape", None);

	assert_eq!(env.lowered(&expression, input.clone()).unwrap_err().code, "CA_103");
	assert_eq!(env.old(&expression, input).unwrap_err().code, "CA_103");
}

#[test]
fn a_sum_type_constructor_inside_an_expression_errors_like_the_old_path() {
	// A variant can never be a value inside an expression, so the lowered path must refuse it with CA_102.
	let env = Env::new();
	let input = batch(vec![factory::int4("v", [1])]);
	let expression = Expression::SumTypeConstructor(SumTypeConstructorExpression {
		namespace: frag("ns"),
		sumtype_name: frag("shape"),
		variant_name: frag("circle"),
		columns: vec![],
		fragment: frag("circle"),
	});

	assert_eq!(env.lowered(&expression, input.clone()).unwrap_err().code, "CA_102");
	assert_eq!(env.old(&expression, input).unwrap_err().code, "CA_102");
}

#[test]
fn every_own_udf_kind_lowers_with_the_rest_of_its_expression() {
	// A kind left unclaimed sends the whole and to the old path, where between over a nullable column errors.
	let env = Env::new();
	let input = batch(vec![
		factory::int4_optional("n", [Some(5), None]),
		factory::bool("b", [true, false]),
		factory::uint1("shape_tag", [0, 1]),
		factory::any("l", [Value::List(vec![Value::Int4(1)]), Value::List(vec![])]),
	]);
	let guard = || between(column("n"), number("0"), number("10"));
	for kind in [
		xor(column("b"), boolean("true")),
		contains(column("l"), vec![number("1")]),
		is_variant("shape", Some(0)),
	] {
		let expression = and(guard(), kind.clone());
		let lowered = env.lowered(&expression, input.clone()).unwrap();
		assert_eq!(strings(&lowered)[0], strings(&env.lowered(&kind, input.clone()).unwrap())[0], "{kind:?}");
	}
}

#[test]
fn a_variant_error_wins_over_a_missing_column_anywhere_in_the_expression() {
	// The old compiler raised variant errors before any row ran, so a missing column next to one must not win.
	let env = Env::new();
	let input = batch(vec![factory::int4("v", [1])]);
	let variant = Expression::SumTypeConstructor(SumTypeConstructorExpression {
		namespace: frag("ns"),
		sumtype_name: frag("shape"),
		variant_name: frag("circle"),
		columns: vec![],
		fragment: frag("circle"),
	});
	let expression = and(column("missing"), xor(boolean("true"), variant));

	assert_eq!(env.lowered(&expression, input.clone()).unwrap_err().code, "CA_102");
	assert_eq!(env.old(&expression, input).unwrap_err().code, "CA_102");
}
