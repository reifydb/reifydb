// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	expression::{Expression, ParameterExpression, TypeExpression},
	value::{batch::batch as scalar_batch, column::factory},
};
use reifydb_evaluate::{lower::LoweredExpr, stack::Variable};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	params::Params,
	value::{Value, value_type::ValueType},
};

use crate::common::{
	Env, Frozen, and, batch, between, boolean, call, closure, column, compare, duration, field_access, frag, none,
	number, registry, rows, strings, symbols, temporal, text, values_of, variable,
};

#[test]
fn every_constant_shape_lowers_to_the_field_and_array_of_the_old_path() {
	// A constant typed differently from today changes every compare and arithmetic built on it.
	let mut frozen = Frozen::load("every_constant_shape_lowers_to_the_field_and_array_of_the_old_path");
	let env = Env::new();
	let shapes = [
		none(),
		boolean("true"),
		boolean("false"),
		number("42"),
		number("300"),
		number("70000"),
		number("5000000000"),
		number("100000000000000000000"),
		number("1.5"),
		text("abc"),
		temporal("2024-01-02"),
		temporal("2024-01-02T03:04:05"),
		temporal("03:04:05"),
		temporal("P1D"),
		duration("30s"),
		duration("3mo"),
	];

	for shape in shapes {
		let lowered = env.lowered(&shape, rows(3)).unwrap();
		let old = frozen.old(&env, &shape, rows(3)).unwrap();
		assert_eq!(lowered.0, old.0, "field of {shape:?}");
		assert_eq!(&lowered.1, &old.1, "array of {shape:?}");
	}
}

#[test]
fn a_constant_is_expanded_to_the_row_count_of_the_batch() {
	// A filter mask shorter than the batch would drop or panic on every row past the first.
	let env = Env::new();

	let result = env.lowered(&boolean("true"), rows(3)).unwrap();

	assert_eq!(result.1.len(), 3);
}

#[test]
fn an_invalid_constant_fails_with_the_same_error_as_the_old_path() {
	// A constant that does not parse must keep its error code and fragment, or goldens that pin it move.
	let mut frozen = Frozen::inline(&[
		"TEMPORAL_005 \"not\" invalid year value 'not' [\"valid examples: 2024, 1999, 2000\"]",
	]);
	let env = Env::new();
	let bad = temporal("not-a-date");

	let lowered = env.lowered(&bad, rows(1)).unwrap_err();
	let old = frozen.old(&env, &bad, rows(1)).unwrap_err();

	assert_eq!(lowered.code, old.code);
	assert_eq!(lowered.fragment.text(), old.fragment.text());
}

#[test]
fn a_variable_lowers_to_the_field_and_array_of_the_old_path() {
	// The field must be named after the variable and typed from its value, never after the stored column.
	let mut frozen = Frozen::load("a_variable_lowers_to_the_field_and_array_of_the_old_path");
	let env = Env::with_symbols(symbols(vec![
		("x", Variable::scalar(Value::Int4(7))),
		("t", Variable::scalar(Value::Utf8("abc".to_string()))),
		("1", Variable::scalar(Value::Int4(9))),
	]));

	for (text, name, expected) in [("$x", "x", "7"), ("$t", "t", "abc"), ("$1", "1", "9")] {
		let lowered = env.lowered(&variable(text), rows(3)).unwrap();
		let old = frozen.old(&env, &variable(text), rows(3)).unwrap();
		assert_eq!(lowered.0, old.0, "field of {text}");
		assert_eq!(&lowered.1, &old.1, "array of {text}");
		assert_eq!(lowered.0.name(), name, "field name of {text}");
		assert_eq!(strings(&lowered), vec![expected; 3], "values of {text}");
	}
}

#[test]
fn a_none_variable_lowers_like_a_literal_none_and_not_as_any() {
	// A none variable typed as any makes every compare on it error where a literal none answers none.
	let env = Env::with_symbols(symbols(vec![("n", Variable::scalar(Value::none()))]));

	let lowered = env.lowered(&variable("$n"), rows(3)).unwrap();
	let literal = env.lowered(&none(), rows(3)).unwrap();

	assert_eq!(lowered.0.data_type(), literal.0.data_type());
	assert_eq!(lowered.0.metadata(), literal.0.metadata());
	assert_eq!(lowered.0.is_nullable(), literal.0.is_nullable());
	assert_eq!(lowered.0.name(), "n");
	assert_eq!(strings(&lowered), vec!["none"; 3]);
}

#[test]
fn env_a_frame_and_a_closure_variable_fail_with_the_same_error_as_the_old_path() {
	// A frame or closure must fail, otherwise the filter would read its first row as a value.
	let mut frozen = Frozen::inline(&[
		"RUNTIME_002 \"\" Variable 'env' contains a dataframe and cannot be used directly in scalar expressions [\"Dataframes must be explicitly converted to scalar values before use in expressions\", \"Use only() for exactly 1 row × 1 column dataframes\", \"Use first() to take the first value from the first column\"]",
		"RUNTIME_002 \"\" Variable 'f' contains a dataframe and cannot be used directly in scalar expressions [\"Dataframes must be explicitly converted to scalar values before use in expressions\", \"Use only() for exactly 1 row × 1 column dataframes\", \"Use first() to take the first value from the first column\"]",
		"RUNTIME_002 \"\" Variable 'c' contains a dataframe and cannot be used directly in scalar expressions [\"Dataframes must be explicitly converted to scalar values before use in expressions\", \"Use only() for exactly 1 row × 1 column dataframes\", \"Use first() to take the first value from the first column\"]",
	]);
	let env = Env::with_symbols(symbols(vec![
		("f", Variable::columns(scalar_batch(vec![factory::int4("a", [1, 2])]).unwrap())),
		("c", closure()),
	]));

	for text in ["$env", "$f", "$c"] {
		let lowered = env.lowered(&variable(text), rows(3)).unwrap_err();
		let old = frozen.old(&env, &variable(text), rows(3)).unwrap_err();
		assert_eq!(lowered, old, "error of {text}");
		assert_eq!(lowered.code, "RUNTIME_002", "code of {text}");
	}
}

#[test]
fn an_undefined_variable_fails_with_variable_not_found_at_its_fragment() {
	// Without the variable fragment the error would point at nothing in the query.
	let mut frozen = Frozen::inline(&["RUNTIME_001 \"$x\" Variable 'x' is not defined []"]);
	let env = Env::new();

	let lowered = env.lowered(&variable("$x"), rows(3)).unwrap_err();
	let old = frozen.old(&env, &variable("$x"), rows(3)).unwrap_err();

	assert_eq!(lowered, old);
	assert_eq!(lowered.code, "RUNTIME_001");
	assert_eq!(lowered.fragment.text(), "$x");
}

#[test]
fn a_record_field_lowers_to_the_value_field_and_type_of_the_old_path() {
	// The value must come from row zero under the field's own name, otherwise the filter reads another value.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"a\", data_type: Int32 } [\"7\", \"7\", \"7\"]",
		"Field { name: \"b\", data_type: LargeUtf8 } [\"abc\", \"abc\", \"abc\"]",
	]);
	let env = Env::with_symbols(symbols(vec![(
		"r",
		Variable::columns(scalar_batch(vec![factory::int4("a", [7]), factory::utf8("b", ["abc"])]).unwrap()),
	)]));

	for (field, expected) in [("a", "7"), ("b", "abc")] {
		let expression = field_access(variable("$r"), field);
		let lowered = env.lowered(&expression, rows(3)).unwrap();
		let old = frozen.old(&env, &expression, rows(3)).unwrap();
		assert_eq!(lowered.0, old.0, "field of $r.{field}");
		assert_eq!(&lowered.1, &old.1, "array of $r.{field}");
		assert_eq!(lowered.0.name(), field, "field name of $r.{field}");
		assert_eq!(strings(&lowered), vec![expected; 3], "values of $r.{field}");
	}
}

#[test]
fn a_missing_record_field_fails_with_the_same_available_fields() {
	// The help must list every field of the otherwise the user cannot see what to read.
	let mut frozen = Frozen::inline(&["RUNTIME_009 \"\" Field 'c' not found on variable 'r' []"]);
	let env = Env::with_symbols(symbols(vec![(
		"r",
		Variable::columns(scalar_batch(vec![factory::int4("a", [7]), factory::utf8("b", ["abc"])]).unwrap()),
	)]));
	let expression = field_access(variable("$r"), "c");

	let lowered = env.lowered(&expression, rows(3)).unwrap_err();
	let old = frozen.old(&env, &expression, rows(3)).unwrap_err();

	assert_eq!(lowered, old);
	assert_eq!(lowered.code, "RUNTIME_009");
	assert_eq!(lowered.help.as_deref(), Some("Available fields: a, b"));
}

#[test]
fn a_field_on_a_one_field_record_or_a_closure_fails_with_no_available_fields() {
	// A one field record is a scalar, so it must fail as a value with no fields, exactly as a closure does.
	let mut frozen = Frozen::inline(&[
		"RUNTIME_009 \"\" Field 'a' not found on variable 's' []",
		"RUNTIME_009 \"\" Field 'a' not found on variable 'c' []",
	]);
	let env = Env::with_symbols(symbols(vec![
		("s", Variable::columns(scalar_batch(vec![factory::int4("a", [7])]).unwrap())),
		("c", closure()),
	]));

	for (text, name) in [("$s", "s"), ("$c", "c")] {
		let expression = field_access(variable(text), "a");
		let lowered = env.lowered(&expression, rows(3)).unwrap_err();
		let old = frozen.old(&env, &expression, rows(3)).unwrap_err();
		assert_eq!(lowered, old, "error of {text}.a");
		assert_eq!(lowered.code, "RUNTIME_009", "code of {text}.a");
		assert_eq!(
			lowered.help.as_deref(),
			Some(format!("The variable '{name}' has no fields").as_str()),
			"help of {text}.a"
		);
	}
}

#[test]
fn a_field_on_an_empty_record_or_a_none_at_row_zero_matches_the_old_path() {
	// A record without rows or with none at row zero must give none, never a panic or a default value.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"a\", data_type: Int32, nullable: true } [\"none\", \"none\", \"none\"]",
		"Field { name: \"a\", data_type: Int32, nullable: true } [\"none\", \"none\", \"none\"]",
	]);
	let env = Env::with_symbols(symbols(vec![
		(
			"e",
			Variable::columns(
				scalar_batch(vec![
					factory::int4("a", Vec::<i32>::new()),
					factory::int4("b", Vec::<i32>::new()),
				])
				.unwrap(),
			),
		),
		(
			"n",
			Variable::columns(
				scalar_batch(vec![factory::int4_optional("a", [None]), factory::int4("b", [1])])
					.unwrap(),
			),
		),
	]));

	for text in ["$e", "$n"] {
		let expression = field_access(variable(text), "a");
		let lowered = env.lowered(&expression, rows(3)).unwrap();
		let old = frozen.old(&env, &expression, rows(3)).unwrap();
		assert_eq!(lowered.0, old.0, "field of {text}.a");
		assert_eq!(&lowered.1, &old.1, "array of {text}.a");
		assert_eq!(strings(&lowered), vec!["none"; 3], "values of {text}.a");
	}
}

#[test]
fn a_field_on_a_for_iterator_or_an_undefined_variable_fails_as_the_old_path() {
	// Each variable kind must keep its own error, otherwise a defined loop variable reads as missing.
	let mut frozen = Frozen::inline(&[
		"RUNTIME_002 \"\" Variable 'i' contains a dataframe and cannot be used directly in scalar expressions [\"Dataframes must be explicitly converted to scalar values before use in expressions\", \"Use only() for exactly 1 row × 1 column dataframes\", \"Use first() to take the first value from the first column\"]",
		"RUNTIME_001 \"$u\" Variable 'u' is not defined []",
	]);
	let env = Env::with_symbols(symbols(vec![(
		"i",
		Variable::ForIterator {
			batch: scalar_batch(vec![factory::int4("a", [1, 2])]).unwrap(),
			index: 0,
		},
	)]));

	let iterator = field_access(variable("$i"), "a");
	let lowered = env.lowered(&iterator, rows(3)).unwrap_err();
	assert_eq!(lowered, frozen.old(&env, &iterator, rows(3)).unwrap_err());
	assert_eq!(lowered.code, "RUNTIME_002");

	let undefined = field_access(variable("$u"), "a");
	let lowered = env.lowered(&undefined, rows(3)).unwrap_err();
	assert_eq!(lowered, frozen.old(&env, &undefined, rows(3)).unwrap_err());
	assert_eq!(lowered.code, "RUNTIME_001");
	assert_eq!(lowered.fragment.text(), "$u");
}

#[test]
fn one_lowered_expr_keeps_the_first_variable_value_after_the_symbol_changes() {
	// The value must stay the one read at the first batch, exactly as the old path caches it.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"x\", data_type: Int32 } [\"1\", \"1\"]",
		"Field { name: \"x\", data_type: Int32 } [\"1\", \"1\"]",
	]);
	let mut env = Env::with_symbols(symbols(vec![("x", Variable::scalar(Value::Int4(1)))]));
	let lowered = LoweredExpr::new(variable("$x"), "test");

	assert_eq!(strings(&lowered.evaluate(&env.ctx(rows(2))).unwrap()), vec!["1", "1"]);
	assert_eq!(values_of(&frozen.recorded()), vec!["1", "1"]);

	env.symbols.set("x".to_string(), Variable::scalar(Value::Int4(2)), false).unwrap();

	assert_eq!(strings(&lowered.evaluate(&env.ctx(rows(2))).unwrap()), vec!["1", "1"]);
	assert_eq!(values_of(&frozen.recorded()), vec!["1", "1"]);
}

#[test]
fn a_parameter_lowers_with_the_rest_of_its_expression() {
	// A parameter must keep the between beside it answering per row over a nullable column.
	let env = Env::with_params(Params::from(vec![Value::Int4(99)]));
	let input = batch(vec![factory::int4_optional("a", [Some(5), None, Some(20)]), factory::int4("id", [1, 2, 3])]);
	let parameter = Expression::Parameter(ParameterExpression::Positional {
		fragment: frag("$1"),
	});
	let expression = and(between(column("a"), number("2"), number("10")), compare("!=", column("id"), parameter));

	let lowered = env.lowered(&expression, input).unwrap();

	assert_eq!(strings(&lowered), vec!["true", "none", "false"]);
}

#[test]
fn a_type_literal_lowers_with_the_rest_of_its_expression() {
	// A type literal must keep the between beside it answering per row over a nullable column.
	let env = Env::with_routines(registry(), RuntimeContext::testing(0, 0));
	let input = batch(vec![factory::int4_optional("a", [Some(5), None, Some(20)])]);
	let int4 = || {
		Expression::Type(TypeExpression {
			fragment: frag("int4"),
			ty: ValueType::Int4,
			constraint: None,
		})
	};
	let expression =
		and(between(column("a"), number("2"), number("10")), call("is::type", vec![column("a"), int4()]));

	let lowered = env.lowered(&expression, input).unwrap();

	assert_eq!(strings(&lowered), vec!["true", "false", "false"]);
}
