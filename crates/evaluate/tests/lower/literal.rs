// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::{batch::batch as scalar_batch, column::factory};
use reifydb_evaluate::{
	expression::{compile::compile_expression, context::CompileContext},
	lower::LoweredExpr,
	stack::Variable,
};
use reifydb_value::value::Value;

use crate::common::{
	Env, boolean, closure, duration, field_access, none, number, rows, strings, symbols, temporal, text, variable,
};

#[test]
fn every_constant_shape_lowers_to_the_field_and_array_of_the_old_path() {
	// A constant typed differently from today changes every compare and arithmetic built on it.
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
		let old = env.old(&shape, rows(3)).unwrap();
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
	let env = Env::new();
	let bad = temporal("not-a-date");

	let lowered = env.lowered(&bad, rows(1)).unwrap_err();
	let old = env.old(&bad, rows(1)).unwrap_err();

	assert_eq!(lowered.code, old.code);
	assert_eq!(lowered.fragment.text(), old.fragment.text());
}

#[test]
fn a_variable_lowers_to_the_field_and_array_of_the_old_path() {
	// The field must be named after the variable and typed from its value, never after the stored column.
	let env = Env::with_symbols(symbols(vec![
		("x", Variable::scalar(Value::Int4(7))),
		("t", Variable::scalar(Value::Utf8("abc".to_string()))),
		("n", Variable::scalar(Value::none())),
		("1", Variable::scalar(Value::Int4(9))),
	]));

	for (text, name, expected) in [("$x", "x", "7"), ("$t", "t", "abc"), ("$n", "n", "none"), ("$1", "1", "9")] {
		let lowered = env.lowered(&variable(text), rows(3)).unwrap();
		let old = env.old(&variable(text), rows(3)).unwrap();
		assert_eq!(lowered.0, old.0, "field of {text}");
		assert_eq!(&lowered.1, &old.1, "array of {text}");
		assert_eq!(lowered.0.name(), name, "field name of {text}");
		assert_eq!(strings(&lowered), vec![expected; 3], "values of {text}");
	}
}

#[test]
fn env_a_frame_and_a_closure_variable_fail_with_the_same_error_as_the_old_path() {
	// A frame or closure must fail, otherwise the filter would read its first row as a value.
	let env = Env::with_symbols(symbols(vec![
		("f", Variable::columns(scalar_batch(vec![factory::int4("a", [1, 2])]).unwrap())),
		("c", closure()),
	]));

	for text in ["$env", "$f", "$c"] {
		let lowered = env.lowered(&variable(text), rows(3)).unwrap_err();
		let old = env.old(&variable(text), rows(3)).unwrap_err();
		assert_eq!(lowered, old, "error of {text}");
		assert_eq!(lowered.code, "RUNTIME_002", "code of {text}");
	}
}

#[test]
fn an_undefined_variable_fails_with_variable_not_found_at_its_fragment() {
	// Without the variable fragment the error would point at nothing in the query.
	let env = Env::new();

	let lowered = env.lowered(&variable("$x"), rows(3)).unwrap_err();
	let old = env.old(&variable("$x"), rows(3)).unwrap_err();

	assert_eq!(lowered, old);
	assert_eq!(lowered.code, "RUNTIME_001");
	assert_eq!(lowered.fragment.text(), "$x");
}

#[test]
fn a_record_field_lowers_to_the_value_field_and_type_of_the_old_path() {
	// The value must come from row zero under the field's own name, otherwise the filter reads another value.
	let env = Env::with_symbols(symbols(vec![(
		"r",
		Variable::columns(scalar_batch(vec![factory::int4("a", [7]), factory::utf8("b", ["abc"])]).unwrap()),
	)]));

	for (field, expected) in [("a", "7"), ("b", "abc")] {
		let expression = field_access(variable("$r"), field);
		let lowered = env.lowered(&expression, rows(3)).unwrap();
		let old = env.old(&expression, rows(3)).unwrap();
		assert_eq!(lowered.0, old.0, "field of $r.{field}");
		assert_eq!(&lowered.1, &old.1, "array of $r.{field}");
		assert_eq!(lowered.0.name(), field, "field name of $r.{field}");
		assert_eq!(strings(&lowered), vec![expected; 3], "values of $r.{field}");
	}
}

#[test]
fn a_missing_record_field_fails_with_the_same_available_fields() {
	// The help must list every field of the record, otherwise the user cannot see what to read.
	let env = Env::with_symbols(symbols(vec![(
		"r",
		Variable::columns(scalar_batch(vec![factory::int4("a", [7]), factory::utf8("b", ["abc"])]).unwrap()),
	)]));
	let expression = field_access(variable("$r"), "c");

	let lowered = env.lowered(&expression, rows(3)).unwrap_err();
	let old = env.old(&expression, rows(3)).unwrap_err();

	assert_eq!(lowered, old);
	assert_eq!(lowered.code, "RUNTIME_009");
	assert_eq!(lowered.help.as_deref(), Some("Available fields: a, b"));
}

#[test]
fn a_field_on_a_one_field_record_or_a_closure_fails_with_no_available_fields() {
	// A one field record is a scalar, so it must fail as a value with no fields, exactly as a closure does.
	let env = Env::with_symbols(symbols(vec![
		("s", Variable::columns(scalar_batch(vec![factory::int4("a", [7])]).unwrap())),
		("c", closure()),
	]));

	for (text, name) in [("$s", "s"), ("$c", "c")] {
		let expression = field_access(variable(text), "a");
		let lowered = env.lowered(&expression, rows(3)).unwrap_err();
		let old = env.old(&expression, rows(3)).unwrap_err();
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
		let old = env.old(&expression, rows(3)).unwrap();
		assert_eq!(lowered.0, old.0, "field of {text}.a");
		assert_eq!(&lowered.1, &old.1, "array of {text}.a");
		assert_eq!(strings(&lowered), vec!["none"; 3], "values of {text}.a");
	}
}

#[test]
fn a_field_on_a_for_iterator_or_an_undefined_variable_fails_as_the_old_path() {
	// Each variable kind must keep its own error, otherwise a defined loop variable reads as missing.
	let env = Env::with_symbols(symbols(vec![(
		"i",
		Variable::ForIterator {
			batch: scalar_batch(vec![factory::int4("a", [1, 2])]).unwrap(),
			index: 0,
		},
	)]));

	let iterator = field_access(variable("$i"), "a");
	let lowered = env.lowered(&iterator, rows(3)).unwrap_err();
	assert_eq!(lowered, env.old(&iterator, rows(3)).unwrap_err());
	assert_eq!(lowered.code, "RUNTIME_002");

	let undefined = field_access(variable("$u"), "a");
	let lowered = env.lowered(&undefined, rows(3)).unwrap_err();
	assert_eq!(lowered, env.old(&undefined, rows(3)).unwrap_err());
	assert_eq!(lowered.code, "RUNTIME_001");
	assert_eq!(lowered.fragment.text(), "$u");
}

#[test]
fn one_lowered_expr_keeps_the_first_variable_value_after_the_symbol_changes() {
	// The value must stay the one read at the first batch, exactly as the old path caches it.
	let mut env = Env::with_symbols(symbols(vec![("x", Variable::scalar(Value::Int4(1)))]));
	let lowered = LoweredExpr::new(variable("$x"), "test");
	let old = compile_expression(
		&CompileContext {
			symbols: &env.symbols,
		},
		&variable("$x"),
	)
	.unwrap();

	assert_eq!(strings(&lowered.evaluate(&env.ctx(rows(2))).unwrap()), vec!["1", "1"]);
	assert_eq!(strings(&old.execute(&env.ctx(rows(2))).unwrap()), vec!["1", "1"]);

	env.symbols.set("x".to_string(), Variable::scalar(Value::Int4(2)), false).unwrap();

	assert_eq!(strings(&lowered.evaluate(&env.ctx(rows(2))).unwrap()), vec!["1", "1"]);
	assert_eq!(strings(&old.execute(&env.ctx(rows(2))).unwrap()), vec!["1", "1"]);
}
