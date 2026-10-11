// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, value_type::ValueType},
};

// A cast whose target is not a plain type name used to unwrap its way into a panic. The panic ran on the
// statement's worker thread and took the whole process down, so a single malformed statement from any
// client ended the database for every other client. These statements must come back as errors instead.
//
// The target of a cast is any type the DDL accepts (a bare name, decimal(p, s), utf8(n), Option(T)). The tests below
// pin the reported diagnostic, not just the fact that something was reported, so that a rewrite which starts accepting
// Option(int4) or which reports a vaguer error has to change them on purpose rather than by accident.

#[test]
fn a_cast_to_an_option_type_keeps_none_and_types_the_column_option() {
	// A cast to Option(T) must type the column Option(T) for none and for a value, and a nested Option stays an
	// error.
	let t = TestEngine::new();

	for (rql, expected) in [
		("MAP { value: cast(none, Option(int4)) }", Value::none_of(ValueType::Int4)),
		("MAP { value: cast(5, Option(int4)) }", Value::Int4(5)),
	] {
		let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
		if let Some(err) = result.error {
			panic!("{rql}: the cast must succeed, got {:?}", err.diagnostic());
		}
		let column =
			result.frames[0].column("value").unwrap().unwrap_or_else(|| panic!("{rql}: no value column"));
		assert_eq!(
			(column.get_type(), column.get_value(0)),
			(ValueType::Option(Box::new(ValueType::Int4)), expected),
			"{rql}"
		);
	}

	let err = t.query_err("MAP { value: cast(none, Option(Option(int4))) }");
	assert!(err.contains("AST_020"), "a cast to a nested Option must report AST_020, got: {err}");
}

#[test]
fn a_cast_to_a_literal_reports_an_error_naming_the_literal() {
	let t = TestEngine::new();

	let err = t.query_err("MAP { value: cast(1, 2) }");

	assert!(err.contains("AST_003"), "a cast whose target is a literal must report AST_003, got: {err}");
	assert!(err.contains("found `2`"), "the diagnostic must point the reader at the literal target, got: {err}");
}

#[test]
fn a_cast_with_a_single_argument_reports_the_shape_it_wanted() {
	let t = TestEngine::new();

	let err = t.query_err("MAP { value: cast(1) }");

	assert!(err.contains("AST_005"), "a cast missing its target type must report AST_005, got: {err}");
	assert!(
		err.contains("cast(value, type)"),
		"the diagnostic must spell out the accepted form so the caller can fix the statement, got: {err}"
	);
}

#[test]
fn a_well_formed_cast_still_works() {
	// The guards above must not reject the shape the language actually supports.
	let t = TestEngine::new();

	let frames = t.query("MAP { value: cast(1, int8) }");

	assert_eq!(TestEngine::row_count(&frames), 1);
}
