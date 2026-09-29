// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, column_view::ColumnView, system_columns::user_columns, value_type::ValueType},
};

#[test]
fn a_closure_parameter_typed_with_a_parameterised_type_parses_and_binds() {
	// A UDF accepts Option(int4) and utf8(10) parameters, so a closure with them must never fail to parse.
	let t = TestEngine::new();

	for (param_type, arg, expected) in [
		(
			"Option(int4)",
			"none",
			Value::None {
				inner: ValueType::Int4,
			},
		),
		("utf8(10)", "'hi'", Value::Utf8("hi".to_string())),
	] {
		let rql = format!("let $f = ($x: {param_type}) {{ $x }}; map {{ v: $f({arg}) }}");
		let result = t.inner().query_as(TestEngine::identity(), &rql, Params::None);

		if let Some(err) = result.error {
			panic!("{param_type}: the closure must parse and run, got {:?}", err.diagnostic());
		}
		let (field, array) = user_columns(&result.frames[0].batch).next().unwrap();
		assert_eq!(
			ColumnView::try_from((array, field.as_ref())).unwrap().get_value(0),
			expected,
			"{param_type}"
		);
	}
}
