// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, column_view::ColumnView, system_columns::user_columns, value_type::ValueType},
};

#[test]
fn a_decimal_cast_to_utf8_gives_its_text() {
	// Every fixed-width number casts to text, so an arbitrary-precision number must never be the exception.
	let t = TestEngine::new();

	let (source, expected) = ("cast('1.5', decimal)", "1.5000000000");
	let result =
		t.inner().query_as(TestEngine::identity(), &format!("map {{ v: cast({source}, utf8) }}"), Params::None);

	if let Some(err) = result.error {
		panic!("{source}: the cast to utf8 must succeed, got {:?}", err.diagnostic());
	}
	let (field, array) = user_columns(&result.frames[0].batch).next().unwrap();
	let column = ColumnView::try_from((array, field.as_ref())).unwrap();
	assert_eq!(
		(column.get_type(), column.get_value(0)),
		(ValueType::Utf8, Value::Utf8(expected.to_string())),
		"{source}"
	);
}
