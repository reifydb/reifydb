// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, value_type::ValueType},
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
	let column = &result.frames[0].columns[0];
	assert_eq!(
		(column.data.get_type(), column.data.get_value(0)),
		(ValueType::Utf8, Value::Utf8(expected.to_string())),
		"{source}"
	);
}
