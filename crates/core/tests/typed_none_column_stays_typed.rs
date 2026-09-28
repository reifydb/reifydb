// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::value_type::ValueType;

fn extended(mut column: ColumnBuffer, other: ColumnBuffer) -> String {
	match column.extend(other) {
		Ok(()) => format!("ok {:?}", column.get_type()),
		Err(err) => format!("error {}", err.diagnostic().message),
	}
}

#[test]
fn a_boolean_column_of_only_nones_is_not_retyped_by_an_int4_column() {
	// Otherwise real boolean nones silently turn into int4 nones, and only the untyped none column may retype.
	let bool_first = extended(ColumnBuffer::none_typed(ValueType::Boolean, 2), ColumnBuffer::int4([1]));
	let int4_first = extended(ColumnBuffer::int4([1, 2]), ColumnBuffer::none_typed(ValueType::Boolean, 2));

	assert!(bool_first.contains("column type mismatch"), "boolean first: {bool_first}");
	assert!(int4_first.contains("column type mismatch"), "int4 first: {int4_first}");
}
