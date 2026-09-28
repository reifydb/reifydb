// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_value::value::{Value, value_type::ValueType};

#[test]
fn from_rows_with_no_none_keeps_the_plain_type() {
	// Every row gives a value, so the column must stay int4, never widen to Option(int4).
	let columns = Columns::from_rows(&["a"], &[vec![Value::Int4(1)], vec![Value::Int4(2)]]);

	assert_eq!(columns.columns[0].get_type(), ValueType::Int4);
}

#[test]
fn from_rows_with_a_none_row_is_optional() {
	// A none row must still make the column Option(int4), otherwise the none reads back as a value.
	let columns = Columns::from_rows(&["a"], &[vec![Value::Int4(1)], vec![Value::none()]]);

	assert_eq!(columns.columns[0].get_type(), ValueType::Option(Box::new(ValueType::Int4)));
}
