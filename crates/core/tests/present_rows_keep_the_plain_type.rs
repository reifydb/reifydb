// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::batch::from_rows;
use reifydb_value::value::{Value, system_columns::column_view, value_type::ValueType};

#[test]
fn from_rows_with_no_none_keeps_the_plain_type() {
	// Every row gives a value, so the column must stay int4, never widen to Option(int4).
	let batch = from_rows(&["a"], &[vec![Value::Int4(1)], vec![Value::Int4(2)]]).unwrap();

	assert_eq!(column_view(&batch, "a").unwrap().unwrap().get_type(), ValueType::Int4);
}

#[test]
fn from_rows_with_a_none_row_is_optional() {
	// A none row must still make the column Option(int4), otherwise the none reads back as a value.
	let batch = from_rows(&["a"], &[vec![Value::Int4(1)], vec![Value::none()]]).unwrap();

	assert_eq!(column_view(&batch, "a").unwrap().unwrap().get_type(), ValueType::Option(Box::new(ValueType::Int4)));
}
