// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{buffer::ColumnBuffer, columns::Columns};
use reifydb_value::value::{Value, value_type::ValueType};

#[test]
fn a_bare_none_value_repeated_over_rows_builds_a_none_column() {
	// Otherwise an untyped none arrives as an any column and a typed branch beside it can not merge.
	let column = ColumnBuffer::from_many(Value::none(), 3);

	assert!(column.is_none(), "got {:?}", column.get_type());
	assert_eq!(column.len(), 3);
}

#[test]
fn a_bare_none_value_in_a_single_row_builds_a_none_column() {
	// Otherwise a one row batch of an untyped none is an any column and never retypes.
	let columns = Columns::single_row([("n", Value::none())]);

	assert!(columns.columns[0].is_none(), "got {:?}", columns.columns[0].get_type());
}

#[test]
fn a_none_value_that_carries_a_type_keeps_that_type() {
	// A typed none must never collapse into the untyped none column, otherwise its declared type is lost.
	let many = ColumnBuffer::from_many(Value::none_of(ValueType::Int4), 2);
	let single = Columns::single_row([("n", Value::none_of(ValueType::Utf8))]);

	assert!(!many.is_none());
	assert_eq!(many.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	assert!(!single.columns[0].is_none());
	assert_eq!(single.columns[0].get_type(), ValueType::Option(Box::new(ValueType::Utf8)));
}
