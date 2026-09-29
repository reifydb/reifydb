// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::{batch::single_row, column::factory};
use reifydb_value::value::{Value, column_view::ColumnView, system_columns::column_view, value_type::ValueType};

#[test]
fn a_bare_none_value_repeated_over_rows_builds_a_none_column() {
	// Otherwise an untyped none arrives as an any column and a typed branch beside it can not merge.
	let column = factory::from_many("n", Value::none(), 3);
	let column = ColumnView::try_from(&column).unwrap();

	assert!(column.is_none(), "got {:?}", column.get_type());
	assert_eq!(column.len(), 3);
}

#[test]
fn a_bare_none_value_in_a_single_row_builds_a_none_column() {
	// Otherwise a one row batch of an untyped none is an any column and never retypes.
	let batch = single_row([("n", Value::none())]).unwrap();
	let column = column_view(&batch, "n").unwrap().unwrap();

	assert!(column.is_none(), "got {:?}", column.get_type());
}

#[test]
fn a_none_value_that_carries_a_type_keeps_that_type() {
	// A typed none must never collapse into the untyped none column, otherwise its declared type is lost.
	let many = factory::from_many("n", Value::none_of(ValueType::Int4), 2);
	let many = ColumnView::try_from(&many).unwrap();
	let single = single_row([("n", Value::none_of(ValueType::Utf8))]).unwrap();
	let single = column_view(&single, "n").unwrap().unwrap();

	assert!(!many.is_none());
	assert_eq!(many.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	assert!(!single.is_none());
	assert_eq!(single.get_type(), ValueType::Option(Box::new(ValueType::Utf8)));
}
