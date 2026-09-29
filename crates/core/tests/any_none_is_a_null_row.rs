// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::builder::ColumnBuilder;
use reifydb_value::value::{
	Value,
	column_view::{ColumnView, ViewData},
	value_type::ValueType,
};

fn any_column_with_a_none_between_values() -> (FieldRef, ArrayRef) {
	let mut builder = ColumnBuilder::with_capacity(ValueType::Option(Box::new(ValueType::Any)), 3);
	builder.push_value(Value::Any(Box::new(Value::Int4(1))));
	builder.push_none();
	builder.push_value(Value::Any(Box::new(Value::Utf8("x".to_string()))));
	builder.finish("c")
}

#[test]
fn a_none_pushed_into_an_any_column_is_a_null_row_with_no_encoded_value() {
	// Otherwise the none is spelled twice: a null bit over an encoded Value::none(), the B3 bug.
	let column = any_column_with_a_none_between_values();
	let column = ColumnView::try_from(&column).unwrap();
	let ViewData::Any {
		container,
		..
	} = &column.data
	else {
		panic!("expected an any column, got {:?}", column.get_type());
	};

	assert!(container.is_null(1));
	assert_eq!(container.value_length(1), 0, "the none row must carry no encoded value");
	assert_eq!(column.get_type(), ValueType::Option(Box::new(ValueType::Any)));
}

#[test]
fn reads_of_an_any_column_with_a_null_row_never_decode_the_empty_row() {
	// An empty null row that reaches the any decoder panics, so serde, equality and value reads must skip it.
	let column = any_column_with_a_none_between_values();

	let bytes = postcard::to_allocvec(&column).unwrap();
	let back: ColumnBuffer = postcard::from_bytes(&bytes).unwrap();
	let json: ColumnBuffer = serde_json::from_str(&serde_json::to_string(&column).unwrap()).unwrap();

	assert_eq!(back, column);
	assert_eq!(json, column);
	assert_eq!(column.get_value(0), Value::Any(Box::new(Value::Int4(1))));
	assert_eq!(column.get_value(1), Value::none());
	assert_eq!(column.get_value(2), Value::Any(Box::new(Value::Utf8("x".to_string()))));
}
