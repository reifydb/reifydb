// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{ArrayRef, Int32Array};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::FieldRef;
use reifydb_core::value::column::{builder::ColumnBuilder, factory, nulls::with_nulls};
use reifydb_value::value::{Value, column_view::ColumnView, value_type::ValueType};

fn option_of(inner: (FieldRef, ArrayRef), defined: &[bool]) -> (FieldRef, ArrayRef) {
	with_nulls(inner, NullBuffer::new(BooleanBuffer::from(defined.to_vec()))).unwrap()
}

fn optional_int4() -> ValueType {
	ValueType::Option(Box::new(ValueType::Int4))
}

fn rows(column: &(FieldRef, ArrayRef)) -> Vec<Value> {
	ColumnView::try_from(column).unwrap().iter().collect()
}

fn stored(column: &(FieldRef, ArrayRef)) -> Vec<i32> {
	column.1.as_any().downcast_ref::<Int32Array>().expect("expected an int4 array").values().to_vec()
}

#[test]
fn two_null_layers_become_one_nullable_layer_with_rows_anded() {
	// A row that is none in any layer must be none in the single layer, and the values under it must stay exactly.
	let column = option_of(option_of(factory::int4("c", [7, 0, 9]), &[true, false, true]), &[true, true, false]);
	assert_eq!(
		ColumnView::try_from(&column).unwrap().get_type(),
		optional_int4(),
		"a layered column must carry exactly one option layer"
	);
	assert_eq!(
		rows(&column),
		vec![Value::Int4(7), Value::none_of(ValueType::Int4), Value::none_of(ValueType::Int4)]
	);
	assert_eq!(stored(&column), vec![7, 0, 9]);
}

#[test]
fn three_null_layers_become_one_nullable_layer_with_rows_anded() {
	// Every layer must be ANDed into the single layer, not only the outer two.
	let column = option_of(
		option_of(
			option_of(factory::int4("c", [1, 2, 3, 4]), &[true, true, true, false]),
			&[true, true, false, true],
		),
		&[false, true, true, true],
	);
	assert_eq!(
		ColumnView::try_from(&column).unwrap().get_type(),
		optional_int4(),
		"a layered column must carry exactly one option layer"
	);
	assert_eq!(
		rows(&column),
		vec![
			Value::none_of(ValueType::Int4),
			Value::Int4(2),
			Value::none_of(ValueType::Int4),
			Value::none_of(ValueType::Int4)
		]
	);
	assert_eq!(stored(&column), vec![1, 2, 3, 4]);
}

#[test]
fn a_builder_for_a_nested_option_type_builds_one_nullable_layer() {
	// A nested option type must build one layer, and a leading none must stay a null row in it.
	let mut builder = ColumnBuilder::with_capacity(ValueType::Option(Box::new(optional_int4())), 3);
	builder.push_none();
	builder.push_value(Value::Int4(2));
	builder.push_value(Value::none_of(ValueType::Int4));
	let column = builder.finish("c");
	assert_eq!(
		ColumnView::try_from(&column).unwrap().get_type(),
		optional_int4(),
		"the built column must carry exactly one option layer"
	);
	assert_eq!(
		rows(&column),
		vec![Value::none_of(ValueType::Int4), Value::Int4(2), Value::none_of(ValueType::Int4)]
	);
	assert_eq!(stored(&column), vec![0, 2, 0]);
}
