// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int32Array;
use arrow_buffer::BooleanBuffer;
use reifydb_core::value::column::{buffer::ColumnBuffer, builder::ColumnBuilder};
use reifydb_value::value::{Value, frame::data::FrameColumnData, value_type::ValueType};

fn option_of(inner: FrameColumnData, defined: &[bool]) -> FrameColumnData {
	FrameColumnData::Option {
		inner: Box::new(inner),
		bitvec: BooleanBuffer::from(defined.to_vec()),
	}
}

fn optional_int4() -> ValueType {
	ValueType::Option(Box::new(ValueType::Int4))
}

fn rows(column: &ColumnBuffer) -> Vec<Value> {
	(0..column.len()).map(|row| column.get_value(row)).collect()
}

#[test]
fn a_depth_two_frame_becomes_one_nullable_layer_with_rows_anded() {
	// A row that is none in any layer must be none in the single layer, and the values under it must stay exactly.
	let frame = option_of(
		option_of(FrameColumnData::Int4(Int32Array::from(vec![7, 0, 9])), &[true, false, true]),
		&[true, true, false],
	);
	let column = ColumnBuffer::from(frame);
	assert_eq!(column.get_type(), optional_int4(), "a converted frame must carry exactly one option layer");
	assert_eq!(
		rows(&column),
		vec![Value::Int4(7), Value::none_of(ValueType::Int4), Value::none_of(ValueType::Int4)]
	);
	assert_eq!(
		serde_json::to_string(&column).unwrap(),
		"{\"Option\":{\"inner\":{\"Int4\":{\"data\":[7,0,9]}},\"bitvec\":{\"bits\":[1],\"len\":3}}}"
	);
}

#[test]
fn a_depth_three_frame_becomes_one_nullable_layer_with_rows_anded() {
	// Every layer must be ANDed into the single layer, not only the outer two.
	let frame = option_of(
		option_of(
			option_of(
				FrameColumnData::Int4(Int32Array::from(vec![1, 2, 3, 4])),
				&[true, true, true, false],
			),
			&[true, true, false, true],
		),
		&[false, true, true, true],
	);
	let column = ColumnBuffer::from(frame);
	assert_eq!(column.get_type(), optional_int4(), "a converted frame must carry exactly one option layer");
	assert_eq!(
		rows(&column),
		vec![
			Value::none_of(ValueType::Int4),
			Value::Int4(2),
			Value::none_of(ValueType::Int4),
			Value::none_of(ValueType::Int4)
		]
	);
	assert_eq!(
		serde_json::to_string(&column).unwrap(),
		"{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,2,3,4]}},\"bitvec\":{\"bits\":[2],\"len\":4}}}"
	);
}

#[test]
fn a_builder_for_a_nested_option_type_builds_one_nullable_layer() {
	// A nested option type must build one layer, and a leading none must never reach an unreachable arm.
	let mut builder = ColumnBuilder::with_capacity(ValueType::Option(Box::new(optional_int4())), 3);
	builder.push_none();
	builder.push_value(Value::Int4(2));
	builder.push_value(Value::none_of(ValueType::Int4));
	let column = builder.finish();
	assert_eq!(column.get_type(), optional_int4(), "the built column must carry exactly one option layer");
	assert_eq!(
		rows(&column),
		vec![Value::none_of(ValueType::Int4), Value::Int4(2), Value::none_of(ValueType::Int4)]
	);
	assert_eq!(
		serde_json::to_string(&column).unwrap(),
		"{\"Option\":{\"inner\":{\"Int4\":{\"data\":[0,2,0]}},\"bitvec\":{\"bits\":[2],\"len\":3}}}"
	);
}
