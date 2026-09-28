// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{
	fragment::Fragment,
	value::{Value, frame::data::FrameColumnData, value_type::ValueType},
};

fn values(buffer: &ColumnBuffer) -> Vec<Value> {
	(0..buffer.len()).map(|row| buffer.get_value(row)).collect()
}

fn nones_of(ty: ValueType, count: usize) -> Vec<Value> {
	vec![Value::none_of(ty); count]
}

fn option_of(ty: ValueType) -> ValueType {
	ValueType::Option(Box::new(ty))
}

fn columns(buffer: ColumnBuffer) -> Columns {
	Columns::new(vec![ColumnWithName::new(Fragment::internal("c"), buffer)])
}

#[test]
fn a_none_column_has_no_defined_row_and_reads_as_option_any() {
	// Otherwise readers of the null buffer see a present row where the column holds no value at all.
	let buffer = ColumnBuffer::none(3);

	assert!(!buffer.is_defined(0));
	assert_eq!(buffer.none_count(), 3);
	assert_eq!(buffer.get_type(), option_of(ValueType::Any));
}

#[test]
fn a_none_column_extended_with_int4_becomes_option_int4_with_the_nones_first() {
	// Otherwise the untyped none rows keep the column untyped and the int4 values can not land.
	let mut buffer = ColumnBuffer::none(3);

	buffer.extend(ColumnBuffer::int4([1, 2])).unwrap();

	assert_eq!(buffer.get_type(), option_of(ValueType::Int4));
	assert_eq!(values(&buffer), [nones_of(ValueType::Int4, 3), vec![Value::Int4(1), Value::Int4(2)]].concat());
}

#[test]
fn an_int4_column_extended_with_a_none_column_stays_int4_with_the_nones_last() {
	// Otherwise the appended none rows arrive untyped and the column stops being int4.
	let mut buffer = ColumnBuffer::int4([1, 2]);

	buffer.extend(ColumnBuffer::none(3)).unwrap();

	assert_eq!(buffer.get_type(), option_of(ValueType::Int4));
	assert_eq!(values(&buffer), [vec![Value::Int4(1), Value::Int4(2)], nones_of(ValueType::Int4, 3)].concat());
}

#[test]
fn appending_a_none_column_and_a_utf8_column_gives_utf8_in_either_order() {
	// Otherwise a batch of only nones next to a utf8 batch fails the append or loses the text.
	let text = || columns(ColumnBuffer::utf8(["a", "b"]));
	let none = || columns(ColumnBuffer::none(1));

	let mut none_first = none();
	none_first.append(text()).unwrap();
	let mut text_first = text();
	text_first.append(none()).unwrap();

	let (a, b) = (Value::Utf8("a".to_string()), Value::Utf8("b".to_string()));
	let text_none = Value::none_of(ValueType::Utf8);
	assert_eq!(none_first.columns[0].get_type(), option_of(ValueType::Utf8));
	assert_eq!(values(&none_first.columns[0]), [text_none.clone(), a.clone(), b.clone()]);
	assert_eq!(text_first.columns[0].get_type(), option_of(ValueType::Utf8));
	assert_eq!(values(&text_first.columns[0]), [a, b, text_none]);
}

#[test]
fn scatter_merging_a_none_column_with_an_option_int4_column_gives_option_int4_in_either_arm_order() {
	// Otherwise a none branch next to an int4 branch builds from the untyped side and panics on the first int4.
	let then_mask = BooleanBuffer::from(vec![true, false, true]);
	let else_mask = BooleanBuffer::from(vec![false, true, false]);
	let int4 = ColumnBuffer::int4_with_bitvec([7, 8, 9], vec![true, true, false]);
	let none = Value::none_of(ValueType::Int4);

	let none_then = ColumnBuffer::none(3).scatter_merge(&int4, &then_mask, &else_mask, 3).unwrap();
	let int4_then = int4.scatter_merge(&ColumnBuffer::none(3), &then_mask, &else_mask, 3).unwrap();

	assert_eq!(none_then.get_type(), option_of(ValueType::Int4));
	assert_eq!(values(&none_then), [none.clone(), Value::Int4(8), none.clone()]);
	assert_eq!(int4_then.get_type(), option_of(ValueType::Int4));
	assert_eq!(values(&int4_then), [Value::Int4(7), none.clone(), none]);
}

#[test]
fn a_none_column_round_trips_through_postcard_and_json_as_itself() {
	// Otherwise a persisted or shipped none column comes back typed, or as an any column with encoded nones.
	let buffer = ColumnBuffer::none(2);

	let from_postcard: ColumnBuffer = postcard::from_bytes(&postcard::to_stdvec(&buffer).unwrap()).unwrap();
	let from_json: ColumnBuffer = serde_json::from_str(&serde_json::to_string(&buffer).unwrap()).unwrap();

	assert_eq!(from_postcard, buffer);
	assert_eq!(from_json, buffer);
	assert_eq!(from_json.get_type(), option_of(ValueType::Any));
}

#[test]
fn a_none_column_leaves_through_the_frame_edge_as_an_optional_any_column_of_nones() {
	// Otherwise clients that know no untyped none column can not read the frame.
	let frame = FrameColumnData::from(ColumnBuffer::none(2));

	let back = ColumnBuffer::from(frame);

	assert_eq!(back, ColumnBuffer::any_optional(vec![None; 2]));
}
