// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::BooleanBuffer;
use arrow_schema::FieldRef;
use reifydb_core::value::{
	batch::{append, batch},
	column::{builder::ColumnBuilder, factory, scatter::scatter_merge},
};
use reifydb_value::value::{Value, column_view::ColumnView, system_columns::column_view, value_type::ValueType};

fn values(buffer: &ColumnView) -> Vec<Value> {
	(0..buffer.len()).map(|row| buffer.get_value(row)).collect()
}

fn view(column: &(FieldRef, ArrayRef)) -> ColumnView<'_> {
	ColumnView::try_from(column).unwrap()
}

fn extended(column: (FieldRef, ArrayRef), other: (FieldRef, ArrayRef)) -> (FieldRef, ArrayRef) {
	let mut builder = ColumnBuilder::from_view(&view(&column));
	builder.extend(&view(&other)).unwrap();
	builder.finish("c")
}

fn nones_of(ty: ValueType, count: usize) -> Vec<Value> {
	vec![Value::none_of(ty); count]
}

fn option_of(ty: ValueType) -> ValueType {
	ValueType::Option(Box::new(ty))
}

fn columns(buffer: (FieldRef, ArrayRef)) -> RecordBatch {
	batch(vec![buffer]).unwrap()
}

#[test]
fn a_none_column_has_no_defined_row_and_reads_as_option_any() {
	// Otherwise readers of the null buffer see a present row where the column holds no value at all.
	let buffer = factory::none("c", 3);
	let buffer = view(&buffer);

	assert!(!buffer.is_defined(0));
	assert_eq!(buffer.none_count(), 3);
	assert_eq!(buffer.get_type(), option_of(ValueType::Any));
}

#[test]
fn a_none_column_extended_with_int4_becomes_option_int4_with_the_nones_first() {
	// Otherwise the untyped none rows keep the column untyped and the int4 values can not land.
	let buffer = extended(factory::none("c", 3), factory::int4("c", [1, 2]));
	let buffer = view(&buffer);

	assert_eq!(buffer.get_type(), option_of(ValueType::Int4));
	assert_eq!(values(&buffer), [nones_of(ValueType::Int4, 3), vec![Value::Int4(1), Value::Int4(2)]].concat());
}

#[test]
fn an_int4_column_extended_with_a_none_column_stays_int4_with_the_nones_last() {
	// Otherwise the appended none rows arrive untyped and the column stops being int4.
	let buffer = extended(factory::int4("c", [1, 2]), factory::none("c", 3));
	let buffer = view(&buffer);

	assert_eq!(buffer.get_type(), option_of(ValueType::Int4));
	assert_eq!(values(&buffer), [vec![Value::Int4(1), Value::Int4(2)], nones_of(ValueType::Int4, 3)].concat());
}

#[test]
fn appending_a_none_column_and_a_utf8_column_gives_utf8_in_either_order() {
	// Otherwise a batch of only nones next to a utf8 batch fails the append or loses the text.
	let text = || columns(factory::utf8("c", ["a", "b"]));
	let none = || columns(factory::none("c", 1));

	let none_first = append(&none(), &text()).unwrap();
	let none_first = column_view(&none_first, "c").unwrap().unwrap();
	let text_first = append(&text(), &none()).unwrap();
	let text_first = column_view(&text_first, "c").unwrap().unwrap();

	let (a, b) = (Value::Utf8("a".to_string()), Value::Utf8("b".to_string()));
	let text_none = Value::none_of(ValueType::Utf8);
	assert_eq!(none_first.get_type(), option_of(ValueType::Utf8));
	assert_eq!(values(&none_first), [text_none.clone(), a.clone(), b.clone()]);
	assert_eq!(text_first.get_type(), option_of(ValueType::Utf8));
	assert_eq!(values(&text_first), [a, b, text_none]);
}

#[test]
fn scatter_merging_a_none_column_with_an_option_int4_column_gives_option_int4_in_either_arm_order() {
	// Otherwise a none branch next to an int4 branch builds from the untyped side and panics on the first int4.
	let then_mask = BooleanBuffer::from(vec![true, false, true]);
	let else_mask = BooleanBuffer::from(vec![false, true, false]);
	let int4 = factory::int4_with_bitvec("c", [7, 8, 9], vec![true, true, false]);
	let int4 = view(&int4);
	let nones = factory::none("c", 3);
	let nones = view(&nones);
	let none = Value::none_of(ValueType::Int4);

	let none_then = scatter_merge(&nones, &int4, &then_mask, &else_mask, 3, "c").unwrap();
	let none_then = view(&none_then);
	let int4_then = scatter_merge(&int4, &nones, &then_mask, &else_mask, 3, "c").unwrap();
	let int4_then = view(&int4_then);

	assert_eq!(none_then.get_type(), option_of(ValueType::Int4));
	assert_eq!(values(&none_then), [none.clone(), Value::Int4(8), none.clone()]);
	assert_eq!(int4_then.get_type(), option_of(ValueType::Int4));
	assert_eq!(values(&int4_then), [Value::Int4(7), none.clone(), none]);
}
