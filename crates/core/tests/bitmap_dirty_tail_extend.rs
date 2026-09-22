// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::{BooleanBuffer, Buffer};
use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::{Value, value_type::ValueType};

fn dirty_tail_column() -> ColumnBuffer {
	let bits = BooleanBuffer::new(Buffer::from_vec(vec![0b1111_1101u8]), 0, 2);
	ColumnBuffer::int4_with_bitvec(vec![7, 0], bits)
}

fn none() -> Value {
	Value::None {
		inner: ValueType::Int4,
	}
}

fn assert_defined_then_none(column: &ColumnBuffer) {
	assert_eq!(column.len(), 4);
	let defined: Vec<bool> = (0..4).map(|i| column.is_defined(i)).collect();
	assert_eq!(defined, vec![true, false, false, false]);
	let values: Vec<Value> = (0..4).map(|i| column.get_value(i)).collect();
	assert_eq!(values, vec![Value::Int4(7), none(), none(), none()]);
}

#[test]
fn extending_a_raw_bitmap_with_dirty_tail_bits_keeps_the_appended_rows_none() {
	// Dirty bits past len must never be OR-ed into the appended rows, otherwise none rows turn defined.
	let mut column = dirty_tail_column();
	column.extend(ColumnBuffer::none_typed(ValueType::Int4, 2)).unwrap();
	assert_defined_then_none(&column);
}

#[test]
fn pushing_none_onto_a_builder_from_a_raw_bitmap_with_dirty_tail_bits_keeps_the_pushed_rows_none() {
	// The builder must mask the dirty bits it inherits, otherwise the pushed none rows read defined.
	let mut builder = dirty_tail_column().into_builder();
	builder.push_none();
	builder.push_none();
	assert_defined_then_none(&builder.finish());
}
