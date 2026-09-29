// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Write as _, sync::Arc};

use arrow_array::{Array, ArrayRef, Int32Array, LargeStringArray};
use arrow_buffer::NullBuffer;
use reifydb_column::{
	persist::{deserialize_block, serialize_block},
	snapshot::{ColumnBlock, ColumnChunks},
};
use reifydb_core::value::column::data::{Column, canonical::Canonical};
use reifydb_value::value::{
	Value,
	constraint::bytes::MaxBytes,
	container::{primitive, varlen_array},
	value_type::{ValueType, field::FieldType},
};

fn hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		write!(out, "{byte:02x}").unwrap();
	}
	out
}

fn unhex(text: &str) -> Vec<u8> {
	(0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap()).collect()
}

fn nullable(ty: ValueType, max_bytes: Option<MaxBytes>, array: ArrayRef) -> Canonical {
	let field_type = FieldType {
		value_type: Some(ValueType::Option(Box::new(ty))),
		max_bytes,
		..FieldType::default()
	};
	Canonical::new(field_type, array).unwrap()
}

fn all_valid_int4(values: Vec<i32>) -> ArrayRef {
	let len = values.len();
	Arc::new(primitive::attach_nulls(Int32Array::from(values), Some(NullBuffer::new_valid(len))))
}

fn all_valid_utf8(values: Vec<&str>) -> ArrayRef {
	let len = values.len();
	Arc::new(varlen_array::attach_nulls(LargeStringArray::from(values), Some(NullBuffer::new_valid(len))))
}

fn nullable_block(ty: ValueType, canonical: Canonical) -> ColumnBlock {
	let schema = Arc::new(vec![("a".to_string(), ty.clone(), true)]);
	ColumnBlock::new(schema, vec![ColumnChunks::single(ty, true, Column::from_canonical(canonical))])
}

fn values(block: &ColumnBlock) -> Vec<Value> {
	block.columns[0].chunks.iter().flat_map(|chunk| (0..chunk.len()).map(|i| chunk.data().get_value(i))).collect()
}

fn assert_block_pinned(block: ColumnBlock, pinned: &str) {
	let written = hex(&serialize_block(&block).unwrap());
	assert_eq!(written, pinned, "block bytes drifted: \"{written}\"");
	let restored = deserialize_block(&unhex(pinned)).unwrap();
	assert_eq!(hex(&serialize_block(&restored).unwrap()), pinned, "decoded block must re-serialize to the pin");
	assert_eq!(*restored.schema, *block.schema, "schema must read back exactly");
	assert!(restored.columns[0].nullable, "the stored column must stay nullable");
	assert_eq!(values(&restored), values(&block), "rows must read back exactly");
}

#[test]
fn nullable_int4_block_with_an_all_set_none_bitmap_is_pinned() {
	// A nullable chunk with zero nones must still store the Option wrapper and an all-set bitmap on disk.
	let canonical = nullable(ValueType::Int4, None, all_valid_int4(vec![1, 2, 3]));
	let block = nullable_block(ValueType::Int4, canonical);
	assert_block_pinned(block.clone(), "020101610501010100011705000000050302040601010703");
	let restored = deserialize_block(&serialize_block(&block).unwrap()).unwrap();
	let chunk = restored.columns[0].chunks[0].to_canonical().unwrap();
	assert!(chunk.view().is_nullable(), "a reloaded chunk must keep its nullable flag");
	assert_eq!(chunk.buffer().logical_nulls().map(|nones| (nones.len(), nones.null_count())), Some((3, 0)));
}

#[test]
fn nullable_utf8_block_with_an_all_set_none_bitmap_is_pinned() {
	// A varlen nullable chunk with zero nones must store the wrapper and its max_bytes exactly on disk.
	let canonical = nullable(ValueType::Utf8, Some(MaxBytes::MAX), all_valid_utf8(vec!["a", "bc"]));
	let block = nullable_block(ValueType::Utf8, canonical);
	assert_block_pinned(block.clone(), "02010161080101010001170801ffffffff0f00000d02016102626301010302");
	let restored = deserialize_block(&serialize_block(&block).unwrap()).unwrap();
	let chunk = restored.columns[0].chunks[0].to_canonical().unwrap();
	assert_eq!(chunk.buffer().logical_nulls().map(|nones| (nones.len(), nones.null_count())), Some((2, 0)));
}

#[test]
fn nullable_int4_block_without_a_none_bitmap_is_pinned() {
	// A nullable schema column whose chunk carries no bitmap must store a bare buffer, never invent a wrapper.
	let canonical = nullable(ValueType::Int4, None, Arc::new(Int32Array::from(vec![1, 2, 3])));
	let block = nullable_block(ValueType::Int4, canonical);
	assert_block_pinned(block.clone(), "020101610501010100011705000000050302040600");
	let restored = deserialize_block(&serialize_block(&block).unwrap()).unwrap();
	let chunk = restored.columns[0].chunks[0].to_canonical().unwrap();
	assert!(chunk.buffer().logical_nulls().is_none(), "a chunk stored without a bitmap must read back without one");
}
