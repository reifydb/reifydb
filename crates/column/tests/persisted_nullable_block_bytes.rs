// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Write as _, sync::Arc};

use arrow_buffer::NullBuffer;
use reifydb_column::{
	persist::{deserialize_block, serialize_block},
	snapshot::{ColumnBlock, ColumnChunks},
};
use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	data::{Column, canonical::Canonical},
};
use reifydb_value::value::{Value, value_type::ValueType};

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

fn all_valid(ty: ValueType, buffer: ColumnBuffer) -> Canonical {
	let len = buffer.len();
	Canonical::new(ty, true, buffer.with_nulls(NullBuffer::new_valid(len)))
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
	let canonical = all_valid(ValueType::Int4, ColumnBuffer::int4([1, 2, 3]));
	let block = nullable_block(ValueType::Int4, canonical);
	assert_block_pinned(block.clone(), "0101016105010101001b0503020406010703");
	let restored = deserialize_block(&serialize_block(&block).unwrap()).unwrap();
	let chunk = restored.columns[0].chunks[0].to_canonical().unwrap();
	assert!(chunk.nullable, "a reloaded chunk must keep its nullable flag");
	assert_eq!(chunk.buffer.nulls().map(|nones| (nones.len(), nones.null_count())), Some((3, 0)));
}

#[test]
fn nullable_utf8_block_with_an_all_set_none_bitmap_is_pinned() {
	// A varlen nullable chunk with zero nones must store the wrapper and its max_bytes exactly on disk.
	let canonical = all_valid(ValueType::Utf8, ColumnBuffer::utf8(["a", "bc"]));
	let block = nullable_block(ValueType::Utf8, canonical);
	assert_block_pinned(block.clone(), "0101016108010101001b0d020161026263ffffffff0f010302");
	let restored = deserialize_block(&serialize_block(&block).unwrap()).unwrap();
	let chunk = restored.columns[0].chunks[0].to_canonical().unwrap();
	assert_eq!(chunk.buffer.nulls().map(|nones| (nones.len(), nones.null_count())), Some((2, 0)));
}

#[test]
fn nullable_int4_block_without_a_none_bitmap_is_pinned() {
	// A nullable schema column whose chunk carries no bitmap must store a bare buffer, never invent a wrapper.
	let canonical = Canonical::new(ValueType::Int4, true, ColumnBuffer::int4([1, 2, 3]));
	let block = nullable_block(ValueType::Int4, canonical);
	assert_block_pinned(block.clone(), "0101016105010101000503020406");
	let restored = deserialize_block(&serialize_block(&block).unwrap()).unwrap();
	let chunk = restored.columns[0].chunks[0].to_canonical().unwrap();
	assert!(chunk.buffer.nulls().is_none(), "a chunk stored without a bitmap must read back without one");
}
