// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_column::{
	compute::min_max,
	snapshot::{ColumnBlock, ColumnChunks},
	stats::block_stats,
};
use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	data::{Column, canonical::Canonical},
};
use reifydb_value::value::{Value, value_type::ValueType};

fn column(buffer: ColumnBuffer) -> Column {
	Column::from_canonical(Canonical::from_column_buffer(&buffer).unwrap())
}

fn one_column_block(ty: ValueType, chunks: ColumnChunks) -> ColumnBlock {
	let nullable = chunks.nullable;
	ColumnBlock::new(Arc::new(vec![("id".to_string(), ty, nullable)]), vec![chunks])
}

#[test]
fn an_empty_column_and_an_all_none_column_report_different_errors() {
	// Without two distinct errors, zero rows is indistinguishable from rows that are all none.
	let empty = min_max(&column(ColumnBuffer::int4(Vec::<i32>::new()))).unwrap_err();
	let all_none = min_max(&column(ColumnBuffer::none_typed(ValueType::Int4, 3))).unwrap_err();
	assert_eq!(empty.0.code, "COL_015");
	assert_eq!(all_none.0.code, "COL_016");
	assert_ne!(empty.0.code, all_none.0.code);
}

#[test]
fn an_empty_and_an_all_none_utf8_column_report_the_same_two_errors() {
	// The two errors must stay distinct on the ordered path too, never only on the numeric one.
	let empty = min_max(&column(ColumnBuffer::utf8(Vec::<String>::new()))).unwrap_err();
	let all_none = min_max(&column(ColumnBuffer::none_typed(ValueType::Utf8, 2))).unwrap_err();
	assert_eq!(empty.0.code, "COL_015");
	assert_eq!(all_none.0.code, "COL_016");
}

#[test]
fn bounds_over_every_native_integer_width_reach_the_type_extremes() {
	// A reduction that widens or re-signs the native values clamps the extremes to the wrong bound.
	assert_eq!(
		min_max(&column(ColumnBuffer::int1([0i8, i8::MAX, -1, i8::MIN, 1]))).unwrap(),
		(Value::Int1(i8::MIN), Value::Int1(i8::MAX))
	);
	assert_eq!(
		min_max(&column(ColumnBuffer::int2([0i16, i16::MAX, -1, i16::MIN, 1]))).unwrap(),
		(Value::Int2(i16::MIN), Value::Int2(i16::MAX))
	);
	assert_eq!(
		min_max(&column(ColumnBuffer::int4([0i32, i32::MAX, -1, i32::MIN, 1]))).unwrap(),
		(Value::Int4(i32::MIN), Value::Int4(i32::MAX))
	);
	assert_eq!(
		min_max(&column(ColumnBuffer::int8([0i64, i64::MAX, -1, i64::MIN, 1]))).unwrap(),
		(Value::Int8(i64::MIN), Value::Int8(i64::MAX))
	);
	assert_eq!(
		min_max(&column(ColumnBuffer::uint1([1u8, u8::MAX, 0, 7]))).unwrap(),
		(Value::Uint1(0), Value::Uint1(u8::MAX))
	);
	assert_eq!(
		min_max(&column(ColumnBuffer::uint2([1u16, u16::MAX, 0, 7]))).unwrap(),
		(Value::Uint2(0), Value::Uint2(u16::MAX))
	);
	assert_eq!(
		min_max(&column(ColumnBuffer::uint4([1u32, u32::MAX, 0, 7]))).unwrap(),
		(Value::Uint4(0), Value::Uint4(u32::MAX))
	);
	assert_eq!(
		min_max(&column(ColumnBuffer::uint8([1u64, u64::MAX, 0, 7]))).unwrap(),
		(Value::Uint8(0), Value::Uint8(u64::MAX))
	);
}

#[test]
fn signed_bounds_ignore_none_rows() {
	// Without the validity bits the bounds come back as -999 and 999, which the column never holds.
	let buffer = ColumnBuffer::int4_with_bitvec([30i32, -999, 10, 999, 50], vec![true, false, true, false, true]);
	assert_eq!(min_max(&column(buffer)).unwrap(), (Value::Int4(10), Value::Int4(50)));
}

#[test]
fn unsigned_bounds_ignore_none_rows() {
	// The masked rows sit at both ends here, so ignoring validity takes over both bounds at once.
	let buffer = ColumnBuffer::uint8_with_bitvec([7u64, u64::MAX, 9, 0, 8], vec![true, false, true, false, true]);
	assert_eq!(min_max(&column(buffer)).unwrap(), (Value::Uint8(7), Value::Uint8(9)));
}

#[test]
fn bounds_ignore_none_rows_past_the_first_validity_word() {
	// Validity is read a word at a time, so a none past the first word catches a per-word off-by-one.
	let mut values = vec![5i32; 200];
	let mut valid = vec![true; 200];
	values[7] = 2;
	values[199] = 9;
	values[130] = -1000;
	valid[130] = false;
	values[131] = 1000;
	valid[131] = false;
	let buffer = ColumnBuffer::int4_with_bitvec(values, valid);
	assert_eq!(min_max(&column(buffer)).unwrap(), (Value::Int4(2), Value::Int4(9)));
}

#[test]
fn the_none_count_of_a_block_equals_the_nones_its_chunks_hold() {
	// A count over the wrong window reports rows that are not there while the bounds still look right.
	let chunks = ColumnChunks::new(
		ValueType::Int4,
		true,
		vec![
			column(ColumnBuffer::int4_with_bitvec(
				[1i32, 0, 3, 0, 5],
				vec![true, false, true, false, true],
			)),
			column(ColumnBuffer::int4([8i32, 9])),
			column(ColumnBuffer::none_typed(ValueType::Int4, 4)),
		],
	);
	let stats = block_stats(&one_column_block(ValueType::Int4, chunks)).unwrap();
	assert_eq!(stats[0].none_count, 6, "two nones in the first chunk, none in the second, four in the third");
	assert_eq!(stats[0].min, Some(Value::Int4(1)));
	assert_eq!(stats[0].max, Some(Value::Int4(9)));
}

#[test]
fn a_sliced_chunk_counts_the_nones_inside_its_own_window() {
	// A count over anything but the slice's own window reads rows the slice does not own.
	let full = column(ColumnBuffer::int4_with_bitvec(
		[1i32, 0, 0, 4, 0, 6, 7, 0],
		vec![true, false, false, true, false, true, true, false],
	));
	let sliced = full.slice(3, 6).unwrap();
	assert_eq!(sliced.len(), 3);
	let chunks = ColumnChunks::new(ValueType::Int4, true, vec![sliced]);
	let stats = block_stats(&one_column_block(ValueType::Int4, chunks)).unwrap();
	assert_eq!(stats[0].none_count, 1, "rows 3 to 5 hold exactly one none");
	assert_eq!(stats[0].min, Some(Value::Int4(4)));
	assert_eq!(stats[0].max, Some(Value::Int4(6)));
}

#[test]
fn a_chunk_without_a_validity_buffer_counts_no_nones() {
	// Without a validity buffer every row is live, so counting rows would mark the column all none.
	let chunks = ColumnChunks::new(ValueType::Uint4, false, vec![column(ColumnBuffer::uint4([4u32, 5, 6]))]);
	let stats = block_stats(&one_column_block(ValueType::Uint4, chunks)).unwrap();
	assert_eq!(stats[0].none_count, 0);
	assert_eq!(stats[0].min, Some(Value::Uint4(4)));
	assert_eq!(stats[0].max, Some(Value::Uint4(6)));
}
