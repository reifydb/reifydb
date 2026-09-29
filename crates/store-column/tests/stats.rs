// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{interface::catalog::column_snapshot::ColumnStats, value::column::factory};
use reifydb_store_column::{
	convert::to_vortex,
	session::new_session,
	snapshot::{ColumnBlock, ColumnChunks},
	stats::block_stats,
};
use reifydb_value::value::{
	Value,
	value_type::{ValueType, field::from_field},
};
use vortex_array::ArrayRef as VortexArrayRef;

fn chunk(column: &(FieldRef, ArrayRef)) -> VortexArrayRef {
	to_vortex(&new_session(), column).unwrap()
}

fn chunks(ty: ValueType, nullable: bool, parts: &[(FieldRef, ArrayRef)]) -> ColumnChunks {
	let field_type = from_field(&parts[0].0).unwrap();
	ColumnChunks::new(ty, nullable, field_type, parts.iter().map(chunk).collect())
}

fn one_column_block(ty: ValueType, chunks: ColumnChunks) -> ColumnBlock {
	let nullable = chunks.nullable;
	ColumnBlock::new(Arc::new(vec![("id".to_string(), ty, nullable)]), vec![chunks])
}

fn stats_of(ty: ValueType, column: (FieldRef, ArrayRef)) -> ColumnStats {
	let nullable = column.0.is_nullable();
	let block = one_column_block(ty.clone(), chunks(ty, nullable, &[column]));
	block_stats(&block, &new_session()).unwrap().remove(0)
}

fn bounds(ty: ValueType, column: (FieldRef, ArrayRef)) -> (Option<Value>, Option<Value>) {
	let stats = stats_of(ty, column);
	(stats.min, stats.max)
}

#[test]
fn an_empty_column_and_an_all_none_column_both_report_no_bounds() {
	// A made-up bound for zero rows or all-none rows would let the pruner drop a block it must keep.
	let empty = stats_of(ValueType::Int4, factory::int4("id", Vec::<i32>::new()));
	let all_none = stats_of(ValueType::Int4, factory::none_typed("id", ValueType::Int4, 3));
	assert_eq!((empty.min, empty.max, empty.none_count), (None, None, 0));
	assert_eq!((all_none.min, all_none.max, all_none.none_count), (None, None, 3));
}

#[test]
fn an_empty_and_an_all_none_utf8_column_both_report_no_bounds() {
	// The ordered utf8 path must refuse a bound exactly like the numeric one.
	let empty = stats_of(ValueType::Utf8, factory::utf8("id", Vec::<String>::new()));
	let all_none = stats_of(ValueType::Utf8, factory::none_typed("id", ValueType::Utf8, 2));
	assert_eq!((empty.min, empty.max, empty.none_count), (None, None, 0));
	assert_eq!((all_none.min, all_none.max, all_none.none_count), (None, None, 2));
}

#[test]
fn bounds_over_every_native_integer_width_reach_the_type_extremes() {
	// A reduction that widens or re-signs the native values clamps the extremes to the wrong bound.
	assert_eq!(
		bounds(ValueType::Int1, factory::int1("id", [0i8, i8::MAX, -1, i8::MIN, 1])),
		(Some(Value::Int1(i8::MIN)), Some(Value::Int1(i8::MAX)))
	);
	assert_eq!(
		bounds(ValueType::Int2, factory::int2("id", [0i16, i16::MAX, -1, i16::MIN, 1])),
		(Some(Value::Int2(i16::MIN)), Some(Value::Int2(i16::MAX)))
	);
	assert_eq!(
		bounds(ValueType::Int4, factory::int4("id", [0i32, i32::MAX, -1, i32::MIN, 1])),
		(Some(Value::Int4(i32::MIN)), Some(Value::Int4(i32::MAX)))
	);
	assert_eq!(
		bounds(ValueType::Int8, factory::int8("id", [0i64, i64::MAX, -1, i64::MIN, 1])),
		(Some(Value::Int8(i64::MIN)), Some(Value::Int8(i64::MAX)))
	);
	assert_eq!(
		bounds(ValueType::Uint1, factory::uint1("id", [1u8, u8::MAX, 0, 7])),
		(Some(Value::Uint1(0)), Some(Value::Uint1(u8::MAX)))
	);
	assert_eq!(
		bounds(ValueType::Uint2, factory::uint2("id", [1u16, u16::MAX, 0, 7])),
		(Some(Value::Uint2(0)), Some(Value::Uint2(u16::MAX)))
	);
	assert_eq!(
		bounds(ValueType::Uint4, factory::uint4("id", [1u32, u32::MAX, 0, 7])),
		(Some(Value::Uint4(0)), Some(Value::Uint4(u32::MAX)))
	);
	assert_eq!(
		bounds(ValueType::Uint8, factory::uint8("id", [1u64, u64::MAX, 0, 7])),
		(Some(Value::Uint8(0)), Some(Value::Uint8(u64::MAX)))
	);
}

#[test]
fn signed_bounds_ignore_none_rows() {
	// Without the validity bits the bounds come back as -999 and 999, which the column never holds.
	let buffer = factory::int4_with_bitvec("id", [30i32, -999, 10, 999, 50], vec![true, false, true, false, true]);
	assert_eq!(bounds(ValueType::Int4, buffer), (Some(Value::Int4(10)), Some(Value::Int4(50))));
}

#[test]
fn unsigned_bounds_ignore_none_rows() {
	// The masked rows sit at both ends here, so ignoring validity takes over both bounds at once.
	let buffer = factory::uint8_with_bitvec("id", [7u64, u64::MAX, 9, 0, 8], vec![true, false, true, false, true]);
	assert_eq!(bounds(ValueType::Uint8, buffer), (Some(Value::Uint8(7)), Some(Value::Uint8(9))));
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
	let buffer = factory::int4_with_bitvec("id", values, valid);
	assert_eq!(bounds(ValueType::Int4, buffer), (Some(Value::Int4(2)), Some(Value::Int4(9))));
}

#[test]
fn the_none_count_of_a_block_equals_the_nones_its_chunks_hold() {
	// A count over the wrong window reports rows that are not there while the bounds still look right.
	let chunks = chunks(
		ValueType::Int4,
		true,
		&[
			factory::int4_with_bitvec("id", [1i32, 0, 3, 0, 5], vec![true, false, true, false, true]),
			factory::int4("id", [8i32, 9]),
			factory::none_typed("id", ValueType::Int4, 4),
		],
	);
	let stats = block_stats(&one_column_block(ValueType::Int4, chunks), &new_session()).unwrap();
	assert_eq!(stats[0].none_count, 6, "two nones in the first chunk, none in the second, four in the third");
	assert_eq!(stats[0].min, Some(Value::Int4(1)));
	assert_eq!(stats[0].max, Some(Value::Int4(9)));
}

#[test]
fn a_sliced_chunk_counts_the_nones_inside_its_own_window() {
	// A count over anything but the slice's own window reads rows the slice does not own.
	let full = factory::int4_with_bitvec(
		"id",
		[1i32, 0, 0, 4, 0, 6, 7, 0],
		vec![true, false, false, true, false, true, true, false],
	);
	let sliced = chunk(&full).slice(3..6).unwrap();
	assert_eq!(sliced.len(), 3);
	let chunks = ColumnChunks::new(ValueType::Int4, true, from_field(&full.0).unwrap(), vec![sliced]);
	let stats = block_stats(&one_column_block(ValueType::Int4, chunks), &new_session()).unwrap();
	assert_eq!(stats[0].none_count, 1, "rows 3 to 5 hold exactly one none");
	assert_eq!(stats[0].min, Some(Value::Int4(4)));
	assert_eq!(stats[0].max, Some(Value::Int4(6)));
}

#[test]
fn a_chunk_without_a_validity_buffer_counts_no_nones() {
	// Without a validity buffer every row is live, so counting rows would mark the column all none.
	let stats = stats_of(ValueType::Uint4, factory::uint4("id", [4u32, 5, 6]));
	assert_eq!(stats.none_count, 0);
	assert_eq!(stats.min, Some(Value::Uint4(4)));
	assert_eq!(stats.max, Some(Value::Uint4(6)));
}
