// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::dictionary::DictionaryEntryId;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn dictionary_id_u1_zero() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U1(0)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u1_zero", &input, &output);
}

#[test]
fn dictionary_id_u1_max() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U1(u8::MAX)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u1_max", &input, &output);
}

#[test]
fn dictionary_id_u1_zero_and_max() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U1(0), DictionaryEntryId::U1(u8::MAX)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u1_zero_and_max", &input, &output);
}

#[test]
fn dictionary_id_u2_zero() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U2(0)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u2_zero", &input, &output);
}

#[test]
fn dictionary_id_u2_max() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U2(u16::MAX)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u2_max", &input, &output);
}

#[test]
fn dictionary_id_u2_zero_and_max() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U2(0), DictionaryEntryId::U2(u16::MAX)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u2_zero_and_max", &input, &output);
}

#[test]
fn dictionary_id_u4_zero() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U4(0)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u4_zero", &input, &output);
}

#[test]
fn dictionary_id_u4_max() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U4(u32::MAX)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u4_max", &input, &output);
}

#[test]
fn dictionary_id_u4_zero_and_max() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U4(0), DictionaryEntryId::U4(u32::MAX)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u4_zero_and_max", &input, &output);
}

#[test]
fn dictionary_id_u8_zero() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U8(0)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u8_zero", &input, &output);
}

#[test]
fn dictionary_id_u8_max() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U8(u64::MAX)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u8_max", &input, &output);
}

#[test]
fn dictionary_id_u8_extremes() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U8(0), DictionaryEntryId::U8(u64::MAX)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u8_extremes", &input, &output);
}

#[test]
fn dictionary_id_u16_zero() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U16(0)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u16_zero", &input, &output);
}

#[test]
fn dictionary_id_u16_max() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U16(u128::MAX)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u16_max", &input, &output);
}

#[test]
fn dictionary_id_u16_extremes() {
	let input = ColumnBuffer::dictionary_id([DictionaryEntryId::U16(0), DictionaryEntryId::U16(u128::MAX)]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_u16_extremes", &input, &output);
}

#[test]
fn dictionary_id_mixed_variants() {
	// All five variants in one column - the round trip MUST preserve each
	// element's variant tag, not collapse to a uniform width.
	let input = ColumnBuffer::dictionary_id([
		DictionaryEntryId::U1(7),
		DictionaryEntryId::U2(1234),
		DictionaryEntryId::U4(123_456_789),
		DictionaryEntryId::U8(0xDEAD_BEEF_CAFE_BABEu64),
		DictionaryEntryId::U16(0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10u128),
	]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_mixed_variants", &input, &output);
}

#[test]
fn dictionary_id_thirty_two_rows_u16() {
	let values: Vec<DictionaryEntryId> = (0..32u128).map(DictionaryEntryId::U16).collect();
	let input = ColumnBuffer::dictionary_id(values);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_thirty_two_u16", &input, &output);
}

#[test]
fn dictionary_id_thirty_two_rows_alternating_variants() {
	let values: Vec<DictionaryEntryId> = (0..32u32)
		.map(|i| match i % 5 {
			0 => DictionaryEntryId::U1((i & 0xFF) as u8),
			1 => DictionaryEntryId::U2((i & 0xFFFF) as u16),
			2 => DictionaryEntryId::U4(i),
			3 => DictionaryEntryId::U8(i as u64),
			_ => DictionaryEntryId::U16(i as u128),
		})
		.collect();
	let input = ColumnBuffer::dictionary_id(values);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_alternating_variants", &input, &output);
}

#[test]
fn dictionary_id_with_undefined() {
	let input = ColumnBuffer::dictionary_id_optional([
		Some(DictionaryEntryId::U1(7)),
		None,
		Some(DictionaryEntryId::U2(1234)),
		None,
		Some(DictionaryEntryId::U4(u32::MAX)),
		None,
		Some(DictionaryEntryId::U8(u64::MAX)),
		None,
		Some(DictionaryEntryId::U16(u128::MAX)),
	]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("dict_id_with_undefined", &input, &output);
}
