// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn uint8_zero() {
	let input = ColumnBuffer::uint8([0u64]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint8_zero", &input, &output);
}

#[test]
fn uint8_max() {
	let input = ColumnBuffer::uint8([u64::MAX]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint8_max", &input, &output);
}

#[test]
fn uint8_endianness_witness() {
	let input = ColumnBuffer::uint8([0x0102_0304_0506_0708u64, 0xDEAD_BEEF_CAFE_BABEu64, 0x8000_0000_0000_0000u64]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint8_endianness", &input, &output);
}

#[test]
fn uint8_thirty_two_rows() {
	let values: Vec<u64> = (0..32u64).map(|i| i.wrapping_mul(0x0101_0101_0101_0101u64)).collect();
	let input = ColumnBuffer::uint8(values);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint8_thirty_two_rows", &input, &output);
}

#[test]
fn uint8_with_undefined() {
	let input =
		ColumnBuffer::uint8_optional([Some(0u64), None, Some(0x8000_0000_0000_0000u64), None, Some(u64::MAX)]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint8_with_undefined", &input, &output);
}
