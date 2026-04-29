// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn uint4_zero() {
	let input = ColumnBuffer::uint4([0u32]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint4_zero", &input, &output);
}

#[test]
fn uint4_max() {
	let input = ColumnBuffer::uint4([u32::MAX]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint4_max", &input, &output);
}

#[test]
fn uint4_endianness_witness() {
	let input = ColumnBuffer::uint4([0x01020304, 0xDEAD_BEEF, 0x8000_0000]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint4_endianness", &input, &output);
}

#[test]
fn uint4_thirty_two_rows() {
	let values: Vec<u32> = (0..32u32).map(|i| i.wrapping_mul(0x0101_0101)).collect();
	let input = ColumnBuffer::uint4(values);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint4_thirty_two_rows", &input, &output);
}

#[test]
fn uint4_with_undefined() {
	let input = ColumnBuffer::uint4_optional([Some(0u32), None, Some(0x8000_0000u32), None, Some(u32::MAX)]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint4_with_undefined", &input, &output);
}
