// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn uint2_zero() {
	let input = ColumnBuffer::uint2([0u16]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint2_zero", &input, &output);
}

#[test]
fn uint2_max() {
	let input = ColumnBuffer::uint2([u16::MAX]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint2_max", &input, &output);
}

#[test]
fn uint2_endianness_witness() {
	let input = ColumnBuffer::uint2([0x0102, 0xFFFE, 0x8000]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint2_endianness", &input, &output);
}

#[test]
fn uint2_thirty_two_rows() {
	let values: Vec<u16> = (0..32u16).map(|i| i * 1000).collect();
	let input = ColumnBuffer::uint2(values);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint2_thirty_two_rows", &input, &output);
}

#[test]
fn uint2_with_undefined() {
	let input = ColumnBuffer::uint2_optional([Some(0u16), None, Some(32_768u16), None, Some(u16::MAX)]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint2_with_undefined", &input, &output);
}
