// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn uint16_zero() {
	let input = ColumnBuffer::uint16([0u128]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint16_zero", &input, &output);
}

#[test]
fn uint16_max() {
	let input = ColumnBuffer::uint16([u128::MAX]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint16_max", &input, &output);
}

#[test]
fn uint16_high_low_word_witness() {
	let v: u128 = 0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10u128;
	let input = ColumnBuffer::uint16([v, !v]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint16_high_low", &input, &output);
}

#[test]
fn uint16_thirty_two_rows() {
	let values: Vec<u128> = (0..32u128).map(|i| i << 80).collect();
	let input = ColumnBuffer::uint16(values);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint16_thirty_two_rows", &input, &output);
}

#[test]
fn uint16_with_undefined() {
	let input = ColumnBuffer::uint16_optional([Some(0u128), None, Some(1u128 << 100), None, Some(u128::MAX)]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint16_with_undefined", &input, &output);
}
