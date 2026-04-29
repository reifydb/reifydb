// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn int16_zero() {
	let input = ColumnBuffer::int16([0i128]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int16_zero", &input, &output);
}

#[test]
fn int16_min_max() {
	let input = ColumnBuffer::int16([i128::MIN, i128::MAX]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int16_min_max", &input, &output);
}

#[test]
fn int16_high_low_word_witness() {
	// Distinct bytes in every 8-byte half catch any 8-byte stride bug.
	let v: i128 = (0x0102_0304_0506_0708i128) | ((0x090A_0B0C_0D0E_0F10i128) << 64);
	let input = ColumnBuffer::int16([v, -v]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int16_high_low", &input, &output);
}

#[test]
fn int16_thirty_two_rows() {
	let values: Vec<i128> = (0..32).map(|i| (i as i128) << 80).collect();
	let input = ColumnBuffer::int16(values);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int16_thirty_two_rows", &input, &output);
}

#[test]
fn int16_with_undefined() {
	let input = ColumnBuffer::int16_optional([Some(i128::MIN), None, Some(0i128), None, Some(i128::MAX)]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int16_with_undefined", &input, &output);
}
