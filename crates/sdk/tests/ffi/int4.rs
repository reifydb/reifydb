// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn int4_zero() {
	let input = ColumnBuffer::int4([0i32]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int4_zero", &input, &output);
}

#[test]
fn int4_min_max() {
	let input = ColumnBuffer::int4([i32::MIN, i32::MAX]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int4_min_max", &input, &output);
}

#[test]
fn int4_endianness_witness() {
	let input = ColumnBuffer::int4([0x01020304, -0x12345678, 0x7FFFFFFF]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int4_endianness", &input, &output);
}

#[test]
fn int4_thirty_two_rows() {
	let values: Vec<i32> = (0..32).map(|i| (i as i32) * 1_000_000 - 16_000_000).collect();
	let input = ColumnBuffer::int4(values);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int4_thirty_two_rows", &input, &output);
}

#[test]
fn int4_with_undefined() {
	let input = ColumnBuffer::int4_optional([Some(i32::MIN), None, Some(0i32), None, Some(i32::MAX)]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int4_with_undefined", &input, &output);
}
