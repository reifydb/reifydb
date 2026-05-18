// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn int8_zero() {
	let input = ColumnBuffer::int8([0i64]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int8_zero", &input, &output);
}

#[test]
fn int8_min_max() {
	let input = ColumnBuffer::int8([i64::MIN, i64::MAX]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int8_min_max", &input, &output);
}

#[test]
fn int8_endianness_witness() {
	let input = ColumnBuffer::int8([0x0102030405060708i64, -0x123456789ABCDEF0i64, 0x7FFF_FFFF_FFFF_FFFFi64]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int8_endianness", &input, &output);
}

#[test]
fn int8_thirty_two_rows() {
	let values: Vec<i64> = (0..32).map(|i| (i as i64) * 1_000_000_000 - 16_000_000_000).collect();
	let input = ColumnBuffer::int8(values);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int8_thirty_two_rows", &input, &output);
}

#[test]
fn int8_with_undefined() {
	let input = ColumnBuffer::int8_optional([Some(i64::MIN), None, Some(0i64), None, Some(i64::MAX)]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int8_with_undefined", &input, &output);
}
