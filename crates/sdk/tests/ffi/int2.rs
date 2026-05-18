// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn int2_zero() {
	let input = ColumnBuffer::int2([0i16]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int2_zero", &input, &output);
}

#[test]
fn int2_min_max() {
	let input = ColumnBuffer::int2([i16::MIN, i16::MAX]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int2_min_max", &input, &output);
}

#[test]
fn int2_endianness_witness() {
	// Specific bytes to detect a byte-swap defect (LE vs BE).
	let input = ColumnBuffer::int2([0x0102, 0x0304, -0x7F00]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int2_endianness", &input, &output);
}

#[test]
fn int2_thirty_two_rows() {
	let values: Vec<i16> = (0..32).map(|i| (i as i16) * 1000 - 16000).collect();
	let input = ColumnBuffer::int2(values);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int2_thirty_two_rows", &input, &output);
}

#[test]
fn int2_with_undefined() {
	let input = ColumnBuffer::int2_optional([Some(i16::MIN), None, Some(0i16), None, Some(i16::MAX)]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int2_with_undefined", &input, &output);
}
