// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn uint1_zero() {
	let input = ColumnBuffer::uint1([0u8]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint1_zero", &input, &output);
}

#[test]
fn uint1_max() {
	let input = ColumnBuffer::uint1([u8::MAX]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint1_max", &input, &output);
}

#[test]
fn uint1_thirty_two_rows() {
	let values: Vec<u8> = (0..32u8).collect();
	let input = ColumnBuffer::uint1(values);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint1_thirty_two_rows", &input, &output);
}

#[test]
fn uint1_with_undefined() {
	let input = ColumnBuffer::uint1_optional([Some(0u8), None, Some(127u8), None, Some(u8::MAX)]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint1_with_undefined", &input, &output);
}
