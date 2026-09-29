// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::factory;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn int4_zero() {
	let input = factory::int4("c", [0i32]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int4_zero", &input, &output);
}

#[test]
fn int4_min_max() {
	let input = factory::int4("c", [i32::MIN, i32::MAX]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int4_min_max", &input, &output);
}

#[test]
fn int4_endianness_witness() {
	let input = factory::int4("c", [0x01020304, -0x12345678, 0x7FFFFFFF]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int4_endianness", &input, &output);
}

#[test]
fn int4_thirty_two_rows() {
	let values: Vec<i32> = (0..32).map(|i| i * 1_000_000 - 16_000_000).collect();
	let input = factory::int4("c", values);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int4_thirty_two_rows", &input, &output);
}

#[test]
fn int4_with_undefined() {
	let input = factory::int4_optional("c", [Some(i32::MIN), None, Some(0i32), None, Some(i32::MAX)]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int4_with_undefined", &input, &output);
}
