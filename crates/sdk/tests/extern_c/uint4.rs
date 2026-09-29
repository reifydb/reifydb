// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::factory;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn uint4_zero() {
	let input = factory::uint4("c", [0u32]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint4_zero", &input, &output);
}

#[test]
fn uint4_max() {
	let input = factory::uint4("c", [u32::MAX]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint4_max", &input, &output);
}

#[test]
fn uint4_endianness_witness() {
	let input = factory::uint4("c", [0x01020304, 0xDEAD_BEEF, 0x8000_0000]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint4_endianness", &input, &output);
}

#[test]
fn uint4_thirty_two_rows() {
	let values: Vec<u32> = (0..32u32).map(|i| i.wrapping_mul(0x0101_0101)).collect();
	let input = factory::uint4("c", values);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint4_thirty_two_rows", &input, &output);
}

#[test]
fn uint4_with_undefined() {
	let input = factory::uint4_with_bitvec(
		"c",
		[0u32, 0, 0x8000_0000u32, 0, u32::MAX],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint4_with_undefined", &input, &output);
}
