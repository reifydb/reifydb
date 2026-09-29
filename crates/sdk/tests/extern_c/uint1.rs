// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::factory;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn uint1_zero() {
	let input = factory::uint1("c", [0u8]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint1_zero", &input, &output);
}

#[test]
fn uint1_max() {
	let input = factory::uint1("c", [u8::MAX]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint1_max", &input, &output);
}

#[test]
fn uint1_thirty_two_rows() {
	let values: Vec<u8> = (0..32u8).collect();
	let input = factory::uint1("c", values);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint1_thirty_two_rows", &input, &output);
}

#[test]
fn uint1_with_undefined() {
	let input = factory::uint1_with_bitvec("c", [0u8, 0, 127u8, 0, u8::MAX], vec![true, false, true, false, true]);
	let output = round_trip_column("u", input.clone());
	assert_column_eq("uint1_with_undefined", &input, &output);
}
