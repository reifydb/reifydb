// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::factory;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn int1_zero() {
	let input = factory::int1("c", [0i8]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int1_zero", &input, &output);
}

#[test]
fn int1_min_max() {
	let input = factory::int1("c", [i8::MIN, i8::MAX]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int1_min_max", &input, &output);
}

#[test]
fn int1_negative_one() {
	// i8 -1 is 0xFF; if a byte-vs-element mix-up exists this exposes it.
	let input = factory::int1("c", [-1i8, 0i8, 1i8]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int1_negative_one", &input, &output);
}

#[test]
fn int1_thirty_two_rows() {
	let values: Vec<i8> = (0..32).map(|i| (i as i8) - 16).collect();
	let input = factory::int1("c", values);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int1_thirty_two_rows", &input, &output);
}

#[test]
fn int1_with_undefined() {
	let input = factory::int1_with_bitvec("c", [i8::MIN, 0, 0i8, 0, i8::MAX], vec![true, false, true, false, true]);
	let output = round_trip_column("i", input.clone());
	assert_column_eq("int1_with_undefined", &input, &output);
}
