// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::factory;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn float8_zero() {
	let input = factory::float8("c", [0.0f64]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_zero", &input, &output);
}

#[test]
fn float8_negative_zero_distinguished_from_zero() {
	let input = factory::float8("c", [-0.0f64]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_negative_zero", &input, &output);
}

#[test]
fn float8_min_max() {
	let input = factory::float8("c", [f64::MIN, f64::MAX]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_min_max", &input, &output);
}

#[test]
fn float8_smallest_subnormal() {
	let input = factory::float8("c", [f64::MIN_POSITIVE, f64::EPSILON, f64::from_bits(1)]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_subnormal", &input, &output);
}

#[test]
fn float8_thirty_two_rows() {
	let values: Vec<f64> = (0..32).map(|i| (i as f64) * 1.5 - 7.5).collect();
	let input = factory::float8("c", values);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_thirty_two_rows", &input, &output);
}

#[test]
fn float8_with_undefined() {
	let input = factory::float8_with_bitvec(
		"c",
		[1.5f64, 0.0, f64::NAN, 0.0, -3.25f64],
		vec![true, false, true, false, true],
	);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_with_undefined", &input, &output);
}
