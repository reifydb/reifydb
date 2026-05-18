// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn float8_zero() {
	let input = ColumnBuffer::float8([0.0f64]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_zero", &input, &output);
}

#[test]
fn float8_negative_zero_distinguished_from_zero() {
	let input = ColumnBuffer::float8([-0.0f64]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_negative_zero", &input, &output);
}

#[test]
fn float8_min_max() {
	let input = ColumnBuffer::float8([f64::MIN, f64::MAX]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_min_max", &input, &output);
}

#[test]
fn float8_nan_and_infinities() {
	let input = ColumnBuffer::float8([f64::NAN, f64::INFINITY, f64::NEG_INFINITY]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_nan_inf", &input, &output);
}

#[test]
fn float8_smallest_subnormal() {
	let input = ColumnBuffer::float8([f64::MIN_POSITIVE, f64::EPSILON, f64::from_bits(1)]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_subnormal", &input, &output);
}

#[test]
fn float8_thirty_two_rows() {
	let values: Vec<f64> = (0..32).map(|i| (i as f64) * 1.5 - 7.5).collect();
	let input = ColumnBuffer::float8(values);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_thirty_two_rows", &input, &output);
}

#[test]
fn float8_with_undefined() {
	let input = ColumnBuffer::float8_optional([Some(1.5f64), None, Some(f64::NAN), None, Some(-3.25f64)]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float8_with_undefined", &input, &output);
}
