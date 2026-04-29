// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn float4_zero() {
	let input = ColumnBuffer::float4([0.0f32]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float4_zero", &input, &output);
}

#[test]
fn float4_negative_zero_distinguished_from_zero() {
	// +0.0 and -0.0 differ in the sign bit; round trip must preserve.
	let input = ColumnBuffer::float4([-0.0f32]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float4_negative_zero", &input, &output);
}

#[test]
fn float4_min_max() {
	let input = ColumnBuffer::float4([f32::MIN, f32::MAX]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float4_min_max", &input, &output);
}

#[test]
fn float4_nan_and_infinities() {
	let input = ColumnBuffer::float4([f32::NAN, f32::INFINITY, f32::NEG_INFINITY]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float4_nan_inf", &input, &output);
}

#[test]
fn float4_smallest_subnormal() {
	let input = ColumnBuffer::float4([f32::MIN_POSITIVE, f32::EPSILON, f32::from_bits(1)]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float4_subnormal", &input, &output);
}

#[test]
fn float4_thirty_two_rows() {
	let values: Vec<f32> = (0..32).map(|i| (i as f32) * 1.5 - 7.5).collect();
	let input = ColumnBuffer::float4(values);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float4_thirty_two_rows", &input, &output);
}

#[test]
fn float4_with_undefined() {
	let input = ColumnBuffer::float4_optional([Some(1.5f32), None, Some(f32::NAN), None, Some(-3.25f32)]);
	let output = round_trip_column("f", input.clone());
	assert_column_eq("float4_with_undefined", &input, &output);
}
