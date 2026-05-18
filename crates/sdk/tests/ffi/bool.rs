// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn bool_single_true() {
	let input = ColumnBuffer::bool([true]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("bool_single_true", &input, &output);
}

#[test]
fn bool_single_false() {
	let input = ColumnBuffer::bool([false]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("bool_single_false", &input, &output);
}

#[test]
fn bool_seven_rows_just_under_byte_boundary() {
	let input = ColumnBuffer::bool([true, false, true, true, false, true, false]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("bool_seven_rows", &input, &output);
}

#[test]
fn bool_eight_rows_byte_boundary() {
	let input = ColumnBuffer::bool([true, false, true, false, true, false, true, false]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("bool_eight_rows", &input, &output);
}

#[test]
fn bool_nine_rows_one_past_byte_boundary() {
	let input = ColumnBuffer::bool([true, false, true, false, true, false, true, false, true]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("bool_nine_rows", &input, &output);
}

#[test]
fn bool_sixty_four_rows_alternating() {
	let values: Vec<bool> = (0..64).map(|i| i % 2 == 0).collect();
	let input = ColumnBuffer::bool(values);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("bool_sixty_four_alternating", &input, &output);
}

#[test]
fn bool_with_undefined_alternating() {
	let input = ColumnBuffer::bool_optional([Some(true), None, Some(false), None, Some(true)]);
	let output = round_trip_column("b", input.clone());
	assert_column_eq("bool_with_undefined", &input, &output);
}
