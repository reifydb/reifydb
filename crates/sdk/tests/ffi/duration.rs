// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::duration::Duration;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn duration_zero() {
	let input = ColumnBuffer::duration([Duration::new(0, 0, 0).expect("valid")]);
	let output = round_trip_column("dur", input.clone());
	assert_column_eq("duration_zero", &input, &output);
}

#[test]
fn duration_pure_months() {
	let input = ColumnBuffer::duration([Duration::new(12, 0, 0).expect("valid")]);
	let output = round_trip_column("dur", input.clone());
	assert_column_eq("duration_months", &input, &output);
}

#[test]
fn duration_pure_days() {
	let input = ColumnBuffer::duration([Duration::new(0, 31, 0).expect("valid")]);
	let output = round_trip_column("dur", input.clone());
	assert_column_eq("duration_days", &input, &output);
}

#[test]
fn duration_pure_nanos() {
	let input = ColumnBuffer::duration([Duration::new(0, 0, 1_000_000_000).expect("valid")]);
	let output = round_trip_column("dur", input.clone());
	assert_column_eq("duration_nanos", &input, &output);
}

#[test]
fn duration_negative_components() {
	let input = ColumnBuffer::duration([Duration::new(-3, -7, -1_500_000_000).expect("valid")]);
	let output = round_trip_column("dur", input.clone());
	assert_column_eq("duration_negative", &input, &output);
}

#[test]
fn duration_extremes() {
	// Duration::new() rejects values that overflow during normalization,
	// so use the largest representable values.
	let input = ColumnBuffer::duration([
		Duration::new(-1_000_000, -1_000_000, i64::MIN / 2).expect("valid"),
		Duration::new(1_000_000, 1_000_000, i64::MAX / 2).expect("valid"),
	]);
	let output = round_trip_column("dur", input.clone());
	assert_column_eq("duration_extremes", &input, &output);
}

#[test]
fn duration_thirty_two_rows() {
	let values: Vec<Duration> =
		(0..32i32).map(|i| Duration::new(i, i * 2, (i as i64) * 1_000_000_000).expect("valid")).collect();
	let input = ColumnBuffer::duration(values);
	let output = round_trip_column("dur", input.clone());
	assert_column_eq("duration_thirty_two_rows", &input, &output);
}

#[test]
fn duration_with_undefined() {
	let input = ColumnBuffer::duration_optional([
		Some(Duration::new(1, 2, 3).unwrap()),
		None,
		Some(Duration::new(-1, -2, -3).unwrap()),
		None,
	]);
	let output = round_trip_column("dur", input.clone());
	assert_column_eq("duration_with_undefined", &input, &output);
}
