// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::datetime::DateTime;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn datetime_epoch() {
	let input = ColumnBuffer::datetime([DateTime::from_nanos(0)]);
	let output = round_trip_column("dt", input.clone());
	assert_column_eq("datetime_epoch", &input, &output);
}

#[test]
fn datetime_one_nanosecond() {
	let input = ColumnBuffer::datetime([DateTime::from_nanos(1)]);
	let output = round_trip_column("dt", input.clone());
	assert_column_eq("datetime_one_nano", &input, &output);
}

#[test]
fn datetime_far_future() {
	// Year 2200ish.
	let input = ColumnBuffer::datetime([DateTime::from_nanos(7_257_600_000_000_000_000u64)]);
	let output = round_trip_column("dt", input.clone());
	assert_column_eq("datetime_far_future", &input, &output);
}

#[test]
fn datetime_max_u64() {
	let input = ColumnBuffer::datetime([DateTime::from_nanos(u64::MAX)]);
	let output = round_trip_column("dt", input.clone());
	assert_column_eq("datetime_max", &input, &output);
}

#[test]
fn datetime_thirty_two_rows() {
	let values: Vec<DateTime> = (0..32u64).map(|i| DateTime::from_nanos(i * 1_000_000_000)).collect();
	let input = ColumnBuffer::datetime(values);
	let output = round_trip_column("dt", input.clone());
	assert_column_eq("datetime_thirty_two_rows", &input, &output);
}

#[test]
fn datetime_with_undefined() {
	let input = ColumnBuffer::datetime_optional([
		Some(DateTime::from_nanos(0)),
		None,
		Some(DateTime::from_nanos(1_000_000_000)),
		None,
	]);
	let output = round_trip_column("dt", input.clone());
	assert_column_eq("datetime_with_undefined", &input, &output);
}
