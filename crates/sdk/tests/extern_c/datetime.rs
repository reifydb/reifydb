// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::factory;
use reifydb_value::value::datetime::DateTime;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn datetime_epoch() {
	let input = factory::datetime("c", [DateTime::from_nanos(0)]);
	let output = round_trip_column("dt", input.clone());
	assert_column_eq("datetime_epoch", &input, &output);
}

#[test]
fn datetime_one_nanosecond() {
	let input = factory::datetime("c", [DateTime::from_nanos(1)]);
	let output = round_trip_column("dt", input.clone());
	assert_column_eq("datetime_one_nano", &input, &output);
}

#[test]
fn datetime_far_future() {
	// Roughly year 2200: far enough out that a 32-bit or seconds-based intermediate would overflow.
	let input = factory::datetime("c", [DateTime::from_nanos(7_257_600_000_000_000_000i64)]);
	let output = round_trip_column("dt", input.clone());
	assert_column_eq("datetime_far_future", &input, &output);
}

#[test]
fn datetime_max_i64() {
	let input = factory::datetime("c", [DateTime::from_nanos(i64::MAX)]);
	let output = round_trip_column("dt", input.clone());
	assert_column_eq("datetime_max", &input, &output);
}

#[test]
fn datetime_thirty_two_rows() {
	let values: Vec<DateTime> = (0..32i64).map(|i| DateTime::from_nanos(i * 1_000_000_000)).collect();
	let input = factory::datetime("c", values);
	let output = round_trip_column("dt", input.clone());
	assert_column_eq("datetime_thirty_two_rows", &input, &output);
}

#[test]
fn datetime_with_undefined() {
	let input = factory::datetime_with_bitvec(
		"c",
		[
			DateTime::from_nanos(0),
			DateTime::default(),
			DateTime::from_nanos(1_000_000_000),
			DateTime::default(),
		],
		vec![true, false, true, false],
	);
	let output = round_trip_column("dt", input.clone());
	assert_column_eq("datetime_with_undefined", &input, &output);
}
