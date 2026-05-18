// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::time::Time;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn time_midnight() {
	let input = ColumnBuffer::time([Time::from_nanos_since_midnight(0).expect("midnight valid")]);
	let output = round_trip_column("t", input.clone());
	assert_column_eq("time_midnight", &input, &output);
}

#[test]
fn time_one_nanosecond_past_midnight() {
	let input = ColumnBuffer::time([Time::from_nanos_since_midnight(1).expect("valid")]);
	let output = round_trip_column("t", input.clone());
	assert_column_eq("time_one_nano", &input, &output);
}

#[test]
fn time_one_second_before_midnight() {
	// 23:59:59 = 24*3600 - 1 seconds = 86399 seconds = 86_399_000_000_000 ns
	let input = ColumnBuffer::time([Time::from_hms_nano(23, 59, 59, 999_999_999).expect("valid")]);
	let output = round_trip_column("t", input.clone());
	assert_column_eq("time_one_second_before_midnight", &input, &output);
}

#[test]
fn time_thirty_two_rows() {
	let values: Vec<Time> = (0..32u32)
		.map(|i| Time::from_hms_nano(i % 24, (i * 7) % 60, (i * 13) % 60, i * 100).expect("valid"))
		.collect();
	let input = ColumnBuffer::time(values);
	let output = round_trip_column("t", input.clone());
	assert_column_eq("time_thirty_two_rows", &input, &output);
}

#[test]
fn time_with_undefined() {
	let input = ColumnBuffer::time_optional([
		Some(Time::from_nanos_since_midnight(0).unwrap()),
		None,
		Some(Time::from_hms(12, 30, 0).unwrap()),
		None,
	]);
	let output = round_trip_column("t", input.clone());
	assert_column_eq("time_with_undefined", &input, &output);
}
