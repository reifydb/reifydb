// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::date::Date;

use super::common::{assert_column_eq, round_trip_column};

#[test]
fn date_epoch() {
	// 1970-01-01 = 0 days since epoch.
	let input = ColumnBuffer::date([Date::from_days_since_epoch(0).expect("epoch is valid")]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("date_epoch", &input, &output);
}

#[test]
fn date_negative_far_past() {
	// Pre-epoch dates use negative days.
	let input = ColumnBuffer::date([Date::from_days_since_epoch(-365 * 100).expect("100 years pre-epoch valid")]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("date_far_past", &input, &output);
}

#[test]
fn date_far_future() {
	let input = ColumnBuffer::date([Date::from_days_since_epoch(365 * 1000).expect("1000 years post-epoch valid")]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("date_far_future", &input, &output);
}

#[test]
fn date_leap_day() {
	// 2024-02-29 - leap day.
	let input = ColumnBuffer::date([Date::from_ymd(2024, 2, 29).expect("leap day valid")]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("date_leap_day", &input, &output);
}

#[test]
fn date_thirty_two_rows() {
	let values: Vec<Date> = (0..32i32).map(|i| Date::from_days_since_epoch(i * 100).expect("valid")).collect();
	let input = ColumnBuffer::date(values);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("date_thirty_two_rows", &input, &output);
}

#[test]
fn date_with_undefined() {
	let input = ColumnBuffer::date_optional([
		Some(Date::from_days_since_epoch(0).unwrap()),
		None,
		Some(Date::from_ymd(2024, 2, 29).unwrap()),
		None,
	]);
	let output = round_trip_column("d", input.clone());
	assert_column_eq("date_with_undefined", &input, &output);
}
