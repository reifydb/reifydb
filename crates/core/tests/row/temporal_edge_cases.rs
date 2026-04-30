// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::encoded::shape::RowShape;
use reifydb_type::value::{date::Date, datetime::DateTime, duration::Duration, time::Time, r#type::Type};

#[test]
fn test_date_boundaries() {
	let shape = RowShape::testing(&[Type::Date]);
	let mut row = shape.allocate();

	let dates = [
		Date::from_ymd(1, 1, 1).unwrap(),      // Minimum reasonable date
		Date::from_ymd(1970, 1, 1).unwrap(),   // Unix epoch
		Date::from_ymd(2000, 2, 29).unwrap(),  // Leap year
		Date::from_ymd(2100, 2, 28).unwrap(),  // Non-leap century
		Date::from_ymd(9999, 12, 31).unwrap(), // Far future
	];

	for date in dates {
		shape.set_date(&mut row, 0, date);
		assert_eq!(shape.get_date(&row, 0), date);
	}
}

#[test]
fn test_datetime_precision_limits() {
	let shape = RowShape::testing(&[Type::DateTime]);
	let mut row = shape.allocate();

	// Test nanosecond precision preservation
	let dt = DateTime::new(2024, 12, 25, 12, 34, 56, 123456789).unwrap();
	shape.set_datetime(&mut row, 0, dt);
	let retrieved = shape.get_datetime(&row, 0);
	assert_eq!(retrieved, dt);

	// Verify nanosecond precision
	assert_eq!(dt.to_nanos(), retrieved.to_nanos());
}

#[test]
fn test_time_edge_values() {
	let shape = RowShape::testing(&[Type::Time]);
	let mut row = shape.allocate();

	let times = [
		Time::new(0, 0, 0, 0).unwrap(),            // Midnight
		Time::new(12, 0, 0, 0).unwrap(),           // Noon
		Time::new(23, 59, 59, 999999999).unwrap(), // Last nanosecond of day
		Time::new(0, 0, 0, 1).unwrap(),            /* First nanosecond after
		                                            * midnight */
	];

	for time in times {
		shape.set_time(&mut row, 0, time);
		assert_eq!(shape.get_time(&row, 0), time);
	}
}

#[test]
fn test_interval_combinations() {
	let shape = RowShape::testing(&[Type::Duration]);
	let mut row = shape.allocate();

	let intervals = [
		Duration::from_seconds(0).unwrap(),
		Duration::from_seconds(-1).unwrap(),
		Duration::from_days(365).unwrap(),
		Duration::from_weeks(-52).unwrap(),
		Duration::new(12, 30, 123456789).unwrap(),    // Complex interval
		Duration::new(-12, -30, -123456789).unwrap(), // Negative complex
		Duration::new(i32::MAX, i32::MAX, 86_399_999_999_999).unwrap(), // Large positive (nanos < 1 day)
		Duration::new(i32::MIN, i32::MIN, -86_399_999_999_999).unwrap(), // Large negative (nanos > -1 day)
	];

	for interval in intervals {
		shape.set_duration(&mut row, 0, interval);
		assert_eq!(shape.get_duration(&row, 0), interval);
	}
}
