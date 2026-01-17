// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Temporal data edge case tests for the encoded encoding system

use reifydb_core::encoded::schema::Schema;
use reifydb_type::value::{date::Date, datetime::DateTime, duration::Duration, time::Time, r#type::Type};

#[test]
fn test_date_boundaries() {
	let schema = Schema::testing(&[Type::Date]);
	let mut row = schema.allocate();

	let dates = [
		Date::from_ymd(1, 1, 1).unwrap(),      // Minimum reasonable date
		Date::from_ymd(1970, 1, 1).unwrap(),   // Unix epoch
		Date::from_ymd(2000, 2, 29).unwrap(),  // Leap year
		Date::from_ymd(2100, 2, 28).unwrap(),  // Non-leap century
		Date::from_ymd(9999, 12, 31).unwrap(), // Far future
	];

	for date in dates {
		schema.set_date(&mut row, 0, date);
		assert_eq!(schema.get_date(&row, 0), date);
	}
}

#[test]
fn test_datetime_precision_limits() {
	let schema = Schema::testing(&[Type::DateTime]);
	let mut row = schema.allocate();

	// Test nanosecond precision preservation
	let dt = DateTime::new(2024, 12, 25, 12, 34, 56, 123456789).unwrap();
	schema.set_datetime(&mut row, 0, dt);
	let retrieved = schema.get_datetime(&row, 0);
	assert_eq!(retrieved, dt);

	// Verify nanosecond precision
	let (sec1, nano1) = dt.to_parts();
	let (sec2, nano2) = retrieved.to_parts();
	assert_eq!(sec1, sec2);
	assert_eq!(nano1, nano2);
}

#[test]
fn test_time_edge_values() {
	let schema = Schema::testing(&[Type::Time]);
	let mut row = schema.allocate();

	let times = [
		Time::new(0, 0, 0, 0).unwrap(),            // Midnight
		Time::new(12, 0, 0, 0).unwrap(),           // Noon
		Time::new(23, 59, 59, 999999999).unwrap(), // Last nanosecond of day
		Time::new(0, 0, 0, 1).unwrap(),            /* First nanosecond after
		                                            * midnight */
	];

	for time in times {
		schema.set_time(&mut row, 0, time);
		assert_eq!(schema.get_time(&row, 0), time);
	}
}

#[test]
fn test_interval_combinations() {
	let schema = Schema::testing(&[Type::Duration]);
	let mut row = schema.allocate();

	let intervals = [
		Duration::from_seconds(0),
		Duration::from_seconds(-1),
		Duration::from_days(365),
		Duration::from_weeks(-52),
		Duration::new(12, 30, 123456789),            // Complex interval
		Duration::new(-12, -30, -123456789),         // Negative complex
		Duration::new(i32::MAX, i32::MAX, i64::MAX), // Max values
		Duration::new(i32::MIN, i32::MIN, i64::MIN), // Min values
	];

	for interval in intervals {
		schema.set_duration(&mut row, 0, interval);
		assert_eq!(schema.get_duration(&row, 0), interval);
	}
}
