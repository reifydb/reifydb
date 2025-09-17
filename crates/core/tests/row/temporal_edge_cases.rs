// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Temporal data edge case tests for the row encoding system

use reifydb_core::row::EncodedRowLayout;
use reifydb_type::*;

#[test]
fn test_date_boundaries() {
	let layout = EncodedRowLayout::new(&[Type::Date]);
	let mut row = layout.allocate_row();

	let dates = [
		Date::from_ymd(1, 1, 1).unwrap(),      // Minimum reasonable date
		Date::from_ymd(1970, 1, 1).unwrap(),   // Unix epoch
		Date::from_ymd(2000, 2, 29).unwrap(),  // Leap year
		Date::from_ymd(2100, 2, 28).unwrap(),  // Non-leap century
		Date::from_ymd(9999, 12, 31).unwrap(), // Far future
	];

	for date in dates {
		layout.set_date(&mut row, 0, date);
		assert_eq!(layout.get_date(&row, 0), date);
	}
}

#[test]
fn test_datetime_precision_limits() {
	let layout = EncodedRowLayout::new(&[Type::DateTime]);
	let mut row = layout.allocate_row();

	// Test nanosecond precision preservation
	let dt = DateTime::new(2024, 12, 25, 12, 34, 56, 123456789).unwrap();
	layout.set_datetime(&mut row, 0, dt);
	let retrieved = layout.get_datetime(&row, 0);
	assert_eq!(retrieved, dt);

	// Verify nanosecond precision
	let (sec1, nano1) = dt.to_parts();
	let (sec2, nano2) = retrieved.to_parts();
	assert_eq!(sec1, sec2);
	assert_eq!(nano1, nano2);
}

#[test]
fn test_time_edge_values() {
	let layout = EncodedRowLayout::new(&[Type::Time]);
	let mut row = layout.allocate_row();

	let times = [
		Time::new(0, 0, 0, 0).unwrap(),            // Midnight
		Time::new(12, 0, 0, 0).unwrap(),           // Noon
		Time::new(23, 59, 59, 999999999).unwrap(), // Last nanosecond of day
		Time::new(0, 0, 0, 1).unwrap(),            /* First nanosecond after
		                                            * midnight */
	];

	for time in times {
		layout.set_time(&mut row, 0, time);
		assert_eq!(layout.get_time(&row, 0), time);
	}
}

#[test]
fn test_interval_combinations() {
	let layout = EncodedRowLayout::new(&[Type::Interval]);
	let mut row = layout.allocate_row();

	let intervals = [
		Interval::from_seconds(0),
		Interval::from_seconds(-1),
		Interval::from_days(365),
		Interval::from_weeks(-52),
		Interval::new(12, 30, 123456789),            // Complex interval
		Interval::new(-12, -30, -123456789),         // Negative complex
		Interval::new(i32::MAX, i32::MAX, i64::MAX), // Max values
		Interval::new(i32::MIN, i32::MIN, i64::MIN), // Min values
	];

	for interval in intervals {
		layout.set_interval(&mut row, 0, interval);
		assert_eq!(layout.get_interval(&row, 0), interval);
	}
}
