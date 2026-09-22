// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

fn total_nanos(duration: &Duration) -> i128 {
	duration.get_days() as i128 * 86_400_000_000_000 + duration.get_nanos() as i128
}

#[test]
fn from_minutes_past_the_nanosecond_range_is_an_error_or_exact() {
	// Scaling minutes to nanos in i64 must never overflow: out of range input is an error or the exact value.
	for minutes in [i64::MIN, i64::MIN / 60_000_000_000 - 1, i64::MAX / 60_000_000_000 + 1, i64::MAX] {
		if let Ok(duration) = Duration::from_minutes(minutes) {
			assert_eq!(duration.get_months(), 0, "minutes {minutes}");
			assert_eq!(total_nanos(&duration), minutes as i128 * 60_000_000_000, "minutes {minutes}");
		}
	}
}

#[test]
fn from_hours_past_the_nanosecond_range_is_an_error_or_exact() {
	// Scaling hours to nanos in i64 must never overflow: out of range input is an error or the exact value.
	for hours in [i64::MIN, i64::MIN / 3_600_000_000_000 - 1, i64::MAX / 3_600_000_000_000 + 1, i64::MAX] {
		if let Ok(duration) = Duration::from_hours(hours) {
			assert_eq!(duration.get_months(), 0, "hours {hours}");
			assert_eq!(total_nanos(&duration), hours as i128 * 3_600_000_000_000, "hours {hours}");
		}
	}
}
