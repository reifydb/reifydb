// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

fn total_nanos(duration: &Duration) -> i128 {
	duration.get_days() as i128 * 86_400_000_000_000 + duration.get_nanos() as i128
}

#[test]
fn from_seconds_past_the_nanosecond_range_is_an_error_or_exact() {
	// Scaling seconds to nanos in i64 must never overflow: out of range input is an error or the exact value.
	for seconds in [i64::MIN, i64::MIN / 1_000_000_000 - 1, i64::MAX / 1_000_000_000 + 1, i64::MAX] {
		if let Ok(duration) = Duration::from_seconds(seconds) {
			assert_eq!(duration.get_months(), 0, "seconds {seconds}");
			assert_eq!(total_nanos(&duration), seconds as i128 * 1_000_000_000, "seconds {seconds}");
		}
	}
}

#[test]
fn from_milliseconds_past_the_nanosecond_range_is_an_error_or_exact() {
	// Scaling millis to nanos in i64 must never overflow: out of range input is an error or the exact value.
	for millis in [i64::MIN, i64::MIN / 1_000_000 - 1, i64::MAX / 1_000_000 + 1, i64::MAX] {
		if let Ok(duration) = Duration::from_milliseconds(millis) {
			assert_eq!(duration.get_months(), 0, "millis {millis}");
			assert_eq!(total_nanos(&duration), millis as i128 * 1_000_000, "millis {millis}");
		}
	}
}
