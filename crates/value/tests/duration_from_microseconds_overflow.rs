// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

fn total_nanos(duration: &Duration) -> i128 {
	duration.get_days() as i128 * 86_400_000_000_000 + duration.get_nanos() as i128
}

#[test]
fn from_microseconds_past_the_nanosecond_range_is_an_error_or_exact() {
	// Scaling micros to nanos in i64 must never overflow: out of range input is an error or the exact value.
	for micros in [i64::MIN, i64::MIN / 1_000 - 1, i64::MAX / 1_000 + 1, i64::MAX] {
		if let Ok(duration) = Duration::from_microseconds(micros) {
			assert_eq!(duration.get_months(), 0, "micros {micros}");
			assert_eq!(total_nanos(&duration), micros as i128 * 1_000, "micros {micros}");
		}
	}
}
