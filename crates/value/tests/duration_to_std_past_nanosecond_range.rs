// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration as StdDuration;

use reifydb_value::value::duration::Duration;

#[test]
fn to_std_past_the_nanosecond_range_keeps_the_exact_duration() {
	// A positive duration past the i64 nanosecond range must convert exactly, never collapse to zero.
	let duration = Duration::from_micros_infallible(10_000_000_000_000_000);
	assert_eq!(duration.to_std(), StdDuration::from_micros(10_000_000_000_000_000));
}
