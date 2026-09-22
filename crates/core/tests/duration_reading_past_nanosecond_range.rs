// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::metrics::sample::Reading;
use reifydb_value::value::duration::Duration;

#[test]
fn a_duration_reading_past_the_nanosecond_range_reads_as_its_exact_micros() {
	// A duration reading past the i64 nanosecond range must read as its micros, never as zero.
	let reading = Reading::Duration(Duration::from_micros_infallible(10_000_000_000_000_000));
	assert_eq!(reading.as_f64(), 1e16);
}
