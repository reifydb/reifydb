// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_sub_console::backoff::Backoff;
use reifydb_value::value::duration::Duration;

fn millis(n: i64) -> Duration {
	Duration::from_milliseconds(n).expect("milliseconds")
}

#[test]
fn the_first_wait_is_half_a_second_and_each_wait_doubles() {
	// A slow first retry delays every reconnect; a missing doubling hammers a struggling tunnel server.
	let mut backoff = Backoff::new();
	let waits: Vec<Duration> = (0..7).map(|_| backoff.next()).collect();
	assert_eq!(
		waits,
		vec![millis(500), millis(1000), millis(2000), millis(4000), millis(8000), millis(16000), millis(30000)]
	);
}

#[test]
fn the_wait_never_exceeds_thirty_seconds() {
	// Without the cap a long outage would push the next retry out by hours.
	let mut backoff = Backoff::new();
	for _ in 0..64 {
		assert!(backoff.next() <= millis(30000));
	}
	assert_eq!(backoff.next(), millis(30000));
}

#[test]
fn reset_starts_over_at_half_a_second() {
	// A session that registered once must retry fast again, never at the capped wait of an old outage.
	let mut backoff = Backoff::new();
	for _ in 0..10 {
		backoff.next();
	}
	backoff.reset();
	assert_eq!(backoff.next(), millis(500));
	assert_eq!(backoff.next(), millis(1000));
}
