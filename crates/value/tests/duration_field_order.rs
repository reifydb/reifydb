// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeSet, HashSet};

use reifydb_value::value::duration::Duration;

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

#[test]
fn a_month_and_thirty_days_are_distinct_values() {
	// A month has no fixed length, so equality must never fold it into thirty days in any container.
	let month = Duration::from_months(1).unwrap();
	let thirty_days = Duration::from_days(30).unwrap();

	assert_ne!(month, thirty_days);
	assert_eq!(HashSet::from([month, thirty_days]).len(), 2);
	assert_eq!(BTreeSet::from([month, thirty_days]).len(), 2);
}

#[test]
fn months_outrank_days_and_days_outrank_nanos() {
	// Order must compare months first, then days, then nanos, never a total in nanos.
	let month = Duration::from_months(1).unwrap();

	assert!(month > Duration::from_days(31).unwrap());
	assert!(month > Duration::from_days(1_000).unwrap());
	assert!(Duration::from_months(-1).unwrap() < Duration::from_days(-31).unwrap());
	assert!(Duration::new(1, -40, 0).unwrap() > Duration::zero());
	assert!(Duration::from_days(1).unwrap() > Duration::from_nanoseconds(NANOS_PER_DAY - 1).unwrap());
}

#[test]
fn sorting_gives_one_order_whatever_the_input_order() {
	// A total order must never leave two distinct durations tied, otherwise the input order leaks into the sort.
	let expected = vec![
		Duration::from_hours(1).unwrap(),
		Duration::from_days(30).unwrap(),
		Duration::from_days(45).unwrap(),
		Duration::from_months(1).unwrap(),
		Duration::new(1, 0, 3_600_000_000_000).unwrap(),
	];
	let mut forward = vec![expected[3], expected[2], expected[0], expected[4], expected[1]];
	let mut backward: Vec<Duration> = forward.iter().rev().copied().collect();

	forward.sort();
	backward.sort();

	assert_eq!(forward, expected);
	assert_eq!(backward, expected);
}
