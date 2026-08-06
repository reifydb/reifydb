// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::value::{datetime::DateTime, duration::Duration};

pub fn millis(value: u64) -> Duration {
	Duration::from_milliseconds_const(value as i64)
}

pub fn secs(value: u64) -> Duration {
	Duration::from_seconds_const(value as i64)
}

pub fn at_millis(value: u64) -> DateTime {
	DateTime::from_millis(value)
}

pub fn at_nanos(value: u64) -> DateTime {
	DateTime::from_nanos(value)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_two_datetime_factories_disagree_by_exactly_a_million() {
		// These replace a workspace of hand-rolled `fn at(u64) -> DateTime` helpers that silently
		// disagreed on their unit: five read millis, three read nanos, all named `at`. Pinning the
		// ratio is what makes a future edit that "simplifies" one of them into the other fail here
		// instead of in whichever suite happened to depend on the offset.
		assert_eq!(at_millis(1), at_nanos(1_000_000));
		assert_ne!(at_millis(1), at_nanos(1));
	}

	#[test]
	fn the_duration_factories_carry_their_named_unit() {
		// A fixture that windows on `millis(60)` and one that ages on `secs(60)` must not be the
		// same span; conflating them is how a retention test passes while retaining nothing.
		assert_eq!(millis(1_000).milliseconds().expect("millis"), secs(1).milliseconds().expect("millis"));
		assert_eq!(millis(60).milliseconds().expect("millis"), 60);
		assert_eq!(secs(60).milliseconds().expect("millis"), 60_000);
	}

	#[test]
	fn the_epoch_is_the_zero_of_both_datetime_factories() {
		// An unstamped row reads as the epoch rather than as absent, so fixtures compare against
		// this value to tell "never stamped" from "stamped at zero". Both factories must agree on
		// where that point is.
		assert_eq!(at_millis(0), at_nanos(0));
	}
}
