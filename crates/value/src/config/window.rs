// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::Config;
use crate::value::duration::Duration;

impl Config {
	pub fn lateness_and_amendable(&self) -> Option<(Duration, Duration)> {
		self.resolve_lateness_and_amendable().ok().flatten()
	}

	pub fn require_lateness_and_amendable(&self) -> (Duration, Duration) {
		match self.resolve_lateness_and_amendable() {
			Ok(Some(pair)) => pair,
			Ok(None) => self.missing("lateness", "a duration"),
			Err(violation) => panic!("{}: {}", self.name, violation),
		}
	}

	fn resolve_lateness_and_amendable(&self) -> Result<Option<(Duration, Duration)>, String> {
		match (self.duration("lateness"), self.duration("amendable")) {
			(Some(lateness), Some(amendable)) if amendable < lateness => Ok(Some((lateness, amendable))),
			(Some(lateness), Some(amendable)) => {
				Err(format!("amendable {amendable} must be strictly less than lateness {lateness}"))
			}
			(Some(lateness), None) => Ok(Some((lateness, lateness))),
			(None, Some(amendable)) => Err(format!("amendable {amendable} requires lateness to be set")),
			(None, None) => Ok(None),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::super::testutil::config;
	use crate::value::{Value, duration::Duration};

	fn secs(n: i64) -> Duration {
		Duration::from_seconds(n).unwrap()
	}

	#[test]
	fn declared_amendable_below_lateness_is_kept_verbatim() {
		let cfg = config(vec![
			("lateness", Value::Duration(secs(20))),
			("amendable", Value::Duration(secs(15))),
		]);
		assert_eq!(cfg.lateness_and_amendable(), Some((secs(20), secs(15))));
	}

	#[test]
	fn unset_amendable_falls_back_to_the_lateness() {
		// An unset amendable must bound the retained tail by the lateness, never leave it unbounded.
		let cfg = config(vec![("lateness", Value::Duration(secs(20)))]);
		assert_eq!(cfg.lateness_and_amendable(), Some((secs(20), secs(20))));
	}

	#[test]
	fn a_defaulted_pair_does_not_trip_the_strict_guard() {
		// The guard must read the declared amendable, otherwise the fallback rejects every window that omits it.
		let cfg = config(vec![("lateness", Value::Duration(secs(1)))]);
		assert_eq!(cfg.lateness_and_amendable(), Some((secs(1), secs(1))));
	}

	#[test]
	fn declared_amendable_equal_to_lateness_is_rejected() {
		// The bound is strict; an amendable equal to the lateness would never seal before the window closes.
		let cfg = config(vec![
			("lateness", Value::Duration(secs(20))),
			("amendable", Value::Duration(secs(20))),
		]);
		assert_eq!(cfg.lateness_and_amendable(), None);
	}

	#[test]
	fn declared_amendable_above_lateness_is_rejected() {
		let cfg = config(vec![
			("lateness", Value::Duration(secs(20))),
			("amendable", Value::Duration(secs(30))),
		]);
		assert_eq!(cfg.lateness_and_amendable(), None);
	}

	#[test]
	fn amendable_without_lateness_is_rejected() {
		// Without a lateness there is no window to amend inside, so the pair must not resolve.
		let cfg = config(vec![("amendable", Value::Duration(secs(15)))]);
		assert_eq!(cfg.lateness_and_amendable(), None);
	}

	#[test]
	fn neither_knob_declared_is_none() {
		let cfg = config(vec![("duration", Value::Duration(secs(60)))]);
		assert_eq!(cfg.lateness_and_amendable(), None);
	}

	#[test]
	fn unzip_splits_the_pair_into_the_two_optional_knobs() {
		let cfg = config(vec![("lateness", Value::Duration(secs(20)))]);
		let (lateness, amendable) = cfg.lateness_and_amendable().unzip();
		assert_eq!(lateness, Some(secs(20)));
		assert_eq!(amendable, Some(secs(20)));
	}

	#[test]
	fn require_returns_the_pair_when_declared() {
		let cfg = config(vec![("lateness", Value::Duration(secs(20)))]);
		assert_eq!(cfg.require_lateness_and_amendable(), (secs(20), secs(20)));
	}

	#[test]
	#[should_panic(expected = "must be strictly less than lateness")]
	fn require_names_the_ordering_violation() {
		let cfg = config(vec![
			("lateness", Value::Duration(secs(20))),
			("amendable", Value::Duration(secs(20))),
		]);
		cfg.require_lateness_and_amendable();
	}

	#[test]
	#[should_panic(expected = "requires lateness to be set")]
	fn require_names_the_missing_lateness_behind_an_amendable() {
		let cfg = config(vec![("amendable", Value::Duration(secs(15)))]);
		cfg.require_lateness_and_amendable();
	}

	#[test]
	#[should_panic(expected = "is missing or not a duration")]
	fn require_panics_when_no_knob_is_declared() {
		let cfg = config(vec![("duration", Value::Duration(secs(60)))]);
		cfg.require_lateness_and_amendable();
	}
}
