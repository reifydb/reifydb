// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::Config;
use crate::value::duration::Duration;

impl Config {
	pub fn lateness_and_amendable(&self) -> Option<(Duration, Option<Duration>)> {
		self.resolve_lateness_and_amendable().ok().flatten()
	}

	pub fn require_lateness_and_amendable(&self) -> (Duration, Option<Duration>) {
		match self.resolve_lateness_and_amendable() {
			Ok(Some(pair)) => pair,
			Ok(None) => self.missing("lateness", "a duration"),
			Err(violation) => panic!("{}: {}", self.name, violation),
		}
	}

	fn resolve_lateness_and_amendable(&self) -> Result<Option<(Duration, Option<Duration>)>, String> {
		match (self.duration("lateness"), self.duration("amendable")) {
			(Some(lateness), Some(amendable)) if amendable < lateness => {
				Ok(Some((lateness, Some(amendable))))
			}
			(Some(lateness), Some(amendable)) => {
				Err(format!("amendable {amendable} must be strictly less than lateness {lateness}"))
			}
			(Some(lateness), None) => Ok(Some((lateness, None))),
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
		let cfg =
			config(vec![("lateness", Value::Duration(secs(20))), ("amendable", Value::Duration(secs(15)))]);
		assert_eq!(cfg.lateness_and_amendable(), Some((secs(20), Some(secs(15)))));
	}

	#[test]
	fn lateness_without_amendable_resolves_with_no_amendable() {
		// Substituting the lateness would arm the sealing slots on a window that never asked to seal.
		let cfg = config(vec![("lateness", Value::Duration(secs(20)))]);
		assert_eq!(cfg.lateness_and_amendable(), Some((secs(20), None)));
	}

	#[test]
	fn declared_amendable_equal_to_lateness_is_rejected() {
		// The bound is strict; an amendable equal to the lateness would never seal before the window closes.
		let cfg =
			config(vec![("lateness", Value::Duration(secs(20))), ("amendable", Value::Duration(secs(20)))]);
		assert_eq!(cfg.lateness_and_amendable(), None);
	}

	#[test]
	fn declared_amendable_above_lateness_is_rejected() {
		let cfg =
			config(vec![("lateness", Value::Duration(secs(20))), ("amendable", Value::Duration(secs(30)))]);
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
	fn unzip_splits_the_pair_into_the_two_knobs() {
		let cfg =
			config(vec![("lateness", Value::Duration(secs(20))), ("amendable", Value::Duration(secs(15)))]);
		let (lateness, amendable) = cfg.lateness_and_amendable().unzip();
		assert_eq!(lateness, Some(secs(20)));
		assert_eq!(amendable, Some(Some(secs(15))));
	}

	#[test]
	fn require_returns_the_pair_when_both_are_declared() {
		let cfg =
			config(vec![("lateness", Value::Duration(secs(20))), ("amendable", Value::Duration(secs(15)))]);
		assert_eq!(cfg.require_lateness_and_amendable(), (secs(20), Some(secs(15))));
	}

	#[test]
	fn require_returns_no_amendable_when_only_the_lateness_is_declared() {
		// A window under the amendable floor declares a lateness alone and must still resolve.
		let cfg = config(vec![("lateness", Value::Duration(secs(20)))]);
		assert_eq!(cfg.require_lateness_and_amendable(), (secs(20), None));
	}

	#[test]
	#[should_panic(expected = "must be strictly less than lateness")]
	fn require_names_the_ordering_violation() {
		let cfg =
			config(vec![("lateness", Value::Duration(secs(20))), ("amendable", Value::Duration(secs(20)))]);
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
