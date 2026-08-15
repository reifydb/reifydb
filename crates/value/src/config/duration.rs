// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::Config;
use crate::value::duration::Duration;

impl Config {
	pub fn duration(&self, key: &str) -> Option<Duration> {
		self.opt(key)
	}

	pub fn require_duration(&self, key: &str) -> Duration {
		self.opt(key).unwrap_or_else(|| self.missing(key, "a duration"))
	}

	pub fn duration_or(&self, key: &str, default: Duration) -> Duration {
		self.opt(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use super::super::testutil::config;
	use crate::value::{Value, duration::Duration, time::Time};

	#[test]
	fn casts_duration_values() {
		let d = Duration::from_seconds(60).unwrap();
		let cfg = config(vec![("d", Value::Duration(d))]);
		assert_eq!(cfg.duration("d"), Some(d));
	}

	#[test]
	fn rejects_other_temporal_and_numeric() {
		let cfg = config(vec![("t", Value::Time(Time::midnight())), ("n", Value::Uint8(60))]);
		assert_eq!(cfg.duration("t"), None, "a time does not coerce to a duration");
		assert_eq!(cfg.duration("n"), None, "a raw integer does not coerce to a duration");
	}

	#[test]
	fn rejects_duration_literal_string() {
		let cfg = config(vec![("d", Value::utf8("1m")), ("sub", Value::utf8("1s"))]);
		assert_eq!(cfg.duration("d"), None, "a duration literal string is not a duration");
		assert_eq!(cfg.duration("sub"), None, "a sub-minute duration literal string is not a duration either");
		assert_eq!(
			cfg.duration_or("sub", Duration::from_seconds(1).unwrap()),
			Duration::from_seconds(1).unwrap(),
			"a string falls through to the default rather than parsing sub-minute"
		);
	}

	#[test]
	#[should_panic(expected = "is missing or not a duration")]
	fn require_panics_on_duration_literal_string() {
		let cfg = config(vec![("d", Value::utf8("1m"))]);
		cfg.require_duration("d");
	}

	#[test]
	fn accepts_sub_minute_duration_value() {
		let sub = Duration::from_seconds(1).unwrap();
		let cfg = config(vec![("sub", Value::Duration(sub))]);
		assert_eq!(
			cfg.require_duration("sub"),
			sub,
			"a sub-minute duration must stay sub-minute, not round up"
		);
	}

	#[test]
	fn or_and_require_behavior() {
		let d = Duration::from_seconds(60).unwrap();
		let default = Duration::zero();
		let cfg = config(vec![("present", Value::Duration(d))]);
		assert_eq!(cfg.duration_or("present", default), d);
		assert_eq!(cfg.duration_or("absent", default), default);
		assert_eq!(cfg.require_duration("present"), d);
	}

	#[test]
	#[should_panic(expected = "is missing or not a duration")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_duration("k");
	}
}
