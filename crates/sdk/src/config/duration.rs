// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

use super::Config;

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
	use reifydb_value::value::{Value, duration::Duration, time::Time};

	use super::super::testutil::config;

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
