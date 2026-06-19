// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::time::Time;

use super::Config;

impl Config {
	pub fn time(&self, key: &str) -> Option<Time> {
		self.opt_coerce(key)
	}

	pub fn require_time(&self, key: &str) -> Time {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "a time"))
	}

	pub fn time_or(&self, key: &str, default: Time) -> Time {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, date::Date, time::Time};

	use super::super::testutil::config;

	#[test]
	fn casts_time_values() {
		let t = Time::new(13, 30, 0, 0).unwrap();
		let cfg = config(vec![("t", Value::Time(t))]);
		assert_eq!(cfg.time("t"), Some(t));
	}

	#[test]
	fn rejects_other_temporal_and_numeric() {
		let cfg = config(vec![("d", Value::Date(Date::new(2024, 1, 1).unwrap())), ("n", Value::Uint8(48600))]);
		assert_eq!(cfg.time("d"), None, "a date does not coerce to a time");
		assert_eq!(cfg.time("n"), None, "an integer does not coerce to a time");
	}

	#[test]
	fn or_and_require_behavior() {
		let t = Time::new(13, 30, 0, 0).unwrap();
		let default = Time::midnight();
		let cfg = config(vec![("present", Value::Time(t))]);
		assert_eq!(cfg.time_or("present", default), t);
		assert_eq!(cfg.time_or("absent", default), default);
		assert_eq!(cfg.require_time("present"), t);
	}

	#[test]
	#[should_panic(expected = "is missing or not a time")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_time("k");
	}
}
