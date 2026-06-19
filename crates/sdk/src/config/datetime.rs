// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::datetime::DateTime;

use super::Config;

impl Config {
	pub fn datetime(&self, key: &str) -> Option<DateTime> {
		self.opt_coerce(key)
	}

	pub fn require_datetime(&self, key: &str) -> DateTime {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "a datetime"))
	}

	pub fn datetime_or(&self, key: &str, default: DateTime) -> DateTime {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, date::Date, datetime::DateTime, time::Time};

	use super::super::testutil::config;

	#[test]
	fn casts_datetime_values() {
		let dt = DateTime::from_nanos(1_700_000_000_000_000_000);
		let cfg = config(vec![("dt", Value::DateTime(dt))]);
		assert_eq!(cfg.datetime("dt"), Some(dt));
	}

	#[test]
	fn rejects_other_temporal() {
		let cfg = config(vec![
			("d", Value::Date(Date::new(2024, 1, 1).unwrap())),
			("t", Value::Time(Time::midnight())),
		]);
		assert_eq!(cfg.datetime("d"), None, "a date does not coerce to a datetime");
		assert_eq!(cfg.datetime("t"), None, "a time does not coerce to a datetime");
	}

	#[test]
	fn or_and_require_behavior() {
		let dt = DateTime::from_nanos(1_700_000_000_000_000_000);
		let default = DateTime::from_nanos(0);
		let cfg = config(vec![("present", Value::DateTime(dt))]);
		assert_eq!(cfg.datetime_or("present", default), dt);
		assert_eq!(cfg.datetime_or("absent", default), default);
		assert_eq!(cfg.require_datetime("present"), dt);
	}

	#[test]
	#[should_panic(expected = "is missing or not a datetime")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_datetime("k");
	}
}
