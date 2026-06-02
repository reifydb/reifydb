// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::date::Date;

use super::Config;

impl Config {
	pub fn date(&self, key: &str) -> Option<Date> {
		self.opt_coerce(key)
	}

	pub fn require_date(&self, key: &str) -> Date {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "a date"))
	}

	pub fn date_or(&self, key: &str, default: Date) -> Date {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, date::Date, datetime::DateTime};

	use super::super::testutil::config;

	#[test]
	fn casts_date_values() {
		let date = Date::new(2024, 3, 14).unwrap();
		let cfg = config(vec![("d", Value::Date(date))]);
		assert_eq!(cfg.date("d"), Some(date));
	}

	#[test]
	fn rejects_other_temporal_and_numeric() {
		let cfg = config(vec![("dt", Value::DateTime(DateTime::from_nanos(0))), ("n", Value::Uint4(20240314))]);
		assert_eq!(cfg.date("dt"), None, "a datetime does not coerce to a date");
		assert_eq!(cfg.date("n"), None, "an integer does not coerce to a date");
	}

	#[test]
	fn or_and_require_behavior() {
		let date = Date::new(2024, 3, 14).unwrap();
		let default = Date::new(1970, 1, 1).unwrap();
		let cfg = config(vec![("present", Value::Date(date))]);
		assert_eq!(cfg.date_or("present", default), date);
		assert_eq!(cfg.date_or("absent", default), default);
		assert_eq!(cfg.require_date("present"), date);
	}

	#[test]
	#[should_panic(expected = "is missing or not a date")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_date("k");
	}
}
