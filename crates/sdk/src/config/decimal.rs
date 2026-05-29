// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::decimal::Decimal;

use super::Config;

impl Config {
	pub fn decimal(&self, key: &str) -> Option<Decimal> {
		self.opt(key)
	}

	pub fn require_decimal(&self, key: &str) -> Decimal {
		self.opt(key).unwrap_or_else(|| self.missing(key, "a decimal"))
	}

	pub fn decimal_or(&self, key: &str, default: Decimal) -> Decimal {
		self.opt(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, decimal::Decimal};

	use super::super::testutil::config;

	#[test]
	fn casts_decimal_values() {
		let d = Decimal::from_i64(50);
		let cfg = config(vec![("d", Value::Decimal(d.clone()))]);
		assert_eq!(cfg.decimal("d"), Some(d));
	}

	#[test]
	fn fixed_width_number_does_not_satisfy_decimal() {
		let cfg = config(vec![("f", Value::float8(50.0)), ("i", Value::Int8(50))]);
		assert_eq!(cfg.decimal("f"), None, "a float is a distinct variant from a decimal");
		assert_eq!(cfg.decimal("i"), None, "a fixed-width integer is not a decimal");
	}

	#[test]
	fn or_and_require_behavior() {
		let d = Decimal::from_i64(50);
		let default = Decimal::from_i64(0);
		let cfg = config(vec![("present", Value::Decimal(d.clone()))]);
		assert_eq!(cfg.decimal_or("present", default.clone()), d);
		assert_eq!(cfg.decimal_or("absent", default.clone()), default);
		assert_eq!(cfg.require_decimal("present"), d);
	}

	#[test]
	#[should_panic(expected = "is missing or not a decimal")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_decimal("k");
	}
}
