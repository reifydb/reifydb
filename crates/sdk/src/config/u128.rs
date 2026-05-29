// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn u128(&self, key: &str) -> Option<u128> {
		self.opt_coerce(key)
	}

	pub fn require_u128(&self, key: &str) -> u128 {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "an unsigned integer"))
	}

	pub fn u128_or(&self, key: &str, default: u128) -> u128 {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_every_unsigned_width() {
		let cfg = config(vec![("a", Value::Uint1(1)), ("b", Value::Uint16(u128::MAX)), ("c", Value::Int8(7))]);
		assert_eq!(cfg.u128("a"), Some(1));
		assert_eq!(cfg.u128("b"), Some(u128::MAX), "Uint16 round-trips through u128");
		assert_eq!(cfg.u128("c"), Some(7), "non-negative signed coerces to u128");
	}

	#[test]
	fn rejects_negative() {
		let cfg = config(vec![("neg", Value::Int16(-1))]);
		assert_eq!(cfg.u128("neg"), None, "negative does not coerce to unsigned");
	}

	#[test]
	fn rejects_non_integer() {
		let cfg = config(vec![("f", Value::float8(1.0)), ("s", Value::utf8("1"))]);
		assert_eq!(cfg.u128("f"), None);
		assert_eq!(cfg.u128("s"), None);
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::Uint16(9))]);
		assert_eq!(cfg.u128_or("present", 1), 9);
		assert_eq!(cfg.u128_or("absent", 1), 1);
		assert_eq!(cfg.require_u128("present"), 9);
	}

	#[test]
	#[should_panic(expected = "is missing or not an unsigned integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_u128("k");
	}
}
