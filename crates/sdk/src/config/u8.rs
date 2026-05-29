// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn u8(&self, key: &str) -> Option<u8> {
		self.opt_coerce(key)
	}

	pub fn require_u8(&self, key: &str) -> u8 {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "an unsigned integer"))
	}

	pub fn u8_or(&self, key: &str, default: u8) -> u8 {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_widths_that_fit() {
		let cfg = config(vec![("a", Value::Uint1(255)), ("b", Value::Uint8(200)), ("c", Value::Int4(5))]);
		assert_eq!(cfg.u8("a"), Some(255));
		assert_eq!(cfg.u8("b"), Some(200), "wider unsigned within u8 range coerces down");
		assert_eq!(cfg.u8("c"), Some(5), "non-negative signed coerces to u8");
	}

	#[test]
	fn rejects_out_of_range_and_negative() {
		let cfg = config(vec![("hi", Value::Uint2(256)), ("neg", Value::Int1(-1))]);
		assert_eq!(cfg.u8("hi"), None, "256 exceeds u8::MAX");
		assert_eq!(cfg.u8("neg"), None, "negative does not coerce to unsigned");
	}

	#[test]
	fn rejects_non_integer() {
		let cfg = config(vec![("f", Value::float8(1.0)), ("s", Value::utf8("1"))]);
		assert_eq!(cfg.u8("f"), None);
		assert_eq!(cfg.u8("s"), None);
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::Uint1(9))]);
		assert_eq!(cfg.u8_or("present", 1), 9);
		assert_eq!(cfg.u8_or("absent", 1), 1);
		assert_eq!(cfg.require_u8("present"), 9);
	}

	#[test]
	#[should_panic(expected = "is missing or not an unsigned integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_u8("k");
	}
}
