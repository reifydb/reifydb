// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn u16(&self, key: &str) -> Option<u16> {
		self.opt_coerce(key)
	}

	pub fn require_u16(&self, key: &str) -> u16 {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "an unsigned integer"))
	}

	pub fn u16_or(&self, key: &str, default: u16) -> u16 {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_widths_that_fit() {
		let cfg = config(vec![("a", Value::Uint2(65535)), ("b", Value::Uint8(1000)), ("c", Value::Int4(7))]);
		assert_eq!(cfg.u16("a"), Some(65535));
		assert_eq!(cfg.u16("b"), Some(1000));
		assert_eq!(cfg.u16("c"), Some(7), "non-negative signed coerces to u16");
	}

	#[test]
	fn rejects_out_of_range_and_negative() {
		let cfg = config(vec![("hi", Value::Uint4(65536)), ("neg", Value::Int2(-1))]);
		assert_eq!(cfg.u16("hi"), None, "65536 exceeds u16::MAX");
		assert_eq!(cfg.u16("neg"), None, "negative does not coerce to unsigned");
	}

	#[test]
	fn rejects_non_integer() {
		let cfg = config(vec![("f", Value::float8(1.0)), ("b", Value::Boolean(false))]);
		assert_eq!(cfg.u16("f"), None);
		assert_eq!(cfg.u16("b"), None);
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::Uint2(9))]);
		assert_eq!(cfg.u16_or("present", 1), 9);
		assert_eq!(cfg.u16_or("absent", 1), 1);
		assert_eq!(cfg.require_u16("present"), 9);
	}

	#[test]
	#[should_panic(expected = "is missing or not an unsigned integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_u16("k");
	}
}
