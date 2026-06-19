// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn i32(&self, key: &str) -> Option<i32> {
		self.opt_coerce(key)
	}

	pub fn require_i32(&self, key: &str) -> i32 {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "an integer"))
	}

	pub fn i32_or(&self, key: &str, default: i32) -> i32 {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_widths_that_fit() {
		let cfg =
			config(vec![("a", Value::Int2(-1000)), ("b", Value::Int8(2147483647)), ("c", Value::Uint4(5))]);
		assert_eq!(cfg.i32("a"), Some(-1000));
		assert_eq!(cfg.i32("b"), Some(2147483647));
		assert_eq!(cfg.i32("c"), Some(5), "unsigned within range coerces to i32");
	}

	#[test]
	fn rejects_out_of_range() {
		let cfg = config(vec![("hi", Value::Int8(2147483648)), ("u", Value::Uint8(u32::MAX as u64))]);
		assert_eq!(cfg.i32("hi"), None, "2147483648 exceeds i32::MAX");
		assert_eq!(cfg.i32("u"), None, "u32::MAX exceeds i32::MAX");
	}

	#[test]
	fn rejects_non_integer() {
		let cfg = config(vec![("f", Value::float8(1.0)), ("s", Value::utf8("1"))]);
		assert_eq!(cfg.i32("f"), None);
		assert_eq!(cfg.i32("s"), None);
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::Int4(-9))]);
		assert_eq!(cfg.i32_or("present", 1), -9);
		assert_eq!(cfg.i32_or("absent", 1), 1);
		assert_eq!(cfg.require_i32("present"), -9);
	}

	#[test]
	#[should_panic(expected = "is missing or not an integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_i32("k");
	}
}
