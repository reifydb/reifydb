// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn i8(&self, key: &str) -> Option<i8> {
		self.opt_coerce(key)
	}

	pub fn require_i8(&self, key: &str) -> i8 {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "an integer"))
	}

	pub fn i8_or(&self, key: &str, default: i8) -> i8 {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_widths_that_fit() {
		let cfg = config(vec![("a", Value::Int1(-128)), ("b", Value::Int8(127)), ("c", Value::Uint4(5))]);
		assert_eq!(cfg.i8("a"), Some(-128));
		assert_eq!(cfg.i8("b"), Some(127), "wider signed within i8 range coerces down");
		assert_eq!(cfg.i8("c"), Some(5), "unsigned within range coerces to i8");
	}

	#[test]
	fn rejects_out_of_range() {
		let cfg = config(vec![("hi", Value::Int2(128)), ("lo", Value::Int2(-129)), ("u", Value::Uint1(200))]);
		assert_eq!(cfg.i8("hi"), None, "128 exceeds i8::MAX");
		assert_eq!(cfg.i8("lo"), None, "-129 below i8::MIN");
		assert_eq!(cfg.i8("u"), None, "200 exceeds i8::MAX");
	}

	#[test]
	fn rejects_non_integer() {
		let cfg = config(vec![("f", Value::float8(1.0)), ("s", Value::utf8("1"))]);
		assert_eq!(cfg.i8("f"), None);
		assert_eq!(cfg.i8("s"), None);
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::Int1(-9))]);
		assert_eq!(cfg.i8_or("present", 1), -9);
		assert_eq!(cfg.i8_or("absent", 1), 1);
		assert_eq!(cfg.require_i8("present"), -9);
	}

	#[test]
	#[should_panic(expected = "is missing or not an integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_i8("k");
	}
}
