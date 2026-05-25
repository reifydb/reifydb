// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn i16(&self, key: &str) -> Option<i16> {
		self.opt_coerce(key)
	}

	pub fn require_i16(&self, key: &str) -> i16 {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "an integer"))
	}

	pub fn i16_or(&self, key: &str, default: i16) -> i16 {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_widths_that_fit() {
		let cfg = config(vec![("a", Value::Int1(-100)), ("b", Value::Int4(32767)), ("c", Value::Uint2(300))]);
		assert_eq!(cfg.i16("a"), Some(-100));
		assert_eq!(cfg.i16("b"), Some(32767));
		assert_eq!(cfg.i16("c"), Some(300), "unsigned within range coerces to i16");
	}

	#[test]
	fn rejects_out_of_range() {
		let cfg = config(vec![("hi", Value::Int4(32768)), ("u", Value::Uint4(40000))]);
		assert_eq!(cfg.i16("hi"), None, "32768 exceeds i16::MAX");
		assert_eq!(cfg.i16("u"), None, "40000 exceeds i16::MAX");
	}

	#[test]
	fn rejects_non_integer() {
		let cfg = config(vec![("f", Value::float4(1.0f32)), ("b", Value::Boolean(true))]);
		assert_eq!(cfg.i16("f"), None);
		assert_eq!(cfg.i16("b"), None);
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::Int2(-9))]);
		assert_eq!(cfg.i16_or("present", 1), -9);
		assert_eq!(cfg.i16_or("absent", 1), 1);
		assert_eq!(cfg.require_i16("present"), -9);
	}

	#[test]
	#[should_panic(expected = "is missing or not an integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_i16("k");
	}
}
