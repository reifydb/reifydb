// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn f32(&self, key: &str) -> Option<f32> {
		self.opt_coerce(key)
	}

	pub fn require_f32(&self, key: &str) -> f32 {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "a number"))
	}

	pub fn f32_or(&self, key: &str, default: f32) -> f32 {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_both_float_widths() {
		let cfg = config(vec![("f4", Value::float4(0.5f32)), ("f8", Value::float8(0.25))]);
		assert_eq!(cfg.f32("f4"), Some(0.5));
		assert_eq!(cfg.f32("f8"), Some(0.25), "Float8 narrows to f32");
	}

	#[test]
	fn casts_integer_widths() {
		let cfg = config(vec![("u", Value::Uint8(4)), ("i", Value::Int2(-3)), ("big", Value::Uint16(5))]);
		assert_eq!(cfg.f32("u"), Some(4.0));
		assert_eq!(cfg.f32("i"), Some(-3.0));
		assert_eq!(cfg.f32("big"), Some(5.0), "Uint16 coerces to f32");
	}

	#[test]
	fn rejects_non_numeric_values() {
		let cfg = config(vec![("s", Value::utf8("1.5")), ("b", Value::Boolean(true))]);
		assert_eq!(cfg.f32("s"), None, "strings are not numbers");
		assert_eq!(cfg.f32("b"), None, "booleans are not numbers");
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::float4(0.5f32))]);
		assert_eq!(cfg.f32_or("present", 1.0), 0.5);
		assert_eq!(cfg.f32_or("absent", 0.25), 0.25);
		assert_eq!(cfg.require_f32("present"), 0.5);
	}

	#[test]
	#[should_panic(expected = "is missing or not a number")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_f32("k");
	}
}
