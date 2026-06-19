// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn f64(&self, key: &str) -> Option<f64> {
		self.opt_coerce(key)
	}

	pub fn require_f64(&self, key: &str) -> f64 {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "a number"))
	}

	pub fn f64_or(&self, key: &str, default: f64) -> f64 {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_both_float_widths() {
		let cfg = config(vec![("f8", Value::float8(0.70)), ("f4", Value::float4(0.5f32))]);
		assert_eq!(cfg.f64("f8"), Some(0.70));
		assert_eq!(cfg.f64("f4"), Some(0.5), "Float4 widens to f64");
	}

	#[test]
	fn casts_every_unsigned_width() {
		let cfg = config(vec![
			("a", Value::Uint1(1)),
			("b", Value::Uint2(2)),
			("c", Value::Uint4(3)),
			("d", Value::Uint8(4)),
			("e", Value::Uint16(5)),
		]);
		assert_eq!(cfg.f64("a"), Some(1.0));
		assert_eq!(cfg.f64("b"), Some(2.0));
		assert_eq!(cfg.f64("c"), Some(3.0));
		assert_eq!(cfg.f64("d"), Some(4.0));
		assert_eq!(cfg.f64("e"), Some(5.0), "Uint16 coerces to f64");
	}

	#[test]
	fn casts_every_signed_width() {
		let cfg = config(vec![
			("a", Value::Int1(-1)),
			("b", Value::Int2(-2)),
			("c", Value::Int4(-3)),
			("d", Value::Int8(-4)),
			("e", Value::Int16(-5)),
		]);
		assert_eq!(cfg.f64("a"), Some(-1.0));
		assert_eq!(cfg.f64("b"), Some(-2.0));
		assert_eq!(cfg.f64("c"), Some(-3.0));
		assert_eq!(cfg.f64("d"), Some(-4.0));
		assert_eq!(cfg.f64("e"), Some(-5.0), "Int16 coerces to f64");
	}

	#[test]
	fn rejects_non_numeric_values() {
		let cfg = config(vec![("s", Value::utf8("1.5")), ("b", Value::Boolean(true))]);
		assert_eq!(cfg.f64("s"), None, "strings are not numbers");
		assert_eq!(cfg.f64("b"), None, "booleans are not numbers");
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::float8(0.70))]);
		assert_eq!(cfg.f64_or("present", 1.0), 0.70);
		assert_eq!(cfg.f64_or("absent", 0.70), 0.70);
		assert_eq!(cfg.require_f64("present"), 0.70);
	}

	#[test]
	#[should_panic(expected = "is missing or not a number")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_f64("k");
	}
}
