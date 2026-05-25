// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn i64(&self, key: &str) -> Option<i64> {
		self.opt_coerce(key)
	}

	pub fn require_i64(&self, key: &str) -> i64 {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "an integer"))
	}

	pub fn i64_or(&self, key: &str, default: i64) -> i64 {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_every_signed_width() {
		let cfg = config(vec![
			("a", Value::Int1(-1)),
			("b", Value::Int2(-2)),
			("c", Value::Int4(-3)),
			("d", Value::Int8(-4)),
			("e", Value::Int16(-5)),
		]);
		assert_eq!(cfg.i64("a"), Some(-1));
		assert_eq!(cfg.i64("b"), Some(-2));
		assert_eq!(cfg.i64("c"), Some(-3));
		assert_eq!(cfg.i64("d"), Some(-4));
		assert_eq!(cfg.i64("e"), Some(-5), "negative Int16 within range coerces to i64");
	}

	#[test]
	fn casts_unsigned_widths_within_range() {
		let cfg = config(vec![
			("a", Value::Uint1(1)),
			("b", Value::Uint2(2)),
			("c", Value::Uint4(3)),
			("d", Value::Uint8(4)),
			("e", Value::Uint16(5)),
		]);
		assert_eq!(cfg.i64("a"), Some(1));
		assert_eq!(cfg.i64("b"), Some(2));
		assert_eq!(cfg.i64("c"), Some(3));
		assert_eq!(cfg.i64("d"), Some(4));
		assert_eq!(cfg.i64("e"), Some(5), "unsigned that fits coerces to i64");
	}

	#[test]
	fn rejects_unsigned_above_i64_max() {
		let cfg = config(vec![("u8", Value::Uint8(u64::MAX)), ("u16", Value::Uint16(i64::MAX as u128 + 1))]);
		assert_eq!(cfg.i64("u8"), None, "Uint8 above i64::MAX is range-checked and rejected");
		assert_eq!(cfg.i64("u16"), None, "Uint16 above i64::MAX is range-checked and rejected");
	}

	#[test]
	fn rejects_non_integer_values() {
		let cfg = config(vec![("f", Value::float8(2.0)), ("s", Value::utf8("1"))]);
		assert_eq!(cfg.i64("f"), None, "floats do not coerce to i64");
		assert_eq!(cfg.i64("s"), None, "strings are not integers");
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::Int4(-9))]);
		assert_eq!(cfg.i64_or("present", 1), -9);
		assert_eq!(cfg.i64_or("absent", 1), 1);
		assert_eq!(cfg.require_i64("present"), -9);
	}

	#[test]
	#[should_panic(expected = "is missing or not an integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_i64("k");
	}
}
