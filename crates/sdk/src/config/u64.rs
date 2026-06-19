// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn u64(&self, key: &str) -> Option<u64> {
		self.opt_coerce(key)
	}

	pub fn require_u64(&self, key: &str) -> u64 {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "an unsigned integer"))
	}

	pub fn u64_or(&self, key: &str, default: u64) -> u64 {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_every_unsigned_width() {
		let cfg = config(vec![
			("a", Value::Uint1(1)),
			("b", Value::Uint2(2)),
			("c", Value::Uint4(3)),
			("d", Value::Uint8(4)),
			("e", Value::Uint16(5)),
		]);
		assert_eq!(cfg.u64("a"), Some(1));
		assert_eq!(cfg.u64("b"), Some(2));
		assert_eq!(cfg.u64("c"), Some(3));
		assert_eq!(cfg.u64("d"), Some(4));
		assert_eq!(cfg.u64("e"), Some(5), "Uint16 within range coerces to u64");
	}

	#[test]
	fn casts_non_negative_signed_width() {
		let cfg = config(vec![
			("a", Value::Int1(1)),
			("b", Value::Int2(2)),
			("c", Value::Int4(3)),
			("d", Value::Int8(4)),
			("e", Value::Int16(5)),
		]);
		assert_eq!(cfg.u64("a"), Some(1));
		assert_eq!(cfg.u64("b"), Some(2));
		assert_eq!(cfg.u64("c"), Some(3));
		assert_eq!(cfg.u64("d"), Some(4));
		assert_eq!(cfg.u64("e"), Some(5), "non-negative Int16 coerces to u64");
	}

	#[test]
	fn rejects_negative_signed() {
		let cfg = config(vec![("a", Value::Int1(-1)), ("b", Value::Int4(-3)), ("c", Value::Int16(-7))]);
		assert_eq!(cfg.u64("a"), None, "negative does not coerce to unsigned");
		assert_eq!(cfg.u64("b"), None, "negative does not coerce to unsigned");
		assert_eq!(cfg.u64("c"), None, "negative does not coerce to unsigned");
	}

	#[test]
	fn rejects_uint16_above_u64_max() {
		let cfg = config(vec![("a", Value::Uint16(u64::MAX as u128 + 1))]);
		assert_eq!(cfg.u64("a"), None, "Uint16 above u64::MAX is range-checked and rejected");
	}

	#[test]
	fn rejects_non_integer_values() {
		let cfg = config(vec![("f", Value::float8(1.0)), ("s", Value::utf8("3")), ("b", Value::Boolean(true))]);
		assert_eq!(cfg.u64("f"), None, "floats do not coerce to u64");
		assert_eq!(cfg.u64("s"), None, "strings are not integers");
		assert_eq!(cfg.u64("b"), None, "booleans are not integers");
	}

	#[test]
	fn opt_and_or_handle_absent() {
		let cfg = config(vec![("present", Value::Uint4(9))]);
		assert_eq!(cfg.u64("absent"), None);
		assert_eq!(cfg.u64_or("present", 1), 9);
		assert_eq!(cfg.u64_or("absent", 1), 1);
	}

	#[test]
	fn require_returns_value_when_present() {
		let cfg = config(vec![("window_duration", Value::Uint8(60))]);
		assert_eq!(cfg.require_u64("window_duration"), 60);
	}

	#[test]
	#[should_panic(expected = "test_op: required config 'window_duration' is missing or not an unsigned integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_u64("window_duration");
	}
}
