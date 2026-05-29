// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn u32(&self, key: &str) -> Option<u32> {
		self.opt_coerce(key)
	}

	pub fn require_u32(&self, key: &str) -> u32 {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "an unsigned integer"))
	}

	pub fn u32_or(&self, key: &str, default: u32) -> u32 {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_integer_widths_that_fit() {
		let cfg = config(vec![
			("a", Value::Uint1(1)),
			("b", Value::Uint4(60)),
			("c", Value::Int2(7)),
			("d", Value::Uint8(120)),
		]);
		assert_eq!(cfg.u32("a"), Some(1));
		assert_eq!(cfg.u32("b"), Some(60));
		assert_eq!(cfg.u32("c"), Some(7), "non-negative signed coerces to u32");
		assert_eq!(cfg.u32("d"), Some(120));
	}

	#[test]
	fn rejects_value_above_u32_max() {
		let cfg = config(vec![("a", Value::Uint8(u32::MAX as u64 + 1))]);
		assert_eq!(cfg.u32("a"), None, "value exceeding u32::MAX is range-checked, not silently truncated");
	}

	#[test]
	fn rejects_negative_and_mistyped() {
		let cfg = config(vec![("neg", Value::Int4(-3)), ("s", Value::utf8("x"))]);
		assert_eq!(cfg.u32("neg"), None, "negative does not coerce to unsigned");
		assert_eq!(cfg.u32("s"), None, "string is not an integer");
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::Uint4(9))]);
		assert_eq!(cfg.u32_or("present", 1), 9);
		assert_eq!(cfg.u32_or("absent", 1), 1);
		assert_eq!(cfg.require_u32("present"), 9);
	}

	#[test]
	#[should_panic(expected = "is missing or not an unsigned integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_u32("k");
	}
}
