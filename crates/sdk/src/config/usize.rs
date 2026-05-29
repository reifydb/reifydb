// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn usize(&self, key: &str) -> Option<usize> {
		self.opt_coerce(key)
	}

	pub fn require_usize(&self, key: &str) -> usize {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "an unsigned integer"))
	}

	pub fn usize_or(&self, key: &str, default: usize) -> usize {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_integer_widths_that_fit() {
		let cfg = config(vec![("a", Value::Uint2(2)), ("b", Value::Uint8(120)), ("c", Value::Int4(7))]);
		assert_eq!(cfg.usize("a"), Some(2));
		assert_eq!(cfg.usize("b"), Some(120));
		assert_eq!(cfg.usize("c"), Some(7), "non-negative signed coerces to usize");
	}

	#[test]
	fn rejects_negative_and_mistyped() {
		let cfg = config(vec![("neg", Value::Int4(-3)), ("b", Value::Boolean(false))]);
		assert_eq!(cfg.usize("neg"), None, "negative does not coerce to unsigned");
		assert_eq!(cfg.usize("b"), None, "boolean is not an integer");
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::Uint8(9))]);
		assert_eq!(cfg.usize_or("present", 1), 9);
		assert_eq!(cfg.usize_or("absent", 1), 1);
		assert_eq!(cfg.require_usize("present"), 9);
	}

	#[test]
	#[should_panic(expected = "is missing or not an unsigned integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_usize("k");
	}
}
