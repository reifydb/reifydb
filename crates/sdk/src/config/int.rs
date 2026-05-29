// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::int::Int;

use super::Config;

impl Config {
	pub fn int(&self, key: &str) -> Option<Int> {
		self.opt(key)
	}

	pub fn require_int(&self, key: &str) -> Int {
		self.opt(key).unwrap_or_else(|| self.missing(key, "an integer"))
	}

	pub fn int_or(&self, key: &str, default: Int) -> Int {
		self.opt(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, int::Int};

	use super::super::testutil::config;

	#[test]
	fn casts_bignum_int_values() {
		let n = Int::from_i64(42);
		let cfg = config(vec![("n", Value::Int(n.clone()))]);
		assert_eq!(cfg.int("n"), Some(n));
	}

	#[test]
	fn fixed_width_int_does_not_satisfy_bignum() {
		let cfg = config(vec![("fixed", Value::Int8(42))]);
		assert_eq!(
			cfg.int("fixed"),
			None,
			"a fixed-width Int8 is a distinct variant from the arbitrary-precision Int"
		);
	}

	#[test]
	fn or_and_require_behavior() {
		let n = Int::from_i64(42);
		let default = Int::from_i64(0);
		let cfg = config(vec![("present", Value::Int(n.clone()))]);
		assert_eq!(cfg.int_or("present", default.clone()), n);
		assert_eq!(cfg.int_or("absent", default.clone()), default);
		assert_eq!(cfg.require_int("present"), n);
	}

	#[test]
	#[should_panic(expected = "is missing or not an integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_int("k");
	}
}
