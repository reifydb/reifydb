// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn i128(&self, key: &str) -> Option<i128> {
		self.opt_coerce(key)
	}

	pub fn require_i128(&self, key: &str) -> i128 {
		self.opt_coerce(key).unwrap_or_else(|| self.missing(key, "an integer"))
	}

	pub fn i128_or(&self, key: &str, default: i128) -> i128 {
		self.opt_coerce(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_every_width_that_fits() {
		let cfg = config(vec![
			("a", Value::Int16(-170141183460469231731687303715884105728)),
			("b", Value::Uint8(u64::MAX)),
			("c", Value::Int4(-3)),
		]);
		assert_eq!(cfg.i128("a"), Some(i128::MIN), "Int16 round-trips through i128");
		assert_eq!(cfg.i128("b"), Some(u64::MAX as i128), "u64::MAX fits in i128");
		assert_eq!(cfg.i128("c"), Some(-3));
	}

	#[test]
	fn rejects_uint16_above_i128_max() {
		let cfg = config(vec![("u", Value::Uint16(i128::MAX as u128 + 1))]);
		assert_eq!(cfg.i128("u"), None, "Uint16 above i128::MAX is range-checked and rejected");
	}

	#[test]
	fn rejects_non_integer() {
		let cfg = config(vec![("f", Value::float8(1.0)), ("b", Value::Boolean(true))]);
		assert_eq!(cfg.i128("f"), None);
		assert_eq!(cfg.i128("b"), None);
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::Int16(-9))]);
		assert_eq!(cfg.i128_or("present", 1), -9);
		assert_eq!(cfg.i128_or("absent", 1), 1);
		assert_eq!(cfg.require_i128("present"), -9);
	}

	#[test]
	#[should_panic(expected = "is missing or not an integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_i128("k");
	}
}
