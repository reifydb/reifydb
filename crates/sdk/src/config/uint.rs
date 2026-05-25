// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::uint::Uint;

use super::Config;

impl Config {
	pub fn uint(&self, key: &str) -> Option<Uint> {
		self.opt(key)
	}

	pub fn require_uint(&self, key: &str) -> Uint {
		self.opt(key).unwrap_or_else(|| self.missing(key, "an unsigned integer"))
	}

	pub fn uint_or(&self, key: &str, default: Uint) -> Uint {
		self.opt(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::{Value, uint::Uint};

	use super::super::testutil::config;

	#[test]
	fn casts_bignum_uint_values() {
		let n = Uint::from_u64(100);
		let cfg = config(vec![("n", Value::Uint(n.clone()))]);
		assert_eq!(cfg.uint("n"), Some(n));
	}

	#[test]
	fn fixed_width_uint_does_not_satisfy_bignum() {
		let cfg = config(vec![("fixed", Value::Uint8(100))]);
		assert_eq!(
			cfg.uint("fixed"),
			None,
			"a fixed-width Uint8 is a distinct variant from the arbitrary-precision Uint"
		);
	}

	#[test]
	fn or_and_require_behavior() {
		let n = Uint::from_u64(100);
		let default = Uint::from_u64(0);
		let cfg = config(vec![("present", Value::Uint(n.clone()))]);
		assert_eq!(cfg.uint_or("present", default.clone()), n);
		assert_eq!(cfg.uint_or("absent", default.clone()), default);
		assert_eq!(cfg.require_uint("present"), n);
	}

	#[test]
	#[should_panic(expected = "is missing or not an unsigned integer")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_uint("k");
	}
}
