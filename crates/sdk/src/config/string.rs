// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn str(&self, key: &str) -> Option<String> {
		self.opt(key)
	}

	pub fn require_str(&self, key: &str) -> String {
		self.opt(key).unwrap_or_else(|| self.missing(key, "a string"))
	}

	pub fn str_or(&self, key: &str, default: &str) -> String {
		self.opt::<String>(key).unwrap_or_else(|| default.to_string())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_utf8_values() {
		let cfg = config(vec![("s", Value::utf8("vwap"))]);
		assert_eq!(cfg.str("s").as_deref(), Some("vwap"));
	}

	#[test]
	fn rejects_non_string_values() {
		let cfg = config(vec![("n", Value::Uint4(1)), ("f", Value::float8(1.0)), ("b", Value::Boolean(true))]);
		assert_eq!(cfg.str("n"), None, "integers are not strings");
		assert_eq!(cfg.str("f"), None, "floats are not strings");
		assert_eq!(cfg.str("b"), None, "booleans are not strings");
	}

	#[test]
	fn str_or_returns_default_on_absent_or_mistyped() {
		let cfg = config(vec![("present", Value::utf8("vwap")), ("wrong", Value::Uint4(1))]);
		assert_eq!(cfg.str_or("present", "base_mint"), "vwap");
		assert_eq!(cfg.str_or("absent", "base_mint"), "base_mint");
		assert_eq!(cfg.str_or("wrong", "base_mint"), "base_mint", "mistyped falls back to default");
	}

	#[test]
	fn require_returns_value_when_present() {
		let cfg = config(vec![("col", Value::utf8("vwap"))]);
		assert_eq!(cfg.require_str("col"), "vwap");
	}

	#[test]
	#[should_panic(expected = "is missing or not a string")]
	fn require_str_panics_on_wrong_type() {
		let cfg = config(vec![("k", Value::Uint4(1))]);
		cfg.require_str("k");
	}
}
