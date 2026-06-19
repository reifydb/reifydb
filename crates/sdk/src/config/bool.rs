// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::Config;

impl Config {
	pub fn bool(&self, key: &str) -> Option<bool> {
		self.opt(key)
	}

	pub fn require_bool(&self, key: &str) -> bool {
		self.opt(key).unwrap_or_else(|| self.missing(key, "a boolean"))
	}

	pub fn bool_or(&self, key: &str, default: bool) -> bool {
		self.opt(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use super::super::testutil::config;

	#[test]
	fn casts_boolean_values() {
		let cfg = config(vec![("t", Value::Boolean(true)), ("f", Value::Boolean(false))]);
		assert_eq!(cfg.bool("t"), Some(true));
		assert_eq!(cfg.bool("f"), Some(false));
	}

	#[test]
	fn rejects_non_boolean_values() {
		let cfg = config(vec![
			("n", Value::Uint4(1)),
			("z", Value::Uint4(0)),
			("f", Value::float8(1.0)),
			("s", Value::utf8("true")),
		]);
		assert_eq!(cfg.bool("n"), None, "integers do not coerce to bool");
		assert_eq!(cfg.bool("z"), None, "zero does not coerce to false");
		assert_eq!(cfg.bool("f"), None, "floats do not coerce to bool");
		assert_eq!(cfg.bool("s"), None, "the string \"true\" is not a boolean");
	}

	#[test]
	fn or_and_require_behavior() {
		let cfg = config(vec![("present", Value::Boolean(true))]);
		assert!(cfg.bool_or("present", false), "present value wins over default");
		assert!(cfg.bool_or("absent", true), "default returned when absent");
		assert!(cfg.require_bool("present"));
	}

	#[test]
	#[should_panic(expected = "is missing or not a boolean")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_bool("k");
	}
}
