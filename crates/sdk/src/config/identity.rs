// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::identity::IdentityId;

use super::Config;

impl Config {
	pub fn identity_id(&self, key: &str) -> Option<IdentityId> {
		self.opt(key)
	}

	pub fn require_identity_id(&self, key: &str) -> IdentityId {
		self.opt(key).unwrap_or_else(|| self.missing(key, "an identity id"))
	}

	pub fn identity_id_or(&self, key: &str, default: IdentityId) -> IdentityId {
		self.opt(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::{Value, identity::IdentityId};

	use super::super::testutil::config;

	#[test]
	fn casts_identity_id_values() {
		let id = IdentityId::root();
		let cfg = config(vec![("id", Value::IdentityId(id))]);
		assert_eq!(cfg.identity_id("id"), Some(id));
	}

	#[test]
	fn rejects_other_values() {
		let cfg = config(vec![("n", Value::Uint8(1)), ("s", Value::utf8("root"))]);
		assert_eq!(cfg.identity_id("n"), None, "an integer is not an identity id");
		assert_eq!(cfg.identity_id("s"), None, "a string is not an identity id");
	}

	#[test]
	fn or_and_require_behavior() {
		let id = IdentityId::root();
		let default = IdentityId::anonymous();
		let cfg = config(vec![("present", Value::IdentityId(id))]);
		assert_eq!(cfg.identity_id_or("present", default), id);
		assert_eq!(cfg.identity_id_or("absent", default), default);
		assert_eq!(cfg.require_identity_id("present"), id);
	}

	#[test]
	#[should_panic(expected = "is missing or not an identity id")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_identity_id("k");
	}
}
