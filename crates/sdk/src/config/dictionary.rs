// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::dictionary::DictionaryEntryId;

use super::Config;

impl Config {
	pub fn dictionary_id(&self, key: &str) -> Option<DictionaryEntryId> {
		self.get(key).and_then(DictionaryEntryId::from_value)
	}

	pub fn require_dictionary_id(&self, key: &str) -> DictionaryEntryId {
		self.dictionary_id(key).unwrap_or_else(|| self.missing(key, "a dictionary id"))
	}

	pub fn dictionary_id_or(&self, key: &str, default: DictionaryEntryId) -> DictionaryEntryId {
		self.dictionary_id(key).unwrap_or(default)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, dictionary::DictionaryEntryId};

	use super::super::testutil::config;

	#[test]
	fn casts_dictionary_id_values() {
		let id = DictionaryEntryId::U4(42);
		let cfg = config(vec![("id", Value::DictionaryId(id))]);
		assert_eq!(cfg.dictionary_id("id"), Some(id));
	}

	#[test]
	fn maps_unsigned_integers_by_width() {
		let cfg = config(vec![("u1", Value::Uint1(7)), ("u4", Value::Uint4(42)), ("u16", Value::Uint16(99))]);
		assert_eq!(cfg.dictionary_id("u1"), Some(DictionaryEntryId::U1(7)));
		assert_eq!(cfg.dictionary_id("u4"), Some(DictionaryEntryId::U4(42)));
		assert_eq!(
			cfg.dictionary_id("u16"),
			Some(DictionaryEntryId::U16(99)),
			"unsigned integers map to a dictionary id by their width"
		);
	}

	#[test]
	fn rejects_signed_and_non_numeric() {
		let cfg = config(vec![("i", Value::Int4(42)), ("b", Value::Boolean(true)), ("s", Value::utf8("42"))]);
		assert_eq!(cfg.dictionary_id("i"), None, "a signed integer is not a dictionary id");
		assert_eq!(cfg.dictionary_id("b"), None, "a boolean is not a dictionary id");
		assert_eq!(cfg.dictionary_id("s"), None, "a string is not a dictionary id");
	}

	#[test]
	fn or_and_require_behavior() {
		let id = DictionaryEntryId::U4(42);
		let default = DictionaryEntryId::U1(0);
		let cfg = config(vec![("present", Value::DictionaryId(id))]);
		assert_eq!(cfg.dictionary_id_or("present", default), id);
		assert_eq!(cfg.dictionary_id_or("absent", default), default);
		assert_eq!(cfg.require_dictionary_id("present"), id);
	}

	#[test]
	#[should_panic(expected = "is missing or not a dictionary id")]
	fn require_panics_when_missing() {
		let cfg = config(vec![]);
		cfg.require_dictionary_id("k");
	}
}
