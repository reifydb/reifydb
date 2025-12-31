// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[cfg(test)]
pub mod test {
	use reifydb_core::{CowVec, EncodedKey, value::encoded::EncodedValues};

	/// Create a test key from a string
	pub fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	/// Create a test value from a string
	pub fn make_value(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	/// Helper to extract values from query results for comparison
	pub async fn from_store(
		parent: &mut reifydb_engine::StandardCommandTransaction,
		key: &EncodedKey,
	) -> Option<EncodedValues> {
		parent.get(key).await.unwrap().map(|m| m.values.clone())
	}
}
