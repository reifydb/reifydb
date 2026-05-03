// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::encoded::key::EncodedKey;

pub fn empty_key() -> EncodedKey {
	EncodedKey::new(Vec::new())
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_empty_key() {
		let key = empty_key();
		assert!(key.as_bytes().is_empty());
	}

	#[test]
	fn test_empty_key_consistency() {
		let key1 = empty_key();
		let key2 = empty_key();
		assert_eq!(key1.as_bytes(), key2.as_bytes());
	}
}
