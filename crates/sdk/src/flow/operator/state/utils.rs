// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::key::operator::state::GroupStateKey;

pub fn empty_key() -> EncodedKey {
	EncodedKey::new(Vec::new())
}

pub fn empty_state_key() -> GroupStateKey {
	GroupStateKey::from_framed(empty_key()).expect("the empty key is framing-valid")
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
