// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::key::operator::{
	keyspace::root::CustomNotCachedSuffix,
	state::{GroupId, GroupStateKey, custom_not_cached_key, custom_not_cached_key_in},
};

use crate::error::{Result, SdkError};

pub fn empty_key() -> EncodedKey {
	EncodedKey::new(Vec::new())
}

pub fn empty_state_key() -> GroupStateKey {
	custom_not_cached_key(&[]).expect("an empty id fits the keyspace")
}

pub fn custom_state_key(id: &[u8]) -> Result<GroupStateKey> {
	custom_not_cached_key(id).ok_or_else(|| too_wide(id))
}

pub fn custom_state_key_in(group: GroupId, id: &[u8]) -> Result<GroupStateKey> {
	custom_not_cached_key_in(group, id).ok_or_else(|| too_wide(id))
}

fn too_wide(id: &[u8]) -> SdkError {
	SdkError::InvalidInput(format!(
		"a custom operator state key is at most {} bytes, got {}",
		CustomNotCachedSuffix::ID_LEN,
		id.len()
	))
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
