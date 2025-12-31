// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Version management for MVCC (Multi-Version Concurrency Control).
//!
//! This module handles versioning by encoding version into keys.
//! Key format: `[original_key_bytes][version_be_u64]`
//!
//! This allows efficient range queries to find all versions of a key,
//! and finding the latest version <= a requested version.

use std::ops::Bound;

use reifydb_core::CommitVersion;
use reifydb_type::Result;

use crate::backend::{PrimitiveStorage, TableId};

/// Size of version suffix in bytes (u64 big-endian)
pub(crate) const VERSION_SIZE: usize = 8;

/// Encode a versioned key: `[key_bytes][version_be]`
///
/// Big-endian version ensures proper ordering: lower versions sort first.
pub fn encode_versioned_key(key: &[u8], version: CommitVersion) -> Vec<u8> {
	let mut result = Vec::with_capacity(key.len() + VERSION_SIZE);
	result.extend_from_slice(key);
	result.extend_from_slice(&version.0.to_be_bytes());
	result
}

/// Decode a versioned key back to (key, version)
#[cfg(test)]
pub fn decode_versioned_key(versioned_key: &[u8]) -> Option<(&[u8], CommitVersion)> {
	if versioned_key.len() < VERSION_SIZE {
		return None;
	}
	let key = &versioned_key[..versioned_key.len() - VERSION_SIZE];
	let version_bytes: [u8; 8] = versioned_key[versioned_key.len() - VERSION_SIZE..].try_into().ok()?;
	let version = CommitVersion(u64::from_be_bytes(version_bytes));
	Some((key, version))
}

/// Extract just the original key from a versioned key
pub fn extract_key(versioned_key: &[u8]) -> Option<&[u8]> {
	if versioned_key.len() < VERSION_SIZE {
		return None;
	}
	Some(&versioned_key[..versioned_key.len() - VERSION_SIZE])
}

/// Extract just the version from a versioned key
pub fn extract_version(versioned_key: &[u8]) -> Option<CommitVersion> {
	if versioned_key.len() < VERSION_SIZE {
		return None;
	}
	let version_bytes: [u8; 8] = versioned_key[versioned_key.len() - VERSION_SIZE..].try_into().ok()?;
	Some(CommitVersion(u64::from_be_bytes(version_bytes)))
}

/// Create range bounds to find all versions of a key
pub fn key_version_range(key: &[u8]) -> (Vec<u8>, Vec<u8>) {
	let start = encode_versioned_key(key, CommitVersion(0));
	let end = encode_versioned_key(key, CommitVersion(u64::MAX));
	(start, end)
}

/// Result of a versioned get operation
#[derive(Debug, Clone)]
pub enum VersionedGetResult {
	/// Found a value at this version
	Value {
		value: Vec<u8>,
		version: CommitVersion,
	},
	/// Found a tombstone (deletion) at this version
	Tombstone,
	/// Key not found at or before the requested version
	NotFound,
}

/// Get the latest version of a key at or before the given version.
pub async fn get_at_version<S: PrimitiveStorage>(
	storage: &S,
	table: TableId,
	key: &[u8],
	version: CommitVersion,
) -> Result<VersionedGetResult> {
	// Create range to cover all versions of this key up to the requested version
	let start = encode_versioned_key(key, CommitVersion(0));
	let end = encode_versioned_key(key, version);

	// Scan in reverse to find the latest version first (just need 1 entry)
	let batch = storage.range_rev_batch(table, Bound::Included(start), Bound::Included(end), 1).await?;

	if let Some(entry) = batch.entries.first() {
		// Verify this entry is for our key
		if let Some(entry_key) = extract_key(&entry.key) {
			if entry_key == key {
				let entry_version = extract_version(&entry.key).unwrap_or(CommitVersion(0));
				return Ok(match &entry.value {
					Some(value) => VersionedGetResult::Value {
						value: value.clone(),
						version: entry_version,
					},
					None => VersionedGetResult::Tombstone,
				});
			}
		}
	}

	Ok(VersionedGetResult::NotFound)
}

/// Async version of get_latest_version - get the latest version number for a key (if any exists).
#[allow(dead_code)]
pub async fn get_latest_version<S: PrimitiveStorage>(
	storage: &S,
	table: TableId,
	key: &[u8],
) -> Result<Option<CommitVersion>> {
	let (start, end) = key_version_range(key);

	// Scan in reverse to find the latest version (just need 1 entry)
	let batch = storage.range_rev_batch(table, Bound::Included(start), Bound::Included(end), 1).await?;

	if let Some(entry) = batch.entries.first() {
		if let Some(entry_key) = extract_key(&entry.key) {
			if entry_key == key {
				return Ok(extract_version(&entry.key));
			}
		}
	}

	Ok(None)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_encode_decode_versioned_key() {
		let key = b"test_key";
		let version = CommitVersion(42);

		let encoded = encode_versioned_key(key, version);
		assert_eq!(encoded.len(), key.len() + 8);

		let (decoded_key, decoded_version) = decode_versioned_key(&encoded).unwrap();
		assert_eq!(decoded_key, key);
		assert_eq!(decoded_version, version);
	}

	#[test]
	fn test_version_ordering() {
		let key = b"key";

		let v1 = encode_versioned_key(key, CommitVersion(1));
		let v2 = encode_versioned_key(key, CommitVersion(2));
		let v10 = encode_versioned_key(key, CommitVersion(10));

		// Lower versions should sort before higher versions
		assert!(v1 < v2);
		assert!(v2 < v10);
	}
}
