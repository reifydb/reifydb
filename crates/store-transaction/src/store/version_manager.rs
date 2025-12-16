// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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
use tracing::instrument;

use crate::backend::{PrimitiveStorage, TableId};

/// Size of version suffix in bytes (u64 big-endian)
const VERSION_SIZE: usize = 8;

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

#[cfg(test)]
impl VersionedGetResult {
	pub fn is_value(&self) -> bool {
		matches!(self, Self::Value { .. })
	}

	pub fn is_tombstone(&self) -> bool {
		matches!(self, Self::Tombstone)
	}

	pub fn into_value(self) -> Option<Vec<u8>> {
		match self {
			Self::Value {
				value,
				..
			} => Some(value),
			_ => None,
		}
	}
}

/// Get the latest version of a key at or before the given version.
///
/// This performs a range scan to find all versions of the key,
/// then returns the latest one that's <= the requested version.
pub fn get_at_version<S: PrimitiveStorage>(
	storage: &S,
	table: TableId,
	key: &[u8],
	version: CommitVersion,
) -> Result<VersionedGetResult> {
	// Create range to cover all versions of this key up to the requested version
	let start = encode_versioned_key(key, CommitVersion(0));
	let end = encode_versioned_key(key, version);

	// Scan in reverse to find the latest version first
	let mut iter = storage.range_rev(
		table,
		Bound::Included(start.as_slice()),
		Bound::Included(end.as_slice()),
		1, // Just need one entry
	)?;

	if let Some(entry_result) = iter.next() {
		let entry = entry_result?;

		// Verify this entry is for our key
		if let Some(entry_key) = extract_key(&entry.key) {
			if entry_key == key {
				let entry_version = extract_version(&entry.key).unwrap_or(CommitVersion(0));
				return Ok(match entry.value {
					Some(value) => VersionedGetResult::Value {
						value,
						version: entry_version,
					},
					None => VersionedGetResult::Tombstone,
				});
			}
		}
	}

	Ok(VersionedGetResult::NotFound)
}

/// Check if a key exists (is not a tombstone) at the given version.
#[cfg(test)]
pub fn contains_at_version<S: PrimitiveStorage>(
	storage: &S,
	table: TableId,
	key: &[u8],
	version: CommitVersion,
) -> Result<bool> {
	Ok(get_at_version(storage, table, key, version)?.is_value())
}

/// Store a value at a specific version.
#[instrument(level = "trace", skip(storage, value), fields(table = ?table, key_len = key.len(), has_value = value.is_some()))]
pub fn put_at_version<S: PrimitiveStorage>(
	storage: &S,
	table: TableId,
	key: &[u8],
	version: CommitVersion,
	value: Option<&[u8]>,
) -> Result<()> {
	let versioned_key = encode_versioned_key(key, version);
	storage.put(table, &versioned_key, value)
}

/// Get the latest version number for a key (if any exists).
#[instrument(level = "trace", skip(storage), fields(table = ?table, key_len = key.len()))]
pub fn get_latest_version<S: PrimitiveStorage>(
	storage: &S,
	table: TableId,
	key: &[u8],
) -> Result<Option<CommitVersion>> {
	let (start, end) = key_version_range(key);

	// Scan in reverse to find the latest version
	let mut iter = storage.range(table, Bound::Included(start.as_slice()), Bound::Included(end.as_slice()), 1)?;

	if let Some(entry_result) = iter.next() {
		let entry = entry_result?;
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
	use crate::backend::memory::MemoryPrimitiveStorage;

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

	#[test]
	fn test_get_at_version() {
		let storage = MemoryPrimitiveStorage::new();

		// Store multiple versions
		put_at_version(&storage, TableId::Multi, b"key", CommitVersion(1), Some(b"v1")).unwrap();
		put_at_version(&storage, TableId::Multi, b"key", CommitVersion(3), Some(b"v3")).unwrap();
		put_at_version(&storage, TableId::Multi, b"key", CommitVersion(5), Some(b"v5")).unwrap();

		// Get at exact versions
		let result = get_at_version(&storage, TableId::Multi, b"key", CommitVersion(1)).unwrap();
		assert_eq!(result.into_value(), Some(b"v1".to_vec()));

		let result = get_at_version(&storage, TableId::Multi, b"key", CommitVersion(3)).unwrap();
		assert_eq!(result.into_value(), Some(b"v3".to_vec()));

		// Get at version between stored versions (should get earlier version)
		let result = get_at_version(&storage, TableId::Multi, b"key", CommitVersion(2)).unwrap();
		assert_eq!(result.into_value(), Some(b"v1".to_vec()));

		let result = get_at_version(&storage, TableId::Multi, b"key", CommitVersion(4)).unwrap();
		assert_eq!(result.into_value(), Some(b"v3".to_vec()));

		// Get at version after all stored versions
		let result = get_at_version(&storage, TableId::Multi, b"key", CommitVersion(10)).unwrap();
		assert_eq!(result.into_value(), Some(b"v5".to_vec()));

		// Get at version before first stored version
		let result = get_at_version(&storage, TableId::Multi, b"key", CommitVersion(0)).unwrap();
		assert!(matches!(result, VersionedGetResult::NotFound));
	}

	#[test]
	fn test_tombstone() {
		let storage = MemoryPrimitiveStorage::new();

		// Store value then tombstone
		put_at_version(&storage, TableId::Multi, b"key", CommitVersion(1), Some(b"value")).unwrap();
		put_at_version(&storage, TableId::Multi, b"key", CommitVersion(2), None).unwrap();

		// Version 1 has value
		let result = get_at_version(&storage, TableId::Multi, b"key", CommitVersion(1)).unwrap();
		assert!(result.is_value());

		// Version 2 and later see tombstone
		let result = get_at_version(&storage, TableId::Multi, b"key", CommitVersion(2)).unwrap();
		assert!(result.is_tombstone());

		let result = get_at_version(&storage, TableId::Multi, b"key", CommitVersion(10)).unwrap();
		assert!(result.is_tombstone());
	}

	#[test]
	fn test_contains_at_version() {
		let storage = MemoryPrimitiveStorage::new();

		put_at_version(&storage, TableId::Multi, b"key", CommitVersion(1), Some(b"value")).unwrap();
		put_at_version(&storage, TableId::Multi, b"key", CommitVersion(2), None).unwrap();

		assert!(contains_at_version(&storage, TableId::Multi, b"key", CommitVersion(1)).unwrap());
		assert!(!contains_at_version(&storage, TableId::Multi, b"key", CommitVersion(2)).unwrap());
		assert!(!contains_at_version(&storage, TableId::Multi, b"key", CommitVersion(0)).unwrap());
	}

	#[test]
	fn test_get_latest_version() {
		let storage = MemoryPrimitiveStorage::new();

		// No versions yet
		assert_eq!(get_latest_version(&storage, TableId::Multi, b"key").unwrap(), None);

		// Add versions
		put_at_version(&storage, TableId::Multi, b"key", CommitVersion(5), Some(b"v5")).unwrap();
		assert_eq!(get_latest_version(&storage, TableId::Multi, b"key").unwrap(), Some(CommitVersion(5)));

		put_at_version(&storage, TableId::Multi, b"key", CommitVersion(10), Some(b"v10")).unwrap();
		assert_eq!(get_latest_version(&storage, TableId::Multi, b"key").unwrap(), Some(CommitVersion(10)));
	}
}
