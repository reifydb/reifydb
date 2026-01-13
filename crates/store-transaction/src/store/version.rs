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
use reifydb_type::{CowVec, Result};

use crate::tier::{EntryKind, RangeCursor, TierStorage};

/// Size of version suffix in bytes (terminator + u64 big-endian)
pub(crate) const VERSION_SIZE: usize = 10; // 2 bytes terminator + 8 bytes version

/// Terminator bytes for key/version separator
const TERMINATOR: [u8; 2] = [0x00, 0x00];

/// Encode a versioned key: `[escaped_key_bytes][0x00 0x00][!version_be]`
///
/// Uses 0x00 escaping (0x00 -> 0x00 0xFF) to allow 0x00 0x00 as terminator.
/// This ensures proper lexicographic ordering for variable-length keys.
///
/// Uses bitwise NOT (!version) for descending order:
/// higher versions encode to lower byte values, sorting first.
pub fn encode_versioned_key(key: &[u8], version: CommitVersion) -> Vec<u8> {
	// Estimate capacity: key + potential escapes + terminator + version
	let mut result = Vec::with_capacity(key.len() + VERSION_SIZE);

	// Escape 0x00 bytes in key (0x00 -> 0x00 0xFF)
	for &byte in key {
		if byte == 0x00 {
			result.push(0x00);
			result.push(0xFF);
		} else {
			result.push(byte);
		}
	}

	// Add terminator
	result.extend_from_slice(&TERMINATOR);

	// Add inverted version (bitwise NOT for descending order)
	result.extend_from_slice(&(!version.0).to_be_bytes());
	result
}

/// Find the terminator position in a versioned key
fn find_terminator(versioned_key: &[u8]) -> Option<usize> {
	let mut i = 0;
	while i + 1 < versioned_key.len() {
		if versioned_key[i] == 0x00 {
			if versioned_key[i + 1] == 0x00 {
				// Found terminator
				return Some(i);
			} else if versioned_key[i + 1] == 0xFF {
				// Escaped 0x00, skip both bytes
				i += 2;
			} else {
				// Invalid escape sequence
				return None;
			}
		} else {
			i += 1;
		}
	}
	None
}

/// Unescape a key (0x00 0xFF -> 0x00)
fn unescape_key(escaped: &[u8]) -> Vec<u8> {
	let mut result = Vec::with_capacity(escaped.len());
	let mut i = 0;
	while i < escaped.len() {
		if i + 1 < escaped.len() && escaped[i] == 0x00 && escaped[i + 1] == 0xFF {
			result.push(0x00);
			i += 2;
		} else {
			result.push(escaped[i]);
			i += 1;
		}
	}
	result
}

/// Decode a versioned key back to (key, version)
#[cfg(test)]
pub fn decode_versioned_key(versioned_key: &[u8]) -> Option<(Vec<u8>, CommitVersion)> {
	let terminator_pos = find_terminator(versioned_key)?;
	let escaped_key = &versioned_key[..terminator_pos];
	let version_start = terminator_pos + 2; // Skip terminator

	if versioned_key.len() < version_start + 8 {
		return None;
	}

	let version_bytes: [u8; 8] = versioned_key[version_start..version_start + 8].try_into().ok()?;
	let version = CommitVersion(!u64::from_be_bytes(version_bytes));
	let key = unescape_key(escaped_key);

	Some((key, version))
}

/// Extract just the original key from a versioned key (unescaped)
pub fn extract_key(versioned_key: &[u8]) -> Option<Vec<u8>> {
	let terminator_pos = find_terminator(versioned_key)?;
	let escaped_key = &versioned_key[..terminator_pos];
	Some(unescape_key(escaped_key))
}

/// Extract just the version from a versioned key
pub fn extract_version(versioned_key: &[u8]) -> Option<CommitVersion> {
	let terminator_pos = find_terminator(versioned_key)?;
	let version_start = terminator_pos + 2;

	if versioned_key.len() < version_start + 8 {
		return None;
	}

	let version_bytes: [u8; 8] = versioned_key[version_start..version_start + 8].try_into().ok()?;
	Some(CommitVersion(!u64::from_be_bytes(version_bytes)))
}

/// Create range bounds to find all versions of a key (newest first)
///
/// With complement encoding, higher versions sort first:
/// - start: version MAX encodes to 0x00...00 (newest, sorts first)
/// - end: version 0 encodes to 0xFF...FF (oldest, sorts last)
pub fn key_version_range(key: &[u8]) -> (Vec<u8>, Vec<u8>) {
	let start = encode_versioned_key(key, CommitVersion(u64::MAX));
	let end = encode_versioned_key(key, CommitVersion(0));
	(start, end)
}

/// Result of a versioned get operation
#[derive(Debug, Clone)]
pub enum VersionedGetResult {
	/// Found a value at this version
	Value {
		value: CowVec<u8>,
		version: CommitVersion,
	},
	/// Found a tombstone (deletion) at this version
	Tombstone,
	/// Key not found at or before the requested version
	NotFound,
}

/// Get the latest version of a key at or before the given version.
pub fn get_at_version<S: TierStorage>(
	storage: &S,
	table: EntryKind,
	key: &[u8],
	version: CommitVersion,
) -> Result<VersionedGetResult> {
	// With complement encoding, higher versions sort first.
	// Range from requested version (sorts first) to version 0 (sorts last).
	let start = encode_versioned_key(key, version);
	let end = encode_versioned_key(key, CommitVersion(0));

	// Forward scan finds newest version first (just need 1 entry)
	let mut cursor = RangeCursor::new();
	let batch = storage.range_next(
		table,
		&mut cursor,
		Bound::Included(start.as_slice()),
		Bound::Included(end.as_slice()),
		1,
	)?;

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

/// Get the latest version number for a key (if any exists).
#[allow(dead_code)]
pub fn get_latest_version<S: TierStorage>(storage: &S, table: EntryKind, key: &[u8]) -> Result<Option<CommitVersion>> {
	let (start, end) = key_version_range(key);

	// Forward scan finds newest version first (just need 1 entry)
	let mut cursor = RangeCursor::new();
	let batch = storage.range_next(
		table,
		&mut cursor,
		Bound::Included(start.as_slice()),
		Bound::Included(end.as_slice()),
		1,
	)?;

	if let Some(entry) = batch.entries.first() {
		if let Some(entry_key) = extract_key(&entry.key) {
			if entry_key == key {
				return Ok(extract_version(&entry.key));
			}
		}
	}

	Ok(None)
}

/// Combined info about a previous version of a key.
/// Used to combine stats tracking and CDC lookups in a single pass.
#[derive(Debug, Clone)]
pub struct PreviousVersionInfo {
	/// The version of the previous entry
	pub version: CommitVersion,
	/// Size of the versioned key in bytes
	pub key_bytes: u64,
	/// Size of the value in bytes (0 if tombstone)
	pub value_bytes: u64,
}

/// Get combined info about the latest version of a key.
/// Returns version, key size, and value size in a single lookup.
/// This combines what `get_latest_version` and `get_previous_value_info` do separately.
#[allow(dead_code)]
pub fn get_previous_version_info<S: TierStorage>(
	storage: &S,
	table: EntryKind,
	key: &[u8],
) -> Result<Option<PreviousVersionInfo>> {
	let (start, end) = key_version_range(key);

	// Forward scan finds newest version first (just need 1 entry)
	let mut cursor = RangeCursor::new();
	let batch = storage.range_next(
		table,
		&mut cursor,
		Bound::Included(start.as_slice()),
		Bound::Included(end.as_slice()),
		1,
	)?;

	if let Some(entry) = batch.entries.first() {
		if let Some(entry_key) = extract_key(&entry.key) {
			if entry_key == key {
				if let Some(version) = extract_version(&entry.key) {
					let key_bytes = entry.key.len() as u64;
					let value_bytes = entry.value.as_ref().map(|v| v.len() as u64).unwrap_or(0);
					return Ok(Some(PreviousVersionInfo {
						version,
						key_bytes,
						value_bytes,
					}));
				}
			}
		}
	}

	Ok(None)
}

/// Get info about the latest version of a key BEFORE the given version.
/// Used by async CDC workers to find the previous state.
pub fn get_version_info_before<S: TierStorage>(
	storage: &S,
	table: EntryKind,
	key: &[u8],
	before_version: CommitVersion,
) -> Result<Option<PreviousVersionInfo>> {
	// If before_version is 0 or 1, there can't be a previous version
	if before_version.0 <= 1 {
		return Ok(None);
	}

	// Scan from before_version - 1 down to 0
	// With complement encoding, lower versions have higher byte values
	let start = encode_versioned_key(key, CommitVersion(before_version.0 - 1));
	let end = encode_versioned_key(key, CommitVersion(0));

	let mut cursor = RangeCursor::new();
	let batch = storage.range_next(
		table,
		&mut cursor,
		Bound::Included(start.as_slice()),
		Bound::Included(end.as_slice()),
		1,
	)?;

	if let Some(entry) = batch.entries.first() {
		if let Some(entry_key) = extract_key(&entry.key) {
			if entry_key == key {
				if let Some(version) = extract_version(&entry.key) {
					let key_bytes = entry.key.len() as u64;
					let value_bytes = entry.value.as_ref().map(|v| v.len() as u64).unwrap_or(0);
					return Ok(Some(PreviousVersionInfo {
						version,
						key_bytes,
						value_bytes,
					}));
				}
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
		// Key (8 bytes) + terminator (2 bytes) + version (8 bytes) = 18 bytes
		assert_eq!(encoded.len(), key.len() + VERSION_SIZE);

		let (decoded_key, decoded_version) = decode_versioned_key(&encoded).unwrap();
		assert_eq!(decoded_key.as_slice(), key);
		assert_eq!(decoded_version, version);
	}

	#[test]
	fn test_encode_decode_with_null_bytes() {
		let key = b"test\x00key\x00";
		let version = CommitVersion(100);

		let encoded = encode_versioned_key(key, version);
		// Key has 2 null bytes, each escaped to 2 bytes, so 9 + 2 = 11 key bytes
		// Plus terminator (2) + version (8) = 21 bytes
		assert_eq!(encoded.len(), 11 + VERSION_SIZE);

		let (decoded_key, decoded_version) = decode_versioned_key(&encoded).unwrap();
		assert_eq!(decoded_key.as_slice(), key);
		assert_eq!(decoded_version, version);
	}

	#[test]
	fn test_version_ordering() {
		let key = b"key";

		let v1 = encode_versioned_key(key, CommitVersion(1));
		let v2 = encode_versioned_key(key, CommitVersion(2));
		let v10 = encode_versioned_key(key, CommitVersion(10));

		// Higher versions should sort before lower versions (descending order)
		assert!(v10 < v2);
		assert!(v2 < v1);
	}
}
