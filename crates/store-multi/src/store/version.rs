// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::common::CommitVersion;
use reifydb_type::{util::cowvec::CowVec, Result};

use crate::tier::{EntryKind, TierStorage};

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
	// The storage layer now handles version lookups directly
	match storage.get(table, key, version)? {
		Some(value) => Ok(VersionedGetResult::Value {
			value,
			version,
		}),
		None => {
			// Need to determine if it's a tombstone or not found
			// Get all versions and check if any version <= requested exists
			let all_versions = storage.get_all_versions(table, key)?;

			// Find the latest version <= requested
			for (v, value) in all_versions {
				if v <= version {
					// Found a version at or before requested
					return match value {
						Some(val) => Ok(VersionedGetResult::Value {
							value: val,
							version: v,
						}),
						None => Ok(VersionedGetResult::Tombstone),
					};
				}
			}

			// No version exists at or before requested version
			Ok(VersionedGetResult::NotFound)
		}
	}
}

#[cfg(test)]
pub mod tests {
	use std::collections::HashMap;

	use reifydb_core::runtime::compute::ComputePool;

	use super::*;
	use crate::hot::{memory::storage::MemoryPrimitiveStorage, storage::HotStorage};

	fn test_compute_pool() -> ComputePool {
		ComputePool::new(2, 8)
	}

	#[test]
	fn test_get_at_version_basic() {
		let storage = MemoryPrimitiveStorage::new(test_compute_pool());

		let key = CowVec::new(b"test_key".to_vec());
		let version = CommitVersion(42);

		// Insert a value
		storage.set(
			version,
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value".to_vec())))])]),
		)
		.unwrap();

		// Get at exact version
		match get_at_version(&storage, EntryKind::Multi, &key, version).unwrap() {
			VersionedGetResult::Value {
				value,
				..
			} => {
				assert_eq!(value.as_slice(), b"value");
			}
			_ => panic!("Expected Value result"),
		}

		// Get at higher version should still work
		match get_at_version(&storage, EntryKind::Multi, &key, CommitVersion(100)).unwrap() {
			VersionedGetResult::Value {
				value,
				..
			} => {
				assert_eq!(value.as_slice(), b"value");
			}
			_ => panic!("Expected Value result"),
		}
	}

	#[test]
	fn test_get_at_version_not_found() {
		let storage = MemoryPrimitiveStorage::new(test_compute_pool());

		let result = get_at_version(&storage, EntryKind::Multi, b"nonexistent", CommitVersion(100)).unwrap();
		assert!(matches!(result, VersionedGetResult::NotFound));
	}

	#[test]
	fn test_get_at_version_tombstone() {
		let storage = MemoryPrimitiveStorage::new(test_compute_pool());

		let key = CowVec::new(b"test_key".to_vec());

		// Insert a tombstone (None value)
		storage.set(CommitVersion(1), HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();

		let result = get_at_version(&storage, EntryKind::Multi, &key, CommitVersion(1)).unwrap();
		assert!(matches!(result, VersionedGetResult::Tombstone));
	}

	#[test]
	fn test_get_at_version_multiple_versions() {
		let storage = HotStorage::memory(test_compute_pool());

		let key = CowVec::new(b"test_key".to_vec());

		// Insert multiple versions
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(5),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v5".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(10),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v10".to_vec())))])]),
		)
		.unwrap();

		// Get at version 3 should return v1 (latest <= 3)
		match get_at_version(&storage, EntryKind::Multi, &key, CommitVersion(3)).unwrap() {
			VersionedGetResult::Value {
				value,
				..
			} => {
				assert_eq!(value.as_slice(), b"v1");
			}
			_ => panic!("Expected Value result"),
		}

		// Get at version 7 should return v5 (latest <= 7)
		match get_at_version(&storage, EntryKind::Multi, &key, CommitVersion(7)).unwrap() {
			VersionedGetResult::Value {
				value,
				..
			} => {
				assert_eq!(value.as_slice(), b"v5");
			}
			_ => panic!("Expected Value result"),
		}

		// Get at version 15 should return v10 (latest <= 15)
		match get_at_version(&storage, EntryKind::Multi, &key, CommitVersion(15)).unwrap() {
			VersionedGetResult::Value {
				value,
				..
			} => {
				assert_eq!(value.as_slice(), b"v10");
			}
			_ => panic!("Expected Value result"),
		}
	}

	#[test]
	fn test_get_at_version_before_any_version() {
		let storage = HotStorage::memory(test_compute_pool());

		let key = CowVec::new(b"test_key".to_vec());

		// Insert at version 10
		storage.set(
			CommitVersion(10),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value".to_vec())))])]),
		)
		.unwrap();

		// Get at version 5 (before any version exists) should return NotFound
		let result = get_at_version(&storage, EntryKind::Multi, &key, CommitVersion(5)).unwrap();
		assert!(matches!(result, VersionedGetResult::NotFound));
	}
}
