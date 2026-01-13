// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Storage-backed resolver for CDC previous version lookups.

use reifydb_core::interface::{PreviousVersionInfo, PreviousVersionResolver, ResolverError, ResolverResult};
use reifydb_core::{CommitVersion, EncodedKey, value::encoded::EncodedValues};
use reifydb_type::CowVec;

use crate::hot::HotStorage;
use crate::store::router::classify_key;
use crate::store::version::{VersionedGetResult, get_at_version};

/// Resolver that looks up previous versions from MVCC storage.
///
/// This implements `PreviousVersionResolver` by querying the hot tier storage
/// for the previous version of a key before a given commit version.
pub struct StorageResolver {
	storage: HotStorage,
}

impl StorageResolver {
	/// Create a new storage resolver backed by the given hot storage.
	pub fn new(storage: HotStorage) -> Self {
		Self { storage }
	}
}

impl PreviousVersionResolver for StorageResolver {
	fn resolve_version_before(
		&self,
		key: &EncodedKey,
		before_version: CommitVersion,
	) -> ResolverResult<Option<PreviousVersionInfo>> {
		if before_version.0 == 0 {
			return Ok(None);
		}

		let table = classify_key(key);
		let prev_version = CommitVersion(before_version.0 - 1);

		match get_at_version(&self.storage, table, key.as_ref(), prev_version) {
			Ok(VersionedGetResult::Value { value, version }) => Ok(Some(PreviousVersionInfo {
				version,
				value: Some(EncodedValues(CowVec::new(value.to_vec()))),
				key_bytes: key.len() as u64,
				value_bytes: value.len() as u64,
			})),
			Ok(VersionedGetResult::Tombstone) | Ok(VersionedGetResult::NotFound) => Ok(None),
			Err(e) => Err(ResolverError::LookupFailed(e.to_string())),
		}
	}

	fn resolve_value_at(
		&self,
		key: &EncodedKey,
		version: CommitVersion,
	) -> ResolverResult<Option<EncodedValues>> {
		let table = classify_key(key);

		match get_at_version(&self.storage, table, key.as_ref(), version) {
			Ok(VersionedGetResult::Value { value, .. }) => {
				Ok(Some(EncodedValues(CowVec::new(value.to_vec()))))
			}
			Ok(_) => Ok(None),
			Err(e) => Err(ResolverError::LookupFailed(e.to_string())),
		}
	}
}
