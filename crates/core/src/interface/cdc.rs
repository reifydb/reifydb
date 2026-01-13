// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::{CommitVersion, EncodedKey, value::encoded::EncodedValues};

#[repr(transparent)]
#[derive(Debug, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct CdcConsumerId(pub(crate) String);

impl CdcConsumerId {
	pub fn new(id: impl Into<String>) -> Self {
		let id = id.into();
		assert_ne!(id, "__FLOW_CONSUMER");
		Self(id)
	}

	pub fn flow_consumer() -> Self {
		Self("__FLOW_CONSUMER".to_string())
	}
}

impl AsRef<str> for CdcConsumerId {
	fn as_ref(&self) -> &str {
		&self.0
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CdcChange {
	Insert {
		key: EncodedKey,
		post: EncodedValues,
	},
	Update {
		key: EncodedKey,
		pre: EncodedValues,
		post: EncodedValues,
	},
	Delete {
		key: EncodedKey,
		pre: Option<EncodedValues>,
	},
}

impl CdcChange {
	/// Get the key for this change.
	pub fn key(&self) -> &EncodedKey {
		match self {
			CdcChange::Insert { key, .. } => key,
			CdcChange::Update { key, .. } => key,
			CdcChange::Delete { key, .. } => key,
		}
	}

	/// Calculate the approximate value bytes for this change (pre + post values).
	pub fn value_bytes(&self) -> usize {
		match self {
			CdcChange::Insert { post, .. } => post.len(),
			CdcChange::Update { pre, post, .. } => pre.len() + post.len(),
			CdcChange::Delete { pre, .. } => pre.as_ref().map(|p| p.len()).unwrap_or(0),
		}
	}
}

/// Structure for storing CDC data with shared metadata
#[derive(Debug, Clone, PartialEq)]
pub struct Cdc {
	pub version: CommitVersion,
	pub timestamp: u64,
	pub changes: Vec<CdcSequencedChange>,
}

impl Cdc {
	pub fn new(version: CommitVersion, timestamp: u64, changes: Vec<CdcSequencedChange>) -> Self {
		Self {
			version,
			timestamp,
			changes,
		}
	}
}

/// Structure for individual changes within a transaction
#[derive(Debug, Clone, PartialEq)]
pub struct CdcSequencedChange {
	pub sequence: u16,
	pub change: CdcChange,
}

impl CdcSequencedChange {
	pub fn key(&self) -> &EncodedKey {
		match &self.change {
			CdcChange::Insert {
				key,
				..
			} => key,
			CdcChange::Update {
				key,
				..
			} => key,
			CdcChange::Delete {
				key,
				..
			} => key,
		}
	}
}

/// Represents the state of a CDC consumer
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerState {
	pub consumer_id: CdcConsumerId,
	pub checkpoint: CommitVersion,
}

/// A batch of CDC entries with continuation info.
#[derive(Debug, Clone)]
pub struct CdcBatch {
	/// The CDC entries in this batch.
	pub items: Vec<Cdc>,
	/// Whether there are more items after this batch.
	pub has_more: bool,
}

impl CdcBatch {
	/// Creates an empty batch with no more results.
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}

	/// Returns true if this batch contains no items.
	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}

// ============================================================================
// Previous Version Resolution
// ============================================================================

/// Information about a previous version of a key.
#[derive(Debug, Clone)]
pub struct PreviousVersionInfo {
	/// The version at which this value was stored.
	pub version: CommitVersion,
	/// The value stored at this version (if available).
	pub value: Option<EncodedValues>,
	/// Size of the key in bytes.
	pub key_bytes: u64,
	/// Size of the value in bytes.
	pub value_bytes: u64,
}

/// Error type for version resolution operations.
#[derive(Debug, Clone)]
pub enum ResolverError {
	/// The lookup operation failed.
	LookupFailed(String),
	/// Storage is not available.
	StorageUnavailable,
}

impl std::fmt::Display for ResolverError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ResolverError::LookupFailed(msg) => write!(f, "Version resolution failed: {}", msg),
			ResolverError::StorageUnavailable => write!(f, "Storage unavailable for resolution"),
		}
	}
}

impl std::error::Error for ResolverError {}

/// Result type for version resolution operations.
pub type ResolverResult<T> = Result<T, ResolverError>;

/// Trait for resolving previous versions and values from MVCC storage.
///
/// This trait is implemented by the storage layer to provide version lookups
/// to the CDC generation module. It decouples CDC from the MVCC storage internals.
///
/// All operations must be thread-safe as CDC generation may happen on multiple
/// shard worker threads concurrently.
pub trait PreviousVersionResolver: Send + Sync + 'static {
	/// Get info about the latest version of a key BEFORE the given version.
	///
	/// This is used to determine if a change is an Insert (no previous version)
	/// or an Update/Delete (has previous version).
	///
	/// # Arguments
	/// * `key` - The encoded key to look up
	/// * `before_version` - Find versions strictly less than this version
	///
	/// # Returns
	/// * `Ok(Some(info))` - Previous version exists with info
	/// * `Ok(None)` - No previous version exists (key is new)
	/// * `Err(_)` - Lookup failed
	fn resolve_version_before(
		&self,
		key: &EncodedKey,
		before_version: CommitVersion,
	) -> ResolverResult<Option<PreviousVersionInfo>>;

	/// Get the value at a specific version.
	///
	/// This is used to resolve the actual pre/post values for CDC entries
	/// when generating fully resolved CDC events.
	///
	/// # Arguments
	/// * `key` - The encoded key to look up
	/// * `version` - The specific version to retrieve
	///
	/// # Returns
	/// * `Ok(Some(value))` - Value exists at this version
	/// * `Ok(None)` - No value at this version
	/// * `Err(_)` - Lookup failed
	fn resolve_value_at(
		&self,
		key: &EncodedKey,
		version: CommitVersion,
	) -> ResolverResult<Option<EncodedValues>>;

	/// Resolve both version info and value for a key before a given version.
	///
	/// This is a convenience method that combines `resolve_version_before` and
	/// `resolve_value_at` for cases where both are needed. Default implementation
	/// calls both methods sequentially, but implementations may optimize this.
	fn resolve_previous_with_value(
		&self,
		key: &EncodedKey,
		before_version: CommitVersion,
	) -> ResolverResult<Option<PreviousVersionInfo>> {
		match self.resolve_version_before(key, before_version)? {
			Some(mut info) => {
				// If we don't already have the value, try to resolve it
				if info.value.is_none() {
					info.value = self.resolve_value_at(key, info.version)?;
				}
				Ok(Some(info))
			}
			None => Ok(None),
		}
	}
}

/// A no-op resolver that always returns None.
///
/// This is useful for testing or cases where previous version lookups
/// are not needed (e.g., all changes treated as inserts).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOpResolver;

impl PreviousVersionResolver for NoOpResolver {
	fn resolve_version_before(
		&self,
		_key: &EncodedKey,
		_before_version: CommitVersion,
	) -> ResolverResult<Option<PreviousVersionInfo>> {
		Ok(None)
	}

	fn resolve_value_at(
		&self,
		_key: &EncodedKey,
		_version: CommitVersion,
	) -> ResolverResult<Option<EncodedValues>> {
		Ok(None)
	}
}
