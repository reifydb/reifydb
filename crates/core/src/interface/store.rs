// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{CommitVersion, CowVec, EncodedKey, EncodedKeyRange, Result, delta::Delta, value::encoded::EncodedValues};

#[derive(Debug, Clone)]
pub struct MultiVersionValues {
	pub key: EncodedKey,
	pub values: EncodedValues,
	pub version: CommitVersion,
}

#[derive(Debug, Clone)]
pub struct SingleVersionValues {
	pub key: EncodedKey,
	pub values: EncodedValues,
}

/// A batch of multi-version range results with continuation info.
#[derive(Debug, Clone)]
pub struct MultiVersionBatch {
	/// The values in this batch.
	pub items: Vec<MultiVersionValues>,
	/// Whether there are more items after this batch.
	pub has_more: bool,
}

impl MultiVersionBatch {
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

/// Trait for committing deltas to multi-version storage.
pub trait MultiVersionCommit: Send + Sync {
	/// Commit a batch of deltas at the given version.
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> Result<()>;
}

/// Trait for getting values from multi-version storage.
pub trait MultiVersionGet: Send + Sync {
	/// Get the value for a key at a specific version.
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionValues>>;
}

/// Trait for checking key existence in multi-version storage.
pub trait MultiVersionContains: Send + Sync {
	/// Check if a key exists at a specific version.
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool>;
}

/// Trait for getting the previous version of a key before a given version.
///
/// This trait allows looking up what value existed for a key before a specific version,
/// which is essential for CDC (Change Data Capture) to determine if a change is an
/// Insert, Update, or Delete.
pub trait MultiVersionGetPrevious: Send + Sync {
	/// Get the previous version of a key before the given version.
	///
	/// # Arguments
	/// * `key` - The encoded key to look up
	/// * `before_version` - Look for versions strictly before this version
	///
	/// # Returns
	/// * `Ok(Some(values))` - Found a previous version with its value
	/// * `Ok(None)` - No previous version exists (this is the first version)
	/// * `Err(_)` - Lookup failed
	fn get_previous_version(
		&self,
		key: &EncodedKey,
		before_version: CommitVersion,
	) -> Result<Option<MultiVersionValues>>;
}

/// Composite trait for multi-version storage capabilities.
pub trait MultiVersionStore:
	Send + Sync + Clone + MultiVersionCommit + MultiVersionGet + MultiVersionGetPrevious + MultiVersionContains + 'static
{
}

/// A batch of single-version range results with continuation info.
#[derive(Debug, Clone)]
pub struct SingleVersionBatch {
	/// The values in this batch.
	pub items: Vec<SingleVersionValues>,
	/// Whether there are more items after this batch.
	pub has_more: bool,
}

impl SingleVersionBatch {
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

/// Trait for committing deltas to single-version storage.
pub trait SingleVersionCommit: Send + Sync {
	/// Commit a batch of deltas.
	fn commit(&mut self, deltas: CowVec<Delta>) -> Result<()>;
}

/// Trait for getting values from single-version storage.
pub trait SingleVersionGet: Send + Sync {
	/// Get the value for a key.
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionValues>>;
}

/// Trait for checking key existence in single-version storage.
pub trait SingleVersionContains: Send + Sync {
	/// Check if a key exists.
	fn contains(&self, key: &EncodedKey) -> Result<bool>;
}

/// Trait for setting values in single-version storage.
pub trait SingleVersionSet: SingleVersionCommit {
	/// Set a value for a key.
	fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Set {
				key: key.clone(),
				values: values.clone(),
			}]),
		)
	}
}

/// Trait for removing values from single-version storage.
pub trait SingleVersionRemove: SingleVersionCommit {
	/// Unset a key, preserving the deleted values for CDC and metrics.
	fn unset(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Unset {
				key: key.clone(),
				values,
			}]),
		)
	}

	/// Remove a key without preserving the deleted values.
	fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		Self::commit(self, CowVec::new(vec![Delta::Remove { key: key.clone() }]))
	}
}

/// Trait for forward range queries with batch-fetch pattern.
pub trait SingleVersionRange: Send + Sync {
	/// Fetch a batch of values in key order (ascending).
	fn range_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch>;

	/// Convenience method with default batch size.
	fn range(&self, range: EncodedKeyRange) -> Result<SingleVersionBatch> {
		self.range_batch(range, 1024)
	}

	/// Range query with prefix.
	fn prefix(&self, prefix: &EncodedKey) -> Result<SingleVersionBatch> {
		self.range(EncodedKeyRange::prefix(prefix))
	}
}

/// Trait for reverse range queries with batch-fetch pattern.
pub trait SingleVersionRangeRev: Send + Sync {
	/// Fetch a batch of values in reverse key order (descending).
	fn range_rev_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch>;

	/// Convenience method with default batch size.
	fn range_rev(&self, range: EncodedKeyRange) -> Result<SingleVersionBatch> {
		self.range_rev_batch(range, 1024)
	}

	/// Reverse range query with prefix.
	fn prefix_rev(&self, prefix: &EncodedKey) -> Result<SingleVersionBatch> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}

/// Composite trait for single-version storage capabilities.
pub trait SingleVersionStore:
	Send
	+ Sync
	+ Clone
	+ SingleVersionCommit
	+ SingleVersionGet
	+ SingleVersionContains
	+ SingleVersionSet
	+ SingleVersionRemove
	+ SingleVersionRange
	+ SingleVersionRangeRev
	+ 'static
{
}
