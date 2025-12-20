// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::{CommitVersion, CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::MultiVersionValues};

/// Composite trait for multi-version storage capabilities.
pub trait MultiVersionStore:
	Send
	+ Sync
	+ Clone
	+ MultiVersionCommit
	+ MultiVersionGet
	+ MultiVersionContains
	+ MultiVersionRange
	+ MultiVersionRangeRev
	+ 'static
{
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
#[async_trait]
pub trait MultiVersionCommit: Send + Sync {
	/// Commit a batch of deltas at the given version.
	async fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> crate::Result<()>;
}

/// Trait for getting values from multi-version storage.
#[async_trait]
pub trait MultiVersionGet: Send + Sync {
	/// Get the value for a key at a specific version.
	async fn get(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<Option<MultiVersionValues>>;
}

/// Trait for checking key existence in multi-version storage.
#[async_trait]
pub trait MultiVersionContains: Send + Sync {
	/// Check if a key exists at a specific version.
	async fn contains(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<bool>;
}

/// Trait for forward range queries with batch-fetch pattern.
#[async_trait]
pub trait MultiVersionRange: Send + Sync {
	/// Fetch a batch of values in key order (ascending).
	///
	/// Returns up to `batch_size` values. The `has_more` field indicates
	/// whether there are more values after this batch.
	async fn range_batch(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch>;

	/// Convenience method with default batch size.
	async fn range(&self, range: EncodedKeyRange, version: CommitVersion) -> crate::Result<MultiVersionBatch> {
		self.range_batch(range, version, 1024).await
	}

	/// Range query with prefix.
	async fn prefix(&self, prefix: &EncodedKey, version: CommitVersion) -> crate::Result<MultiVersionBatch> {
		self.range(EncodedKeyRange::prefix(prefix), version).await
	}
}

/// Trait for reverse range queries with batch-fetch pattern.
#[async_trait]
pub trait MultiVersionRangeRev: Send + Sync {
	/// Fetch a batch of values in reverse key order (descending).
	///
	/// Returns up to `batch_size` values. The `has_more` field indicates
	/// whether there are more values after this batch.
	async fn range_rev_batch(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch>;

	/// Convenience method with default batch size.
	async fn range_rev(&self, range: EncodedKeyRange, version: CommitVersion) -> crate::Result<MultiVersionBatch> {
		self.range_rev_batch(range, version, 1024).await
	}

	/// Reverse range query with prefix.
	async fn prefix_rev(&self, prefix: &EncodedKey, version: CommitVersion) -> crate::Result<MultiVersionBatch> {
		self.range_rev(EncodedKeyRange::prefix(prefix), version).await
	}
}
