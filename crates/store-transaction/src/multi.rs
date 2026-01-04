// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use async_trait::async_trait;
use reifydb_core::{CommitVersion, CowVec, EncodedKey, delta::Delta, interface::MultiVersionValues};

/// Composite trait for multi-version storage capabilities.
pub trait MultiVersionStore:
	Send + Sync + Clone + MultiVersionCommit + MultiVersionGet + MultiVersionContains + 'static
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
