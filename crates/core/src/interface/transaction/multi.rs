// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;

use crate::{
	CommitVersion, EncodedKey, EncodedKeyRange, TransactionId,
	interface::{MultiVersionValues, WithEventBus},
	value::encoded::EncodedValues,
};

/// A batch of multi-version values with continuation info.
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

#[async_trait]
pub trait MultiVersionTransaction: WithEventBus + Send + Sync + Clone + 'static {
	type Query: MultiVersionQueryTransaction;
	type Command: MultiVersionCommandTransaction;

	async fn begin_query(&self) -> crate::Result<Self::Query>;

	async fn begin_command(&self) -> crate::Result<Self::Command>;
}

#[async_trait]
pub trait MultiVersionQueryTransaction: Send + Sync {
	fn version(&self) -> CommitVersion;

	fn id(&self) -> TransactionId;

	async fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<MultiVersionValues>>;

	async fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool>;

	async fn range_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> crate::Result<MultiVersionBatch>;

	async fn range(&mut self, range: EncodedKeyRange) -> crate::Result<MultiVersionBatch> {
		self.range_batch(range, 1024).await
	}

	async fn range_rev_batch(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch>;

	async fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<MultiVersionBatch> {
		self.range_rev_batch(range, 1024).await
	}

	async fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<MultiVersionBatch> {
		self.range(EncodedKeyRange::prefix(prefix)).await
	}

	async fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<MultiVersionBatch> {
		self.range_rev(EncodedKeyRange::prefix(prefix)).await
	}

	async fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> crate::Result<()>;

	async fn read_as_of_version_inclusive(&mut self, version: CommitVersion) -> crate::Result<()> {
		self.read_as_of_version_exclusive(CommitVersion(version.0 + 1)).await
	}
}

#[async_trait]
pub trait MultiVersionCommandTransaction: MultiVersionQueryTransaction {
	async fn set(&mut self, key: &EncodedKey, row: EncodedValues) -> crate::Result<()>;

	async fn remove(&mut self, key: &EncodedKey) -> crate::Result<()>;

	async fn commit(&mut self) -> crate::Result<CommitVersion>;

	async fn rollback(&mut self) -> crate::Result<()>;
}
