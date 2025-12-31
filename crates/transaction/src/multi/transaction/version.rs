// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	CommitVersion,
	interface::{
		EncodableKey, SingleVersionCommandTransaction, SingleVersionQueryTransaction, SingleVersionTransaction,
		TransactionVersionKey,
	},
	value::encoded::EncodedValuesLayout,
};
use reifydb_type::Type;
use tokio::sync::Mutex;

use crate::single::TransactionSingle;

const BLOCK_SIZE: u64 = 100_000;

#[async_trait]
pub trait VersionProvider: Send + Sync + Clone {
	async fn next(&self) -> crate::Result<CommitVersion>;
	async fn current(&self) -> crate::Result<CommitVersion>;
}

#[derive(Debug)]
struct VersionBlock {
	last: u64,
	current: u64,
}

impl VersionBlock {
	fn new(start: u64) -> Self {
		Self {
			last: start + BLOCK_SIZE,
			current: start,
		}
	}

	fn next(&mut self) -> Option<CommitVersion> {
		if self.current < self.last {
			self.current += 1;
			Some(CommitVersion(self.current))
		} else {
			None
		}
	}

	fn current(&self) -> CommitVersion {
		CommitVersion(self.current)
	}
}

#[derive(Clone)]
pub struct StandardVersionProvider {
	single: TransactionSingle,
	current_block: Arc<Mutex<VersionBlock>>,
}

impl StandardVersionProvider {
	pub async fn new(single: TransactionSingle) -> crate::Result<Self> {
		// Load current version and allocate first block
		let current_version = Self::load_current_version(&single).await?;
		let first_block = VersionBlock::new(current_version);

		// Persist the end of first block to storage
		Self::persist_version(&single, first_block.last).await?;

		Ok(Self {
			single,
			current_block: Arc::new(Mutex::new(first_block)),
		})
	}

	async fn load_current_version(single: &TransactionSingle) -> crate::Result<u64> {
		let layout = EncodedValuesLayout::new(&[Type::Uint8]);
		let key = TransactionVersionKey {}.encode();

		let mut tx = single.begin_query([&key]).await?;
		match tx.get(&key).await? {
			None => Ok(0),
			Some(single) => Ok(layout.get_u64(&single.values, 0)),
		}
	}

	async fn persist_version(single: &TransactionSingle, version: u64) -> crate::Result<()> {
		let layout = EncodedValuesLayout::new(&[Type::Uint8]);
		let key = TransactionVersionKey {}.encode();
		let mut values = layout.allocate();
		layout.set_u64(&mut values, 0, version);

		let mut tx = single.begin_command([&key]).await?;
		tx.set(&key, values)?;
		tx.commit().await
	}
}

#[async_trait]
impl VersionProvider for StandardVersionProvider {
	async fn next(&self) -> crate::Result<CommitVersion> {
		// Fast path: try to get version from current block
		let mut block = self.current_block.lock().await;

		if let Some(version) = block.next() {
			return Ok(version);
		}

		// Slow path: block exhausted, allocate new block
		// Allocate new block starting from current end
		let new_start = block.last;
		let new_block = VersionBlock::new(new_start);

		// Persist new block end to storage (expensive operation)
		Self::persist_version(&self.single, new_block.last).await?;

		*block = new_block;

		if let Some(version) = block.next() {
			return Ok(version);
		}

		panic!("Failed to allocate version from new block")
	}

	async fn current(&self) -> crate::Result<CommitVersion> {
		let block = self.current_block.lock().await;
		Ok(block.current())
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::interface::SingleVersionTransaction;

	use super::*;

	#[tokio::test]
	async fn test_new_version_provider() {
		let single = TransactionSingle::testing().await;
		let provider = StandardVersionProvider::new(single).await.unwrap();

		// Should start at version 0
		assert_eq!(provider.current().await.unwrap(), 0);
	}

	#[tokio::test]
	async fn test_next_version_sequential() {
		let single = TransactionSingle::testing().await;
		let provider = StandardVersionProvider::new(single).await.unwrap();

		assert_eq!(provider.next().await.unwrap(), 1);
		assert_eq!(provider.current().await.unwrap(), 1);

		assert_eq!(provider.next().await.unwrap(), 2);
		assert_eq!(provider.current().await.unwrap(), 2);

		assert_eq!(provider.next().await.unwrap(), 3);
		assert_eq!(provider.current().await.unwrap(), 3);
	}

	#[tokio::test]
	async fn test_version_persistence() {
		let single = TransactionSingle::testing().await;

		// Create first provider and get some versions
		{
			let provider = StandardVersionProvider::new(single.clone()).await.unwrap();
			assert_eq!(provider.next().await.unwrap(), 1);
			assert_eq!(provider.next().await.unwrap(), 2);
			assert_eq!(provider.next().await.unwrap(), 3);
		}

		// Create new provider with same storage - should continue from
		// persisted version
		let provider2 = StandardVersionProvider::new(single.clone()).await.unwrap();
		assert_eq!(provider2.next().await.unwrap(), BLOCK_SIZE + 1);
		assert_eq!(provider2.current().await.unwrap(), BLOCK_SIZE + 1);
	}

	#[tokio::test]
	async fn test_block_exhaustion_and_allocation() {
		let single = TransactionSingle::testing().await;
		let provider = StandardVersionProvider::new(single).await.unwrap();

		// Exhaust the first block
		for _ in 0..BLOCK_SIZE {
			provider.next().await.unwrap();
		}

		// Next version should trigger new block allocation
		assert_eq!(provider.current().await.unwrap(), BLOCK_SIZE);
		assert_eq!(provider.next().await.unwrap(), BLOCK_SIZE + 1);
		assert_eq!(provider.current().await.unwrap(), BLOCK_SIZE + 1);

		// Continue with next block
		assert_eq!(provider.next().await.unwrap(), BLOCK_SIZE + 2);
		assert_eq!(provider.current().await.unwrap(), BLOCK_SIZE + 2);
	}

	#[tokio::test]
	async fn test_concurrent_version_allocation() {
		let single = TransactionSingle::testing().await;
		let provider = Arc::new(StandardVersionProvider::new(single).await.unwrap());

		let mut handles = vec![];

		// Spawn multiple tasks to request versions concurrently
		for _ in 0..10 {
			let provider_clone = Arc::clone(&provider);
			let handle = tokio::spawn(async move {
				let mut versions = vec![];
				for _ in 0..100 {
					versions.push(provider_clone.next().await.unwrap());
				}
				versions
			});
			handles.push(handle);
		}

		// Collect all versions from all tasks
		let mut all_versions = vec![];
		for handle in handles {
			let mut versions = handle.await.unwrap();
			all_versions.append(&mut versions);
		}

		// Sort versions to check for uniqueness
		all_versions.sort();

		// Check that all versions are unique (no duplicates)
		for i in 1..all_versions.len() {
			assert_ne!(
				all_versions[i - 1],
				all_versions[i],
				"Duplicate version found: {}",
				all_versions[i]
			);
		}

		// Should have exactly 1000 unique versions (10 tasks * 100
		// versions each)
		assert_eq!(all_versions.len(), 1000);

		// First version should be 1, last should be 1000
		assert_eq!(all_versions[0], 1);
		assert_eq!(all_versions[999], 1000);
	}

	#[test]
	fn test_version_block_behavior() {
		let mut block = VersionBlock::new(100);

		// Should start at the given version
		assert_eq!(block.current(), 100);

		// Should return sequential versions
		assert_eq!(block.next().unwrap(), 101);
		assert_eq!(block.current(), 101);

		assert_eq!(block.next().unwrap(), 102);
		assert_eq!(block.current(), 102);
	}

	#[test]
	fn test_version_block_exhaustion() {
		let mut block = VersionBlock::new(0);

		for _ in 0..BLOCK_SIZE - 2 {
			block.next();
		}

		assert_eq!(block.next().unwrap(), BLOCK_SIZE - 1);
		assert_eq!(block.next().unwrap(), BLOCK_SIZE);

		// exhausted
		assert_eq!(block.next(), None);
	}

	#[tokio::test]
	async fn test_load_existing_version() {
		let single = TransactionSingle::testing().await;

		// Manually set a version in storage
		let layout = EncodedValuesLayout::new(&[Type::Uint8]);
		let key = TransactionVersionKey {}.encode();
		let mut values = layout.allocate();
		layout.set_u64(&mut values, 0, 500u64);

		{
			let mut tx = single.begin_command([&key]).await.unwrap();
			tx.set(&key, values).unwrap();
			tx.commit().await.unwrap();
		} // tx is dropped here, releasing the key lock

		// Create provider - should start from the existing version
		let provider = StandardVersionProvider::new(single.clone()).await.unwrap();
		assert_eq!(provider.current().await.unwrap(), 500);
		assert_eq!(provider.next().await.unwrap(), 501);
	}
}
