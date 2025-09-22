// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{
	Arc, Mutex,
	atomic::{AtomicU64, Ordering},
};

use reifydb_core::{
	CommitVersion,
	interface::{
		EncodableKey, TransactionVersionKey, UnversionedCommandTransaction, UnversionedQueryTransaction,
		UnversionedTransaction,
	},
	value::row::EncodedRowLayout,
};
use reifydb_type::Type;

const BLOCK_SIZE: u64 = 100_000;

pub trait VersionProvider {
	fn next(&self) -> crate::Result<CommitVersion>;
	fn current(&self) -> crate::Result<CommitVersion>;
}

#[derive(Debug)]
struct VersionBlock {
	last: u64,
	current: AtomicU64,
}

impl VersionBlock {
	fn new(start: u64) -> Self {
		Self {
			last: start + BLOCK_SIZE,
			current: AtomicU64::new(start),
		}
	}

	fn next(&self) -> Option<u64> {
		let version = self.current.fetch_add(1, Ordering::Relaxed);
		if version < self.last {
			Some(version + 1)
		} else {
			None
		}
	}

	fn current(&self) -> u64 {
		self.current.load(Ordering::Relaxed)
	}
}

#[derive(Debug)]
pub struct StdVersionProvider<UT>
where
	UT: UnversionedTransaction,
{
	unversioned: UT,
	current_block: Arc<Mutex<VersionBlock>>,
}

impl<UT> StdVersionProvider<UT>
where
	UT: UnversionedTransaction,
{
	pub fn new(unversioned: UT) -> crate::Result<Self> {
		// Load current version and allocate first block
		let current_version = Self::load_current_version(&unversioned)?;
		let first_block = VersionBlock::new(current_version);

		// Persist the end of first block to storage
		Self::persist_version(&unversioned, first_block.last)?;

		Ok(Self {
			unversioned,
			current_block: Arc::new(Mutex::new(first_block)),
		})
	}

	fn load_current_version(unversioned: &UT) -> crate::Result<u64> {
		let layout = EncodedRowLayout::new(&[Type::Uint8]);
		let key = TransactionVersionKey {}.encode();

		unversioned.with_query(|tx| match tx.get(&key)? {
			None => Ok(0),
			Some(unversioned) => Ok(layout.get_u64(&unversioned.row, 0)),
		})
	}

	fn persist_version(unversioned: &UT, version: u64) -> crate::Result<()> {
		let layout = EncodedRowLayout::new(&[Type::Uint8]);
		let key = TransactionVersionKey {}.encode();
		let mut row = layout.allocate_row();
		layout.set_u64(&mut row, 0, version);

		unversioned.with_command(|tx| {
			tx.set(&key, row)?;
			Ok(())
		})
	}
}

impl<UT> VersionProvider for StdVersionProvider<UT>
where
	UT: UnversionedTransaction,
{
	fn next(&self) -> crate::Result<CommitVersion> {
		// Fast path: try to get version from current block
		let mut block = self.current_block.lock().unwrap();
		if let Some(version) = block.next() {
			return Ok(version);
		}

		// Slow path: block exhausted, allocate new block
		// Allocate new block starting from current end
		let new_start = block.last;
		let new_block = VersionBlock::new(new_start);

		// Persist new block end to storage (expensive operation)
		Self::persist_version(&self.unversioned, new_block.last)?;
		*block = new_block;

		if let Some(version) = block.next() {
			return Ok(version);
		}

		panic!("Failed to allocate version from new block")
	}

	fn current(&self) -> crate::Result<CommitVersion> {
		let block = self.current_block.lock().unwrap();
		Ok(block.current())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::event::EventBus;
	use reifydb_storage::memory::Memory;

	use super::*;
	use crate::svl::SingleVersionLock;

	#[test]
	fn test_new_version_provider() {
		let memory = Memory::new();
		let unversioned = SingleVersionLock::new(memory, EventBus::default());
		let provider = StdVersionProvider::new(unversioned).unwrap();

		// Should start at version 0
		assert_eq!(provider.current().unwrap(), 0);
	}

	#[test]
	fn test_next_version_sequential() {
		let memory = Memory::new();
		let unversioned = SingleVersionLock::new(memory, EventBus::default());
		let provider = StdVersionProvider::new(unversioned).unwrap();

		assert_eq!(provider.next().unwrap(), 1);
		assert_eq!(provider.current().unwrap(), 1);

		assert_eq!(provider.next().unwrap(), 2);
		assert_eq!(provider.current().unwrap(), 2);

		assert_eq!(provider.next().unwrap(), 3);
		assert_eq!(provider.current().unwrap(), 3);
	}

	#[test]
	fn test_version_persistence() {
		let memory = Memory::new();
		let unversioned = SingleVersionLock::new(memory, EventBus::default());

		// Create first provider and get some versions
		{
			let provider = StdVersionProvider::new(unversioned.clone()).unwrap();
			assert_eq!(provider.next().unwrap(), 1);
			assert_eq!(provider.next().unwrap(), 2);
			assert_eq!(provider.next().unwrap(), 3);
		}

		// Create new provider with same storage - should continue from
		// persisted version
		let provider2 = StdVersionProvider::new(unversioned.clone()).unwrap();
		assert_eq!(provider2.next().unwrap(), BLOCK_SIZE + 1);
		assert_eq!(provider2.current().unwrap(), BLOCK_SIZE + 1);
	}

	#[test]
	fn test_block_exhaustion_and_allocation() {
		let memory = Memory::new();
		let unversioned = SingleVersionLock::new(memory, EventBus::default());
		let provider = StdVersionProvider::new(unversioned).unwrap();

		// Exhaust the first block
		for _ in 0..BLOCK_SIZE {
			provider.next().unwrap();
		}

		// Next version should trigger new block allocation
		assert_eq!(provider.current().unwrap(), BLOCK_SIZE);
		assert_eq!(provider.next().unwrap(), BLOCK_SIZE + 1);
		assert_eq!(provider.current().unwrap(), BLOCK_SIZE + 1);

		// Continue with next block
		assert_eq!(provider.next().unwrap(), BLOCK_SIZE + 2);
		assert_eq!(provider.current().unwrap(), BLOCK_SIZE + 2);
	}

	#[test]
	fn test_concurrent_version_allocation() {
		use std::{sync::Arc, thread};

		let memory = Memory::new();
		let unversioned = SingleVersionLock::new(memory, EventBus::default());
		let provider = Arc::new(StdVersionProvider::new(unversioned).unwrap());

		let mut handles = vec![];

		// Spawn multiple threads to request versions concurrently
		for _ in 0..10 {
			let provider_clone = Arc::clone(&provider);
			let handle = thread::spawn(move || {
				let mut versions = vec![];
				for _ in 0..100 {
					versions.push(provider_clone.next().unwrap());
				}
				versions
			});
			handles.push(handle);
		}

		// Collect all versions from all threads
		let mut all_versions = vec![];
		for handle in handles {
			let mut versions = handle.join().unwrap();
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

		// Should have exactly 1000 unique versions (10 threads * 100
		// versions each)
		assert_eq!(all_versions.len(), 1000);

		// First version should be 1, last should be 1000
		assert_eq!(all_versions[0], 1);
		assert_eq!(all_versions[999], 1000);
	}

	#[test]
	fn test_version_block_behavior() {
		let block = VersionBlock::new(100);

		// Should start at the given version
		assert_eq!(block.current(), 100);

		// Should return sequential versions
		assert_eq!(block.next(), Some(101));
		assert_eq!(block.current(), 101);

		assert_eq!(block.next(), Some(102));
		assert_eq!(block.current(), 102);
	}

	#[test]
	fn test_version_block_exhaustion() {
		let block = VersionBlock::new(0);

		for _ in 0..BLOCK_SIZE - 2 {
			block.next();
		}

		assert_eq!(block.next(), Some(BLOCK_SIZE - 1));
		assert_eq!(block.next(), Some(BLOCK_SIZE));

		// exhausted
		assert_eq!(block.next(), None);
	}

	#[test]
	fn test_load_existing_version() {
		let memory = Memory::new();
		let unversioned = SingleVersionLock::new(memory, EventBus::default());

		// Manually set a version in storage
		let layout = EncodedRowLayout::new(&[Type::Uint8]);
		let key = TransactionVersionKey {}.encode();
		let mut row = layout.allocate_row();
		layout.set_u64(&mut row, 0, 500u64);
		unversioned
			.with_command(|tx| {
				tx.set(&key, row)?;
				Ok(())
			})
			.unwrap();

		// Create provider - should start from the existing version
		let provider = StdVersionProvider::new(unversioned.clone()).unwrap();
		assert_eq!(provider.current().unwrap(), 500);
		assert_eq!(provider.next().unwrap(), 501);
	}
}
