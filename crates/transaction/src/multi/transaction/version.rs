// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

#[cfg(feature = "native")]
use reifydb_runtime::sync::mutex::native::Mutex;
#[cfg(feature = "wasm")]
use reifydb_runtime::sync::mutex::wasm::Mutex;
use reifydb_core::{
	common::CommitVersion,
	encoded::schema::{Schema, SchemaField},
	key::{EncodableKey, transaction_version::TransactionVersionKey},
};
use reifydb_type::{Result, value::r#type::Type};

use crate::single::TransactionSingle;

const BLOCK_SIZE: u64 = 100_000;

pub trait VersionProvider: Send + Sync + Clone {
	fn next(&self) -> Result<CommitVersion>;
	fn current(&self) -> Result<CommitVersion>;
}

/// Helper struct for initial block setup
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
}

#[derive(Clone)]
pub struct StandardVersionProvider {
	single: TransactionSingle,
	// Lock-free atomic counter for fast-path version allocation
	next_version: Arc<AtomicU64>,
	// Block boundary tracking (only accessed when crossing block boundaries)
	current_block_end: Arc<AtomicU64>,
	// Mutex for block boundary persistence (rare - 1 in BLOCK_SIZE operations)
	block_persist_lock: Arc<Mutex<()>>,
	schema: Schema,
}

impl StandardVersionProvider {
	pub fn new(single: TransactionSingle) -> Result<Self> {
		let schema = Schema::new(vec![SchemaField::unconstrained("version", Type::Uint8)]);

		// Load current version and allocate first block
		let current_version = Self::load_current_version(&schema, &single)?;
		let first_block = VersionBlock::new(current_version);

		// Persist the end of first block to storage
		Self::persist_version(&schema, &single, first_block.last)?;

		Ok(Self {
			single,
			next_version: Arc::new(AtomicU64::new(first_block.current)),
			current_block_end: Arc::new(AtomicU64::new(first_block.last)),
			block_persist_lock: Arc::new(Mutex::new(())),
			schema,
		})
	}

	fn load_current_version(schema: &Schema, single: &TransactionSingle) -> Result<u64> {
		let key = TransactionVersionKey {}.encode();

		let mut tx = single.begin_query([&key])?;
		match tx.get(&key)? {
			None => Ok(0),
			Some(single) => Ok(schema.get_u64(&single.values, 0)),
		}
	}

	fn persist_version(schema: &Schema, single: &TransactionSingle, version: u64) -> Result<()> {
		let key = TransactionVersionKey {}.encode();
		let mut values = schema.allocate();
		schema.set_u64(&mut values, 0, version);

		let mut tx = single.begin_command([&key])?;
		tx.set(&key, values)?;
		tx.commit()
	}
}

impl VersionProvider for StandardVersionProvider {
	fn next(&self) -> Result<CommitVersion> {
		// FAST PATH: Lock-free atomic increment
		let version = self.next_version.fetch_add(1, Ordering::SeqCst) + 1;

		// Check if we're still within the current block
		let block_end = self.current_block_end.load(Ordering::SeqCst);
		if version <= block_end {
			return Ok(CommitVersion(version));
		}

		// SLOW PATH: We've crossed a block boundary, need to persist
		// This is rare (1 in BLOCK_SIZE = 100,000 operations)
		let _lock = self.block_persist_lock.lock();

		// Double-check: another thread may have already extended the block
		let block_end = self.current_block_end.load(Ordering::SeqCst);
		if version <= block_end {
			return Ok(CommitVersion(version));
		}

		// Calculate new block boundary
		// The version we allocated may be beyond the current block_end
		// We need to allocate enough blocks to cover it
		let new_block_start = (version / BLOCK_SIZE) * BLOCK_SIZE;
		let new_block_end = new_block_start + BLOCK_SIZE;

		// Persist the new block boundary to storage
		Self::persist_version(&self.schema, &self.single, new_block_end)?;

		// Update the block end atomically
		self.current_block_end.store(new_block_end, Ordering::SeqCst);

		Ok(CommitVersion(version))
	}

	fn current(&self) -> Result<CommitVersion> {
		Ok(CommitVersion(self.next_version.load(Ordering::SeqCst)))
	}
}

#[cfg(test)]
pub mod tests {
	use std::sync::Arc;

	use super::*;

	#[test]
	fn test_new_version_provider() {
		let single = TransactionSingle::testing();
		let provider = StandardVersionProvider::new(single).unwrap();

		// Should start at version 0
		assert_eq!(provider.current().unwrap(), 0);
	}

	#[test]
	fn test_next_version_sequential() {
		let single = TransactionSingle::testing();
		let provider = StandardVersionProvider::new(single).unwrap();

		assert_eq!(provider.next().unwrap(), 1);
		assert_eq!(provider.current().unwrap(), 1);

		assert_eq!(provider.next().unwrap(), 2);
		assert_eq!(provider.current().unwrap(), 2);

		assert_eq!(provider.next().unwrap(), 3);
		assert_eq!(provider.current().unwrap(), 3);
	}

	#[test]
	fn test_version_persistence() {
		let single = TransactionSingle::testing();

		// Create first provider and get some versions
		{
			let provider = StandardVersionProvider::new(single.clone()).unwrap();
			assert_eq!(provider.next().unwrap(), 1);
			assert_eq!(provider.next().unwrap(), 2);
			assert_eq!(provider.next().unwrap(), 3);
		}

		// Create new provider with same storage - should continue from
		// persisted version
		let provider2 = StandardVersionProvider::new(single.clone()).unwrap();
		assert_eq!(provider2.next().unwrap(), BLOCK_SIZE + 1);
		assert_eq!(provider2.current().unwrap(), BLOCK_SIZE + 1);
	}

	#[test]
	fn test_block_exhaustion_and_allocation() {
		let single = TransactionSingle::testing();
		let provider = StandardVersionProvider::new(single).unwrap();

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
		let single = TransactionSingle::testing();
		let provider = Arc::new(StandardVersionProvider::new(single).unwrap());

		let mut handles = vec![];

		// Spawn multiple tasks to request versions concurrently
		for _ in 0..10 {
			let provider_clone = Arc::clone(&provider);
			let handle = std::thread::spawn(move || {
				let mut versions = vec![];
				for _ in 0..100 {
					versions.push(provider_clone.next().unwrap());
				}
				versions
			});
			handles.push(handle);
		}

		// Collect all versions from all tasks
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

		// Should have exactly 1000 unique versions (10 tasks * 100
		// versions each)
		assert_eq!(all_versions.len(), 1000);

		// First version should be 1, last should be 1000
		assert_eq!(all_versions[0], 1);
		assert_eq!(all_versions[999], 1000);
	}

	#[test]
	fn test_version_block_initialization() {
		let block = VersionBlock::new(100);

		// Should have correct start and end
		assert_eq!(block.current, 100);
		assert_eq!(block.last, 100 + BLOCK_SIZE);
	}

	#[test]
	fn test_load_existing_version() {
		let single = TransactionSingle::testing();

		// Manually set a version in storage
		let schema = Schema::testing(&[Type::Uint8]);
		let key = TransactionVersionKey {}.encode();
		let mut values = schema.allocate();
		schema.set_u64(&mut values, 0, 500u64);

		{
			let mut tx = single.begin_command([&key]).unwrap();
			tx.set(&key, values).unwrap();
			tx.commit().unwrap();
		} // tx is dropped here, releasing the key lock

		// Create provider - should start from the existing version
		let provider = StandardVersionProvider::new(single.clone()).unwrap();
		assert_eq!(provider.current().unwrap(), 500);
		assert_eq!(provider.next().unwrap(), 501);
	}
}
