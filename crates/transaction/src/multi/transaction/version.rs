// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
use reifydb_core::{
	common::CommitVersion,
	key::{EncodableKey, transaction_version::TransactionVersionKey},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{Result, reifydb_assertions, value::value_type::ValueType};

use crate::single::SingleTransaction;

const BLOCK_SIZE: u64 = 100_000;

pub trait VersionProvider: Send + Sync + Clone {
	fn next(&self) -> Result<CommitVersion>;
	fn reserve(&self) -> Result<CommitVersion>;
	fn publish(&self, version: CommitVersion);
	fn current(&self) -> Result<CommitVersion>;

	fn advance_to(&self, _version: CommitVersion) {}
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
}

#[derive(Clone)]
pub struct StandardVersionProvider {
	single: SingleTransaction,

	next_version: Arc<AtomicU64>,

	current_block_end: Arc<AtomicU64>,

	block_persist_lock: Arc<Mutex<()>>,
	shape: RowShape,
}

impl StandardVersionProvider {
	pub fn new(single: SingleTransaction) -> Result<Self> {
		let shape = RowShape::new(vec![RowShapeField::unconstrained("version", ValueType::Uint8)]);

		let current_version = Self::load_current_version(&shape, &single)?;
		let first_block = VersionBlock::new(current_version);

		Self::persist_version(&shape, &single, first_block.last)?;

		Ok(Self {
			single,
			next_version: Arc::new(AtomicU64::new(first_block.current)),
			current_block_end: Arc::new(AtomicU64::new(first_block.last)),
			block_persist_lock: Arc::new(Mutex::new(())),
			shape,
		})
	}

	fn load_current_version(shape: &RowShape, single: &SingleTransaction) -> Result<u64> {
		let key = TransactionVersionKey {}.encode();

		let mut tx = single.begin_query([&key])?;
		match tx.get(&key)? {
			None => Ok(0),
			Some(single) => Ok(shape.get::<u64>(&single.row, 0)),
		}
	}

	fn persist_version(shape: &RowShape, single: &SingleTransaction, version: u64) -> Result<()> {
		let key = TransactionVersionKey {}.encode();
		let mut row = shape.allocate();
		shape.set::<u64>(&mut row, 0, version);

		let mut tx = single.begin_command([&key])?;
		tx.set(&key, row)?;
		tx.commit()
	}
}

impl StandardVersionProvider {
	fn cover_with_persisted_block(&self, version: u64) -> Result<()> {
		let block_end = self.current_block_end.load(Ordering::SeqCst);
		if version <= block_end {
			return Ok(());
		}

		let _lock = self.block_persist_lock.lock();

		let block_end = self.current_block_end.load(Ordering::SeqCst);
		if version <= block_end {
			return Ok(());
		}

		let new_block_start = (version / BLOCK_SIZE) * BLOCK_SIZE;
		let new_block_end = new_block_start + BLOCK_SIZE;

		Self::persist_version(&self.shape, &self.single, new_block_end)?;

		self.current_block_end.store(new_block_end, Ordering::SeqCst);

		reifydb_assertions! {
			assert!(
				version <= new_block_end,
				"handed out a commit version past the block end that was just persisted, so a crash could re-issue \
				 the same version to a different commit and corrupt version history (version={version} \
				 persisted_block_end={new_block_end})"
			);
		}
		Ok(())
	}
}

impl VersionProvider for StandardVersionProvider {
	fn next(&self) -> Result<CommitVersion> {
		let version = self.next_version.fetch_add(1, Ordering::SeqCst) + 1;
		self.cover_with_persisted_block(version)?;
		Ok(CommitVersion(version))
	}

	fn reserve(&self) -> Result<CommitVersion> {
		let version = self.next_version.load(Ordering::SeqCst) + 1;
		self.cover_with_persisted_block(version)?;
		Ok(CommitVersion(version))
	}

	fn publish(&self, version: CommitVersion) {
		self.next_version.fetch_max(version.0, Ordering::SeqCst);
	}

	fn current(&self) -> Result<CommitVersion> {
		Ok(CommitVersion(self.next_version.load(Ordering::SeqCst)))
	}

	fn advance_to(&self, version: CommitVersion) {
		self.next_version.fetch_max(version.0, Ordering::SeqCst);
	}
}

#[cfg(test)]
pub mod tests {
	use std::{sync::Arc, thread};

	use super::*;

	#[test]
	fn test_new_version_provider() {
		let single = SingleTransaction::testing();
		let provider = StandardVersionProvider::new(single).unwrap();

		assert_eq!(provider.current().unwrap(), 0);
	}

	#[test]
	fn test_next_version_sequential() {
		let single = SingleTransaction::testing();
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
		let single = SingleTransaction::testing();

		{
			let provider = StandardVersionProvider::new(single.clone()).unwrap();
			assert_eq!(provider.next().unwrap(), 1);
			assert_eq!(provider.next().unwrap(), 2);
			assert_eq!(provider.next().unwrap(), 3);
		}

		// A restart resumes at the persisted block boundary, never reissuing versions from the
		// block the first provider had already claimed.
		let provider2 = StandardVersionProvider::new(single.clone()).unwrap();
		assert_eq!(provider2.next().unwrap(), BLOCK_SIZE + 1);
		assert_eq!(provider2.current().unwrap(), BLOCK_SIZE + 1);
	}

	#[test]
	fn test_block_exhaustion_and_allocation() {
		let single = SingleTransaction::testing();
		let provider = StandardVersionProvider::new(single).unwrap();

		for _ in 0..BLOCK_SIZE {
			provider.next().unwrap();
		}

		// Crossing a block boundary must stay contiguous; a gap here would strand versions.
		assert_eq!(provider.current().unwrap(), BLOCK_SIZE);
		assert_eq!(provider.next().unwrap(), BLOCK_SIZE + 1);
		assert_eq!(provider.current().unwrap(), BLOCK_SIZE + 1);

		assert_eq!(provider.next().unwrap(), BLOCK_SIZE + 2);
		assert_eq!(provider.current().unwrap(), BLOCK_SIZE + 2);
	}

	#[test]
	fn test_concurrent_version_allocation() {
		let single = SingleTransaction::testing();
		let provider = Arc::new(StandardVersionProvider::new(single).unwrap());

		let mut handles = vec![];

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

		let mut all_versions = vec![];
		for handle in handles {
			let mut versions = handle.join().unwrap();
			all_versions.append(&mut versions);
		}

		all_versions.sort();

		// A duplicate version would make two transactions share one commit point.
		for i in 1..all_versions.len() {
			assert_ne!(
				all_versions[i - 1],
				all_versions[i],
				"Duplicate version found: {}",
				all_versions[i]
			);
		}

		// 10 threads times 100 versions, with no gaps across the block boundaries they crossed.
		assert_eq!(all_versions.len(), 1000);

		assert_eq!(all_versions[0], 1);
		assert_eq!(all_versions[999], 1000);
	}

	#[test]
	fn a_reserved_version_stays_invisible_until_published() {
		// The oracle registers a reserved version on both watermarks before publishing it;
		// if reserve bumped the public counter, a concurrent begin could snapshot at a
		// version no watermark knows about, and the commit frontier could pass it before
		// registration - the torn-snapshot / lost-CDC window this API exists to close.
		let single = SingleTransaction::testing();
		let provider = StandardVersionProvider::new(single).unwrap();

		let reserved = provider.reserve().unwrap();
		assert_eq!(reserved, 1);
		assert_eq!(provider.current().unwrap(), 0, "reserve must not move the public version");

		let reserved_again = provider.reserve().unwrap();
		assert_eq!(reserved_again, 1, "an unpublished reservation must not consume the version");

		provider.publish(reserved);
		assert_eq!(provider.current().unwrap(), 1);
		assert_eq!(provider.next().unwrap(), 2);
	}

	#[test]
	fn reserving_across_a_block_boundary_persists_the_new_block() {
		// A crash between reserve and publish must never let a restart reissue the reserved
		// version to a different commit; the block covering it has to be durable at reserve
		// time, exactly as next() guarantees for published versions.
		let single = SingleTransaction::testing();

		{
			let provider = StandardVersionProvider::new(single.clone()).unwrap();
			for _ in 0..BLOCK_SIZE {
				provider.next().unwrap();
			}
			let reserved = provider.reserve().unwrap();
			assert_eq!(reserved, BLOCK_SIZE + 1);
			provider.publish(reserved);
			assert_eq!(provider.current().unwrap(), BLOCK_SIZE + 1);
		}

		let restarted = StandardVersionProvider::new(single).unwrap();
		assert!(
			restarted.next().unwrap().0 > BLOCK_SIZE + 1,
			"a restart handed out a version at or below one that was already reserved"
		);
	}

	#[test]
	fn test_version_block_initialization() {
		let block = VersionBlock::new(100);

		assert_eq!(block.current, 100);
		assert_eq!(block.last, 100 + BLOCK_SIZE);
	}

	#[test]
	fn test_load_existing_version() {
		let single = SingleTransaction::testing();

		let shape = RowShape::testing(&[ValueType::Uint8]);
		let key = TransactionVersionKey {}.encode();
		let mut row = shape.allocate();
		shape.set::<u64>(&mut row, 0, 500u64);

		{
			let mut tx = single.begin_command([&key]).unwrap();
			tx.set(&key, row).unwrap();
			tx.commit().unwrap();
		} // dropped here, releasing the key lock the provider needs

		let provider = StandardVersionProvider::new(single.clone()).unwrap();
		assert_eq!(provider.current().unwrap(), 500);
		assert_eq!(provider.next().unwrap(), 501);
	}
}
