// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{common::CommitVersion, interface::cdc::Cdc, util::lru::LruCache};

#[derive(Clone)]
pub struct BlockCache {
	inner: Arc<LruCache<CommitVersion, Arc<Vec<Cdc>>>>,
}

impl BlockCache {
	/// Default: 8 blocks * 1024 entries ~= 8K cached entries.
	pub const DEFAULT_CAPACITY: usize = 8;

	pub fn new(capacity: usize) -> Self {
		Self {
			inner: Arc::new(LruCache::new(capacity.max(1))),
		}
	}

	pub fn get(&self, key: CommitVersion) -> Option<Arc<Vec<Cdc>>> {
		self.inner.get(&key)
	}

	pub fn put(&self, key: CommitVersion, value: Arc<Vec<Cdc>>) {
		let _ = self.inner.put(key, value);
	}

	pub fn remove(&self, key: CommitVersion) {
		let _ = self.inner.remove(&key);
	}

	pub fn clear(&self) {
		self.inner.clear();
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn cv(n: u64) -> CommitVersion {
		CommitVersion(n)
	}

	fn empty_block() -> Arc<Vec<Cdc>> {
		Arc::new(Vec::new())
	}

	#[test]
	fn put_then_get_returns_inserted_arc() {
		let cache = BlockCache::new(4);
		let block = empty_block();
		cache.put(cv(1), block.clone());

		let got = cache.get(cv(1)).expect("entry should be present");
		assert!(Arc::ptr_eq(&got, &block));
	}

	#[test]
	fn get_returns_none_for_missing_key() {
		let cache = BlockCache::new(4);
		assert!(cache.get(cv(1)).is_none());

		cache.put(cv(1), empty_block());
		assert!(cache.get(cv(2)).is_none());
	}

	#[test]
	fn put_overwrites_existing_value() {
		let cache = BlockCache::new(4);
		let first = empty_block();
		let second = empty_block();
		assert!(!Arc::ptr_eq(&first, &second));

		cache.put(cv(1), first);
		cache.put(cv(1), second.clone());

		let got = cache.get(cv(1)).expect("entry should be present");
		assert!(Arc::ptr_eq(&got, &second));
	}

	#[test]
	fn remove_drops_value() {
		let cache = BlockCache::new(4);
		cache.put(cv(1), empty_block());

		cache.remove(cv(1));
		assert!(cache.get(cv(1)).is_none());
	}

	#[test]
	fn remove_missing_key_is_noop() {
		let cache = BlockCache::new(4);
		cache.remove(cv(99));

		cache.put(cv(1), empty_block());
		cache.remove(cv(99));
		assert!(cache.get(cv(1)).is_some());
	}

	#[test]
	fn clear_empties_cache() {
		let cache = BlockCache::new(4);
		cache.put(cv(1), empty_block());
		cache.put(cv(2), empty_block());
		cache.put(cv(3), empty_block());

		cache.clear();

		assert!(cache.get(cv(1)).is_none());
		assert!(cache.get(cv(2)).is_none());
		assert!(cache.get(cv(3)).is_none());
	}

	#[test]
	fn eviction_drops_least_recently_used() {
		let cache = BlockCache::new(2);
		cache.put(cv(1), empty_block());
		cache.put(cv(2), empty_block());
		cache.put(cv(3), empty_block());

		assert!(cache.get(cv(1)).is_none(), "oldest entry should be evicted");
		assert!(cache.get(cv(2)).is_some());
		assert!(cache.get(cv(3)).is_some());
	}

	#[test]
	fn get_promotes_recency_so_old_key_survives() {
		let cache = BlockCache::new(2);
		cache.put(cv(1), empty_block());
		cache.put(cv(2), empty_block());

		let _ = cache.get(cv(1));

		cache.put(cv(3), empty_block());

		assert!(cache.get(cv(1)).is_some(), "recently-touched key should survive");
		assert!(cache.get(cv(2)).is_none(), "untouched older key should be evicted");
		assert!(cache.get(cv(3)).is_some());
	}

	#[test]
	fn new_with_zero_capacity_is_clamped_and_usable() {
		let cache = BlockCache::new(0);
		cache.put(cv(1), empty_block());
		assert!(cache.get(cv(1)).is_some());
	}

	#[test]
	fn clone_shares_backing_storage() {
		let a = BlockCache::new(4);
		let b = a.clone();

		let block = empty_block();
		a.put(cv(1), block.clone());

		let got = b.get(cv(1)).expect("clone should see writes from original");
		assert!(Arc::ptr_eq(&got, &block));
	}
}
