// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::hash::Hash;

use moka::{
	policy::EvictionPolicy,
	sync::{Cache, CacheBuilder},
};

pub struct NativeLru<K, V>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	V: Clone + Send + Sync + 'static,
{
	cache: Cache<K, V>,
	capacity: usize,
}

impl<K, V> NativeLru<K, V>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	V: Clone + Send + Sync + 'static,
{
	pub fn new(capacity: usize) -> Self {
		let cache = CacheBuilder::new(capacity as u64).eviction_policy(EvictionPolicy::lru()).build();
		Self {
			cache,
			capacity,
		}
	}

	pub fn get(&self, key: &K) -> Option<V> {
		self.cache.get(key)
	}

	pub fn put(&self, key: K, value: V) -> Option<V> {
		let old = self.cache.get(&key);
		self.cache.insert(key, value);
		self.cache.run_pending_tasks();
		old
	}

	pub fn remove(&self, key: &K) -> Option<V> {
		let old = self.cache.get(key);
		self.cache.invalidate(key);
		self.cache.run_pending_tasks();
		old
	}

	pub fn contains_key(&self, key: &K) -> bool {
		self.cache.contains_key(key)
	}

	pub fn clear(&self) {
		self.cache.invalidate_all();
		self.cache.run_pending_tasks();
	}

	pub fn len(&self) -> usize {
		self.cache.entry_count() as usize
	}

	pub fn capacity(&self) -> usize {
		self.capacity
	}
}
