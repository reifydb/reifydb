// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	hash::Hash,
	mem,
	sync::atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;

pub struct LruCache<K, V> {
	map: DashMap<K, Entry<V>>,
	capacity: usize,
	access_counter: AtomicU64,
}

struct Entry<V> {
	value: V,
	last_access: u64,
}

impl<K: Hash + Eq + Clone, V: Clone> LruCache<K, V> {
	pub fn new(capacity: usize) -> Self {
		assert!(capacity > 0, "LRU cache capacity must be greater than 0");
		Self {
			map: DashMap::with_capacity(capacity),
			capacity,
			access_counter: AtomicU64::new(0),
		}
	}

	pub fn get(&self, key: &K) -> Option<V> {
		if let Some(mut entry) = self.map.get_mut(key) {
			entry.last_access = self.access_counter.fetch_add(1, Ordering::Relaxed);
			Some(entry.value.clone())
		} else {
			None
		}
	}

	pub fn put(&self, key: K, value: V) -> Option<V> {
		let access = self.access_counter.fetch_add(1, Ordering::Relaxed);

		if let Some(mut entry) = self.map.get_mut(&key) {
			let old_value = mem::replace(&mut entry.value, value);
			entry.last_access = access;
			return Some(old_value);
		}

		if self.map.len() >= self.capacity {
			self.evict_lru();
		}

		self.map.insert(
			key,
			Entry {
				value,
				last_access: access,
			},
		);
		None
	}

	pub fn remove(&self, key: &K) -> Option<V> {
		self.map.remove(key).map(|(_, entry)| entry.value)
	}

	pub fn contains_key(&self, key: &K) -> bool {
		self.map.contains_key(key)
	}

	pub fn clear(&self) {
		self.map.clear();
	}

	pub fn len(&self) -> usize {
		self.map.len()
	}

	pub fn is_empty(&self) -> bool {
		self.map.is_empty()
	}

	pub fn capacity(&self) -> usize {
		self.capacity
	}

	fn evict_lru(&self) {
		let mut oldest_key: Option<K> = None;
		let mut oldest_access = u64::MAX;

		for entry in self.map.iter() {
			if entry.last_access < oldest_access {
				oldest_access = entry.last_access;
				oldest_key = Some(entry.key().clone());
			}
		}

		if let Some(key) = oldest_key {
			self.map.remove(&key);
		}
	}
}

#[cfg(test)]
pub mod tests {

	mod lru {
		use crate::util::lru::LruCache;

		#[test]
		fn test_basic_operations() {
			let cache = LruCache::new(2);

			assert_eq!(cache.put(1, "a"), None);
			assert_eq!(cache.put(2, "b"), None);
			assert_eq!(cache.get(&1), Some("a"));
			assert_eq!(cache.get(&2), Some("b"));
			assert_eq!(cache.len(), 2);
		}

		#[test]
		fn test_eviction() {
			let cache = LruCache::new(2);

			cache.put(1, "a");
			cache.put(2, "b");
			let evicted = cache.put(3, "c");

			// Eviction returns None because it's a new key insertion
			// The evicted value is the LRU entry (key 1)
			assert_eq!(evicted, None);
			assert_eq!(cache.get(&1), None);
			assert_eq!(cache.get(&2), Some("b"));
			assert_eq!(cache.get(&3), Some("c"));
		}

		#[test]
		fn test_lru_order() {
			let cache = LruCache::new(2);

			cache.put(1, "a");
			cache.put(2, "b");
			cache.get(&1); // Access 1, making it more recent than 2
			cache.put(3, "c"); // Should evict 2 (least recently used)

			assert_eq!(cache.get(&1), Some("a"));
			assert_eq!(cache.get(&2), None);
			assert_eq!(cache.get(&3), Some("c"));
		}

		#[test]
		fn test_update_existing() {
			let cache = LruCache::new(2);

			cache.put(1, "a");
			let old = cache.put(1, "b");

			assert_eq!(old, Some("a"));
			assert_eq!(cache.get(&1), Some("b"));
			assert_eq!(cache.len(), 1);
		}

		#[test]
		fn test_remove() {
			let cache = LruCache::new(2);

			cache.put(1, "a");
			cache.put(2, "b");
			let removed = cache.remove(&1);

			assert_eq!(removed, Some("a"));
			assert_eq!(cache.get(&1), None);
			assert_eq!(cache.len(), 1);
		}

		#[test]
		fn test_clear() {
			let cache = LruCache::new(2);

			cache.put(1, "a");
			cache.put(2, "b");
			cache.clear();

			assert_eq!(cache.len(), 0);
			assert!(cache.is_empty());
		}

		#[test]
		fn test_contains_key() {
			let cache = LruCache::new(2);

			cache.put(1, "a");
			assert!(cache.contains_key(&1));
			assert!(!cache.contains_key(&2));
		}
	}
}
