// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	hash::Hash,
	marker::PhantomData,
	sync::atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;

/// Single-threaded LRU cache with exact LRU eviction.
///
/// This cache is `!Send` and `!Sync`, designed for use in single-threaded
/// contexts like operator state management.
pub struct LruCache<K, V> {
	map: HashMap<K, Entry<V>>,
	capacity: usize,
	access_counter: u64,
	_marker: PhantomData<*const ()>, // !Send + !Sync
}

struct Entry<V> {
	value: V,
	last_access: u64,
}

impl<K: Hash + Eq, V> LruCache<K, V> {
	pub fn new(capacity: usize) -> Self {
		assert!(capacity > 0, "LRU cache capacity must be greater than 0");
		Self {
			map: HashMap::with_capacity(capacity),
			capacity,
			access_counter: 0,
			_marker: PhantomData,
		}
	}

	/// Get a value and update LRU order.
	pub fn get(&mut self, key: &K) -> Option<&V> {
		let access = self.access_counter;
		self.access_counter += 1;
		if let Some(entry) = self.map.get_mut(key) {
			entry.last_access = access;
			Some(&entry.value)
		} else {
			None
		}
	}

	/// Get a mutable reference to a value and update LRU order.
	pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
		let access = self.access_counter;
		self.access_counter += 1;
		if let Some(entry) = self.map.get_mut(key) {
			entry.last_access = access;
			Some(&mut entry.value)
		} else {
			None
		}
	}

	/// Put a value into the cache. Returns old value if key existed.
	pub fn put(&mut self, key: K, value: V) -> Option<V> {
		let access = self.access_counter;
		self.access_counter += 1;

		// Check if key already exists
		if let Some(entry) = self.map.get_mut(&key) {
			let old_value = std::mem::replace(&mut entry.value, value);
			entry.last_access = access;
			return Some(old_value);
		}

		// Evict if at capacity before inserting
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

	/// Remove a value from the cache.
	pub fn remove(&mut self, key: &K) -> Option<V> {
		self.map.remove(key).map(|entry| entry.value)
	}

	/// Check if cache contains a key (read-only, doesn't update LRU order).
	pub fn contains_key(&self, key: &K) -> bool {
		self.map.contains_key(key)
	}

	/// Clear all entries from the cache.
	pub fn clear(&mut self) {
		self.map.clear();
	}

	/// Get the current number of entries.
	pub fn len(&self) -> usize {
		self.map.len()
	}

	/// Check if the cache is empty.
	pub fn is_empty(&self) -> bool {
		self.map.is_empty()
	}

	/// Get the capacity of the cache.
	pub fn capacity(&self) -> usize {
		self.capacity
	}

	/// Evict the least recently used entry.
	fn evict_lru(&mut self) {
		let oldest_key = self.map.iter().min_by_key(|(_, entry)| entry.last_access).map(|(k, _)| k as *const K);

		if let Some(key_ptr) = oldest_key {
			// SAFETY: key_ptr points to a key in the map, we remove it immediately
			unsafe {
				self.map.remove(&*key_ptr);
			}
		}
	}
}

/// Thread-safe LRU cache using DashMap with approximate LRU eviction.
///
/// Uses an atomic counter for access ordering instead of timestamps,
/// providing better performance while maintaining approximate LRU semantics.
pub struct ConcurrentLruCache<K, V> {
	map: DashMap<K, ConcurrentEntry<V>>,
	capacity: usize,
	access_counter: AtomicU64,
}

struct ConcurrentEntry<V> {
	value: V,
	last_access: u64,
}

impl<K: Hash + Eq + Clone, V: Clone> ConcurrentLruCache<K, V> {
	pub fn new(capacity: usize) -> Self {
		assert!(capacity > 0, "LRU cache capacity must be greater than 0");
		Self {
			map: DashMap::with_capacity(capacity),
			capacity,
			access_counter: AtomicU64::new(0),
		}
	}

	/// Get a value and update LRU order. Returns cloned value.
	pub fn get(&self, key: &K) -> Option<V> {
		if let Some(mut entry) = self.map.get_mut(key) {
			entry.last_access = self.access_counter.fetch_add(1, Ordering::Relaxed);
			Some(entry.value.clone())
		} else {
			None
		}
	}

	/// Put a value into the cache. Returns old value if key existed.
	pub fn put(&self, key: K, value: V) -> Option<V> {
		let access = self.access_counter.fetch_add(1, Ordering::Relaxed);

		// Check if key already exists
		if let Some(mut entry) = self.map.get_mut(&key) {
			let old_value = std::mem::replace(&mut entry.value, value);
			entry.last_access = access;
			return Some(old_value);
		}

		// Evict if at capacity before inserting
		if self.map.len() >= self.capacity {
			self.evict_lru();
		}

		self.map.insert(
			key,
			ConcurrentEntry {
				value,
				last_access: access,
			},
		);
		None
	}

	/// Remove a value from the cache.
	pub fn remove(&self, key: &K) -> Option<V> {
		self.map.remove(key).map(|(_, entry)| entry.value)
	}

	/// Check if cache contains a key (read-only, doesn't update LRU order).
	pub fn contains_key(&self, key: &K) -> bool {
		self.map.contains_key(key)
	}

	/// Clear all entries from the cache.
	pub fn clear(&self) {
		self.map.clear();
	}

	/// Get the current number of entries.
	pub fn len(&self) -> usize {
		self.map.len()
	}

	/// Check if the cache is empty.
	pub fn is_empty(&self) -> bool {
		self.map.is_empty()
	}

	/// Get the capacity of the cache.
	pub fn capacity(&self) -> usize {
		self.capacity
	}

	/// Evict the least recently used entry (approximate).
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
mod tests {

	mod lru {
		use crate::util::LruCache;

		#[test]
		fn test_basic_operations() {
			let mut cache = LruCache::new(2);

			assert_eq!(cache.put(1, "a"), None);
			assert_eq!(cache.put(2, "b"), None);
			assert_eq!(cache.get(&1), Some(&"a"));
			assert_eq!(cache.get(&2), Some(&"b"));
			assert_eq!(cache.len(), 2);
		}

		#[test]
		fn test_eviction() {
			let mut cache = LruCache::new(2);

			cache.put(1, "a");
			cache.put(2, "b");
			let evicted = cache.put(3, "c");

			// Eviction returns None because it's a new key insertion
			// The evicted value is the LRU entry (key 1)
			assert_eq!(evicted, None);
			assert_eq!(cache.get(&1), None);
			assert_eq!(cache.get(&2), Some(&"b"));
			assert_eq!(cache.get(&3), Some(&"c"));
		}

		#[test]
		fn test_lru_order() {
			let mut cache = LruCache::new(2);

			cache.put(1, "a");
			cache.put(2, "b");
			cache.get(&1); // Access 1, making it more recent than 2
			cache.put(3, "c"); // Should evict 2 (least recently used)

			assert_eq!(cache.get(&1), Some(&"a"));
			assert_eq!(cache.get(&2), None);
			assert_eq!(cache.get(&3), Some(&"c"));
		}

		#[test]
		fn test_update_existing() {
			let mut cache = LruCache::new(2);

			cache.put(1, "a");
			let old = cache.put(1, "b");

			assert_eq!(old, Some("a"));
			assert_eq!(cache.get(&1), Some(&"b"));
			assert_eq!(cache.len(), 1);
		}

		#[test]
		fn test_remove() {
			let mut cache = LruCache::new(2);

			cache.put(1, "a");
			cache.put(2, "b");
			let removed = cache.remove(&1);

			assert_eq!(removed, Some("a"));
			assert_eq!(cache.get(&1), None);
			assert_eq!(cache.len(), 1);
		}

		#[test]
		fn test_clear() {
			let mut cache = LruCache::new(2);

			cache.put(1, "a");
			cache.put(2, "b");
			cache.clear();

			assert_eq!(cache.len(), 0);
			assert!(cache.is_empty());
		}

		#[test]
		fn test_contains_key() {
			let mut cache = LruCache::new(2);

			cache.put(1, "a");
			assert!(cache.contains_key(&1));
			assert!(!cache.contains_key(&2));
		}
	}

	mod concurrent {
		use crate::util::ConcurrentLruCache;

		#[test]
		fn test_basic_operations() {
			let cache = ConcurrentLruCache::new(2);

			assert_eq!(cache.put(1, "a"), None);
			assert_eq!(cache.put(2, "b"), None);
			assert_eq!(cache.get(&1), Some("a"));
			assert_eq!(cache.get(&2), Some("b"));
			assert_eq!(cache.len(), 2);
		}

		#[test]
		fn test_eviction() {
			let cache = ConcurrentLruCache::new(2);

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
			let cache = ConcurrentLruCache::new(2);

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
			let cache = ConcurrentLruCache::new(2);

			cache.put(1, "a");
			let old = cache.put(1, "b");

			assert_eq!(old, Some("a"));
			assert_eq!(cache.get(&1), Some("b"));
			assert_eq!(cache.len(), 1);
		}

		#[test]
		fn test_remove() {
			let cache = ConcurrentLruCache::new(2);

			cache.put(1, "a");
			cache.put(2, "b");
			let removed = cache.remove(&1);

			assert_eq!(removed, Some("a"));
			assert_eq!(cache.get(&1), None);
			assert_eq!(cache.len(), 1);
		}

		#[test]
		fn test_clear() {
			let cache = ConcurrentLruCache::new(2);

			cache.put(1, "a");
			cache.put(2, "b");
			cache.clear();

			assert_eq!(cache.len(), 0);
			assert!(cache.is_empty());
		}

		#[test]
		fn test_contains_key() {
			let cache = ConcurrentLruCache::new(2);

			cache.put(1, "a");
			assert!(cache.contains_key(&1));
			assert!(!cache.contains_key(&2));
		}
	}
}
