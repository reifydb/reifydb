// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, hash::Hash};

use tokio::sync::RwLock;

/// Thread-safe LRU cache with tokio::sync::RwLock
pub struct LruCache<K, V> {
	inner: RwLock<LruCacheInner<K, V>>,
}

struct LruCacheInner<K, V> {
	map: HashMap<K, usize>,
	entries: Vec<Entry<K, V>>,
	head: Option<usize>,
	tail: Option<usize>,
	capacity: usize,
}

struct Entry<K, V> {
	key: K,
	value: V,
	prev: Option<usize>,
	next: Option<usize>,
}

impl<K: Hash + Eq + Clone, V: Clone> LruCache<K, V> {
	pub fn new(capacity: usize) -> Self {
		assert!(capacity > 0, "LRU cache capacity must be greater than 0");
		Self {
			inner: RwLock::new(LruCacheInner {
				map: HashMap::with_capacity(capacity),
				entries: Vec::with_capacity(capacity),
				head: None,
				tail: None,
				capacity,
			}),
		}
	}

	/// Get a value and update LRU order. Returns cloned value.
	pub async fn get(&self, key: &K) -> Option<V> {
		let mut inner = self.inner.write().await;
		if let Some(&idx) = inner.map.get(key) {
			inner.move_to_front(idx);
			Some(inner.entries[idx].value.clone())
		} else {
			None
		}
	}

	/// Put a value into the cache. Returns old value if key existed.
	pub async fn put(&self, key: K, value: V) -> Option<V> {
		let mut inner = self.inner.write().await;

		if let Some(&idx) = inner.map.get(&key) {
			let old_value = std::mem::replace(&mut inner.entries[idx].value, value);
			inner.move_to_front(idx);
			return Some(old_value);
		}

		let evicted = if inner.entries.len() >= inner.capacity {
			inner.evict_lru()
		} else {
			None
		};

		let idx = inner.entries.len();
		let old_head = inner.head;

		inner.entries.push(Entry {
			key: key.clone(),
			value,
			prev: None,
			next: old_head,
		});

		if let Some(old_head_idx) = old_head {
			inner.entries[old_head_idx].prev = Some(idx);
		}

		inner.head = Some(idx);

		if inner.tail.is_none() {
			inner.tail = Some(idx);
		}

		inner.map.insert(key, idx);
		evicted
	}

	/// Remove a value from the cache.
	pub async fn remove(&self, key: &K) -> Option<V> {
		let mut inner = self.inner.write().await;

		if let Some(idx) = inner.map.remove(key) {
			inner.unlink(idx);
			let entry = inner.entries.swap_remove(idx);

			if idx < inner.entries.len() {
				let moved_key = inner.entries[idx].key.clone();
				inner.map.insert(moved_key, idx);

				if let Some(prev) = inner.entries[idx].prev {
					inner.entries[prev].next = Some(idx);
				} else {
					inner.head = Some(idx);
				}

				if let Some(next) = inner.entries[idx].next {
					inner.entries[next].prev = Some(idx);
				} else {
					inner.tail = Some(idx);
				}
			}

			Some(entry.value)
		} else {
			None
		}
	}

	/// Check if cache contains a key (read-only, doesn't update LRU order).
	pub async fn contains_key(&self, key: &K) -> bool {
		let inner = self.inner.read().await;
		inner.map.contains_key(key)
	}

	/// Clear all entries from the cache.
	pub async fn clear(&self) {
		let mut inner = self.inner.write().await;
		inner.map.clear();
		inner.entries.clear();
		inner.head = None;
		inner.tail = None;
	}

	/// Get the current number of entries (read-only).
	///
	/// Note: This uses try_read() and returns 0 if the lock is currently held.
	/// For accurate counts in async contexts, consider using an async method if needed.
	pub fn len(&self) -> usize {
		self.inner.try_read().map(|guard| guard.entries.len()).unwrap_or(0)
	}

	/// Check if the cache is empty (read-only).
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// Get the capacity of the cache (read-only).
	pub fn capacity(&self) -> usize {
		self.inner.try_read().map(|guard| guard.capacity).unwrap_or(0)
	}
}

impl<K, V> LruCacheInner<K, V>
where
	K: Hash + Eq + Clone,
{
	fn move_to_front(&mut self, idx: usize) {
		if self.head == Some(idx) {
			return;
		}

		self.unlink(idx);

		self.entries[idx].prev = None;
		self.entries[idx].next = self.head;

		if let Some(old_head) = self.head {
			self.entries[old_head].prev = Some(idx);
		}

		self.head = Some(idx);

		if self.tail.is_none() {
			self.tail = Some(idx);
		}
	}

	fn unlink(&mut self, idx: usize) {
		let entry = &self.entries[idx];
		let prev = entry.prev;
		let next = entry.next;

		if let Some(p) = prev {
			self.entries[p].next = next;
		} else {
			self.head = next;
		}

		if let Some(n) = next {
			self.entries[n].prev = prev;
		} else {
			self.tail = prev;
		}
	}

	fn evict_lru(&mut self) -> Option<V> {
		if let Some(tail_idx) = self.tail {
			let key = self.entries[tail_idx].key.clone();

			if let Some(idx) = self.map.remove(&key) {
				self.unlink(idx);
				let entry = self.entries.swap_remove(idx);

				if idx < self.entries.len() {
					let moved_key = self.entries[idx].key.clone();
					self.map.insert(moved_key, idx);

					if let Some(prev) = self.entries[idx].prev {
						self.entries[prev].next = Some(idx);
					} else {
						self.head = Some(idx);
					}

					if let Some(next) = self.entries[idx].next {
						self.entries[next].prev = Some(idx);
					} else {
						self.tail = Some(idx);
					}
				}

				return Some(entry.value);
			}
		}
		None
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn test_basic_operations() {
		let cache = LruCache::new(2);

		assert_eq!(cache.put(1, "a").await, None);
		assert_eq!(cache.put(2, "b").await, None);
		assert_eq!(cache.get(&1).await, Some("a"));
		assert_eq!(cache.get(&2).await, Some("b"));
		assert_eq!(cache.len(), 2);
	}

	#[tokio::test]
	async fn test_eviction() {
		let cache = LruCache::new(2);

		cache.put(1, "a").await;
		cache.put(2, "b").await;
		let evicted = cache.put(3, "c").await;

		assert_eq!(evicted, Some("a"));
		assert_eq!(cache.get(&1).await, None);
		assert_eq!(cache.get(&2).await, Some("b"));
		assert_eq!(cache.get(&3).await, Some("c"));
	}

	#[tokio::test]
	async fn test_lru_order() {
		let cache = LruCache::new(2);

		cache.put(1, "a").await;
		cache.put(2, "b").await;
		cache.get(&1).await;
		cache.put(3, "c").await;

		assert_eq!(cache.get(&1).await, Some("a"));
		assert_eq!(cache.get(&2).await, None);
		assert_eq!(cache.get(&3).await, Some("c"));
	}

	#[tokio::test]
	async fn test_update_existing() {
		let cache = LruCache::new(2);

		cache.put(1, "a").await;
		let old = cache.put(1, "b").await;

		assert_eq!(old, Some("a"));
		assert_eq!(cache.get(&1).await, Some("b"));
		assert_eq!(cache.len(), 1);
	}

	#[tokio::test]
	async fn test_remove() {
		let cache = LruCache::new(2);

		cache.put(1, "a").await;
		cache.put(2, "b").await;
		let removed = cache.remove(&1).await;

		assert_eq!(removed, Some("a"));
		assert_eq!(cache.get(&1).await, None);
		assert_eq!(cache.len(), 1);
	}

	#[tokio::test]
	async fn test_clear() {
		let cache = LruCache::new(2);

		cache.put(1, "a").await;
		cache.put(2, "b").await;
		cache.clear().await;

		assert_eq!(cache.len(), 0);
		assert!(cache.is_empty());
	}

	#[tokio::test]
	async fn test_contains_key() {
		let cache = LruCache::new(2);

		cache.put(1, "a").await;
		assert!(cache.contains_key(&1).await);
		assert!(!cache.contains_key(&2).await);
	}
}
